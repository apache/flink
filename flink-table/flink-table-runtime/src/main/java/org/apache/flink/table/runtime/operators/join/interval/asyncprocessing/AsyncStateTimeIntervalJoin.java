/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.join.interval.asyncprocessing;

import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.v2.MapState;
import org.apache.flink.api.common.state.v2.MapStateDescriptor;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.state.v2.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.OuterJoinPaddingUtil;
import org.apache.flink.table.runtime.operators.join.interval.EmitAwareCollector;
import org.apache.flink.table.runtime.operators.join.interval.IntervalJoinFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.Collector;
import org.apache.flink.util.function.ThrowingConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The async state version of {@code TimeIntervalJoin}, a function to execute a time interval
 * (time-bounded) stream join. It keeps the state and the logic of the synchronous version with two
 * fixes: rows at time zero or earlier are joined and kept like rows after time zero, and cleanup
 * times saturate instead of overflowing. As in the synchronous version, the event-time operator
 * time is never below zero, so a row whose window ends at or before time zero is not cached. Early
 * firing is not supported.
 *
 * <p>State iterators cannot remove entries, so the changes to a scanned cache are written after the
 * scan. Records of several keys are in flight at a time, so what the callbacks of a record need,
 * such as its join key and the operator time, is taken when its processing starts. Unlike the
 * synchronous version, this function does not enable interruptible timers, which the async state
 * operators do not support.
 */
abstract class AsyncStateTimeIntervalJoin
        extends KeyedCoProcessFunction<RowData, RowData, RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(AsyncStateTimeIntervalJoin.class);

    // If all buckets of a cache expire, up to this many are deleted in one bulk request, which
    // spares ForSt the scan of clear(). More are deleted by clear(), which the synchronous state
    // backends batch instead of running one delete per bucket.
    private static final int MAX_BULK_DELETE_ON_FULL_EXPIRY = 65_536;

    private final FlinkJoinType joinType;
    protected final long leftRelativeSize;
    protected final long rightRelativeSize;

    // Minimum interval by which state is cleaned up
    private final long minCleanUpInterval;
    protected final long allowedLateness;
    private final InternalTypeInfo<RowData> leftType;
    private final InternalTypeInfo<RowData> rightType;
    private final IntervalJoinFunction joinFunction;
    private transient OuterJoinPaddingUtil paddingUtil;

    // cache to store rows from the left stream
    private transient MapState<Long, List<Tuple2<RowData, Boolean>>> leftCache;
    // cache to store rows from the right stream
    private transient MapState<Long, List<Tuple2<RowData, Boolean>>> rightCache;

    // state to record the timer on the left stream. null means no timer set
    private transient ValueState<Long> leftTimerState;
    // state to record the timer on the right stream. null means no timer set
    private transient ValueState<Long> rightTimerState;

    // Points in time until which the respective cache has been cleaned. They start at
    // Long.MIN_VALUE rather than 0, so the first rows at time zero or earlier are joined, and never
    // decrease, since a callback may update them with the time of an earlier timer.
    private long leftExpirationTime = Long.MIN_VALUE;
    private long rightExpirationTime = Long.MIN_VALUE;

    AsyncStateTimeIntervalJoin(
            FlinkJoinType joinType,
            long leftLowerBound,
            long leftUpperBound,
            long allowedLateness,
            long minCleanUpInterval,
            InternalTypeInfo<RowData> leftType,
            InternalTypeInfo<RowData> rightType,
            IntervalJoinFunction joinFunc) {
        this.joinType = joinType;
        this.leftRelativeSize = -leftLowerBound;
        this.rightRelativeSize = leftUpperBound;
        this.minCleanUpInterval = Math.max(0, minCleanUpInterval);
        if (allowedLateness < 0) {
            throw new IllegalArgumentException("The allowed lateness must be non-negative.");
        }
        this.allowedLateness = allowedLateness;
        this.leftType = leftType;
        this.rightType = rightType;
        this.joinFunction = joinFunc;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        LOG.info("Interval join is using async state");
        joinFunction.setRuntimeContext(getRuntimeContext());
        joinFunction.open(DefaultOpenContext.INSTANCE);

        // Initialize the data caches.
        ListTypeInfo<Tuple2<RowData, Boolean>> leftRowListTypeInfo =
                new ListTypeInfo<>(new TupleTypeInfo<>(leftType, BasicTypeInfo.BOOLEAN_TYPE_INFO));
        MapStateDescriptor<Long, List<Tuple2<RowData, Boolean>>> leftMapStateDescriptor =
                new MapStateDescriptor<>(
                        "IntervalJoinLeftCache", BasicTypeInfo.LONG_TYPE_INFO, leftRowListTypeInfo);
        leftCache = getRuntimeContext().getMapState(leftMapStateDescriptor);

        ListTypeInfo<Tuple2<RowData, Boolean>> rightRowListTypeInfo =
                new ListTypeInfo<>(new TupleTypeInfo<>(rightType, BasicTypeInfo.BOOLEAN_TYPE_INFO));
        MapStateDescriptor<Long, List<Tuple2<RowData, Boolean>>> rightMapStateDescriptor =
                new MapStateDescriptor<>(
                        "IntervalJoinRightCache",
                        BasicTypeInfo.LONG_TYPE_INFO,
                        rightRowListTypeInfo);
        rightCache = getRuntimeContext().getMapState(rightMapStateDescriptor);

        // Initialize the timer states.
        ValueStateDescriptor<Long> leftValueStateDescriptor =
                new ValueStateDescriptor<>("IntervalJoinLeftTimerState", Long.class);
        leftTimerState = getRuntimeContext().getState(leftValueStateDescriptor);

        ValueStateDescriptor<Long> rightValueStateDescriptor =
                new ValueStateDescriptor<>("IntervalJoinRightTimerState", Long.class);
        rightTimerState = getRuntimeContext().getState(rightValueStateDescriptor);

        paddingUtil = new OuterJoinPaddingUtil(leftType.toRowSize(), rightType.toRowSize());
    }

    @Override
    public void close() throws Exception {
        if (this.joinFunction != null) {
            this.joinFunction.close();
        }
    }

    @Override
    public void processElement1(RowData leftRow, Context ctx, Collector<RowData> out)
            throws Exception {
        processElement(leftRow, ctx, out, true);
    }

    @Override
    public void processElement2(RowData rightRow, Context ctx, Collector<RowData> out)
            throws Exception {
        processElement(rightRow, ctx, out, false);
    }

    /**
     * Joins the row with the cached rows of the other input and caches it for the rows of the other
     * input still to come: {@code processElement1} and {@code processElement2} of the synchronous
     * version, written once for both inputs.
     */
    private void processElement(RowData row, Context ctx, Collector<RowData> out, boolean isLeft) {
        final RowData joinKey = ctx.getCurrentKey();
        final long operatorTime = getOperatorTime(ctx);
        final long rowTime =
                isLeft
                        ? getTimeForLeftStream(ctx, row, operatorTime)
                        : getTimeForRightStream(ctx, row, operatorTime);
        final long otherQualifiedLowerBound =
                rowTime - (isLeft ? rightRelativeSize : leftRelativeSize);
        final long otherQualifiedUpperBound =
                rowTime + (isLeft ? leftRelativeSize : rightRelativeSize);
        final boolean thisOuter = isLeft ? joinType.isLeftOuter() : joinType.isRightOuter();
        final boolean otherOuter = isLeft ? joinType.isRightOuter() : joinType.isLeftOuter();
        final boolean[] emitted = {false};

        // Check if we need to join the current row against cached rows of the other input.
        // The condition here should be otherMinimumTime < otherQualifiedUpperBound.
        // We use the expiration time of the other cache as an approximation of the
        // otherMinimumTime here, since it is always smaller or equal.
        final StateFuture<Void> joined;
        if ((isLeft ? rightExpirationTime : leftExpirationTime) < otherQualifiedUpperBound) {
            // Upper bound of current join window has not passed the cache expiration time yet.
            // There might be qualifying rows in the cache that the current row needs to be joined
            // with.
            final long expirationTime =
                    isLeft
                            ? updateRightExpirationTime(
                                    calExpirationTime(operatorTime, rightRelativeSize))
                            : updateLeftExpirationTime(
                                    calExpirationTime(operatorTime, leftRelativeSize));
            final MapState<Long, List<Tuple2<RowData, Boolean>>> otherCache =
                    isLeft ? rightCache : leftCache;
            // A collector per record, as the callbacks of other records run in between.
            final EmitAwareCollector joinCollector = new EmitAwareCollector();
            joinCollector.setInnerCollector(out);
            final Map<Long, List<Tuple2<RowData, Boolean>>> updatedEntries = new HashMap<>();
            final Map<Long, List<Tuple2<RowData, Boolean>>> expiredEntries = new HashMap<>();
            final ThrowingConsumer<Map.Entry<Long, List<Tuple2<RowData, Boolean>>>, Exception>
                    joinEntry =
                            entry -> {
                                final long otherTime = entry.getKey();
                                final List<Tuple2<RowData, Boolean>> otherRows = entry.getValue();
                                final boolean expired = otherTime <= expirationTime;
                                if (otherTime >= otherQualifiedLowerBound
                                        && otherTime <= otherQualifiedUpperBound) {
                                    boolean entryUpdated = false;
                                    for (Tuple2<RowData, Boolean> tuple : otherRows) {
                                        joinCollector.reset();
                                        if (isLeft) {
                                            joinFunction.join(
                                                    row, tuple.f0, joinKey, joinCollector);
                                        } else {
                                            joinFunction.join(
                                                    tuple.f0, row, joinKey, joinCollector);
                                        }
                                        emitted[0] |= joinCollector.isEmitted();
                                        if (otherOuter && !tuple.f1 && joinCollector.isEmitted()) {
                                            // Mark the other row as being successfully joined and
                                            // emitted.
                                            tuple.f1 = true;
                                            entryUpdated = true;
                                        }
                                    }
                                    // An expired entry is removed below instead.
                                    if (entryUpdated && !expired) {
                                        updatedEntries.put(otherTime, otherRows);
                                    }
                                }
                                // Clean up the expired rows of the other cache while joining.
                                if (expired) {
                                    if (otherOuter) {
                                        padUnmatched(otherRows, !isLeft, out);
                                    }
                                    expiredEntries.put(otherTime, null);
                                }
                            };
            joined =
                    otherCache
                            .asyncEntries()
                            .thenCompose(entries -> entries.onNext(joinEntry))
                            .thenCompose(ignored -> putAll(otherCache, updatedEntries))
                            .thenCompose(ignored -> putAll(otherCache, expiredEntries));
        } else {
            joined = StateFutureUtils.completedVoidFuture();
        }

        joined.thenCompose(
                ignored -> {
                    // Check if we need to cache the current row. A row that can never match is only
                    // cached to pad it with nulls once its window has closed.
                    if (operatorTime < otherQualifiedUpperBound
                            && (thisOuter || !joinFunction.keyCannotMatch(joinKey))) {
                        // Operator time of the other stream has not exceeded the upper window bound
                        // of the current row. Put it into the cache, since later coming records
                        // from the other stream are expected to be joined with it.
                        return cacheRow(row, rowTime, emitted[0], ctx, isLeft);
                    }
                    if (!emitted[0] && thisOuter) {
                        // Emit a null padding result if the row is not cached and successfully
                        // joined.
                        out.collect(isLeft ? paddingUtil.padLeft(row) : paddingUtil.padRight(row));
                    }
                    return StateFutureUtils.completedVoidFuture();
                });
    }

    private StateFuture<Void> cacheRow(
            RowData row, long rowTime, boolean emitted, Context ctx, boolean isLeft) {
        final MapState<Long, List<Tuple2<RowData, Boolean>>> cache =
                isLeft ? leftCache : rightCache;
        // The cleanup timer of the cache is recorded in the timer state of the other stream.
        final ValueState<Long> timerState = isLeft ? rightTimerState : leftTimerState;
        return cache.asyncGet(rowTime)
                .thenCompose(
                        rows -> {
                            final List<Tuple2<RowData, Boolean>> rowList =
                                    rows == null ? new ArrayList<>(1) : rows;
                            rowList.add(Tuple2.of(row, emitted));
                            return cache.asyncPut(rowTime, rowList);
                        })
                .thenCompose(ignored -> timerState.asyncValue())
                .thenCompose(
                        cleanUpTime ->
                                cleanUpTime == null
                                        ? registerCleanUpTimer(ctx, rowTime, isLeft)
                                        : StateFutureUtils.completedVoidFuture());
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RowData> out) {
        final long operatorTime = getOperatorTime(ctx);
        // In the future, we should separate the left and right watermarks. Otherwise, the
        // registered timer of the faster stream will be delayed, even if the watermarks have
        // already been emitted by the source.
        leftTimerState
                .asyncValue()
                .thenCompose(
                        leftCleanUpTime -> {
                            if (leftCleanUpTime != null && timestamp == leftCleanUpTime) {
                                return removeExpiredRows(
                                        out,
                                        updateRightExpirationTime(
                                                calExpirationTime(operatorTime, rightRelativeSize)),
                                        rightCache,
                                        leftTimerState,
                                        ctx,
                                        false);
                            }
                            return StateFutureUtils.completedVoidFuture();
                        })
                .thenCompose(ignored -> rightTimerState.asyncValue())
                .thenCompose(
                        rightCleanUpTime -> {
                            if (rightCleanUpTime != null && timestamp == rightCleanUpTime) {
                                return removeExpiredRows(
                                        out,
                                        updateLeftExpirationTime(
                                                calExpirationTime(operatorTime, leftRelativeSize)),
                                        leftCache,
                                        rightTimerState,
                                        ctx,
                                        true);
                            }
                            return StateFutureUtils.completedVoidFuture();
                        });
    }

    private long updateLeftExpirationTime(long expirationTime) {
        leftExpirationTime = Math.max(leftExpirationTime, expirationTime);
        return leftExpirationTime;
    }

    private long updateRightExpirationTime(long expirationTime) {
        rightExpirationTime = Math.max(rightExpirationTime, expirationTime);
        return rightExpirationTime;
    }

    /**
     * Calculate the expiration time with the given operator time and relative window size.
     *
     * @param operatorTime the operator time
     * @param relativeSize the relative window size
     * @return the expiration time for cached rows
     */
    private long calExpirationTime(long operatorTime, long relativeSize) {
        if (operatorTime < Long.MAX_VALUE) {
            return operatorTime - relativeSize - allowedLateness - 1;
        } else {
            // When operatorTime = Long.MaxValue, it means the stream has reached the end.
            return Long.MAX_VALUE;
        }
    }

    /**
     * Register a timer for cleaning up rows in a specified time.
     *
     * @param ctx the context to register timer
     * @param rowTime time for the input row
     * @param leftRow whether this row comes from the left stream
     */
    private StateFuture<Void> registerCleanUpTimer(Context ctx, long rowTime, boolean leftRow) {
        final long cleanUpTime =
                saturatedSum(
                        rowTime,
                        leftRow ? leftRelativeSize : rightRelativeSize,
                        minCleanUpInterval,
                        allowedLateness,
                        1);
        registerTimer(ctx, cleanUpTime);
        return (leftRow ? rightTimerState : leftTimerState).asyncUpdate(cleanUpTime);
    }

    /**
     * Returns the sum clamped to the range of {@code long}. A cleanup time that wrapped around
     * would fire at the wrong time, e.g. at once for a very large minimum cleanup interval.
     */
    private static long saturatedSum(long... values) {
        try {
            long sum = 0;
            for (long value : values) {
                sum = Math.addExact(sum, value);
            }
            return sum;
        } catch (ArithmeticException e) {
            BigInteger sum = BigInteger.ZERO;
            for (long value : values) {
                sum = sum.add(BigInteger.valueOf(value));
            }
            return sum.max(BigInteger.valueOf(Long.MIN_VALUE))
                    .min(BigInteger.valueOf(Long.MAX_VALUE))
                    .longValue();
        }
    }

    /**
     * Remove the expired rows. Register a new timer if the cache still holds valid rows after the
     * cleaning up.
     *
     * @param collector the collector to emit results
     * @param expirationTime the expiration time for this cache
     * @param rowCache the row cache
     * @param timerState timer state for the opposite stream
     * @param ctx the context to register the cleanup timer
     * @param removeLeft whether to remove the left rows
     */
    private StateFuture<Void> removeExpiredRows(
            Collector<RowData> collector,
            long expirationTime,
            MapState<Long, List<Tuple2<RowData, Boolean>>> rowCache,
            ValueState<Long> timerState,
            OnTimerContext ctx,
            boolean removeLeft) {
        final Map<Long, List<Tuple2<RowData, Boolean>>> expiredEntries = new HashMap<>();
        // Whether rows are left is tracked apart from their time, which may be zero or negative.
        final boolean[] hasValidRows = {false};
        final long[] earliestTimestamp = {0L};
        final ThrowingConsumer<Long, Exception> classify =
                rowTime -> {
                    if (rowTime <= expirationTime) {
                        expiredEntries.put(rowTime, null);
                    } else if (!hasValidRows[0] || rowTime < earliestTimestamp[0]) {
                        // We find the earliest timestamp that is still valid.
                        earliestTimestamp[0] = rowTime;
                        hasValidRows[0] = true;
                    }
                };

        // We remove all expired keys and do not leave the loop early.
        // Hence, we do a full pass over the state. Only the rows of an outer input are read, to
        // pad the unmatched ones.
        final StateFuture<Void> scanned;
        if (removeLeft ? joinType.isLeftOuter() : joinType.isRightOuter()) {
            final ThrowingConsumer<Map.Entry<Long, List<Tuple2<RowData, Boolean>>>, Exception>
                    padAndClassify =
                            entry -> {
                                if (entry.getKey() <= expirationTime) {
                                    padUnmatched(entry.getValue(), removeLeft, collector);
                                }
                                classify.accept(entry.getKey());
                            };
            scanned =
                    rowCache.asyncEntries().thenCompose(entries -> entries.onNext(padAndClassify));
        } else {
            scanned = rowCache.asyncKeys().thenCompose(keys -> keys.onNext(classify));
        }

        return scanned.thenCompose(
                ignored -> {
                    if (hasValidRows[0]) {
                        // There are rows left in the cache. Register a timer to expire them later.
                        return putAll(rowCache, expiredEntries)
                                .thenCompose(
                                        ignored2 ->
                                                registerCleanUpTimer(
                                                        ctx, earliestTimestamp[0], removeLeft));
                    }
                    // No rows left in the cache. Clear the timer state and the cache.
                    return timerState
                            .asyncClear()
                            .thenCompose(
                                    ignored2 ->
                                            expiredEntries.size() > MAX_BULK_DELETE_ON_FULL_EXPIRY
                                                    ? rowCache.asyncClear()
                                                    : putAll(rowCache, expiredEntries));
                });
    }

    /** Emits a null padding result for each of the rows that has not been joined. */
    private void padUnmatched(
            List<Tuple2<RowData, Boolean>> rows, boolean padLeft, Collector<RowData> out) {
        for (Tuple2<RowData, Boolean> tuple : rows) {
            if (!tuple.f1) {
                out.collect(
                        padLeft ? paddingUtil.padLeft(tuple.f0) : paddingUtil.padRight(tuple.f0));
            }
        }
    }

    /** Puts the entries in one request, if there are any. A null value removes an entry. */
    private static StateFuture<Void> putAll(
            MapState<Long, List<Tuple2<RowData, Boolean>>> cache,
            Map<Long, List<Tuple2<RowData, Boolean>>> entries) {
        return entries.isEmpty()
                ? StateFutureUtils.completedVoidFuture()
                : cache.asyncPutAll(entries);
    }

    /**
     * Return the current time of both streams. It is taken once when processing starts and passed
     * on, since processing time moves on while the callbacks of other records run.
     *
     * @param ctx the context to acquire watermarks
     * @return the operator time
     */
    abstract long getOperatorTime(Context ctx);

    /**
     * Return the time for the target row from the left stream.
     *
     * @param ctx the runtime context
     * @param row the target row
     * @param operatorTime the operator time from {@link #getOperatorTime}
     * @return time for the target row
     */
    abstract long getTimeForLeftStream(Context ctx, RowData row, long operatorTime);

    /**
     * Return the time for the target row from the right stream.
     *
     * @param ctx the runtime context
     * @param row the target row
     * @param operatorTime the operator time from {@link #getOperatorTime}
     * @return time for the target row
     */
    abstract long getTimeForRightStream(Context ctx, RowData row, long operatorTime);

    /**
     * Register a proctime or rowtime timer.
     *
     * @param ctx the context to register the timer
     * @param cleanupTime timestamp for the timer
     */
    abstract void registerTimer(Context ctx, long cleanupTime);
}
