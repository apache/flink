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
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.OuterJoinPaddingUtil;
import org.apache.flink.table.runtime.operators.join.interval.EmitAwareCollector;
import org.apache.flink.table.runtime.operators.join.interval.IntervalJoinFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.function.ThrowingConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * The async state version of {@code TimeIntervalJoin}, a function to execute a time interval
 * (time-bounded) stream join. It keeps the state and the logic of the synchronous version with two
 * fixes: rows at time zero or earlier are joined and kept like rows after time zero, and cleanup
 * times saturate instead of overflowing. As in the synchronous version, the event-time operator
 * time is never below zero, so a row whose window ends at or before time zero is not cached.
 *
 * <p>State iterators cannot remove entries, so the changes to a scanned cache are written after the
 * scan. With early fire, the fired bits of a scanned cache are read in one scan before it, so that
 * the scan of the cache does not wait for a request per bucket. Records of several keys are in
 * flight at a time, so what the callbacks of a record need, such as its join key and the operator
 * time, is taken when its processing starts. Unlike the synchronous version, this function does not
 * enable interruptible timers, which the async state operators do not support.
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

    // Delay after a row's time at which an unmatched outer row is speculatively padded and emitted.
    // A negative value disables early firing, in which case the operator behaves as a plain
    // interval join.
    private final long earlyFireDelay;
    // True only for an outer join with a non-negative window span and a non-negative delay.
    private final boolean earlyFireEnabled;
    // True when the early-fire timer fires on the wall clock while cleanup stays on event time
    // (an event-time interval join early-firing in processing-time mode). The two timer kinds then
    // live in different domains and onTimer discriminates them by timeDomain().
    private final boolean earlyFireCrossDomain;

    // cache to store rows from the left stream
    private transient MapState<Long, List<Tuple2<RowData, Boolean>>> leftCache;
    // cache to store rows from the right stream
    private transient MapState<Long, List<Tuple2<RowData, Boolean>>> rightCache;

    // For each cached outer row, whether its speculative early-fire pad has already been emitted,
    // positionally aligned with the row-time bucket in leftCache / rightCache, as in the
    // synchronous version. Only registered with early fire.
    private transient MapState<Long, List<Boolean>> leftFiredState;
    private transient MapState<Long, List<Boolean>> rightFiredState;

    // Cross-domain early fire only: maps a firing processing-time to the event-time bucket keys
    // whose unmatched outer rows are due to be speculatively padded at that wall-clock instant.
    private transient MapState<Long, List<Long>> leftEarlyFireSchedule;
    private transient MapState<Long, List<Long>> rightEarlyFireSchedule;

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
            IntervalJoinFunction joinFunc,
            long earlyFireDelay,
            boolean earlyFireCrossDomain) {
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
        this.earlyFireDelay = earlyFireDelay;
        this.earlyFireEnabled =
                earlyFireDelay >= 0
                        && joinType.isOuter()
                        && (leftRelativeSize + rightRelativeSize) >= 0;
        this.earlyFireCrossDomain = earlyFireCrossDomain;
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

        // Early-fire bookkeeping, aligned with the caches above, with the names and types of the
        // synchronous version.
        if (earlyFireEnabled) {
            ListTypeInfo<Boolean> firedListTypeInfo =
                    new ListTypeInfo<>(BasicTypeInfo.BOOLEAN_TYPE_INFO);
            leftFiredState =
                    getRuntimeContext()
                            .getMapState(
                                    new MapStateDescriptor<>(
                                            "IntervalJoinLeftFired",
                                            BasicTypeInfo.LONG_TYPE_INFO,
                                            firedListTypeInfo));
            rightFiredState =
                    getRuntimeContext()
                            .getMapState(
                                    new MapStateDescriptor<>(
                                            "IntervalJoinRightFired",
                                            BasicTypeInfo.LONG_TYPE_INFO,
                                            firedListTypeInfo));

            if (earlyFireCrossDomain) {
                ListTypeInfo<Long> bucketListTypeInfo =
                        new ListTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO);
                leftEarlyFireSchedule =
                        getRuntimeContext()
                                .getMapState(
                                        new MapStateDescriptor<>(
                                                "IntervalJoinLeftEarlyFireSchedule",
                                                BasicTypeInfo.LONG_TYPE_INFO,
                                                bucketListTypeInfo));
                rightEarlyFireSchedule =
                        getRuntimeContext()
                                .getMapState(
                                        new MapStateDescriptor<>(
                                                "IntervalJoinRightEarlyFireSchedule",
                                                BasicTypeInfo.LONG_TYPE_INFO,
                                                bucketListTypeInfo));
            }
        }

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
        final boolean thisOuter = isOuter(isLeft);
        final boolean otherOuter = isOuter(!isLeft);
        final long earlyFireTime =
                earlyFireEnabled && thisOuter
                        ? (earlyFireCrossDomain
                                        ? ctx.timerService().currentProcessingTime()
                                        : rowTime)
                                + earlyFireDelay
                        : 0L;
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
            // Fired bits are only kept for the rows of an outer input.
            final MapState<Long, List<Boolean>> otherFiredState =
                    earlyFireEnabled && otherOuter
                            ? (isLeft ? rightFiredState : leftFiredState)
                            : null;
            // A collector per record, as the callbacks of other records run in between.
            final EmitAwareCollector joinCollector = new EmitAwareCollector();
            joinCollector.setInnerCollector(out);
            final Map<Long, List<Boolean>> otherFired = new HashMap<>();
            final Map<Long, List<Tuple2<RowData, Boolean>>> updatedEntries = new HashMap<>();
            final Map<Long, List<Tuple2<RowData, Boolean>>> expiredEntries = new HashMap<>();
            final Map<Long, List<Boolean>> expiredFired = new HashMap<>();
            final ThrowingConsumer<Map.Entry<Long, List<Tuple2<RowData, Boolean>>>, Exception>
                    joinEntry =
                            entry -> {
                                final long otherTime = entry.getKey();
                                final List<Tuple2<RowData, Boolean>> otherRows = entry.getValue();
                                final boolean expired = otherTime <= expirationTime;
                                final List<Boolean> fired =
                                        otherFiredState == null
                                                ? null
                                                : firedBits(
                                                        otherFired.get(otherTime),
                                                        otherRows.size());
                                if (otherTime >= otherQualifiedLowerBound
                                        && otherTime <= otherQualifiedUpperBound) {
                                    boolean entryUpdated = false;
                                    for (int i = 0; i < otherRows.size(); i++) {
                                        final Tuple2<RowData, Boolean> tuple = otherRows.get(i);
                                        joinCollector.reset();
                                        final boolean retract =
                                                fired != null && !tuple.f1 && fired.get(i);
                                        if (retract) {
                                            // The speculative pad for this row was already
                                            // emitted as an insert; arm the collector so the match
                                            // becomes -U(pad)/+U(match).
                                            joinCollector.armRetraction(pad(tuple.f0, !isLeft));
                                        }
                                        if (isLeft) {
                                            joinFunction.join(
                                                    row, tuple.f0, joinKey, joinCollector);
                                        } else {
                                            joinFunction.join(
                                                    tuple.f0, row, joinKey, joinCollector);
                                        }
                                        if (retract && !joinCollector.isEmitted()) {
                                            joinCollector.disarm();
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
                                        padUnmatched(otherRows, fired, !isLeft, out);
                                    }
                                    expiredEntries.put(otherTime, null);
                                    if (otherFired.containsKey(otherTime)) {
                                        expiredFired.put(otherTime, null);
                                    }
                                }
                            };
            joined =
                    readAll(otherFiredState, otherFired)
                            .thenCompose(ignored -> otherCache.asyncEntries())
                            .thenCompose(entries -> entries.onNext(joinEntry))
                            .thenCompose(ignored -> putAll(otherCache, updatedEntries))
                            .thenCompose(ignored -> putAll(otherCache, expiredEntries))
                            .thenCompose(ignored -> putAll(otherFiredState, expiredFired));
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
                        return cacheRow(row, rowTime, earlyFireTime, emitted[0], ctx, isLeft);
                    }
                    if (!emitted[0] && thisOuter) {
                        // Emit a null padding result if the row is not cached and successfully
                        // joined.
                        collectPad(pad(row, isLeft), out);
                    }
                    return StateFutureUtils.completedVoidFuture();
                });
    }

    private StateFuture<Void> cacheRow(
            RowData row,
            long rowTime,
            long earlyFireTime,
            boolean emitted,
            Context ctx,
            boolean isLeft) {
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
                .thenCompose(
                        ignored -> {
                            if (!earlyFireEnabled || !isOuter(isLeft)) {
                                return StateFutureUtils.completedVoidFuture();
                            }
                            // The new tuple has not been speculatively padded yet, so its bit
                            // starts false. Schedule a speculative pad of the row after the delay
                            // if it is unmatched.
                            final StateFuture<Void> appended =
                                    appendFired(isLeft ? leftFiredState : rightFiredState, rowTime);
                            return emitted
                                    ? appended
                                    : appended.thenCompose(
                                            ignored2 ->
                                                    scheduleEarlyFire(
                                                            ctx, rowTime, earlyFireTime, isLeft));
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
        final StateFuture<Void> earlyFired;
        if (earlyFireEnabled && earlyFireCrossDomain) {
            // Cross-domain: early-fire timers are processing-time, cleanup timers are event-time.
            // timeDomain() is the authoritative discriminator (a processing-time value can
            // numerically equal an event-time cleanup value, so timestamp arithmetic is unsafe).
            if (ctx.timeDomain() == TimeDomain.PROCESSING_TIME) {
                // Cleanup is event-time; there is nothing else to do at a processing-time firing.
                fireScheduled(timestamp, true, out)
                        .thenCompose(ignored -> fireScheduled(timestamp, false, out));
                return;
            }
            // EVENT_TIME falls through to the cleanup below; no early fire in this domain.
            earlyFired = StateFutureUtils.completedVoidFuture();
        } else if (earlyFireEnabled) {
            // Natural pairing: the early-fire timer shares its domain with cleanup and fires at
            // rowTime + delay, so the bucket key is recovered as timestamp - delay. Early fire runs
            // before cleanup at a shared timestamp so a row that is both due to fire and due to
            // expire emits its speculative pad here; the cleanup's fired-bit gate then suppresses
            // a second pad. A cleanup-only timestamp finds no live unfired-unmatched row and is a
            // cheap no-op.
            final long rowTime = timestamp - earlyFireDelay;
            earlyFired =
                    earlyFire(rowTime, true, out)
                            .thenCompose(ignored -> earlyFire(rowTime, false, out));
        } else {
            earlyFired = StateFutureUtils.completedVoidFuture();
        }
        // In the future, we should separate the left and right watermarks. Otherwise, the
        // registered timer of the faster stream will be delayed, even if the watermarks have
        // already been emitted by the source.
        earlyFired
                .thenCompose(ignored -> leftTimerState.asyncValue())
                .thenCompose(
                        leftCleanUpTime -> {
                            if (leftCleanUpTime != null && timestamp == leftCleanUpTime) {
                                return removeExpiredRows(
                                        out,
                                        updateRightExpirationTime(
                                                calExpirationTime(operatorTime, rightRelativeSize)),
                                        rightCache,
                                        rightFiredState,
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
                                        leftFiredState,
                                        rightTimerState,
                                        ctx,
                                        true);
                            }
                            return StateFutureUtils.completedVoidFuture();
                        });
    }

    /**
     * Emit the speculative null-padding result for every cached outer row at the given row time
     * that is still unmatched and has not yet had its pad emitted, flipping its fired bit so
     * neither this path nor the later window-close pad emits it again.
     */
    private StateFuture<Void> earlyFire(long rowTime, boolean padLeft, Collector<RowData> out) {
        if (!isOuter(padLeft)) {
            return StateFutureUtils.completedVoidFuture();
        }
        final MapState<Long, List<Boolean>> firedState = padLeft ? leftFiredState : rightFiredState;
        return (padLeft ? leftCache : rightCache)
                .asyncGet(rowTime)
                .thenCompose(
                        rows -> {
                            if (rows == null) {
                                return StateFutureUtils.completedVoidFuture();
                            }
                            return firedState
                                    .asyncGet(rowTime)
                                    .thenCompose(
                                            storedFired -> {
                                                final List<Boolean> fired =
                                                        firedBits(storedFired, rows.size());
                                                boolean changed = false;
                                                for (int i = 0; i < rows.size(); i++) {
                                                    final Tuple2<RowData, Boolean> tuple =
                                                            rows.get(i);
                                                    if (!tuple.f1 && !fired.get(i)) {
                                                        collectPad(pad(tuple.f0, padLeft), out);
                                                        fired.set(i, true);
                                                        changed = true;
                                                    }
                                                }
                                                return changed
                                                        ? firedState.asyncPut(rowTime, fired)
                                                        : StateFutureUtils.completedVoidFuture();
                                            });
                        });
    }

    /**
     * Register the early-fire timer for an unmatched outer row. For the natural pairing the timer
     * shares the cleanup domain and fires at {@code bucketKey + earlyFireDelay}, recoverable later
     * as {@code timestamp - earlyFireDelay}. For the cross-domain case the timer is a
     * processing-time timer at {@code firingTime}, the processing time when {@link #processElement}
     * started plus {@code earlyFireDelay}, and the event-time bucket key is recorded once in the
     * schedule under that firing time so it can be recovered when the processing-time timer fires.
     */
    private StateFuture<Void> scheduleEarlyFire(
            Context ctx, long bucketKey, long firingTime, boolean isLeft) {
        if (!earlyFireCrossDomain) {
            registerTimer(ctx, firingTime);
            return StateFutureUtils.completedVoidFuture();
        }
        final MapState<Long, List<Long>> schedule =
                isLeft ? leftEarlyFireSchedule : rightEarlyFireSchedule;
        return schedule.asyncGet(firingTime)
                .thenCompose(
                        buckets -> {
                            if (buckets != null && buckets.contains(bucketKey)) {
                                return StateFutureUtils.completedVoidFuture();
                            }
                            final List<Long> bucketList =
                                    buckets == null ? new ArrayList<>(1) : buckets;
                            bucketList.add(bucketKey);
                            ctx.timerService().registerProcessingTimeTimer(firingTime);
                            return schedule.asyncPut(firingTime, bucketList);
                        });
    }

    /**
     * Recover the event-time buckets scheduled to fire at the given processing-time and early-fire
     * each one, then drop the schedule entry. A missing entry is a no-op; a bucket already cleaned
     * by event-time expiry resolves to an empty cache lookup inside {@link #earlyFire} and is
     * likewise a no-op.
     */
    private StateFuture<Void> fireScheduled(
            long firingTime, boolean padLeft, Collector<RowData> out) {
        if (!isOuter(padLeft)) {
            return StateFutureUtils.completedVoidFuture();
        }
        final MapState<Long, List<Long>> schedule =
                padLeft ? leftEarlyFireSchedule : rightEarlyFireSchedule;
        return schedule.asyncGet(firingTime)
                .thenCompose(
                        buckets -> {
                            if (buckets == null) {
                                return StateFutureUtils.completedVoidFuture();
                            }
                            StateFuture<Void> fired = StateFutureUtils.completedVoidFuture();
                            // Deduplicate in case a restored synchronous schedule recorded a bucket
                            // more than once.
                            for (long bucketKey : new LinkedHashSet<>(buckets)) {
                                fired =
                                        fired.thenCompose(
                                                ignored -> earlyFire(bucketKey, padLeft, out));
                            }
                            return fired.thenCompose(ignored -> schedule.asyncRemove(firingTime));
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
     * @param firedState the fired bits of the row cache, null without early fire
     * @param timerState timer state for the opposite stream
     * @param ctx the context to register the cleanup timer
     * @param removeLeft whether to remove the left rows
     */
    private StateFuture<Void> removeExpiredRows(
            Collector<RowData> collector,
            long expirationTime,
            MapState<Long, List<Tuple2<RowData, Boolean>>> rowCache,
            @Nullable MapState<Long, List<Boolean>> firedState,
            ValueState<Long> timerState,
            OnTimerContext ctx,
            boolean removeLeft) {
        final boolean removeOuter = isOuter(removeLeft);
        // Fired bits are only kept for the rows of an outer input.
        final MapState<Long, List<Boolean>> outerFiredState = removeOuter ? firedState : null;
        final Map<Long, List<Boolean>> fired = new HashMap<>();
        final Map<Long, List<Tuple2<RowData, Boolean>>> expiredEntries = new HashMap<>();
        final Map<Long, List<Boolean>> expiredFired = new HashMap<>();
        // Whether rows are left is tracked apart from their time, which may be zero or negative.
        final boolean[] hasValidRows = {false};
        final long[] earliestTimestamp = {0L};
        final ThrowingConsumer<Long, Exception> classify =
                rowTime -> {
                    if (rowTime <= expirationTime) {
                        expiredEntries.put(rowTime, null);
                        if (fired.containsKey(rowTime)) {
                            expiredFired.put(rowTime, null);
                        }
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
        if (removeOuter) {
            final ThrowingConsumer<Map.Entry<Long, List<Tuple2<RowData, Boolean>>>, Exception>
                    padAndClassify =
                            entry -> {
                                final long rowTime = entry.getKey();
                                if (rowTime <= expirationTime) {
                                    final List<Tuple2<RowData, Boolean>> rows = entry.getValue();
                                    padUnmatched(
                                            rows,
                                            outerFiredState == null
                                                    ? null
                                                    : firedBits(fired.get(rowTime), rows.size()),
                                            removeLeft,
                                            collector);
                                }
                                classify.accept(rowTime);
                            };
            scanned =
                    readAll(outerFiredState, fired)
                            .thenCompose(ignored -> rowCache.asyncEntries())
                            .thenCompose(entries -> entries.onNext(padAndClassify));
        } else {
            scanned = rowCache.asyncKeys().thenCompose(keys -> keys.onNext(classify));
        }

        return scanned.thenCompose(
                ignored -> {
                    if (hasValidRows[0]) {
                        // There are rows left in the cache. Register a timer to expire them later.
                        return putAll(rowCache, expiredEntries)
                                .thenCompose(ignored2 -> putAll(outerFiredState, expiredFired))
                                .thenCompose(
                                        ignored2 ->
                                                registerCleanUpTimer(
                                                        ctx, earliestTimestamp[0], removeLeft));
                    }
                    // No rows left in the cache. Clear the timer state, the cache and its fired
                    // bits.
                    fired.replaceAll((rowTime, bits) -> null);
                    return timerState
                            .asyncClear()
                            .thenCompose(ignored2 -> removeAll(rowCache, expiredEntries))
                            .thenCompose(ignored2 -> removeAll(outerFiredState, fired));
                });
    }

    /**
     * Emits a null padding result for each of the rows that has not been joined, unless its
     * speculative pad has already been emitted.
     */
    private void padUnmatched(
            List<Tuple2<RowData, Boolean>> rows,
            @Nullable List<Boolean> fired,
            boolean padLeft,
            Collector<RowData> out) {
        for (int i = 0; i < rows.size(); i++) {
            final Tuple2<RowData, Boolean> tuple = rows.get(i);
            if (!tuple.f1 && (fired == null || !fired.get(i))) {
                collectPad(pad(tuple.f0, padLeft), out);
            }
        }
    }

    private boolean isOuter(boolean left) {
        return left ? joinType.isLeftOuter() : joinType.isRightOuter();
    }

    private RowData pad(RowData row, boolean padLeft) {
        return padLeft ? paddingUtil.padLeft(row) : paddingUtil.padRight(row);
    }

    /**
     * Emit a padded outer-join row as an insert, overriding any leaked row kind on the reused row.
     */
    private static void collectPad(RowData paddedRow, Collector<RowData> out) {
        paddedRow.setRowKind(RowKind.INSERT);
        out.collect(paddedRow);
    }

    /**
     * Return the fired-bit list aligned with a cache bucket of the given size. When the stored list
     * is absent or its length no longer matches the bucket (e.g. after a restore), a fresh
     * all-false list is returned so no row is ever treated as already fired.
     */
    private static List<Boolean> firedBits(@Nullable List<Boolean> storedFired, int size) {
        return storedFired != null && storedFired.size() == size
                ? storedFired
                : new ArrayList<>(Collections.nCopies(size, Boolean.FALSE));
    }

    private static StateFuture<Void> appendFired(
            MapState<Long, List<Boolean>> firedState, long rowTime) {
        return firedState
                .asyncGet(rowTime)
                .thenCompose(
                        fired -> {
                            final List<Boolean> firedList =
                                    fired == null ? new ArrayList<>(1) : fired;
                            firedList.add(Boolean.FALSE);
                            return firedState.asyncPut(rowTime, firedList);
                        });
    }

    /** Reads all entries of the state, if there is one, into the map. */
    private static <V> StateFuture<Void> readAll(
            @Nullable MapState<Long, V> state, Map<Long, V> entries) {
        if (state == null) {
            return StateFutureUtils.completedVoidFuture();
        }
        return state.asyncEntries()
                .thenCompose(
                        iterator ->
                                iterator.onNext(
                                        entry -> {
                                            entries.put(entry.getKey(), entry.getValue());
                                        }));
    }

    /**
     * Puts the entries in one request, if there are any. A null value removes an entry. The state
     * may only be null if there are no entries.
     */
    private static <V> StateFuture<Void> putAll(
            @Nullable MapState<Long, V> state, Map<Long, V> entries) {
        return entries.isEmpty()
                ? StateFutureUtils.completedVoidFuture()
                : state.asyncPutAll(entries);
    }

    /**
     * Removes all entries of a fully expired state, given as null values. Up to {@link
     * #MAX_BULK_DELETE_ON_FULL_EXPIRY} are deleted in one request, more by clearing the state.
     */
    private static <V> StateFuture<Void> removeAll(
            @Nullable MapState<Long, V> state, Map<Long, V> nullEntries) {
        return nullEntries.size() > MAX_BULK_DELETE_ON_FULL_EXPIRY
                ? state.asyncClear()
                : putAll(state, nullEntries);
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
