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

package org.apache.flink.table.runtime.operators.join.interval.asyncprocess;

import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.v2.MapState;
import org.apache.flink.api.common.state.v2.MapStateDescriptor;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.state.v2.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.runtime.asyncprocessing.declare.ContextVariable;
import org.apache.flink.runtime.asyncprocessing.declare.DeclarationContext;
import org.apache.flink.runtime.asyncprocessing.declare.DeclarationException;
import org.apache.flink.runtime.asyncprocessing.functions.DeclaringAsyncKeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.NullAwareGetters;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.OuterJoinPaddingUtil;
import org.apache.flink.table.runtime.operators.join.interval.EmitAwareCollector;
import org.apache.flink.table.runtime.operators.join.interval.IntervalJoinFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.Collector;
import org.apache.flink.util.function.ThrowingConsumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** Asynchronous version of TimeIntervalJoin. */
public abstract class AsyncTimeIntervalJoin
        extends DeclaringAsyncKeyedCoProcessFunction<RowData, RowData, RowData, RowData> {
    protected final FlinkJoinType joinType;
    protected final long leftRelativeSize;
    protected final long rightRelativeSize;

    // Minimum interval by which state is cleaned up
    protected final long minCleanUpInterval;
    protected final long allowedLateness;
    protected final InternalTypeInfo<RowData> leftType;
    protected final InternalTypeInfo<RowData> rightType;
    protected final IntervalJoinFunction joinFunction;
    protected transient OuterJoinPaddingUtil paddingUtil;

    protected transient EmitAwareCollector joinCollector;

    // Points in time until which the respective cache has been cleaned.
    protected transient ContextVariable<Long> leftExpirationTime;
    protected transient ContextVariable<Long> rightExpirationTime;

    // Current time on the respective input stream.
    protected transient ContextVariable<Long> leftOperatorTime;
    protected transient ContextVariable<Long> rightOperatorTime;

    // cache to store rows form the left stream
    private transient MapState<Long, List<Tuple2<RowData, Boolean>>> leftCache;
    // cache to store rows from the right stream
    private transient MapState<Long, List<Tuple2<RowData, Boolean>>> rightCache;

    // state to record the timer on the left stream. 0 means no timer set
    private transient ValueState<Long> leftTimerState;
    // state to record the timer on the right stream. 0 means no timer set
    private transient ValueState<Long> rightTimerState;

    protected AsyncTimeIntervalJoin(
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
        joinFunction.setRuntimeContext(getRuntimeContext());
        joinFunction.open(DefaultOpenContext.INSTANCE);
        joinCollector = new EmitAwareCollector();

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
    public void declareVariables(DeclarationContext context) {
        leftExpirationTime =
                context.declareVariable(
                        LongSerializer.INSTANCE,
                        "_AsyncTimeIntervalJoin$leftExpirationTime",
                        () -> 0L);
        rightExpirationTime =
                context.declareVariable(
                        LongSerializer.INSTANCE,
                        "_AsyncTimeIntervalJoin$rightExpirationTime",
                        () -> 0L);
        leftOperatorTime =
                context.declareVariable(
                        LongSerializer.INSTANCE,
                        "_AsyncTimeIntervalJoin$leftOperatorTime",
                        () -> 0L);
        rightOperatorTime =
                context.declareVariable(
                        LongSerializer.INSTANCE,
                        "_AsyncTimeIntervalJoin$rightOperatorTime",
                        () -> 0L);
    }

    @Override
    public ThrowingConsumer<RowData, Exception> declareProcess1(
            DeclarationContext context,
            KeyedCoProcessFunction<RowData, RowData, RowData, RowData>.Context ctx,
            Collector<RowData> out)
            throws DeclarationException {
        ContextVariable<RowData> leftRowVariable = context.declareVariable(null);
        ContextVariable<NullAwareGetters> joinKeyVariable = context.declareVariable(null);
        return context.<RowData>declareChain()
                .thenCompose(
                        leftRow -> {
                            initDeclaredVariables();
                            leftRowVariable.set(leftRow);
                            joinKeyVariable.set((NullAwareGetters) ctx.getCurrentKey());
                            joinFunction.setJoinKey(
                                    new NullAwareGettersWithDeclaredVariables(joinKeyVariable));
                            joinCollector.setInnerCollector(out);
                            updateOperatorTime(ctx);
                            long timeForLeftRow = getTimeForLeftStream(ctx, leftRow);
                            long rightQualifiedLowerBound = timeForLeftRow - rightRelativeSize;
                            long rightQualifiedUpperBound = timeForLeftRow + leftRelativeSize;
                            StateFuture<Boolean> emittedFuture =
                                    StateFutureUtils.completedFuture(false);
                            // Check if we need to join the current row against cached rows of the
                            // right input.
                            // The condition here should be rightMinimumTime <
                            // rightQualifiedUpperBound.
                            // We use rightExpirationTime as an approximation of the
                            // rightMinimumTime here,
                            // since rightExpirationTime <= rightMinimumTime is always true.
                            if (rightExpirationTime.get() < rightQualifiedUpperBound) {
                                // Upper bound of current join window has not passed the cache
                                // expiration time yet.
                                // There might be qualifying rows in the cache that the current row
                                // needs to be joined
                                // with.
                                rightExpirationTime.set(
                                        calExpirationTime(
                                                leftOperatorTime.get(), rightRelativeSize));
                                // Join the leftRow with rows from the right cache.
                                emittedFuture =
                                        iterateRightCache(
                                                leftRow,
                                                rightQualifiedLowerBound,
                                                rightQualifiedUpperBound);
                            }
                            return emittedFuture;
                        })
                .withName("iterRight")
                .thenAccept(
                        emitted -> {
                            long timeForLeftRow = getTimeForLeftStream(ctx, leftRowVariable.get());
                            long rightQualifiedUpperBound = timeForLeftRow + leftRelativeSize;
                            cacheOrEmitLeft(
                                    leftRowVariable.get(),
                                    ctx,
                                    rightQualifiedUpperBound,
                                    timeForLeftRow,
                                    emitted);
                        })
                .withName("cacheOrEmitLeft")
                .finish();
    }

    @Override
    public ThrowingConsumer<RowData, Exception> declareProcess2(
            DeclarationContext context,
            KeyedCoProcessFunction<RowData, RowData, RowData, RowData>.Context ctx,
            Collector<RowData> out)
            throws DeclarationException {
        ContextVariable<RowData> rightRowVariable = context.declareVariable(null);
        ContextVariable<NullAwareGetters> joinKeyVariable = context.declareVariable(null);

        return context.<RowData>declareChain()
                .thenCompose(
                        rightRow -> {
                            initDeclaredVariables();
                            rightRowVariable.set(rightRow);
                            joinKeyVariable.set((NullAwareGetters) ctx.getCurrentKey());
                            joinFunction.setJoinKey(
                                    new NullAwareGettersWithDeclaredVariables(joinKeyVariable));
                            joinCollector.setInnerCollector(out);
                            updateOperatorTime(ctx);
                            long timeForRightRow = getTimeForRightStream(ctx, rightRow);
                            long leftQualifiedLowerBound = timeForRightRow - leftRelativeSize;
                            long leftQualifiedUpperBound = timeForRightRow + rightRelativeSize;
                            StateFuture<Boolean> emittedFuture =
                                    StateFutureUtils.completedFuture(false);

                            // Check if we need to join the current row against cached rows of the
                            // left input.
                            // The condition here should be leftMinimumTime <
                            // leftQualifiedUpperBound.
                            // We use leftExpirationTime as an approximation of the leftMinimumTime
                            // here,
                            // since leftExpirationTime <= leftMinimumTime is always true.
                            if (leftExpirationTime.get() < leftQualifiedUpperBound) {
                                leftExpirationTime.set(
                                        calExpirationTime(
                                                rightOperatorTime.get(), leftRelativeSize));
                                // Join the rightRow with rows from the left cache.
                                emittedFuture =
                                        iterateLeftCache(
                                                rightRow,
                                                leftQualifiedLowerBound,
                                                leftQualifiedUpperBound);
                            }
                            return emittedFuture;
                        })
                .withName("iterLeft")
                .thenAccept(
                        emitted -> {
                            long timeForRightRow =
                                    getTimeForRightStream(ctx, rightRowVariable.get());
                            long leftQualifiedUpperBound = timeForRightRow + rightRelativeSize;
                            cacheOrEmitRight(
                                    rightRowVariable.get(),
                                    ctx,
                                    leftQualifiedUpperBound,
                                    timeForRightRow,
                                    emitted);
                        })
                .withName("cacheOrEmitRight")
                .finish();
    }

    @Override
    public ThrowingConsumer<Long, Exception> declareOnTimer(
            DeclarationContext context, OnTimerContext ctx, Collector<RowData> out)
            throws DeclarationException {
        ContextVariable<NullAwareGetters> joinKeyVariable = context.declareVariable(null);
        return context.<Long>declareChain()
                .thenAccept(
                        timestamp -> {
                            initDeclaredVariables();
                            joinKeyVariable.set((NullAwareGetters) ctx.getCurrentKey());
                            joinFunction.setJoinKey(
                                    new NullAwareGettersWithDeclaredVariables(joinKeyVariable));
                            joinCollector.setInnerCollector(out);
                            updateOperatorTime(ctx);
                            // In the future, we should separate the left and right watermarks.
                            // Otherwise, the
                            // registered timer of the faster stream will be delayed, even if the
                            // watermarks have
                            // already been emitted by the source.
                            leftTimerState
                                    .asyncValue()
                                    .thenAccept(
                                            leftCleanUpTime -> {
                                                if (leftCleanUpTime != null
                                                        && timestamp == leftCleanUpTime) {
                                                    rightExpirationTime.set(
                                                            calExpirationTime(
                                                                    leftOperatorTime.get(),
                                                                    rightRelativeSize));
                                                    removeExpiredRows(
                                                            rightExpirationTime.get(),
                                                            rightCache,
                                                            leftTimerState,
                                                            ctx,
                                                            false);
                                                }
                                            });

                            rightTimerState
                                    .asyncValue()
                                    .thenAccept(
                                            rightCleanUpTime -> {
                                                if (rightCleanUpTime != null
                                                        && timestamp == rightCleanUpTime) {
                                                    leftExpirationTime.set(
                                                            calExpirationTime(
                                                                    rightOperatorTime.get(),
                                                                    leftRelativeSize));
                                                    removeExpiredRows(
                                                            leftExpirationTime.get(),
                                                            leftCache,
                                                            rightTimerState,
                                                            ctx,
                                                            true);
                                                }
                                            });
                        })
                .withName("onTimer")
                .finish();
    }

    private void initDeclaredVariables() {
        if (leftExpirationTime.get() == null) {
            leftExpirationTime.set(0L);
        }
        if (rightExpirationTime.get() == null) {
            rightExpirationTime.set(0L);
        }
        if (leftOperatorTime.get() == null) {
            leftOperatorTime.set(0L);
        }
        if (rightOperatorTime.get() == null) {
            rightOperatorTime.set(0L);
        }
    }

    private void cacheOrEmitLeft(
            RowData leftRow,
            Context ctx,
            long rightQualifiedUpperBound,
            long timeForLeftRow,
            boolean emitted)
            throws Exception {
        if (rightOperatorTime.get() < rightQualifiedUpperBound) {
            // Operator time of right stream has not exceeded the upper window bound of the current
            // row. Put it into the left cache, since later coming records from the right stream are
            // expected to be joined with it.
            updateLeftCache(leftRow, timeForLeftRow, emitted);

            // Register a timer on the RIGHT stream to remove rows.

            registerCleanUpTimer(ctx, timeForLeftRow, true, false);
        } else if (!emitted && joinType.isLeftOuter()) {
            // Emit a null padding result if the left row is not cached and successfully joined.
            joinCollector.collect(paddingUtil.padLeft(leftRow));
        }
    }

    private void cacheOrEmitRight(
            RowData rightRow,
            Context ctx,
            long leftQualifiedUpperBound,
            long timeForRightRow,
            boolean emitted)
            throws Exception {
        if (leftOperatorTime.get() < leftQualifiedUpperBound) {
            // Operator time of left stream has not exceeded the upper window bound of the current
            // row. Put it into the right cache, since later coming records from the left stream are
            // expected to be joined with it.
            updateRightCache(rightRow, timeForRightRow, emitted);

            // Register a timer on the LEFT stream to remove rows.
            registerCleanUpTimer(ctx, timeForRightRow, false, false);
        } else if (!emitted && joinType.isRightOuter()) {
            // Emit a null padding result if the right row is not cached and successfully joined.
            joinCollector.collect(paddingUtil.padRight(rightRow));
        }
    }

    protected boolean tryMatchLeftRowsByTime(
            RowData leftRow,
            Map.Entry<Long, List<Tuple2<RowData, Boolean>>> rightEntry,
            long rightQualifiedLowerBound,
            long rightQualifiedUpperBound,
            Set<Long> toRemove)
            throws Exception {
        boolean emitted = false;
        Long rightTime = rightEntry.getKey();
        if (rightTime >= rightQualifiedLowerBound && rightTime <= rightQualifiedUpperBound) {
            List<Tuple2<RowData, Boolean>> rightRows = rightEntry.getValue();
            boolean entryUpdated = false;
            for (Tuple2<RowData, Boolean> tuple : rightRows) {
                joinCollector.reset();
                joinFunction.join(leftRow, tuple.f0, joinCollector);
                emitted = emitted || joinCollector.isEmitted();
                if (joinType.isRightOuter()) {
                    if (!tuple.f1 && joinCollector.isEmitted()) {
                        // Mark the right row as being successfully joined and emitted.
                        tuple.f1 = true;
                        entryUpdated = true;
                    }
                }
            }
            if (entryUpdated) {
                // Write back the edited entry (mark emitted) for the right cache.
                rightEntry.setValue(rightRows);
            }
        }
        // Clean up the expired right cache row, clean the cache while join
        if (rightTime <= rightExpirationTime.get()) {
            if (joinType.isRightOuter()) {
                List<Tuple2<RowData, Boolean>> rightRows = rightEntry.getValue();
                rightRows.forEach(
                        (Tuple2<RowData, Boolean> tuple) -> {
                            if (!tuple.f1) {
                                // Emit a null padding result if the right row has never
                                // been successfully joined.
                                joinCollector.collect(paddingUtil.padRight(tuple.f0));
                            }
                        });
            }
            toRemove.add(rightTime);
        } // We could do the short-cutting optimization here once we get a state with
        // ordered keys.
        return emitted;
    }

    protected boolean tryMatchRightRowsByTime(
            RowData rightRow,
            Map.Entry<Long, List<Tuple2<RowData, Boolean>>> leftEntry,
            long leftQualifiedLowerBound,
            long leftQualifiedUpperBound,
            Set<Long> toRemove)
            throws Exception {
        boolean emitted = false;
        Long leftTime = leftEntry.getKey();
        if (leftTime >= leftQualifiedLowerBound && leftTime <= leftQualifiedUpperBound) {
            List<Tuple2<RowData, Boolean>> leftRows = leftEntry.getValue();
            boolean entryUpdated = false;
            for (Tuple2<RowData, Boolean> tuple : leftRows) {
                joinCollector.reset();
                joinFunction.join(tuple.f0, rightRow, joinCollector);
                emitted = emitted || joinCollector.isEmitted();
                if (joinType.isLeftOuter()) {
                    if (!tuple.f1 && joinCollector.isEmitted()) {
                        // Mark the left row as being successfully joined and emitted.
                        tuple.f1 = true;
                        entryUpdated = true;
                    }
                }
            }
            if (entryUpdated) {
                // Write back the edited entry (mark emitted) for the right cache.
                leftEntry.setValue(leftRows);
            }
        }

        if (leftTime <= leftExpirationTime.get()) {
            if (joinType.isLeftOuter()) {
                List<Tuple2<RowData, Boolean>> leftRows = leftEntry.getValue();
                leftRows.forEach(
                        (Tuple2<RowData, Boolean> tuple) -> {
                            if (!tuple.f1) {
                                // Emit a null padding result if the left row has never been
                                // successfully joined.
                                joinCollector.collect(paddingUtil.padLeft(tuple.f0));
                            }
                        });
            }
            // eager remove
            toRemove.add(leftTime);
        } // We could do the short-cutting optimization here once we get a state with
        // ordered keys.
        return emitted;
    }

    protected void collectOuterOnTimer(
            Map.Entry<Long, List<Tuple2<RowData, Boolean>>> entry, boolean removeLeft) {

        if (removeLeft && joinType.isLeftOuter()) {
            List<Tuple2<RowData, Boolean>> rows = entry.getValue();
            rows.forEach(
                    (Tuple2<RowData, Boolean> tuple) -> {
                        if (!tuple.f1) {
                            // Emit a null padding result if the row has never been
                            // successfully joined.
                            joinCollector.collect(paddingUtil.padLeft(tuple.f0));
                        }
                    });
        } else if (!removeLeft && joinType.isRightOuter()) {
            List<Tuple2<RowData, Boolean>> rows = entry.getValue();
            rows.forEach(
                    (Tuple2<RowData, Boolean> tuple) -> {
                        if (!tuple.f1) {
                            // Emit a null padding result if the row has never been
                            // successfully joined.
                            joinCollector.collect(paddingUtil.padRight(tuple.f0));
                        }
                    });
        }
    }

    /**
     * Iterate right cache to join the left input row. And remove the expired rows in right cache.
     *
     * @param leftRow the left input row
     * @param rightQualifiedLowerBound the lower bound of qualified right rows
     * @param rightQualifiedUpperBound the upper bound of qualified right rows
     * @return {@code StateFuture<Boolean>} true if the left input row is emitted, false otherwise
     */
    protected StateFuture<Boolean> iterateRightCache(
            RowData leftRow, long rightQualifiedLowerBound, long rightQualifiedUpperBound)
            throws Exception {
        Set<Long> toRemove = new HashSet<>();
        final AtomicBoolean emitted = new AtomicBoolean(false);
        return rightCache
                .asyncEntries()
                .thenCompose(
                        iter ->
                                iter.onNext(
                                        entry -> {
                                            boolean entryEmitted =
                                                    tryMatchLeftRowsByTime(
                                                            leftRow,
                                                            entry,
                                                            rightQualifiedLowerBound,
                                                            rightQualifiedUpperBound,
                                                            toRemove);
                                            if (entryEmitted) {
                                                emitted.set(true);
                                            }
                                        }))
                .thenApply(
                        V -> {
                            for (Long key : toRemove) {
                                rightCache.asyncRemove(key);
                            }
                            return emitted.get();
                        });
    }

    /**
     * Iterate left cache to join the right input row. And remove the expired rows in left cache.
     *
     * @param rightRow the left input row
     * @param leftQualifiedLowerBound the lower bound of qualified left rows
     * @param leftQualifiedUpperBound the upper bound of qualified left rows
     * @return {@code StateFuture<Boolean>} true if the right input row is emitted, false otherwise
     */
    protected StateFuture<Boolean> iterateLeftCache(
            RowData rightRow, long leftQualifiedLowerBound, long leftQualifiedUpperBound)
            throws Exception {
        Set<Long> toRemove = new HashSet<>();
        final AtomicBoolean emitted = new AtomicBoolean(false);
        return leftCache
                .asyncEntries()
                .thenCompose(
                        iter ->
                                iter.onNext(
                                        entry -> {
                                            boolean entryEmitted =
                                                    tryMatchRightRowsByTime(
                                                            rightRow,
                                                            entry,
                                                            leftQualifiedLowerBound,
                                                            leftQualifiedUpperBound,
                                                            toRemove);
                                            if (entryEmitted) {
                                                emitted.set(true);
                                            }
                                        }))
                .thenApply(
                        V -> {
                            for (Long key : toRemove) {
                                leftCache.asyncRemove(key);
                            }
                            return emitted.get();
                        });
    }

    /**
     * Use the left input row to update left cache.
     *
     * @param leftRow the left input row
     * @param timeForLeftRow the time of left input row
     * @param emitted whether the left input row is emitted
     */
    protected void updateLeftCache(RowData leftRow, long timeForLeftRow, boolean emitted)
            throws Exception {
        leftCache
                .asyncGet(timeForLeftRow)
                .thenAccept(
                        leftRowList -> {
                            if (leftRowList == null) {
                                leftRowList = new ArrayList<>(1);
                            }
                            leftRowList.add(Tuple2.of(leftRow, emitted));
                            leftCache.asyncPut(timeForLeftRow, leftRowList);
                        });
    }

    /**
     * Use the right input row to update right cache.
     *
     * @param rightRow the right input row
     * @param timeForRightRow the time of right input row
     * @param emitted whether the right input row is emitted
     */
    protected void updateRightCache(RowData rightRow, long timeForRightRow, boolean emitted)
            throws Exception {
        rightCache
                .asyncGet(timeForRightRow)
                .thenAccept(
                        rightRowList -> {
                            if (rightRowList == null) {
                                rightRowList = new ArrayList<>(1);
                            }
                            rightRowList.add(Tuple2.of(rightRow, emitted));
                            rightCache.asyncPut(timeForRightRow, rightRowList);
                        });
    }

    /**
     * Register a timer for cleaning up rows in a specified time.
     *
     * @param ctx the context to register timer
     * @param rowTime time for the input row
     * @param leftRow whether this row comes from the left stream
     * @param force whether to force register the timer
     */
    protected void registerCleanUpTimer(
            KeyedCoProcessFunction<RowData, RowData, RowData, RowData>.Context ctx,
            long rowTime,
            boolean leftRow,
            boolean force)
            throws IOException {
        if (leftRow) {
            rightTimerState
                    .asyncValue()
                    .thenAccept(
                            v -> {
                                if (force || v == null) {
                                    long cleanUpTime =
                                            rowTime
                                                    + leftRelativeSize
                                                    + minCleanUpInterval
                                                    + allowedLateness
                                                    + 1;
                                    registerTimer(ctx, cleanUpTime);
                                    rightTimerState.asyncUpdate(cleanUpTime);
                                }
                            });
        } else {
            leftTimerState
                    .asyncValue()
                    .thenAccept(
                            v -> {
                                if (force || v == null) {
                                    long cleanUpTime =
                                            rowTime
                                                    + rightRelativeSize
                                                    + minCleanUpInterval
                                                    + allowedLateness
                                                    + 1;
                                    registerTimer(ctx, cleanUpTime);
                                    leftTimerState.asyncUpdate(cleanUpTime);
                                }
                            });
        }
    }

    private void removeExpiredRows(
            long expirationTime,
            MapState<Long, List<Tuple2<RowData, Boolean>>> rowCache,
            ValueState<Long> timerState,
            OnTimerContext ctx,
            boolean removeLeft)
            throws Exception {
        AtomicLong earliestTimestamp = new AtomicLong(-1L);
        Set<Long> toRemove = new HashSet<>();
        rowCache.asyncEntries()
                .thenCompose(
                        iter ->
                                iter.onNext(
                                        entry -> {
                                            Long rowTime = entry.getKey();
                                            if (rowTime <= expirationTime) {
                                                collectOuterOnTimer(entry, removeLeft);
                                                toRemove.add(rowTime);
                                            } else {
                                                // We find the earliest timestamp that is
                                                // still valid.
                                                if (rowTime < earliestTimestamp.get()
                                                        || earliestTimestamp.get() < 0) {
                                                    earliestTimestamp.set(rowTime);
                                                }
                                            }
                                        }))
                .thenAccept(
                        VOID -> {
                            for (Long key : toRemove) {
                                rowCache.asyncRemove(key);
                            }
                            if (earliestTimestamp.get() > 0) {
                                // There are rows left in the cache.
                                // Register a timer to expire them later.
                                registerCleanUpTimer(
                                        ctx, earliestTimestamp.get(), removeLeft, true);
                            } else {
                                // No rows left in the cache. Clear the
                                // states and the timerState will be 0.
                                timerState.asyncClear();
                                rowCache.asyncClear();
                            }
                        });
    }

    /**
     * Calculate the expiration time with the given operator time and relative window size.
     *
     * @param operatorTime the operator time
     * @param relativeSize the relative window size
     * @return the expiration time for cached rows
     */
    protected long calExpirationTime(long operatorTime, long relativeSize) {
        if (operatorTime < Long.MAX_VALUE) {
            return operatorTime - relativeSize - allowedLateness - 1;
        } else {
            // When operatorTime = Long.MaxValue, it means the stream has reached the end.
            return Long.MAX_VALUE;
        }
    }

    /**
     * Update the operator time of the two streams. Must be the first call in all processing methods
     * (i.e., processElement(), onTimer()).
     *
     * @param ctx the context to acquire watermarks
     */
    protected abstract void updateOperatorTime(Context ctx);

    /**
     * Return the time for the target row from the left stream. Requires that
     * [[updateOperatorTime()]] has been called before.
     *
     * @param ctx the runtime context
     * @param row the target row
     * @return time for the target row
     */
    protected abstract long getTimeForLeftStream(Context ctx, RowData row);

    /**
     * Return the time for the target row from the right stream. Requires that
     * [[updateOperatorTime()]] has been called before.
     *
     * @param ctx the runtime context
     * @param row the target row
     * @return time for the target row
     */
    protected abstract long getTimeForRightStream(Context ctx, RowData row);

    /**
     * Register a proctime or rowtime timer.
     *
     * @param ctx the context to register the timer
     * @param cleanupTime timestamp for the timer
     */
    protected abstract void registerTimer(Context ctx, long cleanupTime);

    /** The wrapper class to wrap the {@link NullAwareGetters} with declared variables. */
    private static class NullAwareGettersWithDeclaredVariables implements NullAwareGetters {

        ContextVariable<NullAwareGetters> wrapped;

        public NullAwareGettersWithDeclaredVariables(ContextVariable<NullAwareGetters> wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public boolean anyNull() {
            return wrapped.get().anyNull();
        }

        @Override
        public boolean anyNull(int[] fields) {
            return wrapped.get().anyNull(fields);
        }
    }
}
