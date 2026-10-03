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

package org.apache.flink.table.runtime.operators.join.interval;

import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.OuterJoinPaddingUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A CoProcessFunction to execute time interval (time-bounded) stream inner-join. Two kinds of time
 * criteria: "L.time between R.time + X and R.time + Y" or "R.time between L.time - Y and L.time -
 * X" X and Y might be negative or positive and X <= Y.
 */
abstract class TimeIntervalJoin extends KeyedCoProcessFunction<RowData, RowData, RowData, RowData> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimeIntervalJoin.class);
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

    private transient EmitAwareCollector joinCollector;

    // cache to store rows form the left stream
    private transient MapState<Long, List<Tuple2<RowData, Boolean>>> leftCache;
    // cache to store rows from the right stream
    private transient MapState<Long, List<Tuple2<RowData, Boolean>>> rightCache;

    // For each cached outer row, whether its speculative early-fire pad has already been emitted.
    // The list is positionally aligned 1:1 with the row-time bucket in leftCache / rightCache, so
    // firedState.get(t).get(i) corresponds to cache.get(t).get(i). It is kept as a parallel list
    // rather than a third tuple field so the existing cache serializer stays unchanged. The bit
    // gates both the unmatched window-close pad (it must not be emitted twice) and the retraction
    // on a later match (only a row that was speculatively padded needs correcting).
    private transient MapState<Long, List<Boolean>> leftFiredState;
    private transient MapState<Long, List<Boolean>> rightFiredState;

    // state to record the timer on the left stream. 0 means no timer set
    private transient ValueState<Long> leftTimerState;
    // state to record the timer on the right stream. 0 means no timer set
    private transient ValueState<Long> rightTimerState;

    // Points in time until which the respective cache has been cleaned.
    private long leftExpirationTime = 0L;
    private long rightExpirationTime = 0L;

    // Current time on the respective input stream.
    protected long leftOperatorTime = 0L;
    protected long rightOperatorTime = 0L;

    TimeIntervalJoin(
            FlinkJoinType joinType,
            long leftLowerBound,
            long leftUpperBound,
            long allowedLateness,
            long minCleanUpInterval,
            InternalTypeInfo<RowData> leftType,
            InternalTypeInfo<RowData> rightType,
            IntervalJoinFunction joinFunc,
            long earlyFireDelay) {
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
        // leftRelativeSize + rightRelativeSize equals the window span (leftUpperBound minus
        // leftLowerBound), matching the planner gate that enables update-producing early fire for
        // outer joins.
        this.earlyFireEnabled =
                earlyFireDelay >= 0
                        && joinType.isOuter()
                        && (leftRelativeSize + rightRelativeSize) >= 0;
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

        // Early-fire bookkeeping, aligned with the caches above. New descriptor names restore as
        // empty state from savepoints taken before early firing existed.
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
        joinFunction.setJoinKey(ctx.getCurrentKey());
        joinCollector.setInnerCollector(out);
        updateOperatorTime(ctx);

        long timeForLeftRow = getTimeForLeftStream(ctx, leftRow);
        long rightQualifiedLowerBound = timeForLeftRow - rightRelativeSize;
        long rightQualifiedUpperBound = timeForLeftRow + leftRelativeSize;
        boolean emitted = false;

        // Check if we need to join the current row against cached rows of the right input.
        // The condition here should be rightMinimumTime < rightQualifiedUpperBound.
        // We use rightExpirationTime as an approximation of the rightMinimumTime here,
        // since rightExpirationTime <= rightMinimumTime is always true.
        if (rightExpirationTime < rightQualifiedUpperBound) {
            // Upper bound of current join window has not passed the cache expiration time yet.
            // There might be qualifying rows in the cache that the current row needs to be joined
            // with.
            rightExpirationTime = calExpirationTime(leftOperatorTime, rightRelativeSize);
            // Join the leftRow with rows from the right cache.
            Iterator<Map.Entry<Long, List<Tuple2<RowData, Boolean>>>> rightIterator =
                    rightCache.iterator();
            while (rightIterator.hasNext()) {
                Map.Entry<Long, List<Tuple2<RowData, Boolean>>> rightEntry = rightIterator.next();
                Long rightTime = rightEntry.getKey();
                if (rightTime >= rightQualifiedLowerBound
                        && rightTime <= rightQualifiedUpperBound) {
                    List<Tuple2<RowData, Boolean>> rightRows = rightEntry.getValue();
                    List<Boolean> rightFired =
                            earlyFireEnabled && joinType.isRightOuter()
                                    ? firedBits(rightFiredState, rightTime, rightRows)
                                    : null;
                    boolean entryUpdated = false;
                    for (int i = 0; i < rightRows.size(); i++) {
                        Tuple2<RowData, Boolean> tuple = rightRows.get(i);
                        joinCollector.reset();
                        boolean retract =
                                rightFired != null
                                        && joinType.isRightOuter()
                                        && !tuple.f1
                                        && rightFired.get(i);
                        if (retract) {
                            // The speculative pad for this right row was already emitted as an
                            // insert; arm the collector so the match becomes -U(pad)/+U(match).
                            joinCollector.armRetraction(paddingUtil.padRight(tuple.f0));
                        }
                        joinFunction.join(leftRow, tuple.f0, joinCollector);
                        if (retract && !joinCollector.isEmitted()) {
                            joinCollector.disarm();
                        }
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
                if (rightTime <= rightExpirationTime) {
                    if (joinType.isRightOuter()) {
                        List<Tuple2<RowData, Boolean>> rightRows = rightEntry.getValue();
                        List<Boolean> rightFired =
                                earlyFireEnabled
                                        ? firedBits(rightFiredState, rightTime, rightRows)
                                        : null;
                        for (int i = 0; i < rightRows.size(); i++) {
                            Tuple2<RowData, Boolean> tuple = rightRows.get(i);
                            // Skip a row whose speculative pad already fired: it is correct as
                            // emitted and must not be padded a second time.
                            if (!tuple.f1 && (rightFired == null || !rightFired.get(i))) {
                                collectPad(paddingUtil.padRight(tuple.f0));
                            }
                        }
                    }
                    // eager remove
                    rightIterator.remove();
                    if (earlyFireEnabled) {
                        removeFired(rightFiredState, rightTime);
                    }
                } // We could do the short-cutting optimization here once we get a state with
                // ordered keys.
            }
        }
        // Check if we need to cache the current row.
        if (rightOperatorTime < rightQualifiedUpperBound) {
            // Operator time of right stream has not exceeded the upper window bound of the current
            // row. Put it into the left cache, since later coming records from the right stream are
            // expected to be joined with it.
            List<Tuple2<RowData, Boolean>> leftRowList = leftCache.get(timeForLeftRow);
            if (leftRowList == null) {
                leftRowList = new ArrayList<>(1);
            }
            leftRowList.add(Tuple2.of(leftRow, emitted));
            leftCache.put(timeForLeftRow, leftRowList);
            if (earlyFireEnabled && joinType.isLeftOuter()) {
                // The new tuple has not been speculatively padded yet, so its bit starts false.
                appendFired(leftFiredState, timeForLeftRow);
                if (!emitted) {
                    // Schedule a speculative pad of this unmatched left row after the delay.
                    registerTimer(ctx, timeForLeftRow + earlyFireDelay);
                }
            }
            if (rightTimerState.value() == null) {
                // Register a timer on the RIGHT stream to remove rows.
                registerCleanUpTimer(ctx, timeForLeftRow, true);
            }
        } else if (!emitted && joinType.isLeftOuter()) {
            // Emit a null padding result if the left row is not cached and successfully joined.
            collectPad(paddingUtil.padLeft(leftRow));
        }
    }

    @Override
    public void processElement2(RowData rightRow, Context ctx, Collector<RowData> out)
            throws Exception {
        joinFunction.setJoinKey(ctx.getCurrentKey());
        joinCollector.setInnerCollector(out);
        updateOperatorTime(ctx);
        long timeForRightRow = getTimeForRightStream(ctx, rightRow);
        long leftQualifiedLowerBound = timeForRightRow - leftRelativeSize;
        long leftQualifiedUpperBound = timeForRightRow + rightRelativeSize;
        boolean emitted = false;

        // Check if we need to join the current row against cached rows of the left input.
        // The condition here should be leftMinimumTime < leftQualifiedUpperBound.
        // We use leftExpirationTime as an approximation of the leftMinimumTime here,
        // since leftExpirationTime <= leftMinimumTime is always true.
        if (leftExpirationTime < leftQualifiedUpperBound) {
            leftExpirationTime = calExpirationTime(rightOperatorTime, leftRelativeSize);
            // Join the rightRow with rows from the left cache.
            Iterator<Map.Entry<Long, List<Tuple2<RowData, Boolean>>>> leftIterator =
                    leftCache.iterator();
            while (leftIterator.hasNext()) {
                Map.Entry<Long, List<Tuple2<RowData, Boolean>>> leftEntry = leftIterator.next();
                Long leftTime = leftEntry.getKey();
                if (leftTime >= leftQualifiedLowerBound && leftTime <= leftQualifiedUpperBound) {
                    List<Tuple2<RowData, Boolean>> leftRows = leftEntry.getValue();
                    List<Boolean> leftFired =
                            earlyFireEnabled && joinType.isLeftOuter()
                                    ? firedBits(leftFiredState, leftTime, leftRows)
                                    : null;
                    boolean entryUpdated = false;
                    for (int i = 0; i < leftRows.size(); i++) {
                        Tuple2<RowData, Boolean> tuple = leftRows.get(i);
                        joinCollector.reset();
                        boolean retract =
                                leftFired != null
                                        && joinType.isLeftOuter()
                                        && !tuple.f1
                                        && leftFired.get(i);
                        if (retract) {
                            // The speculative pad for this left row was already emitted as an
                            // insert; arm the collector so the match becomes -U(pad)/+U(match).
                            joinCollector.armRetraction(paddingUtil.padLeft(tuple.f0));
                        }
                        joinFunction.join(tuple.f0, rightRow, joinCollector);
                        if (retract && !joinCollector.isEmitted()) {
                            joinCollector.disarm();
                        }
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

                if (leftTime <= leftExpirationTime) {
                    if (joinType.isLeftOuter()) {
                        List<Tuple2<RowData, Boolean>> leftRows = leftEntry.getValue();
                        List<Boolean> leftFired =
                                earlyFireEnabled
                                        ? firedBits(leftFiredState, leftTime, leftRows)
                                        : null;
                        for (int i = 0; i < leftRows.size(); i++) {
                            Tuple2<RowData, Boolean> tuple = leftRows.get(i);
                            // Skip a row whose speculative pad already fired: it is correct as
                            // emitted and must not be padded a second time.
                            if (!tuple.f1 && (leftFired == null || !leftFired.get(i))) {
                                collectPad(paddingUtil.padLeft(tuple.f0));
                            }
                        }
                    }
                    // eager remove
                    leftIterator.remove();
                    if (earlyFireEnabled) {
                        removeFired(leftFiredState, leftTime);
                    }
                } // We could do the short-cutting optimization here once we get a state with
                // ordered keys.
            }
        }
        // Check if we need to cache the current row.
        if (leftOperatorTime < leftQualifiedUpperBound) {
            // Operator time of left stream has not exceeded the upper window bound of the current
            // row. Put it into the right cache, since later coming records from the left stream are
            // expected to be joined with it.
            List<Tuple2<RowData, Boolean>> rightRowList = rightCache.get(timeForRightRow);
            if (null == rightRowList) {
                rightRowList = new ArrayList<>(1);
            }
            rightRowList.add(Tuple2.of(rightRow, emitted));
            rightCache.put(timeForRightRow, rightRowList);
            if (earlyFireEnabled && joinType.isRightOuter()) {
                // The new tuple has not been speculatively padded yet, so its bit starts false.
                appendFired(rightFiredState, timeForRightRow);
                if (!emitted) {
                    // Schedule a speculative pad of this unmatched right row after the delay.
                    registerTimer(ctx, timeForRightRow + earlyFireDelay);
                }
            }
            if (leftTimerState.value() == null) {
                // Register a timer on the LEFT stream to remove rows.
                registerCleanUpTimer(ctx, timeForRightRow, false);
            }
        } else if (!emitted && joinType.isRightOuter()) {
            // Emit a null padding result if the right row is not cached and successfully joined.
            collectPad(paddingUtil.padRight(rightRow));
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RowData> out)
            throws Exception {
        joinFunction.setJoinKey(ctx.getCurrentKey());
        joinCollector.setInnerCollector(out);
        updateOperatorTime(ctx);

        // Early fire runs before cleanup at a shared timestamp so a row that is both due to fire
        // and
        // due to expire emits its speculative pad here; the cleanup branch's fired-bit gate then
        // suppresses a second pad. A cleanup-only timestamp finds no live unfired-unmatched row at
        // timestamp - earlyFireDelay and is a cheap no-op.
        if (earlyFireEnabled) {
            long rowTime = timestamp - earlyFireDelay;
            if (joinType.isLeftOuter()) {
                earlyFire(leftCache, leftFiredState, rowTime, true);
            }
            if (joinType.isRightOuter()) {
                earlyFire(rightCache, rightFiredState, rowTime, false);
            }
        }

        // In the future, we should separate the left and right watermarks. Otherwise, the
        // registered timer of the faster stream will be delayed, even if the watermarks have
        // already been emitted by the source.
        Long leftCleanUpTime = leftTimerState.value();
        if (leftCleanUpTime != null && timestamp == leftCleanUpTime) {
            rightExpirationTime = calExpirationTime(leftOperatorTime, rightRelativeSize);
            removeExpiredRows(
                    joinCollector,
                    rightExpirationTime,
                    rightCache,
                    rightFiredState,
                    leftTimerState,
                    ctx,
                    false);
        }

        Long rightCleanUpTime = rightTimerState.value();
        if (rightCleanUpTime != null && timestamp == rightCleanUpTime) {
            leftExpirationTime = calExpirationTime(rightOperatorTime, leftRelativeSize);
            removeExpiredRows(
                    joinCollector,
                    leftExpirationTime,
                    leftCache,
                    leftFiredState,
                    rightTimerState,
                    ctx,
                    true);
        }
    }

    /**
     * Emit the speculative null-padding result for every cached outer row at the given row time
     * that is still unmatched and has not yet had its pad emitted, flipping its fired bit so
     * neither this path nor the later window-close pad emits it again.
     */
    private void earlyFire(
            MapState<Long, List<Tuple2<RowData, Boolean>>> rowCache,
            MapState<Long, List<Boolean>> firedState,
            long rowTime,
            boolean padLeft)
            throws Exception {
        List<Tuple2<RowData, Boolean>> rows = rowCache.get(rowTime);
        if (rows == null) {
            return;
        }
        List<Boolean> fired = firedBits(firedState, rowTime, rows);
        boolean changed = false;
        for (int i = 0; i < rows.size(); i++) {
            Tuple2<RowData, Boolean> tuple = rows.get(i);
            if (!tuple.f1 && !fired.get(i)) {
                collectPad(
                        padLeft ? paddingUtil.padLeft(tuple.f0) : paddingUtil.padRight(tuple.f0));
                fired.set(i, true);
                changed = true;
            }
        }
        if (changed) {
            firedState.put(rowTime, fired);
        }
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
    private void registerCleanUpTimer(Context ctx, long rowTime, boolean leftRow)
            throws IOException {
        if (leftRow) {
            long cleanUpTime =
                    rowTime + leftRelativeSize + minCleanUpInterval + allowedLateness + 1;
            registerTimer(ctx, cleanUpTime);
            rightTimerState.update(cleanUpTime);
        } else {
            long cleanUpTime =
                    rowTime + rightRelativeSize + minCleanUpInterval + allowedLateness + 1;
            registerTimer(ctx, cleanUpTime);
            leftTimerState.update(cleanUpTime);
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
    private void removeExpiredRows(
            Collector<RowData> collector,
            long expirationTime,
            MapState<Long, List<Tuple2<RowData, Boolean>>> rowCache,
            MapState<Long, List<Boolean>> firedState,
            ValueState<Long> timerState,
            OnTimerContext ctx,
            boolean removeLeft)
            throws Exception {
        Iterator<Map.Entry<Long, List<Tuple2<RowData, Boolean>>>> iterator = rowCache.iterator();

        long earliestTimestamp = -1L;

        // We remove all expired keys and do not leave the loop early.
        // Hence, we do a full pass over the state.
        while (iterator.hasNext()) {
            Map.Entry<Long, List<Tuple2<RowData, Boolean>>> entry = iterator.next();
            Long rowTime = entry.getKey();
            if (rowTime <= expirationTime) {
                boolean removeOuter =
                        (removeLeft && joinType.isLeftOuter())
                                || (!removeLeft && joinType.isRightOuter());
                if (removeOuter) {
                    List<Tuple2<RowData, Boolean>> rows = entry.getValue();
                    List<Boolean> fired =
                            earlyFireEnabled ? firedBits(firedState, rowTime, rows) : null;
                    for (int i = 0; i < rows.size(); i++) {
                        Tuple2<RowData, Boolean> tuple = rows.get(i);
                        // Emit a null padding result only if the row was never matched and its
                        // speculative pad has not already been emitted.
                        if (!tuple.f1 && (fired == null || !fired.get(i))) {
                            collectPad(
                                    removeLeft
                                            ? paddingUtil.padLeft(tuple.f0)
                                            : paddingUtil.padRight(tuple.f0));
                        }
                    }
                }
                iterator.remove();
                if (earlyFireEnabled) {
                    removeFired(firedState, rowTime);
                }
            } else {
                // We find the earliest timestamp that is still valid.
                if (rowTime < earliestTimestamp || earliestTimestamp < 0) {
                    earliestTimestamp = rowTime;
                }
            }
        }

        if (earliestTimestamp > 0) {
            // There are rows left in the cache. Register a timer to expire them later.
            registerCleanUpTimer(ctx, earliestTimestamp, removeLeft);
        } else {
            // No rows left in the cache. Clear the states and the timerState will be 0.
            timerState.clear();
            rowCache.clear();
            if (earlyFireEnabled && firedState != null) {
                firedState.clear();
            }
        }
    }

    /**
     * Emit a padded outer-join row as an insert, overriding any leaked row kind on the reused row.
     */
    private void collectPad(RowData paddedRow) {
        paddedRow.setRowKind(RowKind.INSERT);
        joinCollector.collect(paddedRow);
    }

    /**
     * Return the fired-bit list aligned with the given cache bucket. Only called when early firing
     * is enabled. When the stored list is absent or its length no longer matches the bucket (e.g.
     * after a restore), a fresh all-false list of the right length is rebuilt so no row is ever
     * treated as already fired.
     */
    private List<Boolean> firedBits(
            MapState<Long, List<Boolean>> firedState,
            long rowTime,
            List<Tuple2<RowData, Boolean>> rows)
            throws Exception {
        if (firedState != null) {
            List<Boolean> fired = firedState.get(rowTime);
            if (fired != null && fired.size() == rows.size()) {
                return fired;
            }
        }
        List<Boolean> fired = new ArrayList<>(rows.size());
        for (int i = 0; i < rows.size(); i++) {
            fired.add(Boolean.FALSE);
        }
        return fired;
    }

    private void appendFired(MapState<Long, List<Boolean>> firedState, long rowTime)
            throws Exception {
        List<Boolean> fired = firedState.get(rowTime);
        if (fired == null) {
            fired = new ArrayList<>(1);
        }
        fired.add(Boolean.FALSE);
        firedState.put(rowTime, fired);
    }

    private void removeFired(MapState<Long, List<Boolean>> firedState, long rowTime)
            throws Exception {
        if (firedState != null) {
            firedState.remove(rowTime);
        }
    }

    /**
     * Update the operator time of the two streams. Must be the first call in all processing methods
     * (i.e., processElement(), onTimer()).
     *
     * @param ctx the context to acquire watermarks
     */
    abstract void updateOperatorTime(Context ctx);

    /**
     * Return the time for the target row from the left stream. Requires that
     * [[updateOperatorTime()]] has been called before.
     *
     * @param ctx the runtime context
     * @param row the target row
     * @return time for the target row
     */
    abstract long getTimeForLeftStream(Context ctx, RowData row);

    /**
     * Return the time for the target row from the right stream. Requires that
     * [[updateOperatorTime()]] has been called before.
     *
     * @param ctx the runtime context
     * @param row the target row
     * @return time for the target row
     */
    abstract long getTimeForRightStream(Context ctx, RowData row);

    /**
     * Register a proctime or rowtime timer.
     *
     * @param ctx the context to register the timer
     * @param cleanupTime timestamp for the timer
     */
    abstract void registerTimer(Context ctx, long cleanupTime);

    @Override
    public boolean useInterruptibleTimers(ReadableConfig config) {
        return true;
    }
}
