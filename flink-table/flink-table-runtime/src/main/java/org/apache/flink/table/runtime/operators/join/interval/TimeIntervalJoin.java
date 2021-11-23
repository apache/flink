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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.OuterJoinPaddingUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A CoProcessFunction to execute time interval (time-bounded) stream inner-join. Two kinds of time
 * criteria: "L.time between R.time + X and R.time + Y" or "R.time between L.time - Y and L.time -
 * X" X and Y might be negative or positive and X <= Y.
 */
abstract class TimeIntervalJoin extends KeyedCoProcessFunction<RowData, RowData, RowData, RowData> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimeIntervalJoin.class);

    private static final String LEFT_RECORDS_STATE_NAME = "left-records";
    private static final String RIGHT_RECORDS_STATE_NAME = "right-records";

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

    private transient EmitAwareCollector joinCollector;

    // cache to store rows form the left stream
    private transient StateView leftCache;
    // cache to store rows from the right stream
    private transient StateView rightCache;

    // state to record the timer on the left stream. 0 means no timer set
    private transient ValueState<Long> leftTimerState;
    // state to record the timer on the right stream. 0 means no timer set
    private transient ValueState<Long> rightTimerState;

    // Points in time until which the respective cache has been cleaned.
    private long leftExpirationTime = Long.MIN_VALUE;
    private long rightExpirationTime = Long.MIN_VALUE;

    // Current time on the respective input stream.
    protected long leftOperatorTime = Long.MIN_VALUE;
    protected long rightOperatorTime = Long.MIN_VALUE;

    TimeIntervalJoin(
            FlinkJoinType joinType,
            long leftLowerBound,
            long leftUpperBound,
            long allowedLateness,
            InternalTypeInfo<RowData> leftType,
            InternalTypeInfo<RowData> rightType,
            IntervalJoinFunction joinFunc) {
        this.joinType = joinType;
        this.leftRelativeSize = -leftLowerBound;
        this.rightRelativeSize = leftUpperBound;
        minCleanUpInterval = (leftRelativeSize + rightRelativeSize) / 2;
        if (allowedLateness < 0) {
            throw new IllegalArgumentException("The allowed lateness must be non-negative.");
        }
        this.allowedLateness = allowedLateness;
        this.leftType = leftType;
        this.rightType = rightType;
        this.joinFunction = joinFunc;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        joinFunction.setRuntimeContext(getRuntimeContext());
        joinFunction.open(parameters);
        joinCollector = new EmitAwareCollector();

        // Initialize the data caches.
        leftCache = new StateView(getRuntimeContext(), LEFT_RECORDS_STATE_NAME, leftType);
        rightCache = new StateView(getRuntimeContext(), RIGHT_RECORDS_STATE_NAME, rightType);

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
        boolean findMatchingData = false;

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
            List<Long> rightAllTimes = rightCache.getAllTimes();
            for (long rightTime : rightAllTimes) {
                if (rightTime >= rightQualifiedLowerBound
                        && rightTime <= rightQualifiedUpperBound) {
                    List<Tuple2<RowData, Boolean>> rightRecords =
                            rightCache.getRecordsAndIsEmitted(rightTime);
                    for (Tuple2<RowData, Boolean> tuple : rightRecords) {
                        if (joinType.isSemiAnti()) {
                            if (joinFunction.isMatchCondition(leftRow, tuple.f0)) {
                                // In semi join, if a record in left stream can match one record in
                                // right stream, it can be output immediately and needn't be checked
                                // continually and be stored.
                                // In anti join, if a record in left stream can match one record in
                                // right stream, it can be eliminated immediately and needn't be
                                // checked continually and be stored.
                                findMatchingData = true;
                                if (joinType == FlinkJoinType.SEMI) {
                                    joinCollector.collect(leftRow);
                                }
                                break;
                            }
                        } else {
                            joinCollector.reset();
                            joinFunction.join(leftRow, tuple.f0, joinCollector);
                            emitted = emitted || joinCollector.isEmitted();
                            if (joinType.isRightOuter()) {
                                if (!tuple.f1 && joinCollector.isEmitted()) {
                                    // Mark the right row as being successfully joined and emitted.
                                    rightCache.markEmitted(rightTime, tuple.f0);
                                }
                            }
                        }
                    }
                }
                if (rightTime <= rightExpirationTime) {
                    if (joinType.isRightOuter()) {
                        List<Tuple2<RowData, Boolean>> rightRecords =
                                rightCache.getRecordsAndIsEmitted(rightTime);
                        for (Tuple2<RowData, Boolean> tuple : rightRecords) {
                            if (!tuple.f1) {
                                // Emit a null padding result if the right row has never
                                // been successfully joined.
                                joinCollector.collect(paddingUtil.padRight(tuple.f0));
                            }
                        }
                    } else if (joinType == FlinkJoinType.ANTI) {
                        // In anti join, in order to clean up the right cache safely, we should
                        // refresh the left cache to drop the invalid records.
                        cleanUpLeftRecordsByRightTimeInAnti(rightTime);
                    }

                    // eager remove
                    rightCache.removeRecords(rightTime);
                } // We could do the short-cutting optimization here once we get a state with
                // ordered keys.

                if (joinType.isSemiAnti() && findMatchingData) {
                    return;
                }
            }
        }
        // Check if we need to cache the current row.
        if (rightOperatorTime < rightQualifiedUpperBound) {
            // Operator time of right stream has not exceeded the upper window bound of the current
            // row. Put it into the left cache, since later coming records from the right stream are
            // expected to be joined with it.
            leftCache.addRecord(timeForLeftRow, leftRow, emitted);
            if (rightTimerState.value() == null) {
                // Register a timer on the RIGHT stream to remove rows.
                registerCleanUpTimer(ctx, timeForLeftRow, true);
            }
        } else if (!emitted && joinType.isLeftOuter()) {
            // Emit a null padding result if the left row is not cached and successfully joined.
            joinCollector.collect(paddingUtil.padLeft(leftRow));
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
            List<Long> leftAllTimes = leftCache.getAllTimes();
            for (long leftTime : leftAllTimes) {
                if (leftTime >= leftQualifiedLowerBound && leftTime <= leftQualifiedUpperBound) {
                    List<Tuple2<RowData, Boolean>> leftRows =
                            leftCache.getRecordsAndIsEmitted(leftTime);
                    for (Tuple2<RowData, Boolean> tuple : leftRows) {
                        // In semi join, if a record in right stream can match one record in left
                        // stream, the left record can be output immediately and removed from state.
                        // In anti join, if a record in right stream can match one record in left
                        // stream, the left record can be removed from state immediately.
                        if (joinType.isSemiAnti()) {
                            if (joinFunction.isMatchCondition(tuple.f0, rightRow)) {
                                if (joinType == FlinkJoinType.SEMI) {
                                    joinCollector.collect(tuple.f0);
                                }
                                // In semi/anti join, 'emitted' is always false.
                                leftCache.removeSingleRecord(leftTime, tuple.f0, false);
                            }
                            continue;
                        }

                        joinCollector.reset();
                        joinFunction.join(tuple.f0, rightRow, joinCollector);
                        emitted = emitted || joinCollector.isEmitted();
                        if (joinType.isLeftOuter()) {
                            if (!tuple.f1 && joinCollector.isEmitted()) {
                                // Mark the left row as being successfully joined and emitted.
                                leftCache.markEmitted(leftTime, tuple.f0);
                            }
                        }
                    }
                }
                if (leftTime <= leftExpirationTime) {
                    if (joinType.isLeftOuter()) {
                        List<Tuple2<RowData, Boolean>> leftRecords =
                                leftCache.getRecordsAndIsEmitted(leftTime);
                        for (Tuple2<RowData, Boolean> tuple : leftRecords) {
                            if (!tuple.f1) {
                                // Emit a null padding result if the left row has
                                // never been successfully joined.
                                joinCollector.collect(paddingUtil.padLeft(tuple.f0));
                            }
                        }
                    } else if (joinType == FlinkJoinType.ANTI) {
                        // Get all right times to help clean up the left records to output left
                        // valid records.
                        List<Long> rightAllTimes = rightCache.getAllTimes();
                        for (long rightTime : rightAllTimes) {
                            if (leftTime < calExpirationTime(rightTime, leftRelativeSize)) {
                                cleanUpLeftRecordsByRightTimeInAnti(rightTime);
                            }
                        }
                    }
                    // eager remove
                    leftCache.removeRecords(leftTime);
                } // We could do the short-cutting optimization here once we get a state with
                // ordered keys.
            }
        }
        // Check if we need to cache the current row.
        if (leftOperatorTime < leftQualifiedUpperBound) {
            // Operator time of left stream has not exceeded the upper window bound of the current
            // row. Put it into the right cache, since later coming records from the left stream are
            // expected to be joined with it.
            rightCache.addRecord(timeForRightRow, rightRow, emitted);
            if (leftTimerState.value() == null) {
                // Register a timer on the LEFT stream to remove rows.
                registerCleanUpTimer(ctx, timeForRightRow, false);
            }
        } else if (!emitted && joinType.isRightOuter()) {
            // Emit a null padding result if the right row is not cached and successfully joined.
            joinCollector.collect(paddingUtil.padRight(rightRow));
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RowData> out)
            throws Exception {
        joinFunction.setJoinKey(ctx.getCurrentKey());
        joinCollector.setInnerCollector(out);
        updateOperatorTime(ctx);
        // In the future, we should separate the left and right watermarks. Otherwise, the
        // registered timer of the faster stream will be delayed, even if the watermarks have
        // already been emitted by the source.
        Long leftCleanUpTime = leftTimerState.value();
        if (leftCleanUpTime != null && timestamp == leftCleanUpTime) {
            rightExpirationTime = calExpirationTime(leftOperatorTime, rightRelativeSize);
            removeExpiredRows(
                    joinCollector, rightExpirationTime, rightCache, leftTimerState, ctx, false);
        }

        Long rightCleanUpTime = rightTimerState.value();
        if (rightCleanUpTime != null && timestamp == rightCleanUpTime) {
            leftExpirationTime = calExpirationTime(rightOperatorTime, leftRelativeSize);
            removeExpiredRows(
                    joinCollector, leftExpirationTime, leftCache, rightTimerState, ctx, true);
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
            // prevent expiration time out of bounds
            if (relativeSize >= 0) {
                if (operatorTime < Long.MIN_VALUE + relativeSize + allowedLateness + 1) {
                    return Long.MIN_VALUE;
                }
            } else {
                if (operatorTime - relativeSize < Long.MIN_VALUE + allowedLateness + 1) {
                    return Long.MIN_VALUE;
                }
            }

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
            StateView rowCache,
            ValueState<Long> timerState,
            OnTimerContext ctx,
            boolean removeLeft)
            throws Exception {
        List<Long> allTimes = rowCache.getAllTimes();

        long earliestTimestamp = -1L;

        // We remove all expired keys and do not leave the loop early.
        // Hence, we do a full pass over the state.
        for (long rowTime : allTimes) {
            if (rowTime <= expirationTime) {
                List<Tuple2<RowData, Boolean>> rowRecords =
                        rowCache.getRecordsAndIsEmitted(rowTime);
                if (removeLeft && joinType.isLeftOuter()) {
                    for (Tuple2<RowData, Boolean> tuple : rowRecords) {
                        if (!tuple.f1) {
                            // Emit a null padding result if the right row has
                            // never been successfully joined.
                            collector.collect(paddingUtil.padLeft(tuple.f0));
                        }
                    }
                } else if (!removeLeft && joinType.isRightOuter()) {
                    for (Tuple2<RowData, Boolean> tuple : rowRecords) {
                        if (!tuple.f1) {
                            // Emit a null padding result if the right row has never
                            // been successfully joined.
                            collector.collect(paddingUtil.padRight(tuple.f0));
                        }
                    }
                } else if (removeLeft && joinType == FlinkJoinType.ANTI) {
                    // clean up left invalid records by right time
                    List<Long> rightAllTimes = rightCache.getAllTimes();
                    for (long rightTime : rightAllTimes) {
                        if (expirationTime < calExpirationTime(rightTime, leftRelativeSize)) {
                            cleanUpLeftRecordsByRightTimeInAnti(rightTime);
                        }
                    }
                    // output the remaining left records
                    List<Tuple2<RowData, Boolean>> remainingLeftRecords =
                            rowCache.getRecordsAndIsEmitted(rowTime);
                    for (Tuple2<RowData, Boolean> rowDataAndIsEmitted : remainingLeftRecords) {
                        joinCollector.collect(rowDataAndIsEmitted.f0);
                    }
                } else if (!removeLeft && joinType == FlinkJoinType.ANTI) {
                    // In anti join, we should refresh the left cache to drop the invalid
                    // records to clear up the right cache safely.
                    cleanUpLeftRecordsByRightTimeInAnti(expirationTime);
                }
                // eager remove
                rowCache.removeRecords(rowTime);
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
        }
    }

    private void cleanUpLeftRecordsByRightTimeInAnti(long rightTime) throws Exception {
        List<Long> leftAllTimes = leftCache.getAllTimes();
        List<Tuple2<RowData, Boolean>> rightRecords = rightCache.getRecordsAndIsEmitted(rightTime);
        long tempLeftCleanUpTime = calExpirationTime(rightTime, leftRelativeSize);
        for (Tuple2<RowData, Boolean> rightRecord : rightRecords) {
            for (long leftTime : leftAllTimes) {
                if (leftTime < tempLeftCleanUpTime) {
                    List<Tuple2<RowData, Boolean>> leftRecords =
                            leftCache.getRecordsAndIsEmitted(leftTime);
                    for (Tuple2<RowData, Boolean> leftRecord : leftRecords) {
                        if (!matchAllConditions(
                                leftRecord.f0, rightRecord.f0, leftTime, rightTime, joinFunction)) {
                            joinCollector.collect(leftRecord.f0);
                        }
                        leftCache.removeSingleRecord(leftTime, leftRecord.f0, leftRecord.f1);
                    }
                }
            }
        }
    }

    private boolean matchAllConditions(
            RowData left,
            RowData right,
            long leftTime,
            long rightTime,
            IntervalJoinFunction joinFunction) {
        final long rightLowerBound = leftTime - rightRelativeSize;
        final long rightUpperBound = leftTime + leftRelativeSize;

        if (!joinFunction.isMatchCondition(left, right)) {
            return false;
        }
        return rightTime >= rightLowerBound && rightTime <= rightUpperBound;
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

    private static final class StateView {
        // store records in the mapping like:
        // <event-time / proc-time, <<record, isEmitted>, frequency>
        private final MapState<Long, Map<Tuple2<RowData, Boolean>, Integer>> recordState;

        private StateView(
                RuntimeContext ctx, String stateName, InternalTypeInfo<RowData> recordType) {

            MapTypeInfo<Tuple2<RowData, Boolean>, Integer> mapTypeInfo =
                    new MapTypeInfo<>(new TupleTypeInfo<>(recordType, Types.BOOLEAN), Types.INT);
            MapStateDescriptor<Long, Map<Tuple2<RowData, Boolean>, Integer>> recordStateDesc =
                    new MapStateDescriptor<>(stateName, Types.LONG, mapTypeInfo);

            this.recordState = ctx.getMapState(recordStateDesc);
        }

        public List<Long> getAllTimes() throws Exception {
            List<Long> allTimes = new ArrayList<>();
            for (Long time : recordState.keys()) {
                allTimes.add(time);
            }
            return allTimes;
        }

        public void addRecord(long time, RowData record, boolean isEmitted) throws Exception {
            Map<Tuple2<RowData, Boolean>, Integer> rowDataMap = recordState.get(time);
            Integer frequency;

            if (rowDataMap == null) {
                rowDataMap = new HashMap<>();
                frequency = 1;
            } else {
                frequency = rowDataMap.get(record);
                frequency = frequency == null ? 1 : frequency + 1;
            }

            Tuple2<RowData, Boolean> rowDataAndEmittedInfo = Tuple2.of(record, isEmitted);
            rowDataMap.put(rowDataAndEmittedInfo, frequency);
            recordState.put(time, rowDataMap);
        }

        public List<Tuple2<RowData, Boolean>> getRecordsAndIsEmitted(long time) throws Exception {
            List<Tuple2<RowData, Boolean>> resultList = new ArrayList<>();
            Map<Tuple2<RowData, Boolean>, Integer> rowDataMap = recordState.get(time);
            if (rowDataMap == null) {
                return resultList;
            }
            for (Tuple2<RowData, Boolean> rowDataAndIsEmitted : rowDataMap.keySet()) {
                Integer remainTimes = rowDataMap.get(rowDataAndIsEmitted);
                if (remainTimes == null) {
                    continue;
                }
                while (remainTimes > 0) {
                    resultList.add(rowDataAndIsEmitted);
                    remainTimes--;
                }
            }
            return resultList;
        }

        public void markEmitted(long time, RowData record) throws Exception {
            Tuple2<RowData, Boolean> rowDataAndIsEmitted = Tuple2.of(record, false);
            Map<Tuple2<RowData, Boolean>, Integer> rowDataMap = recordState.get(time);
            if (rowDataMap == null) {
                return;
            }

            Integer frequency = rowDataMap.get(rowDataAndIsEmitted);
            // The state doesn't have this record
            if (frequency == null) {
                return;
            }
            if (frequency == 0) {
                rowDataMap.remove(rowDataAndIsEmitted);
                return;
            }

            // change the tag 'isEmitted'
            rowDataMap.remove(rowDataAndIsEmitted);
            rowDataAndIsEmitted.f1 = true;
            rowDataMap.put(rowDataAndIsEmitted, frequency);
            recordState.put(time, rowDataMap);
        }

        public void removeRecords(long time) throws Exception {
            recordState.remove(time);
        }

        public void removeSingleRecord(long time, RowData record, boolean isEmitted)
                throws Exception {
            Map<Tuple2<RowData, Boolean>, Integer> rowDataMap = recordState.get(time);
            if (rowDataMap == null) {
                return;
            }
            Tuple2<RowData, Boolean> rowDataAndIsEmitted = Tuple2.of(record, isEmitted);
            Integer frequency = rowDataMap.get(rowDataAndIsEmitted);
            if (frequency == null) {
                return;
            }
            if (frequency > 1) {
                frequency = frequency - 1;
                rowDataMap.put(rowDataAndIsEmitted, frequency);
            } else {
                rowDataMap.remove(rowDataAndIsEmitted);
            }
            if (rowDataMap.size() == 0) {
                recordState.remove(time);
            } else {
                recordState.put(time, rowDataMap);
            }
        }

        public void clear() {
            recordState.clear();
        }
    }
}
