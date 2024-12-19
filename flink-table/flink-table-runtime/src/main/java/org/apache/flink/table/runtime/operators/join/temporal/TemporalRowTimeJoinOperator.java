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

package org.apache.flink.table.runtime.operators.join.temporal;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.join.temporal.utils.RowtimeComparator;
import org.apache.flink.table.runtime.operators.join.temporal.utils.TemporalRowTimeJoinHelper;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The operator for temporal join (FOR SYSTEM_TIME AS OF o.rowtime) on row time in sync state, it
 * has no limitation about message types of the left input and right input, this means the operator
 * deals changelog well.
 *
 * <p>For Event-time temporal join, its probe side is a regular table, its build side is a versioned
 * table, the version of versioned table can extract from the build side state. This operator works
 * by keeping on the state collection of probe and build records to process on next watermark. The
 * idea is that between watermarks we are collecting those elements and once we are sure that there
 * will be no updates we emit the correct result and clean up the expired data in state.
 *
 * <p>Cleaning up the state drops all of the "old" values from the probe side, where "old" is
 * defined as older then the current watermark. Build side is also cleaned up in the similar
 * fashion, however we always keep at least one record - the latest one - even if it's past the last
 * watermark.
 *
 * <p>One more trick is how the emitting results and cleaning up is triggered. It is achieved by
 * registering timers for the keys. We could register a timer for every probe and build side
 * element's event time (when watermark exceeds this timer, that's when we are emitting and/or
 * cleaning up the state). However this would cause huge number of registered timers. For example
 * with following evenTimes of probe records accumulated: {1, 2, 5, 8, 9}, if we had received
 * Watermark(10), it would trigger 5 separate timers for the same key. To avoid that we always keep
 * only one single registered timer for any given key, registered for the minimal value. Upon
 * triggering it, we process all records with event times older then or equal to currentWatermark.
 */
public class TemporalRowTimeJoinOperator extends BaseTwoInputStreamOperatorWithStateRetention {

    private static final long serialVersionUID = 6642514795175288193L;

    private static final String NEXT_LEFT_INDEX_STATE_NAME = "next-index";
    private static final String LEFT_STATE_NAME = "left";
    private static final String RIGHT_STATE_NAME = "right";
    private static final String REGISTERED_TIMER_STATE_NAME = "timer";
    private static final String TIMERS_STATE_NAME = "timers";

    private final boolean isLeftOuterJoin;
    private final InternalTypeInfo<RowData> leftType;
    private final InternalTypeInfo<RowData> rightType;
    private final GeneratedJoinCondition generatedJoinCondition;
    private final int leftTimeAttribute;
    private final int rightTimeAttribute;

    private final RowtimeComparator rightRowtimeComparator;

    /** Incremental index generator for {@link #leftState}'s keys. */
    private transient ValueState<Long> nextLeftIndex;

    /**
     * Mapping from artificial row index (generated by `nextLeftIndex`) into the left side `Row`. We
     * can not use List to accumulate Rows, because we need efficient deletes of the oldest rows.
     *
     * <p>TODO: this could be OrderedMultiMap[Jlong, Row] indexed by row's timestamp, to avoid full
     * map traversals (if we have lots of rows on the state that exceed `currentWatermark`).
     */
    private transient MapState<Long, RowData> leftState;

    /**
     * Mapping from timestamp to right side `Row`.
     *
     * <p>TODO: having `rightState` as an OrderedMapState would allow us to avoid sorting cost once
     * per watermark
     */
    private transient MapState<Long, RowData> rightState;

    // Long for correct handling of default null
    private transient ValueState<Long> registeredTimer;
    private transient TimestampedCollector<RowData> collector;
    private transient InternalTimerService<VoidNamespace> timerService;

    private transient JoinCondition joinCondition;
    private transient JoinedRowData outRow;
    private transient GenericRowData rightNullRow;

    private transient SyncStateTemporalRowTimeJoinHelper temporalRowTimeJoinHelper;

    public TemporalRowTimeJoinOperator(
            InternalTypeInfo<RowData> leftType,
            InternalTypeInfo<RowData> rightType,
            GeneratedJoinCondition generatedJoinCondition,
            int leftTimeAttribute,
            int rightTimeAttribute,
            long minRetentionTime,
            long maxRetentionTime,
            boolean isLeftOuterJoin) {
        super(minRetentionTime, maxRetentionTime);
        this.leftType = leftType;
        this.rightType = rightType;
        this.generatedJoinCondition = generatedJoinCondition;
        this.leftTimeAttribute = leftTimeAttribute;
        this.rightTimeAttribute = rightTimeAttribute;
        this.rightRowtimeComparator = new RowtimeComparator(rightTimeAttribute);
        this.isLeftOuterJoin = isLeftOuterJoin;
    }

    @Override
    public void open() throws Exception {
        super.open();
        joinCondition =
                generatedJoinCondition.newInstance(getRuntimeContext().getUserCodeClassLoader());
        joinCondition.setRuntimeContext(getRuntimeContext());
        joinCondition.open(DefaultOpenContext.INSTANCE);

        nextLeftIndex =
                getRuntimeContext()
                        .getState(
                                new ValueStateDescriptor<>(NEXT_LEFT_INDEX_STATE_NAME, Types.LONG));
        leftState =
                getRuntimeContext()
                        .getMapState(
                                new MapStateDescriptor<>(LEFT_STATE_NAME, Types.LONG, leftType));
        rightState =
                getRuntimeContext()
                        .getMapState(
                                new MapStateDescriptor<>(RIGHT_STATE_NAME, Types.LONG, rightType));
        registeredTimer =
                getRuntimeContext()
                        .getState(
                                new ValueStateDescriptor<>(
                                        REGISTERED_TIMER_STATE_NAME, Types.LONG));

        timerService =
                getInternalTimerService(TIMERS_STATE_NAME, VoidNamespaceSerializer.INSTANCE, this);

        outRow = new JoinedRowData();
        rightNullRow = new GenericRowData(rightType.toRowType().getFieldCount());
        collector = new TimestampedCollector<>(output);
        this.temporalRowTimeJoinHelper = new SyncStateTemporalRowTimeJoinHelper();
    }

    @Override
    public void processElement1(StreamRecord<RowData> element) throws Exception {
        RowData row = element.getValue();
        leftState.put(getNextLeftIndex(), row);
        registerSmallestTimer(
                this.temporalRowTimeJoinHelper.getLeftTime(
                        row)); // Timer to emit and clean up the state

        registerProcessingCleanupTimer();
    }

    @Override
    public void processElement2(StreamRecord<RowData> element) throws Exception {
        RowData row = element.getValue();

        long rowTime = this.temporalRowTimeJoinHelper.getRightTime(row);
        rightState.put(rowTime, row);
        registerSmallestTimer(rowTime); // Timer to clean up the state

        registerProcessingCleanupTimer();
    }

    @Override
    public void onEventTime(InternalTimer<Object, VoidNamespace> timer) throws Exception {
        registeredTimer.clear();
        long lastUnprocessedTime =
                this.temporalRowTimeJoinHelper.emitResultAndCleanUpState(
                        timerService.currentWatermark());
        if (lastUnprocessedTime < Long.MAX_VALUE) {
            registerTimer(lastUnprocessedTime);
        }

        // if we have more state at any side, then update the timer, else clean it up.
        if (stateCleaningEnabled) {
            if (lastUnprocessedTime < Long.MAX_VALUE || !rightState.isEmpty()) {
                registerProcessingCleanupTimer();
            } else {
                cleanupLastTimer();
                nextLeftIndex.clear();
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (joinCondition != null) {
            joinCondition.close();
        }
        super.close();
    }

    /**
     * Removes all expired version in the versioned table's state according to current watermark.
     */
    private void cleanupExpiredVersionInState(long currentWatermark, List<RowData> rightRowsSorted)
            throws Exception {
        int i = 0;
        int indexToKeep =
                this.temporalRowTimeJoinHelper.firstIndexToKeep(currentWatermark, rightRowsSorted);
        // clean old version data that behind current watermark
        while (i < indexToKeep) {
            long rightTime = this.temporalRowTimeJoinHelper.getRightTime(rightRowsSorted.get(i));
            rightState.remove(rightTime);
            i += 1;
        }
    }

    /**
     * The method to be called when a cleanup timer fires.
     *
     * @param time The timestamp of the fired timer.
     */
    @Override
    public void cleanupState(long time) {
        leftState.clear();
        rightState.clear();
        nextLeftIndex.clear();
        registeredTimer.clear();
    }

    private void registerSmallestTimer(long timestamp) throws IOException {
        Long currentRegisteredTimer = registeredTimer.value();
        if (currentRegisteredTimer == null) {
            registerTimer(timestamp);
        } else if (currentRegisteredTimer > timestamp) {
            timerService.deleteEventTimeTimer(VoidNamespace.INSTANCE, currentRegisteredTimer);
            registerTimer(timestamp);
        }
    }

    private void registerTimer(long timestamp) throws IOException {
        registeredTimer.update(timestamp);
        timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, timestamp);
    }

    private List<RowData> getRightRowSorted(RowtimeComparator rowtimeComparator) throws Exception {
        List<RowData> rightRows = new ArrayList<>();
        for (RowData row : rightState.values()) {
            rightRows.add(row);
        }
        rightRows.sort(rowtimeComparator);
        return rightRows;
    }

    private long getNextLeftIndex() throws IOException {
        Long index = nextLeftIndex.value();
        if (index == null) {
            index = 0L;
        }
        nextLeftIndex.update(index + 1);
        return index;
    }

    @VisibleForTesting
    static String getNextLeftIndexStateName() {
        return NEXT_LEFT_INDEX_STATE_NAME;
    }

    @VisibleForTesting
    static String getRegisteredTimerStateName() {
        return REGISTERED_TIMER_STATE_NAME;
    }

    private class SyncStateTemporalRowTimeJoinHelper extends TemporalRowTimeJoinHelper {
        public SyncStateTemporalRowTimeJoinHelper() {
            super(
                    joinCondition,
                    isLeftOuterJoin,
                    rightNullRow,
                    collector,
                    outRow,
                    leftTimeAttribute,
                    rightTimeAttribute);
        }

        /**
         * @return a row time of the oldest unprocessed probe record or Long.MaxValue, if all
         *     records have been processed.
         */
        public long emitResultAndCleanUpState(long currentWatermark) throws Exception {
            List<RowData> rightRowsSorted = getRightRowSorted(rightRowtimeComparator);
            long lastUnprocessedTime = Long.MAX_VALUE;

            Iterator<Map.Entry<Long, RowData>> leftIterator = leftState.entries().iterator();
            // the output records' order should keep same with left input records arrival order
            final Map<Long, RowData> orderedLeftRecords = new TreeMap<>();

            while (leftIterator.hasNext()) {
                Map.Entry<Long, RowData> entry = leftIterator.next();
                Long leftSeq = entry.getKey();
                RowData leftRow = entry.getValue();
                long leftTime = getLeftTime(leftRow);
                if (leftTime <= currentWatermark) {
                    orderedLeftRecords.put(leftSeq, leftRow);
                    leftIterator.remove();
                } else {
                    lastUnprocessedTime = Math.min(lastUnprocessedTime, leftTime);
                }
            }
            emitTriggeredLeftRecordsInOrder(orderedLeftRecords, rightRowsSorted);
            cleanupExpiredVersionInState(currentWatermark, rightRowsSorted);
            return lastUnprocessedTime;
        }
    }
}
