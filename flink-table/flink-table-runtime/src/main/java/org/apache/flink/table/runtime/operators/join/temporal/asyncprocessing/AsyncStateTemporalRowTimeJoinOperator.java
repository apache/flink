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

package org.apache.flink.table.runtime.operators.join.temporal.asyncprocessing;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.state.v2.MapState;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.runtime.state.v2.MapStateDescriptor;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.runtime.state.v2.ValueStateDescriptor;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The operator for temporal join (FOR SYSTEM_TIME AS OF o.rowtime) on row time in async state, it has no
 * limitation about message types of the left input and right input, this means the operator deals
 * changelog well.
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
public class AsyncStateTemporalRowTimeJoinOperator extends BaseTwoInputAsyncStateStreamOperatorWithStateRetention {

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

    private transient AsyncStateTemporalRowTimeJoinHelper temporalRowTimeJoinHelper;

    public AsyncStateTemporalRowTimeJoinOperator(
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
                        .getValueState(
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
                        .getValueState(
                                new ValueStateDescriptor<>(
                                        REGISTERED_TIMER_STATE_NAME, Types.LONG));

        timerService =
                getInternalTimerService(TIMERS_STATE_NAME, VoidNamespaceSerializer.INSTANCE, this);

        outRow = new JoinedRowData();
        rightNullRow = new GenericRowData(rightType.toRowType().getFieldCount());
        collector = new TimestampedCollector<>(output);
        this.temporalRowTimeJoinHelper = new AsyncStateTemporalRowTimeJoinHelper();
    }

    @Override
    public void processElement1(StreamRecord<RowData> element) throws Exception {
        RowData row = element.getValue();
        getNextLeftIndex().thenAccept(nextLeftIndex -> {
            StateFuture<Void> putFuture = leftState.asyncPut(nextLeftIndex, row);
            putFuture.thenAccept(
                    VOID -> {
                        // Timer to emit and clean up the state
                        registerSmallestTimer(this.temporalRowTimeJoinHelper.getLeftTime(row));
                        registerProcessingCleanupTimer();
                    }
            );
        });
    }

    @Override
    public void processElement2(StreamRecord<RowData> element) throws Exception {
        RowData row = element.getValue();

        long rowTime = this.temporalRowTimeJoinHelper.getRightTime(row);
        StateFuture<Void> putFuture = rightState.asyncPut(rowTime, row);
        putFuture.thenAccept(
                VOID -> {
                    // Timer to clean up the state
                    registerSmallestTimer(rowTime);
                    registerProcessingCleanupTimer();
                }
        );
    }

    @Override
    public void onEventTime(InternalTimer<Object, VoidNamespace> timer) throws Exception {
        StateFuture<Void> clearFuture = registeredTimer.asyncClear();
        clearFuture.thenAccept(
                VOID -> {
                    StateFuture<Long> lastUnprocessedTimeFuture = this.temporalRowTimeJoinHelper.emitResultAndCleanUpState(
                            timerService.currentWatermark());
                    lastUnprocessedTimeFuture.thenAccept(lastUnprocessedTime -> {
                        if (lastUnprocessedTime < Long.MAX_VALUE) {
                            registerTimer(lastUnprocessedTime);
                        }
                        // if we have more state at any side, then update the timer, else clean it up.
                        if (stateCleaningEnabled) {
                            if (lastUnprocessedTime < Long.MAX_VALUE || !rightState.isEmpty()) {
                                registerProcessingCleanupTimer();
                            } else {
                                cleanupLastTimer();
                                nextLeftIndex.asyncClear();
                            }
                        }
                    });
                }
        );
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
    private StateFuture<Void> cleanupExpiredVersionInState(long currentWatermark, List<RowData> rightRowsSorted)
            throws Exception {
        int i = 0;
        int indexToKeep = this.temporalRowTimeJoinHelper.firstIndexToKeep(currentWatermark, rightRowsSorted);
        List<StateFuture<Void>> removeFutureList = new ArrayList<>();
        // clean old version data that behind current watermark
        while (i < indexToKeep) {
            long rightTime = this.temporalRowTimeJoinHelper.getRightTime(rightRowsSorted.get(i));
            removeFutureList.add(rightState.asyncRemove(rightTime));
            i += 1;
        }
        return StateFutureUtils.combineAll(removeFutureList).thenApply(VOID -> null);
    }

    /**
     * The method to be called when a cleanup timer fires.
     *
     * @param time The timestamp of the fired timer.
     */
    @Override
    public StateFuture<Void> cleanupState(long time) {
        return StateFutureUtils.combineAll(
                        Arrays.asList(
                                leftState.asyncClear(),
                                rightState.asyncClear(),
                                nextLeftIndex.asyncClear(),
                                registeredTimer.asyncClear()))
                .thenApply(VOID -> null);
    }

    private StateFuture<Void> registerSmallestTimer(long timestamp) throws IOException {
        return registeredTimer.asyncValue().thenAccept(currentRegisteredTimer -> {
            if (currentRegisteredTimer == null) {
                registerTimer(timestamp);
            } else if (currentRegisteredTimer > timestamp) {
                timerService.deleteEventTimeTimer(VoidNamespace.INSTANCE, currentRegisteredTimer);
                registerTimer(timestamp);
            }
        });
    }

    private StateFuture<Void> registerTimer(long timestamp) throws IOException {
        return registeredTimer.asyncUpdate(timestamp).thenCompose(VOID -> {
            timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, timestamp);
            return StateFutureUtils.completedVoidFuture();
        });
    }

    private StateFuture<List<RowData>> getRightRowSorted(RowtimeComparator rowtimeComparator) throws Exception {
        List<RowData> rightRows = new ArrayList<>();
        return rightState
                .asyncValues()
                .thenCompose(
                        stateIterator ->
                                stateIterator.onNext(
                                        rowData -> {
                                            rightRows.add(rowData);
                                        })
                )
                .thenApply(VOID -> {
                    rightRows.sort(rowtimeComparator);
                    return rightRows;
                });
    }

    private StateFuture<Long> getNextLeftIndex() throws IOException {
        return nextLeftIndex.asyncValue().thenCompose(index -> {
            long currentIndex = (index != null) ? index : 0L;
            return nextLeftIndex.asyncUpdate(currentIndex + 1).thenApply(VOID -> currentIndex);
        });
    }

    @VisibleForTesting
    static String getNextLeftIndexStateName() {
        return NEXT_LEFT_INDEX_STATE_NAME;
    }

    @VisibleForTesting
    static String getRegisteredTimerStateName() {
        return REGISTERED_TIMER_STATE_NAME;
    }

    private class AsyncStateTemporalRowTimeJoinHelper extends TemporalRowTimeJoinHelper {
        public AsyncStateTemporalRowTimeJoinHelper() {
            super(joinCondition,
                    isLeftOuterJoin,
                    rightNullRow,
                    collector,
                    outRow,
                    leftTimeAttribute,
                    rightTimeAttribute);
        }

        /**
         * @return a row time of the oldest unprocessed probe record or Long.MaxValue, if all records
         *     have been processed.
         */
        public StateFuture<Long> emitResultAndCleanUpState(long currentWatermark) throws Exception {
            StateFuture<List<RowData>> rightRowsSortedFuture = getRightRowSorted(rightRowtimeComparator);

            return rightRowsSortedFuture.thenCompose(rightRowsSorted -> {
                return leftState.asyncEntries().thenCompose(leftEntriesIteratorFuture -> {
                    final AtomicLong lastUnprocessedTime = new AtomicLong(Long.MAX_VALUE);
                    // the output records' order should keep same with left input records arrival order
                    final Map<Long, RowData> orderedLeftRecords = new TreeMap<>();
                    return leftEntriesIteratorFuture.onNext(entry -> {
                        Long leftSeq = entry.getKey();
                        RowData leftRow = entry.getValue();
                        long leftTime = getLeftTime(leftRow);
                        if (leftTime <= currentWatermark) {
                            orderedLeftRecords.put(leftSeq, leftRow);
                            leftState.asyncRemove(leftSeq);
                        } else {
                            lastUnprocessedTime.updateAndGet(prev -> Math.min(prev, leftTime));
                        }
                    }).thenCompose(VOID -> {
                        emitTriggeredLeftRecordsInOrder(orderedLeftRecords, rightRowsSorted);
                        return cleanupExpiredVersionInState(currentWatermark, rightRowsSorted);
                    }).thenCompose(VOID -> {
                        return StateFutureUtils.completedFuture(lastUnprocessedTime.get());
                    });
                });
            });
        }
    }
}
