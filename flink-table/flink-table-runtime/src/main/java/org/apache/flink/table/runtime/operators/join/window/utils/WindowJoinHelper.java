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

package org.apache.flink.table.runtime.operators.join.window.utils;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.JoinConditionWithNullFilters;
import org.apache.flink.table.runtime.operators.join.window.WindowJoinOperator;
import org.apache.flink.table.runtime.operators.join.window.asyncprocessing.AsyncStateWindowJoinOperator;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowTimerService;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.function.BiConsumerWithException;

import javax.annotation.Nullable;

import java.time.ZoneId;
import java.util.IdentityHashMap;

import static org.apache.flink.table.runtime.util.TimeWindowUtil.isWindowFired;

/**
 * A helper to do the window join operations for {@link WindowJoinOperator} and {@link
 * AsyncStateWindowJoinOperator}.
 */
public abstract class WindowJoinHelper {

    private static final String LEFT_LATE_ELEMENTS_DROPPED_METRIC_NAME =
            "leftNumLateRecordsDropped";
    private static final String LEFT_LATE_ELEMENTS_DROPPED_RATE_METRIC_NAME =
            "leftLateRecordsDroppedRate";
    private static final String RIGHT_LATE_ELEMENTS_DROPPED_METRIC_NAME =
            "rightNumLateRecordsDropped";
    private static final String RIGHT_LATE_ELEMENTS_DROPPED_RATE_METRIC_NAME =
            "rightLateRecordsDroppedRate";
    private static final String WATERMARK_LATENCY_METRIC_NAME = "watermarkLatency";

    private final ZoneId shiftTimeZone;

    private final WindowTimerService<Long> windowTimerService;

    protected final RowDataSerializer leftSerializer;

    protected final RowDataSerializer rightSerializer;

    protected final JoinConditionWithNullFilters joinCondition;

    /** This is used for emitting elements with a given timestamp. */
    protected final TimestampedCollector<RowData> collector;

    private final WindowJoinProcessor windowJoinProcessor;

    // ------------------------------------------------------------------------
    // Metrics
    // ------------------------------------------------------------------------

    private Meter leftLateRecordsDroppedRate;
    private Meter rightLateRecordsDroppedRate;

    public WindowJoinHelper(
            RowDataSerializer leftSerializer,
            RowDataSerializer rightSerializer,
            ZoneId shiftTimeZone,
            WindowTimerService<Long> windowTimerService,
            JoinConditionWithNullFilters joinCondition,
            TimestampedCollector<RowData> collector,
            FlinkJoinType joinType) {
        this.leftSerializer = leftSerializer;
        this.rightSerializer = rightSerializer;
        this.shiftTimeZone = shiftTimeZone;
        this.windowTimerService = windowTimerService;
        this.joinCondition = joinCondition;
        this.collector = collector;

        this.windowJoinProcessor = getWindowJoinProcessor(joinType);
    }

    public void registerMetric(OperatorMetricGroup metrics) {
        // register metrics
        Counter leftNumLateRecordsDropped = metrics.counter(LEFT_LATE_ELEMENTS_DROPPED_METRIC_NAME);
        this.leftLateRecordsDroppedRate =
                metrics.meter(
                        LEFT_LATE_ELEMENTS_DROPPED_RATE_METRIC_NAME,
                        new MeterView(leftNumLateRecordsDropped));
        Counter rightNumLateRecordsDropped =
                metrics.counter(RIGHT_LATE_ELEMENTS_DROPPED_METRIC_NAME);
        this.rightLateRecordsDroppedRate =
                metrics.meter(
                        RIGHT_LATE_ELEMENTS_DROPPED_RATE_METRIC_NAME,
                        new MeterView(rightNumLateRecordsDropped));
        metrics.gauge(
                WATERMARK_LATENCY_METRIC_NAME,
                () -> {
                    long watermark = windowTimerService.currentWatermark();
                    if (watermark < 0) {
                        return 0L;
                    } else {
                        return windowTimerService.currentProcessingTime() - watermark;
                    }
                });
    }

    public void processElement(
            StreamRecord<RowData> element,
            int windowEndIndex,
            Meter lateRecordsDroppedRate,
            BiConsumerWithException<Long, RowData, Exception> accStateConsumer)
            throws Exception {
        RowData inputRow = element.getValue();
        long windowEnd = inputRow.getLong(windowEndIndex);
        if (isWindowFired(windowEnd, windowTimerService.currentWatermark(), shiftTimeZone)) {
            // element is late and should be dropped
            lateRecordsDroppedRate.markEvent();
            return;
        }
        if (RowDataUtil.isAccumulateMsg(inputRow)) {
            accStateConsumer.accept(windowEnd, inputRow);
        } else {
            // Window join could not handle retraction input stream
            throw new UnsupportedOperationException(
                    "This is a bug and should not happen. Please file an issue.");
        }
        // always register time for every element
        windowTimerService.registerEventTimeWindowTimer(windowEnd);
    }

    public void joinAndClear(
            long windowEnd,
            @Nullable Iterable<RowData> leftRecords,
            @Nullable Iterable<RowData> rightRecords)
            throws Exception {
        windowJoinProcessor.doJoin(leftRecords, rightRecords);
        // clear state
        if (leftRecords != null) {
            clearState(windowEnd, true);
        }
        if (rightRecords != null) {
            clearState(windowEnd, false);
        }
    }

    public Meter getLeftLateRecordsDroppedRate() {
        return leftLateRecordsDroppedRate;
    }

    public Meter getRightLateRecordsDroppedRate() {
        return rightLateRecordsDroppedRate;
    }

    public abstract void clearState(long windowEnd, boolean isLeft) throws Exception;

    private WindowJoinProcessor getWindowJoinProcessor(FlinkJoinType joinType) {
        switch (joinType) {
            case INNER:
                return new InnerWindowJoinProcessor();
            case SEMI:
                return new SemiAntiWindowJoinProcessor(false);
            case ANTI:
                return new SemiAntiWindowJoinProcessor(true);
            case LEFT:
                return new LeftOuterWindowJoinProcessor();
            case RIGHT:
                return new RightOuterWindowJoinProcessor();
            case FULL:
                return new FullOuterWindowJoinProcessor();
            default:
                throw new IllegalArgumentException("Invalid join type: " + joinType);
        }
    }

    /**
     * A processor to do the different logic for different {@link FlinkJoinType}.
     *
     * <p>TODO FLINK-37106 consider extracting common methods in different WindowJoinProcessor
     */
    private interface WindowJoinProcessor {

        void doJoin(
                @Nullable Iterable<RowData> leftRecords, @Nullable Iterable<RowData> rightRecords);
    }

    private class SemiAntiWindowJoinProcessor implements WindowJoinProcessor {

        private final boolean isAntiJoin;

        public SemiAntiWindowJoinProcessor(boolean isAntiJoin) {
            this.isAntiJoin = isAntiJoin;
        }

        @Override
        public void doJoin(
                @Nullable Iterable<RowData> leftRecords, @Nullable Iterable<RowData> rightRecords) {
            if (leftRecords == null) {
                return;
            }
            if (rightRecords == null) {
                if (isAntiJoin) {
                    for (RowData leftRecord : leftRecords) {
                        collector.collect(leftRecord);
                    }
                }
                return;
            }
            for (RowData leftRecord : leftRecords) {
                boolean matches = false;
                for (RowData rightRecord : rightRecords) {
                    if (joinCondition.apply(leftRecord, rightRecord)) {
                        matches = true;
                        break;
                    }
                }
                if (matches) {
                    if (!isAntiJoin) {
                        // emit left record if there are matched rows on the other side
                        collector.collect(leftRecord);
                    }
                } else {
                    if (isAntiJoin) {
                        // emit left record if there is no matched row on the other side
                        collector.collect(leftRecord);
                    }
                }
            }
        }
    }

    private class InnerWindowJoinProcessor implements WindowJoinProcessor {

        private final JoinedRowData outRow = new JoinedRowData();

        @Override
        public void doJoin(
                @Nullable Iterable<RowData> leftRecords, @Nullable Iterable<RowData> rightRecords) {
            if (leftRecords == null || rightRecords == null) {
                return;
            }
            for (RowData leftRecord : leftRecords) {
                for (RowData rightRecord : rightRecords) {
                    if (joinCondition.apply(leftRecord, rightRecord)) {
                        outRow.setRowKind(RowKind.INSERT);
                        outRow.replace(leftRecord, rightRecord);
                        collector.collect(outRow);
                    }
                }
            }
        }
    }

    private abstract class AbstractOuterWindowJoinProcessor implements WindowJoinProcessor {

        private final RowData leftNullRow = new GenericRowData(leftSerializer.getArity());
        private final RowData rightNullRow = new GenericRowData(rightSerializer.getArity());
        private final JoinedRowData outRow = new JoinedRowData();

        protected void outputNullPadding(RowData row, boolean isLeft) {
            if (isLeft) {
                outRow.replace(row, rightNullRow);
            } else {
                outRow.replace(leftNullRow, row);
            }
            outRow.setRowKind(RowKind.INSERT);
            collector.collect(outRow);
        }

        protected void outputNullPadding(Iterable<RowData> rows, boolean isLeft) {
            for (RowData row : rows) {
                outputNullPadding(row, isLeft);
            }
        }

        protected void output(RowData inputRow, RowData otherRow, boolean inputIsLeft) {
            if (inputIsLeft) {
                outRow.replace(inputRow, otherRow);
            } else {
                outRow.replace(otherRow, inputRow);
            }
            outRow.setRowKind(RowKind.INSERT);
            collector.collect(outRow);
        }
    }

    private class LeftOuterWindowJoinProcessor extends AbstractOuterWindowJoinProcessor {

        @Override
        public void doJoin(
                @Nullable Iterable<RowData> leftRecords, @Nullable Iterable<RowData> rightRecords) {
            if (leftRecords == null) {
                return;
            }
            if (rightRecords == null) {
                outputNullPadding(leftRecords, true);
            } else {
                for (RowData leftRecord : leftRecords) {
                    boolean matches = false;
                    for (RowData rightRecord : rightRecords) {
                        if (joinCondition.apply(leftRecord, rightRecord)) {
                            output(leftRecord, rightRecord, true);
                            matches = true;
                        }
                    }
                    if (!matches) {
                        // padding null for left side
                        outputNullPadding(leftRecord, true);
                    }
                }
            }
        }
    }

    private class RightOuterWindowJoinProcessor extends AbstractOuterWindowJoinProcessor {

        @Override
        public void doJoin(
                @Nullable Iterable<RowData> leftRecords, @Nullable Iterable<RowData> rightRecords) {
            if (rightRecords == null) {
                return;
            }
            if (leftRecords == null) {
                outputNullPadding(rightRecords, false);
            } else {
                for (RowData rightRecord : rightRecords) {
                    boolean matches = false;
                    for (RowData leftRecord : leftRecords) {
                        if (joinCondition.apply(leftRecord, rightRecord)) {
                            output(leftRecord, rightRecord, true);
                            matches = true;
                        }
                    }
                    if (!matches) {
                        outputNullPadding(rightRecord, false);
                    }
                }
            }
        }
    }

    private class FullOuterWindowJoinProcessor extends AbstractOuterWindowJoinProcessor {

        @Override
        public void doJoin(
                @Nullable Iterable<RowData> leftRecords, @Nullable Iterable<RowData> rightRecords) {
            if (leftRecords == null && rightRecords == null) {
                return;
            }
            if (rightRecords == null) {
                outputNullPadding(leftRecords, true);
            } else if (leftRecords == null) {
                outputNullPadding(rightRecords, false);
            } else {
                IdentityHashMap<RowData, Boolean> emittedRightRecords = new IdentityHashMap<>();
                for (RowData leftRecord : leftRecords) {
                    boolean matches = false;
                    for (RowData rightRecord : rightRecords) {
                        if (joinCondition.apply(leftRecord, rightRecord)) {
                            output(leftRecord, rightRecord, true);
                            matches = true;
                            emittedRightRecords.put(rightRecord, Boolean.TRUE);
                        }
                    }
                    // padding null for left side
                    if (!matches) {
                        outputNullPadding(leftRecord, true);
                    }
                }
                // padding null for never emitted right side
                for (RowData rightRecord : rightRecords) {
                    if (!emittedRightRecords.containsKey(rightRecord)) {
                        outputNullPadding(rightRecord, false);
                    }
                }
            }
        }
    }
}
