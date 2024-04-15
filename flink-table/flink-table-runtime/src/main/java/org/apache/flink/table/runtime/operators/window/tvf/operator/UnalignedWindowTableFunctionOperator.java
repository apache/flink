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

package org.apache.flink.table.runtime.operators.window.tvf.operator;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.MergingState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalMergingState;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.dataview.PerWindowStateDataViewStore;
import org.apache.flink.table.runtime.dataview.StateDataViewStore;
import org.apache.flink.table.runtime.generated.NamespaceAggsHandleFunctionBase;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.groupwindow.assigners.GroupWindowAssigner;
import org.apache.flink.table.runtime.operators.window.groupwindow.assigners.MergingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.groupwindow.internal.MergingWindowProcessFunction;
import org.apache.flink.table.runtime.operators.window.groupwindow.triggers.EventTimeTriggers;
import org.apache.flink.table.runtime.operators.window.groupwindow.triggers.ProcessingTimeTriggers;
import org.apache.flink.table.runtime.operators.window.groupwindow.triggers.Trigger;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowAggOperator;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.BiConsumerWithException;

import java.time.ZoneId;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.table.runtime.util.TimeWindowUtil.toEpochMillsForTimer;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The operator for unaligned window table function.
 *
 * <p>See more details about aligned window and unaligned window in {@link WindowAggOperator}.
 *
 * <p>Note: The operator only applies for Window TVF with set semantics (e.g SESSION) instead of row
 * semantics (e.g TUMBLE/HOP/CUMULATE).
 *
 * <p>This operator emits result at the end of window instead of per record.
 *
 * <p>This operator will not compact changelog records.
 *
 * <p>This operator will keep the original order of input records when outputting.
 */
public class UnalignedWindowTableFunctionOperator extends WindowTableFunctionOperatorBase
        implements Triggerable<RowData, TimeWindow> {

    private static final long serialVersionUID = 1L;

    private static final String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";
    private static final String LATE_ELEMENTS_DROPPED_RATE_METRIC_NAME = "lateRecordsDroppedRate";
    private static final String WATERMARK_LATENCY_METRIC_NAME = "watermarkLatency";

    private final Trigger<TimeWindow> trigger;

    private final TypeSerializer<RowData> inputSerializer;

    private final TypeSerializer<TimeWindow> windowSerializer;

    private transient InternalTimerService<TimeWindow> internalTimerService;

    // a counter to tag the order of all input streams when entering the operator
    private transient ValueState<Long> counterState;

    private transient InternalMapState<RowData, TimeWindow, Long, RowData> windowState;

    private transient TriggerContextImpl triggerContext;

    private transient MergingWindowProcessFunction<RowData, TimeWindow> windowFunction;

    private transient NamespaceAggsHandleFunctionBase<TimeWindow> windowAggregator;

    // ------------------------------------------------------------------------
    // Metrics
    // ------------------------------------------------------------------------

    private transient Counter numLateRecordsDropped;
    private transient Meter lateRecordsDroppedRate;
    private transient Gauge<Long> watermarkLatency;

    public UnalignedWindowTableFunctionOperator(
            GroupWindowAssigner<TimeWindow> windowAssigner,
            TypeSerializer<TimeWindow> windowSerializer,
            TypeSerializer<RowData> inputSerializer,
            int rowtimeIndex,
            ZoneId shiftTimeZone) {
        super(windowAssigner, rowtimeIndex, shiftTimeZone);
        this.trigger = createTrigger(windowAssigner);
        this.windowSerializer = checkNotNull(windowSerializer);
        this.inputSerializer = checkNotNull(inputSerializer);
    }

    @Override
    public void open() throws Exception {
        super.open();

        internalTimerService =
                getInternalTimerService("session-window-tvf-timers", windowSerializer, this);

        triggerContext = new TriggerContextImpl();
        triggerContext.open();

        ValueStateDescriptor<Long> counterStateDescriptor =
                new ValueStateDescriptor<>("session-window-tvf-counter", LongSerializer.INSTANCE);
        counterState = getRuntimeContext().getState(counterStateDescriptor);

        MapStateDescriptor<Long, RowData> windowStateDescriptor =
                new MapStateDescriptor<>(
                        "session-window-tvf-acc", LongSerializer.INSTANCE, inputSerializer);

        windowState =
                (InternalMapState<RowData, TimeWindow, Long, RowData>)
                        getOrCreateKeyedState(windowSerializer, windowStateDescriptor);

        windowAggregator = new DummyWindowAggregator();
        windowAggregator.open(
                new PerWindowStateDataViewStore(
                        getKeyedStateBackend(), windowSerializer, getRuntimeContext()));

        WindowContextImpl windowContext = new WindowContextImpl();

        windowFunction =
                new MergingWindowProcessFunction<>(
                        (MergingWindowAssigner<TimeWindow>) windowAssigner,
                        windowAggregator,
                        windowSerializer,
                        0);

        windowFunction.open(windowContext);

        this.numLateRecordsDropped = metrics.counter(LATE_ELEMENTS_DROPPED_METRIC_NAME);
        this.lateRecordsDroppedRate =
                metrics.meter(
                        LATE_ELEMENTS_DROPPED_RATE_METRIC_NAME,
                        new MeterView(numLateRecordsDropped));
        this.watermarkLatency =
                metrics.gauge(
                        WATERMARK_LATENCY_METRIC_NAME,
                        () -> {
                            long watermark = internalTimerService.currentWatermark();
                            if (watermark < 0) {
                                return 0L;
                            } else {
                                return internalTimerService.currentProcessingTime() - watermark;
                            }
                        });
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (windowAggregator != null) {
            windowAggregator.close();
        }
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        RowData inputRow = element.getValue();

        long timestamp;
        if (windowAssigner.isEventTime()) {
            if (inputRow.isNullAt(rowtimeIndex)) {
                // null timestamp would be dropped
                numNullRowTimeRecordsDropped.inc();
                return;
            }
            timestamp = inputRow.getTimestamp(rowtimeIndex, 3).getMillisecond();
        } else {
            timestamp = getProcessingTimeService().getCurrentProcessingTime();
        }

        // no matter if order exceeds the Long.MAX_VALUE
        Long order = counterState.value();
        if (null == order) {
            order = 0L;
        }
        counterState.update(order + 1);

        timestamp = TimeWindowUtil.toUtcTimestampMills(timestamp, shiftTimeZone);
        // the windows which the input row should be placed into
        Collection<TimeWindow> affectedWindows =
                windowFunction.assignStateNamespace(inputRow, timestamp);
        boolean isElementDropped = true;
        for (TimeWindow window : affectedWindows) {
            isElementDropped = false;
            windowState.setCurrentNamespace(window);
            windowState.put(order, inputRow);
        }

        // the actual window which the input row is belongs to
        Collection<TimeWindow> actualWindows =
                windowFunction.assignActualWindows(inputRow, timestamp);
        Preconditions.checkArgument(
                (affectedWindows.isEmpty() && actualWindows.isEmpty())
                        || (!affectedWindows.isEmpty() && !actualWindows.isEmpty()));
        for (TimeWindow window : actualWindows) {
            triggerContext.setWindow(window);
            boolean triggerResult = triggerContext.onElement(inputRow, timestamp);
            if (triggerResult) {
                emitWindowResult(window);
            }
            // clear up state
            registerCleanupTimer(window);
        }
        if (isElementDropped) {
            // markEvent will increase numLateRecordsDropped
            lateRecordsDroppedRate.markEvent();
        }
    }

    private void registerCleanupTimer(TimeWindow window) {
        long cleanupTime = getCleanupTime(window);
        if (cleanupTime == Long.MAX_VALUE) {
            // no need to clean up because we didn't set one
            return;
        }
        if (windowAssigner.isEventTime()) {
            triggerContext.registerEventTimeTimer(cleanupTime);
        } else {
            triggerContext.registerProcessingTimeTimer(cleanupTime);
        }
    }

    private void emitWindowResult(TimeWindow window) throws Exception {
        TimeWindow stateWindow = windowFunction.getStateWindow(window);
        windowState.setCurrentNamespace(stateWindow);
        Iterator<Map.Entry<Long, RowData>> iterator = windowState.iterator();
        // build a sorted map
        TreeMap<Long, RowData> sortedMap = new TreeMap<>();
        while (iterator.hasNext()) {
            Map.Entry<Long, RowData> entry = iterator.next();
            sortedMap.put(entry.getKey(), entry.getValue());
        }
        // emit the sorted map
        for (Map.Entry<Long, RowData> entry : sortedMap.entrySet()) {
            collect(entry.getValue(), Collections.singletonList(window));
        }
    }

    @Override
    public void onEventTime(InternalTimer<RowData, TimeWindow> timer) throws Exception {
        triggerContext.setWindow(timer.getNamespace());
        if (triggerContext.onEventTime(timer.getTimestamp())) {
            // fire
            emitWindowResult(triggerContext.window);
        }

        if (windowAssigner.isEventTime()) {
            windowFunction.cleanWindowIfNeeded(triggerContext.window, timer.getTimestamp());
        }
    }

    @Override
    public void onProcessingTime(InternalTimer<RowData, TimeWindow> timer) throws Exception {
        triggerContext.setWindow(timer.getNamespace());
        if (triggerContext.onProcessingTime(timer.getTimestamp())) {
            // fire
            emitWindowResult(triggerContext.window);
        }

        if (!windowAssigner.isEventTime()) {
            windowFunction.cleanWindowIfNeeded(triggerContext.window, timer.getTimestamp());
        }
    }

    /**
     * In case this leads to a value greated than {@link Long#MAX_VALUE} then a cleanup time of
     * {@link Long#MAX_VALUE} is returned.
     */
    private long getCleanupTime(TimeWindow window) {
        // In case this leads to a value greater than Long.MAX_VALUE, then a cleanup
        // time of Long.MAX_VALUE is returned.
        long cleanupTime = Math.max(0, window.maxTimestamp());
        cleanupTime = cleanupTime >= window.maxTimestamp() ? cleanupTime : Long.MAX_VALUE;
        return toEpochMillsForTimer(cleanupTime, shiftTimeZone);
    }

    private static Trigger<TimeWindow> createTrigger(
            GroupWindowAssigner<TimeWindow> windowAssigner) {
        if (windowAssigner.isEventTime()) {
            return EventTimeTriggers.afterEndOfWindow();
        } else {
            return ProcessingTimeTriggers.afterEndOfWindow();
        }
    }

    private class WindowContextImpl
            implements MergingWindowProcessFunction.MergingContext<RowData, TimeWindow> {

        @Override
        public void deleteCleanupTimer(TimeWindow window) throws Exception {
            long cleanupTime = UnalignedWindowTableFunctionOperator.this.getCleanupTime(window);
            if (cleanupTime == Long.MAX_VALUE) {
                // no need to clean up because we didn't set one
                return;
            }
            if (windowAssigner.isEventTime()) {
                triggerContext.deleteEventTimeTimer(cleanupTime);
            } else {
                triggerContext.deleteProcessingTimeTimer(cleanupTime);
            }
        }

        @Override
        public <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor)
                throws Exception {
            requireNonNull(stateDescriptor, "The state properties must not be null");
            return UnalignedWindowTableFunctionOperator.this.getPartitionedState(stateDescriptor);
        }

        @Override
        public RowData currentKey() {
            return (RowData) UnalignedWindowTableFunctionOperator.this.getCurrentKey();
        }

        @Override
        public long currentProcessingTime() {
            return internalTimerService.currentProcessingTime();
        }

        @Override
        public long currentWatermark() {
            return internalTimerService.currentWatermark();
        }

        @Override
        public ZoneId getShiftTimeZone() {
            return shiftTimeZone;
        }

        @Override
        public void clearWindowState(TimeWindow window) throws Exception {
            windowState.setCurrentNamespace(window);
            windowState.clear();
        }

        @Override
        public void clearTrigger(TimeWindow window) throws Exception {
            triggerContext.setWindow(window);
            triggerContext.clear();
        }

        @Override
        public void onMerge(TimeWindow newWindow, Collection<TimeWindow> mergedWindows)
                throws Exception {
            triggerContext.setWindow(newWindow);
            triggerContext.setMergedWindows(mergedWindows);
            triggerContext.onMerge();
        }

        @Override
        public void clearPreviousState(TimeWindow window) throws Exception {}

        @Override
        public RowData getWindowAccumulators(TimeWindow window) throws Exception {
            return null;
        }

        @Override
        public void setWindowAccumulators(TimeWindow window, RowData acc) throws Exception {}

        @Override
        public BiConsumerWithException<TimeWindow, Collection<TimeWindow>, Throwable>
                getWindowStateMergingConsumer() {
            return new MergingConsumer(windowState);
        }
    }

    private class TriggerContextImpl implements Trigger.OnMergeContext {

        private TimeWindow window;
        private Collection<TimeWindow> mergedWindows;

        public void open() throws Exception {
            trigger.open(this);
        }

        public boolean onElement(RowData row, long timestamp) throws Exception {
            return trigger.onElement(row, timestamp, window);
        }

        public boolean onProcessingTime(long time) throws Exception {
            return trigger.onProcessingTime(time, window);
        }

        public boolean onEventTime(long time) throws Exception {
            return trigger.onEventTime(time, window);
        }

        public void onMerge() throws Exception {
            trigger.onMerge(window, this);
        }

        public void setWindow(TimeWindow window) {
            this.window = window;
        }

        public void setMergedWindows(Collection<TimeWindow> mergedWindows) {
            this.mergedWindows = mergedWindows;
        }

        @Override
        public long getCurrentProcessingTime() {
            return internalTimerService.currentProcessingTime();
        }

        @Override
        public long getCurrentWatermark() {
            return internalTimerService.currentWatermark();
        }

        @Override
        public void registerProcessingTimeTimer(long time) {
            internalTimerService.registerProcessingTimeTimer(window, time);
        }

        @Override
        public void registerEventTimeTimer(long time) {
            internalTimerService.registerEventTimeTimer(window, time);
        }

        @Override
        public void deleteProcessingTimeTimer(long time) {
            internalTimerService.deleteProcessingTimeTimer(window, time);
        }

        @Override
        public void deleteEventTimeTimer(long time) {
            internalTimerService.deleteEventTimeTimer(window, time);
        }

        @Override
        public ZoneId getShiftTimeZone() {
            return shiftTimeZone;
        }

        public void clear() throws Exception {
            trigger.clear(window);
        }

        @Override
        public <S extends MergingState<?, ?>> void mergePartitionedState(
                StateDescriptor<S, ?> stateDescriptor) {
            if (mergedWindows != null && !mergedWindows.isEmpty()) {
                try {
                    State state =
                            UnalignedWindowTableFunctionOperator.this.getOrCreateKeyedState(
                                    windowSerializer, stateDescriptor);
                    if (state instanceof InternalMergingState) {
                        ((InternalMergingState<RowData, TimeWindow, ?, ?, ?>) state)
                                .mergeNamespaces(window, mergedWindows);
                    } else {
                        throw new IllegalArgumentException(
                                "The given state descriptor does not refer to a mergeable state (MergingState)");
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Error while merging state.", e);
                }
            }
        }

        @Override
        public MetricGroup getMetricGroup() {
            return UnalignedWindowTableFunctionOperator.this.getMetricGroup();
        }

        @Override
        public <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) {
            try {
                return UnalignedWindowTableFunctionOperator.this.getPartitionedState(
                        window, windowSerializer, stateDescriptor);
            } catch (Exception e) {
                throw new RuntimeException("Could not retrieve state", e);
            }
        }
    }

    private static class MergingConsumer
            implements BiConsumerWithException<TimeWindow, Collection<TimeWindow>, Throwable> {

        private final InternalMapState<RowData, TimeWindow, Long, RowData> windowState;

        public MergingConsumer(InternalMapState<RowData, TimeWindow, Long, RowData> windowState) {
            this.windowState = windowState;
        }

        @Override
        public void accept(
                TimeWindow stateWindowResult, Collection<TimeWindow> stateWindowsToBeMerged)
                throws Throwable {
            for (TimeWindow mergedWindow : stateWindowsToBeMerged) {
                windowState.setCurrentNamespace(mergedWindow);
                Iterator<Map.Entry<Long, RowData>> iterator = windowState.iterator();
                windowState.setCurrentNamespace(stateWindowResult);
                while (iterator.hasNext()) {
                    Map.Entry<Long, RowData> entry = iterator.next();
                    windowState.put(entry.getKey(), entry.getValue());
                }
            }
        }
    }

    /**
     * A dummy window aggregator is used to reuse the same logic in legacy group session window
     * operator.
     *
     * <p>Instead of using window aggregator, we use a custom {@link MergingConsumer} to merge the
     * accumulators in state.
     */
    private static class DummyWindowAggregator
            implements NamespaceAggsHandleFunctionBase<TimeWindow> {

        private final IllegalStateException thrown =
                new IllegalStateException(
                        "The function should not be called in DummyWindowAggregator");

        @Override
        public void open(StateDataViewStore store) throws Exception {}

        @Override
        public void setAccumulators(TimeWindow namespace, RowData accumulators) throws Exception {
            throw thrown;
        }

        @Override
        public void accumulate(RowData inputRow) throws Exception {
            throw thrown;
        }

        @Override
        public void retract(RowData inputRow) throws Exception {
            throw thrown;
        }

        @Override
        public void merge(TimeWindow namespace, RowData otherAcc) throws Exception {
            throw thrown;
        }

        @Override
        public RowData createAccumulators() throws Exception {
            throw thrown;
        }

        @Override
        public RowData getAccumulators() throws Exception {
            throw thrown;
        }

        @Override
        public void cleanup(TimeWindow namespace) throws Exception {
            throw thrown;
        }

        @Override
        public void close() throws Exception {}
    }

    @VisibleForTesting
    public Counter getNumLateRecordsDropped() {
        return numLateRecordsDropped;
    }

    @VisibleForTesting
    public Gauge<Long> getWatermarkLatency() {
        return watermarkLatency;
    }
}
