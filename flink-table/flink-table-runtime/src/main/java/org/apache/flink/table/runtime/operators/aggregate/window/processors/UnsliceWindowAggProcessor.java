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

package org.apache.flink.table.runtime.operators.aggregate.window.processors;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.MergingState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalMergingState;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.groupwindow.internal.MergingWindowProcessFunction;
import org.apache.flink.table.runtime.operators.window.groupwindow.triggers.EventTimeTriggers;
import org.apache.flink.table.runtime.operators.window.groupwindow.triggers.ProcessingTimeTriggers;
import org.apache.flink.table.runtime.operators.window.groupwindow.triggers.Trigger;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowTimerService;
import org.apache.flink.table.runtime.operators.window.tvf.unslicing.UnsliceAssigner;
import org.apache.flink.table.runtime.operators.window.tvf.unslicing.UnslicingWindowProcessor;
import org.apache.flink.table.runtime.operators.window.tvf.unslicing.UnslicingWindowTimerServiceImpl;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.BiConsumerWithException;

import java.time.ZoneId;
import java.util.Collection;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.table.runtime.util.TimeWindowUtil.toEpochMillsForTimer;

/**
 * An window aggregate processor implementation which works for {@link UnsliceAssigner}, e.g.
 * session windows.
 */
public class UnsliceWindowAggProcessor extends AbstractWindowAggProcessor<TimeWindow>
        implements UnslicingWindowProcessor<TimeWindow> {

    private final UnsliceAssigner<TimeWindow> unsliceAssigner;

    private final Trigger<TimeWindow> trigger;

    // ----------------------------------------------------------------------------------------

    private transient MetricGroup metrics;

    protected transient MergingWindowProcessFunction<RowData, TimeWindow> windowFunction;

    private transient TriggerContextImpl triggerContext;

    public UnsliceWindowAggProcessor(
            GeneratedNamespaceAggsHandleFunction<TimeWindow> genAggsHandler,
            UnsliceAssigner<TimeWindow> unsliceAssigner,
            TypeSerializer<RowData> accSerializer,
            int indexOfCountStar,
            ZoneId shiftTimeZone) {
        super(
                genAggsHandler,
                unsliceAssigner,
                accSerializer,
                unsliceAssigner.isEventTime(),
                indexOfCountStar,
                shiftTimeZone);
        this.unsliceAssigner = unsliceAssigner;
        if (isEventTime) {
            trigger = EventTimeTriggers.afterEndOfWindow();
        } else {
            trigger = ProcessingTimeTriggers.afterEndOfWindow();
        }
    }

    @Override
    public void open(Context<TimeWindow> context) throws Exception {
        super.open(context);
        this.metrics = context.getRuntimeContext().getMetricGroup();
        this.windowFunction =
                new MergingWindowProcessFunction<>(
                        unsliceAssigner.getMergingWindowAssigner(),
                        aggregator,
                        unsliceAssigner
                                .getMergingWindowAssigner()
                                .getWindowSerializer(new ExecutionConfig()),
                        // TODO support allowedLateness
                        0L);

        triggerContext = new TriggerContextImpl();
        triggerContext.open();

        WindowContextImpl windowContext = new WindowContextImpl();

        this.windowFunction.open(windowContext);
    }

    @Override
    public boolean processElement(RowData key, RowData element) throws Exception {
        // the windows which the input row should be placed into
        Optional<TimeWindow> affectedWindowOp =
                unsliceAssigner.assignStateNamespace(element, clockService, windowFunction);
        boolean isElementDropped = true;
        if (affectedWindowOp.isPresent()) {
            TimeWindow affectedWindow = affectedWindowOp.get();
            isElementDropped = false;

            RowData acc = windowState.value(affectedWindow);
            if (acc == null) {
                acc = aggregator.createAccumulators();
            }
            aggregator.setAccumulators(affectedWindow, acc);

            if (RowDataUtil.isAccumulateMsg(element)) {
                aggregator.accumulate(element);
            } else {
                aggregator.retract(element);
            }
            acc = aggregator.getAccumulators();
            windowState.update(affectedWindow, acc);
        }

        // the actual window which the input row is belongs to
        Optional<TimeWindow> actualWindowOp =
                unsliceAssigner.assignActualWindow(element, clockService, windowFunction);
        Preconditions.checkArgument(
                (affectedWindowOp.isPresent() && actualWindowOp.isPresent())
                        || (!affectedWindowOp.isPresent() && !actualWindowOp.isPresent()));

        if (actualWindowOp.isPresent()) {
            TimeWindow actualWindow = actualWindowOp.get();
            triggerContext.setWindow(actualWindow);
            // register a timer for the window to fire and clean up
            long triggerTime = toEpochMillsForTimer(actualWindow.maxTimestamp(), shiftTimeZone);
            if (isEventTime) {
                triggerContext.registerEventTimeTimer(triggerTime);
            } else {
                triggerContext.registerProcessingTimeTimer(triggerTime);
            }
        }
        return isElementDropped;
    }

    @Override
    public void fireWindow(long timerTimestamp, TimeWindow window) throws Exception {
        windowFunction.prepareAggregateAccumulatorForEmit(window);
        RowData aggResult = aggregator.getValue(window);
        triggerContext.setWindow(window);
        final boolean isFired;
        if (isEventTime) {
            isFired = triggerContext.onEventTime(timerTimestamp);
        } else {
            isFired = triggerContext.onProcessingTime(timerTimestamp);
        }
        // we shouldn't emit an empty window
        if (isFired && !emptySupplier.get()) {
            collect(aggResult);
        }
    }

    @Override
    public void clearWindow(long timerTimestamp, TimeWindow window) throws Exception {
        windowFunction.cleanWindowIfNeeded(window, timerTimestamp);
    }

    @Override
    public void advanceProgress(long progress) throws Exception {}

    @Override
    public void prepareCheckpoint() throws Exception {}

    @Override
    public TypeSerializer<TimeWindow> createWindowSerializer() {
        return unsliceAssigner
                .getMergingWindowAssigner()
                .getWindowSerializer(new ExecutionConfig());
    }

    @Override
    protected WindowTimerService<TimeWindow> getWindowTimerService() {
        return new UnslicingWindowTimerServiceImpl(ctx.getTimerService(), shiftTimeZone);
    }

    private class WindowContextImpl
            implements MergingWindowProcessFunction.MergingContext<RowData, TimeWindow> {

        @Override
        public long currentProcessingTime() {
            return ctx.getTimerService().currentProcessingTime();
        }

        @Override
        public long currentWatermark() {
            return ctx.getTimerService().currentWatermark();
        }

        @Override
        public ZoneId getShiftTimeZone() {
            return shiftTimeZone;
        }

        @Override
        public RowData getWindowAccumulators(TimeWindow window) throws Exception {
            return windowState.value(window);
        }

        @Override
        public void setWindowAccumulators(TimeWindow window, RowData acc) throws Exception {
            windowState.update(window, acc);
        }

        @Override
        public void clearWindowState(TimeWindow window) throws Exception {
            windowState.clear(window);
            aggregator.cleanup(window);
        }

        @Override
        public void clearPreviousState(TimeWindow window) throws Exception {}

        @Override
        public void clearTrigger(TimeWindow window) throws Exception {
            triggerContext.setWindow(window);
            triggerContext.clear();
        }

        @Override
        public void deleteCleanupTimer(TimeWindow window) throws Exception {
            long cleanupTime = toEpochMillsForTimer(window.maxTimestamp(), shiftTimeZone);
            if (cleanupTime == Long.MAX_VALUE) {
                // no need to clean up because we didn't set one
                return;
            }
            if (unsliceAssigner.isEventTime()) {
                triggerContext.deleteEventTimeTimer(cleanupTime);
            } else {
                triggerContext.deleteProcessingTimeTimer(cleanupTime);
            }
        }

        @Override
        public void onMerge(TimeWindow newWindow, Collection<TimeWindow> mergedWindows)
                throws Exception {
            triggerContext.setWindow(newWindow);
            triggerContext.setMergedWindows(mergedWindows);
            triggerContext.onMerge();
        }

        @Override
        public <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor)
                throws Exception {
            requireNonNull(stateDescriptor, "The state properties must not be null");
            return ctx.getKeyedStateBackend()
                    .getPartitionedState(
                            VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            stateDescriptor);
        }

        @Override
        public RowData currentKey() {
            return ctx.getKeyedStateBackend().getCurrentKey();
        }

        @Override
        public BiConsumerWithException<TimeWindow, Collection<TimeWindow>, Throwable>
                getWindowStateMergingConsumer() {
            return new MergingWindowProcessFunction.DefaultAccMergingConsumer<>(this, aggregator);
        }
    }

    private class TriggerContextImpl implements Trigger.OnMergeContext {

        private TimeWindow window;

        private Collection<TimeWindow> mergedWindows;

        public void open() throws Exception {
            trigger.open(this);
        }

        @Override
        public MetricGroup getMetricGroup() {
            return metrics;
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
            return ctx.getTimerService().currentProcessingTime();
        }

        @Override
        public long getCurrentWatermark() {
            return ctx.getTimerService().currentWatermark();
        }

        @Override
        public void registerProcessingTimeTimer(long time) {
            ctx.getTimerService().registerProcessingTimeTimer(window, time);
        }

        @Override
        public void registerEventTimeTimer(long time) {
            ctx.getTimerService().registerEventTimeTimer(window, time);
        }

        @Override
        public void deleteProcessingTimeTimer(long time) {
            ctx.getTimerService().deleteProcessingTimeTimer(window, time);
        }

        @Override
        public void deleteEventTimeTimer(long time) {
            ctx.getTimerService().deleteEventTimeTimer(window, time);
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
                            ctx.getKeyedStateBackend()
                                    .getOrCreateKeyedState(
                                            createWindowSerializer(), stateDescriptor);
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
        public <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) {
            try {
                return ctx.getKeyedStateBackend()
                        .getPartitionedState(
                                VoidNamespace.INSTANCE,
                                VoidNamespaceSerializer.INSTANCE,
                                stateDescriptor);
            } catch (Exception e) {
                throw new RuntimeException("Could not retrieve state", e);
            }
        }
    }
}
