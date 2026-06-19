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

package org.apache.flink.table.runtime.operators.window.async.tvf.common;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.v2.ListState;
import org.apache.flink.api.common.state.v2.ListStateDescriptor;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.AsyncStateTableStreamOperator;
import org.apache.flink.table.runtime.operators.window.async.tvf.state.AsyncStateKeyContext;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowAggOperator;
import org.apache.flink.table.runtime.util.AsyncStateUtils;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A processor that processes elements for windows with async state api.
 *
 * <p>Different with {@link WindowAggOperator}, this class mainly handles processing related to
 * async state.
 *
 * <p>You can see more at {@link WindowAggOperator}.
 */
@Internal
public final class AsyncStateWindowAggOperator<K, W> extends AsyncStateTableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData>, Triggerable<K, W>, KeyContext {

    private static final long serialVersionUID = 1L;

    private static final String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";
    private static final String LATE_ELEMENTS_DROPPED_RATE_METRIC_NAME = "lateRecordsDroppedRate";
    private static final String WATERMARK_LATENCY_METRIC_NAME = "watermarkLatency";

    /** The concrete window operator implementation. */
    private final AsyncStateWindowProcessor<W> windowProcessor;

    private final boolean isEventTime;

    // ------------------------------------------------------------------------

    /** This is used for emitting elements with a given timestamp. */
    private transient TimestampedCollector<RowData> collector;

    /** The service to register timers. */
    private transient InternalTimerService<W> internalTimerService;

    /** The tracked processing time triggered last time. */
    private transient long lastTriggeredProcessingTime;

    /** The operator state to store watermark. */
    private transient ListState<Long> watermarkState;

    // ------------------------------------------------------------------------
    // Metrics
    // ------------------------------------------------------------------------

    private transient Counter numLateRecordsDropped;
    private transient Meter lateRecordsDroppedRate;

    private transient Gauge<Long> watermarkLatency;

    public AsyncStateWindowAggOperator(
            AsyncStateWindowProcessor<W> windowProcessor, boolean isEventTime) {
        this.windowProcessor = windowProcessor;
        this.isEventTime = isEventTime;
    }

    @Override
    public void open() throws Exception {
        super.open();

        lastTriggeredProcessingTime = Long.MIN_VALUE;
        collector = new TimestampedCollector<>(output);
        collector.eraseTimestamp();

        internalTimerService =
                getInternalTimerService(
                        "window-timers", windowProcessor.createWindowSerializer(), this);
        // Restore the watermark of timerService to prevent expired data from being treated as
        // not expired when flushWindowBuffer is executed.
        internalTimerService.initializeWatermark(currentWatermark);

        windowProcessor.open(
                new WindowProcessorAsyncStateContext<>(
                        getContainingTask(),
                        getContainingTask().getEnvironment().getMemoryManager(),
                        computeMemorySize(),
                        internalTimerService,
                        new AsyncStateKeyContext(this, getAsyncKeyedStateBackend()),
                        collector,
                        getRuntimeContext()));
        // initialize watermark
        windowProcessor.initializeWatermark(currentWatermark);

        // metrics
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
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        ListStateDescriptor<Long> watermarkStateDesc =
                new ListStateDescriptor<>("watermark", LongSerializer.INSTANCE);
        this.watermarkState =
                ((OperatorStateBackend) context.getOperatorStateStore())
                        .getUnionListState(watermarkStateDesc);
        if (context.isRestored()) {
            AtomicLong minWatermark = new AtomicLong(Long.MAX_VALUE);
            watermarkState
                    .asyncGet()
                    .thenCompose(
                            its ->
                                    its.onNext(
                                            watermark -> {
                                                minWatermark.set(
                                                        Math.min(watermark, minWatermark.get()));
                                            }))
                    .thenAccept(
                            VOID -> {
                                if (minWatermark.get() != Long.MAX_VALUE) {
                                    this.currentWatermark = minWatermark.get();
                                }
                            });
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        this.watermarkState.asyncUpdate(Collections.singletonList(currentWatermark));
    }

    @Override
    public void close() throws Exception {
        super.close();
        collector = null;
        windowProcessor.close();
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        RowData inputRow = element.getValue();
        RowData currentKey = (RowData) getCurrentKey();
        windowProcessor
                .processElement(currentKey, inputRow)
                .thenAccept(
                        isElementDropped -> {
                            if (isElementDropped) {
                                // markEvent will increase numLateRecordsDropped
                                lateRecordsDroppedRate.markEvent();
                            }
                        });
    }

    @Override
    public Watermark preProcessWatermark(Watermark mark) throws Exception {
        if (mark.getTimestamp() > currentWatermark) {
            // If this is a proctime window, progress should not be advanced by watermark, or it'll
            // disturb timer-based processing
            if (isEventTime) {
                windowProcessor.advanceProgress(null, mark.getTimestamp());
            }
            return super.preProcessWatermark(mark);
        } else {
            return super.preProcessWatermark(new Watermark(currentWatermark));
        }
    }

    @Override
    public void onEventTime(InternalTimer<K, W> timer) throws Exception {
        onTimer(timer);
    }

    @Override
    public void onProcessingTime(InternalTimer<K, W> timer) throws Exception {
        StateFuture<Void> advanceFuture = AsyncStateUtils.REUSABLE_VOID_STATE_FUTURE;
        if (timer.getTimestamp() > lastTriggeredProcessingTime) {
            // similar to the watermark advance,
            // we need to notify WindowProcessor first to flush buffer into state
            lastTriggeredProcessingTime = timer.getTimestamp();
            advanceFuture =
                    windowProcessor.advanceProgress(
                            (RowData) getCurrentKey(), timer.getTimestamp());
            // timers registered in advanceProgress() should always be smaller than current timer
            // so, it should be safe to trigger current timer straightforwards.
        }
        advanceFuture.thenAccept(VOID -> onTimer(timer));
    }

    private void onTimer(InternalTimer<K, W> timer) throws Exception {
        W window = timer.getNamespace();
        windowProcessor
                .fireWindow(timer.getTimestamp(), window)
                .thenAccept(VOID -> windowProcessor.clearWindow(timer.getTimestamp(), window));
        // we don't need to clear window timers,
        // because there should only be one timer for each window now, which is current timer.
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        super.prepareSnapshotPreBarrier(checkpointId);
        windowProcessor.prepareCheckpoint();
        drainStateRequests();
    }

    /** Context implementation for {@link AsyncStateWindowProcessor.AsyncStateContext}. */
    private static final class WindowProcessorAsyncStateContext<W>
            implements AsyncStateWindowProcessor.AsyncStateContext<W> {

        private final Object operatorOwner;
        private final MemoryManager memoryManager;
        private final long memorySize;
        private final InternalTimerService<W> timerService;
        private final AsyncStateKeyContext asyncStateKeyContext;
        private final Output<RowData> collector;
        private final RuntimeContext runtimeContext;

        private WindowProcessorAsyncStateContext(
                Object operatorOwner,
                MemoryManager memoryManager,
                long memorySize,
                InternalTimerService<W> timerService,
                AsyncStateKeyContext asyncStateKeyContext,
                Output<RowData> collector,
                RuntimeContext runtimeContext) {
            this.operatorOwner = operatorOwner;
            this.memoryManager = memoryManager;
            this.memorySize = memorySize;
            this.timerService = timerService;
            this.asyncStateKeyContext = checkNotNull(asyncStateKeyContext);
            this.collector = checkNotNull(collector);
            this.runtimeContext = checkNotNull(runtimeContext);
        }

        @Override
        public Object getOperatorOwner() {
            return operatorOwner;
        }

        @Override
        public MemoryManager getMemoryManager() {
            return memoryManager;
        }

        @Override
        public long getMemorySize() {
            return memorySize;
        }

        @Override
        public AsyncStateKeyContext getAsyncKeyContext() {
            return asyncStateKeyContext;
        }

        @Override
        public InternalTimerService<W> getTimerService() {
            return timerService;
        }

        @Override
        public void output(RowData result) {
            collector.collect(result);
        }

        @Override
        public RuntimeContext getRuntimeContext() {
            return runtimeContext;
        }
    }

    // ------------------------------------------------------------------------------
    // Visible For Testing
    // ------------------------------------------------------------------------------

    @VisibleForTesting
    public Counter getNumLateRecordsDropped() {
        return numLateRecordsDropped;
    }

    @VisibleForTesting
    public Gauge<Long> getWatermarkLatency() {
        return watermarkLatency;
    }
}
