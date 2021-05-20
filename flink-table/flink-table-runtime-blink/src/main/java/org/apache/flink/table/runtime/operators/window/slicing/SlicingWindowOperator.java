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

package org.apache.flink.table.runtime.operators.window.slicing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
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
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.operators.aggregate.window.processors.SliceSharedWindowAggProcessor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link SlicingWindowOperator} implements an optimized processing for aligned windows which
 * can apply the slicing optimization. The core idea of slicing optimization is to divide all
 * elements from a data stream into a finite number of non-overlapping chunks (a.k.a. slices).
 *
 * <h3>Concept of Aligned Window and Unaligned Window</h3>
 *
 * <p>We divide windows into 2 categories: Aligned Windows and Unaligned Windows.
 *
 * <p>Aligned Windows are windows have predetermined window boundaries and windows can be divided
 * into finite number of non-overlapping chunks. The boundary of an aligned window is determined
 * independently from the time characteristic of the data stream, or messages it receives. For
 * example, hopping (sliding) window is an aligned window as the window boundaries are predetermined
 * based on the window size and slide. Aligned windows include tumbling, hopping, cumulative
 * windows.
 *
 * <p>Unaligned Windows are windows determined dynamically based on elements. For example, session
 * window is an unaligned window as the window boundaries are determined based on the messages
 * timestamps and their correlations. Currently, unaligned windows include session window only.
 *
 * <p>Because aligned windows can be divided into finite number of non-overlapping chunks (a.k.a.
 * slices), which can apply efficient processing to share intermediate results.
 *
 * <h3>Concept of Slice</h3>
 *
 * <p>Dividing a window of aligned windows into a finite number of non-overlapping chunks, where the
 * chunks are slices. It has the following properties:
 *
 * <ul>
 *   <li>An element must only belong to a single slice.
 *   <li>Slices are non-overlapping, i.e. S_i and S_j should not have any shared elements if i != j.
 *   <li>A window is consist of a finite number of slices.
 * </ul>
 *
 * <h3>Abstraction of Slicing Window Operator</h3>
 *
 * <p>A slicing window operator is a simple wrap of {@link SlicingWindowProcessor}. It delegates all
 * the important methods to the underlying processor, where the processor can have different
 * implementation for aggregate and topk or others.
 *
 * <p>A {@link SlicingWindowProcessor} usually leverages the {@link SliceAssigner} to assign slices
 * and calculate based on the slices. See {@link SliceSharedWindowAggProcessor} as an example.
 *
 * <p>Note: since {@link SlicingWindowProcessor} leverages slicing optimization for aligned windows,
 * therefore, it doesn't support unaligned windows, e.g. session window.
 *
 * <p>Note: currently, {@link SlicingWindowOperator} doesn't support early-fire and late-arrival.
 * Thus late elements (elements belong to emitted windows) will be simply dropped.
 */
@Internal
public final class SlicingWindowOperator<K, W> extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData>, Triggerable<K, W>, KeyContext {

    private static final long serialVersionUID = 1L;

    private static final String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";
    private static final String LATE_ELEMENTS_DROPPED_RATE_METRIC_NAME = "lateRecordsDroppedRate";
    private static final String WATERMARK_LATENCY_METRIC_NAME = "watermarkLatency";

    /** The concrete window operator implementation. */
    private final SlicingWindowProcessor<W> windowProcessor;

    // ------------------------------------------------------------------------

    /** This is used for emitting elements with a given timestamp. */
    protected transient TimestampedCollector<RowData> collector;

    /** Flag to prevent duplicate function.close() calls in close() and dispose(). */
    private transient boolean functionsClosed = false;

    /** The service to register timers. */
    private transient InternalTimerService<W> internalTimerService;

    /** The tracked processing time triggered last time. */
    private transient long lastTriggeredProcessingTime;

    // ------------------------------------------------------------------------
    // Metrics
    // ------------------------------------------------------------------------

    private transient Counter numLateRecordsDropped;
    private transient Meter lateRecordsDroppedRate;
    private transient Gauge<Long> watermarkLatency;

    public SlicingWindowOperator(SlicingWindowProcessor<W> windowProcessor) {
        this.windowProcessor = windowProcessor;
        setChainingStrategy(ChainingStrategy.ALWAYS);
    }

    @Override
    public void open() throws Exception {
        super.open();
        functionsClosed = false;

        lastTriggeredProcessingTime = Long.MIN_VALUE;
        collector = new TimestampedCollector<>(output);
        collector.eraseTimestamp();

        internalTimerService =
                getInternalTimerService(
                        "window-timers", windowProcessor.createWindowSerializer(), this);

        windowProcessor.open(
                new WindowProcessorContext<>(
                        getContainingTask(),
                        getContainingTask().getEnvironment().getMemoryManager(),
                        computeMemorySize(),
                        internalTimerService,
                        getKeyedStateBackend(),
                        collector,
                        getRuntimeContext()));

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
    public void close() throws Exception {
        super.close();
        collector = null;
        functionsClosed = true;
        windowProcessor.close();
    }

    @Override
    public void dispose() throws Exception {
        super.dispose();
        collector = null;
        if (!functionsClosed) {
            functionsClosed = true;
            windowProcessor.close();
        }
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        RowData inputRow = element.getValue();
        RowData currentKey = (RowData) getCurrentKey();
        boolean isElementDropped = windowProcessor.processElement(currentKey, inputRow);
        if (isElementDropped) {
            // markEvent will increase numLateRecordsDropped
            lateRecordsDroppedRate.markEvent();
        }
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        windowProcessor.advanceProgress(mark.getTimestamp());
        super.processWatermark(mark);
    }

    @Override
    public void onEventTime(InternalTimer<K, W> timer) throws Exception {
        onTimer(timer);
    }

    @Override
    public void onProcessingTime(InternalTimer<K, W> timer) throws Exception {
        if (timer.getTimestamp() > lastTriggeredProcessingTime) {
            // similar to the watermark advance,
            // we need to notify WindowProcessor first to flush buffer into state
            lastTriggeredProcessingTime = timer.getTimestamp();
            windowProcessor.advanceProgress(timer.getTimestamp());
            // timers registered in advanceProgress() should always be smaller than current timer
            // so, it should be safe to trigger current timer straightforwards.
        }
        onTimer(timer);
    }

    private void onTimer(InternalTimer<K, W> timer) throws Exception {
        setCurrentKey(timer.getKey());
        W window = timer.getNamespace();
        windowProcessor.fireWindow(window);
        windowProcessor.clearWindow(window);
        // we don't need to clear window timers,
        // because there should only be one timer for each window now, which is current timer.
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        windowProcessor.prepareCheckpoint();
    }

    /** Context implementation for {@link SlicingWindowProcessor.Context}. */
    private static final class WindowProcessorContext<W>
            implements SlicingWindowProcessor.Context<W> {

        private final Object operatorOwner;
        private final MemoryManager memoryManager;
        private final long memorySize;
        private final InternalTimerService<W> timerService;
        private final KeyedStateBackend<RowData> keyedStateBackend;
        private final Output<RowData> collector;
        private final RuntimeContext runtimeContext;

        private WindowProcessorContext(
                Object operatorOwner,
                MemoryManager memoryManager,
                long memorySize,
                InternalTimerService<W> timerService,
                KeyedStateBackend<RowData> keyedStateBackend,
                Output<RowData> collector,
                RuntimeContext runtimeContext) {
            this.operatorOwner = operatorOwner;
            this.memoryManager = memoryManager;
            this.memorySize = memorySize;
            this.timerService = timerService;
            this.keyedStateBackend = checkNotNull(keyedStateBackend);
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
        public KeyedStateBackend<RowData> getKeyedStateBackend() {
            return keyedStateBackend;
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
