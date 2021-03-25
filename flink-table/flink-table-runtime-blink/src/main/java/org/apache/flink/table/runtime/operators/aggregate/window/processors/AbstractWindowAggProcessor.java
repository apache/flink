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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.dataview.PerWindowStateDataViewStore;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.generated.NamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.operators.aggregate.window.buffers.WindowBuffer;
import org.apache.flink.table.runtime.operators.aggregate.window.combines.WindowCombineFunction;
import org.apache.flink.table.runtime.operators.window.slicing.ClockService;
import org.apache.flink.table.runtime.operators.window.slicing.SliceAssigner;
import org.apache.flink.table.runtime.operators.window.slicing.SlicingWindowProcessor;
import org.apache.flink.table.runtime.operators.window.state.WindowValueState;

/** A base implementation of {@link SlicingWindowProcessor} for window aggregate. */
public abstract class AbstractWindowAggProcessor implements SlicingWindowProcessor<Long> {
    private static final long serialVersionUID = 1L;

    protected final GeneratedNamespaceAggsHandleFunction<Long> genAggsHandler;
    protected final WindowBuffer.Factory windowBufferFactory;
    protected final WindowCombineFunction.Factory combineFactory;
    protected final SliceAssigner sliceAssigner;
    protected final TypeSerializer<RowData> accSerializer;
    protected final boolean isEventTime;

    // ----------------------------------------------------------------------------------------

    protected transient long currentProgress;

    protected transient Context<Long> ctx;

    protected transient ClockService clockService;

    protected transient InternalTimerService<Long> timerService;

    protected transient NamespaceAggsHandleFunction<Long> aggregator;

    protected transient WindowBuffer windowBuffer;

    /** state schema: [key, window_end, accumulator]. */
    protected transient WindowValueState<Long> windowState;

    protected transient JoinedRowData reuseOutput;

    public AbstractWindowAggProcessor(
            GeneratedNamespaceAggsHandleFunction<Long> genAggsHandler,
            WindowBuffer.Factory bufferFactory,
            WindowCombineFunction.Factory combinerFactory,
            SliceAssigner sliceAssigner,
            TypeSerializer<RowData> accSerializer) {
        this.genAggsHandler = genAggsHandler;
        this.windowBufferFactory = bufferFactory;
        this.combineFactory = combinerFactory;
        this.sliceAssigner = sliceAssigner;
        this.accSerializer = accSerializer;
        this.isEventTime = sliceAssigner.isEventTime();
    }

    @Override
    public void open(Context<Long> context) throws Exception {
        this.ctx = context;
        final LongSerializer namespaceSerializer = LongSerializer.INSTANCE;
        ValueState<RowData> state =
                ctx.getKeyedStateBackend()
                        .getOrCreateKeyedState(
                                namespaceSerializer,
                                new ValueStateDescriptor<>("window-aggs", accSerializer));
        this.windowState =
                new WindowValueState<>((InternalValueState<RowData, Long, RowData>) state);
        this.clockService = ClockService.of(ctx.getTimerService());
        this.timerService = ctx.getTimerService();
        this.aggregator =
                genAggsHandler.newInstance(ctx.getRuntimeContext().getUserCodeClassLoader());
        this.aggregator.open(
                new PerWindowStateDataViewStore(
                        ctx.getKeyedStateBackend(), namespaceSerializer, ctx.getRuntimeContext()));
        final WindowCombineFunction combineFunction =
                combineFactory.create(
                        ctx.getRuntimeContext(),
                        ctx.getTimerService(),
                        ctx.getKeyedStateBackend(),
                        windowState,
                        isEventTime);
        this.windowBuffer =
                windowBufferFactory.create(
                        ctx.getOperatorOwner(),
                        ctx.getMemoryManager(),
                        ctx.getMemorySize(),
                        combineFunction);

        this.reuseOutput = new JoinedRowData();
        this.currentProgress = Long.MIN_VALUE;
    }

    @Override
    public boolean processElement(RowData key, RowData element) throws Exception {
        long sliceEnd = sliceAssigner.assignSliceEnd(element, clockService);
        if (!isEventTime) {
            // always register processing time for every element when processing time mode
            timerService.registerProcessingTimeTimer(sliceEnd, sliceEnd - 1);
        }
        if (isEventTime && sliceEnd - 1 <= currentProgress) {
            // element is late and should be dropped
            return true;
        }
        windowBuffer.addElement(key, sliceEnd, element);
        return false;
    }

    @Override
    public void advanceProgress(long progress) throws Exception {
        if (progress > currentProgress) {
            currentProgress = progress;
            windowBuffer.advanceProgress(currentProgress);
        }
    }

    @Override
    public void prepareCheckpoint() throws Exception {
        windowBuffer.flush();
    }

    @Override
    public void clearWindow(Long windowEnd) throws Exception {
        Iterable<Long> expires = sliceAssigner.expiredSlices(windowEnd);
        for (Long slice : expires) {
            windowState.clear(slice);
            aggregator.cleanup(slice);
        }
    }

    @Override
    public void close() throws Exception {
        if (aggregator != null) {
            aggregator.close();
        }
        if (windowBuffer != null) {
            windowBuffer.close();
        }
    }

    @Override
    public TypeSerializer<Long> createWindowSerializer() {
        return LongSerializer.INSTANCE;
    }

    protected void collect(RowData aggResult) {
        reuseOutput.replace(ctx.getKeyedStateBackend().getCurrentKey(), aggResult);
        ctx.output(reuseOutput);
    }
}
