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

package org.apache.flink.table.runtime.operators.deduplicate.window.processors;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.aggregate.window.buffers.WindowBuffer;
import org.apache.flink.table.runtime.operators.window.slicing.SlicingWindowProcessor;
import org.apache.flink.table.runtime.operators.window.slicing.WindowTimerService;
import org.apache.flink.table.runtime.operators.window.slicing.WindowTimerServiceImpl;
import org.apache.flink.table.runtime.operators.window.state.WindowValueState;

import java.time.ZoneId;

import static org.apache.flink.table.runtime.util.TimeWindowUtil.isWindowFired;

/** A rowtime window deduplicate processor. */
public final class RowTimeWindowDeduplicateProcessor implements SlicingWindowProcessor<Long> {
    private static final long serialVersionUID = 1L;

    private final WindowBuffer.Factory bufferFactory;
    private final TypeSerializer<RowData> inputSerializer;
    private final int windowEndIndex;
    private final ZoneId shiftTimeZone;

    // ----------------------------------------------------------------------------------------

    private transient long currentProgress;

    private transient Context<Long> ctx;

    private transient WindowTimerService<Long> windowTimerService;

    private transient WindowBuffer windowBuffer;

    /** state schema: [key, window_end, first/last record]. */
    private transient WindowValueState<Long> windowState;

    public RowTimeWindowDeduplicateProcessor(
            TypeSerializer<RowData> inputSerializer,
            WindowBuffer.Factory bufferFactory,
            int windowEndIndex,
            ZoneId shiftTimeZone) {
        this.inputSerializer = inputSerializer;
        this.bufferFactory = bufferFactory;
        this.windowEndIndex = windowEndIndex;
        this.shiftTimeZone = shiftTimeZone;
    }

    @Override
    public void open(Context<Long> context) throws Exception {
        this.ctx = context;
        final LongSerializer namespaceSerializer = LongSerializer.INSTANCE;
        ValueStateDescriptor<RowData> valueStateDescriptor =
                new ValueStateDescriptor<>("window_deduplicate", inputSerializer);
        ValueState<RowData> state =
                ctx.getKeyedStateBackend()
                        .getOrCreateKeyedState(namespaceSerializer, valueStateDescriptor);
        this.windowTimerService = new WindowTimerServiceImpl(ctx.getTimerService(), shiftTimeZone);
        this.windowState =
                new WindowValueState<>((InternalValueState<RowData, Long, RowData>) state);
        this.windowBuffer =
                bufferFactory.create(
                        ctx.getOperatorOwner(),
                        ctx.getMemoryManager(),
                        ctx.getMemorySize(),
                        ctx.getRuntimeContext(),
                        windowTimerService,
                        ctx.getKeyedStateBackend(),
                        windowState,
                        true,
                        shiftTimeZone);
        this.currentProgress = Long.MIN_VALUE;
    }

    @Override
    public void initializeWatermark(long watermark) {
        currentProgress = watermark;
    }

    @Override
    public boolean processElement(RowData key, RowData element) throws Exception {
        long sliceEnd = element.getLong(windowEndIndex);
        if (isWindowFired(sliceEnd, currentProgress, shiftTimeZone)) {
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
        windowState.clear(windowEnd);
    }

    @Override
    public void close() throws Exception {
        if (windowBuffer != null) {
            windowBuffer.close();
        }
    }

    @Override
    public TypeSerializer<Long> createWindowSerializer() {
        return LongSerializer.INSTANCE;
    }

    @Override
    public void fireWindow(Long windowEnd) throws Exception {
        RowData data = windowState.value(windowEnd);
        if (data != null) {
            ctx.output(data);
        }
    }
}
