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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.operators.aggregate.window.buffers.WindowBuffer;
import org.apache.flink.table.runtime.operators.window.MergeCallback;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowTimerService;
import org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigner;
import org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceSharedAssigner;
import org.apache.flink.table.runtime.operators.window.tvf.slicing.SlicingWindowProcessor;
import org.apache.flink.table.runtime.operators.window.tvf.slicing.SlicingWindowTimerServiceImpl;

import java.time.ZoneId;

import static org.apache.flink.table.runtime.util.TimeWindowUtil.getNextTriggerWatermark;
import static org.apache.flink.table.runtime.util.TimeWindowUtil.isWindowFired;

/** A base implementation of {@link SlicingWindowProcessor} for window aggregate. */
public abstract class AbstractSliceWindowAggProcessor extends AbstractWindowAggProcessor<Long>
        implements SlicingWindowProcessor<Long> {
    protected final WindowBuffer.Factory windowBufferFactory;
    protected final SliceAssigner sliceAssigner;
    protected final long windowInterval;

    // ----------------------------------------------------------------------------------------

    /** The next progress to trigger windows. */
    private transient long nextTriggerProgress;

    protected transient WindowBuffer windowBuffer;

    public AbstractSliceWindowAggProcessor(
            GeneratedNamespaceAggsHandleFunction<Long> genAggsHandler,
            WindowBuffer.Factory bufferFactory,
            SliceAssigner sliceAssigner,
            TypeSerializer<RowData> accSerializer,
            int indexOfCountStar,
            ZoneId shiftTimeZone) {
        super(
                genAggsHandler,
                sliceAssigner,
                accSerializer,
                sliceAssigner.isEventTime(),
                indexOfCountStar,
                shiftTimeZone);
        this.windowBufferFactory = bufferFactory;
        this.sliceAssigner = sliceAssigner;
        this.windowInterval = sliceAssigner.getSliceEndInterval();
    }

    @Override
    public void open(Context<Long> context) throws Exception {
        super.open(context);
        this.windowBuffer =
                windowBufferFactory.create(
                        ctx.getOperatorOwner(),
                        ctx.getMemoryManager(),
                        ctx.getMemorySize(),
                        ctx.getRuntimeContext(),
                        windowTimerService,
                        ctx.getKeyedStateBackend(),
                        windowState,
                        isEventTime,
                        shiftTimeZone);

        this.nextTriggerProgress = Long.MIN_VALUE;
    }

    @Override
    protected WindowTimerService<Long> getWindowTimerService() {
        return new SlicingWindowTimerServiceImpl(ctx.getTimerService(), shiftTimeZone);
    }

    @Override
    public boolean processElement(RowData key, RowData element) throws Exception {
        long sliceEnd = sliceAssigner.assignSliceEnd(element, clockService);
        if (!isEventTime) {
            // always register processing time for every element when processing time mode
            windowTimerService.registerProcessingTimeWindowTimer(sliceEnd);
        }

        if (isEventTime && isWindowFired(sliceEnd, currentProgress, shiftTimeZone)) {
            // the assigned slice has been triggered, which means current element is late,
            // but maybe not need to drop
            long lastWindowEnd = sliceAssigner.getLastWindowEnd(sliceEnd);
            if (isWindowFired(lastWindowEnd, currentProgress, shiftTimeZone)) {
                // the last window has been triggered, so the element can be dropped now
                return true;
            } else {
                windowBuffer.addElement(key, sliceStateMergeTarget(sliceEnd), element);
                // we need to register a timer for the next unfired window,
                // because this may the first time we see elements under the key
                long unfiredFirstWindow = sliceEnd;
                while (isWindowFired(unfiredFirstWindow, currentProgress, shiftTimeZone)) {
                    unfiredFirstWindow += windowInterval;
                }
                windowTimerService.registerEventTimeWindowTimer(unfiredFirstWindow);
                return false;
            }
        } else {
            // the assigned slice hasn't been triggered, accumulate into the assigned slice
            windowBuffer.addElement(key, sliceEnd, element);
            return false;
        }
    }

    /**
     * Returns the slice state target to merge the given slice into when firing windows. For
     * unshared windows, there should no merging happens, so the merge target should be just the
     * given {@code sliceToMerge}. For shared windows, the merge target should be the shared slice
     * state.
     *
     * @see SliceSharedAssigner#mergeSlices(long, MergeCallback)
     */
    protected abstract long sliceStateMergeTarget(long sliceToMerge) throws Exception;

    @Override
    public void advanceProgress(long progress) throws Exception {
        if (progress > currentProgress) {
            currentProgress = progress;
            if (currentProgress >= nextTriggerProgress) {
                // in order to buffer as much as possible data, we only need to call
                // advanceProgress() when currentWatermark may trigger window.
                // this is a good optimization when receiving late but un-dropped events, because
                // they will register small timers and normal watermark will flush the buffer
                windowBuffer.advanceProgress(currentProgress);
                nextTriggerProgress =
                        getNextTriggerWatermark(
                                currentProgress, windowInterval, shiftTimeZone, useDayLightSaving);
            }
        }
    }

    @Override
    public void prepareCheckpoint() throws Exception {
        windowBuffer.flush();
    }

    @Override
    public void clearWindow(long timerTimestamp, Long windowEnd) throws Exception {
        Iterable<Long> expires = sliceAssigner.expiredSlices(windowEnd);
        for (Long slice : expires) {
            windowState.clear(slice);
            aggregator.cleanup(slice);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (windowBuffer != null) {
            windowBuffer.close();
        }
    }

    @Override
    public TypeSerializer<Long> createWindowSerializer() {
        return LongSerializer.INSTANCE;
    }
}
