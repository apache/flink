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

package org.apache.flink.table.runtime.operators.aggregate.asyncwindow.buffers;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.operators.window.async.tvf.combines.AsyncStateRecordsCombiner;
import org.apache.flink.table.runtime.operators.window.async.tvf.state.AsyncStateKeyContext;
import org.apache.flink.table.runtime.operators.window.async.tvf.state.WindowAsyncState;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowTimerService;
import org.apache.flink.table.runtime.typeutils.AbstractRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.PagedTypeSerializer;
import org.apache.flink.table.runtime.typeutils.WindowKeySerializer;
import org.apache.flink.table.runtime.util.KeyValueIterator;
import org.apache.flink.table.runtime.util.WindowKey;
import org.apache.flink.table.runtime.util.collections.binary.BytesMap;
import org.apache.flink.table.runtime.util.collections.binary.WindowBytesMultiMap;

import javax.annotation.Nullable;

import java.io.EOFException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.flink.table.runtime.util.AsyncStateUtils.REUSABLE_VOID_STATE_FUTURE;
import static org.apache.flink.table.runtime.util.TimeWindowUtil.isWindowFired;

/**
 * An implementation of {@link AsyncStateWindowBuffer} that buffers input elements in a {@link
 * WindowBytesMultiMap} and combines buffered elements into async state when flushing.
 */
public final class AsyncStateRecordsWindowBuffer implements AsyncStateWindowBuffer {

    private final AsyncStateRecordsCombiner combineFunction;
    private final WindowBytesMultiMap recordsBuffer;
    private final WindowKey reuseWindowKey;
    private final AbstractRowDataSerializer<RowData> recordSerializer;
    private final ZoneId shiftTimeZone;
    private final RecordEqualiser keyEqualiser;
    private final AsyncStateKeyContext keyContext;

    private long minSliceEnd = Long.MAX_VALUE;

    public AsyncStateRecordsWindowBuffer(
            Object operatorOwner,
            MemoryManager memoryManager,
            long memorySize,
            AsyncStateRecordsCombiner combineFunction,
            PagedTypeSerializer<RowData> keySer,
            AbstractRowDataSerializer<RowData> inputSer,
            RecordEqualiser keyEqualiser,
            AsyncStateKeyContext keyContext,
            ZoneId shiftTimeZone) {
        this.combineFunction = combineFunction;
        this.recordsBuffer =
                new WindowBytesMultiMap(
                        operatorOwner, memoryManager, memorySize, keySer, inputSer.getArity());
        this.recordSerializer = inputSer;
        this.keyEqualiser = keyEqualiser;
        this.keyContext = keyContext;
        this.reuseWindowKey = new WindowKeySerializer(keySer).createInstance();
        this.shiftTimeZone = shiftTimeZone;
    }

    @Override
    public StateFuture<Void> addElement(RowData dataKey, long sliceEnd, RowData element)
            throws Exception {
        StateFuture<Void> resultFuture = REUSABLE_VOID_STATE_FUTURE;

        // track the lowest trigger time, if watermark exceeds the trigger time,
        // it means there are some elements in the buffer belong to a window going to be fired,
        // and we need to flush the buffer into state for firing.
        minSliceEnd = Math.min(sliceEnd, minSliceEnd);

        reuseWindowKey.replace(sliceEnd, dataKey);
        BytesMap.LookupInfo<WindowKey, Iterator<RowData>> lookup =
                recordsBuffer.lookup(reuseWindowKey);
        try {
            recordsBuffer.append(lookup, recordSerializer.toBinaryRow(element));
        } catch (EOFException e) {
            // buffer is full, flush it to state
            resultFuture = flush(dataKey);
            // remember to add the input element again
            addElement(dataKey, sliceEnd, element);
        }
        return resultFuture;
    }

    @Override
    public StateFuture<Void> advanceProgress(@Nullable RowData currentKey, long progress)
            throws Exception {
        if (isWindowFired(minSliceEnd, progress, shiftTimeZone)) {
            // there should be some window to be fired, flush buffer to state first
            return flush(currentKey);
        }
        return REUSABLE_VOID_STATE_FUTURE;
    }

    @Override
    public StateFuture<Void> flush(@Nullable RowData currentKey) throws Exception {
        StateFuture<Void> flushFuture = REUSABLE_VOID_STATE_FUTURE;
        if (recordsBuffer.getNumKeys() > 0) {
            // due to the delayed processing of async requests, all objects cannot be reused, so
            // they must be copied.
            KeyValueIterator<WindowKey, Iterator<RowData>> entryIterator =
                    recordsBuffer.getEntryIterator(true);
            while (entryIterator.advanceNext()) {
                WindowKey windowKey = entryIterator.getKey();
                long window = windowKey.getWindow();
                List<RowData> allData = itertorToList(entryIterator.getValue());
                if (currentKey != null && keyEqualiser.equals(currentKey, windowKey.getKey())) {
                    flushFuture = combineFunction.asyncCombine(window, allData.iterator());
                } else {
                    // no need to wait for combining the records excluding current key
                    keyContext.asyncProcessWithKey(
                            windowKey.getKey(),
                            () -> combineFunction.asyncCombine(window, allData.iterator()));
                }
            }
            recordsBuffer.reset();
            // reset trigger time
            minSliceEnd = Long.MAX_VALUE;
        }
        return flushFuture;
    }

    /**
     * Convert iterator to list.
     *
     * <p>This may put some pressure on heap memory since the data in the iterator comes from
     * managed memory. We can optimize this method once we come up with a better approach.
     */
    private List<RowData> itertorToList(Iterator<RowData> records) {
        List<RowData> list = new ArrayList<>();
        while (records.hasNext()) {
            list.add(records.next());
        }
        return list;
    }

    @Override
    public void close() throws Exception {
        recordsBuffer.free();
        combineFunction.close();
    }

    // ------------------------------------------------------------------------------------------
    // Factory
    // ------------------------------------------------------------------------------------------

    /**
     * Factory to create {@link AsyncStateRecordsWindowBuffer} with {@link
     * AsyncStateRecordsCombiner.Factory}.
     */
    public static final class Factory implements AsyncStateWindowBuffer.Factory {

        private static final long serialVersionUID = 1L;

        private final PagedTypeSerializer<RowData> keySer;
        private final AbstractRowDataSerializer<RowData> inputSer;
        private final AsyncStateRecordsCombiner.Factory factory;
        private final GeneratedRecordEqualiser generatedKeyEqualiser;

        public Factory(
                PagedTypeSerializer<RowData> keySer,
                AbstractRowDataSerializer<RowData> inputSer,
                AsyncStateRecordsCombiner.Factory combinerFactory,
                GeneratedRecordEqualiser generatedKeyEqualiser) {
            this.keySer = keySer;
            this.inputSer = inputSer;
            this.factory = combinerFactory;
            this.generatedKeyEqualiser = generatedKeyEqualiser;
        }

        @Override
        public AsyncStateWindowBuffer create(
                Object operatorOwner,
                MemoryManager memoryManager,
                long memorySize,
                RuntimeContext runtimeContext,
                WindowTimerService<Long> timerService,
                AsyncStateKeyContext keyContext,
                WindowAsyncState<Long> windowState,
                boolean isEventTime,
                ZoneId shiftTimeZone)
                throws Exception {
            AsyncStateRecordsCombiner combiner =
                    factory.createRecordsCombiner(
                            runtimeContext, timerService, windowState, isEventTime);
            RecordEqualiser keyEqualiser =
                    generatedKeyEqualiser.newInstance(runtimeContext.getUserCodeClassLoader());
            return new AsyncStateRecordsWindowBuffer(
                    operatorOwner,
                    memoryManager,
                    memorySize,
                    combiner,
                    keySer,
                    inputSer,
                    keyEqualiser,
                    keyContext,
                    shiftTimeZone);
        }
    }
}
