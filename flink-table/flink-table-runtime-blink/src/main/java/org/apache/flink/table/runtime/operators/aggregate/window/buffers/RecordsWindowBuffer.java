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

package org.apache.flink.table.runtime.operators.aggregate.window.buffers;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.combines.RecordsCombiner;
import org.apache.flink.table.runtime.operators.window.slicing.WindowTimerService;
import org.apache.flink.table.runtime.operators.window.state.WindowState;
import org.apache.flink.table.runtime.typeutils.AbstractRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.PagedTypeSerializer;
import org.apache.flink.table.runtime.typeutils.WindowKeySerializer;
import org.apache.flink.table.runtime.util.KeyValueIterator;
import org.apache.flink.table.runtime.util.WindowKey;
import org.apache.flink.table.runtime.util.collections.binary.BytesMap.LookupInfo;
import org.apache.flink.table.runtime.util.collections.binary.WindowBytesMultiMap;
import org.apache.flink.util.Collector;

import java.io.EOFException;
import java.time.ZoneId;
import java.util.Iterator;

import static org.apache.flink.table.runtime.util.StateConfigUtil.isStateImmutableInStateBackend;
import static org.apache.flink.table.runtime.util.TimeWindowUtil.isWindowFired;

/**
 * An implementation of {@link WindowBuffer} that buffers input elements in a {@link
 * WindowBytesMultiMap} and combines buffered elements into state when flushing.
 */
public final class RecordsWindowBuffer implements WindowBuffer {

    private final RecordsCombiner combineFunction;
    private final WindowBytesMultiMap recordsBuffer;
    private final WindowKey reuseWindowKey;
    private final AbstractRowDataSerializer<RowData> recordSerializer;
    private final ZoneId shiftTimeZone;
    // copy key and input record if necessary(e.g., heap state backend),
    // because key and record are reused.
    private final boolean requiresCopy;

    private long minSliceEnd = Long.MAX_VALUE;

    public RecordsWindowBuffer(
            Object operatorOwner,
            MemoryManager memoryManager,
            long memorySize,
            RecordsCombiner combineFunction,
            PagedTypeSerializer<RowData> keySer,
            AbstractRowDataSerializer<RowData> inputSer,
            boolean requiresCopy,
            ZoneId shiftTimeZone) {
        this.combineFunction = combineFunction;
        this.recordsBuffer =
                new WindowBytesMultiMap(
                        operatorOwner, memoryManager, memorySize, keySer, inputSer.getArity());
        this.recordSerializer = inputSer;
        this.reuseWindowKey = new WindowKeySerializer(keySer).createInstance();
        this.requiresCopy = requiresCopy;
        this.shiftTimeZone = shiftTimeZone;
    }

    @Override
    public void addElement(RowData key, long sliceEnd, RowData element) throws Exception {
        // track the lowest trigger time, if watermark exceeds the trigger time,
        // it means there are some elements in the buffer belong to a window going to be fired,
        // and we need to flush the buffer into state for firing.
        minSliceEnd = Math.min(sliceEnd, minSliceEnd);

        reuseWindowKey.replace(sliceEnd, key);
        LookupInfo<WindowKey, Iterator<RowData>> lookup = recordsBuffer.lookup(reuseWindowKey);
        try {
            recordsBuffer.append(lookup, recordSerializer.toBinaryRow(element));
        } catch (EOFException e) {
            // buffer is full, flush it to state
            flush();
            // remember to add the input element again
            addElement(key, sliceEnd, element);
        }
    }

    @Override
    public void advanceProgress(long progress) throws Exception {
        if (isWindowFired(minSliceEnd, progress, shiftTimeZone)) {
            // there should be some window to be fired, flush buffer to state first
            flush();
        }
    }

    @Override
    public void flush() throws Exception {
        if (recordsBuffer.getNumKeys() > 0) {
            KeyValueIterator<WindowKey, Iterator<RowData>> entryIterator =
                    recordsBuffer.getEntryIterator(requiresCopy);
            while (entryIterator.advanceNext()) {
                combineFunction.combine(entryIterator.getKey(), entryIterator.getValue());
            }
            recordsBuffer.reset();
            // reset trigger time
            minSliceEnd = Long.MAX_VALUE;
        }
    }

    @Override
    public void close() throws Exception {
        recordsBuffer.free();
        combineFunction.close();
    }

    // ------------------------------------------------------------------------------------------
    // Factory
    // ------------------------------------------------------------------------------------------

    /** Factory to create {@link RecordsWindowBuffer} with {@link RecordsCombiner.Factory}. */
    public static final class Factory implements WindowBuffer.Factory {

        private static final long serialVersionUID = 1L;

        private final PagedTypeSerializer<RowData> keySer;
        private final AbstractRowDataSerializer<RowData> inputSer;
        private final RecordsCombiner.Factory factory;

        public Factory(
                PagedTypeSerializer<RowData> keySer,
                AbstractRowDataSerializer<RowData> inputSer,
                RecordsCombiner.Factory combinerFactory) {
            this.keySer = keySer;
            this.inputSer = inputSer;
            this.factory = combinerFactory;
        }

        @Override
        public WindowBuffer create(
                Object operatorOwner,
                MemoryManager memoryManager,
                long memorySize,
                RuntimeContext runtimeContext,
                WindowTimerService<Long> timerService,
                KeyedStateBackend<RowData> stateBackend,
                WindowState<Long> windowState,
                boolean isEventTime,
                ZoneId shiftTimeZone)
                throws Exception {
            RecordsCombiner combiner =
                    factory.createRecordsCombiner(
                            runtimeContext, timerService, stateBackend, windowState, isEventTime);
            boolean requiresCopy = !isStateImmutableInStateBackend(stateBackend);
            return new RecordsWindowBuffer(
                    operatorOwner,
                    memoryManager,
                    memorySize,
                    combiner,
                    keySer,
                    inputSer,
                    requiresCopy,
                    shiftTimeZone);
        }
    }

    /** Factory to create {@link RecordsWindowBuffer} with {@link RecordsCombiner.LocalFactory}. */
    public static final class LocalFactory implements WindowBuffer.LocalFactory {

        private static final long serialVersionUID = 1L;

        private final PagedTypeSerializer<RowData> keySer;
        private final AbstractRowDataSerializer<RowData> inputSer;
        private final RecordsCombiner.LocalFactory localFactory;

        public LocalFactory(
                PagedTypeSerializer<RowData> keySer,
                AbstractRowDataSerializer<RowData> inputSer,
                RecordsCombiner.LocalFactory localFactory) {
            this.keySer = keySer;
            this.inputSer = inputSer;
            this.localFactory = localFactory;
        }

        @Override
        public WindowBuffer create(
                Object operatorOwner,
                MemoryManager memoryManager,
                long memorySize,
                RuntimeContext runtimeContext,
                Collector<RowData> collector,
                ZoneId shiftTimeZone)
                throws Exception {
            RecordsCombiner combiner =
                    localFactory.createRecordsCombiner(runtimeContext, collector);
            return new RecordsWindowBuffer(
                    operatorOwner,
                    memoryManager,
                    memorySize,
                    combiner,
                    keySer,
                    inputSer,
                    false,
                    shiftTimeZone);
        }
    }
}
