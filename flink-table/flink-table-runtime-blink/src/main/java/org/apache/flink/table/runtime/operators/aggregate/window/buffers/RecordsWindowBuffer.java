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

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.aggregate.window.combines.WindowCombineFunction;
import org.apache.flink.table.runtime.typeutils.AbstractRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.PagedTypeSerializer;
import org.apache.flink.table.runtime.typeutils.WindowKeySerializer;
import org.apache.flink.table.runtime.util.KeyValueIterator;
import org.apache.flink.table.runtime.util.WindowKey;
import org.apache.flink.table.runtime.util.collections.binary.BytesMap.LookupInfo;
import org.apache.flink.table.runtime.util.collections.binary.WindowBytesMultiMap;

import java.io.EOFException;
import java.util.Iterator;

/**
 * An implementation of {@link WindowBuffer} that buffers input elements in a {@link
 * WindowBytesMultiMap} and combines buffered elements into state when flushing.
 */
public final class RecordsWindowBuffer implements WindowBuffer {

    private final WindowCombineFunction combineFunction;
    private final WindowBytesMultiMap recordsBuffer;
    private final WindowKey reuseWindowKey;
    private final AbstractRowDataSerializer<RowData> recordSerializer;

    private long minTriggerTime = Long.MAX_VALUE;

    public RecordsWindowBuffer(
            Object operatorOwner,
            MemoryManager memoryManager,
            long memorySize,
            WindowCombineFunction combineFunction,
            PagedTypeSerializer<RowData> keySer,
            AbstractRowDataSerializer<RowData> inputSer) {
        this.combineFunction = combineFunction;
        this.recordsBuffer =
                new WindowBytesMultiMap(
                        operatorOwner, memoryManager, memorySize, keySer, inputSer.getArity());
        this.recordSerializer = inputSer;
        this.reuseWindowKey = new WindowKeySerializer(keySer).createInstance();
    }

    @Override
    public void addElement(RowData key, long sliceEnd, RowData element) throws Exception {
        // track the lowest trigger time, if watermark exceeds the trigger time,
        // it means there are some elements in the buffer belong to a window going to be fired,
        // and we need to flush the buffer into state for firing.
        minTriggerTime = Math.min(sliceEnd - 1, minTriggerTime);

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
        if (progress >= minTriggerTime) {
            // there should be some window to be fired, flush buffer to state first
            flush();
        }
    }

    @Override
    public void flush() throws Exception {
        if (recordsBuffer.getNumKeys() > 0) {
            KeyValueIterator<WindowKey, Iterator<RowData>> entryIterator =
                    recordsBuffer.getEntryIterator();
            while (entryIterator.advanceNext()) {
                combineFunction.combine(entryIterator.getKey(), entryIterator.getValue());
            }
            recordsBuffer.reset();
            // reset trigger time
            minTriggerTime = Long.MAX_VALUE;
        }
    }

    @Override
    public void close() throws Exception {
        recordsBuffer.free();
    }

    // ------------------------------------------------------------------------------------------
    // Factory
    // ------------------------------------------------------------------------------------------

    /** Factory to create {@link RecordsWindowBuffer}. */
    public static final class Factory implements WindowBuffer.Factory {

        private static final long serialVersionUID = 1L;

        private final PagedTypeSerializer<RowData> keySer;
        private final AbstractRowDataSerializer<RowData> inputSer;

        public Factory(
                PagedTypeSerializer<RowData> keySer, AbstractRowDataSerializer<RowData> inputSer) {
            this.keySer = keySer;
            this.inputSer = inputSer;
        }

        @Override
        public WindowBuffer create(
                Object operatorOwner,
                MemoryManager memoryManager,
                long memorySize,
                WindowCombineFunction combineFunction) {
            return new RecordsWindowBuffer(
                    operatorOwner, memoryManager, memorySize, combineFunction, keySer, inputSer);
        }
    }
}
