/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.sort;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.RandomAccessInputView;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.util.MemorySegmentPool;
import org.apache.flink.util.MutableObjectIterator;

import java.io.IOException;
import java.util.ArrayList;

import static org.apache.flink.util.Preconditions.checkArgument;

/** In memory KV sortable buffer for binary row, it already has records in memory. */
public class BinaryKVInMemorySortBuffer extends BinaryIndexedSortable {

    private final BinaryRowDataSerializer valueSerializer;

    public static BinaryKVInMemorySortBuffer createBuffer(
            NormalizedKeyComputer normalizedKeyComputer,
            BinaryRowDataSerializer keySerializer,
            BinaryRowDataSerializer valueSerializer,
            RecordComparator comparator,
            ArrayList<MemorySegment> recordBufferSegments,
            long numElements,
            MemorySegmentPool pool)
            throws IOException {
        BinaryKVInMemorySortBuffer sorter =
                new BinaryKVInMemorySortBuffer(
                        normalizedKeyComputer,
                        keySerializer,
                        valueSerializer,
                        comparator,
                        recordBufferSegments,
                        pool);
        sorter.load(numElements, sorter.recordBuffer);
        return sorter;
    }

    private BinaryKVInMemorySortBuffer(
            NormalizedKeyComputer normalizedKeyComputer,
            BinaryRowDataSerializer keySerializer,
            BinaryRowDataSerializer valueSerializer,
            RecordComparator comparator,
            ArrayList<MemorySegment> recordBufferSegments,
            MemorySegmentPool memorySegmentPool) {
        super(
                normalizedKeyComputer,
                keySerializer,
                comparator,
                recordBufferSegments,
                memorySegmentPool);
        this.valueSerializer = valueSerializer;
    }

    @Override
    public void writeToOutput(AbstractPagedOutputView output) throws IOException {
        final int numRecords = this.numRecords;
        int currentMemSeg = 0;
        int currentRecord = 0;

        while (currentRecord < numRecords) {
            final MemorySegment currentIndexSegment = this.sortIndex.get(currentMemSeg++);

            // go through all records in the memory segment
            for (int offset = 0;
                    currentRecord < numRecords && offset <= this.lastIndexEntryOffset;
                    currentRecord++, offset += this.indexEntrySize) {
                final long pointer = currentIndexSegment.getLong(offset);
                this.recordBuffer.setReadPosition(pointer);
                this.serializer.copyFromPagesToView(this.recordBuffer, output);
                this.valueSerializer.copyFromPagesToView(this.recordBuffer, output);
            }
        }
    }

    private void load(long numElements, RandomAccessInputView recordInputView) throws IOException {
        for (int index = 0; index < numElements; index++) {
            serializer.checkSkipReadForFixLengthPart(recordInputView);
            long pointer = recordInputView.getReadPosition();
            BinaryRowData row = serializer1.mapFromPages(row1, recordInputView);
            valueSerializer.checkSkipReadForFixLengthPart(recordInputView);
            recordInputView.skipBytes(recordInputView.readInt());
            boolean success = checkNextIndexOffset();
            checkArgument(success);
            writeIndexAndNormalizedKey(row, pointer);
        }
    }

    /**
     * Gets an iterator over all KV records in this buffer in their logical order.
     *
     * @return An iterator returning the records in their logical order.
     */
    public final MutableObjectIterator<Tuple2<BinaryRowData, BinaryRowData>> getIterator() {
        return new MutableObjectIterator<Tuple2<BinaryRowData, BinaryRowData>>() {
            private final int size = size();
            private int current = 0;

            private int currentSegment = 0;
            private int currentOffset = 0;

            private MemorySegment currentIndexSegment = sortIndex.get(0);

            @Override
            public Tuple2<BinaryRowData, BinaryRowData> next(
                    Tuple2<BinaryRowData, BinaryRowData> kv) {
                if (this.current < this.size) {
                    this.current++;
                    if (this.currentOffset > lastIndexEntryOffset) {
                        this.currentOffset = 0;
                        this.currentIndexSegment = sortIndex.get(++this.currentSegment);
                    }

                    long pointer = this.currentIndexSegment.getLong(this.currentOffset);
                    this.currentOffset += indexEntrySize;

                    try {
                        return getRecordFromBuffer(kv.f0, kv.f1, pointer);
                    } catch (IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                } else {
                    return null;
                }
            }

            @Override
            public Tuple2<BinaryRowData, BinaryRowData> next() {
                throw new RuntimeException("Not support!");
            }
        };
    }

    private Tuple2<BinaryRowData, BinaryRowData> getRecordFromBuffer(
            BinaryRowData reuseKey, BinaryRowData reuseValue, long pointer) throws IOException {
        this.recordBuffer.setReadPosition(pointer);
        reuseKey = this.serializer.mapFromPages(reuseKey, this.recordBuffer);
        reuseValue = this.serializer.mapFromPages(reuseValue, this.recordBuffer);
        return Tuple2.of(reuseKey, reuseValue);
    }
}
