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

package org.apache.flink.table.runtime.operators.sort;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.SimpleCollectingOutputView;
import org.apache.flink.runtime.operators.sort.HeapSort;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.typeutils.AbstractRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.util.MemorySegmentPool;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Fixed heap-size buffer. This is mainly designed for mini-batch topN operator to reduce the cost
 * of per-record state visit. When writing records into the buffer, it will only track the topN
 * records. When the operator is ready to emit or snapshot, it can get the sorted topN records from
 * the buffer.
 *
 * <p>The memory is divided into the two parts (key area and record area).
 *
 * <p>Key area: a key entry contains pointer to record and normalized key
 *
 * <p>|--- 8 Bytes (pointer to record) ---| |---- len(NK) Bytes (normalized key) -----|
 *
 * <p>The key area always keep the first key entry to store the temporary data. It's useful when
 * comparing the input to the head of the heap and later to replace the current head with the input.
 */
public class BinaryFixHeapSizeSortedBuffer extends BinaryInMemorySortBuffer {

    // --------------------------------------------------------------------------------------------
    // Internal Sorter and comparator
    // --------------------------------------------------------------------------------------------

    private static final HeapSort INTERNAL_SORTER = new HeapSort();

    private final int heapCapacity;

    transient boolean sorted;

    public static BinaryFixHeapSizeSortedBuffer createExtendedBinaryInMemoryFixedHeapSorter(
            NormalizedKeyComputer normalizedKeyComputer,
            AbstractRowDataSerializer<RowData> inputSerializer,
            BinaryRowDataSerializer serializer,
            RecordComparator comparator,
            MemorySegmentPool memoryPool,
            int heapCapacity) {
        ArrayList<MemorySegment> recordBufferSegments = new ArrayList<>(16);
        SimpleCollectingOutputView recordCollector =
                new SimpleCollectingOutputView(
                        recordBufferSegments, memoryPool, memoryPool.pageSize());
        return new BinaryFixHeapSizeSortedBuffer(
                normalizedKeyComputer,
                inputSerializer,
                serializer,
                comparator,
                recordBufferSegments,
                recordCollector,
                memoryPool,
                memoryPool.freePages(),
                heapCapacity);
    }

    private BinaryFixHeapSizeSortedBuffer(
            NormalizedKeyComputer normalizedKeyComputer,
            AbstractRowDataSerializer<RowData> inputSerializer,
            BinaryRowDataSerializer serializer,
            RecordComparator comparator,
            ArrayList<MemorySegment> recordBufferSegments,
            SimpleCollectingOutputView recordCollector,
            MemorySegmentPool pool,
            int totalNumBuffers,
            int heapCapacity) {
        super(
                normalizedKeyComputer,
                inputSerializer,
                serializer,
                comparator,
                recordBufferSegments,
                recordCollector,
                pool,
                totalNumBuffers);
        Preconditions.checkArgument(
                indexEntriesPerSegment >= 2,
                "The index entries per segment should larger than 2. Please increase the default page size.");

        this.heapCapacity = heapCapacity;

        this.currentSortIndexOffset = indexEntrySize;
        this.sorted = false;
    }

    @Override
    public boolean write(RowData record) throws IOException {
        BinaryRowData binaryRecord = inputSerializer.toBinaryRow(record);
        long recordOffset;
        if (numRecords < heapCapacity) {
            recordOffset = writeRecordToBuffer(binaryRecord);
            writeIndexAndNormalizedKey(binaryRecord, recordOffset);
            if (numRecords == heapCapacity) {
                INTERNAL_SORTER.heapify(this, 1, heapCapacity + 1);
            }
        } else {
            if (numKeyBytes != 0) {
                // always write the normalized key at head
                normalizedKeyComputer.putKey(record, sortIndex.get(0), OFFSET_LEN);
            }

            if (compareToHead(binaryRecord) >= 0) {
                return true;
            }

            recordOffset = writeRecordToBuffer(binaryRecord);

            // write offset into into the key area
            sortIndex.get(0).putLong(0, recordOffset);
            swap(0, 0, 0, indexEntrySize);
            // down heap;
            INTERNAL_SORTER.downHeap(this, 0, 1, heapCapacity + 1);
        }
        return true;
    }

    @Override
    public MutableObjectIterator<BinaryRowData> getIterator() {
        if (!sorted) {
            INTERNAL_SORTER.sort(this, 1, numRecords + 1);
            sorted = true;
        }

        return new BinaryIndexedIterator<BinaryRowData>(indexEntrySize) {
            @Override
            BinaryRowData getRecordFromBuffer(BinaryRowData target, long pointer)
                    throws IOException {
                recordBuffer.setReadPosition(pointer);
                return serializer.mapFromPages(target, recordBuffer);
            }
        };
    }

    @Override
    public void reset() {
        super.reset();
        this.currentSortIndexOffset = indexEntrySize;
        this.sorted = false;
    }

    // -------------------------------------------------------------------------

    /** Compare with the head of the heap to determine whether to add this record. */
    private int compareToHead(BinaryRowData record) {
        if (numKeyBytes != 0) {
            int val =
                    normalizedKeyComputer.compareKey(
                            sortIndex.get(0),
                            OFFSET_LEN,
                            sortIndex.get(0),
                            indexEntrySize + OFFSET_LEN);

            if (val != 0 || this.normalizedKeyFullyDetermines) {
                return this.useNormKeyUninverted ? val : -val;
            }
        }
        long pointer = sortIndex.get(0).getLong(indexEntrySize);
        recordBuffer.setReadPosition(pointer);
        try {
            return comparator.compare(record, serializer1.mapFromPages(row1, recordBuffer));
        } catch (IOException ex) {
            throw new RuntimeException("Error comparing two records.", ex);
        }
    }
}
