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

package org.apache.flink.table.runtime.util.collections.binary;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.RandomAccessInputView;
import org.apache.flink.runtime.io.disk.SimpleCollectingOutputView;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.sort.HeapSort;
import org.apache.flink.runtime.operators.sort.IndexedSortable;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.WindowKeySerializer;
import org.apache.flink.table.runtime.util.KeyValueIterator;
import org.apache.flink.table.runtime.util.WindowKey;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.MathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A binary map in the structure like {@code Map<K, ArrayList<V>>}, where there are multiple values
 * under a single key, and they are all bytes based. The memory is divided into three areas:
 *
 * <p>Bucket area: pointer + hashcode
 *
 * <pre>
 *   |---- 4 Bytes (pointer to key entry) ----|
 *   |----- 4 Bytes (key hashcode) ----------|
 * </pre>
 *
 * <p>Key area: a key entry contains key data, array size and pre-allocated pointer array.
 *
 * <pre>
 *   |--- 4 + len(K) Bytes (key data) -----------|
 *   |--- 4 Bytes (array size) ------------------|
 *   |--- 4 x (array capacity) Bytes (array)-----|
 * </pre>
 *
 * <p>Value area: a value entry contains the value data.
 *
 * <pre>
 *   |--- 4 + len(V) Bytes (value data) -----|
 * </pre>
 *
 * <p>The binary map is mainly designed for topN operator. When the array is full and the new
 * records satisfies the topN condition, it will replace the pointer in the array with the pointer
 * of the new records. When it's time to emit, the binary map will also sort the records under the
 * key.
 */
public class WindowBytesSortedArrayMultiMap extends AbstractBytesMultiMap<WindowKey> {

    private static final Logger LOG = LoggerFactory.getLogger(WindowBytesSortedArrayMultiMap.class);

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    /** The address of the array size under the specified key. */
    private int arraySizeAddress;

    /** The size of the array under the specified key. */
    private int arraySize;

    /** The address of the array under the specified key. */
    private int arrayAddress;

    // --------------------------------------------------------------------------------------------
    // Sorter && comparator
    // --------------------------------------------------------------------------------------------

    /** The sorter to sort the data. */
    private final HeapSort innerSorter;

    /** The comparator to determine the order of the data. */
    private final RecordComparator comparator;

    // --------------------------------------------------------------------------------------------
    // Value serializer
    // --------------------------------------------------------------------------------------------

    /** The value used in comparision. */
    private final BinaryRowData reusedLeftValue;

    /** The serializer to serialize the value. */
    private final BinaryRowDataSerializer valueSerializerForComparison;

    /** The value used in comparision. */
    private final BinaryRowData reusedRightValue;

    // --------------------------------------------------------------------------------------------
    // Array attribute
    // --------------------------------------------------------------------------------------------

    private final int arrayCapacity;

    public WindowBytesSortedArrayMultiMap(
            Object owner,
            MemoryManager memoryManager,
            int memorySize,
            LogicalType[] keyTypes,
            LogicalType[] valueTypes,
            int arrayCapacity,
            RecordComparator comparator) {
        super(
                owner,
                memoryManager,
                memorySize,
                new WindowKeySerializer(keyTypes.length),
                valueTypes);
        checkArgument(
                memorySize > INIT_BUCKET_MEMORY_IN_BYTES, "The minBucketMemorySize is not valid!");
        checkArgument(keyTypes.length > 0);
        checkArgument(
                memoryManager.getPageSize() > arrayCapacity * ELEMENT_POINT_LENGTH,
                "The size of array should not large than the page size.");

        this.valueSerializerForComparison = new BinaryRowDataSerializer(valueTypes.length);
        this.reusedLeftValue = valueSerializer.createInstance();
        this.reusedRightValue = valueSerializerForComparison.createInstance();

        this.arrayCapacity = arrayCapacity;
        this.comparator = comparator;
        this.innerSorter = new HeapSort();
        this.recordArea = new ArrayRecordArea();

        this.reusedValue = ((ArrayRecordArea) recordArea).valueIterator();

        int initBucketSegmentNum =
                MathUtils.roundDownToPowerOf2((int) (INIT_BUCKET_MEMORY_IN_BYTES / segmentSize));
        // allocate and initialize MemorySegments for bucket area
        initBucketSegments(initBucketSegmentNum);
    }

    @Override
    protected void appendRecordWhenFoundKey(
            LookupInfo<WindowKey, Iterator<RowData>> info, BinaryRowData value) throws IOException {
        if (arraySize == arrayCapacity) {
            if (comparator.compare(value, reusedRecord) >= 0) {
                return;
            }
            int valueOffset = ((ArrayRecordArea) recordArea).appendValue(value);
            ((ArrayRecordArea) recordArea).updatePointerAtHead(valueOffset);
            innerSorter.downHeap(((ArrayRecordArea) recordArea), -1, 1, arraySize + 1);
        } else {
            int valueOffset = ((ArrayRecordArea) recordArea).appendValue(value);
            ((ArrayRecordArea) recordArea).appendPointerAtTail(valueOffset);

            if (arraySize == arrayCapacity) {
                this.innerSorter.heapify(((ArrayRecordArea) recordArea), 0, arrayCapacity);
            }
        }
    }

    public KeyValueIterator<WindowKey, Iterator<RowData>> getEntryIterator() {
        return ((ArrayRecordArea) recordArea).entryIterator();
    }

    private class ArrayRecordArea
            implements BytesMap.RecordArea<WindowKey, Iterator<RowData>>, IndexedSortable {

        private final ArrayList<MemorySegment> keySegments = new ArrayList<>();
        private final ArrayList<MemorySegment> valSegments = new ArrayList<>();

        private final RandomAccessInputView keyInView;
        private final RandomAccessInputView valInView;
        private final RandomAccessInputView valInViewForComparison;

        private final SimpleCollectingOutputView keyOutView;
        private final SimpleCollectingOutputView valOutView;

        private final ValueIterator reusedValueIterator = new ValueIterator();

        public ArrayRecordArea() {
            this.keyOutView = new SimpleCollectingOutputView(keySegments, memoryPool, segmentSize);
            this.valOutView = new SimpleCollectingOutputView(valSegments, memoryPool, segmentSize);
            this.keyInView = new RandomAccessInputView(keySegments, segmentSize);
            this.valInView = new RandomAccessInputView(valSegments, segmentSize);
            this.valInViewForComparison = new RandomAccessInputView(valSegments, segmentSize);
        }

        // ----------------------- IndexedSortable -----------------------

        @Override
        public int compare(int i, int j) {
            int addressI = calculateAddress(i);
            int addressJ = calculateAddress(j);

            int pointerI, pointerJ, pos;
            keyInView.setReadPosition(addressI);
            pos = keyInView.getCurrentPositionInSegment();
            pointerI = keyInView.getCurrentSegment().getInt(pos);

            keyInView.setReadPosition(addressJ);
            pos = keyInView.getCurrentPositionInSegment();
            pointerJ = keyInView.getCurrentSegment().getInt(pos);

            valInView.setReadPosition(pointerI);
            valInViewForComparison.setReadPosition(pointerJ);
            try {
                return comparator.compare(
                        valueSerializer.mapFromPages(reusedLeftValue, valInView),
                        valueSerializerForComparison.mapFromPages(
                                reusedRightValue, valInViewForComparison));
            } catch (IOException ioex) {
                throw new RuntimeException("Error comparing two records.", ioex);
            }
        }

        @Override
        public int compare(
                int segmentNumberI, int segmentOffsetI, int segmentNumberJ, int segmentOffsetJ) {
            throw new UnsupportedOperationException(
                    "Unsupported Operation. Please use index to locate the element");
        }

        @Override
        public void swap(int i, int j) {
            int addressI = calculateAddress(i);
            int addressJ = calculateAddress(j);

            keyInView.setReadPosition(addressI);
            MemorySegment segmentI = keyInView.getCurrentSegment();
            int posI = keyInView.getCurrentPositionInSegment();
            int pointerI = segmentI.getInt(posI);

            keyInView.setReadPosition(addressJ);
            MemorySegment segmentJ = keyInView.getCurrentSegment();
            int posJ = keyInView.getCurrentPositionInSegment();
            int pointerJ = segmentJ.getInt(posJ);

            segmentJ.putInt(posJ, pointerI);
            segmentI.putInt(posI, pointerJ);
        }

        @Override
        public void swap(
                int segmentNumberI, int segmentOffsetI, int segmentNumberJ, int segmentOffsetJ) {
            throw new UnsupportedOperationException(
                    "Unsupported Operation. Please use index to locate the element");
        }

        @Override
        public int size() {
            return arraySize;
        }

        @Override
        public int recordSize() {
            // return the size of the pointer
            return ELEMENT_POINT_LENGTH;
        }

        @Override
        public int recordsPerSegment() {
            // store the key in key area which cause it's difficult to measure.
            throw new UnsupportedOperationException(
                    "Unsupported Operation. ArrayRecordArea will also "
                            + "store the key part of the records whose size is out-of-control.");
        }

        // ----------------------- RecordArea -----------------------

        @Override
        public void setReadPosition(int position) {
            keyInView.setReadPosition(position);
        }

        @Override
        public boolean readKeyAndEquals(WindowKey lookupKey) throws IOException {
            reusedKey = keySerializer.mapFromPages(reusedKey, keyInView);
            return lookupKey.equals(reusedKey);
        }

        @Override
        public Iterator<RowData> readValue(Iterator<RowData> reuse) throws IOException {
            // set up the address of the array size and array
            arraySize = readPointer(keyInView);
            arraySizeAddress = (int) keyInView.getReadPosition() - ELEMENT_POINT_LENGTH;
            arrayAddress = (int) keyInView.getReadPosition();
            // read head of the array (it may be the head of the heap)
            int head = readPointer(keyInView);
            valInView.setReadPosition(head);
            valueSerializer.mapFromPages(reusedRecord, valInView);
            return reuse;
        }

        @Override
        public int appendRecord(
                LookupInfo<WindowKey, Iterator<RowData>> lookupInfo, BinaryRowData value)
                throws IOException {
            int keyOffset = (int) writeKey(keyOutView, lookupInfo.key);

            // write the array size into key area
            arraySizeAddress = (int) writePointer(keyOutView, 1);

            // skip to the size
            arrayAddress = arraySizeAddress + ELEMENT_POINT_LENGTH;

            // pre-allocate the array in the key area
            preAllocateArray();

            int recordOffset = appendValue(value);
            updatePointerAtHead(recordOffset);
            return keyOffset;
        }

        private int appendValue(BinaryRowData value) throws IOException {
            long recordOffset = valOutView.getCurrentOffset();
            recordOffset += valueSerializer.serializeToPages(value, valOutView);
            if (recordOffset > Integer.MAX_VALUE) {
                LOG.warn(
                        "We can't handle key area with more than Integer.MAX_VALUE bytes,"
                                + " because the pointer is a integer.");
                throw new EOFException();
            }
            return (int) recordOffset;
        }

        @Override
        public long getSegmentsSize() {
            return (valSegments.size() + keySegments.size()) * ((long) segmentSize);
        }

        @Override
        public void release() {
            returnSegments(keySegments);
            returnSegments(valSegments);

            valSegments.clear();
            keySegments.clear();
        }

        @Override
        public void reset() {
            release();

            keyOutView.reset();
            valOutView.reset();
            keyInView.setReadPosition(0);
            valInView.setReadPosition(0);
            valInViewForComparison.setReadPosition(0);
        }

        // ----------------------- Iterator -----------------------

        public EntryIterator entryIterator() {
            return new EntryIterator();
        }

        public ValueIterator valueIterator() {
            return reusedValueIterator;
        }

        final class EntryIterator implements KeyValueIterator<WindowKey, Iterator<RowData>> {
            private int count;
            private int nextKeyAreaAddress;

            public EntryIterator() {
                count = 0;
                nextKeyAreaAddress = 0;
                if (numKeys > 0) {
                    recordArea.setReadPosition(0);
                }
            }

            @Override
            public boolean advanceNext() throws IOException {
                if (count < numKeys) {
                    count++;
                    keyInView.setReadPosition(nextKeyAreaAddress);
                    keySerializer.mapFromPages(reusedKey, keyInView);
                    // set up the array size && address
                    // TODO: readValue should only read the head of the array when the array is
                    // sorted.
                    readValue(reusedValue);
                    // sort array
                    innerSorter.sort(ArrayRecordArea.this);
                    nextKeyAreaAddress = calculateAddress(arrayCapacity);
                    // If the size of the array < the capacity of the array, the array is unsorted.
                    // Therefore, we need to re-read the first value.
                    reusedValueIterator.reset();
                    return true;
                }
                return false;
            }

            @Override
            public WindowKey getKey() {
                return reusedKey;
            }

            @Override
            public Iterator<RowData> getValue() {
                return reusedValue;
            }

            public boolean hasNext() {
                return count < numKeys;
            }
        }

        final class ValueIterator implements Iterator<RowData> {
            private int readCount;
            long offset = 0;

            public void reset() {
                setReadPosition(calculateAddress(0));
                readCount = 0;
            }

            @Override
            public boolean hasNext() {
                return readCount < arraySize;
            }

            @Override
            public RowData next() {
                if (hasNext()) {
                    try {
                        // reuse first value data row
                        offset = readPointer(keyInView);
                        valInView.setReadPosition(offset);
                        valueSerializer.mapFromPages(reusedRecord, valInView);
                    } catch (IOException e) {
                        throw new RuntimeException(
                                "Exception happened while iterating"
                                        + " value list of a key in WindowBytesSortedMultiMap");
                    }
                    readCount++;
                    return reusedRecord;
                }
                return null;
            }
        }

        // ----------------------- Utils -----------------------

        /** Calculate the address of the element index under the key. */
        private int calculateAddress(int elementIndex) {
            long lastPos = keyInView.getReadPosition();
            keyInView.setReadPosition(arrayAddress);
            int remainSize =
                    keyInView.getCurrentSegment().size() - keyInView.getCurrentPositionInSegment();

            // keep the read pos unchanged.
            keyInView.setReadPosition(lastPos);
            if (remainSize >= (elementIndex + 1) * ELEMENT_POINT_LENGTH) {
                return arrayAddress + elementIndex * ELEMENT_POINT_LENGTH;
            } else {
                return arrayAddress
                        + remainSize
                        + (elementIndex - remainSize / ELEMENT_POINT_LENGTH) * ELEMENT_POINT_LENGTH;
            }
        }

        private void preAllocateArray() throws IOException {
            // the array is in the new segment
            if (keyOutView.getCurrentPositionInSegment() >= keyOutView.getCurrentSegment().size()) {
                keyOutView.advance();
            }
            int newKeyAddress = calculateAddress(arrayCapacity);
            keyOutView.skipBytesToWrite(newKeyAddress - arrayAddress);
        }

        private void updatePointerAtHead(int pointer) {
            // update pointer at head
            updateValuePointer(keyInView, pointer, calculateAddress(0));
        }

        private void appendPointerAtTail(int pointer) {
            // write pointer into the array
            updateValuePointer(keyInView, pointer, calculateAddress(arraySize));
            // update array size
            updateValuePointer(keyInView, ++arraySize, arraySizeAddress);
        }
    }
}
