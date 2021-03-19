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
import org.apache.flink.runtime.memory.AbstractPagedInputView;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.PagedTypeSerializer;
import org.apache.flink.table.runtime.util.KeyValueIterator;
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
 * A binary map in the structure like {@code Map<K, List<V>>}, where there are multiple values under
 * a single key, and they are all bytes based. It can be used for performing aggregations where the
 * accumulator of aggregations are unfixed-width. The memory is divided into three areas:
 *
 * <p>Bucket area: pointer + hashcode
 *
 * <pre>
 *   |---- 4 Bytes (pointer to key entry) ----|
 *   |----- 4 Bytes (key hashcode) ----------|
 * </pre>
 *
 * <p>Key area: a key entry contains key data, pointer to the tail value, and the head value entry.
 *
 * <pre>
 *   |--- 4 + len(K) Bytes  (key data) ------|
 *   |--- 4 Bytes (pointer to tail value) ---|
 *   |--- 4 Bytes (pointer to next value) ---|
 *   |--- 4 + len(V) Bytes (value data) -----|
 * </pre>
 *
 * <p>Value area: a value entry contains a pointer to the next value and the value data. Pointer is
 * -1 if this is the last entry.
 *
 * <pre>
 *   |--- 4 Bytes (pointer to next value) ---|
 *   |--- 4 + len(V) Bytes (value data) -----|
 * </pre>
 */
public abstract class AbstractBytesMultiMap<K> extends BytesMap<K, Iterator<RowData>> {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractBytesMultiMap.class);

    /** Used to serialize map key into RecordArea's MemorySegments. */
    protected final PagedTypeSerializer<K> keySerializer;

    /** Used to serialize hash map key and value into RecordArea's MemorySegments. */
    private final BinaryRowDataSerializer valueSerializer;

    /** Pointer to the tail value under a key. */
    private int endPtr;

    /** Offset of `endPtr` in key area. */
    private int endPtrOffset;

    /** Pointer to the second value under a key. */
    private int pointerToSecondValue;

    private BinaryRowData reusedRecord;

    private long numKeys = 0;

    public AbstractBytesMultiMap(
            final Object owner,
            MemoryManager memoryManager,
            long memorySize,
            PagedTypeSerializer<K> keySerializer,
            LogicalType[] valueTypes) {
        this(owner, memoryManager, memorySize, keySerializer, valueTypes.length);
    }

    public AbstractBytesMultiMap(
            final Object owner,
            MemoryManager memoryManager,
            long memorySize,
            PagedTypeSerializer<K> keySerializer,
            int valueArity) {
        super(owner, memoryManager, memorySize, keySerializer);
        checkArgument(valueArity > 0);

        this.recordArea = new RecordArea();
        this.keySerializer = keySerializer;
        this.valueSerializer = new BinaryRowDataSerializer(valueArity);
        this.reusedValue = ((RecordArea) this.recordArea).valueIterator(-1);
        this.reusedRecord = valueSerializer.createInstance();

        checkArgument(
                memorySize > INIT_BUCKET_MEMORY_IN_BYTES, "The minBucketMemorySize is not valid!");
        int initBucketSegmentNum =
                MathUtils.roundDownToPowerOf2((int) (INIT_BUCKET_MEMORY_IN_BYTES / segmentSize));

        // allocate and initialize MemorySegments for bucket area
        initBucketSegments(initBucketSegmentNum);

        LOG.info(
                "BytesMultiMap with initial memory segments {}, {} in bytes, init allocating {} for bucket area.",
                reservedNumBuffers,
                reservedNumBuffers * segmentSize,
                initBucketSegmentNum);
    }

    // ----------------------- Abstract Interface -----------------------

    @Override
    public long getNumKeys() {
        return numKeys;
    }

    // ----------------------- Public Interface -----------------------

    /** Append an value into the hash map's record area. */
    public void append(LookupInfo<K, Iterator<RowData>> lookupInfo, BinaryRowData value)
            throws IOException {
        try {
            if (lookupInfo.found) {
                // append value only if there exits key-value pair.
                int newPointer = ((RecordArea) recordArea).appendValue(value);
                if (pointerToSecondValue == -1) {
                    // this is the second value
                    ((RecordArea) recordArea).updateValuePointerInKeyArea(newPointer, endPtr);
                } else {
                    ((RecordArea) recordArea).updateValuePointerInValueArea(newPointer, endPtr);
                }
                // update pointer of the tail value under a key
                endPtr = newPointer;
                ((RecordArea) recordArea).updateValuePointerInKeyArea(newPointer, endPtrOffset);
            } else {
                if (numKeys >= growthThreshold) {
                    growAndRehash();
                    // update info's bucketSegmentIndex and bucketOffset
                    lookup(lookupInfo.key);
                }
                // append key and value if it does not exists
                int pointerToAppended = recordArea.appendRecord(lookupInfo, value);
                bucketSegments
                        .get(lookupInfo.bucketSegmentIndex)
                        .putInt(lookupInfo.bucketOffset, pointerToAppended);
                bucketSegments
                        .get(lookupInfo.bucketSegmentIndex)
                        .putInt(
                                lookupInfo.bucketOffset + ELEMENT_POINT_LENGTH,
                                lookupInfo.keyHashCode);
                numKeys++;
            }
            numElements++;
        } catch (EOFException e) {
            numSpillFiles++;
            spillInBytes += recordArea.getSegmentsSize();
            throw e;
        }
    }

    public KeyValueIterator<K, Iterator<RowData>> getEntryIterator() {
        return ((RecordArea) recordArea).entryIterator();
    }

    /** release the map's record and bucket area's memory segments. */
    public void free() {
        free(false);
    }

    /** @param reservedFixedMemory reserved fixed memory or not. */
    @Override
    public void free(boolean reservedFixedMemory) {
        recordArea.release();
        numKeys = 0;
        super.free(reservedFixedMemory);
    }

    /** reset the map's record and bucket area's memory segments for reusing. */
    @Override
    public void reset() {
        super.reset();
        // reset the record segments.
        recordArea.reset();
        numKeys = 0;
    }

    // ----------------------- Record Area -----------------------

    private final class RecordArea implements BytesMap.RecordArea<K, Iterator<RowData>> {
        private final ArrayList<MemorySegment> keySegments = new ArrayList<>();
        private final ArrayList<MemorySegment> valSegments = new ArrayList<>();

        private final RandomAccessInputView keyInView;
        private final RandomAccessInputView valInView;
        private final SimpleCollectingOutputView keyOutView;
        private final SimpleCollectingOutputView valOutView;

        private ValueIterator reusedValueIterator = new ValueIterator(0);

        RecordArea() {
            this.keyOutView = new SimpleCollectingOutputView(keySegments, memoryPool, segmentSize);
            this.valOutView = new SimpleCollectingOutputView(valSegments, memoryPool, segmentSize);
            this.keyInView = new RandomAccessInputView(keySegments, segmentSize);
            this.valInView = new RandomAccessInputView(valSegments, segmentSize);
        }

        public void release() {
            returnSegments(valSegments);
            returnSegments(keySegments);
            valSegments.clear();
            keySegments.clear();
        }

        public void reset() {
            release();
            // request a new memory segment from freeMemorySegments
            // reset segmentNum and positionInSegment
            keyOutView.reset();
            valOutView.reset();
            valInView.setReadPosition(0);
            keyInView.setReadPosition(0);
        }

        // ----------------------- Append -----------------------
        private int appendValue(BinaryRowData value) throws IOException {
            final long offsetOfPointer = writePointer(valOutView, -1);
            valueSerializer.serializeToPages(value, valOutView);
            if (offsetOfPointer > Integer.MAX_VALUE) {
                LOG.warn(
                        "We can't handle key area with more than Integer.MAX_VALUE bytes,"
                                + " because the pointer is a integer.");
                throw new EOFException();
            }
            return (int) offsetOfPointer;
        }

        // ----------------------- Read -----------------------
        @Override
        public void setReadPosition(int position) {
            keyInView.setReadPosition(position);
        }

        public boolean readKeyAndEquals(K lookupKey) throws IOException {
            reusedKey = keySerializer.mapFromPages(reusedKey, keyInView);
            return lookupKey.equals(reusedKey);
        }

        @Override
        public Iterator<RowData> readValue(Iterator<RowData> reuse) throws IOException {
            endPtr = readPointer(keyInView);
            endPtrOffset = (int) keyInView.getReadPosition() - ELEMENT_POINT_LENGTH;
            pointerToSecondValue = readPointer(keyInView);
            return reuse;
        }

        /** The key is not exist before. Add key and first value to key area. */
        @Override
        public int appendRecord(LookupInfo<K, Iterator<RowData>> lookupInfo, BinaryRowData value)
                throws IOException {
            int lastPosition = (int) keyOutView.getCurrentOffset();
            // write key to keyOutView
            int skip = keySerializer.serializeToPages(lookupInfo.key, keyOutView);
            int keyOffset = lastPosition + skip;

            // skip the pointer to the tail value.
            endPtrOffset = skipPointer(keyOutView);

            // write a value entry: a next-pointer and value data
            long pointerOfEndValue = writePointer(keyOutView, -1);
            // write first value to keyOutView.
            valueSerializer.serializeToPages(value, keyOutView);

            if (pointerOfEndValue > Integer.MAX_VALUE) {
                LOG.warn(
                        "We can't handle key area with more than Integer.MAX_VALUE bytes,"
                                + " because the pointer is a integer.");
                throw new EOFException();
            }
            endPtr = (int) pointerOfEndValue;
            // update pointer to the tail value
            updateValuePointerInKeyArea(endPtr, endPtrOffset);

            return keyOffset;
        }

        @Override
        public long getSegmentsSize() {
            return (valSegments.size() + keySegments.size()) * ((long) segmentSize);
        }

        void updateValuePointerInKeyArea(int newPointer, int ptrOffset) throws IOException {
            updateValuePointer(keyInView, newPointer, ptrOffset);
        }

        void updateValuePointerInValueArea(int newPointer, int ptrOffset) throws IOException {
            updateValuePointer(valInView, newPointer, ptrOffset);
        }

        /** Update the content from specific offset. */
        private void updateValuePointer(RandomAccessInputView view, int newPointer, int ptrOffset)
                throws IOException {
            view.setReadPosition(ptrOffset);
            int currPosInSeg = view.getCurrentPositionInSegment();
            view.getCurrentSegment().putInt(currPosInSeg, newPointer);
        }

        KeyValueIterator<K, Iterator<RowData>> entryIterator() {
            return new EntryIterator();
        }

        final class EntryIterator implements KeyValueIterator<K, Iterator<RowData>> {
            private int count;

            public EntryIterator() {
                count = 0;
                if (numKeys > 0) {
                    recordArea.setReadPosition(0);
                }
            }

            @Override
            public boolean advanceNext() throws IOException {
                if (count < numKeys) {
                    count++;
                    keySerializer.mapFromPages(reusedKey, keyInView);
                    // skip end pointer of value
                    skipPointer(keyInView);
                    // read pointer to second value
                    pointerToSecondValue = readPointer(keyInView);
                    reusedRecord = valueSerializer.mapFromPages(reusedRecord, keyInView);
                    reusedValueIterator.setOffset(pointerToSecondValue);
                    return true;
                }
                return false;
            }

            @Override
            public K getKey() {
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

        Iterator<RowData> valueIterator(int valueOffset) {
            reusedValueIterator.setOffset(valueOffset);
            return reusedValueIterator;
        }

        final class ValueIterator implements Iterator<RowData> {
            private int offset;
            private boolean isFirstRead;

            public ValueIterator(int offset) {
                this.offset = offset;
                this.isFirstRead = true;
            }

            public void setOffset(int offset) {
                this.offset = offset;
                this.isFirstRead = true;
            }

            @Override
            public boolean hasNext() {
                return isFirstRead || offset != -1;
            }

            @Override
            public RowData next() {
                if (isFirstRead) {
                    isFirstRead = false;
                    return reusedRecord;
                }
                if (hasNext()) {
                    valInView.setReadPosition(offset);
                    try {
                        this.offset = readPointer(valInView);
                        // reuse first value data row
                        valueSerializer.mapFromPages(reusedRecord, valInView);
                    } catch (IOException e) {
                        throw new RuntimeException(
                                "Exception happened while iterating"
                                        + " value list of a key in BytesMultiMap");
                    }
                    return reusedRecord;
                }
                return null;
            }
        }
    }

    /** Write value into the output view, and return offset of the value. */
    private long writePointer(SimpleCollectingOutputView outputView, int value) throws IOException {
        int oldPosition = (int) outputView.getCurrentOffset();
        int skip = checkSkipWriteForPointer(outputView);
        outputView.getCurrentSegment().putInt(outputView.getCurrentPositionInSegment(), value);
        // advance position in segment
        outputView.skipBytesToWrite(ELEMENT_POINT_LENGTH);
        return oldPosition + skip;
    }

    private int readPointer(AbstractPagedInputView inputView) throws IOException {
        checkSkipReadForPointer(inputView);
        int value = inputView.getCurrentSegment().getInt(inputView.getCurrentPositionInSegment());
        // advance position in segment
        inputView.skipBytesToRead(ELEMENT_POINT_LENGTH);
        return value;
    }

    private int skipPointer(SimpleCollectingOutputView outputView) throws IOException {
        int oldPosition = (int) outputView.getCurrentOffset();
        // skip 4 bytes for pointer to the tail value
        int skip = checkSkipWriteForPointer(outputView);
        outputView.skipBytesToWrite(ELEMENT_POINT_LENGTH);
        return oldPosition + skip;
    }

    private void skipPointer(AbstractPagedInputView inputView) throws IOException {
        checkSkipReadForPointer(inputView);
        inputView.skipBytesToRead(ELEMENT_POINT_LENGTH);
    }

    /** For pointer needing update, skip unaligned part (4 bytes) for convenient updating. */
    private int checkSkipWriteForPointer(AbstractPagedOutputView outView) throws IOException {
        // skip if there is no enough size.
        int available = outView.getSegmentSize() - outView.getCurrentPositionInSegment();
        if (available < ELEMENT_POINT_LENGTH) {
            outView.advance();
            return available;
        }
        return 0;
    }

    /** For pointer needing update, skip unaligned part (4 bytes) for convenient updating. */
    private void checkSkipReadForPointer(AbstractPagedInputView source) throws IOException {
        // skip if there is no enough size.
        // Note: Use currentSegmentLimit instead of segmentSize.
        int available = source.getCurrentSegmentLimit() - source.getCurrentPositionInSegment();
        if (available < ELEMENT_POINT_LENGTH) {
            source.advance();
        }
    }
}
