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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.PagedTypeSerializer;
import org.apache.flink.table.runtime.util.KeyValueIterator;
import org.apache.flink.table.types.logical.LogicalType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * A binary map in the structure like {@code Map<K, LinkedList<V>>}, where there are multiple values
 * under a single key, and they are all bytes based. It can be used for performing aggregations
 * where the accumulator of aggregations are unfixed-width. The memory is divided into three areas:
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
public class AbstractBytesLinkedMultiMap<K> extends AbstractBytesMultiMap<K> {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractBytesLinkedMultiMap.class);

    /** Pointer to the tail value under a key. */
    private int endPtr;

    /** Offset of `endPtr` in key area. */
    private int endPtrOffset;

    public AbstractBytesLinkedMultiMap(
            Object owner,
            MemoryManager memoryManager,
            long memorySize,
            PagedTypeSerializer<K> keySerializer,
            LogicalType[] valueTypes) {
        super(owner, memoryManager, memorySize, keySerializer, valueTypes);
        this.recordArea = new RecordArea();
        this.reusedValue = ((RecordArea) this.recordArea).valueIterator(-1);
    }

    @Override
    public KeyValueIterator<K, Iterator<RowData>> getEntryIterator() {
        return ((RecordArea) recordArea).entryIterator();
    }

    @Override
    protected void appendRecordWhenFoundKey(
            LookupInfo<K, Iterator<RowData>> info, BinaryRowData value) throws IOException {
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
    }

    // ----------------------- Record Area -----------------------

    private final class RecordArea implements BytesMap.RecordArea<K, Iterator<RowData>> {
        private final ArrayList<MemorySegment> keySegments = new ArrayList<>();
        private final ArrayList<MemorySegment> valSegments = new ArrayList<>();

        private final RandomAccessInputView keyInView;
        private final RandomAccessInputView valInView;
        private final SimpleCollectingOutputView keyOutView;
        private final SimpleCollectingOutputView valOutView;

        private final ValueIterator reusedValueIterator = new ValueIterator(0);

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
        public int appendRecord(
                BytesMap.LookupInfo<K, Iterator<RowData>> lookupInfo, BinaryRowData value)
                throws IOException {
            int keyOffset = (int) writeKey(keyOutView, lookupInfo.key);

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

        void updateValuePointerInKeyArea(int newPointer, int ptrOffset) {
            updateValuePointer(keyInView, newPointer, ptrOffset);
        }

        void updateValuePointerInValueArea(int newPointer, int ptrOffset) {
            updateValuePointer(valInView, newPointer, ptrOffset);
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
}
