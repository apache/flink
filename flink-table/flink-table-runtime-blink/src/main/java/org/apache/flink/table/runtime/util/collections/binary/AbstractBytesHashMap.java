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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.disk.RandomAccessInputView;
import org.apache.flink.runtime.io.disk.SimpleCollectingOutputView;
import org.apache.flink.runtime.memory.AbstractPagedInputView;
import org.apache.flink.runtime.memory.MemoryManager;
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
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Bytes based hash map. It can be used for performing aggregations where the aggregated values are
 * fixed-width, because the data is stored in continuous memory, AggBuffer of variable length cannot
 * be applied to this HashMap. The KeyValue form in hash map is designed to reduce the cost of key
 * fetching in lookup. The memory is divided into two areas:
 *
 * <p>Bucket area: pointer + hashcode.
 *
 * <ul>
 *   <li>Bytes 0 to 4: a pointer to the record in the record area
 *   <li>Bytes 4 to 8: key's full 32-bit hashcode
 * </ul>
 *
 * <p>Record area: the actual data in linked list records, a record has four parts:
 *
 * <ul>
 *   <li>Bytes 0 to 4: len(k)
 *   <li>Bytes 4 to 4 + len(k): key data
 *   <li>Bytes 4 + len(k) to 8 + len(k): len(v)
 *   <li>Bytes 8 + len(k) to 8 + len(k) + len(v): value data
 * </ul>
 *
 * <p>{@code BytesHashMap} are influenced by Apache Spark BytesToBytesMap.
 */
public abstract class AbstractBytesHashMap<K> extends BytesMap<K, BinaryRowData> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractBytesHashMap.class);

    /**
     * Set true when valueTypeInfos.length == 0. Usually in this case the BytesHashMap will be used
     * as a HashSet. The value from {@link AbstractBytesHashMap#append(LookupInfo info,
     * BinaryRowData value)} will be ignored when hashSetMode set. The reusedValue will always point
     * to a 16 bytes long MemorySegment acted as each BytesHashMap entry's value part when appended
     * to make the BytesHashMap's spilling work compatible.
     */
    private final boolean hashSetMode;

    /** Used to serialize map key into RecordArea's MemorySegments. */
    protected final PagedTypeSerializer<K> keySerializer;

    /** Used to serialize hash map value into RecordArea's MemorySegments. */
    private final BinaryRowDataSerializer valueSerializer;

    private volatile RecordArea.EntryIterator destructiveIterator = null;

    public AbstractBytesHashMap(
            final Object owner,
            MemoryManager memoryManager,
            long memorySize,
            PagedTypeSerializer<K> keySerializer,
            LogicalType[] valueTypes) {
        this(owner, memoryManager, memorySize, keySerializer, valueTypes.length);
    }

    public AbstractBytesHashMap(
            final Object owner,
            MemoryManager memoryManager,
            long memorySize,
            PagedTypeSerializer<K> keySerializer,
            int valueArity) {
        super(owner, memoryManager, memorySize, keySerializer);

        this.recordArea = new RecordArea();

        this.keySerializer = keySerializer;
        this.valueSerializer = new BinaryRowDataSerializer(valueArity);
        if (valueArity == 0) {
            this.hashSetMode = true;
            this.reusedValue = new BinaryRowData(0);
            this.reusedValue.pointTo(MemorySegmentFactory.wrap(new byte[8]), 0, 8);
            LOG.info("BytesHashMap with hashSetMode = true.");
        } else {
            this.hashSetMode = false;
            this.reusedValue = this.valueSerializer.createInstance();
        }

        checkArgument(
                memorySize > INIT_BUCKET_MEMORY_IN_BYTES, "The minBucketMemorySize is not valid!");
        final int initBucketSegmentNum =
                MathUtils.roundDownToPowerOf2((int) (INIT_BUCKET_MEMORY_IN_BYTES / segmentSize));

        // allocate and initialize MemorySegments for bucket area
        initBucketSegments(initBucketSegmentNum);

        LOG.info(
                "BytesHashMap with initial memory segments {}, {} in bytes, init allocating {} for bucket area.",
                reservedNumBuffers,
                reservedNumBuffers * segmentSize,
                initBucketSegmentNum);
    }

    // ----------------------- Abstract Interface -----------------------

    @Override
    public long getNumKeys() {
        return numElements;
    }

    // ----------------------- Public interface -----------------------

    /**
     * Append an value into the hash map's record area.
     *
     * @return An BinaryRowData mapping to the memory segments in the map's record area belonging to
     *     the newly appended value.
     * @throws EOFException if the map can't allocate much more memory.
     */
    public BinaryRowData append(LookupInfo<K, BinaryRowData> lookupInfo, BinaryRowData value)
            throws IOException {
        try {
            if (numElements >= growthThreshold) {
                growAndRehash();
                // update info's bucketSegmentIndex and bucketOffset
                lookup(lookupInfo.key);
            }
            BinaryRowData toAppend = hashSetMode ? reusedValue : value;
            int pointerToAppended = recordArea.appendRecord(lookupInfo, toAppend);
            bucketSegments
                    .get(lookupInfo.bucketSegmentIndex)
                    .putInt(lookupInfo.bucketOffset, pointerToAppended);
            bucketSegments
                    .get(lookupInfo.bucketSegmentIndex)
                    .putInt(lookupInfo.bucketOffset + ELEMENT_POINT_LENGTH, lookupInfo.keyHashCode);
            numElements++;
            recordArea.setReadPosition(pointerToAppended);
            ((RecordArea) recordArea).skipKey();
            return recordArea.readValue(reusedValue);
        } catch (EOFException e) {
            numSpillFiles++;
            spillInBytes += recordArea.getSegmentsSize();
            throw e;
        }
    }

    public long getNumSpillFiles() {
        return numSpillFiles;
    }

    public long getUsedMemoryInBytes() {
        return bucketSegments.size() * ((long) segmentSize) + recordArea.getSegmentsSize();
    }

    public long getSpillInBytes() {
        return spillInBytes;
    }

    public long getNumElements() {
        return numElements;
    }

    /** Returns an iterator for iterating over the entries of this map. */
    @SuppressWarnings("WeakerAccess")
    public KeyValueIterator<K, BinaryRowData> getEntryIterator(boolean requiresCopy) {
        if (destructiveIterator != null) {
            throw new IllegalArgumentException(
                    "DestructiveIterator is not null, so this method can't be invoke!");
        }
        return ((RecordArea) recordArea).entryIterator(requiresCopy);
    }

    /** @return the underlying memory segments of the hash map's record area */
    @SuppressWarnings("WeakerAccess")
    public ArrayList<MemorySegment> getRecordAreaMemorySegments() {
        return ((RecordArea) recordArea).segments;
    }

    @SuppressWarnings("WeakerAccess")
    public List<MemorySegment> getBucketAreaMemorySegments() {
        return bucketSegments;
    }

    /** release the map's record and bucket area's memory segments. */
    public void free() {
        free(false);
    }

    /** @param reservedRecordMemory reserved fixed memory or not. */
    public void free(boolean reservedRecordMemory) {
        recordArea.release();
        destructiveIterator = null;
        super.free(reservedRecordMemory);
    }

    /** reset the map's record and bucket area's memory segments for reusing. */
    public void reset() {
        // reset the record segments.
        recordArea.reset();
        destructiveIterator = null;
        super.reset();
    }

    /**
     * @return true when BytesHashMap's valueTypeInfos.length == 0. Any appended value will be
     *     ignored and replaced with a reusedValue as a present tag.
     */
    @VisibleForTesting
    boolean isHashSetMode() {
        return hashSetMode;
    }

    // ----------------------- Private methods -----------------------

    static int getVariableLength(LogicalType[] types) {
        int length = 0;
        for (LogicalType type : types) {
            if (!BinaryRowData.isInFixedLengthPart(type)) {
                // find a better way of computing generic type field variable-length
                // right now we use a small value assumption
                length += 16;
            }
        }
        return length;
    }

    // ----------------------- Record Area -----------------------

    private final class RecordArea implements BytesMap.RecordArea<K, BinaryRowData> {
        private final ArrayList<MemorySegment> segments = new ArrayList<>();

        private final RandomAccessInputView inView;
        private final SimpleCollectingOutputView outView;

        RecordArea() {
            this.outView = new SimpleCollectingOutputView(segments, memoryPool, segmentSize);
            this.inView = new RandomAccessInputView(segments, segmentSize);
        }

        public void release() {
            returnSegments(segments);
            segments.clear();
        }

        public void reset() {
            release();
            // request a new memory segment from freeMemorySegments
            // reset segmentNum and positionInSegment
            outView.reset();
            inView.setReadPosition(0);
        }

        // ----------------------- Append -----------------------
        public int appendRecord(LookupInfo<K, BinaryRowData> lookupInfo, BinaryRowData value)
                throws IOException {
            final long oldLastPosition = outView.getCurrentOffset();
            // serialize the key into the BytesHashMap record area
            int skip = keySerializer.serializeToPages(lookupInfo.getKey(), outView);
            long offset = oldLastPosition + skip;

            // serialize the value into the BytesHashMap record area
            valueSerializer.serializeToPages(value, outView);
            if (offset > Integer.MAX_VALUE) {
                LOG.warn(
                        "We can't handle key area with more than Integer.MAX_VALUE bytes,"
                                + " because the pointer is a integer.");
                throw new EOFException();
            }
            return (int) offset;
        }

        @Override
        public long getSegmentsSize() {
            return segments.size() * ((long) segmentSize);
        }

        // ----------------------- Read -----------------------
        public void setReadPosition(int position) {
            inView.setReadPosition(position);
        }

        public boolean readKeyAndEquals(K lookupKey) throws IOException {
            reusedKey = keySerializer.mapFromPages(reusedKey, inView);
            return lookupKey.equals(reusedKey);
        }

        /** @throws IOException when invalid memory address visited. */
        void skipKey() throws IOException {
            keySerializer.skipRecordFromPages(inView);
        }

        public BinaryRowData readValue(BinaryRowData reuse) throws IOException {
            // depends on BinaryRowDataSerializer to check writing skip
            // and to find the real start offset of the data
            return valueSerializer.mapFromPages(reuse, inView);
        }

        // ----------------------- Iterator -----------------------

        private KeyValueIterator<K, BinaryRowData> entryIterator(boolean requiresCopy) {
            return new EntryIterator(requiresCopy);
        }

        private final class EntryIterator extends AbstractPagedInputView
                implements KeyValueIterator<K, BinaryRowData> {

            private int count = 0;
            private int currentSegmentIndex = 0;
            private final boolean requiresCopy;

            private EntryIterator(boolean requiresCopy) {
                super(segments.get(0), segmentSize, 0);
                destructiveIterator = this;
                this.requiresCopy = requiresCopy;
            }

            @Override
            public boolean advanceNext() throws IOException {
                if (count < numElements) {
                    count++;
                    // segment already is useless any more.
                    keySerializer.mapFromPages(reusedKey, this);
                    valueSerializer.mapFromPages(reusedValue, this);
                    return true;
                }
                return false;
            }

            @Override
            public K getKey() {
                return requiresCopy ? keySerializer.copy(reusedKey) : reusedKey;
            }

            @Override
            public BinaryRowData getValue() {
                return requiresCopy ? reusedValue.copy() : reusedValue;
            }

            public boolean hasNext() {
                return count < numElements;
            }

            @Override
            protected int getLimitForSegment(MemorySegment segment) {
                return segmentSize;
            }

            @Override
            protected MemorySegment nextSegment(MemorySegment current) {
                return segments.get(++currentSegmentIndex);
            }
        }
    }
}
