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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.util.LazyMemorySegmentPool;
import org.apache.flink.util.MathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Base class for {@link BytesHashMap} and BytesMultiMap.
 *
 * @param <K> type of the map key.
 * @param <V> type of the map value.
 */
public abstract class BytesMap<K, V> {
    private static final Logger LOG = LoggerFactory.getLogger(BytesMap.class);

    public static final int BUCKET_SIZE = 8;
    protected static final int END_OF_LIST = Integer.MAX_VALUE;
    protected static final int STEP_INCREMENT = 1;
    protected static final int ELEMENT_POINT_LENGTH = 4;
    public static final int RECORD_EXTRA_LENGTH = 8;
    protected static final int BUCKET_SIZE_BITS = 3;

    protected final int numBucketsPerSegment;
    protected final int numBucketsPerSegmentBits;
    protected final int numBucketsPerSegmentMask;
    protected final int lastBucketPosition;

    protected final int segmentSize;
    protected final LazyMemorySegmentPool memoryPool;
    protected List<MemorySegment> bucketSegments;

    protected final int reservedNumBuffers;

    protected long numElements = 0;
    protected int numBucketsMask;
    // get the second hashcode based log2NumBuckets and numBucketsMask2
    protected int log2NumBuckets;
    protected int numBucketsMask2;

    protected static final double LOAD_FACTOR = 0.75;
    // a smaller bucket can make the best of l1/l2/l3 cache.
    protected static final long INIT_BUCKET_MEMORY_IN_BYTES = 1024 * 1024L;

    /** The map will be expanded once the number of elements exceeds this threshold. */
    protected int growthThreshold;

    /** The segments where the actual data is stored. */
    protected RecordArea<K, V> recordArea;

    /** Used as a reused object when lookup and iteration. */
    protected K reusedKey;

    /** Used as a reused object when retrieve the map's value by key and iteration. */
    protected V reusedValue;

    /** Used as a reused object which lookup returned. */
    private final LookupInfo<K, V> reuseLookupInfo;

    // metric
    protected long numSpillFiles;
    protected long spillInBytes;

    public BytesMap(
            final Object owner,
            MemoryManager memoryManager,
            long memorySize,
            TypeSerializer<K> keySerializer) {
        this.memoryPool = new LazyMemorySegmentPool(owner, memoryManager, memorySize);
        this.segmentSize = memoryPool.pageSize();
        this.reservedNumBuffers = (int) (memorySize / segmentSize);
        this.numBucketsPerSegment = segmentSize / BUCKET_SIZE;
        this.numBucketsPerSegmentBits = MathUtils.log2strict(this.numBucketsPerSegment);
        this.numBucketsPerSegmentMask = (1 << this.numBucketsPerSegmentBits) - 1;
        this.lastBucketPosition = (numBucketsPerSegment - 1) * BUCKET_SIZE;

        this.reusedKey = keySerializer.createInstance();
        this.reuseLookupInfo = new LookupInfo<>();
    }

    /** Returns the number of keys in this map. */
    public abstract long getNumKeys();

    protected void initBucketSegments(int numBucketSegments) {
        if (numBucketSegments < 1) {
            throw new RuntimeException("Too small memory allocated for BytesHashMap");
        }
        this.bucketSegments = new ArrayList<>(numBucketSegments);
        for (int i = 0; i < numBucketSegments; i++) {
            bucketSegments.add(i, memoryPool.nextSegment());
        }

        resetBucketSegments(this.bucketSegments);
        int numBuckets = numBucketSegments * numBucketsPerSegment;
        this.log2NumBuckets = MathUtils.log2strict(numBuckets);
        this.numBucketsMask = (1 << MathUtils.log2strict(numBuckets)) - 1;
        this.numBucketsMask2 = (1 << MathUtils.log2strict(numBuckets >> 1)) - 1;
        this.growthThreshold = (int) (numBuckets * LOAD_FACTOR);
    }

    protected void resetBucketSegments(List<MemorySegment> resetBucketSegs) {
        for (MemorySegment segment : resetBucketSegs) {
            for (int j = 0; j <= lastBucketPosition; j += BUCKET_SIZE) {
                segment.putInt(j, END_OF_LIST);
            }
        }
    }

    public long getNumSpillFiles() {
        return numSpillFiles;
    }

    public long getSpillInBytes() {
        return spillInBytes;
    }

    public long getNumElements() {
        return numElements;
    }

    /** @param reservedRecordMemory reserved fixed memory or not. */
    public void free(boolean reservedRecordMemory) {
        returnSegments(this.bucketSegments);
        this.bucketSegments.clear();
        if (!reservedRecordMemory) {
            memoryPool.close();
        }
        numElements = 0;
    }

    /** reset the map's record and bucket area's memory segments for reusing. */
    public void reset() {
        setBucketVariables(bucketSegments);
        resetBucketSegments(bucketSegments);
        numElements = 0;
        LOG.info(
                "reset BytesHashMap with record memory segments {}, {} in bytes, init allocating {} for bucket area.",
                memoryPool.freePages(),
                memoryPool.freePages() * segmentSize,
                bucketSegments.size());
    }

    /**
     * @param key by which looking up the value in the hash map. Only support the key in the
     *     BinaryRowData form who has only one MemorySegment.
     * @return {@link LookupInfo}
     */
    public LookupInfo<K, V> lookup(K key) {
        final int hashCode1 = key.hashCode();
        int newPos = hashCode1 & numBucketsMask;
        // which segment contains the bucket
        int bucketSegmentIndex = newPos >>> numBucketsPerSegmentBits;
        // offset of the bucket in the segment
        int bucketOffset = (newPos & numBucketsPerSegmentMask) << BUCKET_SIZE_BITS;

        boolean found = false;
        int step = STEP_INCREMENT;
        int hashCode2 = 0;
        int findElementPtr;
        try {
            do {
                findElementPtr = bucketSegments.get(bucketSegmentIndex).getInt(bucketOffset);
                if (findElementPtr == END_OF_LIST) {
                    // This is a new key.
                    break;
                } else {
                    final int storedHashCode =
                            bucketSegments
                                    .get(bucketSegmentIndex)
                                    .getInt(bucketOffset + ELEMENT_POINT_LENGTH);
                    if (hashCode1 == storedHashCode) {
                        recordArea.setReadPosition(findElementPtr);
                        if (recordArea.readKeyAndEquals(key)) {
                            // we found an element with a matching key, and not just a hash
                            // collision
                            found = true;
                            reusedValue = recordArea.readValue(reusedValue);
                            break;
                        }
                    }
                }
                if (step == 1) {
                    hashCode2 = calcSecondHashCode(hashCode1);
                }
                newPos = (hashCode1 + step * hashCode2) & numBucketsMask;
                // which segment contains the bucket
                bucketSegmentIndex = newPos >>> numBucketsPerSegmentBits;
                // offset of the bucket in the segment
                bucketOffset = (newPos & numBucketsPerSegmentMask) << BUCKET_SIZE_BITS;
                step += STEP_INCREMENT;
            } while (true);
        } catch (IOException ex) {
            throw new RuntimeException(
                    "Error reading record from the aggregate map: " + ex.getMessage(), ex);
        }
        reuseLookupInfo.set(found, hashCode1, key, reusedValue, bucketSegmentIndex, bucketOffset);
        return reuseLookupInfo;
    }

    /** @throws EOFException if the map can't allocate much more memory. */
    protected void growAndRehash() throws EOFException {
        // allocate the new data structures
        int required = 2 * bucketSegments.size();
        if (required * (long) numBucketsPerSegment > Integer.MAX_VALUE) {
            LOG.warn(
                    "We can't handle more than Integer.MAX_VALUE buckets (eg. because hash functions return int)");
            throw new EOFException();
        }

        int numAllocatedSegments = required - memoryPool.freePages();
        if (numAllocatedSegments > 0) {
            LOG.warn(
                    "BytesHashMap can't allocate {} pages, and now used {} pages",
                    required,
                    reservedNumBuffers);
            throw new EOFException();
        }

        List<MemorySegment> newBucketSegments = memoryPool.allocateSegments(required);
        setBucketVariables(newBucketSegments);

        long reHashStartTime = System.currentTimeMillis();
        resetBucketSegments(newBucketSegments);
        // Re-mask (we don't recompute the hashcode because we stored all 32 bits of it)
        for (MemorySegment memorySegment : bucketSegments) {
            for (int j = 0; j < numBucketsPerSegment; j++) {
                final int recordPointer = memorySegment.getInt(j * BUCKET_SIZE);
                if (recordPointer != END_OF_LIST) {
                    final int hashCode1 =
                            memorySegment.getInt(j * BUCKET_SIZE + ELEMENT_POINT_LENGTH);
                    int newPos = hashCode1 & numBucketsMask;
                    int bucketSegmentIndex = newPos >>> numBucketsPerSegmentBits;
                    int bucketOffset = (newPos & numBucketsPerSegmentMask) << BUCKET_SIZE_BITS;
                    int step = STEP_INCREMENT;
                    long hashCode2 = 0;
                    while (newBucketSegments.get(bucketSegmentIndex).getInt(bucketOffset)
                            != END_OF_LIST) {
                        if (step == 1) {
                            hashCode2 = calcSecondHashCode(hashCode1);
                        }
                        newPos = (int) ((hashCode1 + step * hashCode2) & numBucketsMask);
                        // which segment contains the bucket
                        bucketSegmentIndex = newPos >>> numBucketsPerSegmentBits;
                        // offset of the bucket in the segment
                        bucketOffset = (newPos & numBucketsPerSegmentMask) << BUCKET_SIZE_BITS;
                        step += STEP_INCREMENT;
                    }
                    newBucketSegments.get(bucketSegmentIndex).putInt(bucketOffset, recordPointer);
                    newBucketSegments
                            .get(bucketSegmentIndex)
                            .putInt(bucketOffset + ELEMENT_POINT_LENGTH, hashCode1);
                }
            }
        }
        LOG.info(
                "The rehash take {} ms for {} segments",
                (System.currentTimeMillis() - reHashStartTime),
                required);
        this.memoryPool.returnAll(this.bucketSegments);
        this.bucketSegments = newBucketSegments;
    }

    protected void returnSegments(List<MemorySegment> segments) {
        memoryPool.returnAll(segments);
    }

    private void setBucketVariables(List<MemorySegment> bucketSegments) {
        int numBuckets = bucketSegments.size() * numBucketsPerSegment;
        this.log2NumBuckets = MathUtils.log2strict(numBuckets);
        this.numBucketsMask = (1 << MathUtils.log2strict(numBuckets)) - 1;
        this.numBucketsMask2 = (1 << MathUtils.log2strict(numBuckets >> 1)) - 1;
        this.growthThreshold = (int) (numBuckets * LOAD_FACTOR);
    }

    // M(the num of buckets) is the nth power of 2,  so the second hash code must be odd, and always
    // is
    // H2(K) = 1 + 2 * ((H1(K)/M) mod (M-1))
    protected int calcSecondHashCode(final int firstHashCode) {
        return ((((firstHashCode >> log2NumBuckets)) & numBucketsMask2) << 1) + 1;
    }

    /** Record area. */
    interface RecordArea<K, V> {

        void setReadPosition(int position);

        boolean readKeyAndEquals(K lookupKey) throws IOException;

        V readValue(V reuse) throws IOException;

        int appendRecord(LookupInfo<K, V> lookupInfo, BinaryRowData value) throws IOException;

        long getSegmentsSize();

        void release();

        void reset();
    }

    /** Result fetched when looking up a key. */
    public static final class LookupInfo<K, V> {
        boolean found;
        K key;
        V value;

        /**
         * The hashcode of the look up key passed to {@link BytesMap#lookup(K)}, Caching this
         * hashcode here allows us to avoid re-hashing the key when inserting a value for that key.
         * The same purpose with bucketSegmentIndex, bucketOffset.
         */
        int keyHashCode;

        int bucketSegmentIndex;
        int bucketOffset;

        LookupInfo() {
            this.found = false;
            this.keyHashCode = -1;
            this.key = null;
            this.value = null;
            this.bucketSegmentIndex = -1;
            this.bucketOffset = -1;
        }

        void set(
                boolean found,
                int keyHashCode,
                K key,
                V value,
                int bucketSegmentIndex,
                int bucketOffset) {
            this.found = found;
            this.keyHashCode = keyHashCode;
            this.key = key;
            this.value = value;
            this.bucketSegmentIndex = bucketSegmentIndex;
            this.bucketOffset = bucketOffset;
        }

        public boolean isFound() {
            return found;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }
    }
}
