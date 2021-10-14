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

package org.apache.flink.table.runtime.hashtable;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.util.RowIterator;
import org.apache.flink.util.MathUtils;

/** Build iterator from bucket to match probe row. */
public class LookupBucketIterator implements RowIterator<BinaryRowData> {

    private final BinaryHashTable table;
    private final int segmentSizeBits;
    private final int segmentSizeMask;

    private MemorySegment bucket;

    private MemorySegment[] overflowSegments;

    private BinaryHashPartition partition;

    private int bucketInSegmentOffset;

    private int pointerOffset;

    private int searchHashCode;

    private int hashCodeOffset;

    private int countInBucket;

    private int numInBucket;

    private BinaryRowData reuse;

    private BinaryRowData instance;

    LookupBucketIterator(BinaryHashTable table) {
        this.table = table;
        this.reuse = table.binaryBuildSideSerializer.createInstance();
        this.segmentSizeBits = MathUtils.log2strict(table.pageSize());
        this.segmentSizeMask = table.pageSize() - 1;
    }

    public void set(
            MemorySegment bucket,
            MemorySegment[] overflowSegments,
            BinaryHashPartition partition,
            int searchHashCode,
            int bucketInSegmentOffset) {
        this.bucket = bucket;
        this.overflowSegments = overflowSegments;
        this.partition = partition;
        this.searchHashCode = searchHashCode;
        this.bucketInSegmentOffset = bucketInSegmentOffset;
        this.pointerOffset =
                bucketInSegmentOffset + BinaryHashBucketArea.BUCKET_POINTER_START_OFFSET;
        this.hashCodeOffset =
                this.bucketInSegmentOffset + BinaryHashBucketArea.BUCKET_HEADER_LENGTH;
        this.countInBucket =
                bucket.getShort(bucketInSegmentOffset + BinaryHashBucketArea.HEADER_COUNT_OFFSET);
        this.numInBucket = 0;
    }

    @Override
    public boolean advanceNext() {
        // loop over all segments that are involved in the bucket (original bucket plus overflow
        // buckets)
        while (countInBucket != 0) {
            table.probedSet.setMemorySegment(
                    bucket, this.bucketInSegmentOffset + BinaryHashBucketArea.PROBED_FLAG_OFFSET);
            while (this.numInBucket < this.countInBucket) {

                final int thisCode = this.bucket.getInt(this.hashCodeOffset);
                this.hashCodeOffset += BinaryHashBucketArea.HASH_CODE_LEN;

                this.numInBucket++;

                // check if the hash code matches
                if (thisCode == this.searchHashCode) {
                    // get the pointer to the pair
                    final int pointer = bucket.getInt(pointerOffset);
                    pointerOffset += BinaryHashBucketArea.POINTER_LEN;

                    // deserialize the key to check whether it is really equal, or whether we had
                    // only a hash collision
                    try {
                        this.partition.setReadPosition(pointer);
                        BinaryRowData row =
                                table.binaryBuildSideSerializer.mapFromPages(reuse, this.partition);
                        if (table.applyCondition(row)) {
                            if (table.type.needSetProbed()) {
                                table.probedSet.set(numInBucket - 1);
                            }
                            this.instance = row;
                            return true;
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(
                                "Error deserializing key or value from the hashtable: "
                                        + e.getMessage(),
                                e);
                    }
                } else {
                    pointerOffset += BinaryHashBucketArea.POINTER_LEN;
                }
            }

            // this segment is done. check if there is another chained bucket
            final int forwardPointer =
                    this.bucket.getInt(
                            this.bucketInSegmentOffset
                                    + BinaryHashBucketArea.HEADER_FORWARD_OFFSET);
            if (forwardPointer == BinaryHashBucketArea.BUCKET_FORWARD_POINTER_NOT_SET) {
                this.instance = null;
                return false;
            }

            final int overflowSegIndex = forwardPointer >>> this.segmentSizeBits;
            this.bucket = this.overflowSegments[overflowSegIndex];
            this.bucketInSegmentOffset = forwardPointer & this.segmentSizeMask;
            this.pointerOffset =
                    bucketInSegmentOffset + BinaryHashBucketArea.BUCKET_POINTER_START_OFFSET;
            this.countInBucket =
                    this.bucket.getShort(
                            this.bucketInSegmentOffset + BinaryHashBucketArea.HEADER_COUNT_OFFSET);
            this.hashCodeOffset =
                    this.bucketInSegmentOffset + BinaryHashBucketArea.BUCKET_HEADER_LENGTH;
            this.numInBucket = 0;
        }
        this.instance = null;
        return false;
    }

    @Override
    public BinaryRowData getRow() {
        return instance;
    }
}
