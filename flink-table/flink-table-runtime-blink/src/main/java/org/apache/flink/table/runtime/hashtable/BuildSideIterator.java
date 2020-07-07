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
import org.apache.flink.runtime.operators.util.BitSet;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.util.RowIterator;

import java.io.IOException;
import java.util.ArrayList;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Iterate all the elements in memory which has not been(or has been) probed during probe phase.
 */
public class BuildSideIterator implements RowIterator<BinaryRowData> {

	private final BinaryRowDataSerializer accessor;

	private final ArrayList<BinaryHashPartition> partitionsBeingBuilt;

	private final BitSet probedSet;

	private final boolean matchedOrUnmatched;

	private int areaIndex;

	private BinaryRowData reuse;

	private BucketIterator bucketIterator;

	BuildSideIterator(
			BinaryRowDataSerializer accessor,
			BinaryRowData reuse,
			ArrayList<BinaryHashPartition> partitionsBeingBuilt,
			BitSet probedSet,
			boolean matchedOrUnmatched) {
		this.accessor = accessor;
		this.partitionsBeingBuilt = partitionsBeingBuilt;
		this.probedSet = probedSet;
		this.reuse = reuse;
		this.matchedOrUnmatched = matchedOrUnmatched;
		this.areaIndex = -1;
	}

	@Override
	public boolean advanceNext() {
		if (bucketIterator != null && bucketIterator.advanceNext()) {
			return true;
		} else {
			areaIndex++;
			while (areaIndex < partitionsBeingBuilt.size()) {
				BinaryHashPartition partition = partitionsBeingBuilt.get(areaIndex);
				if (partition.isInMemory()) {
					BucketIterator iterator = new BucketIterator(partition.bucketArea, accessor, reuse, probedSet, matchedOrUnmatched);
					if (iterator.advanceNext()) {
						bucketIterator = iterator;
						return true;
					}
				}
				areaIndex++;
			}
			return false;
		}
	}

	@Override
	public BinaryRowData getRow() {
		return bucketIterator.getRow();
	}

	/**
	 * Partition bucket iterator.
	 */
	public static class BucketIterator implements RowIterator<BinaryRowData> {

		private BinaryHashBucketArea area;

		private final BinaryRowDataSerializer accessor;

		private final BitSet probedSet;

		private final boolean matchedOrUnmatched;

		private MemorySegment bucketSegment;

		private MemorySegment[] overflowSegments;

		private int scanCount;

		private int bucketInSegmentOffset;

		private int pointerOffset;

		private int countInBucket;

		private int numInBucket;

		private BinaryRowData reuse;

		private BinaryRowData instance;

		BucketIterator(
				BinaryHashBucketArea area,
				BinaryRowDataSerializer accessor,
				BinaryRowData reuse,
				BitSet probedSet,
				boolean matchedOrUnmatched) {
			this.area = area;
			this.accessor = accessor;
			this.probedSet = probedSet;
			this.reuse = reuse;
			this.matchedOrUnmatched = matchedOrUnmatched;

			scanCount = -1;
			moveToNextBucket();
		}

		@Override
		public boolean advanceNext() {
			// search unprobed record in bucket, while none found move to next bucket and search.
			while (true) {
				this.instance = nextInBucket(reuse);
				if (instance == null) {
					// return null while there are no more buckets.
					if (!moveToNextBucket()) {
						return false;
					}
				} else {
					return true;
				}
			}
		}

		@Override
		public BinaryRowData getRow() {
			return instance;
		}

		/**
		 * Move to next bucket, return true while move to a on heap bucket, return false while move
		 * to a spilled bucket
		 * or there is no more bucket.
		 */
		private boolean moveToNextBucket() {
			scanCount++;
			if (scanCount >= area.numBuckets) {
				return false;
			}
			// move to next bucket, update all the current bucket status with new bucket information.
			final int bucketArrayPos = scanCount >> area.table.bucketsPerSegmentBits;
			final int currentBucketInSegmentOffset = (scanCount & area.table.bucketsPerSegmentMask) << BinaryHashBucketArea.BUCKET_SIZE_BITS;
			MemorySegment currentBucket = area.buckets[bucketArrayPos];
			setBucket(currentBucket, area.overflowSegments, currentBucketInSegmentOffset);
			return true;
		}

		// update current bucket status.
		private void setBucket(
				MemorySegment bucket, MemorySegment[] overflowSegments,
				int bucketInSegmentOffset) {
			this.bucketSegment = bucket;
			this.overflowSegments = overflowSegments;
			this.bucketInSegmentOffset = bucketInSegmentOffset;
			this.pointerOffset = bucketInSegmentOffset + BinaryHashBucketArea.BUCKET_POINTER_START_OFFSET;
			this.countInBucket = bucket.getShort(bucketInSegmentOffset + BinaryHashBucketArea.HEADER_COUNT_OFFSET);
			this.numInBucket = 0;
			// reset probedSet with probedFlags offset in this bucket.
			this.probedSet.setMemorySegment(bucketSegment, this.bucketInSegmentOffset + BinaryHashBucketArea.PROBED_FLAG_OFFSET);
		}

		private BinaryRowData nextInBucket(BinaryRowData reuse) {
			// loop over all segments that are involved in the bucket (original bucket plus overflow buckets)
			while (countInBucket != 0) {

				checkNotNull(bucketSegment);

				while (this.numInBucket < this.countInBucket) {
					boolean probed = probedSet.get(numInBucket);
					numInBucket++;
					if (matchedOrUnmatched == probed) {
						try {
							this.area.partition.setReadPosition(bucketSegment.getInt(pointerOffset));
							reuse = this.accessor.mapFromPages(reuse, this.area.partition);
							this.pointerOffset += BinaryHashBucketArea.POINTER_LEN;
							return reuse;
						} catch (IOException ioex) {
							throw new RuntimeException("Error deserializing key or value from the hashtable: " +
									ioex.getMessage(), ioex);
						}
					} else {
						this.pointerOffset += BinaryHashBucketArea.POINTER_LEN;
					}
				}

				// this segment is done. check if there is another chained bucket
				final int forwardPointer = this.bucketSegment.getInt(this.bucketInSegmentOffset + BinaryHashBucketArea.HEADER_FORWARD_OFFSET);
				if (forwardPointer == BinaryHashBucketArea.BUCKET_FORWARD_POINTER_NOT_SET) {
					return null;
				}

				final int overflowSegIndex = forwardPointer >>> area.table.segmentSizeBits;
				this.bucketSegment = this.overflowSegments[overflowSegIndex];
				this.bucketInSegmentOffset = forwardPointer & area.table.segmentSizeMask;
				this.pointerOffset = bucketInSegmentOffset + BinaryHashBucketArea.BUCKET_POINTER_START_OFFSET;
				this.countInBucket = this.bucketSegment.getShort(this.bucketInSegmentOffset + BinaryHashBucketArea.HEADER_COUNT_OFFSET);
				this.numInBucket = 0;
				// reset probedSet with probedFlags offset in this bucket.
				this.probedSet.setMemorySegment(bucketSegment, this.bucketInSegmentOffset + BinaryHashBucketArea.PROBED_FLAG_OFFSET);
			}
			return null;
		}
	}
}
