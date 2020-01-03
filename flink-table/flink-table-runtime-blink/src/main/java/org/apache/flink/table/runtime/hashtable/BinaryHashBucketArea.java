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
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.disk.RandomAccessInputView;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.util.MathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.runtime.hashtable.BaseHybridHashTable.partitionLevelHash;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Bucket area for hash table.
 *
 * <p>The layout of the buckets inside a memory segment is as follows:</p>
 * <pre>
 * +----------------------------- Bucket x ----------------------------
 * |element count (2 bytes) | probedFlags (2 bytes) | next-bucket-in-chain-pointer (4 bytes) |
 * |
 * |hashCode 1 (4 bytes) | hashCode 2 (4 bytes) | hashCode 3 (4 bytes) |
 * | ... hashCode n-1 (4 bytes) | hashCode n (4 bytes)
 * |
 * |pointer 1 (4 bytes) | pointer 2 (4 bytes) | pointer 3 (4 bytes) |
 * | ... pointer n-1 (4 bytes) | pointer n (4 bytes)
 * |
 * +---------------------------- Bucket x + 1--------------------------
 * | ...
 * |
 * </pre>
 */
public class BinaryHashBucketArea {

	private static final Logger LOG = LoggerFactory.getLogger(BinaryHashBucketArea.class);

	/**
	 * Log 2 of bucket size.
	 */
	static final int BUCKET_SIZE_BITS = 7;

	/**
	 * 128, bucket size of bytes.
	 */
	static final int BUCKET_SIZE = 0x1 << BUCKET_SIZE_BITS;

	/**
	 * The length of the hash code stored in the bucket.
	 */
	static final int HASH_CODE_LEN = 4;

	/**
	 * The length of a pointer from a hash bucket to the record in the buffers.
	 */
	static final int POINTER_LEN = 4;

	/**
	 * The number of bytes that the entry in the hash structure occupies, in bytes.
	 * It corresponds to a 4 byte hash value and an 4 byte pointer.
	 */
	public static final int RECORD_BYTES = HASH_CODE_LEN + POINTER_LEN;

	/**
	 * Offset of the field in the bucket header indicating the bucket's element count.
	 */
	static final int HEADER_COUNT_OFFSET = 0;

	/**
	 * Offset of the field in the bucket header that holds the probed bit set.
	 */
	static final int PROBED_FLAG_OFFSET = 2;

	/**
	 * Offset of the field in the bucket header that holds the forward pointer to its
	 * first overflow bucket.
	 */
	static final int HEADER_FORWARD_OFFSET = 4;

	/**
	 * Total length for bucket header.
	 */
	static final int BUCKET_HEADER_LENGTH = 8;

	/**
	 * The maximum number of elements that can be loaded in a bucket.
	 */
	static final int NUM_ENTRIES_PER_BUCKET = (BUCKET_SIZE - BUCKET_HEADER_LENGTH) / RECORD_BYTES;

	/**
	 * Offset of record pointer.
	 */
	static final int BUCKET_POINTER_START_OFFSET = BUCKET_HEADER_LENGTH + HASH_CODE_LEN * NUM_ENTRIES_PER_BUCKET;

	/**
	 * Constant for the forward pointer, indicating that the pointer is not set.
	 */
	static final int BUCKET_FORWARD_POINTER_NOT_SET = 0xFFFFFFFF;

	/**
	 * Constant for the bucket header to init. (count: 0, probedFlag: 0, forwardPointerNotSet: ~0x0)
	 */
	private static final long BUCKET_HEADER_INIT;

	static {
		if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
			BUCKET_HEADER_INIT = 0xFFFFFFFF00000000L;
		} else {
			BUCKET_HEADER_INIT = 0x00000000FFFFFFFFL;
		}
	}

	/**
	 * The load factor used when none specified in constructor.
	 */
	private static final double DEFAULT_LOAD_FACTOR = 0.75;

	final BinaryHashTable table;
	private final double estimatedRowCount;
	private final double loadFactor;
	private final boolean spillingAllowed;

	BinaryHashPartition partition;
	private int size;

	MemorySegment[] buckets;
	int numBuckets;
	private int numBucketsMask;
	// segments in which overflow buckets from the table structure are stored
	MemorySegment[] overflowSegments;
	int numOverflowSegments; // the number of actual segments in the overflowSegments array
	private int nextOverflowBucket; // the next free bucket in the current overflow segment
	private int threshold;

	private boolean inReHash = false;

	BinaryHashBucketArea(BinaryHashTable table, double estimatedRowCount, int maxSegs) {
		this(table, estimatedRowCount, maxSegs, DEFAULT_LOAD_FACTOR, true);
	}

	BinaryHashBucketArea(BinaryHashTable table, double estimatedRowCount, int maxSegs, boolean spillingAllowed) {
		this(table, estimatedRowCount, maxSegs, DEFAULT_LOAD_FACTOR, spillingAllowed);
	}

	private BinaryHashBucketArea(
			BinaryHashTable table,
			double estimatedRowCount,
			int maxSegs,
			double loadFactor,
			boolean spillingAllowed) {
		this.table = table;
		this.estimatedRowCount = estimatedRowCount;
		this.loadFactor = loadFactor;
		this.spillingAllowed = spillingAllowed;
		this.size = 0;

		int minNumBuckets = (int) Math.ceil((estimatedRowCount / loadFactor / NUM_ENTRIES_PER_BUCKET));
		int bucketNumSegs = MathUtils.roundDownToPowerOf2(Math.max(1, Math.min(maxSegs, (minNumBuckets >>> table.bucketsPerSegmentBits) +
				((minNumBuckets & table.bucketsPerSegmentMask) == 0 ? 0 : 1))));
		int numBuckets = bucketNumSegs << table.bucketsPerSegmentBits;

		int threshold = (int) (numBuckets * NUM_ENTRIES_PER_BUCKET * loadFactor);

		MemorySegment[] buckets = new MemorySegment[bucketNumSegs];
		table.ensureNumBuffersReturned(bucketNumSegs);

		// go over all segments that are part of the table
		for (int i = 0; i < bucketNumSegs; i++) {
			final MemorySegment seg = table.getNextBuffer();
			initMemorySegment(seg);
			buckets[i] = seg;
		}

		setNewBuckets(buckets, numBuckets, threshold);
	}

	private void setNewBuckets(MemorySegment[] buckets, int numBuckets, int threshold) {
		this.buckets = buckets;
		checkArgument(MathUtils.isPowerOf2(buckets.length));
		this.numBuckets = numBuckets;
		this.numBucketsMask = numBuckets - 1;
		this.overflowSegments = new MemorySegment[2];
		this.numOverflowSegments = 0;
		this.nextOverflowBucket = 0;
		this.threshold = threshold;
	}

	public void setPartition(BinaryHashPartition partition) {
		this.partition = partition;
	}

	private void resize() throws IOException {
		MemorySegment[] oldBuckets = this.buckets;
		int oldNumBuckets = numBuckets;
		MemorySegment[] oldOverflowSegments = overflowSegments;
		int newNumSegs = oldBuckets.length * 2;
		int newNumBuckets = newNumSegs << table.bucketsPerSegmentBits;
		int newThreshold = (int) (newNumBuckets * NUM_ENTRIES_PER_BUCKET * loadFactor);

		// We can't resize if not spillingAllowed and there are not enough buffers.
		if (!spillingAllowed && newNumSegs > table.remainBuffers()) {
			return;
		}

		// request new buckets.
		MemorySegment[] newBuckets = new MemorySegment[newNumSegs];
		for (int i = 0; i < newNumSegs; i++) {
			MemorySegment seg = table.getNextBuffer();
			if (seg == null) {
				final int spilledPart = table.spillPartition();
				if (spilledPart == partition.partitionNumber) {
					// this bucket is no longer in-memory
					// free new segments.
					for (int j = 0; j < i; j++) {
						table.free(newBuckets[j]);
					}
					return;
				}
				seg = table.getNextBuffer();
				if (seg == null) {
					throw new RuntimeException(
							"Bug in HybridHashJoin: No memory became available after spilling a partition.");
				}
			}
			initMemorySegment(seg);
			newBuckets[i] = seg;
		}

		setNewBuckets(newBuckets, newNumBuckets, newThreshold);

		reHash(oldBuckets, oldNumBuckets, oldOverflowSegments);
	}

	private void reHash(
			MemorySegment[] oldBuckets,
			int oldNumBuckets,
			MemorySegment[] oldOverflowSegments) throws IOException {
		long reHashStartTime = System.currentTimeMillis();
		inReHash = true;
		int scanCount = -1;
		while (true) {
			scanCount++;
			if (scanCount >= oldNumBuckets) {
				break;
			}
			// move to next bucket, update all the current bucket status with new bucket information.
			final int bucketArrayPos = scanCount >> table.bucketsPerSegmentBits;
			int bucketInSegOffset = (scanCount & table.bucketsPerSegmentMask) << BUCKET_SIZE_BITS;
			MemorySegment bucketSeg = oldBuckets[bucketArrayPos];

			int countInBucket = bucketSeg.getShort(bucketInSegOffset + HEADER_COUNT_OFFSET);
			int numInBucket = 0;
			while (countInBucket != 0) {
				int hashCodeOffset = bucketInSegOffset + BUCKET_HEADER_LENGTH;
				int pointerOffset = bucketInSegOffset + BUCKET_POINTER_START_OFFSET;
				while (numInBucket < countInBucket) {
					int hashCode = bucketSeg.getInt(hashCodeOffset);
					int pointer = bucketSeg.getInt(pointerOffset);
					if (!insertToBucket(hashCode, pointer, false)) {
						buildBloomFilterAndFree(oldBuckets, oldNumBuckets, oldOverflowSegments);
						return;
					}
					numInBucket++;
					hashCodeOffset += HASH_CODE_LEN;
					pointerOffset += POINTER_LEN;
				}

				// this segment is done. check if there is another chained bucket
				int forwardPointer = bucketSeg.getInt(bucketInSegOffset + HEADER_FORWARD_OFFSET);
				if (forwardPointer == BUCKET_FORWARD_POINTER_NOT_SET) {
					break;
				}

				final int overflowSegIndex = forwardPointer >>> table.segmentSizeBits;
				bucketSeg = oldOverflowSegments[overflowSegIndex];
				bucketInSegOffset = forwardPointer & table.segmentSizeMask;
				countInBucket = bucketSeg.getShort(bucketInSegOffset + HEADER_COUNT_OFFSET);
				numInBucket = 0;
			}
		}

		freeMemory(oldBuckets, oldOverflowSegments);
		inReHash = false;
		LOG.info("The rehash take {} ms for {} segments", (System.currentTimeMillis() - reHashStartTime), numBuckets);
	}

	private void initMemorySegment(MemorySegment seg) {
		// go over all buckets in the segment
		for (int k = 0; k < table.bucketsPerSegment; k++) {
			final int bucketOffset = k * BUCKET_SIZE;

			// init count and probeFlag and forward pointer together.
			seg.putLong(bucketOffset + HEADER_COUNT_OFFSET, BUCKET_HEADER_INIT);
		}
	}

	private boolean insertToBucket(
			MemorySegment bucket,
			int bucketInSegmentPos,
			int hashCode,
			int pointer,
			boolean sizeAddAndCheckResize) throws IOException {
		final int count = bucket.getShort(bucketInSegmentPos + HEADER_COUNT_OFFSET);
		if (count < NUM_ENTRIES_PER_BUCKET) {
			// we are good in our current bucket, put the values
			bucket.putShort(bucketInSegmentPos + HEADER_COUNT_OFFSET, (short) (count + 1)); // update count
			bucket.putInt(bucketInSegmentPos + BUCKET_HEADER_LENGTH + (count * HASH_CODE_LEN), hashCode);    // hash code
			bucket.putInt(bucketInSegmentPos + BUCKET_POINTER_START_OFFSET + (count * POINTER_LEN), pointer); // pointer
		} else {
			// we need to go to the overflow buckets
			final int originalForwardPointer = bucket.getInt(bucketInSegmentPos + HEADER_FORWARD_OFFSET);
			final int forwardForNewBucket;

			if (originalForwardPointer != BUCKET_FORWARD_POINTER_NOT_SET) {

				// forward pointer set
				final int overflowSegIndex = originalForwardPointer >>> table.segmentSizeBits;
				final int segOffset = originalForwardPointer & table.segmentSizeMask;
				final MemorySegment seg = overflowSegments[overflowSegIndex];

				final short obCount = seg.getShort(segOffset + HEADER_COUNT_OFFSET);

				// check if there is space in this overflow bucket
				if (obCount < NUM_ENTRIES_PER_BUCKET) {
					// space in this bucket and we are done
					seg.putShort(segOffset + HEADER_COUNT_OFFSET, (short) (obCount + 1)); // update count
					seg.putInt(segOffset + BUCKET_HEADER_LENGTH + (obCount * HASH_CODE_LEN), hashCode);    // hash code
					seg.putInt(segOffset + BUCKET_POINTER_START_OFFSET + (obCount * POINTER_LEN), pointer); // pointer
					return true;
				} else {
					// no space here, we need a new bucket. this current overflow bucket will be the
					// target of the new overflow bucket
					forwardForNewBucket = originalForwardPointer;
				}
			} else {
				// no overflow bucket yet, so we need a first one
				forwardForNewBucket = BUCKET_FORWARD_POINTER_NOT_SET;
			}

			// we need a new overflow bucket
			MemorySegment overflowSeg;
			final int overflowBucketNum;
			final int overflowBucketOffset;

			// first, see if there is space for an overflow bucket remaining in the last overflow segment
			if (nextOverflowBucket == 0) {
				// no space left in last bucket, or no bucket yet, so create an overflow segment
				overflowSeg = table.getNextBuffer();
				if (overflowSeg == null) {
					if (!spillingAllowed) {
						// In this corner case, we steal memory from heap.
						// Because the linked hash conflict solution, the required memory
						// calculation are not accurate, in this case, we apply for insufficient
						// memory from heap.
						// NOTE: must be careful, the steal memory should not return to table.
						overflowSeg = MemorySegmentFactory.allocateUnpooledSegment(table.segmentSize, this);
					} else {
						final int spilledPart = table.spillPartition();
						if (spilledPart == partition.partitionNumber) {
							// this bucket is no longer in-memory
							return false;
						}
						overflowSeg = table.getNextBuffer();
						if (overflowSeg == null) {
							throw new RuntimeException("Bug in HybridHashJoin: No memory became available after spilling a partition.");
						}
					}
				}
				overflowBucketOffset = 0;
				overflowBucketNum = numOverflowSegments;

				// add the new overflow segment
				if (overflowSegments.length <= numOverflowSegments) {
					MemorySegment[] newSegsArray = new MemorySegment[overflowSegments.length * 2];
					System.arraycopy(overflowSegments, 0, newSegsArray, 0, overflowSegments.length);
					overflowSegments = newSegsArray;
				}
				overflowSegments[numOverflowSegments] = overflowSeg;
				numOverflowSegments++;
			} else {
				// there is space in the last overflow bucket
				overflowBucketNum = numOverflowSegments - 1;
				overflowSeg = overflowSegments[overflowBucketNum];
				overflowBucketOffset = nextOverflowBucket << BUCKET_SIZE_BITS;
			}

			// next overflow bucket is one ahead. if the segment is full, the next will be at the beginning
			// of a new segment
			nextOverflowBucket = (nextOverflowBucket == table.bucketsPerSegmentMask ? 0 : nextOverflowBucket + 1);

			// insert the new overflow bucket in the chain of buckets
			// 1) set the old forward pointer
			// 2) let the bucket in the main table point to this one
			overflowSeg.putInt(overflowBucketOffset + HEADER_FORWARD_OFFSET, forwardForNewBucket);
			final int pointerToNewBucket = (overflowBucketNum << table.segmentSizeBits) + overflowBucketOffset;
			bucket.putInt(bucketInSegmentPos + HEADER_FORWARD_OFFSET, pointerToNewBucket);

			// finally, insert the values into the overflow buckets
			overflowSeg.putInt(overflowBucketOffset + BUCKET_HEADER_LENGTH, hashCode);    // hash code
			overflowSeg.putInt(overflowBucketOffset + BUCKET_POINTER_START_OFFSET, pointer); // pointer

			// set the count to one
			overflowSeg.putShort(overflowBucketOffset + HEADER_COUNT_OFFSET, (short) 1);

			// initiate the probed bitset to 0.
			overflowSeg.putShort(overflowBucketOffset + PROBED_FLAG_OFFSET, (short) 0);
		}

		if (sizeAddAndCheckResize && ++size > threshold) {
			resize();
		}
		return true;
	}

	private int findBucket(int hash) {
		// Avoid two layer hash conflict
		return partitionLevelHash(hash) & this.numBucketsMask;
	}

	/**
	 * Insert into bucket by hashCode and pointer.
	 * @return return false when spill own partition.
	 */
	boolean insertToBucket(int hashCode, int pointer, boolean sizeAddAndCheckResize) throws IOException {
		final int posHashCode = findBucket(hashCode);
		// get the bucket for the given hash code
		final int bucketArrayPos = posHashCode >> table.bucketsPerSegmentBits;
		final int bucketInSegmentPos = (posHashCode & table.bucketsPerSegmentMask) << BUCKET_SIZE_BITS;
		final MemorySegment bucket = this.buckets[bucketArrayPos];
		return insertToBucket(bucket, bucketInSegmentPos, hashCode, pointer, sizeAddAndCheckResize);
	}

	/**
	 * Append record and insert to bucket.
	 */
	boolean appendRecordAndInsert(BinaryRow record, int hashCode) throws IOException {
		final int posHashCode = findBucket(hashCode);
		// get the bucket for the given hash code
		final int bucketArrayPos = posHashCode >> table.bucketsPerSegmentBits;
		final int bucketInSegmentPos = (posHashCode & table.bucketsPerSegmentMask) << BUCKET_SIZE_BITS;
		final MemorySegment bucket = this.buckets[bucketArrayPos];

		if (!table.tryDistinctBuildRow ||
				!partition.isInMemory() ||
				!findFirstSameBuildRow(bucket, hashCode, bucketInSegmentPos, record)) {
			int pointer = partition.insertIntoBuildBuffer(record);
			if (pointer != -1) {
				// record was inserted into an in-memory partition. a pointer must be inserted into the buckets
				insertToBucket(bucket, bucketInSegmentPos, hashCode, pointer, true);
				return true;
			} else {
				return false;
			}
		} else {
			// distinct build rows in memory.
			return true;
		}
	}

	/**
	 * For distinct build.
	 */
	private boolean findFirstSameBuildRow(
			MemorySegment bucket,
			int searchHashCode,
			int bucketInSegmentOffset,
			BinaryRow buildRowToInsert) {
		int posInSegment = bucketInSegmentOffset + BUCKET_HEADER_LENGTH;
		int countInBucket = bucket.getShort(bucketInSegmentOffset + HEADER_COUNT_OFFSET);
		int numInBucket = 0;
		RandomAccessInputView view = partition.getBuildStateInputView();
		while (countInBucket != 0) {
			while (numInBucket < countInBucket) {

				final int thisCode = bucket.getInt(posInSegment);
				posInSegment += HASH_CODE_LEN;

				if (thisCode == searchHashCode) {
					final int pointer = bucket.getInt(bucketInSegmentOffset +
							BUCKET_POINTER_START_OFFSET + (numInBucket * POINTER_LEN));
					numInBucket++;
					try {
						view.setReadPosition(pointer);
						BinaryRow row = table.binaryBuildSideSerializer.mapFromPages(table.reuseBuildRow, view);
						if (buildRowToInsert.equals(row)) {
							return true;
						}
					} catch (IOException e) {
						throw new RuntimeException("Error deserializing key or value from the hashtable: " +
								e.getMessage(), e);
					}
				} else {
					numInBucket++;
				}
			}

			// this segment is done. check if there is another chained bucket
			final int forwardPointer = bucket.getInt(bucketInSegmentOffset + HEADER_FORWARD_OFFSET);
			if (forwardPointer == BUCKET_FORWARD_POINTER_NOT_SET) {
				return false;
			}

			final int overflowSegIndex = forwardPointer >>> table.segmentSizeBits;
			bucket = overflowSegments[overflowSegIndex];
			bucketInSegmentOffset = forwardPointer & table.segmentSizeMask;
			countInBucket = bucket.getShort(bucketInSegmentOffset + HEADER_COUNT_OFFSET);
			posInSegment = bucketInSegmentOffset + BUCKET_HEADER_LENGTH;
			numInBucket = 0;
		}
		return false;
	}

	/**
	 * Probe start lookup joined build rows.
	 */
	void startLookup(int hashCode) {
		final int posHashCode = findBucket(hashCode);

		// get the bucket for the given hash code
		final int bucketArrayPos = posHashCode >> table.bucketsPerSegmentBits;
		final int bucketInSegmentOffset = (posHashCode & table.bucketsPerSegmentMask) << BUCKET_SIZE_BITS;
		final MemorySegment bucket = this.buckets[bucketArrayPos];
		table.bucketIterator.set(bucket, overflowSegments, partition, hashCode, bucketInSegmentOffset);
	}

	void returnMemory(List<MemorySegment> target) {
		returnMemory(target, buckets, overflowSegments);
	}

	void freeMemory() {
		returnMemory(table.availableMemory, buckets, overflowSegments);
	}

	private void freeMemory(MemorySegment[] buckets, MemorySegment[] overflowSegments) {
		returnMemory(table.availableMemory, buckets, overflowSegments);
	}

	private void returnMemory(
			List<MemorySegment> target, MemorySegment[] buckets, MemorySegment[] overflowSegments) {
		Collections.addAll(target, buckets);
		for (MemorySegment segment : overflowSegments) {
			if (segment != null &&
					// except stealing from heap.
					segment.getOwner() != this) {
				target.add(segment);
			}
		}
	}

	/**
	 * Three situations:
	 * 1.Not use bloom filter, just free memory.
	 * 2.In rehash, free new memory and let rehash go build bloom filter from old memory.
	 * 3.Not in rehash and use bloom filter, build it and free memory.
	 */
	void buildBloomFilterAndFree() {
		if (inReHash || !table.useBloomFilters) {
			freeMemory();
		} else {
			buildBloomFilterAndFree(buckets, numBuckets, overflowSegments);
		}
	}

	private void buildBloomFilterAndFree(
			MemorySegment[] buckets,
			int numBuckets,
			MemorySegment[] overflowSegments) {
		if (table.useBloomFilters) {
			long numRecords = (long) Math.max(partition.getBuildSideRecordCount() * 1.5, estimatedRowCount);

			// BloomFilter size min of:
			// 1.remain buffers
			// 2.bf size for numRecords when fpp is 0.05
			// 3.max init bucket area buffers.
			int segSize = Math.min(
					Math.min(table.remainBuffers(),
					HashTableBloomFilter.optimalSegmentNumber(numRecords, table.pageSize(), 0.05)),
					table.maxInitBufferOfBucketArea(table.partitionsBeingBuilt.size()));

			if (segSize > 0) {
				HashTableBloomFilter filter = new HashTableBloomFilter(
						table.getNextBuffers(MathUtils.roundDownToPowerOf2(segSize)), numRecords);

				// Add all records to bloom filter.
				int scanCount = -1;
				while (true) {
					scanCount++;
					if (scanCount >= numBuckets) {
						break;
					}
					// move to next bucket, update all the current bucket status with new bucket information.
					final int bucketArrayPos = scanCount >> table.bucketsPerSegmentBits;
					int bucketInSegOffset = (scanCount & table.bucketsPerSegmentMask) << BUCKET_SIZE_BITS;
					MemorySegment bucketSeg = buckets[bucketArrayPos];

					int countInBucket = bucketSeg.getShort(bucketInSegOffset + HEADER_COUNT_OFFSET);
					int numInBucket = 0;
					while (countInBucket != 0) {
						int hashCodeOffset = bucketInSegOffset + BUCKET_HEADER_LENGTH;
						while (numInBucket < countInBucket) {
							filter.addHash(bucketSeg.getInt(hashCodeOffset));
							numInBucket++;
							hashCodeOffset += HASH_CODE_LEN;
						}

						// this segment is done. check if there is another chained bucket
						int forwardPointer = bucketSeg.getInt(bucketInSegOffset + HEADER_FORWARD_OFFSET);
						if (forwardPointer == BUCKET_FORWARD_POINTER_NOT_SET) {
							break;
						}

						final int overflowSegIndex = forwardPointer >>> table.segmentSizeBits;
						bucketSeg = overflowSegments[overflowSegIndex];
						bucketInSegOffset = forwardPointer & table.segmentSizeMask;
						countInBucket = bucketSeg.getShort(bucketInSegOffset + HEADER_COUNT_OFFSET);
						numInBucket = 0;
					}
				}

				partition.bloomFilter = filter;
			}
		}

		freeMemory(buckets, overflowSegments);
	}
}
