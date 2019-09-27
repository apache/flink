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

package org.apache.flink.table.runtime.operators.aggregate;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.memory.MemorySegmentSource;
import org.apache.flink.runtime.io.disk.RandomAccessInputView;
import org.apache.flink.runtime.io.disk.SimpleCollectingOutputView;
import org.apache.flink.runtime.memory.AbstractPagedInputView;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.runtime.typeutils.BinaryRowSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Bytes based hash map.
 * It can be used for performing aggregations where the aggregated values are fixed-width, because
 * the data is stored in continuous memory, AggBuffer of variable length cannot be applied to this
 * HashMap.
 * The KeyValue form in hash map is designed to reduce the cost of key fetching in lookup.
 * The memory is divided into two areas:
 * <p/>
 * - Bucket area: this contains: pointer + hashcode.
 * Bytes 0 to 8: a pointer to the record in the record area
 * Bytes 8 to 16: a holds key's full 32-bit hashcode
 * <p/>
 * - Record area: this contains the actual data in linked list records.
 * A BytesHashMap's record has four parts:
 * Bytes 0 to 4: len(k)
 * Bytes 4 to 4 + len(k): key data
 * Bytes 4 + len(k) to 8 + len(k): len(v)
 * Bytes 8 + len(k) to 8 + len(k) + len(v): value data
 *
 * <p>{@code BytesHashMap} are influenced by Apache Spark BytesToBytesMap.
 */
public class BytesHashMap {

	private static final Logger LOG = LoggerFactory.getLogger(BytesHashMap.class);

	public static final int BUCKET_SIZE = 16;
	public static final int RECORD_EXTRA_LENGTH = 8;
	private static final int BUCKET_SIZE_BITS = 4;

	private static final int ELEMENT_POINT_LENGTH = 8;

	private static final long END_OF_LIST = Long.MAX_VALUE;
	private static final int STEP_INCREMENT = 1;

	private static final double LOAD_FACTOR = 0.75;
	//a smaller bucket can make the best of l1/l2/l3 cache.
	private static final long INIT_BUCKET_MEMORY_IN_BYTES = 1024 * 1024L;

	private final int numBucketsPerSegment;
	private final int numBucketsPerSegmentBits;
	private final int numBucketsPerSegmentMask;
	private final int lastBucketPosition;

	private final int segmentSize;
	/**
	 * The segments where the actual data is stored.
	 */
	private final RecordArea recordArea;
	/**
	 * Set true when valueTypeInfos.length == 0. Usually in this case the BytesHashMap will be used as a HashSet.
	 * The value from {@link BytesHashMap#append(LookupInfo info, BinaryRow value)}
	 * will be ignored when hashSetMode set.
	 * The reusedValue will always point to a 16 bytes long MemorySegment acted as
	 * each BytesHashMap entry's value part when appended to make the BytesHashMap's spilling work compatible.
	 */
	private final boolean hashSetMode;
	/**
	 * Used to serialize hash map key and value into RecordArea's MemorySegments.
	 */
	private final BinaryRowSerializer valueSerializer;
	private final BinaryRowSerializer keySerializer;
	/**
	 * Used as a reused object which lookup returned.
	 */
	private final LookupInfo reuseLookInfo;

	private final MemoryManager memoryManager;

	/**
	 * Used as a reused object when retrieve the map's value by key and iteration.
	 */
	private BinaryRow reusedValue;
	/**
	 * Used as a reused object when lookup and iteration.
	 */
	private BinaryRow reusedKey;

	private final List<MemorySegment> freeMemorySegments;
	private List<MemorySegment> bucketSegments;

	private long numElements = 0;
	private int numBucketsMask;
	//get the second hashcode based log2NumBuckets and numBucketsMask2
	private int log2NumBuckets;
	private int numBucketsMask2;

	/**
	 * The map will be expanded once the number of elements exceeds this threshold.
	 */
	private int growthThreshold;
	private volatile RecordArea.DestructiveEntryIterator destructiveIterator = null;

	private final int reservedNumBuffers;

	//metric
	private long numSpillFiles;
	private long spillInBytes;

	public BytesHashMap(
			final Object owner,
			MemoryManager memoryManager,
			long memorySize,
			LogicalType[] keyTypes,
			LogicalType[] valueTypes) {
		this(owner, memoryManager, memorySize, keyTypes, valueTypes, false);
	}

	public BytesHashMap(
			final Object owner,
			MemoryManager memoryManager,
			long memorySize,
			LogicalType[] keyTypes,
			LogicalType[] valueTypes,
			boolean inferBucketMemory) {
		this.segmentSize = memoryManager.getPageSize();
		this.reservedNumBuffers = (int) (memorySize / segmentSize);
		this.memoryManager = memoryManager;
		try {
			this.freeMemorySegments = memoryManager.allocatePages(owner, reservedNumBuffers);
		} catch (MemoryAllocationException e) {
			throw new IllegalArgumentException("BytesHashMap can't allocate " + reservedNumBuffers + " pages", e);
		}
		this.numBucketsPerSegment = segmentSize / BUCKET_SIZE;
		this.numBucketsPerSegmentBits = MathUtils.log2strict(this.numBucketsPerSegment);
		this.numBucketsPerSegmentMask = (1 << this.numBucketsPerSegmentBits) - 1;
		this.lastBucketPosition = (numBucketsPerSegment - 1) * BUCKET_SIZE;

		checkArgument(keyTypes.length > 0);
		this.keySerializer = new BinaryRowSerializer(keyTypes.length);
		this.reusedKey = this.keySerializer.createInstance();

		if (valueTypes.length == 0) {
			this.valueSerializer = new BinaryRowSerializer(0);
			this.hashSetMode = true;
			this.reusedValue = new BinaryRow(0);
			this.reusedValue.pointTo(MemorySegmentFactory.wrap(new byte[8]), 0, 8);
			LOG.info("BytesHashMap with hashSetMode = true.");
		} else {
			this.valueSerializer = new BinaryRowSerializer(valueTypes.length);
			this.hashSetMode = false;
			this.reusedValue = this.valueSerializer.createInstance();
		}

		this.reuseLookInfo = new LookupInfo();

		this.recordArea = new RecordArea();

		int initBucketSegmentNum;
		if (inferBucketMemory) {
			initBucketSegmentNum = calcNumBucketSegments(keyTypes, valueTypes);
		} else {
			checkArgument(memorySize > INIT_BUCKET_MEMORY_IN_BYTES, "The minBucketMemorySize is not valid!");
			initBucketSegmentNum = MathUtils.roundDownToPowerOf2((int) (INIT_BUCKET_MEMORY_IN_BYTES / segmentSize));
		}

		// allocate and initialize MemorySegments for bucket area
		initBucketSegments(initBucketSegmentNum);

		LOG.info("BytesHashMap with initial memory segments {}, {} in bytes, init allocating {} for bucket area.",
				reservedNumBuffers, reservedNumBuffers * segmentSize, initBucketSegmentNum);
	}

	static int getVariableLength(LogicalType[] types) {
		int length = 0;
		for (LogicalType type : types) {
			if (!BinaryRow.isInFixedLengthPart(type)) {
				// find a better way of computing generic type field variable-length
				// right now we use a small value assumption
				length += 16;
			}
		}
		return length;
	}

	private int calcNumBucketSegments(LogicalType[] keyTypes, LogicalType[] valueTypes) {
		int calcRecordLength = reusedValue.getFixedLengthPartSize() + getVariableLength(valueTypes) +
				reusedKey.getFixedLengthPartSize() + getVariableLength(keyTypes);
		// We aim for a 200% utilization of the bucket table.
		double averageBucketSize = BUCKET_SIZE / LOAD_FACTOR;
		double fraction = averageBucketSize / (averageBucketSize + calcRecordLength + RECORD_EXTRA_LENGTH);
		// We make the number of buckets a power of 2 so that taking modulo is efficient.
		// To avoid rehash as far as possible, here use roundUpToPowerOfTwo firstly
		int ret = Math.max(1, MathUtils.roundDownToPowerOf2((int) (reservedNumBuffers * fraction)));
		// We can't handle more than Integer.MAX_VALUE buckets (eg. because hash functions return int)
		if ((long) ret * numBucketsPerSegment > Integer.MAX_VALUE) {
			ret = MathUtils.roundDownToPowerOf2(Integer.MAX_VALUE / numBucketsPerSegment);
		}
		return ret;
	}

	// ----------------------- Public interface -----------------------

	/**
	 * @return true when BytesHashMap's valueTypeInfos.length == 0.
	 * Any appended value will be ignored and replaced with a reusedValue as a present tag.
	 */
	public boolean isHashSetMode() {
		return hashSetMode;
	}

	/**
	 * @param key by which looking up the value in the hash map.
	 *            Only support the key in the BinaryRow form who has only one MemorySegment.
	 * @return {@link LookupInfo}
	 */
	public LookupInfo lookup(BinaryRow key) {
		// check the looking up key having only one memory segment
		checkArgument(key.getSegments().length == 1);
		final int hashCode1 = key.hashCode();
		int newPos = hashCode1 & numBucketsMask;
		// which segment contains the bucket
		int bucketSegmentIndex = newPos >>> numBucketsPerSegmentBits;
		// offset of the bucket in the segment
		int bucketOffset = (newPos & numBucketsPerSegmentMask) << BUCKET_SIZE_BITS;

		boolean found = false;
		int step = STEP_INCREMENT;
		long hashCode2 = 0;
		long findElementPtr;
		try {
			do {
				findElementPtr = bucketSegments.get(bucketSegmentIndex).getLong(bucketOffset);
				if (findElementPtr == END_OF_LIST) {
					// This is a new key.
					break;
				} else {
					final int storedHashCode = bucketSegments.get(bucketSegmentIndex).getInt(
							bucketOffset + ELEMENT_POINT_LENGTH);
					if (hashCode1 == storedHashCode) {
						recordArea.setReadPosition(findElementPtr);
						if (recordArea.readKeyAndEquals(key)) {
							// we found an element with a matching key, and not just a hash collision
							found = true;
							reusedValue = recordArea.readValue(reusedValue);
							break;
						}
					}
				}
				if (step == 1) {
					hashCode2 = calcSecondHashCode(hashCode1);
				}
				newPos = (int) ((hashCode1 + step * hashCode2) & numBucketsMask);
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
		reuseLookInfo.set(
				found, hashCode1, key, reusedValue, bucketSegmentIndex, bucketOffset);
		return reuseLookInfo;
	}

	// M(the num of buckets) is the nth power of 2,  so the second hash code must be odd, and always is
	// H2(K) = 1 + 2 * ((H1(K)/M) mod (M-1))
	private long calcSecondHashCode(final int firstHashCode) {
		return ((((long) (firstHashCode >> log2NumBuckets)) & numBucketsMask2) << 1) + 1L;
	}

	/**
	 * Append an value into the hash map's record area.
	 *
	 * @return An BinaryRow mapping to the memory segments in the map's record area belonging to
	 * the newly appended value.
	 * @throws EOFException if the map can't allocate much more memory.
	 */
	public BinaryRow append(LookupInfo info, BinaryRow value) throws IOException {
		try {
			if (numElements >= growthThreshold) {
				growAndRehash();
				//update info's bucketSegmentIndex and bucketOffset
				lookup(info.key);
			}
			BinaryRow toAppend = hashSetMode ? reusedValue : value;
			long pointerToAppended = recordArea.appendRecord(info.key, toAppend);
			bucketSegments.get(info.bucketSegmentIndex).putLong(info.bucketOffset, pointerToAppended);
			bucketSegments.get(info.bucketSegmentIndex).putInt(
					info.bucketOffset + ELEMENT_POINT_LENGTH, info.keyHashCode);
			numElements++;
			recordArea.setReadPosition(pointerToAppended);
			recordArea.skipKey();
			return recordArea.readValue(reusedValue);
		} catch (EOFException e) {
			numSpillFiles++;
			spillInBytes += recordArea.segments.size() * ((long) segmentSize);
			throw e;
		}

	}

	public long getNumSpillFiles() {
		return numSpillFiles;
	}

	public long getUsedMemoryInBytes() {
		return (bucketSegments.size() + recordArea.segments.size()) * ((long) segmentSize);
	}

	public long getSpillInBytes() {
		return spillInBytes;
	}

	public long getNumElements() {
		return numElements;
	}

	private void initBucketSegments(int numBucketSegments) {
		if (numBucketSegments < 1) {
			throw new RuntimeException("Too small memory allocated for BytesHashMap");
		}
		this.bucketSegments = new ArrayList<>(numBucketSegments);
		for (int i = 0; i < numBucketSegments; i++) {
			bucketSegments.add(i, freeMemorySegments.remove(freeMemorySegments.size() - 1));
		}

		resetBucketSegments(this.bucketSegments);
		int numBuckets = numBucketSegments * numBucketsPerSegment;
		this.log2NumBuckets = MathUtils.log2strict(numBuckets);
		this.numBucketsMask = (1 << MathUtils.log2strict(numBuckets)) - 1;
		this.numBucketsMask2 = (1 << MathUtils.log2strict(numBuckets >> 1)) - 1;
		this.growthThreshold = (int) (numBuckets * LOAD_FACTOR);
	}

	private void resetBucketSegments(List<MemorySegment> resetBucketSegs) {
		for (MemorySegment segment: resetBucketSegs) {
			for (int j = 0; j <= lastBucketPosition; j += BUCKET_SIZE) {
				segment.putLong(j, END_OF_LIST);
			}
		}
	}

	/**
	 * @throws EOFException if the map can't allocate much more memory.
	 */
	private void growAndRehash() throws EOFException {
		// allocate the new data structures
		int required = 2 * bucketSegments.size();
		if (required * numBucketsPerSegment > Integer.MAX_VALUE) {
			LOG.warn("We can't handle more than Integer.MAX_VALUE buckets (eg. because hash functions return int)");
			throw new EOFException();
		}
		List<MemorySegment> newBucketSegments = new ArrayList<>(required);

		try {
			int freeNumSegments = freeMemorySegments.size();
			int numAllocatedSegments = required - freeNumSegments;
			if (numAllocatedSegments > 0) {
				throw new MemoryAllocationException();
			}
			int needNumFromFreeSegments = required - newBucketSegments.size();
			for (int end = needNumFromFreeSegments; end > 0; end--) {
				newBucketSegments.add(freeMemorySegments.remove(freeMemorySegments.size() - 1));
			}

			int numBuckets = newBucketSegments.size() * numBucketsPerSegment;
			this.log2NumBuckets = MathUtils.log2strict(numBuckets);
			this.numBucketsMask = (1 << MathUtils.log2strict(numBuckets)) - 1;
			this.numBucketsMask2 = (1 << MathUtils.log2strict(numBuckets >> 1)) - 1;
			this.growthThreshold = (int) (numBuckets * LOAD_FACTOR);
		} catch (MemoryAllocationException e) {
			LOG.warn("BytesHashMap can't allocate {} pages, and now used {} pages",
					required, reservedNumBuffers, e);
			throw new EOFException();
		}
		long reHashStartTime = System.currentTimeMillis();
		resetBucketSegments(newBucketSegments);
		// Re-mask (we don't recompute the hashcode because we stored all 32 bits of it)
		for (MemorySegment memorySegment : bucketSegments) {
			for (int j = 0; j < numBucketsPerSegment; j++) {
				final long recordPointer = memorySegment.getLong(j * BUCKET_SIZE);
				if (recordPointer != END_OF_LIST) {
					final int hashCode1 = memorySegment.getInt(j * BUCKET_SIZE + ELEMENT_POINT_LENGTH);
					int newPos = hashCode1 & numBucketsMask;
					int bucketSegmentIndex = newPos >>> numBucketsPerSegmentBits;
					int bucketOffset = (newPos & numBucketsPerSegmentMask) << BUCKET_SIZE_BITS;
					int step = STEP_INCREMENT;
					long hashCode2 = 0;
					while (newBucketSegments.get(bucketSegmentIndex).getLong(bucketOffset) != END_OF_LIST) {
						if (step == 1) {
							hashCode2 = calcSecondHashCode(hashCode1);
						}
						newPos = (int) ((hashCode1 + step * hashCode2) & numBucketsMask);
						bucketSegmentIndex = newPos >>> numBucketsPerSegmentBits;
						bucketOffset = (newPos & numBucketsPerSegmentMask) << BUCKET_SIZE_BITS;
						step += STEP_INCREMENT;
					}
					newBucketSegments.get(bucketSegmentIndex).putLong(bucketOffset, recordPointer);
					newBucketSegments.get(bucketSegmentIndex).putInt(bucketOffset + ELEMENT_POINT_LENGTH, hashCode1);
				}
			}
		}
		LOG.info("The rehash take {} ms for {} segments", (System.currentTimeMillis() - reHashStartTime), required);
		this.freeMemorySegments.addAll(this.bucketSegments);
		this.bucketSegments = newBucketSegments;
	}

	/**
	 * Returns a destructive iterator for iterating over the entries of this map. It frees each page
	 * as it moves onto next one. Notice: it is illegal to call any method on the map after
	 * `destructiveIterator()` has been called.
	 * @return an entry iterator for iterating the value appended in the hash map.
	 */
	public MutableObjectIterator<Entry> getEntryIterator() {
		if (destructiveIterator != null) {
			throw new IllegalArgumentException("DestructiveIterator is not null, so this method can't be invoke!");
		}
		return recordArea.destructiveEntryIterator();
	}

	/**
	 * @return the underlying memory segments of the hash map's record area
	 */
	public ArrayList<MemorySegment> getRecordAreaMemorySegments() {
		return recordArea.segments;
	}

	public List<MemorySegment> getBucketAreaMemorySegments() {
		return bucketSegments;
	}

	/**
	 * release the map's record and bucket area's memory segments.
	 */
	public void free() {
		free(false);
	}

	/**
	 * @param reservedRecordMemory reserved fixed memory or not.
	 */
	public void free(boolean reservedRecordMemory) {
		returnSegments(this.bucketSegments);
		this.bucketSegments.clear();
		recordArea.release();
		if (!reservedRecordMemory) {
			memoryManager.release(freeMemorySegments);
		}
		numElements = 0;
		destructiveIterator = null;
	}


	/**
	 * reset the map's record and bucket area's memory segments for reusing.
	 */
	public void reset() {
		int numBuckets = bucketSegments.size() * numBucketsPerSegment;
		this.log2NumBuckets = MathUtils.log2strict(numBuckets);
		this.numBucketsMask = (1 << MathUtils.log2strict(numBuckets)) - 1;
		this.numBucketsMask2 = (1 << MathUtils.log2strict(numBuckets >> 1)) - 1;
		this.growthThreshold = (int) (numBuckets * LOAD_FACTOR);
		//reset the record segments.
		recordArea.reset();
		resetBucketSegments(bucketSegments);
		numElements = 0;
		destructiveIterator = null;
		LOG.info("reset BytesHashMap with record memory segments {}, {} in bytes, init allocating {} for bucket area.",
				freeMemorySegments.size(), freeMemorySegments.size() * segmentSize, bucketSegments.size());
	}

	private void returnSegments(List<MemorySegment> segments) {
		freeMemorySegments.addAll(segments);
	}

	// ----------------------- Record Area -----------------------

	private final class RecordArea {
		private final ArrayList<MemorySegment> segments = new ArrayList<>();

		private final RandomAccessInputView inView;
		private final SimpleCollectingOutputView outView;

		RecordArea() {
			this.outView = new SimpleCollectingOutputView(segments, new RecordAreaMemorySource(), segmentSize);
			this.inView = new RandomAccessInputView(segments, segmentSize);
		}

		void release() {
			returnSegments(segments);
			segments.clear();
		}

		void reset() {
			release();
			// request a new memory segment from freeMemorySegments
			// reset segmentNum and positionInSegment
			outView.reset();
			inView.setReadPosition(0);
		}

		// ----------------------- Memory Management -----------------------

		private final class RecordAreaMemorySource implements MemorySegmentSource {
			@Override
			public MemorySegment nextSegment() {
				int s = freeMemorySegments.size();
				if (s > 0) {
					return freeMemorySegments.remove(s - 1);
				} else {
					return null;
				}
			}
		}

		// ----------------------- Append -----------------------
		private long appendRecord(BinaryRow key, BinaryRow value) throws IOException {
			final long oldLastPosition = outView.getCurrentOffset();
			// serialize the key into the BytesHashMap record area
			int skip = keySerializer.serializeToPages(key, outView);

			// serialize the value into the BytesHashMap record area
			valueSerializer.serializeToPages(value, outView);
			return oldLastPosition + skip;
		}

		// ----------------------- Read -----------------------
		void setReadPosition(long position) {
			inView.setReadPosition(position);
		}

		boolean readKeyAndEquals(BinaryRow lookup) throws IOException {
			reusedKey = keySerializer.mapFromPages(reusedKey, inView);
			return lookup.equals(reusedKey);
		}

		/**
		 * @throws IOException when invalid memory address visited.
		 */
		void skipKey() throws IOException {
			inView.skipBytes(inView.readInt());
		}

		BinaryRow readValue(BinaryRow reuse) throws IOException {
			// depends on BinaryRowSerializer to check writing skip
			// and to find the real start offset of the data
			return valueSerializer.mapFromPages(reuse, inView);
		}

		// ----------------------- Iterator -----------------------

		MutableObjectIterator<Entry> destructiveEntryIterator() {
			return new RecordArea.DestructiveEntryIterator();
		}

		final class DestructiveEntryIterator extends AbstractPagedInputView implements MutableObjectIterator<Entry> {

			private int count = 0;
			private int currentSegmentIndex = 0;

			public DestructiveEntryIterator() {
				super(segments.get(0), segmentSize, 0);
				destructiveIterator = this;
			}

			public boolean hasNext() {
				return count < numElements;
			}

			@Override
			public Entry next(Entry reuse) throws IOException {
				if (hasNext()) {
					count++;
					//segment already is useless any more.
					keySerializer.mapFromPages(reuse.getKey(), this);
					valueSerializer.mapFromPages(reuse.getValue(), this);
					return reuse;
				} else {
					return null;
				}
			}

			@Override
			public Entry next() throws IOException {
				throw new UnsupportedOperationException("");
			}

			@Override
			protected int getLimitForSegment(MemorySegment segment) {
				return segmentSize;
			}

			@Override
			protected MemorySegment nextSegment(MemorySegment current) throws EOFException, IOException {
				return segments.get(++currentSegmentIndex);
			}
		}
	}

	/**
	 * Handle returned by {@link BytesHashMap#lookup(BinaryRow)} function.
	 */
	public static final class LookupInfo {
		private boolean found;
		private BinaryRow key;
		private BinaryRow value;

		/**
		 * The hashcode of the look up key passed to {@link BytesHashMap#lookup(BinaryRow)},
		 * Caching this hashcode here allows us to avoid re-hashing the key when inserting a value
		 * for that key.
		 * The same purpose with bucketSegmentIndex, bucketOffset.
		 */
		private int keyHashCode;
		private int bucketSegmentIndex;
		private int bucketOffset;

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
				BinaryRow key,
				BinaryRow value,
				int bucketSegmentIndex, int bucketOffset) {
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

		public BinaryRow getValue() {
			return value;
		}
	}

	/**
	 * BytesHashMap Entry contains key and value field.
	 */
	public static final class Entry {
		private final BinaryRow key;
		private final BinaryRow value;

		public Entry(BinaryRow key, BinaryRow value) {
			this.key = key;
			this.value = value;
		}

		public BinaryRow getKey() {
			return key;
		}

		public BinaryRow getValue() {
			return value;
		}
	}
}
