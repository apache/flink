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

package org.apache.flink.runtime.operators.hash;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.SameTypePairComparator;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.RandomAccessInputView;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This hash table supports updating elements. If the new element has the same size as the old element, then
 * the update is done in-place. Otherwise a hole is created at the place of the old record, which will
 * eventually be removed by a compaction.
 *
 * The memory is divided into three areas:
 *  - Bucket area: they contain bucket heads:
 *    an 8 byte pointer to the first link of a linked list in the record area
 *  - Record area: this contains the actual data in linked list elements. A linked list element starts
 *    with an 8 byte pointer to the next element, and then the record follows.
 *  - Staging area: This is a small, temporary storage area for writing updated records. This is needed,
 *    because before serializing a record, there is no way to know in advance how large will it be.
 *    Therefore, we can't serialize directly into the record area when we are doing an update, because
 *    if it turns out to be larger than the old record, then it would override some other record
 *    that happens to be after the old one in memory. The solution is to serialize to the staging area first,
 *    and then copy it to the place of the original if it has the same size, otherwise allocate a new linked
 *    list element at the end of the record area, and mark the old one as abandoned. This creates "holes" in
 *    the record area, so compactions are eventually needed.
 *
 * Compaction happens by deleting everything in the bucket area, and then reinserting all elements.
 * The reinsertion happens by forgetting the structure (the linked lists) of the record area, and reading it
 * sequentially, and inserting all non-abandoned records, starting from the beginning of the record area.
 * Note, that insertions never override a record that hasn't been read by the reinsertion sweep, because
 * both the insertions and readings happen sequentially in the record area, and the insertions obviously
 * never overtake the reading sweep.
 *
 * Note: we have to abandon the old linked list element even when the updated record has a smaller size
 * than the original, because otherwise we wouldn't know where the next record starts during a reinsertion
 * sweep.
 *
 * The number of buckets depends on how large are the records. The serializer might be able to tell us this,
 * so in this case, we will calculate the number of buckets upfront, and won't do resizes.
 * If the serializer doesn't know the size, then we start with a small number of buckets, and do resizes as more
 * elements are inserted than the number of buckets.
 *
 * The number of memory segments given to the staging area is usually one, because it just needs to hold
 * one record.
 *
 * Note: For hashing, we couldn't just take the lower bits, but have to use a proper hash function from
 * MathUtils because of its avalanche property, so that changing only some high bits of
 * the original value won't leave the lower bits of the hash unaffected.
 * This is because when choosing the bucket for a record, we mask only the
 * lower bits (see numBucketsMask). Lots of collisions would occur when, for example,
 * the original value that is hashed is some bitset, where lots of different values
 * that are different only in the higher bits will actually occur.
 */

public class InPlaceMutableHashTable<T> extends AbstractMutableHashTable<T> {

	private static final Logger LOG = LoggerFactory.getLogger(InPlaceMutableHashTable.class);

	/** The minimum number of memory segments InPlaceMutableHashTable needs to be supplied with in order to work. */
	private static final int MIN_NUM_MEMORY_SEGMENTS = 3;

	// Note: the following two constants can't be negative, because negative values are reserved for storing the
	// negated size of the record, when it is abandoned (not part of any linked list).

	/** The last link in the linked lists will have this as next pointer. */
	private static final long END_OF_LIST = Long.MAX_VALUE;

	/** This value means that prevElemPtr is "pointing to the bucket head", and not into the record segments. */
	private static final long INVALID_PREV_POINTER = Long.MAX_VALUE - 1;


	private static final long RECORD_OFFSET_IN_LINK = 8;


	/**
	 * This initially contains all the memory we have, and then segments
	 * are taken from it by bucketSegments, recordArea, and stagingSegments.
	 */
	private final ArrayList<MemorySegment> freeMemorySegments;

	private final int numAllMemorySegments;

	private final int segmentSize;

	/**
	 * These will contain the bucket heads.
	 * The bucket heads are pointers to the linked lists containing the actual records.
	 */
	private MemorySegment[] bucketSegments;

	private static final int bucketSize = 8, bucketSizeBits = 3;

	private int numBuckets;
	private int numBucketsMask;
	private final int numBucketsPerSegment, numBucketsPerSegmentBits, numBucketsPerSegmentMask;

	/**
	 * The segments where the actual data is stored.
	 */
	private final RecordArea recordArea;

	/**
	 * Segments for the staging area.
	 * (It should contain at most one record at all times.)
	 */
	private final ArrayList<MemorySegment> stagingSegments;
	private final RandomAccessInputView stagingSegmentsInView;
	private final StagingOutputView stagingSegmentsOutView;

	private T reuse;

	/** This is the internal prober that insertOrReplaceRecord uses. */
	private final HashTableProber<T> prober;

	/** The number of elements currently held by the table. */
	private long numElements = 0;

	/** The number of bytes wasted by updates that couldn't overwrite the old record due to size change. */
	private long holes = 0;

	/**
	 * If the serializer knows the size of the records, then we can calculate the optimal number of buckets
	 * upfront, so we don't need resizes.
	 */
	private boolean enableResize;


	public InPlaceMutableHashTable(TypeSerializer<T> serializer, TypeComparator<T> comparator, List<MemorySegment> memory) {
		super(serializer, comparator);
		this.numAllMemorySegments = memory.size();
		this.freeMemorySegments = new ArrayList<>(memory);

		// some sanity checks first
		if (freeMemorySegments.size() < MIN_NUM_MEMORY_SEGMENTS) {
			throw new IllegalArgumentException("Too few memory segments provided. InPlaceMutableHashTable needs at least " +
				MIN_NUM_MEMORY_SEGMENTS + " memory segments.");
		}

		// Get the size of the first memory segment and record it. All further buffers must have the same size.
		// the size must also be a power of 2
		segmentSize = freeMemorySegments.get(0).size();
		if ( (segmentSize & segmentSize - 1) != 0) {
			throw new IllegalArgumentException("Hash Table requires buffers whose size is a power of 2.");
		}

		this.numBucketsPerSegment = segmentSize / bucketSize;
		this.numBucketsPerSegmentBits = MathUtils.log2strict(this.numBucketsPerSegment);
		this.numBucketsPerSegmentMask = (1 << this.numBucketsPerSegmentBits) - 1;

		recordArea = new RecordArea(segmentSize);

		stagingSegments = new ArrayList<>();
		stagingSegments.add(forcedAllocateSegment());
		stagingSegmentsInView = new RandomAccessInputView(stagingSegments, segmentSize);
		stagingSegmentsOutView = new StagingOutputView(stagingSegments, segmentSize);

		prober = new HashTableProber<>(buildSideComparator, new SameTypePairComparator<>(buildSideComparator));

		enableResize = buildSideSerializer.getLength() == -1;
	}

	/**
	 * Gets the total capacity of this hash table, in bytes.
	 *
	 * @return The hash table's total capacity.
	 */
	public long getCapacity() {
		return numAllMemorySegments * (long)segmentSize;
	}

	/**
	 * Gets the number of bytes currently occupied in this hash table.
	 *
	 * @return The number of bytes occupied.
	 */
	public long getOccupancy() {
		return numAllMemorySegments * segmentSize - freeMemorySegments.size() * segmentSize;
	}

	private void open(int numBucketSegments) {
		synchronized (stateLock) {
			if (!closed) {
				throw new IllegalStateException("currently not closed.");
			}
			closed = false;
		}

		allocateBucketSegments(numBucketSegments);

		stagingSegments.add(forcedAllocateSegment());

		reuse = buildSideSerializer.createInstance();
	}

	/**
	 * Initialize the hash table
	 */
	@Override
	public void open() {
		open(calcInitialNumBucketSegments());
	}

	@Override
	public void close() {
		// make sure that we close only once
		synchronized (stateLock) {
			if (closed) {
				// We have to do this here, because the ctor already allocates a segment to the record area and
				// the staging area, even before we are opened. So we might have segments to free, even if we
				// are closed.
				recordArea.giveBackSegments();
				freeMemorySegments.addAll(stagingSegments);
				stagingSegments.clear();

				return;
			}
			closed = true;
		}

		LOG.debug("Closing InPlaceMutableHashTable and releasing resources.");

		releaseBucketSegments();

		recordArea.giveBackSegments();

		freeMemorySegments.addAll(stagingSegments);
		stagingSegments.clear();

		numElements = 0;
		holes = 0;
	}

	@Override
	public void abort() {
		LOG.debug("Aborting InPlaceMutableHashTable.");
		close();
	}

	@Override
	public List<MemorySegment> getFreeMemory() {
		if (!this.closed) {
			throw new IllegalStateException("Cannot return memory while InPlaceMutableHashTable is open.");
		}

		return freeMemorySegments;
	}

	private int calcInitialNumBucketSegments() {
		int recordLength = buildSideSerializer.getLength();
		double fraction; // fraction of memory to use for the buckets
		if (recordLength == -1) {
			// We don't know the record length, so we start with a small number of buckets, and do resizes if
			// necessary.
			// It seems that resizing is quite efficient, so we can err here on the too few bucket segments side.
			// Even with small records, we lose only ~15% speed.
			fraction = 0.1;
		} else {
			// We know the record length, so we can find a good value for the number of buckets right away, and
			// won't need any resizes later. (enableResize is false in this case, so no resizing will happen.)
			// Reasoning behind the formula:
			// We are aiming for one bucket per record, and one bucket contains one 8 byte pointer. The total
			// memory overhead of an element will be approximately 8+8 bytes, as the record in the record area
			// is preceded by a pointer (for the linked list).
			fraction = 8.0 / (16 + recordLength);
		}

		// We make the number of buckets a power of 2 so that taking modulo is efficient.
		int ret = Math.max(1, MathUtils.roundDownToPowerOf2((int)(numAllMemorySegments * fraction)));

		// We can't handle more than Integer.MAX_VALUE buckets (eg. because hash functions return int)
		if ((long)ret * numBucketsPerSegment > Integer.MAX_VALUE) {
			ret = MathUtils.roundDownToPowerOf2(Integer.MAX_VALUE / numBucketsPerSegment);
		}
		return ret;
	}

	private void allocateBucketSegments(int numBucketSegments) {
		if (numBucketSegments < 1) {
			throw new RuntimeException("Bug in InPlaceMutableHashTable");
		}

		bucketSegments = new MemorySegment[numBucketSegments];
		for(int i = 0; i < bucketSegments.length; i++) {
			bucketSegments[i] = forcedAllocateSegment();
			// Init all pointers in all buckets to END_OF_LIST
			for(int j = 0; j < numBucketsPerSegment; j++) {
				bucketSegments[i].putLong(j << bucketSizeBits, END_OF_LIST);
			}
		}
		numBuckets = numBucketSegments * numBucketsPerSegment;
		numBucketsMask = (1 << MathUtils.log2strict(numBuckets)) - 1;
	}

	private void releaseBucketSegments() {
		freeMemorySegments.addAll(Arrays.asList(bucketSegments));
		bucketSegments = null;
	}

	private MemorySegment allocateSegment() {
		int s = freeMemorySegments.size();
		if (s > 0) {
			return freeMemorySegments.remove(s - 1);
		} else {
			return null;
		}
	}

	private MemorySegment forcedAllocateSegment() {
		MemorySegment segment = allocateSegment();
		if (segment == null) {
			throw new RuntimeException("Bug in InPlaceMutableHashTable: A free segment should have been available.");
		}
		return segment;
	}

	/**
	 * Searches the hash table for a record with the given key.
	 * If it is found, then it is overridden with the specified record.
	 * Otherwise, the specified record is inserted.
	 * @param record The record to insert or to replace with.
	 * @throws IOException (EOFException specifically, if memory ran out)
     */
	@Override
	public void insertOrReplaceRecord(T record) throws IOException {
		if (closed) {
			return;
		}

		T match = prober.getMatchFor(record, reuse);
		if (match == null) {
			prober.insertAfterNoMatch(record);
		} else {
			prober.updateMatch(record);
		}
	}

	/**
	 * Inserts the given record into the hash table.
	 * Note: this method doesn't care about whether a record with the same key is already present.
	 * @param record The record to insert.
	 * @throws IOException (EOFException specifically, if memory ran out)
     */
	@Override
	public void insert(T record) throws IOException {
		if (closed) {
			return;
		}

		final int hashCode = MathUtils.jenkinsHash(buildSideComparator.hash(record));
		final int bucket = hashCode & numBucketsMask;
		final int bucketSegmentIndex = bucket >>> numBucketsPerSegmentBits; // which segment contains the bucket
		final MemorySegment bucketSegment = bucketSegments[bucketSegmentIndex];
		final int bucketOffset = (bucket & numBucketsPerSegmentMask) << bucketSizeBits; // offset of the bucket in the segment
		final long firstPointer = bucketSegment.getLong(bucketOffset);

		try {
			final long newFirstPointer = recordArea.appendPointerAndRecord(firstPointer, record);
			bucketSegment.putLong(bucketOffset, newFirstPointer);
		} catch (EOFException ex) {
			compactOrThrow();
			insert(record);
			return;
		}

		numElements++;
		resizeTableIfNecessary();
	}

	private void resizeTableIfNecessary() throws IOException {
		if (enableResize && numElements > numBuckets) {
			final long newNumBucketSegments = 2L * bucketSegments.length;
			// Checks:
			// - we can't handle more than Integer.MAX_VALUE buckets
			// - don't take more memory than the free memory we have left
			// - the buckets shouldn't occupy more than half of all our memory
			if (newNumBucketSegments * numBucketsPerSegment < Integer.MAX_VALUE &&
				newNumBucketSegments - bucketSegments.length < freeMemorySegments.size() &&
				newNumBucketSegments < numAllMemorySegments / 2) {
				// do the resize
				rebuild(newNumBucketSegments);
			}
		}
	}

	/**
	 * Returns an iterator that can be used to iterate over all the elements in the table.
	 * WARNING: Doing any other operation on the table invalidates the iterator! (Even
	 * using getMatchFor of a prober!)
	 * @return the iterator
     */
	@Override
	public EntryIterator getEntryIterator() {
		return new EntryIterator();
	}

	public <PT> HashTableProber<PT> getProber(TypeComparator<PT> probeTypeComparator, TypePairComparator<PT, T> pairComparator) {
		return new HashTableProber<>(probeTypeComparator, pairComparator);
	}

	/**
	 * This function reinitializes the bucket segments,
	 * reads all records from the record segments (sequentially, without using the pointers or the buckets),
	 * and rebuilds the hash table.
	 */
	private void rebuild() throws IOException {
		rebuild(bucketSegments.length);
	}

	/** Same as above, but the number of bucket segments of the new table can be specified. */
	private void rebuild(long newNumBucketSegments) throws IOException {
		// Get new bucket segments
		releaseBucketSegments();
		allocateBucketSegments((int)newNumBucketSegments);

		T record = buildSideSerializer.createInstance();
		try {
			EntryIterator iter = getEntryIterator();
			recordArea.resetAppendPosition();
			recordArea.setWritePosition(0);
			while ((record = iter.next(record)) != null && !closed) {
				final int hashCode = MathUtils.jenkinsHash(buildSideComparator.hash(record));
				final int bucket = hashCode & numBucketsMask;
				final int bucketSegmentIndex = bucket >>> numBucketsPerSegmentBits; // which segment contains the bucket
				final MemorySegment bucketSegment = bucketSegments[bucketSegmentIndex];
				final int bucketOffset = (bucket & numBucketsPerSegmentMask) << bucketSizeBits; // offset of the bucket in the segment
				final long firstPointer = bucketSegment.getLong(bucketOffset);

				long ptrToAppended = recordArea.noSeekAppendPointerAndRecord(firstPointer, record);
				bucketSegment.putLong(bucketOffset, ptrToAppended);
			}
			recordArea.freeSegmentsAfterAppendPosition();
			holes = 0;

		} catch (EOFException ex) {
			throw new RuntimeException("Bug in InPlaceMutableHashTable: we shouldn't get out of memory during a rebuild, " +
				"because we aren't allocating any new memory.");
		}
	}

	/**
	 * If there is wasted space (due to updated records not fitting in their old places), then do a compaction.
	 * Else, throw EOFException to indicate that memory ran out.
	 * @throws IOException
	 */
	private void compactOrThrow() throws IOException {
		if (holes > (double)recordArea.getTotalSize() * 0.05) {
			rebuild();
		} else {
			throw new EOFException("InPlaceMutableHashTable memory ran out. " + getMemoryConsumptionString());
		}
	}

	/**
	 * @return String containing a summary of the memory consumption for error messages
	 */
	private String getMemoryConsumptionString() {
		return "InPlaceMutableHashTable memory stats:\n" +
			"Total memory:     " + numAllMemorySegments * segmentSize + "\n" +
			"Free memory:      " + freeMemorySegments.size() * segmentSize + "\n" +
			"Bucket area:      " + numBuckets * 8  + "\n" +
			"Record area:      " + recordArea.getTotalSize() + "\n" +
			"Staging area:     " + stagingSegments.size() * segmentSize + "\n" +
			"Num of elements:  " + numElements + "\n" +
			"Holes total size: " + holes;
	}


	/**
	 * This class encapsulates the memory segments that belong to the record area. It
	 *  - can append a record
	 *  - can overwrite a record at an arbitrary position (WARNING: the new record must have the same size
	 *    as the old one)
	 *  - can be rewritten by calling resetAppendPosition
	 *  - takes memory from InPlaceMutableHashTable.freeMemorySegments on append
	 */
	private final class RecordArea
	{
		private final ArrayList<MemorySegment> segments = new ArrayList<>();

		private final RecordAreaOutputView outView;
		private final RandomAccessInputView inView;

		private final int segmentSizeBits;
		private final int segmentSizeMask;

		private long appendPosition = 0;


		public RecordArea(int segmentSize) {
			int segmentSizeBits = MathUtils.log2strict(segmentSize);

			if ((segmentSize & (segmentSize - 1)) != 0) {
				throw new IllegalArgumentException("Segment size must be a power of 2!");
			}

			this.segmentSizeBits = segmentSizeBits;
			this.segmentSizeMask = segmentSize - 1;

			outView = new RecordAreaOutputView(segmentSize);
			try {
				addSegment();
			} catch (EOFException ex) {
				throw new RuntimeException("Bug in InPlaceMutableHashTable: we should have caught it earlier " +
					"that we don't have enough segments.");
			}
			inView = new RandomAccessInputView(segments, segmentSize);
		}


		private void addSegment() throws EOFException {
			MemorySegment m = allocateSegment();
			if (m == null) {
				throw new EOFException();
			}
			segments.add(m);
		}

		/**
		 * Moves all its memory segments to freeMemorySegments.
		 * Warning: this will leave the RecordArea in an unwritable state: you have to
		 * call setWritePosition before writing again.
		 */
		public void giveBackSegments() {
			freeMemorySegments.addAll(segments);
			segments.clear();

			resetAppendPosition();
		}

		public long getTotalSize() {
			return segments.size() * (long)segmentSize;
		}

		// ----------------------- Output -----------------------

		private void setWritePosition(long position) throws EOFException {
			if (position > appendPosition) {
				throw new IndexOutOfBoundsException();
			}

			final int segmentIndex = (int) (position >>> segmentSizeBits);
			final int offset = (int) (position & segmentSizeMask);

			// If position == appendPosition and the last buffer is full,
			// then we will be seeking to the beginning of a new segment
			if (segmentIndex == segments.size()) {
				addSegment();
			}

			outView.currentSegmentIndex = segmentIndex;
			outView.seekOutput(segments.get(segmentIndex), offset);
		}

		/**
		 * Sets appendPosition and the write position to 0, so that appending starts
		 * overwriting elements from the beginning. (This is used in rebuild.)
		 *
		 * Note: if data was written to the area after the current appendPosition
		 * before a call to resetAppendPosition, it should still be readable. To
		 * release the segments after the current append position, call
		 * freeSegmentsAfterAppendPosition()
		 */
		public void resetAppendPosition() {
			appendPosition = 0;

			// this is just for safety (making sure that we fail immediately
			// if a write happens without calling setWritePosition)
			outView.currentSegmentIndex = -1;
			outView.seekOutput(null, -1);
		}

		/**
		 * Releases the memory segments that are after the current append position.
		 * Note: The situation that there are segments after the current append position
		 * can arise from a call to resetAppendPosition().
		 */
		public void freeSegmentsAfterAppendPosition() {
			final int appendSegmentIndex = (int)(appendPosition >>> segmentSizeBits);
			while (segments.size() > appendSegmentIndex + 1 && !closed) {
				freeMemorySegments.add(segments.get(segments.size() - 1));
				segments.remove(segments.size() - 1);
			}
		}

		/**
		 * Overwrites the long value at the specified position.
		 * @param pointer Points to the position to overwrite.
		 * @param value The value to write.
		 * @throws IOException
		 */
		public void overwritePointerAt(long pointer, long value) throws IOException {
			setWritePosition(pointer);
			outView.writeLong(value);
		}

		/**
		 * Overwrites a record at the specified position. The record is read from a DataInputView  (this will be the staging area).
		 * WARNING: The record must not be larger than the original record.
		 * @param pointer Points to the position to overwrite.
		 * @param input The DataInputView to read the record from
		 * @param size The size of the record
		 * @throws IOException
		 */
		public void overwriteRecordAt(long pointer, DataInputView input, int size) throws IOException {
			setWritePosition(pointer);
			outView.write(input, size);
		}

		/**
		 * Appends a pointer and a record. The record is read from a DataInputView (this will be the staging area).
		 * @param pointer The pointer to write (Note: this is NOT the position to write to!)
		 * @param input The DataInputView to read the record from
		 * @param recordSize The size of the record
		 * @return A pointer to the written data
		 * @throws IOException (EOFException specifically, if memory ran out)
		 */
		public long appendPointerAndCopyRecord(long pointer, DataInputView input, int recordSize) throws IOException {
			setWritePosition(appendPosition);
			final long oldLastPosition = appendPosition;
			outView.writeLong(pointer);
			outView.write(input, recordSize);
			appendPosition += 8 + recordSize;
			return oldLastPosition;
		}

		/**
		 * Appends a pointer and a record.
		 * @param pointer The pointer to write (Note: this is NOT the position to write to!)
		 * @param record The record to write
		 * @return A pointer to the written data
		 * @throws IOException (EOFException specifically, if memory ran out)
		 */
		public long appendPointerAndRecord(long pointer, T record) throws IOException {
			setWritePosition(appendPosition);
			return noSeekAppendPointerAndRecord(pointer, record);
		}

		/**
		 * Appends a pointer and a record. Call this function only if the write position is at the end!
		 * @param pointer The pointer to write (Note: this is NOT the position to write to!)
		 * @param record The record to write
		 * @return A pointer to the written data
		 * @throws IOException (EOFException specifically, if memory ran out)
		 */
		public long noSeekAppendPointerAndRecord(long pointer, T record) throws IOException {
			final long oldLastPosition = appendPosition;
			final long oldPositionInSegment = outView.getCurrentPositionInSegment();
			final long oldSegmentIndex = outView.currentSegmentIndex;
			outView.writeLong(pointer);
			buildSideSerializer.serialize(record, outView);
			appendPosition += outView.getCurrentPositionInSegment() - oldPositionInSegment +
				outView.getSegmentSize() * (outView.currentSegmentIndex - oldSegmentIndex);
			return oldLastPosition;
		}

		public long getAppendPosition() {
			return appendPosition;
		}

		// ----------------------- Input -----------------------

		public void setReadPosition(long position) {
			inView.setReadPosition(position);
		}

		public long getReadPosition() {
			return inView.getReadPosition();
		}

		/**
		 * Note: this is sometimes a negated length instead of a pointer (see HashTableProber.updateMatch).
		 */
		public long readPointer() throws IOException {
			return inView.readLong();
		}

		public T readRecord(T reuse) throws IOException {
			return buildSideSerializer.deserialize(reuse, inView);
		}

		public void skipBytesToRead(int numBytes) throws IOException {
			inView.skipBytesToRead(numBytes);
		}

		// -----------------------------------------------------

		private final class RecordAreaOutputView extends AbstractPagedOutputView {

			public int currentSegmentIndex;

			public RecordAreaOutputView(int segmentSize) {
				super(segmentSize, 0);
			}

			@Override
			protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent) throws EOFException {
				currentSegmentIndex++;
				if (currentSegmentIndex == segments.size()) {
					addSegment();
				}
				return segments.get(currentSegmentIndex);
			}

			@Override
			public void seekOutput(MemorySegment seg, int position) {
				super.seekOutput(seg, position);
			}
		}
	}


	private final class StagingOutputView extends AbstractPagedOutputView {

		private final ArrayList<MemorySegment> segments;

		private final int segmentSizeBits;

		private int currentSegmentIndex;


		public StagingOutputView(ArrayList<MemorySegment> segments, int segmentSize)
		{
			super(segmentSize, 0);
			this.segmentSizeBits = MathUtils.log2strict(segmentSize);
			this.segments = segments;
		}

		/**
		 * Seeks to the beginning.
		 */
		public void reset() {
			seekOutput(segments.get(0), 0);
			currentSegmentIndex = 0;
		}

		@Override
		protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent) throws EOFException {
			currentSegmentIndex++;
			if (currentSegmentIndex == segments.size()) {
				MemorySegment m = allocateSegment();
				if (m == null) {
					throw new EOFException();
				}
				segments.add(m);
			}
			return segments.get(currentSegmentIndex);
		}

		public long getWritePosition() {
			return (((long) currentSegmentIndex) << segmentSizeBits) + getCurrentPositionInSegment();
		}
	}


	/**
	 * A prober for accessing the table.
	 * In addition to getMatchFor and updateMatch, it also has insertAfterNoMatch.
	 * Warning: Don't modify the table between calling getMatchFor and the other methods!
	 * @param <PT> The type of the records that we are probing with
     */
	public final class HashTableProber<PT> extends AbstractHashTableProber<PT, T>{

		public HashTableProber(TypeComparator<PT> probeTypeComparator, TypePairComparator<PT, T> pairComparator) {
			super(probeTypeComparator, pairComparator);
		}

		private int bucketSegmentIndex;
		private int bucketOffset;
		private long curElemPtr;
		private long prevElemPtr;
		private long nextPtr;
		private long recordEnd;

		/**
		 * Searches the hash table for the record with the given key.
		 * (If there would be multiple matches, only one is returned.)
		 * @param record The record whose key we are searching for
		 * @param targetForMatch If a match is found, it will be written here
         * @return targetForMatch if a match is found, otherwise null.
         */
		@Override
		public T getMatchFor(PT record, T targetForMatch) {
			if (closed) {
				return null;
			}

			final int hashCode = MathUtils.jenkinsHash(probeTypeComparator.hash(record));
			final int bucket = hashCode & numBucketsMask;
			bucketSegmentIndex = bucket >>> numBucketsPerSegmentBits; // which segment contains the bucket
			final MemorySegment bucketSegment = bucketSegments[bucketSegmentIndex];
			bucketOffset = (bucket & numBucketsPerSegmentMask) << bucketSizeBits; // offset of the bucket in the segment

			curElemPtr = bucketSegment.getLong(bucketOffset);

			pairComparator.setReference(record);

			T currentRecordInList = targetForMatch;

			prevElemPtr = INVALID_PREV_POINTER;
			try {
				while (curElemPtr != END_OF_LIST && !closed) {
					recordArea.setReadPosition(curElemPtr);
					nextPtr = recordArea.readPointer();

					currentRecordInList = recordArea.readRecord(currentRecordInList);
					recordEnd = recordArea.getReadPosition();
					if (pairComparator.equalToReference(currentRecordInList)) {
						// we found an element with a matching key, and not just a hash collision
						return currentRecordInList;
					}

					prevElemPtr = curElemPtr;
					curElemPtr = nextPtr;
				}
			} catch (IOException ex) {
				throw new RuntimeException("Error deserializing record from the hashtable: " + ex.getMessage(), ex);
			}
			return null;
		}

		@Override
		public T getMatchFor(PT probeSideRecord) {
			return getMatchFor(probeSideRecord, buildSideSerializer.createInstance());
		}

		/**
		 * This method can be called after getMatchFor returned a match.
		 * It will overwrite the record that was found by getMatchFor.
		 * Warning: The new record should have the same key as the old!
		 * WARNING; Don't do any modifications to the table between
		 * getMatchFor and updateMatch!
		 * @param newRecord The record to override the old record with.
		 * @throws IOException (EOFException specifically, if memory ran out)
         */
		@Override
		public void updateMatch(T newRecord) throws IOException {
			if (closed) {
				return;
			}
			if (curElemPtr == END_OF_LIST) {
				throw new RuntimeException("updateMatch was called after getMatchFor returned no match");
			}

			try {
				// determine the new size
				stagingSegmentsOutView.reset();
				buildSideSerializer.serialize(newRecord, stagingSegmentsOutView);
				final int newRecordSize = (int)stagingSegmentsOutView.getWritePosition();
				stagingSegmentsInView.setReadPosition(0);

				// Determine the size of the place of the old record.
				final int oldRecordSize = (int)(recordEnd - (curElemPtr + RECORD_OFFSET_IN_LINK));

				if (newRecordSize == oldRecordSize) {
					// overwrite record at its original place
					recordArea.overwriteRecordAt(curElemPtr + RECORD_OFFSET_IN_LINK, stagingSegmentsInView, newRecordSize);
				} else {
					// new record has a different size than the old one, append new at the end of the record area.
					// Note: we have to do this, even if the new record is smaller, because otherwise EntryIterator
					// wouldn't know the size of this place, and wouldn't know where does the next record start.

					final long pointerToAppended =
						recordArea.appendPointerAndCopyRecord(nextPtr, stagingSegmentsInView, newRecordSize);

					// modify the pointer in the previous link
					if (prevElemPtr == INVALID_PREV_POINTER) {
						// list had only one element, so prev is in the bucketSegments
						bucketSegments[bucketSegmentIndex].putLong(bucketOffset, pointerToAppended);
					} else {
						recordArea.overwritePointerAt(prevElemPtr, pointerToAppended);
					}

					// write the negated size of the hole to the place where the next pointer was, so that EntryIterator
					// will know the size of the place without reading the old record.
					// The negative sign will mean that the record is abandoned, and the
					// the -1 is for avoiding trouble in case of a record having 0 size. (though I think this should
					// never actually happen)
					// Note: the last record in the record area can't be abandoned. (EntryIterator makes use of this fact.)
					recordArea.overwritePointerAt(curElemPtr, -oldRecordSize - 1);

					holes += oldRecordSize;
				}
			} catch (EOFException ex) {
				compactOrThrow();
				insertOrReplaceRecord(newRecord);
			}
		}

		/**
		 * This method can be called after getMatchFor returned null.
		 * It inserts the given record to the hash table.
		 * Important: The given record should have the same key as the record
		 * that was given to getMatchFor!
		 * WARNING; Don't do any modifications to the table between
		 * getMatchFor and insertAfterNoMatch!
		 * @throws IOException (EOFException specifically, if memory ran out)
		 */
		public void insertAfterNoMatch(T record) throws IOException {
			if (closed) {
				return;
			}

			// create new link
			long pointerToAppended;
			try {
				pointerToAppended = recordArea.appendPointerAndRecord(END_OF_LIST ,record);
			} catch (EOFException ex) {
				compactOrThrow();
				insert(record);
				return;
			}

			// add new link to the end of the list
			if (prevElemPtr == INVALID_PREV_POINTER) {
				// list was empty
				bucketSegments[bucketSegmentIndex].putLong(bucketOffset, pointerToAppended);
			} else {
				// update the pointer of the last element of the list.
				recordArea.overwritePointerAt(prevElemPtr, pointerToAppended);
			}

			numElements++;
			resizeTableIfNecessary();
		}
	}


	/**
	 * WARNING: Doing any other operation on the table invalidates the iterator! (Even
	 * using getMatchFor of a prober!)
	 */
	public final class EntryIterator implements MutableObjectIterator<T> {

		private final long endPosition;

		public EntryIterator() {
			endPosition = recordArea.getAppendPosition();
			if (endPosition == 0) {
				return;
			}
			recordArea.setReadPosition(0);
		}

		@Override
		public T next(T reuse) throws IOException {
			if (endPosition != 0 && recordArea.getReadPosition() < endPosition) {
				// Loop until we find a non-abandoned record.
				// Note: the last record in the record area can't be abandoned.
				while (!closed) {
					final long pointerOrNegatedLength = recordArea.readPointer();
					final boolean isAbandoned = pointerOrNegatedLength < 0;
					if (!isAbandoned) {
						reuse = recordArea.readRecord(reuse);
						return reuse;
					} else {
						// pointerOrNegatedLength is storing a length, because the record was abandoned.
						recordArea.skipBytesToRead((int)-(pointerOrNegatedLength + 1));
					}
				}
				return null; // (we were closed)
			} else {
				return null;
			}
		}

		@Override
		public T next() throws IOException {
			return next(buildSideSerializer.createInstance());
		}
	}

	/**
	 * A facade for doing such operations on the hash table that are needed for a reduce operator driver.
	 */
	public final class ReduceFacade {

		private final HashTableProber<T> prober;

		private final boolean objectReuseEnabled;

		private final ReduceFunction<T> reducer;

		private final Collector<T> outputCollector;

		private T reuse;


		public ReduceFacade(ReduceFunction<T> reducer, Collector<T> outputCollector, boolean objectReuseEnabled) {
			this.reducer = reducer;
			this.outputCollector = outputCollector;
			this.objectReuseEnabled = objectReuseEnabled;
			this.prober = getProber(buildSideComparator, new SameTypePairComparator<>(buildSideComparator));
			this.reuse = buildSideSerializer.createInstance();
		}

		/**
		 * Looks up the table entry that has the same key as the given record, and updates it by performing
		 * a reduce step.
		 * @param record The record to update.
		 * @throws Exception
         */
		public void updateTableEntryWithReduce(T record) throws Exception {
			T match = prober.getMatchFor(record, reuse);
			if (match == null) {
				prober.insertAfterNoMatch(record);
			} else {
				// do the reduce step
				T res = reducer.reduce(match, record);

				// We have given reuse to the reducer UDF, so create new one if object reuse is disabled
				if (!objectReuseEnabled) {
					reuse = buildSideSerializer.createInstance();
				}

				prober.updateMatch(res);
			}
		}

		/**
		 * Emits all elements currently held by the table to the collector.
		 */
		public void emit() throws IOException {
			T record = buildSideSerializer.createInstance();
			EntryIterator iter = getEntryIterator();
			while ((record = iter.next(record)) != null && !closed) {
				outputCollector.collect(record);
				if (!objectReuseEnabled) {
					record = buildSideSerializer.createInstance();
				}
			}
		}

		/**
		 * Emits all elements currently held by the table to the collector,
		 * and resets the table. The table will have the same number of buckets
		 * as before the reset, to avoid doing resizes again.
		 */
		public void emitAndReset() throws IOException {
			final int oldNumBucketSegments = bucketSegments.length;
			emit();
			close();
			open(oldNumBucketSegments);
		}
	}
}
