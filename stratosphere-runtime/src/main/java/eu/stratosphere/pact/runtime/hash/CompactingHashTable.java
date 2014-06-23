/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.hash;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypePairComparator;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.ListMemorySegmentSource;
import eu.stratosphere.pact.runtime.util.IntArrayList;
import eu.stratosphere.pact.runtime.util.LongArrayList;
import eu.stratosphere.pact.runtime.util.MathUtils;
import eu.stratosphere.util.MutableObjectIterator;


/**
 * An implementation of an in-memory Hash Table for variable-length records. 
 * <p>
 * The design of this class follows on many parts the design presented in
 * "Hash joins and hash teams in Microsoft SQL Server", by Goetz Graefe et al..
 *<p>
 *
 *
 * <hr>
 * 
 * The layout of the buckets inside a memory segment is as follows:
 * 
 * <pre>
 * +----------------------------- Bucket x ----------------------------
 * |Partition (1 byte) | reserved (3 bytes) | element count (4 bytes) |
 * | next-bucket-in-chain-pointer (8 bytes) |
 * |
 * |hashCode 1 (4 bytes) | hashCode 2 (4 bytes) | hashCode 3 (4 bytes) |
 * | ... hashCode n-1 (4 bytes) | hashCode n (4 bytes)
 * |
 * |pointer 1 (8 bytes) | pointer 2 (8 bytes) | pointer 3 (8 bytes) |
 * | ... pointer n-1 (8 bytes) | pointer n (8 bytes)
 * |
 * +---------------------------- Bucket x + 1--------------------------
 * |Partition (1 byte) | reserved (3 bytes) | element count (4 bytes) |
 * | next-bucket-in-chain-pointer (8 bytes) |
 * |
 * |hashCode 1 (4 bytes) | hashCode 2 (4 bytes) | hashCode 3 (4 bytes) |
 * | ... hashCode n-1 (4 bytes) | hashCode n (4 bytes)
 * |
 * |pointer 1 (8 bytes) | pointer 2 (8 bytes) | pointer 3 (8 bytes) |
 * | ... pointer n-1 (8 bytes) | pointer n (8 bytes)
 * +-------------------------------------------------------------------
 * | ...
 * |
 * </pre>
 * @param <T>
 * 
 * @param T record type stored in hash table
 * 
 */
public class CompactingHashTable<T> extends AbstractMutableHashTable<T>{

	private static final Log LOG = LogFactory.getLog(CompactingHashTable.class);
	
	// ------------------------------------------------------------------------
	//                         Internal Constants
	// ------------------------------------------------------------------------
	
	private static final int MIN_NUM_MEMORY_SEGMENTS = 33;
	
	/**
	 * The maximum number of partitions
	 */
	private static final int MAX_NUM_PARTITIONS = 32;
	
	/**
	 * The default record width that is used when no width is given. The record width is
	 * used to determine the ratio of the number of memory segments intended for partition
	 * buffers and the number of memory segments in the hash-table structure. 
	 */
	private static final int DEFAULT_RECORD_LEN = 24; //FIXME maybe find a better default
	
	/**
	 * The length of the hash code stored in the bucket.
	 */
	private static final int HASH_CODE_LEN = 4;
	
	/**
	 * The length of a pointer from a hash bucket to the record in the buffers.
	 */
	private static final int POINTER_LEN = 8;
	
	/**
	 * The number of bytes that the entry in the hash structure occupies, in bytes.
	 * It corresponds to a 4 byte hash value and an 8 byte pointer.
	 */
	private static final int RECORD_TABLE_BYTES = HASH_CODE_LEN + POINTER_LEN;
	
	/**
	 * The total storage overhead per record, in bytes. This corresponds to the space in the
	 * actual hash table buckets, consisting of a 4 byte hash value and an 8 byte
	 * pointer, plus the overhead for the stored length field.
	 */
	private static final int RECORD_OVERHEAD_BYTES = RECORD_TABLE_BYTES + 2;
	
	// -------------------------- Bucket Size and Structure -------------------------------------
	
	private static final int NUM_INTRA_BUCKET_BITS = 7;
	
	private static final int HASH_BUCKET_SIZE = 0x1 << NUM_INTRA_BUCKET_BITS;
	
	private static final int BUCKET_HEADER_LENGTH = 16;
	
	private static final int NUM_ENTRIES_PER_BUCKET = (HASH_BUCKET_SIZE - BUCKET_HEADER_LENGTH) / RECORD_TABLE_BYTES;
	
	private static final int BUCKET_POINTER_START_OFFSET = BUCKET_HEADER_LENGTH + (NUM_ENTRIES_PER_BUCKET * HASH_CODE_LEN);
	
	// ------------------------------ Bucket Header Fields ------------------------------
	
	/**
	 * Offset of the field in the bucket header indicating the bucket's partition.
	 */
	private static final int HEADER_PARTITION_OFFSET = 0;
	
	/**
	 * Offset of the field in the bucket header indicating the bucket's status (spilled or in-memory).
	 */
	private static final int HEADER_COUNT_OFFSET = 4;
	
	/**
	 * Offset of the field in the bucket header that holds the forward pointer to its
	 * first overflow bucket.
	 */
	private static final int HEADER_FORWARD_OFFSET = 8;
	
	/**
	 * Constant for the forward pointer, indicating that the pointer is not set. 
	 */
	private static final long BUCKET_FORWARD_POINTER_NOT_SET = ~0x0L;
	
	// ------------------------------------------------------------------------
	//                              Members
	// ------------------------------------------------------------------------

	/**
	 * The free memory segments currently available to the hash join.
	 */
	private final ArrayList<MemorySegment> availableMemory;
	
	/**
	 * The size of the segments used by the hash join buckets. All segments must be of equal size to ease offset computations.
	 */
	private final int segmentSize;
	
	/**
	 * The number of hash table buckets in a single memory segment - 1.
	 * Because memory segments can be comparatively large, we fit multiple buckets into one memory segment.
	 * This variable is a mask that is 1 in the lower bits that define the number of a bucket
	 * in a segment.
	 */
	private final int bucketsPerSegmentMask;
	
	/**
	 * The number of bits that describe the position of a bucket in a memory segment. Computed as log2(bucketsPerSegment).
	 */
	private final int bucketsPerSegmentBits;
	
	/**
	 * An estimate for the average record length.
	 */
	private final int avgRecordLen;
	
	// ------------------------------------------------------------------------
	
	/**
	 * The partitions of the hash table.
	 */
	private final ArrayList<InMemoryPartition<T>> partitions;
	
	/**
	 * The array of memory segments that contain the buckets which form the actual hash-table
	 * of hash-codes and pointers to the elements.
	 */
	private MemorySegment[] buckets;
	
	/**
	 * temporary storage for partition compaction (always attempts to allocate as many segments as the largest partition)
	 */
	private InMemoryPartition<T> compactionMemory;
	
	/**
	 * The number of buckets in the current table. The bucket array is not necessarily fully
	 * used, when not all buckets that would fit into the last segment are actually used.
	 */
	private int numBuckets;
	
	/**
	 * flag necessary so a resize is never triggered during a resize since the code paths are interleaved
	 */
	private boolean isResizing = false;
	
	private AtomicBoolean closed = new AtomicBoolean();
	
	private boolean running = true;
		
	private int pageSizeInBits;

	// ------------------------------------------------------------------------
	//                         Construction and Teardown
	// ------------------------------------------------------------------------
	
	public CompactingHashTable(TypeSerializer<T> buildSideSerializer, TypeComparator<T> buildSideComparator, List<MemorySegment> memorySegments)
	{
		this(buildSideSerializer, buildSideComparator, memorySegments, DEFAULT_RECORD_LEN);
	}
	
	public CompactingHashTable(TypeSerializer<T> buildSideSerializer, TypeComparator<T> buildSideComparator, List<MemorySegment> memorySegments, int avgRecordLen)
	{
		super(buildSideSerializer, buildSideComparator);
		// some sanity checks first
		if (memorySegments == null) {
			throw new NullPointerException();
		}
		if (memorySegments.size() < MIN_NUM_MEMORY_SEGMENTS) {
			throw new IllegalArgumentException("Too few memory segments provided. Hash Table needs at least " + 
				MIN_NUM_MEMORY_SEGMENTS + " memory segments.");
		}
		
		this.availableMemory = (memorySegments instanceof ArrayList) ? 
				(ArrayList<MemorySegment>) memorySegments :
				new ArrayList<MemorySegment>(memorySegments);

		
		this.avgRecordLen = buildSideSerializer.getLength() > 0 ? buildSideSerializer.getLength() : avgRecordLen;
		
		// check the size of the first buffer and record it. all further buffers must have the same size.
		// the size must also be a power of 2
		this.segmentSize = memorySegments.get(0).size();
		if ( (this.segmentSize & this.segmentSize - 1) != 0) {
			throw new IllegalArgumentException("Hash Table requires buffers whose size is a power of 2.");
		}
		int bucketsPerSegment = this.segmentSize >> NUM_INTRA_BUCKET_BITS;
		if (bucketsPerSegment == 0) {
			throw new IllegalArgumentException("Hash Table requires buffers of at least " + HASH_BUCKET_SIZE + " bytes.");
		}
		this.bucketsPerSegmentMask = bucketsPerSegment - 1;
		this.bucketsPerSegmentBits = MathUtils.log2strict(bucketsPerSegment);
		
		this.partitions = new ArrayList<InMemoryPartition<T>>();
		
		// because we allow to open and close multiple times, the state is initially closed
		this.closed.set(true);
		// so far no partition has any MemorySegments
	}
	
	
	// ------------------------------------------------------------------------
	//                              Life-Cycle
	// ------------------------------------------------------------------------
	
	/**
	 * Build the hash table
	 * 
	 * @throws IOException Thrown, if an I/O problem occurs while spilling a partition.
	 */
	public void open() {
		// sanity checks
		if (!this.closed.compareAndSet(true, false)) {
			throw new IllegalStateException("Hash Table cannot be opened, because it is currently not closed.");
		}
		
		// create the partitions
		final int partitionFanOut = getPartitioningFanOutNoEstimates(this.availableMemory.size()); 
		createPartitions(partitionFanOut);
		
		// set up the table structure. the write behind buffers are taken away, as are one buffer per partition
		final int numBuckets = getInitialTableSize(this.availableMemory.size(), this.segmentSize, 
			partitionFanOut, this.avgRecordLen);
		
		initTable(numBuckets, (byte) partitionFanOut);
	}

	
	/**
	 * Closes the hash table. This effectively releases all internal structures and closes all
	 * open files and removes them. The call to this method is valid both as a cleanup after the
	 * complete inputs were properly processed, and as an cancellation call, which cleans up
	 * all resources that are currently held by the hash join. If another process still access the hash 
	 * table after close has been called no operations will be performed.
	 */
	public void close() {
		// make sure that we close only once
		if (!this.closed.compareAndSet(false, true)) {
			return;
		}
		
		LOG.debug("Closing hash table and releasing resources.");
		
		// release the table structure
		releaseTable();
		
		// clear the memory in the partitions
		clearPartitions();
	}
	
	public void abort() {
		this.running = false;
		
		LOG.debug("Cancelling hash table operations.");
	}
	
	public List<MemorySegment> getFreeMemory() {
		if (!this.closed.get()) {
			throw new IllegalStateException("Cannot return memory while join is open.");
		}
		
		return this.availableMemory;
	}
	
	
	public void buildTable(final MutableObjectIterator<T> input) throws IOException {
		T record = this.buildSideSerializer.createInstance();
		
		// go over the complete input and insert every element into the hash table
		while (this.running && ((record = input.next(record)) != null)) {
			insert(record);
		}
	}
	
	public final void insert(T record) throws IOException {
		if(this.closed.get()) {
			return;
		}
		final int hashCode = hash(this.buildSideComparator.hash(record));
		final int posHashCode = hashCode % this.numBuckets;
		
		// get the bucket for the given hash code
		final int bucketArrayPos = posHashCode >>> this.bucketsPerSegmentBits;
		final int bucketInSegmentPos = (posHashCode & this.bucketsPerSegmentMask) << NUM_INTRA_BUCKET_BITS;
		final MemorySegment bucket = this.buckets[bucketArrayPos];
		
		// get the basic characteristics of the bucket
		final int partitionNumber = bucket.get(bucketInSegmentPos + HEADER_PARTITION_OFFSET);
		InMemoryPartition<T> partition = this.partitions.get(partitionNumber);
		
		
		long pointer;
		try {
			pointer = partition.appendRecord(record);
			if((pointer >> this.pageSizeInBits) > this.compactionMemory.getBlockCount()) {
				this.compactionMemory.allocateSegments((int)(pointer >> this.pageSizeInBits));
			}
		} catch (EOFException e) {
			try {
				compactPartition(partitionNumber);
				// retry append
				partition = this.partitions.get(partitionNumber); // compaction invalidates reference
				pointer = partition.appendRecord(record);
			} catch (EOFException ex) {
				throw new RuntimeException("Memory ran out. Compaction failed. " + 
											getMemoryConsumptionString() +
											" Message: " + ex.getMessage());
			} catch (IndexOutOfBoundsException ex) {
				throw new RuntimeException("Memory ran out. Compaction failed. " + 
											getMemoryConsumptionString() +
											" Message: " + ex.getMessage());
			}
		} catch (IndexOutOfBoundsException e1) {
			try {
				compactPartition(partitionNumber);
				// retry append
				partition = this.partitions.get(partitionNumber); // compaction invalidates reference
				pointer = partition.appendRecord(record);
			} catch (EOFException ex) {
				throw new RuntimeException("Memory ran out. Compaction failed. " + 
											getMemoryConsumptionString() +
											" Message: " + ex.getMessage());
			} catch (IndexOutOfBoundsException ex) {
				throw new RuntimeException("Memory ran out. Compaction failed. " + 
											getMemoryConsumptionString() +
											" Message: " + ex.getMessage());
			}
		}
		insertBucketEntryFromStart(partition, bucket, bucketInSegmentPos, hashCode, pointer);
	}
	
	
	@Override
	public <PT> HashTableProber<PT> getProber(TypeComparator<PT> probeSideComparator, TypePairComparator<PT, T> pairComparator) {
		return new HashTableProber<PT>(probeSideComparator, pairComparator);
	}
	
	/**
	 * 
	 * @return Iterator over hash table
	 * @see EntryIterator
	 */
	public MutableObjectIterator<T> getEntryIterator() {
		return new EntryIterator(this);
	}
	
	/**
	 * Replaces record in hash table if record already present or append record if not.
	 * May trigger expensive compaction.
	 * 
	 * @param record record to insert or replace
	 * @param tempHolder instance of T that will be overwritten
	 * @throws IOException
	 */
	public void insertOrReplaceRecord(T record, T tempHolder) throws IOException {
		if(this.closed.get()) {
			return;
		}
		
		final int searchHashCode = hash(this.buildSideComparator.hash(record));
		final int posHashCode = searchHashCode % this.numBuckets;
		
		// get the bucket for the given hash code
		MemorySegment originalBucket = this.buckets[posHashCode >> this.bucketsPerSegmentBits];
		int originalBucketOffset = (posHashCode & this.bucketsPerSegmentMask) << NUM_INTRA_BUCKET_BITS;
		MemorySegment bucket = originalBucket;
		int bucketInSegmentOffset = originalBucketOffset;
		
		// get the basic characteristics of the bucket
		final int partitionNumber = bucket.get(bucketInSegmentOffset + HEADER_PARTITION_OFFSET);
		InMemoryPartition<T> partition = this.partitions.get(partitionNumber);
		final MemorySegment[] overflowSegments = partition.overflowSegments;
		
		this.buildSideComparator.setReference(record);
		
		int countInSegment = bucket.getInt(bucketInSegmentOffset + HEADER_COUNT_OFFSET);
		int numInSegment = 0;
		int posInSegment = bucketInSegmentOffset + BUCKET_HEADER_LENGTH;
		
		long currentForwardPointer = BUCKET_FORWARD_POINTER_NOT_SET;

		// loop over all segments that are involved in the bucket (original bucket plus overflow buckets)
		while (true) {
			
			while (numInSegment < countInSegment) {
				
				final int thisCode = bucket.getInt(posInSegment);
				posInSegment += HASH_CODE_LEN;
					
				// check if the hash code matches
				if (thisCode == searchHashCode) {
					// get the pointer to the pair
					final int pointerOffset = bucketInSegmentOffset + BUCKET_POINTER_START_OFFSET + (numInSegment * POINTER_LEN);
					final long pointer = bucket.getLong(pointerOffset);
					numInSegment++;
					
					// deserialize the key to check whether it is really equal, or whether we had only a hash collision
					try {
						partition.readRecordAt(pointer, tempHolder);
						if (this.buildSideComparator.equalToReference(tempHolder)) {
							long newPointer = partition.appendRecord(record);
							bucket.putLong(pointerOffset, newPointer);
							partition.setCompaction(false);
							if((newPointer >> this.pageSizeInBits) > this.compactionMemory.getBlockCount()) {
								this.compactionMemory.allocateSegments((int)(newPointer >> this.pageSizeInBits));
							}
							return;
						}
					} catch (EOFException e) {
						// system is out of memory so we attempt to reclaim memory with a copy compact run
						long newPointer;
						try {
							compactPartition(partition.getPartitionNumber());
							// retry append
							partition = this.partitions.get(partitionNumber); // compaction invalidates reference
							newPointer = partition.appendRecord(record);
						} catch (EOFException ex) {
							throw new RuntimeException("Memory ran out. Compaction failed. " + 
														getMemoryConsumptionString() +
														" Message: " + ex.getMessage());
						} catch (IndexOutOfBoundsException ex) {
							throw new RuntimeException("Memory ran out. Compaction failed. " + 
														getMemoryConsumptionString() +
														" Message: " + ex.getMessage());
						}
						bucket.putLong(pointerOffset, newPointer);
						return;
					} catch (IndexOutOfBoundsException e) {
						// system is out of memory so we attempt to reclaim memory with a copy compact run
						long newPointer;
						try {
							compactPartition(partition.getPartitionNumber());
							// retry append
							partition = this.partitions.get(partitionNumber); // compaction invalidates reference
							newPointer = partition.appendRecord(record);
						} catch (EOFException ex) {
							throw new RuntimeException("Memory ran out. Compaction failed. " + 
														getMemoryConsumptionString() +
														" Message: " + ex.getMessage());
						} catch (IndexOutOfBoundsException ex) {
							throw new RuntimeException("Memory ran out. Compaction failed. " + 
														getMemoryConsumptionString() +
														" Message: " + ex.getMessage());
						}
						bucket.putLong(pointerOffset, newPointer);
						return;
					} catch (IOException e) {
						throw new RuntimeException("Error deserializing record from the hashtable: " + e.getMessage(), e);
					} 
				}
				else {
					numInSegment++;
				}
			}
			
			// this segment is done. check if there is another chained bucket
			long newForwardPointer = bucket.getLong(bucketInSegmentOffset + HEADER_FORWARD_OFFSET);
			if (newForwardPointer == BUCKET_FORWARD_POINTER_NOT_SET) {
				// nothing found. append and insert
				long pointer = partition.appendRecord(record);
				//insertBucketEntryFromStart(partition, originalBucket, originalBucketOffset, searchHashCode, pointer);
				insertBucketEntryFromSearch(partition, originalBucket, bucket, originalBucketOffset, bucketInSegmentOffset, countInSegment, currentForwardPointer, searchHashCode, pointer);
				if((pointer >> this.pageSizeInBits) > this.compactionMemory.getBlockCount()) {
					this.compactionMemory.allocateSegments((int)(pointer >> this.pageSizeInBits));
				}
				return;
			}
			
			final int overflowSegNum = (int) (newForwardPointer >>> 32);
			bucket = overflowSegments[overflowSegNum];
			bucketInSegmentOffset = (int) (newForwardPointer & 0xffffffff);
			countInSegment = bucket.getInt(bucketInSegmentOffset + HEADER_COUNT_OFFSET);
			posInSegment = bucketInSegmentOffset + BUCKET_HEADER_LENGTH;
			numInSegment = 0;
			currentForwardPointer = newForwardPointer;
		}
	}

	private final void insertBucketEntryFromStart(InMemoryPartition<T> p, MemorySegment bucket, 
			int bucketInSegmentPos, int hashCode, long pointer)
	throws IOException
	{
		boolean checkForResize = false;
		// find the position to put the hash code and pointer
		final int count = bucket.getInt(bucketInSegmentPos + HEADER_COUNT_OFFSET);
		if (count < NUM_ENTRIES_PER_BUCKET) {
			// we are good in our current bucket, put the values
			bucket.putInt(bucketInSegmentPos + BUCKET_HEADER_LENGTH + (count * HASH_CODE_LEN), hashCode);	// hash code
			bucket.putLong(bucketInSegmentPos + BUCKET_POINTER_START_OFFSET + (count * POINTER_LEN), pointer); // pointer
			bucket.putInt(bucketInSegmentPos + HEADER_COUNT_OFFSET, count + 1); // update count
		} else {
			// we need to go to the overflow buckets
			final long originalForwardPointer = bucket.getLong(bucketInSegmentPos + HEADER_FORWARD_OFFSET);
			final long forwardForNewBucket;
			
			if (originalForwardPointer != BUCKET_FORWARD_POINTER_NOT_SET) {
				
				// forward pointer set
				final int overflowSegNum = (int) (originalForwardPointer >>> 32);
				final int segOffset = (int) (originalForwardPointer & 0xffffffff);
				final MemorySegment seg = p.overflowSegments[overflowSegNum];
				
				final int obCount = seg.getInt(segOffset + HEADER_COUNT_OFFSET);
				
				// check if there is space in this overflow bucket
				if (obCount < NUM_ENTRIES_PER_BUCKET) {
					// space in this bucket and we are done
					seg.putInt(segOffset + BUCKET_HEADER_LENGTH + (obCount * HASH_CODE_LEN), hashCode);	// hash code
					seg.putLong(segOffset + BUCKET_POINTER_START_OFFSET + (obCount * POINTER_LEN), pointer); // pointer
					seg.putInt(segOffset + HEADER_COUNT_OFFSET, obCount + 1); // update count
					return;
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
			if (p.nextOverflowBucket == 0) {
				// no space left in last bucket, or no bucket yet, so create an overflow segment
				overflowSeg = getNextBuffer();
				overflowBucketOffset = 0;
				overflowBucketNum = p.numOverflowSegments;
				
				// add the new overflow segment
				if (p.overflowSegments.length <= p.numOverflowSegments) {
					MemorySegment[] newSegsArray = new MemorySegment[p.overflowSegments.length * 2];
					System.arraycopy(p.overflowSegments, 0, newSegsArray, 0, p.overflowSegments.length);
					p.overflowSegments = newSegsArray;
				}
				p.overflowSegments[p.numOverflowSegments] = overflowSeg;
				p.numOverflowSegments++;
				checkForResize = true;
			} else {
				// there is space in the last overflow bucket
				overflowBucketNum = p.numOverflowSegments - 1;
				overflowSeg = p.overflowSegments[overflowBucketNum];
				overflowBucketOffset = p.nextOverflowBucket << NUM_INTRA_BUCKET_BITS;
			}
			
			// next overflow bucket is one ahead. if the segment is full, the next will be at the beginning
			// of a new segment
			p.nextOverflowBucket = (p.nextOverflowBucket == this.bucketsPerSegmentMask ? 0 : p.nextOverflowBucket + 1);
			
			// insert the new overflow bucket in the chain of buckets
			// 1) set the old forward pointer
			// 2) let the bucket in the main table point to this one
			overflowSeg.putLong(overflowBucketOffset + HEADER_FORWARD_OFFSET, forwardForNewBucket);
			final long pointerToNewBucket = (((long) overflowBucketNum) << 32) | ((long) overflowBucketOffset);
			bucket.putLong(bucketInSegmentPos + HEADER_FORWARD_OFFSET, pointerToNewBucket);
			
			// finally, insert the values into the overflow buckets
			overflowSeg.putInt(overflowBucketOffset + BUCKET_HEADER_LENGTH, hashCode);	// hash code
			overflowSeg.putLong(overflowBucketOffset + BUCKET_POINTER_START_OFFSET, pointer); // pointer
			
			// set the count to one
			overflowSeg.putInt(overflowBucketOffset + HEADER_COUNT_OFFSET, 1); 
			if(checkForResize && !this.isResizing) {
				// check if we should resize buckets
				if(this.buckets.length <= getOverflowSegmentCount()) {
					resizeHashTable();
				}
			}
		}
	}
	
	private final void insertBucketEntryFromSearch(InMemoryPartition<T> partition, MemorySegment originalBucket, MemorySegment currentBucket, int originalBucketOffset, int currentBucketOffset, int countInCurrentBucket, long currentForwardPointer, int hashCode, long pointer) throws IOException {
		boolean checkForResize = false;
		if (countInCurrentBucket < NUM_ENTRIES_PER_BUCKET) {
			// we are good in our current bucket, put the values
			currentBucket.putInt(currentBucketOffset + BUCKET_HEADER_LENGTH + (countInCurrentBucket * HASH_CODE_LEN), hashCode);	// hash code
			currentBucket.putLong(currentBucketOffset + BUCKET_POINTER_START_OFFSET + (countInCurrentBucket * POINTER_LEN), pointer); // pointer
			currentBucket.putInt(currentBucketOffset + HEADER_COUNT_OFFSET, countInCurrentBucket + 1); // update count
		} else {
			// we need a new overflow bucket
			MemorySegment overflowSeg;
			final int overflowBucketNum;
			final int overflowBucketOffset;
			
			// first, see if there is space for an overflow bucket remaining in the last overflow segment
			if (partition.nextOverflowBucket == 0) {
				// no space left in last bucket, or no bucket yet, so create an overflow segment
				overflowSeg = getNextBuffer();
				overflowBucketOffset = 0;
				overflowBucketNum = partition.numOverflowSegments;
				
				// add the new overflow segment
				if (partition.overflowSegments.length <= partition.numOverflowSegments) {
					MemorySegment[] newSegsArray = new MemorySegment[partition.overflowSegments.length * 2];
					System.arraycopy(partition.overflowSegments, 0, newSegsArray, 0, partition.overflowSegments.length);
					partition.overflowSegments = newSegsArray;
				}
				partition.overflowSegments[partition.numOverflowSegments] = overflowSeg;
				partition.numOverflowSegments++;
				checkForResize = true;
			} else {
				// there is space in the last overflow segment
				overflowBucketNum = partition.numOverflowSegments - 1;
				overflowSeg = partition.overflowSegments[overflowBucketNum];
				overflowBucketOffset = partition.nextOverflowBucket << NUM_INTRA_BUCKET_BITS;
			}
			
			// next overflow bucket is one ahead. if the segment is full, the next will be at the beginning
			// of a new segment
			partition.nextOverflowBucket = (partition.nextOverflowBucket == this.bucketsPerSegmentMask ? 0 : partition.nextOverflowBucket + 1);
			
			// insert the new overflow bucket in the chain of buckets
			// 1) set the old forward pointer
			// 2) let the bucket in the main table point to this one
			overflowSeg.putLong(overflowBucketOffset + HEADER_FORWARD_OFFSET, currentForwardPointer);
			final long pointerToNewBucket = (((long) overflowBucketNum) << 32) | ((long) overflowBucketOffset);
			originalBucket.putLong(originalBucketOffset + HEADER_FORWARD_OFFSET, pointerToNewBucket);
			
			// finally, insert the values into the overflow buckets
			overflowSeg.putInt(overflowBucketOffset + BUCKET_HEADER_LENGTH, hashCode);	// hash code
			overflowSeg.putLong(overflowBucketOffset + BUCKET_POINTER_START_OFFSET, pointer); // pointer
			
			// set the count to one
			overflowSeg.putInt(overflowBucketOffset + HEADER_COUNT_OFFSET, 1);
			if(checkForResize && !this.isResizing) {
				// check if we should resize buckets
				if(this.buckets.length <= getOverflowSegmentCount()) {
					resizeHashTable();
				}
			}
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//                          Setup and Tear Down of Structures
	// --------------------------------------------------------------------------------------------

	private void createPartitions(int numPartitions) {
		this.partitions.clear();
		
		ListMemorySegmentSource memSource = new ListMemorySegmentSource(this.availableMemory);
		this.pageSizeInBits = MathUtils.log2strict(this.segmentSize);
		
		for (int i = 0; i < numPartitions; i++) {
			this.partitions.add(new InMemoryPartition<T>(this.buildSideSerializer, i, memSource, this.segmentSize, pageSizeInBits));
		}
		this.compactionMemory = new InMemoryPartition<T>(this.buildSideSerializer, -1, memSource, this.segmentSize, pageSizeInBits);
	}
	
	private void clearPartitions() {
		for (int i = 0; i < this.partitions.size(); i++) {
			InMemoryPartition<T> p = this.partitions.get(i);
			p.clearAllMemory(this.availableMemory);
		}
		this.partitions.clear();
		this.compactionMemory.clearAllMemory(availableMemory);
	}
	
	private void initTable(int numBuckets, byte numPartitions) {
		final int bucketsPerSegment = this.bucketsPerSegmentMask + 1;
		final int numSegs = (numBuckets >>> this.bucketsPerSegmentBits) + ( (numBuckets & this.bucketsPerSegmentMask) == 0 ? 0 : 1);
		final MemorySegment[] table = new MemorySegment[numSegs];
		
		// go over all segments that are part of the table
		for (int i = 0, bucket = 0; i < numSegs && bucket < numBuckets; i++) {
			final MemorySegment seg = getNextBuffer();
			
			// go over all buckets in the segment
			for (int k = 0; k < bucketsPerSegment && bucket < numBuckets; k++, bucket++) {
				final int bucketOffset = k * HASH_BUCKET_SIZE;	
				
				// compute the partition that the bucket corresponds to
				final byte partition = assignPartition(bucket, numPartitions);
				
				// initialize the header fields
				seg.put(bucketOffset + HEADER_PARTITION_OFFSET, partition);
				seg.putInt(bucketOffset + HEADER_COUNT_OFFSET, 0);
				seg.putLong(bucketOffset + HEADER_FORWARD_OFFSET, BUCKET_FORWARD_POINTER_NOT_SET);
			}
			
			table[i] = seg;
		}
		this.buckets = table;
		this.numBuckets = numBuckets;
	}
	
	private void releaseTable() {
		// set the counters back
		this.numBuckets = 0;
		if (this.buckets != null) {
			for (int i = 0; i < this.buckets.length; i++) {
				this.availableMemory.add(this.buckets[i]);
			}
			this.buckets = null;
		}
	}
	
	private final MemorySegment getNextBuffer() {
		// check if the list directly offers memory
		int s = this.availableMemory.size();
		if (s > 0) {
			return this.availableMemory.remove(s-1);
		} else {
			throw new RuntimeException("Memory ran out. " + getMemoryConsumptionString());
		}
	}

	// --------------------------------------------------------------------------------------------
	//                             Utility Computational Functions
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the number of partitions to be used for an initial hash-table, when no estimates are
	 * available.
	 * <p>
	 * The current logic makes sure that there are always between 10 and 32 partitions, and close
	 * to 0.1 of the number of buffers.
	 * 
	 * @param numBuffers The number of buffers available.
	 * @return The number of partitions to use.
	 */
	private static final int getPartitioningFanOutNoEstimates(int numBuffers) {
		return Math.max(10, Math.min(numBuffers / 10, MAX_NUM_PARTITIONS));
	}
	
	/**
	 * @return String containing a summary of the memory consumption for error messages
	 */
	private String getMemoryConsumptionString() {
		String result = new String("numPartitions: " + this.partitions.size() + 
				" minPartition: " + getMinPartition() +
				" maxPartition: " + getMaxPartition() +
				" number of overflow segments: " + getOverflowSegmentCount() +
				" bucketSize: " + this.buckets.length +
				" Overall memory: " + getSize() + 
				" Partition memory: " + getPartitionSize());
		return result;
	}
	
	/**
	 * Size of all memory segments owned by this hash table
	 * 
	 * @return size in bytes
	 */
	private long getSize() {
		long numSegments = 0;
		numSegments += this.availableMemory.size();
		numSegments += this.buckets.length;
		for(InMemoryPartition<T> p : this.partitions) {
			numSegments += p.getBlockCount();
			numSegments += p.numOverflowSegments;
		}
		numSegments += this.compactionMemory.getBlockCount();
		return numSegments*this.segmentSize;
	}
	
	/**
	 * Size of all memory segments owned by the partitions of this hash table excluding the compaction partition
	 * 
	 * @return size in bytes
	 */
	private long getPartitionSize() {
		long numSegments = 0;
		for(InMemoryPartition<T> p : this.partitions) {
			numSegments += p.getBlockCount();
		}
		return numSegments*this.segmentSize;
	}
	
	/**
	 * @return number of memory segments in the largest partition
	 */
	private int getMaxPartition() {
		int maxPartition = 0;
		for(InMemoryPartition<T> p1 : this.partitions) {
			if(p1.getBlockCount() > maxPartition) {
				maxPartition = p1.getBlockCount();
			}
		}
		return maxPartition;
	}
	
	/**
	 * @return number of memory segments in the smallest partition
	 */
	private int getMinPartition() {
		int minPartition = Integer.MAX_VALUE;
		for(InMemoryPartition<T> p1 : this.partitions) {
			if(p1.getBlockCount() < minPartition) {
				minPartition = p1.getBlockCount();
			}
		}
		return minPartition;
	}
	
	/**
	 * @return number of memory segments used in overflow buckets
	 */
	private int getOverflowSegmentCount() {
		int result = 0;
		for(InMemoryPartition<T> p : this.partitions) {
			result += p.numOverflowSegments;
		}
		return result;
	}
	
	/**
	 * tries to find a good value for the number of buckets
	 * will ensure that the number of buckets is a multiple of numPartitions
	 * 
	 * @return number of buckets
	 */
	private static final int getInitialTableSize(int numBuffers, int bufferSize, int numPartitions, int recordLenBytes) {
		final long totalSize = ((long) bufferSize) * numBuffers;
		final long numRecordsStorable = totalSize / (recordLenBytes + RECORD_OVERHEAD_BYTES);
		final long bucketBytes = numRecordsStorable * RECORD_OVERHEAD_BYTES;
		long numBuckets = bucketBytes / (2 * HASH_BUCKET_SIZE) + 1;
		while(numBuckets % numPartitions != 0) {
			numBuckets++;
		}
		return numBuckets > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) numBuckets;
	}
	
	/**
	 * Assigns a partition to a bucket.
	 * 
	 * @param bucket bucket index
	 * @param numPartitions number of partitions
	 * @return The hash code for the integer.
	 */
	private static final byte assignPartition(int bucket, byte numPartitions) {
		return (byte) (bucket % numPartitions);
	}
	
	/**
	 * FOR TESTING ONLY
	 */
	public void triggerFullCompaction() throws IOException {
		for(int i = 0; i < this.partitions.size(); i++) {
			compactPartition(i);
		}
	}
	
	/**
	 * FOR TESTING ONLY
	 */
	public boolean triggerResize() throws IOException {
		return resizeHashTable();
	}
	
	/**
	 * Attempts to double the number of buckets
	 * 
	 * @return true on success
	 * @throws IOException 
	 */
	private boolean resizeHashTable() throws IOException {
		final int newNumBuckets = 2*this.numBuckets;
		final int bucketsPerSegment = this.bucketsPerSegmentMask + 1;
		final int newNumSegments = (newNumBuckets + (bucketsPerSegment-1)) / bucketsPerSegment;
		final int additionalSegments = newNumSegments-this.buckets.length;
		final int numPartitions = this.partitions.size();
		if(this.availableMemory.size() < additionalSegments) {
			for(int i = 0; i < numPartitions; i++) {
				compactPartition(i);
				if(this.availableMemory.size() < additionalSegments) {
					break;
				}
			}
		}
		if(this.availableMemory.size() < additionalSegments || this.closed.get()) {
			return false;
		} else {
			this.isResizing = true;
			// allocate new buckets
			final int startOffset = (this.numBuckets * HASH_BUCKET_SIZE) % this.segmentSize;
			MemorySegment[] newBuckets = new MemorySegment[additionalSegments];
			final int oldNumBuckets = this.numBuckets;
			final int oldNumSegments = this.buckets.length;
			MemorySegment[] mergedBuckets = new MemorySegment[newNumSegments];
			System.arraycopy(this.buckets, 0, mergedBuckets, 0, this.buckets.length);
			System.arraycopy(newBuckets, 0, mergedBuckets, this.buckets.length, newBuckets.length);
			this.buckets = mergedBuckets;
			this.numBuckets = newNumBuckets;
			// initialize all new buckets
			boolean oldSegment = (startOffset != 0);
			final int startSegment = oldSegment ? (oldNumSegments-1) : oldNumSegments;
			for (int i = startSegment, bucket = oldNumBuckets; i < newNumSegments && bucket < this.numBuckets; i++) {
				MemorySegment seg;
				int bucketOffset = 0;
				if(oldSegment) { // the first couple of new buckets may be located on an old segment
					seg = this.buckets[i];
					for (int k = (oldNumBuckets % bucketsPerSegment) ; k < bucketsPerSegment && bucket < this.numBuckets; k++, bucket++) {
						bucketOffset = k * HASH_BUCKET_SIZE;	
						// initialize the header fields
						seg.put(bucketOffset + HEADER_PARTITION_OFFSET, assignPartition(bucket, (byte)numPartitions));
						seg.putInt(bucketOffset + HEADER_COUNT_OFFSET, 0);
						seg.putLong(bucketOffset + HEADER_FORWARD_OFFSET, BUCKET_FORWARD_POINTER_NOT_SET);
					}
				} else {
					seg = getNextBuffer();
					// go over all buckets in the segment
					for (int k = 0; k < bucketsPerSegment && bucket < this.numBuckets; k++, bucket++) {
						bucketOffset = k * HASH_BUCKET_SIZE;	
						// initialize the header fields
						seg.put(bucketOffset + HEADER_PARTITION_OFFSET, assignPartition(bucket, (byte)numPartitions));
						seg.putInt(bucketOffset + HEADER_COUNT_OFFSET, 0);
						seg.putLong(bucketOffset + HEADER_FORWARD_OFFSET, BUCKET_FORWARD_POINTER_NOT_SET);
					}
				}				
				this.buckets[i] = seg;
				oldSegment = false; // we write on at most one old segment
			}
			int hashOffset = 0;
			int hash = 0;
			int pointerOffset = 0;
			long pointer = 0;
			IntArrayList hashList = new IntArrayList(NUM_ENTRIES_PER_BUCKET);
			LongArrayList pointerList = new LongArrayList(NUM_ENTRIES_PER_BUCKET);
			IntArrayList overflowHashes = new IntArrayList(64);
			LongArrayList overflowPointers = new LongArrayList(64);
			// go over all buckets and split them between old and new buckets
			for(int i = 0; i < numPartitions; i++) {
				InMemoryPartition<T> partition = this.partitions.get(i);
				final MemorySegment[] overflowSegments = partition.overflowSegments;
				int posHashCode = 0;
				for (int j = 0, bucket = i; j < this.buckets.length && bucket < oldNumBuckets; j++) {
					MemorySegment segment = this.buckets[j];
					// go over all buckets in the segment belonging to the partition
					for (int k = bucket % bucketsPerSegment; k < bucketsPerSegment && bucket < oldNumBuckets; k += numPartitions, bucket += numPartitions) {
						int bucketOffset = k * HASH_BUCKET_SIZE;
						if((int)segment.get(bucketOffset + HEADER_PARTITION_OFFSET) != i) {
							throw new IOException("Accessed wrong bucket! wanted: " + i + " got: " + segment.get(bucketOffset + HEADER_PARTITION_OFFSET));
						}
						// loop over all segments that are involved in the bucket (original bucket plus overflow buckets)
						int countInSegment = segment.getInt(bucketOffset + HEADER_COUNT_OFFSET);
						int numInSegment = 0;
						pointerOffset = bucketOffset + BUCKET_POINTER_START_OFFSET;
						hashOffset = bucketOffset + BUCKET_HEADER_LENGTH;
						while (true) {
							while (numInSegment < countInSegment) {
								hash = segment.getInt(hashOffset);
								if((hash % this.numBuckets) != bucket && (hash % this.numBuckets) != (bucket+oldNumBuckets)) {
									throw new IOException("wanted: " + bucket + " or " + (bucket + oldNumBuckets) + " got: " + hash%this.numBuckets);
								}
								pointer = segment.getLong(pointerOffset);
								hashList.add(hash);
								pointerList.add(pointer);
								pointerOffset += POINTER_LEN;
								hashOffset += HASH_CODE_LEN;
								numInSegment++;
							}
							// this segment is done. check if there is another chained bucket
							final long forwardPointer = segment.getLong(bucketOffset + HEADER_FORWARD_OFFSET);
							if (forwardPointer == BUCKET_FORWARD_POINTER_NOT_SET) {
								break;
							}
							final int overflowSegNum = (int) (forwardPointer >>> 32);
							segment = overflowSegments[overflowSegNum];
							bucketOffset = (int)(forwardPointer & 0xffffffff);
							countInSegment = segment.getInt(bucketOffset + HEADER_COUNT_OFFSET);
							pointerOffset = bucketOffset + BUCKET_POINTER_START_OFFSET;
							hashOffset = bucketOffset + BUCKET_HEADER_LENGTH;
							numInSegment = 0;
						}
						segment = this.buckets[j];
						bucketOffset = k * HASH_BUCKET_SIZE;
						// reset bucket for re-insertion
						segment.putInt(bucketOffset + HEADER_COUNT_OFFSET, 0);
						segment.putLong(bucketOffset + HEADER_FORWARD_OFFSET, BUCKET_FORWARD_POINTER_NOT_SET);
						// refill table
						if(hashList.size() != pointerList.size()) {
							throw new IOException("Pointer and hash counts do not match. hashes: " + hashList.size() + " pointer: " + pointerList.size());
						}
						int newSegmentIndex = (bucket + oldNumBuckets) / bucketsPerSegment;
						MemorySegment newSegment = this.buckets[newSegmentIndex];
						// we need to avoid overflows in the first run
						int oldBucketCount = 0;
						int newBucketCount = 0;
						while(!hashList.isEmpty()) {
							hash = hashList.removeInt(hashList.size()-1);
							pointer = pointerList.removeLong(pointerList.size()-1);
							posHashCode = hash % this.numBuckets;
							if(posHashCode == bucket && oldBucketCount < NUM_ENTRIES_PER_BUCKET) {
								bucketOffset = (bucket % bucketsPerSegment) * HASH_BUCKET_SIZE;
								insertBucketEntryFromStart(partition, segment, bucketOffset, hash, pointer);
								oldBucketCount++;
							} else if(posHashCode == (bucket + oldNumBuckets) && newBucketCount < NUM_ENTRIES_PER_BUCKET) {
								bucketOffset = ((bucket + oldNumBuckets) % bucketsPerSegment) * HASH_BUCKET_SIZE;
								insertBucketEntryFromStart(partition, newSegment, bucketOffset, hash, pointer);
								newBucketCount++;
							} else if(posHashCode == (bucket + oldNumBuckets) || posHashCode == bucket) {
								overflowHashes.add(hash);
								overflowPointers.add(pointer);
							} else {
								throw new IOException("Accessed wrong bucket. Target: " + bucket + " or " + (bucket + oldNumBuckets) + " Hit: " + posHashCode);
							}
						}
						hashList.clear();
						pointerList.clear();
					}
				}
				// reset partition's overflow buckets and reclaim their memory
				this.availableMemory.addAll(partition.resetOverflowBuckets());
				// clear overflow lists
				int bucketArrayPos = 0;
				int bucketInSegmentPos = 0;
				MemorySegment bucket = null;
				while(!overflowHashes.isEmpty()) {
					hash = overflowHashes.removeInt(overflowHashes.size()-1);
					pointer = overflowPointers.removeLong(overflowPointers.size()-1);
					posHashCode = hash % this.numBuckets; 
					bucketArrayPos = posHashCode >>> this.bucketsPerSegmentBits;
					bucketInSegmentPos = (posHashCode & this.bucketsPerSegmentMask) << NUM_INTRA_BUCKET_BITS;
					bucket = this.buckets[bucketArrayPos];
					insertBucketEntryFromStart(partition, bucket, bucketInSegmentPos, hash, pointer);
				}
				overflowHashes.clear();
				overflowPointers.clear();
			}
			this.isResizing = false;
			return true;
		}
	}
	
	/**
	 * Compacts (garbage collects) partition with copy-compact strategy using compaction partition
	 * 
	 * @param partitionNumber partition to compact
	 * @throws IOException 
	 */
	private void compactPartition(final int partitionNumber) throws IOException {
		// do nothing if table was closed, parameter is invalid or no garbage exists
		if(this.closed.get() || partitionNumber >= this.partitions.size() || this.partitions.get(partitionNumber).isCompacted()) {
			return;
		}
		// release all segments owned by compaction partition
		this.compactionMemory.clearAllMemory(availableMemory);
		this.compactionMemory.allocateSegments(1);
		this.compactionMemory.pushDownPages();
		T tempHolder = this.buildSideSerializer.createInstance();
		final int numPartitions = this.partitions.size();
		InMemoryPartition<T> partition = this.partitions.remove(partitionNumber);
		MemorySegment[] overflowSegments = partition.overflowSegments;
		long pointer = 0L;
		int pointerOffset = 0;
		int bucketOffset = 0;
		final int bucketsPerSegment = this.bucketsPerSegmentMask + 1;
		for (int i = 0, bucket = partitionNumber; i < this.buckets.length && bucket < this.numBuckets; i++) {
			MemorySegment segment = this.buckets[i];
			// go over all buckets in the segment belonging to the partition
			for (int k = bucket % bucketsPerSegment; k < bucketsPerSegment && bucket < this.numBuckets; k += numPartitions, bucket += numPartitions) {
				bucketOffset = k * HASH_BUCKET_SIZE;
				if((int)segment.get(bucketOffset + HEADER_PARTITION_OFFSET) != partitionNumber) {
					throw new IOException("Accessed wrong bucket! wanted: " + partitionNumber + " got: " + segment.get(bucketOffset + HEADER_PARTITION_OFFSET));
				}
				// loop over all segments that are involved in the bucket (original bucket plus overflow buckets)
				int countInSegment = segment.getInt(bucketOffset + HEADER_COUNT_OFFSET);
				int numInSegment = 0;
				pointerOffset = bucketOffset + BUCKET_POINTER_START_OFFSET;
				while (true) {
					while (numInSegment < countInSegment) {
						pointer = segment.getLong(pointerOffset);
						partition.readRecordAt(pointer, tempHolder);
						pointer = this.compactionMemory.appendRecord(tempHolder);
						segment.putLong(pointerOffset, pointer);
						pointerOffset += POINTER_LEN;
						numInSegment++;
					}
					// this segment is done. check if there is another chained bucket
					final long forwardPointer = segment.getLong(bucketOffset + HEADER_FORWARD_OFFSET);
					if (forwardPointer == BUCKET_FORWARD_POINTER_NOT_SET) {
						break;
					}
					final int overflowSegNum = (int) (forwardPointer >>> 32);
					segment = overflowSegments[overflowSegNum];
					bucketOffset = (int)(forwardPointer & 0xffffffff);
					countInSegment = segment.getInt(bucketOffset + HEADER_COUNT_OFFSET);
					pointerOffset = bucketOffset + BUCKET_POINTER_START_OFFSET;
					numInSegment = 0;
				}
				segment = this.buckets[i];
			}
		}
		// swap partition with compaction partition
		this.compactionMemory.setPartitionNumber(partitionNumber);
		this.partitions.add(partitionNumber, compactionMemory);
		this.partitions.get(partitionNumber).overflowSegments = partition.overflowSegments;
		this.partitions.get(partitionNumber).numOverflowSegments = partition.numOverflowSegments;
		this.partitions.get(partitionNumber).nextOverflowBucket = partition.nextOverflowBucket;
		this.partitions.get(partitionNumber).setCompaction(true);
		//this.partitions.get(partitionNumber).pushDownPages();
		this.compactionMemory = partition;
		this.compactionMemory.resetRecordCounter();
		this.compactionMemory.setPartitionNumber(-1);
		this.compactionMemory.overflowSegments = null;
		this.compactionMemory.numOverflowSegments = 0;
		this.compactionMemory.nextOverflowBucket = 0;
		// try to allocate maximum segment count
		this.compactionMemory.clearAllMemory(this.availableMemory);
		int maxSegmentNumber = this.getMaxPartition();
		this.compactionMemory.allocateSegments(maxSegmentNumber);
		this.compactionMemory.resetRWViews();
		this.compactionMemory.pushDownPages();
	}
	
	/**
	 * Compacts partition but may not reclaim all garbage
	 * 
	 * @param partition partition number
	 * @throws IOException 
	 */
	@SuppressWarnings("unused")
	private void fastCompactPartition(int partitionNumber) throws IOException {
		// stop if no garbage exists
		if(this.partitions.get(partitionNumber).isCompacted()) {
			return;
		}
		//TODO IMPLEMENT ME
		return;
	}
	
	/**
	 * This function hashes an integer value. It is adapted from Bob Jenkins' website
	 * <a href="http://www.burtleburtle.net/bob/hash/integer.html">http://www.burtleburtle.net/bob/hash/integer.html</a>.
	 * The hash function has the <i>full avalanche</i> property, meaning that every bit of the value to be hashed
	 * affects every bit of the hash value. 
	 * 
	 * @param code The integer to be hashed.
	 * @return The hash code for the integer.
	 */
	private static final int hash(int code) {
		code = (code + 0x7ed55d16) + (code << 12);
		code = (code ^ 0xc761c23c) ^ (code >>> 19);
		code = (code + 0x165667b1) + (code << 5);
		code = (code + 0xd3a2646c) ^ (code << 9);
		code = (code + 0xfd7046c5) + (code << 3);
		code = (code ^ 0xb55a4f09) ^ (code >>> 16);
		return code >= 0 ? code : -(code + 1);
	}
	
	/**
	 * Iterator that traverses the whole hash table once
	 * 
	 * If entries are inserted during iteration they may be overlooked by the iterator
	 */
	public class EntryIterator implements MutableObjectIterator<T> {
		
		private CompactingHashTable<T> table;
		
		private ArrayList<T> cache; // holds full bucket including its overflow buckets
				
		private int currentBucketIndex = 0;
		private int currentSegmentIndex = 0;
		private int currentBucketOffset = 0;
		private int bucketsPerSegment;
		
		private boolean done;
		
		private EntryIterator(CompactingHashTable<T> compactingHashTable) {
			this.table = compactingHashTable;
			this.cache = new ArrayList<T>(64);
			this.done = false;
			this.bucketsPerSegment = table.bucketsPerSegmentMask + 1;
		}

		@Override
		public T next(T reuse) throws IOException {
			if(done || this.table.closed.get()) {
				return null;
			} else if(!cache.isEmpty()) {
				reuse = cache.remove(cache.size()-1);
				return reuse;
			} else {
				while(!done && cache.isEmpty()) {
					done = !fillCache();
				}
				if(!done) {
					reuse = cache.remove(cache.size()-1);
					return reuse;
				} else {
					return null;
				}
			}
		}

		/**
		 * utility function that inserts all entries from a bucket and its overflow buckets into the cache
		 * 
		 * @return true if last bucket was not reached yet
		 * @throws IOException
		 */
		private boolean fillCache() throws IOException {
			if(currentBucketIndex >= table.numBuckets) {
				return false;
			}
			MemorySegment bucket = table.buckets[currentSegmentIndex];
			// get the basic characteristics of the bucket
			final int partitionNumber = bucket.get(currentBucketOffset + HEADER_PARTITION_OFFSET);
			final InMemoryPartition<T> partition = table.partitions.get(partitionNumber);
			final MemorySegment[] overflowSegments = partition.overflowSegments;
			
			int countInSegment = bucket.getInt(currentBucketOffset + HEADER_COUNT_OFFSET);
			int numInSegment = 0;
			int posInSegment = currentBucketOffset + BUCKET_POINTER_START_OFFSET;
			int bucketOffset = currentBucketOffset;

			// loop over all segments that are involved in the bucket (original bucket plus overflow buckets)
			while (true) {
				while (numInSegment < countInSegment) {
					long pointer = bucket.getLong(posInSegment);
					posInSegment += POINTER_LEN;
					numInSegment++;
					T target = table.buildSideSerializer.createInstance();
					try {
						partition.readRecordAt(pointer, target);
						cache.add(target);
					} catch (IOException e) {
							throw new RuntimeException("Error deserializing record from the Hash Table: " + e.getMessage(), e);
					}
				}
				// this segment is done. check if there is another chained bucket
				final long forwardPointer = bucket.getLong(bucketOffset + HEADER_FORWARD_OFFSET);
				if (forwardPointer == BUCKET_FORWARD_POINTER_NOT_SET) {
					break;
				}
				final int overflowSegNum = (int) (forwardPointer >>> 32);
				bucket = overflowSegments[overflowSegNum];
				bucketOffset = (int)(forwardPointer & 0xffffffff);
				countInSegment = bucket.getInt(bucketOffset + HEADER_COUNT_OFFSET);
				posInSegment = bucketOffset + BUCKET_POINTER_START_OFFSET;
				numInSegment = 0;
			}
			currentBucketIndex++;
			if(currentBucketIndex % bucketsPerSegment == 0) {
				currentSegmentIndex++;
				currentBucketOffset = 0;
			} else {
				currentBucketOffset += HASH_BUCKET_SIZE;
			}
			return true;
		}
		
	}
	
	public final class HashTableProber<PT> extends AbstractHashTableProber<PT, T>{
		
		private InMemoryPartition<T> partition;
		
		private MemorySegment bucket;
		
		private int pointerOffsetInBucket;
		
		
		private HashTableProber(TypeComparator<PT> probeTypeComparator, TypePairComparator<PT, T> pairComparator)
		{
			super(probeTypeComparator, pairComparator);
		}
		
		public boolean getMatchFor(PT probeSideRecord, T targetForMatch) {
			if(closed.get()) {
				return false;
			}
			final int searchHashCode = hash(this.probeTypeComparator.hash(probeSideRecord));
			
			final int posHashCode = searchHashCode % numBuckets;
			
			// get the bucket for the given hash code
			MemorySegment bucket = buckets[posHashCode >> bucketsPerSegmentBits];
			int bucketInSegmentOffset = (posHashCode & bucketsPerSegmentMask) << NUM_INTRA_BUCKET_BITS;
			
			// get the basic characteristics of the bucket
			final int partitionNumber = bucket.get(bucketInSegmentOffset + HEADER_PARTITION_OFFSET);
			final InMemoryPartition<T> p = partitions.get(partitionNumber);
			final MemorySegment[] overflowSegments = p.overflowSegments;
			
			this.pairComparator.setReference(probeSideRecord);
			
			int countInSegment = bucket.getInt(bucketInSegmentOffset + HEADER_COUNT_OFFSET);
			int numInSegment = 0;
			int posInSegment = bucketInSegmentOffset + BUCKET_HEADER_LENGTH;

			// loop over all segments that are involved in the bucket (original bucket plus overflow buckets)
			while (true) {
				
				while (numInSegment < countInSegment) {
					
					final int thisCode = bucket.getInt(posInSegment);
					posInSegment += HASH_CODE_LEN;
						
					// check if the hash code matches
					if (thisCode == searchHashCode) {
						// get the pointer to the pair
						final int pointerOffset = bucketInSegmentOffset + BUCKET_POINTER_START_OFFSET + (numInSegment * POINTER_LEN);
						final long pointer = bucket.getLong(pointerOffset);
						numInSegment++;
						
						// deserialize the key to check whether it is really equal, or whether we had only a hash collision
						try {
							p.readRecordAt(pointer, targetForMatch);
							
							if (this.pairComparator.equalToReference(targetForMatch)) {
								this.partition = p;
								this.bucket = bucket;
								this.pointerOffsetInBucket = pointerOffset;
								return true;
							}
						}
						catch (IOException e) {
							throw new RuntimeException("Error deserializing record from the hashtable: " + e.getMessage(), e);
						}
					}
					else {
						numInSegment++;
					}
				}
				
				// this segment is done. check if there is another chained bucket
				final long forwardPointer = bucket.getLong(bucketInSegmentOffset + HEADER_FORWARD_OFFSET);
				if (forwardPointer == BUCKET_FORWARD_POINTER_NOT_SET) {
					return false;
				}
				
				final int overflowSegNum = (int) (forwardPointer >>> 32);
				bucket = overflowSegments[overflowSegNum];
				bucketInSegmentOffset = (int) (forwardPointer & 0xffffffff);
				countInSegment = bucket.getInt(bucketInSegmentOffset + HEADER_COUNT_OFFSET);
				posInSegment = bucketInSegmentOffset + BUCKET_HEADER_LENGTH;
				numInSegment = 0;
			}
		}
		
		public void updateMatch(T record) throws IOException {
			if(closed.get()) {
				return;
			}
			long newPointer;
			try {
				newPointer = this.partition.appendRecord(record);
			} catch (EOFException e) {
				// system is out of memory so we attempt to reclaim memory with a copy compact run
				try {
					int partitionNumber = this.partition.getPartitionNumber();
					compactPartition(partitionNumber);
					// retry append
					this.partition = partitions.get(partitionNumber);
					newPointer = this.partition.appendRecord(record);
				} catch (EOFException ex) {
					throw new RuntimeException("Memory ran out. Compaction failed. " + 
												getMemoryConsumptionString() +
												" Message: " + ex.getMessage());
				} catch (IndexOutOfBoundsException ex) {
					throw new RuntimeException("Memory ran out. Compaction failed. " + 
												getMemoryConsumptionString() +
												" Message: " + ex.getMessage());
				}
			} catch (IndexOutOfBoundsException e) {
				// system is out of memory so we attempt to reclaim memory with a copy compact run
				try {
					int partitionNumber = this.partition.getPartitionNumber();
					compactPartition(partitionNumber);
					// retry append
					this.partition = partitions.get(partitionNumber);
					newPointer = this.partition.appendRecord(record);
				} catch (EOFException ex) {
					throw new RuntimeException("Memory ran out. Compaction failed. " + 
												getMemoryConsumptionString() +
												" Message: " + ex.getMessage());
				} catch (IndexOutOfBoundsException ex) {
					throw new RuntimeException("Memory ran out. Compaction failed. " + 
												getMemoryConsumptionString() +
												" Message: " + ex.getMessage());
				}
			}
			this.bucket.putLong(this.pointerOffsetInBucket, newPointer);
			this.partition.setCompaction(false);
		}
	}
}
