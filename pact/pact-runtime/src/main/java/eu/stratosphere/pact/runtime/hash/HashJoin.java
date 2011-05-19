/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.hash;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.LinkedBlockingQueue;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.services.iomanager.BlockChannelWriter;
import eu.stratosphere.nephele.services.iomanager.Buffer;
import eu.stratosphere.nephele.services.iomanager.Channel;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.iomanager.SerializationFactory;
import eu.stratosphere.nephele.services.memorymanager.DataInputView;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;


/**
 *
 *
 * The layout of the buckets inside a memory segment is as follows:
 * 
 * <pre>
 * +----------------------------- Bucket x ----------------------------
 * |Partition (1 byte) | Status (1 byte) | element count (2 bytes) |
 * | next-bucket-in-chain-pointer (8 bytes) | reserved (4 bytes) |
 * |
 * |hashCode 1 (4 bytes) | hashCode 2 (4 bytes) | hashCode 3 (4 bytes) |
 * | ... hashCode n-1 (4 bytes) | hashCode n (4 bytes)
 * |
 * |pointer 1 (8 bytes) | pointer 2 (8 bytes) | pointer 3 (8 bytes) |
 * | ... pointer n-1 (8 bytes) | pointer n (8 bytes)
 * +---------------------------- Bucket x + 1--------------------------
 * |Partition (1 byte) | Status (1 byte) | element count (2 bytes) |
 * | next-bucket-in-chain-pointer (8 bytes) | reserved (4 bytes) |
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
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class HashJoin<K extends Key, V extends Value>
{
	// ------------------------------------------------------------------------
	//                         Internal Constants
	// ------------------------------------------------------------------------
	
	private static final int MIN_NUM_MEMORY_SEGMENTS = 33;
	
	private static final int MAX_NUM_PARTITIONS = Byte.MAX_VALUE;
	
	private static final int DEFAULT_RECORD_LEN = 100;
	
	/**
	 * The length of the hash code stored in the bucket.
	 */
	private static final int HASH_CODE_LEN = 4;
	
	/**
	 * The length of a pointer from a hash bucket to the record in the buffers.
	 */
	private static final int POINTER_LEN = 8;
	
	/**
	 * The storage overhead per record, in bytes. This corresponds to the space in the
	 * actual hash table buckets, consisting of a 4 byte hash value and an 8 byte
	 * pointer.
	 */
	private static final int RECORD_OVERHEAD_BYTES = HASH_CODE_LEN + POINTER_LEN;
	
	// -------------------------- Bucket Size and Structure -------------------------------------
	
	private static final int NUM_INTRA_BUCKET_BITS = 10;
	
	private static final int HASH_BUCKET_SIZE = 0x1 << NUM_INTRA_BUCKET_BITS;
	
	private static final int BUCKET_HEADER_LENGTH = 16;
	
	private static final int NUM_ENTRIES_PER_BUCKET = (HASH_BUCKET_SIZE - BUCKET_HEADER_LENGTH) / RECORD_OVERHEAD_BYTES;
	
	private static final int BUCKET_POINTER_START_OFFSET = BUCKET_HEADER_LENGTH + (NUM_ENTRIES_PER_BUCKET * HASH_CODE_LEN);
	
	// ------------------------------ Bucket Header Fields ------------------------------
	
	private static final int HEADER_PARTITION_OFFSET = 0;
	
	private static final int HEADER_STATUS_OFFSET = 1;
	
	private static final int HEADER_COUNT_OFFSET = 2;
	
	private static final int HEADER_FORWARD_OFFSET = 4;	
	
	
	private static final long BUCKET_FORWARD_POINTER_NOT_SET = ~(0x0L);
	
	private static final byte BUCKET_STATUS_SPILLED = 1;
	
	private static final byte BUCKET_STATUS_IN_MEMORY = 0;
	
	
	// ------------------------------------------------------------------------
	//                              Members
	// ------------------------------------------------------------------------
	
	/**
	 * An iterator over the input that will be used to build the hash-table.
	 */
	private final Iterator<KeyValuePair<K, V>> buildSideInput;
	
	/**
	 * An iterator over the input that will be used to probe the hash-table.
	 */
	private final Iterator<KeyValuePair<K, V>> probeSideInput;
	
	/**
	 * The free memory segments currently available to the hash join.
	 */
	private final List<MemorySegment> availableMemory;
	
	/**
	 * The queue of buffers that can be used for write-behind. Any buffer that is written
	 * asynchronously to disk is returned through this queue. hence, it may sometimes contain more
	 */
	private final LinkedBlockingQueue<Buffer.Output> writeBehindBuffers;
	
	/**
	 * The I/O manager used to instantiate writers for the spilled partitions.
	 */
	private final IOManager ioManager;
	
	/**
	 * The lock that synchronizes the closing.
	 */
	private final Object closeLock = new Object();
	
	/**
	 * The size of the segments used by the hash join buckets. All segments must be of equal size to ease offset computations.
	 */
	private final int segmentSize;
	
	/**
	 * The number of write-behind buffers used.
	 */
	private final int numWriteBehindBuffers;
	
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
	 * The partitions that are built by processing the current partition.
	 */
	private final ArrayList<Partition> partitionsBeingBuilt;
	
	private HashBucketIterator<K, V> bucketIterator;
	
	private ProbeSideIterator<K, V> probeIterator;
	
	/**
	 * The channel enumerator that is used while processing the current partition to create
	 * channels for the spill partitions it requires.
	 */
	private Channel.Enumerator currentEnumerator;
	
	private MemorySegment[] buckets;
	
	private int numBuckets;
	
	
	
	

	
	/**
	 * The number of buffers in the write behind queue that are actually not write behind buffers,
	 * but regular buffers that only have not yet returned. This is part of an optimization that the
	 * spilling code needs not wait until the partition is completely spilled before proceeding.
	 */
	private int writeBehindBuffersAvailable;
	
	private int currentRecursionDepth;
	
	/**
	 * 
	 */
	private volatile boolean closed;

	
	// ------------------------------------------------------------------------
	//                         Construction and Teardown
	// ------------------------------------------------------------------------
	
	public HashJoin(Iterator<KeyValuePair<K, V>> buildSideInput, Iterator<KeyValuePair<K, V>> probeSideInput,
			List<MemorySegment> memorySegments,
			IOManager ioManager)
	{
		this(buildSideInput, probeSideInput, memorySegments, ioManager, DEFAULT_RECORD_LEN);
	}
	
	
	public HashJoin(Iterator<KeyValuePair<K, V>> buildSideInput, Iterator<KeyValuePair<K, V>> probeSideInput,
			List<MemorySegment> memorySegments,
			IOManager ioManager,
			int avgRecordLen)
	{
		// some sanity checks first
		if (buildSideInput == null || probeSideInput == null || memorySegments == null) {
			throw new NullPointerException();
		}
		if (memorySegments.size() < MIN_NUM_MEMORY_SEGMENTS) {
			throw new IllegalArgumentException("Too few memory segments provided. Hash Join needs at leas " + 
				MIN_NUM_MEMORY_SEGMENTS + " memory segments.");
		}
		
		// assign the members
		this.buildSideInput = buildSideInput;
		this.probeSideInput = probeSideInput;
		this.availableMemory = memorySegments;
		this.ioManager = ioManager;
		
		this.avgRecordLen = avgRecordLen < 1 ? DEFAULT_RECORD_LEN : avgRecordLen;
		
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
		this.bucketsPerSegmentBits = log2floor(bucketsPerSegment);
		
		// take away the write behind buffers
		this.writeBehindBuffers = new LinkedBlockingQueue<Buffer.Output>();
		this.numWriteBehindBuffers = getNumWriteBehindBuffers(memorySegments.size());
		for (int i = this.numWriteBehindBuffers; i > 0; --i)
		{
			this.writeBehindBuffers.add(new Buffer.Output(memorySegments.remove(memorySegments.size() - 1)));
		}
		
		this.partitionsBeingBuilt = new ArrayList<HashJoin.Partition>();
	}
	
	
	// ------------------------------------------------------------------------
	//                              Life-Cycle
	// ------------------------------------------------------------------------
	
	public void open() throws IOException
	{
		// open builds the initial table by consuming the build-side input
		this.currentRecursionDepth = 0;
		buildInitialTable(this.buildSideInput);
		
		// the first prober is the probe-side input
		this.probeIterator = new ProbeSideIterator<K, V>(this.probeSideInput);
		
		this.bucketIterator = new HashBucketIterator<K, V>();
	}
	
	public boolean nextKey() throws IOException
	{
		if (this.probeIterator.nextKey()) {
			final K currKey = this.probeIterator.getCurrentKey();
			
			final int hash = hash(currKey.hashCode(), this.currentRecursionDepth);
			final int posHashCode = hash % this.numBuckets;
			
			// get the bucket for the given hash code
			final int bucketArrayPos = posHashCode >> this.bucketsPerSegmentBits;
			final int bucketInSegmentOffset = (posHashCode & this.bucketsPerSegmentMask) << NUM_INTRA_BUCKET_BITS;
			final MemorySegment bucket = this.buckets[bucketArrayPos];
			
			// get the basic characteristics of the bucket
			final int partitionNumber = bucket.get(bucketInSegmentOffset + HEADER_PARTITION_OFFSET);
			final Partition p = this.partitionsBeingBuilt.get(partitionNumber);
			
			this.bucketIterator.set(bucket, p.overflowSegments, p.partitionBuffers, currKey, hash, bucketInSegmentOffset);
			return true;
		}
		
		// this partition is done!
		return false;
	}
	
	/**
	 * @return
	 */
	public Iterator<V> getProbeSideIterator()
	{
		return this.probeIterator;
	}
	
	/**
	 * @return
	 */
	public HashBucketIterator<K, V> getBuildSideIterator()
	{
		return this.bucketIterator;
	}
	
	/**
	 * Closes the hash table. This effectively releases all internal structures and closes all
	 * open files and removes them.
	 */
	public void close()
	{
		// make sure that we close only once
		synchronized (this.closeLock) {
			if (this.closed) {
				return;
			}
			this.closed = true;
		}
		
		// clear the iterators, so the next call to next() will notice
		
		// release the table structure
		releaseTable();
		
		// clear the memory in the partitions
		clearPartitions();
		
		// clear the partitions that are still to be done (that have files on disk)
		
		// return the write-behind buffers
		for (int i = 0; i < this.numWriteBehindBuffers; i++) {
			try {
				this.availableMemory.add(this.writeBehindBuffers.take().dispose());
			}
			catch (InterruptedException iex) {
				throw new RuntimeException("Hashtable closing was interrupted");
			}
		}
	}
	

	
	// ------------------------------------------------------------------------
	//                       Hash Table Building
	// ------------------------------------------------------------------------
	
	
	/**
	 * @param input
	 * @throws IOException
	 */
	protected void buildInitialTable(final Iterator<KeyValuePair<K, V>> input)
	throws IOException
	{
		// create the partitions
		final int partitionFanOut = getPartitioningFanOutNoEstimates(this.availableMemory.size());
		if (partitionFanOut > MAX_NUM_PARTITIONS) {
			throw new RuntimeException("Hash join created ");
		}
		createPartitions(partitionFanOut);
		
		// set up the table structure. the write behind buffers are taken away, as are one buffer per partition
		final int numBuckets = getInitialTableSize(this.availableMemory.size(), this.segmentSize, 
			partitionFanOut, this.avgRecordLen);
		initTable(numBuckets, (byte) partitionFanOut);
		
		// go over the complete input and insert every element into the hash table
		while (input.hasNext())
		{
			final KeyValuePair<K, V> pair = input.next();
			final int hashCode = hash(pair.getKey().hashCode(), 0);
			insertIntoTable(pair, hashCode);
		}
		
		// finalize the partitions
		for (int i = 0; i < this.partitionsBeingBuilt.size(); i++) {
			Partition p = this.partitionsBeingBuilt.get(i);
			p.finalizeBuildPhase();
		}
	}
	
	/**
	 * @param pair
	 * @param hashCode
	 * @throws IOException
	 */
	protected final void insertIntoTable(final KeyValuePair<K, V> pair, final int hashCode)
	throws IOException
	{
		final int posHashCode = hashCode % this.numBuckets;
		
		// get the bucket for the given hash code
		final int bucketArrayPos = posHashCode >> this.bucketsPerSegmentBits;
		final int bucketInSegmentPos = (posHashCode & this.bucketsPerSegmentMask) << NUM_INTRA_BUCKET_BITS;
		final MemorySegment bucket = this.buckets[bucketArrayPos];
		
		// get the basic characteristics of the bucket
		final int partitionNumber = bucket.get(bucketInSegmentPos + HEADER_PARTITION_OFFSET);
		
		// get the partition descriptor for the bucket
		if (partitionNumber < 0 || partitionNumber >= this.partitionsBeingBuilt.size()) {
			throw new RuntimeException("Error: Hash structures in Hash-Join are corrupt. Invalid partition number for bucket.");
		}
		final Partition p = this.partitionsBeingBuilt.get(partitionNumber);
		
		// --------- Step 1: Get the partition for this pair and put the pair into the buffer ---------
		
		long pointer = p.insertIntoBuffer(pair);
		if (pointer == -1) {
			// element was not written because the buffer was full. get the next buffer.
			// if no buffer is available, we need to spill a partition
			MemorySegment nextSeg = getNextBuffer();
			if (nextSeg == null) {
				spillPartition();
				nextSeg = getNextBuffer();
				if (nextSeg == null) {
					throw new RuntimeException("Bug in HybridHashJoin: No memory became available after spilling partition.");
				}
			}
			
			// add the buffer to the partition.
			p.addBuffer(nextSeg);
			
			// retry to write into the buffer
			pointer = p.insertIntoBuffer(pair);
			if (pointer == -1) {
				// retry failed, throw an exception
				throw new IOException("Record could not be added to fresh buffer. Probably cause: Record length exceeds buffer size limit.");
			}
		}
		
		// --------- Step 2: Add the pointer and the hash code to the hash bucket ---------
		
		if (p.isInMemory()) {
			// in-memory partition: add the pointer and the hash-value to the list
			// find the position to put the hash code and pointer
			final int count = bucket.getShort(bucketInSegmentPos + HEADER_COUNT_OFFSET);
			if (count < NUM_ENTRIES_PER_BUCKET)
			{
				// we are good in our current bucket, put the values
				bucket.putInt(bucketInSegmentPos + BUCKET_HEADER_LENGTH + (count * HASH_CODE_LEN), hashCode);	// hash code
				bucket.putLong(bucketInSegmentPos + BUCKET_POINTER_START_OFFSET + (count * POINTER_LEN), pointer); // pointer
				bucket.putShort(bucketInSegmentPos + HEADER_COUNT_OFFSET, (short) (count + 1)); // update count
			}
			else {
				// we need to go to the overflow buckets
				final long originalForwardPointer = bucket.getLong(bucketInSegmentPos + HEADER_FORWARD_OFFSET);
				final long forwardForNewBucket;
				
				if (originalForwardPointer != BUCKET_FORWARD_POINTER_NOT_SET) {
					
					// forward pointer set
					final int overflowSegNum = (int) (originalForwardPointer >>> 32);
					final int segOffset = (int) (originalForwardPointer & 0xffffffff);
					final MemorySegment seg = p.overflowSegments[overflowSegNum];
					
					final short obCount = seg.getShort(segOffset + HEADER_COUNT_OFFSET);
					
					// check if there is space in this overflow bucket
					if (obCount < NUM_ENTRIES_PER_BUCKET) {
						// space in this bucket and we are done
						seg.putInt(segOffset + BUCKET_HEADER_LENGTH + (obCount * HASH_CODE_LEN), hashCode);	// hash code
						seg.putLong(segOffset + BUCKET_POINTER_START_OFFSET + (obCount * POINTER_LEN), pointer); // pointer
						seg.putShort(segOffset + HEADER_COUNT_OFFSET, (short) (obCount + 1)); // update count
						return;
					}
					else {
						// no space here, we need a new bucket. this current overflow bucket will be the
						// target of the new overflow bucket
						forwardForNewBucket = originalForwardPointer;
					}
				}
				else {
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
					if (overflowSeg == null) {
						// no memory available to create overflow bucket. we need to spill a partition
						final int spilledPart = spillPartition();
						if (spilledPart == partitionNumber) {
							// this bucket is no longer in-memory
							return;
						}
						overflowSeg = getNextBuffer();
						if (overflowSeg == null) {
							throw new RuntimeException("Bug in HybridHashJoin: No memory became available after spilling a partition.");
						}
					}
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
				}
				else {
					// there is space in the last overflow bucket
					overflowBucketNum = p.numOverflowSegments - 1;
					overflowSeg = p.overflowSegments[overflowBucketNum];
					overflowBucketOffset = p.nextOverflowBucket << NUM_INTRA_BUCKET_BITS;
				}
				
				// next overflow bucket is one ahead. if the segment is full, the next will be at the beginning
				// of a new segment
				p.nextOverflowBucket = (p.nextOverflowBucket == this.bucketsPerSegmentMask ? 
						0 : p.nextOverflowBucket + 1);
				
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
				overflowSeg.putShort(overflowBucketOffset + HEADER_COUNT_OFFSET, (short) 1); 
			}

		}
		else {
			// partition not in memory, so add nothing to the table at the moment
			return;
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//                          Setup and Tear Down of Structures
	// --------------------------------------------------------------------------------------------
	
	/**
	 * @param numPartitions
	 */
	protected void createPartitions(int numPartitions)
	{
		// sanity check
		if (this.availableMemory.size() < numPartitions) {
			throw new RuntimeException("Bug in Hybrid Hash Join: Cannot create more partisions than number of available buffers.");
		}
		
		this.currentEnumerator = this.ioManager.createChannelEnumerator();
		
		this.partitionsBeingBuilt.clear();
		for (int i = 0; i < numPartitions; i++) {
			Partition p = new Partition(
				this.availableMemory.remove(this.availableMemory.size() - 1),
				this.writeBehindBuffers);
			this.partitionsBeingBuilt.add(p);
		}
	}
	
	protected void clearPartitions()
	{
		for (int i = this.partitionsBeingBuilt.size() - 1; i >= 0; --i)
		{
			Partition p = this.partitionsBeingBuilt.get(i);
			this.availableMemory.add(p.currentPartitionBuffer);
			p.currentPartitionBuffer = null;
			
			// return the overflow segments
			for (int k = 0; k < p.numOverflowSegments; k++) {
				this.availableMemory.add(p.overflowSegments[k]);
			}
			
			// return the partition buffers
			for (int k = p.partitionBuffers.size() - 1; k >= 0; --k) {
				this.availableMemory.add(p.partitionBuffers.get(k));
			}
		}
		this.partitionsBeingBuilt.clear();
	}
	
	/**
	 * @param numBuckets
	 * @param partitionLevel
	 * @param numPartitions
	 * @return
	 */
	protected void initTable(int numBuckets, byte numPartitions)
	{
		final int bucketsPerSegment = this.bucketsPerSegmentMask + 1;
		final int numSegs = (numBuckets >>> this.bucketsPerSegmentBits) + 1;
		final MemorySegment[] table = new MemorySegment[numSegs];
		
		// go over all segments that are part of the table
		for (int i = 0, bucket = 0; i < numSegs && bucket < numBuckets; i++) {
			final MemorySegment seg = this.availableMemory.remove(this.availableMemory.size() - 1);
			
			// go over all buckets in the segment
			for (int k = 0; k < bucketsPerSegment && bucket < numBuckets; k++, bucket++) {
				final int bucketOffset = k * HASH_BUCKET_SIZE;	
				
				// compute the partition that the bucket corresponds to
				final byte partition = assignPartition(bucket, numPartitions);
				
				// initialize the header fields
				seg.put(bucketOffset + HEADER_PARTITION_OFFSET, partition);
				seg.put(bucketOffset + HEADER_STATUS_OFFSET, BUCKET_STATUS_IN_MEMORY);
				seg.putShort(bucketOffset + HEADER_COUNT_OFFSET, (short) 0);
				seg.putLong(bucketOffset + HEADER_FORWARD_OFFSET, BUCKET_FORWARD_POINTER_NOT_SET);
			}
			
			table[i] = seg;
		}
		
		this.buckets = table;
		this.numBuckets = numBuckets;
	}
	
	/**
	 * Releases the table (the array of buckets) and returns the occupied memory segments to the list of free segments.
	 */
	protected void releaseTable()
	{
		// set the counters back
		this.numBuckets = 0;
		
		for (int i = 0; i < this.buckets.length; i++) {
			this.availableMemory.add(this.buckets[i]);
		}
		this.buckets = null;
	}
	
	// --------------------------------------------------------------------------------------------
	//                                    Memory Handling
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Selects a partition and spills it. The number of the spilled partition is returned.
	 * 
	 * @return The number of the spilled partition.
	 */
	protected int spillPartition() throws IOException
	{
		// find the largest partition
		ArrayList<Partition> partitions = this.partitionsBeingBuilt;
		int largestNumBlocks = 0;
		int largestPartNum = -1;
		
		for (int i = 0; i < partitions.size(); i++) {
			Partition p = partitions.get(i);
			if (p.isInMemory() && p.blockCounter > largestNumBlocks) {
				largestNumBlocks = p.blockCounter;
				largestPartNum = i;
			}
		}
		final Partition p = partitions.get(largestPartNum);
		
		// spill the partition
		if (this.currentEnumerator == null) {
			this.currentEnumerator = this.ioManager.createChannelEnumerator();
		}
		int numBuffersFreed = p.spillPartition(this.availableMemory, this.ioManager, this.currentEnumerator.next());
		this.writeBehindBuffersAvailable += numBuffersFreed;
		
		// grab as many buffers as are available directly
		Buffer.Output currBuff = null;
		while (this.writeBehindBuffersAvailable > 0 && (currBuff = this.writeBehindBuffers.poll()) != null) {
			this.availableMemory.add(currBuff.dispose());
			this.writeBehindBuffersAvailable--;
		}
		
		return largestPartNum;
	}
	
	/**
	 * Gets the next buffer to be used with the hash-table, either for an in-memory partition, or for the
	 * table buckets. This method returns <tt>null</tt>, if no more buffer is available. Spilling a partition
	 * may free new buffers then.
	 * 
	 * @return The next buffer to be used by the hash-table, or null, if no buffer remains.
	 * @throws IOException Thrown, if the thread is interrupted while grabbing the next buffer. The I/O
	 *                     exception replaces the <tt>InterruptedException</tt> to consolidate the exception
	 *                     signatures.
	 */
	private final MemorySegment getNextBuffer() throws IOException
	{
		// check if the list directly offers memory
		int s = this.availableMemory.size();
		if (s > 0) {
			return this.availableMemory.remove(s-1);
		}
		
		// check if there are write behind buffers that actually are to be used for the hash table
		if (this.writeBehindBuffersAvailable > 0)
		{
			// grab at least one, no matter what
			MemorySegment toReturn;
			try {
				toReturn = this.writeBehindBuffers.take().dispose();
			}
			catch (InterruptedException iex) {
				throw new IOException("Hybrid Hash Join was interrupted while taking a buffer.");
			}
			this.writeBehindBuffersAvailable--;
			
			// grab as many more buffers as are available directly
			Buffer.Output currBuff = null;
			while (this.writeBehindBuffersAvailable > 0 && (currBuff = this.writeBehindBuffers.poll()) != null) {
				this.availableMemory.add(currBuff.dispose());
				this.writeBehindBuffersAvailable--;
			}
			
			return toReturn;
		}
		else {
			// no memory available
			return null;
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//                             Utility Computational Functions
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Determines the number of buffers to be used for asynchronous write behind. It is currently
	 * computed as the logarithm of the number of buffers to the base 4, rounded up, minus 2.
	 * The upper limit for the number of write behind buffers is however set to six.
	 * 
	 * @param numBuffers The number of available buffers.
	 * @return The number 
	 */
	public static final int getNumWriteBehindBuffers(int numBuffers)
	{
		int numIOBufs = (int) (Math.log(numBuffers) / Math.log(4) - 1.5);
		return numIOBufs > 6 ? 6 : numIOBufs;
	}
	
	/**
	 * Gets the number of partitions to be used for an initial hash-table, when no estimates are
	 * available.
	 * <p>
	 * The current logic makes sure that there are always between 10 and 127 partitions, and close
	 * to 0.1 of the number of buffers.
	 * 
	 * @param numBuffers The number of buffers available.
	 * @return The number of partitions to use.
	 */
	public static final int getPartitioningFanOutNoEstimates(int numBuffers)
	{
		return Math.max(10, Math.min(numBuffers / 10, MAX_NUM_PARTITIONS));
	}
	
	public static final int getInitialTableSize(int numBuffers, int bufferSize, int numPartitions, int recordLenBytes)
	{
		// ----------------------------------------------------------------------------------------
		// the following observations hold:
		// 1) If the records are assumed to be very large, then many buffers need to go to the partitions
		//    and fewer to the table
		// 2) If the records are small, then comparatively many have to go to the buckets, and fewer to the
		//    partitions
		// 3) If the bucket-table is chosen too small, we will eventually get many collisions and will grow the
		//    hash table, incrementally adding buffers.
		// 4) If the bucket-table is chosen to be large and we actually need more buffers for the partitions, we
		//    cannot subtract them afterwards from the table
		//
		// ==> We start with a comparatively small hash-table. We aim for a 200% utilization of the bucket table
		//     when all the partition buffers are full. Most likely, that will cause some buckets to be re-hashed
		//     and grab additional buffers away from the partitions.
		// NOTE: This decision may be subject to changes after conclusive experiments!
		// ----------------------------------------------------------------------------------------
		
		final long totalSize = ((long) bufferSize) * numBuffers;
		final long numRecordsStorable = totalSize / (recordLenBytes + RECORD_OVERHEAD_BYTES);
		final long bucketBytes = numRecordsStorable * RECORD_OVERHEAD_BYTES;
		final long numBuckets = bucketBytes / (2 * HASH_BUCKET_SIZE) + 1;
		
		return numBuckets > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) numBuckets;
	}
	
	/**
	 * Assigns a partition to a bucket.
	 * 
	 * @param code The integer to be hashed.
	 * @return The hash code for the integer.
	 */
	public static final byte assignPartition(int bucket, byte maxParts)
	{
		return (byte) (bucket % maxParts);
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
	public static final int hash(int code, int level)
	{
		code = (code + 0x7ed55d16) + (code << 12);
		code = (code ^ 0xc761c23c) ^ (code >>> 19);
		code = (code + 0x165667b1) + (code << 5);
		code = (code + 0xd3a2646c) ^ (code << 9);
		code = (code + 0xfd7046c5) + (code << 3);
		code = (code ^ 0xb55a4f09) ^ (code >>> 16);
		return code >= 0 ? code : - (code + 1);
	}
	
	/**
	 * Computes the logarithm of the given value to the base of 2, rounded down. It corresponds to the
	 * position of the highest non-zero bit. The position is counted, starting with 0 from the least
	 * significant bit to the most significant bit. For example, <code>log2floor(16) = 4</code>, and
	 * <code>log2floor(10) = 3</code>.
	 * 
	 * @param value The value to compute the logarithm for.
	 * @return The logarithm (rounded down) to the base of 2.
	 * @throws ArithmeticException Thrown, if the given value is zero.
	 */
	public static final int log2floor(int value) throws ArithmeticException
	{
		if (value == 0) {
			throw new ArithmeticException("Logarithm of zero is undefined.");
		}
		
		int log = 0;
		while ((value = value >>> 1) != 0) {
			log++;
		}
		
		return log;
	}

	
	// ------------------------------------------------------------------------
	//                      Hash Table Data Structures
	// ------------------------------------------------------------------------
	
	/**
	 * A partition in a hash table. The partition may be in-memory, in which case it has several partition
	 * buffers that contain the records, or it may be spilled. In the latter case, it has only a single
	 * partition buffer in which it collects records to be spilled once the block is full.
	 */
	private static final class Partition
	{
		private final ArrayList<MemorySegment> partitionBuffers;	// this partition's buffers
		
		private final LinkedBlockingQueue<Buffer.Output> bufferReturnQueue;	// queue to return write buffers
		
		private MemorySegment currentPartitionBuffer;
		
		private MemorySegment[] overflowSegments;
		
		private BlockChannelWriter spillingWriter;			// the channel writer, if partition is spilled
		
		private long recordCounter;							// number of records in this partition
		
		private int blockCounter;							// number of blocks in this partition
		
		private int numOverflowSegments;					// the number of actual segments in the overflowSegments array
		
		private int nextOverflowBucket;						// the next free bucket in the current overflow segment
		
		/**
		 * Creates a new partition, initially in memory, with one buffer.
		 * 
		 * @param initialBuffer The initial buffer for this partition.
		 * @param writeBehindBuffers The queue from which to pop buffers for writing, once the partition is spilled.
		 */
		private Partition(MemorySegment initialBuffer, LinkedBlockingQueue<Buffer.Output> bufferReturnQueue)
		{
			this.partitionBuffers = new ArrayList<MemorySegment>(4);
			this.bufferReturnQueue = bufferReturnQueue;
			this.recordCounter = 0;
			this.blockCounter = 0;
			
			this.overflowSegments = new MemorySegment[2];
			this.numOverflowSegments = 0;
			this.nextOverflowBucket = 0;
			
			addBuffer(initialBuffer);
		}
		
		/**
		 * Checks whether this partition is in memory or spilled.
		 * 
		 * @return True, if the partition is in memory, false if it is spilled.
		 */
		public final boolean isInMemory()
		{
			return this.spillingWriter == null;
		}
		
		/**
		 * Inserts the given object into the current buffer. This method returns a pointer that
		 * can be used to address the written record in this partition, if it is in-memory. The returned
		 * pointers have no expressiveness in the case where the partition is spilled.
		 * <p>
		 * If the partition is in-memory and its buffers are full, then <code>-1</code> is returned.
		 * The partition then needs to be assigned another buffer, or it may be spilled.
		 * <p>
		 * If the partition is spilled, then this method never returns <code>-1</code>, because the
		 * partition automatically grabs another write-behind buffer.
		 * 
		 * @param object The object to be written to the partition.
		 * @return A pointer to the object in the partition, or <code>-1</code>, if the partition buffers are full.
		 * @throws IOException Thrown, when this is a spilled partition and the write failed.
		 */
		public final long insertIntoBuffer(IOReadableWritable object) throws IOException
		{
			if (isInMemory())
			{
				final int bufferNum = this.blockCounter - 1;
				final MemorySegment targetBuffer = this.currentPartitionBuffer;
				final long pointer = (((long) bufferNum) << 32) | targetBuffer.outputView.getPosition();

				try {
					object.write(targetBuffer.outputView);
					this.recordCounter++;
					return pointer;
				}
				catch (IOException ioex) {
					// signal buffer full
					return -1;
				}
			}
			else {
				// partition is a spilled partition
				try {
					object.write(this.currentPartitionBuffer.outputView);
				}
				catch (IOException ioex) {
					final MemorySegment toSpill = this.currentPartitionBuffer;
					this.currentPartitionBuffer = null;
					
					// buffer is full, send this buffer off
					spillBuffer(toSpill);
					
					// get a new one and insert the object
					addBuffer(getNextWriteBehindBuffer());
					
					try {
						object.write(this.currentPartitionBuffer.outputView);
					}
					catch (IOException iioex) {
						throw new IOException("Record could not be added to fresh buffer. " +
								"Probably cause: Record length exceeds buffer size limit.");
					}
				}
				
				this.recordCounter++;
				return 0;
			}
		}
		
		public int getNumberOfMemoryBuffersHeld()
		{
			return this.partitionBuffers.size() + this.numOverflowSegments;
		}
		
		/**
		 * Adds a new buffer to this partition. This method should only be externally used on partitions that are
		 * in memory, though this method does not check that this is the case.
		 * 
		 * @param segment The new buffer for this partition.
		 */
		public void addBuffer(MemorySegment segment)
		{
			// write 4 bytes for the block number
			segment.putInt(0, this.blockCounter);
			
			// skip 4 bytes for the remaining header
			segment.outputView.setPosition(8);
			
			// save the old buffer
			if (this.currentPartitionBuffer != null) {
				this.partitionBuffers.add(this.currentPartitionBuffer);
			}
			
			// now add the buffer
			this.currentPartitionBuffer = segment;
			this.blockCounter++;
		}
		
		/**
		 * Spills this partition to disk and sets it up such that it continues spilling records that are added to
		 * it. The spilling process must free at least one buffer, either in the partition's record buffers, or in
		 * the memory segments for overflow buckets.
		 * The partition immediately takes back one buffer to use it for further spilling.
		 * 
		 * @param target The list to which memory segments from overflow buckets are added.
		 * @param ioAccess The I/O manager to be used to create a writer to disk.
		 * @param targetChannel The id of the target channel for this partition.
		 * @return The number of buffers that were freed by spilling this partition.
		 * @throws IOException Thrown, if the writing failed.
		 */
		public int spillPartition(List<MemorySegment> target, IOManager ioAccess,
				Channel.ID targetChannel)
		throws IOException
		{
			if (!isInMemory()) {
				throw new RuntimeException("Bug in Hybrid Hash Join: " +
						"Request to spill a partition that has already been spilled.");
			}
			if (this.blockCounter + this.numOverflowSegments < 2) {
				throw new RuntimeException("Bug in Hybrid Hash Join: " +
					"Request to spill a partition with less than two buffers.");
			}
			
			for (int i = 0; i < this.numOverflowSegments; i++) {
				target.add(this.overflowSegments[i]);
			}
			this.overflowSegments = null;
			this.numOverflowSegments = 0;
			this.nextOverflowBucket = 0;
			
			// create the channel block writer
			this.spillingWriter = ioAccess.createBlockChannelWriter(targetChannel, this.bufferReturnQueue);
			int numBlocks = this.partitionBuffers.size();
			
			// spill all blocks and release them
			for (int i = 0; i < numBlocks; i++) {
				spillBuffer(this.partitionBuffers.get(i));
			}
			this.partitionBuffers.clear();
			
			// we keep the block that is currently being filled, as it is most likely not full, yet
			// return the number of blocks that become available
			return numBlocks;
		}
		
		public void finalizeBuildPhase()
		{
			if (isInMemory()) {
				this.partitionBuffers.add(this.currentPartitionBuffer);
			}
			else {
				throw new RuntimeException("Spilled Partitions not supported!");
			}
		}
		
		/**
		 * Finalizes and spills the given buffer.
		 * 
		 * @param buffer
		 * @throws IOException
		 */
		private final void spillBuffer(MemorySegment buffer)
		throws IOException
		{
			buffer.putInt(4, buffer.outputView.getPosition());
			this.spillingWriter.writeBlock(new Buffer.Output(buffer));
		}
		
		/**
		 * Gets the next write-behind buffer.
		 * 
		 * @return The next write-behind buffer.
		 * @throws IOException Thrown, if the thread was interrupted while waiting for the next buffer.
		 */
		private final MemorySegment getNextWriteBehindBuffer() throws IOException
		{
			try {
				final Buffer.Output buffer = this.bufferReturnQueue.take();
				return buffer.dispose();
			}
			catch (InterruptedException iex) {
				throw new IOException("Hybrid Hash Join Partition was interrupted while taking a buffer.");
			}
		}
		
	} // end partition 
	
	
	public static final class HashBucketIterator<K extends Key, V extends Value>
	{
		private MemorySegment bucket;
		
		private MemorySegment[] overflowSegments;
		
		private ArrayList<MemorySegment> partitionBuffers;
		
		private K searchKey;
		
		private int bucketInSegmentOffset;
		
		private int searchHashCode;
		
		private int posInSegment;
		
		private int countInSegment;
		
		private int numInSegment;
		
		
		
		private void set(MemorySegment bucket, MemorySegment[] overflowSegments, ArrayList<MemorySegment> partitionBuffers,
				K searchKey, int searchHashCode, int bucketInSegmentOffset)
		{
			this.bucket = bucket;
			this.overflowSegments = overflowSegments;
			this.partitionBuffers = partitionBuffers;
			this.searchKey = searchKey;
			this.searchHashCode = searchHashCode;
			this.bucketInSegmentOffset = bucketInSegmentOffset;
			
			this.posInSegment = this.bucketInSegmentOffset + BUCKET_HEADER_LENGTH;
			this.countInSegment = bucket.getShort(bucketInSegmentOffset + HEADER_COUNT_OFFSET);
			this.numInSegment = 0;
		}
		

		public boolean next(KeyValuePair<K, V> target)
		{
			// loop over all segments that are involved in the bucket (original bucket plus overflow buckets)
			while (true)
			{
				while (this.numInSegment < this.countInSegment)
				{
					final int thisCode = this.bucket.getInt(this.posInSegment);
					this.posInSegment += HASH_CODE_LEN;
						
					// check if the hash code matches
					if (thisCode == this.searchHashCode) {
						// get the pointer to the pair
						final long pointer = this.bucket.getLong(bucketInSegmentOffset + BUCKET_POINTER_START_OFFSET + (numInSegment * POINTER_LEN));
						this.numInSegment++;
							
						// deserialize the key to check whether it is really equal, or whether we had only a hash collision
						final DataInputView buffer = this.partitionBuffers.get((int) (pointer >>> 32)).inputView;
						buffer.setPosition((int) (pointer & 0xffffffff));
						final K key = target.getKey();
							
						try {
							key.read(buffer);
							if (key.equals(this.searchKey)) {
								// match!
								target.getValue().read(buffer);
							return true;
							}
						}
						catch (IOException ioex) {
							throw new RuntimeException("Error deserializing key or value from the hashtable. " +
									"Possible reason: Erroneous serialization logic for data type.");
						}
					}
					else {
						this.numInSegment++;
					}
				}
				
				// this segment is done. check if there is another chained bucket
				final long forwardPointer = this.bucket.getLong(this.bucketInSegmentOffset + HEADER_FORWARD_OFFSET);
				if (forwardPointer == BUCKET_FORWARD_POINTER_NOT_SET) {
					return false;
				}
				
				final int overflowSegNum = (int) (forwardPointer >>> 32);
				this.bucket = this.overflowSegments[overflowSegNum];
				this.bucketInSegmentOffset = (int) (forwardPointer & 0xffffffff);
				this.countInSegment = this.bucket.getShort(this.bucketInSegmentOffset + HEADER_COUNT_OFFSET);
			}
		}

	} // end HashBucketIterator
	

	protected static final class ProbeSideIterator<K extends Key, V extends Value> implements Iterator<V>
	{
		private final Iterator<KeyValuePair<K, V>> source;
		
		private K currentKey;
		
		private K nextKey;
		
		private V nextValue;
		
		private boolean nextIsNewKey;
		
		// --------------------------------------------------------------------
		
		protected ProbeSideIterator(Iterator<KeyValuePair<K, V>> source)
		{
			this.source = source;
		}
		
		// --------------------------------------------------------------------

		protected boolean nextKey()
		{
			if (this.nextIsNewKey) {
				this.currentKey = this.nextKey;
				this.nextIsNewKey = false;
				return true;
			}
			
			while (this.source.hasNext()) {
				final KeyValuePair<K, V> nextPair = this.source.next();
				final K key = nextPair.getKey();
				if (!key.equals(this.currentKey)) {
					// reached next key
					this.currentKey = key;
					this.nextKey = key;
					this.nextValue = nextPair.getValue();
					return true;
				}
			}
			
			this.currentKey = null;
			this.nextKey = null;
			this.nextValue = null;
			return false;
		}
		
		protected K getCurrentKey()
		{
			return this.currentKey;
		}
		
		/* (non-Javadoc)
		 * @see java.util.Iterator#hasNext()
		 */
		@Override
		public boolean hasNext()
		{
			if (this.nextIsNewKey) {
				return false;
			}
			else if (this.nextKey != null) {
				return true;
			}
			else if (this.source.hasNext()) {
				final KeyValuePair<K, V> nextPair = this.source.next();
				final K key = nextPair.getKey();
				this.nextKey = key;
				this.nextValue = nextPair.getValue();
				
				if (key.equals(this.currentKey)) {
					return true;
				}
				else {
					// reached next key
					this.nextIsNewKey = true;
					return false;
				}
			}
			else {
				return false;
			}
		}

		/* (non-Javadoc)
		 * @see java.util.Iterator#next()
		 */
		@Override
		public V next()
		{
			if (!this.nextIsNewKey && (this.nextKey != null || hasNext())) {
				final V next = this.nextValue;
				this.nextKey = null;
				this.nextValue = null;
				return next;
			}
			else {
				throw new NoSuchElementException();
			}
		}

		/* (non-Javadoc)
		 * @see java.util.Iterator#remove()
		 */
		@Override
		public void remove()
		{
			throw new UnsupportedOperationException();
		}
		
	} // end ProbeSideIterator
	
}
