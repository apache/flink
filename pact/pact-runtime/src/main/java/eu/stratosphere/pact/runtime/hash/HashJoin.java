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
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.services.iomanager.BlockChannelReader;
import eu.stratosphere.nephele.services.iomanager.BlockChannelWriter;
import eu.stratosphere.nephele.services.iomanager.BulkBlockChannelReader;
import eu.stratosphere.nephele.services.iomanager.Channel;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.DataInputView;
import eu.stratosphere.nephele.services.memorymanager.DataOutputView;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.NullKeyFieldException;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.InstantiationUtil;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.hash.HashJoin.Partition.PartitionIterator;
import eu.stratosphere.pact.runtime.util.MathUtils;


/**
 * An implementation of a Hybrid Hash Join. The join starts operating in memory and gradually starts
 * spilling contents to disk, when the memory is not sufficient. It does not need to know a priori 
 * how large the input will be.
 * <p>
 * The design of this class follows on many parts the design presented in
 * "Hash joins and hash teams in Microsoft SQL Server", by Goetz Graefe et al. In its current state, the
 * implementation lacks features like dynamic role reversal, partition tuning, or histogram guided partitioning. 
 *<p>
 *
 *
 * <hr>
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
 * |
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
public class HashJoin
{
	private static final Log LOG = LogFactory.getLog(HashJoin.class);
	
	// ------------------------------------------------------------------------
	//                         Internal Constants
	// ------------------------------------------------------------------------
	
	/**
	 * The maximum number of recursive partitionings that the join does before giving up.
	 */
	private static final int MAX_RECURSION_DEPTH = 3;
	
	/**
	 * The minimum number of memory segments the hash join needs to be supplied with in order to work.
	 */
	private static final int MIN_NUM_MEMORY_SEGMENTS = 33;
	
	/**
	 * The maximum number of partitions, which defines the spilling granularity. Each recursion, the
	 * data is divided maximally into that many partitions, which are processed in one chuck.
	 */
	private static final int MAX_NUM_PARTITIONS = Byte.MAX_VALUE;
	
	/**
	 * The default record width that is used when no width is given. The record width is
	 * used to determine the ratio of the number of memory segments intended for partition
	 * buffers and the number of memory segments in the hash-table structure. 
	 */
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
	 * The number of bytes for the serialized record length in the partition buffers.
	 */
	private static final int SERIALIZED_LENGTH_FIELD_BYTES = 0;
	
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
	private static final int RECORD_OVERHEAD_BYTES = RECORD_TABLE_BYTES + SERIALIZED_LENGTH_FIELD_BYTES;
	
	// -------------------------- Bucket Size and Structure -------------------------------------
	
	private static final int NUM_INTRA_BUCKET_BITS = 10;
	
	private static final int HASH_BUCKET_SIZE = 0x1 << NUM_INTRA_BUCKET_BITS;
	
	private static final int BUCKET_HEADER_LENGTH = 16;
	
	private static final int NUM_ENTRIES_PER_BUCKET = (HASH_BUCKET_SIZE - BUCKET_HEADER_LENGTH) / RECORD_OVERHEAD_BYTES;
	
	private static final int BUCKET_POINTER_START_OFFSET = BUCKET_HEADER_LENGTH + (NUM_ENTRIES_PER_BUCKET * HASH_CODE_LEN);
	
	// ------------------------------ Bucket Header Fields ------------------------------
	
	/**
	 * Offset of the field in the bucket header indicating the bucket's partition.
	 */
	private static final int HEADER_PARTITION_OFFSET = 0;
	
	/**
	 * Offset of the field in the bucket header indicating the bucket's status (spilled or in-memory).
	 */
	private static final int HEADER_STATUS_OFFSET = 1;
	
	/**
	 * Offset of the field in the bucket header indicating the bucket's element count.
	 */
	private static final int HEADER_COUNT_OFFSET = 2;
	
	/**
	 * Offset of the field in the bucket header that holds the forward pointer to its
	 * first overflow bucket.
	 */
	private static final int HEADER_FORWARD_OFFSET = 4;	
	
	/**
	 * Constant for the forward pointer, indicating that the pointer is not set. 
	 */
	private static final long BUCKET_FORWARD_POINTER_NOT_SET = ~0x0L;
	
//	private static final byte BUCKET_STATUS_SPILLED = 1;
	
	/**
	 * Constant for the bucket status, indicating that the bucket is in memory.
	 */
	private static final byte BUCKET_STATUS_IN_MEMORY = 0;
	
	// ------------------------------ Partition Header Fields ------------------------------
	
	/**
	 * The length of the header in the partition buffer blocks.
	 */
	private static final int PARTITION_BLOCK_HEADER_LEN = 8;
	
	/**
	 * The offset of the field where the length (size) of the partition block is stored
	 * in its header.
	 */
	private static final int PARTITION_BLOCK_SIZE_OFFSET = 4;
	
	
	// ------------------------------------------------------------------------
	//                              Members
	// ------------------------------------------------------------------------
	
	/**
	 * An iterator over the input that will be used to build the hash-table.
	 */
	private final MutableObjectIterator<PactRecord> buildSideInput;
	
	/**
	 * An iterator over the input that will be used to probe the hash-table.
	 */
	private final MutableObjectIterator<PactRecord> probeSideInput;
	
	/**
	 * The class of the field that is hash key.
	 */
	private final Class<? extends Key>[] keyClasses;
	
	/**
	 * The key positions in the records from the build side.
	 */
	private final int[] buildSideKeyFields;

	/**
	 * The key positions in the records from the probe side.
	 */
	private final int[] probeSideKeyFields;
	
	/**
	 * Instances of the key fields to deserialize into.
	 */
	private Key[] keyHolders;
	
	/**
	 * The free memory segments currently available to the hash join.
	 */
	private final List<MemorySegment> availableMemory;
	
	/**
	 * The queue of buffers that can be used for write-behind. Any buffer that is written
	 * asynchronously to disk is returned through this queue. hence, it may sometimes contain more
	 */
	private final LinkedBlockingQueue<MemorySegment> writeBehindBuffers;
	
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
	 * The total number of memory segments available to the hash join.
	 */
	private final int totalNumBuffers;
	
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
	
	/**
	 * The partitions that have been spilled previously and are pending to be processed.
	 */
	private final ArrayList<Partition> partitionsPending;
	
	/**
	 * Iterator over the elements in the hash table.
	 */
	private HashBucketIterator bucketIterator;
	
	/**
	 * Iterator over the elements from the probe side.
	 */
	private ProbeIterator probeIterator;
	
	/**
	 * The reader for the spilled-file of the probe partition that is currently read.
	 */
	private BlockChannelReader currentSpilledProbeSide;
	
	/**
	 * The channel enumerator that is used while processing the current partition to create
	 * channels for the spill partitions it requires.
	 */
	private Channel.Enumerator currentEnumerator;
	
	/**
	 * The array of memory segments that contain the buckets which form the actual hash-table
	 * of hash-codes and pointers to the elements.
	 */
	private MemorySegment[] buckets;
	
	/**
	 * The number of buckets in the current table. The bucket array is not necessarily fully
	 * used, when not all buckets that would fit into the last segment are actually used.
	 */
	private int numBuckets;
	
	/**
	 * The number of buffers in the write behind queue that are actually not write behind buffers,
	 * but regular buffers that only have not yet returned. This is part of an optimization that the
	 * spilling code needs not wait until the partition is completely spilled before proceeding.
	 */
	private int writeBehindBuffersAvailable;
	
	/**
	 * The recursion depth of the partition that is currently processed. The initial table
	 * has a recursion depth of 0. Partitions spilled from a table that is built for a partition
	 * with recursion depth <i>n</i> have a recursion depth of <i>n+1</i>. 
	 */
	private int currentRecursionDepth;
	
	/**
	 * Flag indicating that the closing logic has been invoked.
	 */
	private volatile boolean closed;

	
	// ------------------------------------------------------------------------
	//                         Construction and Teardown
	// ------------------------------------------------------------------------
	
	public HashJoin(MutableObjectIterator<PactRecord> buildSideInput, MutableObjectIterator<PactRecord> probeSideInput,
			int[] buildSideKeyPositions, int[] probeSideKeyPositions,
			Class<? extends Key>[] keyClasses, List<MemorySegment> memorySegments, IOManager ioManager)
	{
		this(buildSideInput, probeSideInput, buildSideKeyPositions, probeSideKeyPositions, keyClasses,
			memorySegments, ioManager, DEFAULT_RECORD_LEN);
	}
	
	
	public HashJoin(MutableObjectIterator<PactRecord> buildSideInput, MutableObjectIterator<PactRecord> probeSideInput,
			int[] buildSideKeyPositions, int[] probeSideKeyPositions,
			Class<? extends Key>[] keyClasses, List<MemorySegment> memorySegments,	IOManager ioManager, int avgRecordLen)
	{
		// some sanity checks first
		if (buildSideInput == null || probeSideInput == null || memorySegments == null) {
			throw new NullPointerException();
		}
		if (memorySegments.size() < MIN_NUM_MEMORY_SEGMENTS) {
			throw new IllegalArgumentException("Too few memory segments provided. Hash Join needs at least " + 
				MIN_NUM_MEMORY_SEGMENTS + " memory segments.");
		}
		if (buildSideKeyPositions.length != probeSideKeyPositions.length || buildSideKeyPositions.length != keyClasses.length) {
			throw new IllegalArgumentException("Insonsistent definition of key fields: " +
					"The index arrays for both inputs and the type arrays must be of equal length."); 
		}
		
		// assign the members
		this.buildSideInput = buildSideInput;
		this.probeSideInput = probeSideInput;
		this.availableMemory = memorySegments;
		this.ioManager = ioManager;
		this.buildSideKeyFields = buildSideKeyPositions;
		this.probeSideKeyFields = probeSideKeyPositions;
		this.keyClasses = keyClasses;
		
		this.keyHolders = new Key[keyClasses.length];
		for (int i = 0; i < keyClasses.length; i++) {
			if (keyClasses[i] == null) {
				throw new NullPointerException("Key type " + i + " is null.");
			}
			this.keyHolders[i] = InstantiationUtil.instantiate(keyClasses[i], Key.class);
		}
		
		this.avgRecordLen = avgRecordLen < 1 ? DEFAULT_RECORD_LEN : avgRecordLen;
		
		// check the size of the first buffer and record it. all further buffers must have the same size.
		// the size must also be a power of 2
		this.totalNumBuffers = memorySegments.size();
		this.segmentSize = memorySegments.get(0).size();
		if ( (this.segmentSize & this.segmentSize - 1) != 0) {
			throw new IllegalArgumentException("Hash Table requires buffers whose size is a power of 2.");
		}
		int bucketsPerSegment = this.segmentSize >> NUM_INTRA_BUCKET_BITS;
		if (bucketsPerSegment == 0) {
			throw new IllegalArgumentException("Hash Table requires buffers of at least " + HASH_BUCKET_SIZE + " bytes.");
		}
		this.bucketsPerSegmentMask = bucketsPerSegment - 1;
		this.bucketsPerSegmentBits = MathUtils.log2floor(bucketsPerSegment);
		
		// take away the write behind buffers
		this.writeBehindBuffers = new LinkedBlockingQueue<MemorySegment>();
		this.numWriteBehindBuffers = getNumWriteBehindBuffers(memorySegments.size());
		for (int i = this.numWriteBehindBuffers; i > 0; --i)
		{
			this.writeBehindBuffers.add(memorySegments.remove(memorySegments.size() - 1));
		}
		
		this.partitionsBeingBuilt = new ArrayList<HashJoin.Partition>();
		this.partitionsPending = new ArrayList<HashJoin.Partition>();
	}
	
	
	// ------------------------------------------------------------------------
	//                              Life-Cycle
	// ------------------------------------------------------------------------
	
	/**
	 * Opens the hash join. This method reads the build-side input and constructs the initial
	 * hash table, gradually spilling partitions that do not fit into memory. 
	 * 
	 * @throws IOException Thrown, if an I/O problem occurs while spilling a partition.
	 */
	public void open() throws IOException
	{
		// open builds the initial table by consuming the build-side input
		this.currentRecursionDepth = 0;
		buildInitialTable(this.buildSideInput);
		
		// the first prober is the probe-side input
		this.probeIterator = new ProbeIterator(this.probeSideInput);
		
		// the bucket iterator can remain constant over the time
		// it needs extra fields to deserialize the keys into
		Key[] keyHolders = new Key[this.keyClasses.length];
		for (int i = 0; i < keyHolders.length; i++) {
			keyHolders[i] = InstantiationUtil.instantiate(this.keyClasses[i], Key.class);
		}
		this.bucketIterator = new HashBucketIterator(this.buildSideKeyFields, keyHolders);
	}
	
	/**
	 * @return
	 * @throws IOException
	 */
	public boolean nextRecord() throws IOException
	{
		final ProbeIterator probeIter = this.probeIterator;
		final Key[] keyHolders = this.keyHolders;
		
		PactRecord next;
		while ((next = probeIter.next()) != null)
		{
			if (!next.getFieldsInto(this.probeSideKeyFields, keyHolders)) {
				throw new NullKeyFieldException(); 
			}
			
			final int hash = hash(keyHolders, this.currentRecursionDepth);
			final int posHashCode = hash % this.numBuckets;
			
			// get the bucket for the given hash code
			final int bucketArrayPos = posHashCode >> this.bucketsPerSegmentBits;
			final int bucketInSegmentOffset = (posHashCode & this.bucketsPerSegmentMask) << NUM_INTRA_BUCKET_BITS;
			final MemorySegment bucket = this.buckets[bucketArrayPos];
			
			// get the basic characteristics of the bucket
			final int partitionNumber = bucket.get(bucketInSegmentOffset + HEADER_PARTITION_OFFSET);
			final Partition p = this.partitionsBeingBuilt.get(partitionNumber);
			
			// for an in-memory partition, process set the return iterators, else spill the probe records
			if (p.isInMemory()) {
				this.bucketIterator.set(bucket, p.overflowSegments, p.inMemoryBuffers, keyHolders, hash, bucketInSegmentOffset);
				return true;
			}
			else {
				p.insertIntoProbeBuffer(next);
			}
		}
		
		// -------------- partition done ---------------
		
		// finalize and cleanup the partitions of the current table
		int buffersAvailable = 0;
		for (int i = 0; i < this.partitionsBeingBuilt.size(); i++) {
			Partition p = this.partitionsBeingBuilt.get(i);
			buffersAvailable += p.finalizeProbePhase(this.availableMemory, this.partitionsPending);
		}
		this.partitionsBeingBuilt.clear();
		this.writeBehindBuffersAvailable += buffersAvailable;
		
		// release the table memory
		releaseTable();
		
		// check if there are pending partitions
		if (!this.partitionsPending.isEmpty())
		{
			final Partition p = this.partitionsPending.get(0);
			
			// build the next table
			buildTableFromSpilledPartition(p);
			
			// set the probe side - gather memory segments for reading
			LinkedBlockingQueue<MemorySegment> returnQueue = new LinkedBlockingQueue<MemorySegment>();
			this.currentSpilledProbeSide = this.ioManager.createBlockChannelReader(p.probeSideChannel.getChannelID(), returnQueue);
			
			List<MemorySegment> memory = new ArrayList<MemorySegment>();
			memory.add(getNextBuffer());
			memory.add(getNextBuffer());
			
			BlockReaderIterator probeReader = new BlockReaderIterator(this.currentSpilledProbeSide, returnQueue, memory, this.availableMemory, p.probeBlockCounter);
			this.probeIterator.set(probeReader);
			
			// unregister the pending partition
			this.partitionsPending.remove(0);
			this.currentRecursionDepth = p.recursionLevel + 1;
			
			// recursively get the next
			return nextRecord();
		}
		else {
			// no more data
			return false;
		}
	}
	
	/**
	 * @return
	 */
	public PactRecord getCurrentProbeRecord()
	{
		return this.probeIterator.getCurrent();
	}
	
	/**
	 * @return
	 */
	public HashBucketIterator getBuildSideIterator()
	{
		return this.bucketIterator;
	}
	
	/**
	 * Closes the hash table. This effectively releases all internal structures and closes all
	 * open files and removes them. The call to this method is valid both as a cleanup after the
	 * complete inputs were properly processed, and as an cancellation call, which cleans up
	 * all resources that are currently held by the hash join.
	 */
	public List<MemorySegment> close()
	{
		// make sure that we close only once
		synchronized (this.closeLock) {
			if (this.closed) {
				return new ArrayList<MemorySegment>();
			}
			this.closed = true;
		}
		
		// clear the iterators, so the next call to next() will notice
		this.bucketIterator = null;
		this.probeIterator = null;
		
		// release the table structure
		releaseTable();
		
		// clear the memory in the partitions
		clearPartitions();
		
		// clear the current probe side channel, if there is one
		if (this.currentSpilledProbeSide != null) {
			try {
				this.currentSpilledProbeSide.closeAndDelete();
			}
			catch (Throwable t) {
				LOG.warn("Could not close and delete the temp file for the current spilled partition probe side.", t);
			}
		}
		
		// clear the partitions that are still to be done (that have files on disk)
		for (int i = 0; i < this.partitionsPending.size(); i++) {
			final Partition p = this.partitionsPending.get(i);
			p.buildSideChannel.deleteChannel();
			p.probeSideChannel.deleteChannel();
		}
		
		// return the write-behind buffers
		for (int i = 0; i < this.numWriteBehindBuffers + this.writeBehindBuffersAvailable; i++) {
			try {
				this.availableMemory.add(this.writeBehindBuffers.take());
			}
			catch (InterruptedException iex) {
				throw new RuntimeException("Hashtable closing was interrupted");
			}
		}
		
		return this.availableMemory;
	}
	

	
	// ------------------------------------------------------------------------
	//                       Hash Table Building
	// ------------------------------------------------------------------------
	
	
	/**
	 * @param input
	 * @throws IOException
	 */
	protected void buildInitialTable(final MutableObjectIterator<PactRecord> input)
	throws IOException
	{
		// create the partitions
		final int partitionFanOut = getPartitioningFanOutNoEstimates(this.availableMemory.size());
		if (partitionFanOut > MAX_NUM_PARTITIONS) {
			throw new RuntimeException("Hash join created ");
		}
		createPartitions(partitionFanOut, 0);
		
		// set up the table structure. the write behind buffers are taken away, as are one buffer per partition
		final int numBuckets = getInitialTableSize(this.availableMemory.size(), this.segmentSize, 
			partitionFanOut, this.avgRecordLen);
		initTable(numBuckets, (byte) partitionFanOut);
		
		final int[] positions = this.buildSideKeyFields;
		final Key[] keys = this.keyHolders;
		
		// go over the complete input and insert every element into the hash table
		PactRecord record = new PactRecord();
		while (input.next(record))
		{
			int hashCode = 0;
			for (int i = 0; i < positions.length; i++) {
				Key k = keys[i];
				if ((k = record.getField(positions[i], k)) != null) {
					hashCode ^= hash(k.hashCode(), 0);
				}
				else {
					throw new NullKeyFieldException();
				}
			}
			insertIntoTable(record, hashCode);
		}

		// finalize the partitions
		for (int i = 0; i < this.partitionsBeingBuilt.size(); i++) {
			Partition p = this.partitionsBeingBuilt.get(i);
			p.finalizeBuildPhase(this.ioManager, this.currentEnumerator);
		}
	}
	
	/**
	 * @param p
	 * @throws IOException
	 */
	protected void buildTableFromSpilledPartition(final Partition p)
	throws IOException
	{
		final int nextRecursionLevel = p.recursionLevel + 1;
		if (nextRecursionLevel > MAX_RECURSION_DEPTH) {
			throw new RuntimeException("Hash join exceeded maximum number of recursions, without reducing "
				+ "partitions enough to be memory resident. Probably cause: Too many duplicate keys.");
		}
		
		final Key[] keyHolders = this.keyHolders;
		
		// we distinguish two cases here:
		// 1) The partition fits entirely into main memory. That is the case if we have enough buffers for
		//    all partition segments, plus enough buffers to hold the table structure.
		//    --> We read the partition in as it is and create a hashtable that references only
		//        that single partition.
		// 2) We can not guarantee that enough memory segments are available and read the partition
		//    in, distributing its data among newly created partitions.
		
		final int totalBuffersAvailable = this.availableMemory.size() + this.writeBehindBuffersAvailable;
		if (totalBuffersAvailable != this.totalNumBuffers - this.numWriteBehindBuffers) {
			throw new RuntimeException("Hash Join bug in memory management: Memory buffers leaked.");
		}
		
		long numBuckets = (p.buildSideRecordCounter * RECORD_TABLE_BYTES) / (HASH_BUCKET_SIZE - BUCKET_HEADER_LENGTH) + 1;
		
		// we need to consider the worst case where everything hashes to one bucket which needs to overflow by the same
		// number of total buckets again.
		final long totalBuffersNeeded = (numBuckets * 2) / (this.bucketsPerSegmentMask + 1) + p.buildSideBlockCounter + 1;
		
		if (totalBuffersNeeded < totalBuffersAvailable)
		{
			// we are guaranteed to stay in memory
			ensureNumBuffersReturned(p.buildSideBlockCounter);
			
			// first read the partition in
			final BulkBlockChannelReader reader = this.ioManager.createBulkBlockChannelReader(p.buildSideChannel.getChannelID(), 
				this.availableMemory, p.buildSideBlockCounter);
			reader.closeAndDelete(); // call waits until all is read
			final List<MemorySegment> partitionBuffers = reader.getFullSegments();
			final Partition newPart = new Partition(0, nextRecursionLevel, partitionBuffers, p.buildSideRecordCounter);
			
			this.partitionsBeingBuilt.add(newPart);
			
			// erect the buckets
			initTable((int) numBuckets, (byte) 1);
			
			// now, index the partition through a hash table
			PartitionIterator pIter = newPart.getPartitionIterator(this.buildSideKeyFields, keyHolders);
			
			while (pIter.next()) {
				final int hashCode = hash(keyHolders, nextRecursionLevel);
				final int posHashCode = hashCode % this.numBuckets;
				final long pointer = pIter.getPointer();
				
				// get the bucket for the given hash code
				final int bucketArrayPos = posHashCode >> this.bucketsPerSegmentBits;
				final int bucketInSegmentPos = (posHashCode & this.bucketsPerSegmentMask) << NUM_INTRA_BUCKET_BITS;
				final MemorySegment bucket = this.buckets[bucketArrayPos];
				
				insertBucketEntry(newPart, bucket, bucketInSegmentPos, hashCode, pointer);
			}
		}
		else {
			// we need to partition and partially spill
			final int avgRecordLenPartition = (int) (((long) p.buildSideBlockCounter) * 
					(this.segmentSize - PARTITION_BLOCK_HEADER_LEN) / p.buildSideRecordCounter);
			
			final int bucketCount = (int) (((long) totalBuffersAvailable) * RECORD_TABLE_BYTES / 
					(avgRecordLenPartition + RECORD_OVERHEAD_BYTES));
			
			// compute in how many splits, we'd need to partition the result 
			final int splits = (int) (totalBuffersNeeded / totalBuffersAvailable) + 1;
			final int partitionFanOut = Math.min(10 * splits /* being conservative */, MAX_NUM_PARTITIONS);
			
			createPartitions(partitionFanOut, nextRecursionLevel);
			
			// set up the table structure. the write behind buffers are taken away, as are one buffer per partition
			initTable(bucketCount, (byte) partitionFanOut);
			
			// go over the complete input and insert every element into the hash table
			// first set up the reader with some memory.
			final List<MemorySegment> segments = new ArrayList<MemorySegment>(2);
			segments.add(getNextBuffer());
			segments.add(getNextBuffer());
			
			final BlockReaderIterator reader = new BlockReaderIterator(this.ioManager,
					p.buildSideChannel.getChannelID(), segments, this.availableMemory, p.buildSideBlockCounter);
			
			PactRecord rec = new PactRecord();
			while (reader.next(rec))
			{	
				final int hashCode = hashBuildeSideRecord(rec, nextRecursionLevel);
				insertIntoTable(rec, hashCode);
			}

			// finalize the partitions
			for (int i = 0; i < this.partitionsBeingBuilt.size(); i++) {
				Partition part = this.partitionsBeingBuilt.get(i);
				part.finalizeBuildPhase(this.ioManager, this.currentEnumerator);
			}
		}
	}
	
	/**
	 * @param pair
	 * @param hashCode
	 * @throws IOException
	 */
	protected final void insertIntoTable(final PactRecord record, final int hashCode)
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
		
		long pointer = p.insertIntoBuildBuffer(record);
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
			p.addBuildSideBuffer(nextSeg);
			
			// retry to write into the buffer
			pointer = p.insertIntoBuildBuffer(record);
			if (pointer == -1) {
				// retry failed, throw an exception
				throw new IOException("Record could not be added to fresh buffer. Probably cause: Record length exceeds buffer size limit.");
			}
		}
		
		// --------- Step 2: Add the pointer and the hash code to the hash bucket ---------
		
		if (p.isInMemory()) {
			// in-memory partition: add the pointer and the hash-value to the list
			insertBucketEntry(p, bucket, bucketInSegmentPos, hashCode, pointer);
		}
		else {
			// partition not in memory, so add nothing to the table at the moment
			return;
		}
	}
	
	/**
	 * @param p
	 * @param bucket
	 * @param bucketInSegmentPos
	 * @param hashCode
	 * @param pointer
	 * @throws IOException
	 */
	private final void insertBucketEntry(final Partition p, final MemorySegment bucket, final int bucketInSegmentPos, final int hashCode,
			final long pointer)
	throws IOException
	{
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
					if (spilledPart == p.partitionNumber) {
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
	
	// --------------------------------------------------------------------------------------------
	//                          Setup and Tear Down of Structures
	// --------------------------------------------------------------------------------------------
	
	/**
	 * @param numPartitions
	 */
	protected void createPartitions(int numPartitions, int recursionLevel)
	{
		// sanity check
		ensureNumBuffersReturned(numPartitions);
		
		this.currentEnumerator = this.ioManager.createChannelEnumerator();
		
		this.partitionsBeingBuilt.clear();
		for (int i = 0; i < numPartitions; i++) {
			Partition p = new Partition(i,recursionLevel,
				this.availableMemory.remove(this.availableMemory.size() - 1),
				this.writeBehindBuffers);
			this.partitionsBeingBuilt.add(p);
		}
	}
	
	/**
	 * This method clears all partitions currently residing (partially) in memory. It releases all memory
	 * and deletes all spilled partitions.
	 * <p>
	 * This method is intended for a hard cleanup in the case that the join is aborted.
	 */
	protected void clearPartitions()
	{
		for (int i = this.partitionsBeingBuilt.size() - 1; i >= 0; --i)
		{
			Partition p = this.partitionsBeingBuilt.get(i);
			if (p.currentPartitionBuffer != null) {
				this.availableMemory.add(p.currentPartitionBuffer);
				p.currentPartitionBuffer = null;
			}
			
			// return the overflow segments
			if (p.overflowSegments != null) {
				for (int k = 0; k < p.numOverflowSegments; k++) {
					this.availableMemory.add(p.overflowSegments[k]);
				}
			}
			
			// return the partition buffers
			for (int k = p.inMemoryBuffers.size() - 1; k >= 0; --k) {
				this.availableMemory.add(p.inMemoryBuffers.get(k));
			}
			
			// clear the channels
			try {
				if (p.buildSideChannel != null) {
					p.buildSideChannel.close();
					p.buildSideChannel.deleteChannel();
				}
				if (p.probeSideChannel != null) {
					p.probeSideChannel.close();
					p.probeSideChannel.deleteChannel();
				}
			}
			catch (IOException ioex) {
				throw new RuntimeException("Error deleting the partition files. Some temporary files might not be removed.");
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
		final int numSegs = (numBuckets >>> this.bucketsPerSegmentBits) + ( (numBuckets & this.bucketsPerSegmentMask) == 0 ? 0 : 1);
		final MemorySegment[] table = new MemorySegment[numSegs];
		
		ensureNumBuffersReturned(numSegs);
		
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
		
		if (this.buckets != null) {
			for (int i = 0; i < this.buckets.length; i++) {
				this.availableMemory.add(this.buckets[i]);
			}
			this.buckets = null;
		}
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
			if (p.isInMemory() && p.buildSideBlockCounter > largestNumBlocks) {
				largestNumBlocks = p.buildSideBlockCounter;
				largestPartNum = i;
			}
		}
		final Partition p = partitions.get(largestPartNum);
		
		// spill the partition
		int numBuffersFreed = p.spillPartition(this.availableMemory, this.ioManager, this.currentEnumerator.next());
		this.writeBehindBuffersAvailable += numBuffersFreed;
		
		// grab as many buffers as are available directly
		MemorySegment currBuff = null;
		while (this.writeBehindBuffersAvailable > 0 && (currBuff = this.writeBehindBuffers.poll()) != null) {
			this.availableMemory.add(currBuff);
			this.writeBehindBuffersAvailable--;
		}
		
		return largestPartNum;
	}
	
	private final int hashBuildeSideRecord(PactRecord record, int recursionLevel)
	{
		int hashCode = 0;
		for (int i = 0; i < this.buildSideKeyFields.length; i++) {
			Key k = this.keyHolders[i];
			if ((k = record.getField(this.buildSideKeyFields[i], k)) != null) {
				hashCode ^= hash(k.hashCode(), recursionLevel);
			}
			else {
				throw new NullKeyFieldException();
			}
		}
		return hashCode;
	}
	
	private final int hash(Key[] fields, int recursionLevel)
	{
		int code = 0;
		for (int i = 0; i < fields.length; i++) {
			code ^= hash(fields[i].hashCode(), recursionLevel);
		}
		return code;
	}
	
	/**
	 * This method makes sure that at least a certain number of memory segments is in the list of free segments.
	 * Free memory can be in the list of free segments, or in the return-queue where segments used to write behind are
	 * put. The number of segments that are in that return-queue, but are actually reclaimable is tracked. This method
	 * makes sure at least a certain number of buffers is reclaimed.
	 *  
	 * @param minRequiredAvailable The minimum number of buffers that needs to be reclaimed.
	 */
	private final void ensureNumBuffersReturned(final int minRequiredAvailable)
	{
		if (minRequiredAvailable > this.availableMemory.size() + this.writeBehindBuffersAvailable) {
			throw new IllegalArgumentException("More buffers requested available than totally available.");
		}
		
		try {
			while (this.availableMemory.size() < minRequiredAvailable) {
				this.availableMemory.add(this.writeBehindBuffers.take());
				this.writeBehindBuffersAvailable--;
			}
		}
		catch (InterruptedException iex) {
			throw new RuntimeException("Hash Join was interrupted.");
		}
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
	private final MemorySegment getNextBuffer()
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
				toReturn = this.writeBehindBuffers.take();
			}
			catch (InterruptedException iex) {
				throw new RuntimeException("Hybrid Hash Join was interrupted while taking a buffer.");
			}
			this.writeBehindBuffersAvailable--;
			
			// grab as many more buffers as are available directly
			MemorySegment currBuff = null;
			while (this.writeBehindBuffersAvailable > 0 && (currBuff = this.writeBehindBuffers.poll()) != null) {
				this.availableMemory.add(currBuff);
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
		final long bucketBytes = numRecordsStorable * RECORD_TABLE_BYTES;
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
		final int rotation = level * 11;
		
		code = (code << rotation) | (code >>> -rotation);

		code = (code + 0x7ed55d16) + (code << 12);
		code = (code ^ 0xc761c23c) ^ (code >>> 19);
		code = (code + 0x165667b1) + (code << 5);
		code = (code + 0xd3a2646c) ^ (code << 9);
		code = (code + 0xfd7046c5) + (code << 3);
		code = (code ^ 0xb55a4f09) ^ (code >>> 16);
		return code >= 0 ? code : -(code + 1);
	}

	
	// ------------------------------------------------------------------------
	//                      Hash Table Data Structures
	// ------------------------------------------------------------------------
	
	/**
	 * A partition in a hash table. The partition contains the actual records, which are referenced by pointers from
	 * the hash buckets. The partition may be in-memory, in which case it has several partition
	 * buffers that contain the records, or it may be spilled. In the latter case, it has only a single
	 * partition buffer in which it collects records to be spilled once the block is full, and a channel writer to
	 * spill the records.
	 * <p>
	 * The partition contains structures relevant to both the build-side and the probe-side. For spilled partitions,
	 * it contains references to the temp files for both sides, as well as counters for the number of records and
	 * the number of blocks.
	 * <p>
	 * In addition to the structures that contain the actual records, it anchors the lists of overflow buckets for
	 * partition, because the overflow buckets are typically released together when a partition is spilled.  
	 */
	protected static final class Partition
	{
		// --------------------------------- Table Structure Auxiliaries ------------------------------------
		
		private MemorySegment[] overflowSegments;			// segments in which overflow buckets from the table structure are stored
		
		private int numOverflowSegments;					// the number of actual segments in the overflowSegments array
		
		private int nextOverflowBucket;						// the next free bucket in the current overflow segment
	
		// -------------------------------------- Record Buffers --------------------------------------------
		
		private MemorySegment currentPartitionBuffer;		// the partition buffer into which elements are put (build and probe side)
		
		private final List<MemorySegment> inMemoryBuffers;	// this partition's buffers
		
		// ------------------------------------------ Spilling ----------------------------------------------
		
		private BlockChannelWriter buildSideChannel;		// the channel writer for the build side, if partition is spilled
		
		private BlockChannelWriter probeSideChannel;		// the channel writer from the probe side, if partition is spilled
		
		private long buildSideRecordCounter;				// number of build-side records in this partition
		
		private long probeSideRecordCounter;				// number of probe-side records in this partition 
		
		private int buildSideBlockCounter;					// number of build-side blocks in this partition
		
		private int probeBlockCounter;						// number of probe-side blocks in this partition
		
		private final LinkedBlockingQueue<MemorySegment> bufferReturnQueue;	// queue to return write buffers

		// ----------------------------------------- General ------------------------------------------------
		
		private final int partitionNumber;					// the number of the partition
		
		private final int recursionLevel;					// the recursion level on which this partition lives
		
		
		// --------------------------------------------------------------------------------------------------
		
		/**
		 * Creates a new partition, initially in memory, with one buffer for the build side. The partition is
		 * initialized to expect record insertions for the build side.
		 * 
		 * @param partitionNumber The number of the partition.
		 * @param recursionLevel The recursion level - zero for partitions from the initial build, <i>n + 1</i> for
		 *                       partitions that are created from spilled partition with recursion level <i>n</i>. 
		 * @param initialBuffer The initial buffer for this partition.
		 * @param writeBehindBuffers The queue from which to pop buffers for writing, once the partition is spilled.
		 */
		private Partition(int partitionNumber, int recursionLevel,
				MemorySegment initialBuffer, LinkedBlockingQueue<MemorySegment> bufferReturnQueue)
		{
			this.partitionNumber = partitionNumber;
			this.recursionLevel = recursionLevel;
			
			this.inMemoryBuffers = new ArrayList<MemorySegment>(4);
			this.bufferReturnQueue = bufferReturnQueue;
			this.buildSideRecordCounter = 0;
			this.buildSideBlockCounter = 1;
			
			this.overflowSegments = new MemorySegment[2];
			this.numOverflowSegments = 0;
			this.nextOverflowBucket = 0;
			
			initBuffer(initialBuffer, 0);
			this.currentPartitionBuffer = initialBuffer;
		}
		
		private Partition(int partitionNumber, int recursionLevel, List<MemorySegment> buffers,
				long buildSideRecordCounter)
		{
			this.partitionNumber = partitionNumber;
			this.recursionLevel = recursionLevel;
			
			this.inMemoryBuffers = buffers;
			this.bufferReturnQueue = null;
			this.buildSideBlockCounter = buffers.size();
			this.buildSideRecordCounter = buildSideRecordCounter;
			
			this.overflowSegments = new MemorySegment[2];
			this.numOverflowSegments = 0;
			this.nextOverflowBucket = 0;
		}

		// --------------------------------------------------------------------------------------------------
		
		/**
		 * Gets the partition number of this partition.
		 * 
		 * @return This partition's number.
		 */
		public int getPartitionNumber()
		{
			return this.partitionNumber;
		}
		
		/**
		 * Gets this partition's recursion level.
		 * 
		 * @return The partition's recursion level.
		 */
		public int getRecursionLevel()
		{
			return this.recursionLevel;
		}
		
		/**
		 * Checks whether this partition is in memory or spilled.
		 * 
		 * @return True, if the partition is in memory, false if it is spilled.
		 */
		public final boolean isInMemory()
		{
			return this.buildSideChannel == null;
		}
		
		// --------------------------------------------------------------------------------------------------
		
		/**
		 * Inserts the given object into the current buffer. This method returns a pointer that
		 * can be used to address the written record in this partition, if it is in-memory. The returned
		 * pointers have no expressiveness in the case where the partition is spilled.
		 * <p>
		 * If the partition is in-memory and its buffers are full, then <code>-1</code> is returned.
		 * The partition then needs to be assigned another buffer, or it may be spilled.
		 * <p>
		 * If the partition is spilled, then this method never returns <code>-1</code>, because the
		 * partition automatically grabs another write-behind buffer once the current buffer is full.
		 * 
		 * @param object The object to be written to the partition.
		 * @return A pointer to the object in the partition, or <code>-1</code>, if the partition buffers are full.
		 * @throws IOException Thrown, when this is a spilled partition and the write failed.
		 */
		public final long insertIntoBuildBuffer(IOReadableWritable object) throws IOException
		{
			final int offset = insertIntoCurrentBuffer(object);
			
			if (isInMemory())
			{
				if (offset >= 0) {
					this.buildSideRecordCounter++;
					return (((long) (this.buildSideBlockCounter - 1)) << 32) | offset;
				}
				else {
					// signal buffer full
					return -1;
				}
			}
			else {
				// partition is a spilled partition
				if (offset == -1) {
					// buffer is full, send this buffer off
					spillBuffer(this.currentPartitionBuffer, this.buildSideChannel);
					
					// get a new one and insert the object
					this.currentPartitionBuffer = getNextWriteBehindBuffer();
					initBuffer(this.currentPartitionBuffer, this.buildSideBlockCounter);
					this.buildSideBlockCounter++;
					
					if (insertIntoBuildBuffer(object) == -1) {
						throw new IOException("Record could not be added to fresh buffer. " +
								"Probably cause: Record length exceeds buffer size limit.");
					}
				}
				
				this.buildSideRecordCounter++;
				return 0;
			}
		}
		
		
		/**
		 * Inserts the given record into the probe side buffers. This method is only applicable when the
		 * partition was spilled while processing the build side. In that case, it inserts the record to into
		 * the current buffer and add that one to the spilled channel once it is full.
		 * <p>
		 * If this method is invoked when the partition is still being built, it has undefined behavior.
		 *   
		 * @param object The record to be inserted into the probe side buffers.
		 * @throws IOException Thrown, if the buffer is full, needs to be spilled, and spilling causes an error.
		 */
		public final void insertIntoProbeBuffer(IOReadableWritable object) throws IOException
		{
			final int offset = insertIntoCurrentBuffer(object);
			if (offset == -1) {
				// buffer is full, send this buffer off
				spillBuffer(this.currentPartitionBuffer, this.probeSideChannel);
				
				// get a new one and insert the object
				this.currentPartitionBuffer = getNextWriteBehindBuffer();
				initBuffer(this.currentPartitionBuffer, this.probeBlockCounter);
				this.probeBlockCounter++;
				
				if (insertIntoBuildBuffer(object) == -1) {
					throw new IOException("Record could not be added to fresh buffer. " +
							"Probably cause: Record length exceeds buffer size limit.");
				}
			}
			
			this.probeSideRecordCounter++;
		}
		
		
		private final int insertIntoCurrentBuffer(IOReadableWritable record)
		{
			final DataOutputView outView = this.currentPartitionBuffer.outputView;
			final int startPos = this.currentPartitionBuffer.outputView.getPosition();
			
			// partition is a spilled partition
			try {
//				this.currentPartitionBuffer.outputView.skip(2);
				record.write(outView);
//				final int len = outView.getPosition() - startPos - 2;
//				if (len > 0x7fff) {
//					throw new RuntimeException("Record too long for serialization logic.");
//				}
//				this.currentPartitionBuffer.putShort(startPos, (short) len);
				return startPos;
			}
			catch (IOException ioex) {
				outView.setPosition(startPos);
				return -1;
			}
		}
		
		/**
		 * Adds a new buffer to this partition. This method should only be externally used on partitions that are
		 * in memory, though this method does not check that this is the case.
		 * 
		 * @param segment The new buffer for this partition.
		 */
		public void addBuildSideBuffer(MemorySegment segment)
		{
			initBuffer(segment, this.buildSideBlockCounter);
			
			// save the old buffer
			this.inMemoryBuffers.add(this.currentPartitionBuffer);
			
			// now add the buffer
			this.currentPartitionBuffer = segment;
			this.buildSideBlockCounter++;
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
			// sanity checks
			if (!isInMemory()) {
				throw new RuntimeException("Bug in Hybrid Hash Join: " +
						"Request to spill a partition that has already been spilled.");
			}
			if (this.buildSideBlockCounter + this.numOverflowSegments < 2) {
				throw new RuntimeException("Bug in Hybrid Hash Join: " +
					"Request to spill a partition with less than two buffers.");
			}
			
			// return the memory from the overflow segments
			for (int i = 0; i < this.numOverflowSegments; i++) {
				target.add(this.overflowSegments[i]);
			}
			this.overflowSegments = null;
			this.numOverflowSegments = 0;
			this.nextOverflowBucket = 0;
			
			// create the channel block writer
			this.buildSideChannel = ioAccess.createBlockChannelWriter(targetChannel, this.bufferReturnQueue);
			int numBlocks = this.inMemoryBuffers.size();
			
			// spill all blocks and release them
			for (int i = 0; i < numBlocks; i++) {
				spillBuffer(this.inMemoryBuffers.get(i), this.buildSideChannel);
			}
			this.inMemoryBuffers.clear();
			
			// we keep the block that is currently being filled, as it is most likely not full, yet
			// return the number of blocks that become available
			return numBlocks;
		}
		
		
		/**
		 * @param spilledPartitions
		 * @param ioAccess
		 * @param probeChannelEnumerator
		 * @throws IOException
		 */
		public void finalizeBuildPhase(IOManager ioAccess, Channel.Enumerator probeChannelEnumerator)
		throws IOException
		{
			if (isInMemory()) {
				this.inMemoryBuffers.add(this.currentPartitionBuffer);
				this.currentPartitionBuffer = null;
			}
			else {
				// spilled partition: write the last buffer and close the channel
				spillBuffer(this.currentPartitionBuffer, this.buildSideChannel);
				this.buildSideChannel.close();
				
				// create the channel for the probe side and claim one buffer for it
				this.probeSideChannel = ioAccess.createBlockChannelWriter(probeChannelEnumerator.next(), this.bufferReturnQueue);
				this.currentPartitionBuffer = getNextWriteBehindBuffer();
				initBuffer(this.currentPartitionBuffer, 0);
				this.probeBlockCounter = 1;
			}
		}
		
		/**
		 * @param freeMemory
		 * @param spilledPartitions
		 * @throws IOException
		 */
		public int finalizeProbePhase(List<MemorySegment> freeMemory, List<Partition> spilledPartitions)
		throws IOException
		{
			if (isInMemory()) {
				// in this case, return all memory buffers
				
				// return the overflow segments
				for (int k = 0; k < this.numOverflowSegments; k++) {
					freeMemory.add(this.overflowSegments[k]);
				}
				this.overflowSegments = null;
				this.numOverflowSegments = 0;
				this.nextOverflowBucket = 0;
				
				// return the partition buffers
				for (int k = this.inMemoryBuffers.size() - 1; k >= 0; --k) {
					freeMemory.add(this.inMemoryBuffers.get(k));
				}
				this.inMemoryBuffers.clear();
				return 0;
			}
			else {
				// flush the last probe side buffer and register this partition as pending
				spillBuffer(this.currentPartitionBuffer, this.probeSideChannel);
				this.currentPartitionBuffer = null;
				this.probeSideChannel.close();
				
				spilledPartitions.add(this);
				return 1;
			}
		}
		
		/**
		 * Initializes a memory segment to be used as a record buffer for the hash table. This method
		 * records the block sequence number and reserves header space.
		 * 
		 * @param segment The segment to be initialized.
		 * @param blockNumber The sequence number of the block in the collection of blocks.
		 */
		private final void initBuffer(MemorySegment segment, int blockNumber)
		{
			// write 4 bytes for the block number
			segment.putInt(0, blockNumber);
			
			// skip 4 bytes for the remaining header
			segment.outputView.setPosition(PARTITION_BLOCK_HEADER_LEN);
		}
		
		/**
		 * Finalizes and spills the given buffer.
		 * 
		 * @param buffer
		 * @throws IOException
		 */
		private final void spillBuffer(MemorySegment buffer, BlockChannelWriter writer)
		throws IOException
		{
			buffer.putInt(PARTITION_BLOCK_SIZE_OFFSET, buffer.outputView.getPosition());
			writer.writeBlock(buffer);
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
				return this.bufferReturnQueue.take();
			}
			catch (InterruptedException iex) {
				throw new IOException("Hybrid Hash Join Partition was interrupted while taking a buffer.");
			}
		}
		
		public PartitionIterator getPartitionIterator(int[] keyPositions, Key[] keyHolders)
		{
			return new PartitionIterator(keyPositions, keyHolders);
		}
		
		
		protected final class PartitionIterator
		{
			private final PactRecord record;
			
			private final int[] keyPositions;
			
			private final Key[] keyHolders;
			
			private DataInputView currentSeg;
			
			private int currentSegNum;
			
			private int currentEnd;
			
			private long currentPointer;
			
			
			private PartitionIterator(int[] keyPositions, Key[] keyHolders)
			{	
				this.record = new PactRecord();
				this.keyPositions = keyPositions;
				this.keyHolders = keyHolders;
				
				final MemorySegment seg = Partition.this.inMemoryBuffers.get(0); 
				this.currentSeg = seg.inputView;
				this.currentSeg.setPosition(PARTITION_BLOCK_HEADER_LEN);
				this.currentEnd = seg.getInt(PARTITION_BLOCK_SIZE_OFFSET);
			}
			
			
			protected final boolean next()
			{
				final int pos = this.currentSeg.getPosition();
				
				if (pos < this.currentEnd) {
					// compute the start pointer
					this.currentPointer = (((long) this.currentSegNum) << 32) | pos;
					
					// read key and also the value
					try {
						this.record.read(this.currentSeg);
						this.record.getFieldsInto(this.keyPositions, this.keyHolders);
					}
					catch (IOException e) {
						throw new RuntimeException("Error while deserializing record from spilled buffer. " +
							"Probably cause: Bad Serialization code.", e);
					}
					return true;
				}
				else {
					this.currentSegNum++;
					if (this.currentSegNum < Partition.this.inMemoryBuffers.size()) {
						final MemorySegment seg = Partition.this.inMemoryBuffers.get(this.currentSegNum); 
						this.currentSeg = seg.inputView;
						this.currentSeg.setPosition(PARTITION_BLOCK_HEADER_LEN);
						this.currentEnd = seg.getInt(PARTITION_BLOCK_SIZE_OFFSET);
						return next();
					}
					else {
						return false;
					}
				}
			}
			
			protected final long getPointer()
			{
				return this.currentPointer;
			}
		}
		
	} // end partition 
	
	
	// ======================================================================================================
	
	
	/**
	 *
	 */
	protected static final class HashBucketIterator
	{
		private final Key[] deserializationObjects;
		
		private final int[] keyPositions;
		
		private Key[] searchKeys;
		
		private MemorySegment bucket;
		
		private MemorySegment[] overflowSegments;
		
		private List<MemorySegment> partitionBuffers;
		
		private int bucketInSegmentOffset;
		
		private int searchHashCode;
		
		private int posInSegment;
		
		private int countInSegment;
		
		private int numInSegment;
		
		private int originalBucketInSegmentOffset;
		
		private MemorySegment originalBucket;
		
		
		private HashBucketIterator(int[] keyPositions, Key[] deserializationHolders)
		{
			this.keyPositions = keyPositions;
			this.deserializationObjects = deserializationHolders;
		}
		
		
		private void set(MemorySegment bucket, MemorySegment[] overflowSegments, List<MemorySegment> partitionBuffers,
				Key[] searchKeys, int searchHashCode, int bucketInSegmentOffset)
		{
			this.bucket = bucket;
			this.originalBucket = bucket;
			this.overflowSegments = overflowSegments;
			this.partitionBuffers = partitionBuffers;
			this.searchKeys = searchKeys;
			this.searchHashCode = searchHashCode;
			this.bucketInSegmentOffset = bucketInSegmentOffset;
			this.originalBucketInSegmentOffset = bucketInSegmentOffset;
			
			this.posInSegment = this.bucketInSegmentOffset + BUCKET_HEADER_LENGTH;
			this.countInSegment = bucket.getShort(bucketInSegmentOffset + HEADER_COUNT_OFFSET);
			this.numInSegment = 0;
		}
		

		public boolean next(PactRecord target)
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
							
						try {
							target.read(buffer);
							if (target.equalsFields(this.keyPositions, this.searchKeys, this.deserializationObjects)) {
								return true;
							}
						}
						catch (IOException ioex) {
							throw new RuntimeException("Error deserializing key or value from the hashtable: " +
									ioex.getMessage(), ioex);
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
				this.posInSegment = this.bucketInSegmentOffset + BUCKET_HEADER_LENGTH;
				this.numInSegment = 0;
			}
		}
		
		public void reset()
		{
			this.bucket = this.originalBucket;
			this.bucketInSegmentOffset = this.originalBucketInSegmentOffset;
			
			this.posInSegment = this.bucketInSegmentOffset + BUCKET_HEADER_LENGTH;
			this.countInSegment = bucket.getShort(bucketInSegmentOffset + HEADER_COUNT_OFFSET);
			this.numInSegment = 0;
		}

	} // end HashBucketIterator
	

	// ======================================================================================================
	
	public static final class ProbeIterator
	{
		private MutableObjectIterator<PactRecord> source;
		
		private PactRecord instance = new PactRecord();
		
		private PactRecord current;
		
		public ProbeIterator(MutableObjectIterator<PactRecord> source)
		{
			set(source);
		}
		
		public void set(MutableObjectIterator<PactRecord> source) 
		{
			this.source = source;
			if (this.current == null) {
				this.current = this.instance;
			}
		}
		
		public PactRecord next() throws IOException
		{
			if (this.source.next(this.instance)) {
				this.current = this.instance;
				return this.current;
			}
			else {
				return null;
			}
		}
		
		public PactRecord getCurrent()
		{
			return this.current;
		}
	}

	// ======================================================================================================
	
	protected static class BlockReaderIterator implements MutableObjectIterator<PactRecord>
	{		
		private MemorySegment currentSegment;
		
		private int currentEndPos;
	
		private final BlockChannelReader reader;
		
		private final LinkedBlockingQueue<MemorySegment> returnQueue;
		
		private int numRequestsRemaining;
		
		private int numReturnsRemaining;
		
		private final List<MemorySegment> freeMemTarget;
		
		
		public BlockReaderIterator(IOManager ioAccess, Channel.ID channel, List<MemorySegment> segments,
				List<MemorySegment> freeMemTarget, int numBlocks)
		throws IOException
		{
			this(ioAccess, channel, new LinkedBlockingQueue<MemorySegment>(), segments, freeMemTarget, numBlocks);
		}
		
		
		public BlockReaderIterator(IOManager ioAccess, Channel.ID channel,  LinkedBlockingQueue<MemorySegment> returnQueue,
				List<MemorySegment> segments, List<MemorySegment> freeMemTarget, int numBlocks)
		throws IOException
		{
			this(ioAccess.createBlockChannelReader(channel, returnQueue), returnQueue,
				segments, freeMemTarget, numBlocks);
		}
		
		
		public BlockReaderIterator(BlockChannelReader reader, LinkedBlockingQueue<MemorySegment> returnQueue,
				List<MemorySegment> segments, List<MemorySegment> freeMemTarget, int numBlocks)
		throws IOException
		{
			this.reader = reader;
			this.returnQueue = returnQueue;
			this.freeMemTarget = freeMemTarget;
			
			this.numRequestsRemaining = numBlocks;
			
			// send off the first requests
			for (; !segments.isEmpty() && this.numRequestsRemaining > 0; numRequestsRemaining--) {
				this.reader.readBlock(segments.remove(segments.size() - 1));
			}
			
			// return the remaining memory, if there is memory left
			while (!segments.isEmpty()) {
				freeMemTarget.add(segments.remove(segments.size() - 1));
			}

			this.numReturnsRemaining = numBlocks - 1;
			
			try {
				this.currentSegment = returnQueue.take();
				this.currentSegment.inputView.setPosition(PARTITION_BLOCK_HEADER_LEN);
				this.currentEndPos = this.currentSegment.getInt(PARTITION_BLOCK_SIZE_OFFSET);
			}
			catch (InterruptedException iex) {
				throw new RuntimeException(iex);
			}
		}

		/* (non-Javadoc)
		 * @see java.util.Iterator#next()
		 */
		@Override
		public boolean next(PactRecord target) throws IOException
		{
			if (this.currentSegment != null) {
				// get the next element from the buffer
				target.read(this.currentSegment.inputView);
				
				int pos = this.currentSegment.inputView.getPosition();
				if (pos < this.currentEndPos) {
					return true;
				}
				else if (pos == this.currentEndPos) {
					// segment done
					// send another request, if more blocks remain
					if (this.numRequestsRemaining > 0) {
						this.reader.readBlock(this.currentSegment);
						this.numRequestsRemaining--;
					}
					else {
						this.freeMemTarget.add(this.currentSegment);
					}
					
					// grab the next block
					if (this.numReturnsRemaining > 0) {
						try {
							this.currentSegment = this.returnQueue.take();
							this.numReturnsRemaining--;
							this.currentSegment.inputView.setPosition(PARTITION_BLOCK_HEADER_LEN);
							this.currentEndPos = this.currentSegment.getInt(PARTITION_BLOCK_SIZE_OFFSET);
						}
						catch (InterruptedException iex) {
							throw new RuntimeException(iex);
						}
					}
					else {
						this.currentSegment = null;
					}
					
					return true;
				}
				else {
					// serialization error
					throw new RuntimeException("Deserialization error while reading record from spilled partition. " +
							"Deserialization consumed more bytes than serialization produced.");
				}
			}
			else {
				return false;
			}
		}
		
	} // end BlockReaderIterator
}
