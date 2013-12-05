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
import java.util.concurrent.atomic.AtomicBoolean;

import eu.stratosphere.pact.runtime.iterative.io.HashPartitionIterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.services.iomanager.BlockChannelReader;
import eu.stratosphere.nephele.services.iomanager.BulkBlockChannelReader;
import eu.stratosphere.nephele.services.iomanager.Channel;
import eu.stratosphere.nephele.services.iomanager.ChannelReaderInputView;
import eu.stratosphere.nephele.services.iomanager.HeaderlessChannelReaderInputView;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.SeekableDataOutputView;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.generic.types.TypeComparator;
import eu.stratosphere.pact.generic.types.TypePairComparator;
import eu.stratosphere.pact.generic.types.TypeSerializer;
import eu.stratosphere.pact.runtime.io.ChannelReaderInputViewIterator;
import eu.stratosphere.nephele.services.memorymanager.MemorySegmentSource;
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
 * @param <BT> The type of records from the build side that are stored in the hash table.
 * @param <PT> The type of records from the probe side that are stored in the hash table.
 */
public class MutableHashTable<BT, PT> implements MemorySegmentSource {
	
	private static final Log LOG = LogFactory.getLog(MutableHashTable.class);
	
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
	private static final int DEFAULT_RECORD_LEN = 24;
	
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
	
	static final int NUM_INTRA_BUCKET_BITS = 7;
	
	static final int HASH_BUCKET_SIZE = 0x1 << NUM_INTRA_BUCKET_BITS;
	
	static final int BUCKET_HEADER_LENGTH = 16;
	
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
	
	// ------------------------------------------------------------------------
	//                              Members
	// ------------------------------------------------------------------------

	/**
	 * The utilities to serialize the build side data types.
	 */
	protected final TypeSerializer<BT> buildSideSerializer;

	/**
	 * The utilities to serialize the probe side data types.
	 */
	protected final TypeSerializer<PT> probeSideSerializer;
	
	/**
	 * The utilities to hash and compare the build side data types.
	 */
	protected final TypeComparator<BT> buildSideComparator;

	/**
	 * The utilities to hash and compare the probe side data types.
	 */
	private final TypeComparator<PT> probeSideComparator;
	
	/**
	 * The comparator used to determine (in)equality between probe side and build side records.
	 */
	private final TypePairComparator<PT, BT> recordComparator;
	
	/**
	 * The free memory segments currently available to the hash join.
	 */
	protected final List<MemorySegment> availableMemory;
	
	/**
	 * The queue of buffers that can be used for write-behind. Any buffer that is written
	 * asynchronously to disk is returned through this queue. hence, it may sometimes contain more
	 */
	protected final LinkedBlockingQueue<MemorySegment> writeBehindBuffers;
	
	/**
	 * The I/O manager used to instantiate writers for the spilled partitions.
	 */
	protected final IOManager ioManager;
	
	/**
	 * The size of the segments used by the hash join buckets. All segments must be of equal size to ease offset computations.
	 */
	protected final int segmentSize;
	
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
	protected final int bucketsPerSegmentMask;
	
	/**
	 * The number of bits that describe the position of a bucket in a memory segment. Computed as log2(bucketsPerSegment).
	 */
	protected final int bucketsPerSegmentBits;
	
	/**
	 * An estimate for the average record length.
	 */
	private final int avgRecordLen;
	
	// ------------------------------------------------------------------------
	
	/**
	 * The partitions that are built by processing the current partition.
	 */
	protected final ArrayList<HashPartition<BT, PT>> partitionsBeingBuilt;
	
	/**
	 * The partitions that have been spilled previously and are pending to be processed.
	 */
	private final ArrayList<HashPartition<BT, PT>> partitionsPending;
	
	/**
	 * Iterator over the elements in the hash table.
	 */
	private HashBucketIterator<BT, PT> bucketIterator;
	
//	private LazyHashBucketIterator<BT, PT> lazyBucketIterator;
	
	/**
	 * Iterator over the elements from the probe side.
	 */
	protected ProbeIterator<PT> probeIterator;
	
	/**
	 * The reader for the spilled-file of the probe partition that is currently read.
	 */
	private BlockChannelReader currentSpilledProbeSide;
	
	/**
	 * The channel enumerator that is used while processing the current partition to create
	 * channels for the spill partitions it requires.
	 */
	protected Channel.Enumerator currentEnumerator;
	
	/**
	 * The array of memory segments that contain the buckets which form the actual hash-table
	 * of hash-codes and pointers to the elements.
	 */
	protected MemorySegment[] buckets;
	
	/**
	 * The number of buckets in the current table. The bucket array is not necessarily fully
	 * used, when not all buckets that would fit into the last segment are actually used.
	 */
	protected int numBuckets;
	
	/**
	 * The number of buffers in the write behind queue that are actually not write behind buffers,
	 * but regular buffers that only have not yet returned. This is part of an optimization that the
	 * spilling code needs not wait until the partition is completely spilled before proceeding.
	 */
	protected int writeBehindBuffersAvailable;
	
	/**
	 * The recursion depth of the partition that is currently processed. The initial table
	 * has a recursion depth of 0. Partitions spilled from a table that is built for a partition
	 * with recursion depth <i>n</i> have a recursion depth of <i>n+1</i>. 
	 */
	protected int currentRecursionDepth;
	
	/**
	 * Flag indicating that the closing logic has been invoked.
	 */
	protected AtomicBoolean closed = new AtomicBoolean();
	
	/**
	 * If true, build side partitions are kept for multiple probe steps.
	 */
	protected boolean keepBuildSidePartitions = false;
	
	protected boolean furtherPartitioning = false;

	// ------------------------------------------------------------------------
	//                         Construction and Teardown
	// ------------------------------------------------------------------------
	
	public MutableHashTable(TypeSerializer<BT> buildSideSerializer, TypeSerializer<PT> probeSideSerializer,
			TypeComparator<BT> buildSideComparator, TypeComparator<PT> probeSideComparator,
			TypePairComparator<PT, BT> comparator, List<MemorySegment> memorySegments, IOManager ioManager)
	{
		this(buildSideSerializer, probeSideSerializer, buildSideComparator, probeSideComparator, comparator,
			memorySegments, ioManager, DEFAULT_RECORD_LEN);
	}
	
	public MutableHashTable(TypeSerializer<BT> buildSideSerializer, TypeSerializer<PT> probeSideSerializer,
			TypeComparator<BT> buildSideComparator, TypeComparator<PT> probeSideComparator,
			TypePairComparator<PT, BT> comparator, List<MemorySegment> memorySegments,
			IOManager ioManager, int avgRecordLen)
	{
		// some sanity checks first
		if (memorySegments == null) {
			throw new NullPointerException();
		}
		if (memorySegments.size() < MIN_NUM_MEMORY_SEGMENTS) {
			throw new IllegalArgumentException("Too few memory segments provided. Hash Join needs at least " + 
				MIN_NUM_MEMORY_SEGMENTS + " memory segments.");
		}
		
		// assign the members
		this.buildSideSerializer = buildSideSerializer;
		this.probeSideSerializer = probeSideSerializer;
		this.buildSideComparator = buildSideComparator;
		this.probeSideComparator = probeSideComparator;
		this.recordComparator = comparator;
		this.availableMemory = memorySegments;
		this.ioManager = ioManager;
		
		this.avgRecordLen = avgRecordLen > 0 ? avgRecordLen : 
				buildSideSerializer.getLength() == -1 ? DEFAULT_RECORD_LEN : buildSideSerializer.getLength();
		
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
		this.bucketsPerSegmentBits = MathUtils.log2strict(bucketsPerSegment);
		
		// take away the write behind buffers
		this.writeBehindBuffers = new LinkedBlockingQueue<MemorySegment>();
		this.numWriteBehindBuffers = getNumWriteBehindBuffers(memorySegments.size());
		
		this.partitionsBeingBuilt = new ArrayList<HashPartition<BT, PT>>();
		this.partitionsPending = new ArrayList<HashPartition<BT, PT>>();
		
		// because we allow to open and close multiple times, the state is initially closed
		this.closed.set(true);
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
	public void open(final MutableObjectIterator<BT> buildSide, final MutableObjectIterator<PT> probeSide)
	throws IOException
	{
		// sanity checks
		if (!this.closed.compareAndSet(true, false)) {
			throw new IllegalStateException("Hash Join cannot be opened, because it is currently not closed.");
		}
		
		// grab the write behind buffers first
		for (int i = this.numWriteBehindBuffers; i > 0; --i)
		{
			this.writeBehindBuffers.add(this.availableMemory.remove(this.availableMemory.size() - 1));
		}
		// open builds the initial table by consuming the build-side input
		this.currentRecursionDepth = 0;
		buildInitialTable(buildSide);
		
		// the first prober is the probe-side input
		this.probeIterator = new ProbeIterator<PT>(probeSide, this.probeSideSerializer.createInstance());
		
		// the bucket iterator can remain constant over the time
		this.bucketIterator = new HashBucketIterator<BT, PT>(this.buildSideSerializer, this.recordComparator);
//		this.lazyBucketIterator = new LazyHashBucketIterator<BT, PT>(this.recordComparator);
	}
	
	protected boolean processProbeIter() throws IOException{
		final ProbeIterator<PT> probeIter = this.probeIterator;
		final TypeComparator<PT> probeAccessors = this.probeSideComparator;
		
		PT next;
		while ((next = probeIter.next()) != null) {
			final int hash = hash(probeAccessors.hash(next), this.currentRecursionDepth);
			final int posHashCode = hash % this.numBuckets;
			
			// get the bucket for the given hash code
			final int bucketArrayPos = posHashCode >> this.bucketsPerSegmentBits;
			final int bucketInSegmentOffset = (posHashCode & this.bucketsPerSegmentMask) << NUM_INTRA_BUCKET_BITS;
			final MemorySegment bucket = this.buckets[bucketArrayPos];
			
			// get the basic characteristics of the bucket
			final int partitionNumber = bucket.get(bucketInSegmentOffset + HEADER_PARTITION_OFFSET);
			final HashPartition<BT, PT> p = this.partitionsBeingBuilt.get(partitionNumber);
			
			// for an in-memory partition, process set the return iterators, else spill the probe records
			if (p.isInMemory()) {
				this.recordComparator.setReference(next);
				this.bucketIterator.set(bucket, p.overflowSegments, p, hash, bucketInSegmentOffset);
				return true;
			}
			else {
				p.insertIntoProbeBuffer(next);
			}
		}
		
		// -------------- partition done ---------------
		
		return false;
	}
	
	protected boolean prepareNextPartition() throws IOException {
		// finalize and cleanup the partitions of the current table
		int buffersAvailable = 0;
		for (int i = 0; i < this.partitionsBeingBuilt.size(); i++) {
			final HashPartition<BT, PT> p = this.partitionsBeingBuilt.get(i);
			p.setFurtherPatitioning(this.furtherPartitioning);
			buffersAvailable += p.finalizeProbePhase(this.availableMemory, this.partitionsPending);
		}
		
		this.partitionsBeingBuilt.clear();
		this.writeBehindBuffersAvailable += buffersAvailable;
		
		releaseTable();

		if (this.currentSpilledProbeSide != null) {
			this.currentSpilledProbeSide.closeAndDelete();
			this.currentSpilledProbeSide = null;
		}

		// check if there are pending partitions
		if (!this.partitionsPending.isEmpty())
		{
			final HashPartition<BT, PT> p = this.partitionsPending.get(0);

			// build the next table
			buildTableFromSpilledPartition(p);

			// set the probe side - gather memory segments for reading
			LinkedBlockingQueue<MemorySegment> returnQueue = new LinkedBlockingQueue<MemorySegment>();
			this.currentSpilledProbeSide = this.ioManager.createBlockChannelReader(p.getProbeSideChannel().getChannelID(), returnQueue);

			List<MemorySegment> memory = new ArrayList<MemorySegment>();
			memory.add(getNextBuffer());
			memory.add(getNextBuffer());

			ChannelReaderInputViewIterator<PT> probeReader = new ChannelReaderInputViewIterator<PT>(this.currentSpilledProbeSide,
				returnQueue, memory, this.availableMemory, this.probeSideSerializer, p.getProbeSideBlockCount());
			this.probeIterator.set(probeReader);

			// unregister the pending partition
			this.partitionsPending.remove(0);
			this.currentRecursionDepth = p.getRecursionLevel() + 1;
			
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
	 * @throws IOException
	 */
	public boolean nextRecord() throws IOException {
		
		final boolean probeProcessing = processProbeIter();
		if(probeProcessing) {
			return true;
		}
		return prepareNextPartition();
	}
	
	public HashBucketIterator<BT, PT> getMatchesFor(PT record) throws IOException
	{
		final TypeComparator<PT> probeAccessors = this.probeSideComparator;
		final int hash = hash(probeAccessors.hash(record), this.currentRecursionDepth);
		final int posHashCode = hash % this.numBuckets;
		
		// get the bucket for the given hash code
		final int bucketArrayPos = posHashCode >> this.bucketsPerSegmentBits;
		final int bucketInSegmentOffset = (posHashCode & this.bucketsPerSegmentMask) << NUM_INTRA_BUCKET_BITS;
		final MemorySegment bucket = this.buckets[bucketArrayPos];
		
		// get the basic characteristics of the bucket
		final int partitionNumber = bucket.get(bucketInSegmentOffset + HEADER_PARTITION_OFFSET);
		final HashPartition<BT, PT> p = this.partitionsBeingBuilt.get(partitionNumber);
		
		// for an in-memory partition, process set the return iterators, else spill the probe records
		if (p.isInMemory()) {
			this.recordComparator.setReference(record);
			this.bucketIterator.set(bucket, p.overflowSegments, p, hash, bucketInSegmentOffset);
			return this.bucketIterator;
		}
		else {
			throw new IllegalStateException("Method is not applicable to partially spilled hash tables.");
		}
	}
	
//	public LazyHashBucketIterator<BT, PT> getLazyMatchesFor(PT record) throws IOException
//	{
//		final TypeComparator<PT> probeAccessors = this.probeSideComparator;
//		final int hash = hash(probeAccessors.hash(record), this.currentRecursionDepth);
//		final int posHashCode = hash % this.numBuckets;
//		
//		// get the bucket for the given hash code
//		final int bucketArrayPos = posHashCode >> this.bucketsPerSegmentBits;
//		final int bucketInSegmentOffset = (posHashCode & this.bucketsPerSegmentMask) << NUM_INTRA_BUCKET_BITS;
//		final MemorySegment bucket = this.buckets[bucketArrayPos];
//		
//		// get the basic characteristics of the bucket
//		final int partitionNumber = bucket.get(bucketInSegmentOffset + HEADER_PARTITION_OFFSET);
//		final HashPartition<BT, PT> p = this.partitionsBeingBuilt.get(partitionNumber);
//		
//		// for an in-memory partition, process set the return iterators, else spill the probe records
//		if (p.isInMemory()) {
//			this.recordComparator.setReference(record);
//			this.lazyBucketIterator.set(bucket, p.overflowSegments, p, hash, bucketInSegmentOffset);
//			return this.lazyBucketIterator;
//		}
//		else {
//			throw new IllegalStateException("Method is not applicable to partially spilled hash tables.");
//		}
//	}
	
	/**
	 * @return
	 */
	public PT getCurrentProbeRecord() {
		return this.probeIterator.getCurrent();
	}
	
	/**
	 * @return
	 */
	public HashBucketIterator<BT, PT> getBuildSideIterator() {
		return this.bucketIterator;
	}

	public MutableObjectIterator<BT> getPartitionEntryIterator() {
		return new HashPartitionIterator<BT, PT>(this.partitionsBeingBuilt.iterator(), this.buildSideSerializer);
	}
	
	/**
	 * Closes the hash table. This effectively releases all internal structures and closes all
	 * open files and removes them. The call to this method is valid both as a cleanup after the
	 * complete inputs were properly processed, and as an cancellation call, which cleans up
	 * all resources that are currently held by the hash join.
	 */
	public void close() {
		// make sure that we close only once
		if (!this.closed.compareAndSet(false, true)) {
			return;
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
			final HashPartition<BT, PT> p = this.partitionsPending.get(i);
			p.clearAllMemory(this.availableMemory);
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
	}
	
	public List<MemorySegment> getFreedMemory()
	{
		if (!this.closed.get()) {
			throw new IllegalStateException("Cannot return memory while join is open.");
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
	protected void buildInitialTable(final MutableObjectIterator<BT> input)
	throws IOException
	{
		// create the partitions
		final int partitionFanOut = getPartitioningFanOutNoEstimates(this.availableMemory.size());
		if (partitionFanOut > MAX_NUM_PARTITIONS) {
			throw new RuntimeException("Hash join partitions estimate exeeds maximum number of partitions."); 
		}
		createPartitions(partitionFanOut, 0);
		
		// set up the table structure. the write behind buffers are taken away, as are one buffer per partition
		final int numBuckets = getInitialTableSize(this.availableMemory.size(), this.segmentSize, 
			partitionFanOut, this.avgRecordLen);
		initTable(numBuckets, (byte) partitionFanOut);
		
		final TypeComparator<BT> buildTypeComparator = this.buildSideComparator;
		final BT record = this.buildSideSerializer.createInstance();
		
		// go over the complete input and insert every element into the hash table
		while (input.next(record)) {
			final int hashCode = hash(buildTypeComparator.hash(record), 0);
			insertIntoTable(record, hashCode);
		}

		// finalize the partitions
		for (int i = 0; i < this.partitionsBeingBuilt.size(); i++) {
			HashPartition<BT, PT> p = this.partitionsBeingBuilt.get(i);
			p.finalizeBuildPhase(this.ioManager, this.currentEnumerator, this.writeBehindBuffers);
		}
	}
	
	/**
	 * @param p
	 * @throws IOException
	 */
	protected void buildTableFromSpilledPartition(final HashPartition<BT, PT> p) throws IOException {
		
		final int nextRecursionLevel = p.getRecursionLevel() + 1;
		if (nextRecursionLevel > MAX_RECURSION_DEPTH) {
			throw new RuntimeException("Hash join exceeded maximum number of recursions, without reducing "
				+ "partitions enough to be memory resident. Probably cause: Too many duplicate keys.");
		}
		
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
		
		long numBuckets = (p.getBuildSideRecordCount() * RECORD_TABLE_BYTES) / (HASH_BUCKET_SIZE - BUCKET_HEADER_LENGTH) + 1;
		
		// we need to consider the worst case where everything hashes to one bucket which needs to overflow by the same
		// number of total buckets again.
		final long totalBuffersNeeded = (numBuckets * 2) / (this.bucketsPerSegmentMask + 1) + p.getBuildSideBlockCount() + 1;
		
		if (totalBuffersNeeded < totalBuffersAvailable) {
			// we are guaranteed to stay in memory
			ensureNumBuffersReturned(p.getBuildSideBlockCount());
			
			// first read the partition in
			final BulkBlockChannelReader reader = this.ioManager.createBulkBlockChannelReader(p.getBuildSideChannel().getChannelID(), 
				this.availableMemory, p.getBuildSideBlockCount());
			// call waits until all is read
			if (keepBuildSidePartitions && p.recursionLevel == 0) {
				reader.close(); // keep the partitions
			} else {
				reader.closeAndDelete();
			}
			
			final List<MemorySegment> partitionBuffers = reader.getFullSegments();
			final HashPartition<BT, PT> newPart = new HashPartition<BT, PT>(this.buildSideSerializer, this.probeSideSerializer,
					0, nextRecursionLevel, partitionBuffers, p.getBuildSideRecordCount(), this.segmentSize, p.getLastSegmentLimit());
			
			this.partitionsBeingBuilt.add(newPart);
			
			// erect the buckets
			initTable((int) numBuckets, (byte) 1);
			
			// now, index the partition through a hash table
			final HashPartition<BT, PT>.PartitionIterator pIter = newPart.getPartitionIterator(this.buildSideComparator);
			final BT record = this.buildSideSerializer.createInstance();
			
			while (pIter.next(record)) {
				final int hashCode = hash(pIter.getCurrentHashCode(), nextRecursionLevel);
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
			final int avgRecordLenPartition = (int) (((long) p.getBuildSideBlockCount()) * 
					this.segmentSize / p.getBuildSideRecordCount());
			
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
			
			final BlockChannelReader inReader = this.ioManager.createBlockChannelReader(p.getBuildSideChannel().getChannelID());
			final ChannelReaderInputView inView = new HeaderlessChannelReaderInputView(inReader, segments,
						p.getBuildSideBlockCount(), p.getLastSegmentLimit(), false);
			final ChannelReaderInputViewIterator<BT> inIter = new ChannelReaderInputViewIterator<BT>(inView, 
					this.availableMemory, this.buildSideSerializer);
			final TypeComparator<BT> btComparator = this.buildSideComparator;
			final BT rec = this.buildSideSerializer.createInstance();
			while (inIter.next(rec))
			{	
				final int hashCode = hash(btComparator.hash(rec), nextRecursionLevel);
				insertIntoTable(rec, hashCode);
			}

			// finalize the partitions
			for (int i = 0; i < this.partitionsBeingBuilt.size(); i++) {
				HashPartition<BT, PT> part = this.partitionsBeingBuilt.get(i);
				part.finalizeBuildPhase(this.ioManager, this.currentEnumerator, this.writeBehindBuffers);
			}
		}
	}
	
	/**
	 * @param record
	 * @param hashCode
	 * @throws IOException
	 */
	protected final void insertIntoTable(final BT record, final int hashCode) throws IOException {
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
		final HashPartition<BT, PT> p = this.partitionsBeingBuilt.get(partitionNumber);
		
		// --------- Step 1: Get the partition for this pair and put the pair into the buffer ---------
		
		long pointer = p.insertIntoBuildBuffer(record);
		if (pointer != -1) {
			// record was inserted into an in-memory partition. a pointer must be inserted into the buckets
			insertBucketEntry(p, bucket, bucketInSegmentPos, hashCode, pointer);
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
	final void insertBucketEntry(final HashPartition<BT, PT> p, final MemorySegment bucket, 
			final int bucketInSegmentPos, final int hashCode, final long pointer)
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
					if (spilledPart == p.getPartitionNumber()) {
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
			overflowSeg.putShort(overflowBucketOffset + HEADER_COUNT_OFFSET, (short) 1); 
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//                          Setup and Tear Down of Structures
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Returns a new inMemoryPartition object.
	 * This is required as a plug for ReOpenableMutableHashTable.
	 */
	protected HashPartition<BT, PT> getNewInMemoryPartition(int number, int recursionLevel) {
		return new HashPartition<BT, PT>(this.buildSideSerializer, this.probeSideSerializer,
				number, recursionLevel, this.availableMemory.remove(this.availableMemory.size() - 1),
				this, this.segmentSize);
	}
	/**
	 * @param numPartitions
	 */
	protected void createPartitions(int numPartitions, int recursionLevel) {
		// sanity check
		ensureNumBuffersReturned(numPartitions);
		
		this.currentEnumerator = this.ioManager.createChannelEnumerator();
		
		this.partitionsBeingBuilt.clear();
		for (int i = 0; i < numPartitions; i++) {
			HashPartition<BT, PT> p = getNewInMemoryPartition(i, recursionLevel);
			this.partitionsBeingBuilt.add(p);
		}
	}
	
	/**
	 * This method clears all partitions currently residing (partially) in memory. It releases all memory
	 * and deletes all spilled partitions.
	 * <p>
	 * This method is intended for a hard cleanup in the case that the join is aborted.
	 */
	protected void clearPartitions() {
		for (int i = this.partitionsBeingBuilt.size() - 1; i >= 0; --i) {
			final HashPartition<BT, PT> p = this.partitionsBeingBuilt.get(i);
			try {
				p.clearAllMemory(this.availableMemory);
			} catch (Exception e) {
				LOG.error("Error during partition cleanup.", e);
			}
		}
		this.partitionsBeingBuilt.clear();
	}
	
	/**
	 * @param numBuckets
	 * @param numPartitions
	 * @return
	 */
	protected void initTable(int numBuckets, byte numPartitions) {
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
	protected void releaseTable() {
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
	protected int spillPartition() throws IOException {
		// find the largest partition
		ArrayList<HashPartition<BT, PT>> partitions = this.partitionsBeingBuilt;
		int largestNumBlocks = 0;
		int largestPartNum = -1;
		
		for (int i = 0; i < partitions.size(); i++) {
			HashPartition<BT, PT> p = partitions.get(i);
			if (p.isInMemory() && p.getBuildSideBlockCount() > largestNumBlocks) {
				largestNumBlocks = p.getBuildSideBlockCount();
				largestPartNum = i;
			}
		}
		final HashPartition<BT, PT> p = partitions.get(largestPartNum);
		
		// spill the partition
		int numBuffersFreed = p.spillPartition(this.availableMemory, this.ioManager, 
										this.currentEnumerator.next(), this.writeBehindBuffers);
		this.writeBehindBuffersAvailable += numBuffersFreed;
		// grab as many buffers as are available directly
		MemorySegment currBuff = null;
		while (this.writeBehindBuffersAvailable > 0 && (currBuff = this.writeBehindBuffers.poll()) != null) {
			this.availableMemory.add(currBuff);
			this.writeBehindBuffersAvailable--;
		}
		return largestPartNum;
	}
	
	/**
	 * This method makes sure that at least a certain number of memory segments is in the list of free segments.
	 * Free memory can be in the list of free segments, or in the return-queue where segments used to write behind are
	 * put. The number of segments that are in that return-queue, but are actually reclaimable is tracked. This method
	 * makes sure at least a certain number of buffers is reclaimed.
	 *  
	 * @param minRequiredAvailable The minimum number of buffers that needs to be reclaimed.
	 */
	final void ensureNumBuffersReturned(final int minRequiredAvailable) {
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
	final MemorySegment getNextBuffer() {
		// check if the list directly offers memory
		int s = this.availableMemory.size();
		if (s > 0) {
			return this.availableMemory.remove(s-1);
		}
		
		// check if there are write behind buffers that actually are to be used for the hash table
		if (this.writeBehindBuffersAvailable > 0) {
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
		} else {
			// no memory available
			return null;
		}
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.io.MemorySegmentSource#nextSegment()
	 */
	@Override
	public MemorySegment nextSegment() {
		final MemorySegment seg = getNextBuffer();
		if (seg == null) {
			try {
				spillPartition();
			} catch (IOException ioex) {
				throw new RuntimeException("Error spilling Hash Join Partition" + (ioex.getMessage() == null ?
					"." : ": " + ioex.getMessage()), ioex);
			}
			
			MemorySegment fromSpill = getNextBuffer();
			if (fromSpill == null) {
				throw new RuntimeException("BUG in Hybrid Hash Join: Spilling did not free a buffer.");
			} else {
				return fromSpill;
			}
		} else {
			return seg;
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
	public static final int getNumWriteBehindBuffers(int numBuffers) {
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
	public static final int getPartitioningFanOutNoEstimates(int numBuffers) {
		return Math.max(10, Math.min(numBuffers / 10, MAX_NUM_PARTITIONS));
	}
	
	public static final int getInitialTableSize(int numBuffers, int bufferSize, int numPartitions, int recordLenBytes) {
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
	 * @param bucket
	 * @param maxParts
	 * @return The hash code for the integer.
	 */
	public static final byte assignPartition(int bucket, byte numPartitions) {
		return (byte) (bucket % numPartitions);
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
	public static final int hash(int code, int level) {
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
	
	// ======================================================================================================
	
	/**
	 *
	 */
	public static class HashBucketIterator<BT, PT> implements MutableObjectIterator<BT> {
		
		private final TypeSerializer<BT> accessor;
		
		private final TypePairComparator<PT, BT> comparator;
		
		private MemorySegment bucket;
		
		private MemorySegment[] overflowSegments;
		
		private HashPartition<BT, PT> partition;
		
		private int bucketInSegmentOffset;
		
		private int searchHashCode;
		
		private int posInSegment;
		
		private int countInSegment;
		
		private int numInSegment;
		
		private int originalBucketInSegmentOffset;
		
		private MemorySegment originalBucket;
		
		private long lastPointer;
		
		
		HashBucketIterator(TypeSerializer<BT> accessor, TypePairComparator<PT, BT> comparator) {
			this.accessor = accessor;
			this.comparator = comparator;
		}
		
		
		void set(MemorySegment bucket, MemorySegment[] overflowSegments, HashPartition<BT, PT> partition,
				int searchHashCode, int bucketInSegmentOffset)
		{
			this.bucket = bucket;
			this.originalBucket = bucket;
			this.overflowSegments = overflowSegments;
			this.partition = partition;
			this.searchHashCode = searchHashCode;
			this.bucketInSegmentOffset = bucketInSegmentOffset;
			this.originalBucketInSegmentOffset = bucketInSegmentOffset;
			
			this.posInSegment = this.bucketInSegmentOffset + BUCKET_HEADER_LENGTH;
			this.countInSegment = bucket.getShort(bucketInSegmentOffset + HEADER_COUNT_OFFSET);
			this.numInSegment = 0;
		}
		

		public boolean next(BT target) {
			// loop over all segments that are involved in the bucket (original bucket plus overflow buckets)
			while (true) {
				
				while (this.numInSegment < this.countInSegment) {
					
					final int thisCode = this.bucket.getInt(this.posInSegment);
					this.posInSegment += HASH_CODE_LEN;
						
					// check if the hash code matches
					if (thisCode == this.searchHashCode) {
						// get the pointer to the pair
						final long pointer = this.bucket.getLong(this.bucketInSegmentOffset + 
													BUCKET_POINTER_START_OFFSET + (this.numInSegment * POINTER_LEN));
						this.numInSegment++;
						
						// deserialize the key to check whether it is really equal, or whether we had only a hash collision
						try {
							this.partition.setReadPosition(pointer);
							this.accessor.deserialize(target, this.partition);
							if (this.comparator.equalToReference(target)) {
								this.lastPointer = pointer;
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
		
		public void writeBack(BT value) throws IOException {
			final SeekableDataOutputView outView = this.partition.getWriteView();
			outView.setWritePosition(this.lastPointer);
			this.accessor.serialize(value, outView);
		}
		
		public void reset() {
			this.bucket = this.originalBucket;
			this.bucketInSegmentOffset = this.originalBucketInSegmentOffset;
			
			this.posInSegment = this.bucketInSegmentOffset + BUCKET_HEADER_LENGTH;
			this.countInSegment = bucket.getShort(bucketInSegmentOffset + HEADER_COUNT_OFFSET);
			this.numInSegment = 0;
		}

	} // end HashBucketIterator
	

	// ======================================================================================================
	
//	public static final class LazyHashBucketIterator<BT, PT> {
//		
//		private final TypePairComparator<PT, BT> comparator;
//		
//		private MemorySegment bucket;
//		
//		private MemorySegment[] overflowSegments;
//		
//		private HashPartition<BT, PT> partition;
//		
//		private int bucketInSegmentOffset;
//		
//		private int searchHashCode;
//		
//		private int posInSegment;
//		
//		private int countInSegment;
//		
//		private int numInSegment;
//		
//		private LazyHashBucketIterator(TypePairComparator<PT, BT> comparator) {
//			this.comparator = comparator;
//		}
//		
//		
//		void set(MemorySegment bucket, MemorySegment[] overflowSegments, HashPartition<BT, PT> partition,
//				int searchHashCode, int bucketInSegmentOffset) {
//			
//			this.bucket = bucket;
//			this.overflowSegments = overflowSegments;
//			this.partition = partition;
//			this.searchHashCode = searchHashCode;
//			this.bucketInSegmentOffset = bucketInSegmentOffset;
//			
//			this.posInSegment = this.bucketInSegmentOffset + BUCKET_HEADER_LENGTH;
//			this.countInSegment = bucket.getShort(bucketInSegmentOffset + HEADER_COUNT_OFFSET);
//			this.numInSegment = 0;
//		}
//
//		public boolean next(BT target) {
//			// loop over all segments that are involved in the bucket (original bucket plus overflow buckets)
//			while (true) {
//				
//				while (this.numInSegment < this.countInSegment) {
//					
//					final int thisCode = this.bucket.getInt(this.posInSegment);
//					this.posInSegment += HASH_CODE_LEN;
//						
//					// check if the hash code matches
//					if (thisCode == this.searchHashCode) {
//						// get the pointer to the pair
//						final long pointer = this.bucket.getLong(this.bucketInSegmentOffset + 
//													BUCKET_POINTER_START_OFFSET + (this.numInSegment * POINTER_LEN));
//						this.numInSegment++;
//							
//						// check whether it is really equal, or whether we had only a hash collision
//						LazyDeSerializable lds = (LazyDeSerializable) target;
//						lds.setDeSerializer(this.partition, this.partition.getWriteView(), pointer);
//						if (this.comparator.equalToReference(target)) {
//							return true;
//						}
//					}
//					else {
//						this.numInSegment++;
//					}
//				}
//				
//				// this segment is done. check if there is another chained bucket
//				final long forwardPointer = this.bucket.getLong(this.bucketInSegmentOffset + HEADER_FORWARD_OFFSET);
//				if (forwardPointer == BUCKET_FORWARD_POINTER_NOT_SET) {
//					return false;
//				}
//				
//				final int overflowSegNum = (int) (forwardPointer >>> 32);
//				this.bucket = this.overflowSegments[overflowSegNum];
//				this.bucketInSegmentOffset = (int) (forwardPointer & 0xffffffff);
//				this.countInSegment = this.bucket.getShort(this.bucketInSegmentOffset + HEADER_COUNT_OFFSET);
//				this.posInSegment = this.bucketInSegmentOffset + BUCKET_HEADER_LENGTH;
//				this.numInSegment = 0;
//			}
//		}
//	} 
	

	// ======================================================================================================
	
	public static final class ProbeIterator<PT> {
		
		private MutableObjectIterator<PT> source;
		
		private final PT instance;
		
		private PT current;
		
		
		ProbeIterator(MutableObjectIterator<PT> source, PT instance) {
			this.instance = instance;
			set(source);
		}
		
		void set(MutableObjectIterator<PT> source) {
			this.source = source;
			if (this.current == null) {
				this.current = this.instance;
			}
		}
		
		public PT next() throws IOException {
			if (this.source.next(this.instance)) {
				this.current = this.instance;
				return this.current;
			} else {
				return null;
			}
		}
		
		public PT getCurrent() {
			return this.current;
		}
	}
}
