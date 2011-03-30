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
import java.util.concurrent.LinkedBlockingQueue;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.services.iomanager.BlockChannelWriter;
import eu.stratosphere.nephele.services.iomanager.Buffer;
import eu.stratosphere.nephele.services.iomanager.Channel;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;


/**
 *
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class HashJoin<K extends Key, V extends Value>
{
	// ------------------------------------------------------------------------
	//                             Constants
	// ------------------------------------------------------------------------
	
	private static final int MIN_NUM_MEMORY_SEGMENTS = 33;

	
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
	private final LinkedBlockingQueue<MemorySegment> writeBehindBuffers;
	
	/**
	 * The I/O manager used to instantiate writers for the spilled partitions.
	 */
	private final IOManager ioManager;
	
	// ------------------------------------------------------------------------
	
	/**
	 * The partitions that are built by processing the current partition.
	 */
	private final ArrayList<Partition> partitionsBeingBuilt;
	
	/**
	 * The channel enumerator that is used while processing the current partition to create
	 * channels for the spill partitions it requires.
	 */
	private Channel.Enumerator currentEnumerator;
	
	/**
	 * The number of buffers in the write behind queue that are actually not write behind buffers,
	 * but regular buffers that only have not yet returned. This is part of an optimization that the
	 * spilling code needs not wait until the partition is completely spilled before proceeding.
	 */
	private int writeBehindBuffersAvailable;

	// ------------------------------------------------------------------------
	//                         Construction and Teardown
	// ------------------------------------------------------------------------
	
	public HashJoin(Iterator<KeyValuePair<K, V>> buildSideInput, Iterator<KeyValuePair<K, V>> probeSideInput,
			List<MemorySegment> memorySegments,
			IOManager ioManager)
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
		
		// take away the write behind buffers
		this.writeBehindBuffers = new LinkedBlockingQueue<MemorySegment>();
		for (int i = getNumWriteBehindBuffers(memorySegments.size()); i > 0; --i)
		{
			this.writeBehindBuffers.add(memorySegments.remove(memorySegments.size() - 1));
		}
		
		//
		this.partitionsBeingBuilt = new ArrayList<HashJoin.Partition>();
	}
	
	
	// ------------------------------------------------------------------------
	//                              Life-Cycle
	// ------------------------------------------------------------------------
	
	public void open() throws IOException
	{
		// open builds the initial table by consuming the build-side input
		
		// first thing is to determine the initial fan-out of the hash join
	}
	
	public void next() throws IOException
	{
		// open builds the initial table by consuming the build-side input
	}
	
	/**
	 * Closes the hash table. This effectively releases all internal structures and closes all
	 * open files and removes them.
	 */
	public void close()
	{
		// 
	}
	

	
	// ------------------------------------------------------------------------
	//                       Hash Table Building
	// ------------------------------------------------------------------------
	
	
	public void buildInitialTable(final Iterator<KeyValuePair<K, V>> input)
	{
		// create the partitions
		int partitionFanOut = getPartitioningFanOutNoEstimates(this.availableMemory.size());
		createPartitions(partitionFanOut);
		
		// set up the table structure
		
		
		// go over the complete input
		while (input.hasNext())
		{
			final KeyValuePair<K, V> pair = input.next();
			final int hashCode = hash2(pair.getKey().hashCode());
			
			
			
			// get the hash bucket
			
			// write the pair in the current partition buffer
		}
	}
	
	
	private void createPartitions(int numPartitions)
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
	
	
	private final void insertIntoTable(final KeyValuePair<K, V> pair, int hashCode)
	throws IOException
	{
		final Partition p = null;
		
		// Step 1: Get the partition for this pair and put the pair into the buffer
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
			
			// add the buffer to the partition. giving a partition a new buffer may free up one or more previously used 
			// buffers, if the partition is spilled. Take those buffers back in that case.
			p.addBuffer(nextSeg);
			
			// retry to write into the buffer
			pointer = p.insertIntoBuffer(pair);
			if (pointer == -1) {
				// retry failed, throw an exception
				throw new IOException("Record could not be added to fresh buffer. Probably cause: Record length exceeds buffer size limit.");
			}
		}
		
		// Step 2: Add the pointer and the hash code to the hash bucket
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
			return this.availableMemory.get(s -1);
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
				throw new IOException("Hybrid Hash Join was interrupted while taking a buffer.");
			}
			this.writeBehindBuffersAvailable--;
			
			// grab as many more buffers as are available directly
			MemorySegment currSeg = null;
			while (this.writeBehindBuffersAvailable > 0 && (currSeg = this.writeBehindBuffers.poll()) != null) {
				this.availableMemory.add(currSeg);
				this.writeBehindBuffersAvailable--;
			}
			
			return toReturn;
		}
		else {
			// no memory available
			return null;
		}
	}
	
	
	/**
	 * Selects a partition and spills it. The number of the spilled partition is returned.
	 * 
	 * @return The number of the spilled partition.
	 */
	private int spillPartition() throws IOException
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
		Partition p = partitions.get(largestPartNum);
		
		// spill the partition
		if (this.currentEnumerator == null) {
			this.currentEnumerator = this.ioManager.createChannelEnumerator();
		}
		int numBuffersFreed = p.spillPartition(this.ioManager, this.currentEnumerator.next());
		this.writeBehindBuffersAvailable += numBuffersFreed;
		
		// grab as many buffers as are available directly
		MemorySegment currSeg = null;
		while (this.writeBehindBuffersAvailable > 0 && (currSeg = this.writeBehindBuffers.poll()) != null) {
			this.availableMemory.add(currSeg);
			this.writeBehindBuffersAvailable--;
		}
		
		return largestPartNum;
	}
	
	public static int getInitialTableSize(int numBuffers)
	{
		// each slot needs at least two buffers: one for the data, one for the hash structure
		// further more, we want to keep some buffers for asynchronous writes and to extend
		// the pools for some buckets
		
		// determine the free memory
		Runtime r = Runtime.getRuntime();
		long freeMemory = r.freeMemory() + (r.maxMemory() - r.totalMemory());
		
		return 0;
	}
	
	// ------------------------------------------------------------------------
	//                  Utility Computational Functions
	// ------------------------------------------------------------------------
	
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
	 * The current logic makes sure that there are always between 10 and 100 partitions, and close
	 * to 0.1 of the number of buffers.
	 * 
	 * @param numBuffers The number of buffers available.
	 * @return The number of partitions to use.
	 */
	public static final int getPartitioningFanOutNoEstimates(int numBuffers)
	{
		return Math.max(10, Math.min(numBuffers / 10, 100));
	}
	
	/**
	 * This function hashes an integer value to ensure most uniform distribution across the
	 * integer spectrum. The code is adapted from Bob Jenkins' hash code (http://www.burtleburtle.net/bob/c/lookup3.c),
	 * specifically from the <code>final()</code> function.
	 * 
	 * @param code The integer to be hashed.
	 * @return The hash code for the integer.
	 */
	public static final int hash1(int code)
	{
		int a = (code & 0xff) + ((code >>> 8) & 0xff) + ((code >>> 16) & 0xff) + ((code >>> 24) & 0xff);
		int b = 0x9e3779b1;
		int c = 0x6b43a9b5;
		
		c ^= b;
		c -= (b << 14) | (b >>> 18);
		a ^= c;
		a -= (c << 11) | (c >>> 21);
		b ^= a;
		b -= (a << 25) | (a >>> 7);
		c ^= b;
		c -= (b << 16) | (b >>> 16);
		a ^= c;
		a -= (c << 4) | (c >>> 28);
		b ^= a;
		b -= (a << 14) | (a >>> 18);
		c ^= b;
		c -= (b << 24) | (b >>> 8);
		
		return c;
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
	public static final int hash2(int code)
	{
		code = (code + 0x7ed55d16) + (code << 12);
		code = (code ^ 0xc761c23c) ^ (code >>> 19);
		code = (code + 0x165667b1) + (code << 5);
		code = (code + 0xd3a2646c) ^ (code << 9);
		code = (code + 0xfd7046c5) + (code << 3);
		code = (code ^ 0xb55a4f09) ^ (code >>> 16);
		return code;
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
		private final ArrayList<Buffer.Output> partitionBuffers;	// this partition's buffers
		
		private final LinkedBlockingQueue<MemorySegment> writeBehindBuffers;	// queue for write buffers
		
		private BlockChannelWriter spillingWriter;					// the channel writer, if partition is spilled
		
		private long recordCounter;									// number of records in this partition
		
		private int blockCounter;									// number of blocks in this partition
		
		
		/**
		 * Creates a new partition, initially in memory, with one buffer.
		 * 
		 * @param initialBuffer The initial buffer for this partition.
		 * @param writeBehindBuffers The queue from which to pop buffers for writing, once the partition is spilled.
		 */
		private Partition(MemorySegment initialBuffer, LinkedBlockingQueue<MemorySegment> writeBehindBuffers)
		{
			this.partitionBuffers = new ArrayList<Buffer.Output>(4);
			this.writeBehindBuffers = writeBehindBuffers;
			this.recordCounter = 0;
			this.blockCounter = 0;
			
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
				final int bufferNum = this.partitionBuffers.size() - 1;
				final Buffer.Output targetBuffer = this.partitionBuffers.get(bufferNum);
				final long pointer = (((long) bufferNum) << 32) | targetBuffer.getPosition();

				if (targetBuffer.write(object)) {
					this.recordCounter++;
					return pointer;
				}
				else {
					// signal buffer full
					return -1;
				}
			}
			else {
				// partition is a spilled partition
				final Buffer.Output targetBuffer = this.partitionBuffers.get(0);
				if (!targetBuffer.write(object))
				{
					// buffer is full, send this buffer off
					this.partitionBuffers.clear();
					spillBuffer(targetBuffer);
					
					// get a new one and insert the object
					addBuffer(getNextWriteBehindBuffer());
					final Buffer.Output newBuffer = this.partitionBuffers.get(0);
					
					if (!newBuffer.write(object)) {
						throw new IOException("Record could not be added to fresh buffer. " +
								"Probably cause: Record length exceeds buffer size limit.");
					}
				}
				
				this.recordCounter++;
				return 0;
			}
		}
		
		/**
		 * Adds a new buffer to this partition. This method should only be externally used on partitions that are
		 * in memory, though this method does not check that this is the case.
		 * 
		 * @param segment The new buffer for this partition.
		 */
		public void addBuffer(MemorySegment segment)
		{
			// simply add the buffer
			Buffer.Output buffer = new Buffer.Output(segment);
			this.partitionBuffers.add(buffer);
			this.blockCounter++;
		}
		
		/**
		 * Spills this partition to disk and sets it up such that it continues spilling records that are added to
		 * it.
		 * 
		 * @param ioAccess The I/O manager to be used to create a writer to disk.
		 * @param targetChannel The id of the target channel for this partition.
		 * @return The number of buffers that were freed by spilling this partition.
		 * @throws IOException Thrown, if the writing failed.
		 */
		public int spillPartition(IOManager ioAccess, Channel.ID targetChannel)
		throws IOException
		{
			if (!isInMemory()) {
				throw new RuntimeException("Bug in Hybrid Hash Join: " +
						"Request to spill a partition that has already been spilled.");
			}
			if (this.blockCounter < 2) {
				throw new RuntimeException("Bug in Hybrid Hash Join: " +
					"Request to spill a partition with less than two buffers.");
			}
			
			// create the channel block writer
			this.spillingWriter = ioAccess.createBlockChannWriter(targetChannel, writeBehindBuffers);
			int numBlocks = this.partitionBuffers.size();
			
			// spill all blocks and release them
			for (int i = 0; i < numBlocks; i++) {
				spillBuffer(this.partitionBuffers.get(i));
			}
			this.partitionBuffers.clear();
			
			// reclaim one buffer
			addBuffer(getNextWriteBehindBuffer());
			
			// return the number of blocks that become available
			return numBlocks - 1;
		}
		
		/**
		 * Finalizes and spills the given buffer.
		 * 
		 * @param buffer
		 * @throws IOException
		 */
		private final void spillBuffer(Buffer.Output buffer)
		throws IOException
		{
			this.spillingWriter.writeBlock(buffer);
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
				return this.writeBehindBuffers.take();
			}
			catch (InterruptedException iex) {
				throw new IOException("Hybrid Hash Join Partition was interrupted while taking a buffer.");
			}
		}
		
	} // end partition 
	
	
}
