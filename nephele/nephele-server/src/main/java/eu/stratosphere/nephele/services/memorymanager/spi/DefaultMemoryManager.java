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

package eu.stratosphere.nephele.services.memorymanager.spi;


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.template.AbstractInvokable;


/**
 * Default MemoryManager implementation giving hard memory guarantees. The implementation has the following properties:
 * <ul>
 * <li>arbitrary segment sizes (smaller than 2GB)</li>
 * <li>{@code allocate()} and {@code release()} calls in arbitrary order are supported</li>
 * <li>allocation data is stored in a dedicated structure</li>
 * <li>first-fit selection strategy</li>
 * <li>automatic re-integration of released segments</li>
 * </ul>
 * This implementation uses internal byte arrays to allocate the required memory and allows allocation sizes greater
 * than 2GB. Due to the fact that the length of a single java byte array is bounded by {@link #java.lang.Integer.MAX_VALUE} (2GB),
 * the manager works 2 dimensional byte array (i.e. with memory chunks). Please be aware that in order to keep the array
 * access methods in the {@link DefaultMemorySegment} fast and simple, the actual allocated memory segments must not
 * exceed 2GB and must be contained in a single memory chunk.
 * 
 * @author Alexander Alexandrov
 * @author Stephan Ewen
 */
public class DefaultMemoryManager implements MemoryManager
{
	/**
	 * An enumeration describing the allocation strategy of the memory manager.
	 */
	public enum AllocationStrategy {
		COMPACT;
	}
	
	// ------------------------------------------------------------------------
	
	/**
	 * Logging.
	 */
	private static final Log LOG = LogFactory.getLog(DefaultMemoryManager.class);

	/**
	 * The minimal chunk size, 16 KiBytes.
	 */
	private static final int MIN_CHUNK_SIZE = 16 * 1024;
	
	/**
	 * Maximal memory chunk size.
	 */
	private static final int MAX_CHUNK_SIZE = Integer.MAX_VALUE;

	// ------------------------------------------------------------------------
	
	/**
	 * The lock used on the shared structures.
	 */
	private final Object lock = new Object();
	
	/**
	 * A list of free segments ordered by start address.
	 */
	private final LinkedList<FreeSegmentEntry> freeSegments;
	
	/**
	 * Map from the owner of the segments to the segments.
	 */
	private final HashMap<AbstractInvokable, ArrayList<DefaultMemorySegment>> allocatedSegments;
	
	/**
	 * The initial total size, for verification.
	 */
	private final long totalSize;

	/**
	 * The strategy used by the memory manager.
	 */
	private AllocationStrategy strategy;
	
	/**
	 * The allocated memory for this memoryManager instance.
	 */
	private byte[][] memory;
	
	/**
	 * Size of the array chunks (defaults to MAX_CHUNK_SIZE).
	 */
	private int chunkSize;
	
	/**
	 * A boolean flag indicating whether the close() has already been invoked.
	 */
	private boolean isShutDown = false;

	// ------------------------------------------------------------------------
	// Constructors / Destructors
	// ------------------------------------------------------------------------

	/**
	 * Creates a memory manager with the given capacity, using the default chunk size and
	 * the default allocation strategy.
	 * 
	 * @param memorySize The total size of the memory to be managed by this memory manager.
	 */
	public DefaultMemoryManager(long memorySize) {
		this(memorySize, MAX_CHUNK_SIZE);
	}

	/**
	 * Creates a memory manager with the given capacity and given chunk size.
	 * The default allocation strategy is used.
	 * 
	 * @param memorySize The total size of the memory to be managed by this memory manager.
	 * @param chunkSize The size of the chunks in which the memory manager allocates its memory.
	 */
	public DefaultMemoryManager(long memorySize, int chunkSize)
	{
		this(memorySize, chunkSize, AllocationStrategy.COMPACT);
	}

	/**
	 * Creates a memory manager with the given capacity and given chunk size.
	 * The default allocation strategy is used.
	 * 
	 * @param memorySize The total size of the memory to be managed by this memory manager.
	 * @param chunkSize The size of the chunks in which the memory manager allocates its memory.
	 */
	public DefaultMemoryManager(long memorySize, int chunkSize, AllocationStrategy allocationStrategy)
	{
		// sanity checks
		if (memorySize <= 0) {
			throw new IllegalArgumentException("Size of total memory must be positive.");
		}
		if (chunkSize <= MIN_CHUNK_SIZE) {
			throw new IllegalArgumentException("The chunk size must be at least " + MIN_CHUNK_SIZE + " bytes.");
		}
		if (allocationStrategy == null) {
			throw new IllegalArgumentException("The allocation strategy must not be null.");
		}
		
		this.totalSize = memorySize;
		
		// initialize the free segments and allocated segments tracking structures
		this.freeSegments = new LinkedList<FreeSegmentEntry>();
		this.allocatedSegments = new HashMap<AbstractInvokable, ArrayList<DefaultMemorySegment>>();
		this.strategy = allocationStrategy;

		// calculate the number of required memory chunks and the length of the last chunk
		int numberOfFullChunks = checkedDownCast(memorySize / chunkSize);
		int lastChunkSize = checkedDownCast(memorySize % chunkSize);
		
		int numberOfChunks = lastChunkSize == 0 ? numberOfFullChunks : numberOfFullChunks + 1;

		// initialize memory chunks
		this.chunkSize = chunkSize;
		this.memory = new byte[numberOfChunks][];
		
		// add the full chunks
		for (int i = 0; i < numberOfFullChunks; i++) {
			// allocate memory of the specified size
			this.memory[i] = new byte[chunkSize];
			this.freeSegments.add(new FreeSegmentEntry(i, i * ((long) this.chunkSize), i * ((long) this.chunkSize) + this.memory[i].length));
		}
		
		// add the last chunk
		if (lastChunkSize > 0) {
			// allocate memory of the specified size
			this.memory[numberOfFullChunks] = new byte[lastChunkSize];
			this.freeSegments.add(new FreeSegmentEntry(numberOfFullChunks, numberOfFullChunks * ((long) this.chunkSize), numberOfFullChunks * ((long) this.chunkSize) + lastChunkSize));
		}
	}


	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.memorymanager.MemoryManager#shutdown()
	 */
	@Override
	public void shutdown()
	{
		// -------------------- BEGIN CRITICAL SECTION -------------------
		synchronized (this.lock) {
			if (!isShutDown) {
				LOG.debug("Shutting down MemoryManager instance " + toString());
	
				// mark as shutdown and release memory
				isShutDown = true;
				memory = null;
				chunkSize = 0;
				
				freeSegments.clear();
				
				// go over all allocated segments and release them
				for (ArrayList<DefaultMemorySegment> segments : allocatedSegments.values()) {
					for (DefaultMemorySegment seg : segments) {
						seg.clearMemoryReferences();
					}
				}
			}
		}
		// -------------------- END CRITICAL SECTION -------------------
	}
	
	/**
	 * Gets the memory allocation strategy from this Memory Manager.
	 * <p>
	 * See {@link eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager.AllocationStrategy} for
	 * a description of the allocation strategies.
	 * 
	 * @return The the memory allocation strategy.
	 */
	public AllocationStrategy getStrategy() {
		return strategy;
	}

	
	/**
	 * Sets the memory allocation strategy for the Memory Manager.
	 * <p>
	 * See {@link eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager.AllocationStrategy} for
	 * a description of the allocation strategies.
	 *
	 * @param strategy The memory allocation to set.
	 */
	public void setStrategy(AllocationStrategy strategy) {
		this.strategy = strategy;
	}

	// ------------------------------------------------------------------------
	//                 MemoryManager interface implementation
	// ------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.memorymanager.MemoryManager#allocate(eu.stratosphere.nephele.template.AbstractInvokable, long, int, int)
	 */
	@Override
	public List<MemorySegment> allocate(AbstractInvokable owner, long totalMemory, int minNumSegments, int minSegmentSize)
	throws MemoryAllocationException
	{
		if (minSegmentSize > this.chunkSize) {
			throw new MemoryAllocationException("The memory chunks of this MemoryManager are too small to serve " +
					"segments of minimal size " + minSegmentSize);
		}
		if (LOG.isDebugEnabled() && owner.getEnvironment() != null) {
			LOG.info("Allocating " + totalMemory + " bytes in at least " + minNumSegments + " buffers for " + 
					owner.getEnvironment().getTaskName() + 
					" (" + (owner.getEnvironment().getIndexInSubtaskGroup() + 1) + "/" +
					owner.getEnvironment().getCurrentNumberOfSubtasks() + ")");
		}
		
		final ArrayList<MemorySegment> segments = new ArrayList<MemorySegment>(minNumSegments + 1);
		
		// -------------------- BEGIN CRITICAL SECTION -------------------
		synchronized (this.lock)
		{
			if (isShutDown) {
				throw new IllegalStateException("Memory Manager has been shut down.");
			}
			
			// get the list to store the allocated segment for this owner
			ArrayList<DefaultMemorySegment> segsByThisOwner = allocatedSegments.get(owner);
			if (segsByThisOwner == null) {
				segsByThisOwner = new ArrayList<DefaultMemorySegment>();
				allocatedSegments.put(owner, segsByThisOwner);
			}
			segsByThisOwner.ensureCapacity(segsByThisOwner.size() + minNumSegments + 1);
			
			// allocate depending on the selected allocation strategy
			if (this.strategy == AllocationStrategy.COMPACT) {
				internalAllocateCompact(owner, segments, segsByThisOwner, totalMemory, minNumSegments, minSegmentSize);
			}
			
		}
		// -------------------- END CRITICAL SECTION -------------------
		
		return segments;
	}
	
	/**
	 * Tries to allocate a memory segment of the specified size.
	 * 
	 * @param task The task for which the memory is allocated.
	 * @param segmentSize The size of the memory to be allocated.
	 * @return The allocated memory segment.
	 * 
	 * @throws MemoryAllocationException Thrown, if not enough memory is available.
	 * @throws IllegalArgumentException Thrown, if the size parameter is not a positive integer.
	 */
	public MemorySegment allocate(AbstractInvokable owner, int segmentSize)
	throws MemoryAllocationException
	{
		if (segmentSize < 1) {
			throw new IllegalArgumentException();
		}
		if (owner == null) {
			throw new IllegalArgumentException("The owner of a memory segment must not be null.");
		}
		
		// -------------------- BEGIN CRITICAL SECTION -------------------
		synchronized (this.lock)
		{
			if (isShutDown) {
				throw new IllegalStateException("Memory Manager has been shut down.");
			}

			DefaultMemorySegment segment = internalAllocateStrict(owner, segmentSize);
					
			// store the allocated segment for this owner
			ArrayList<DefaultMemorySegment> segsByThisOwner = allocatedSegments.get(owner);

			if (segsByThisOwner == null) {
				segsByThisOwner = new ArrayList<DefaultMemorySegment>();
				allocatedSegments.put(owner, segsByThisOwner);
			}
		
			segsByThisOwner.add(segment);
			
			return segment;
		}
		// -------------------- END CRITICAL SECTION -------------------
	}

	/**
	 * Tries to allocate a collection of <code>numberOfBuffers</code> memory
	 * segments of the specified <code>segmentSize</code> and <code>segmentType</code>.
	 * <p>
	 * WARNING: The use of this method is restricted to tests. Because of fragmentation, caused by the fact
	 * that the memory in java cannot be allocated in chunks lager than <code>Integer.MAX_VALUE</code>, this
	 * method may fail with large memory sizes in the presence of concurrently active memory consumers.
	 * 
	 * @param task The task for which the memory is allocated.
	 * @param numberOfSegments The number of segments to allocate.
	 * @param segmentSize The size of the memory segments to be allocated.
	 * @return A collection of allocated memory segments.
	 * 
	 * @throws MemoryAllocationException Thrown, if the memory segments could not be allocated.
	 */
	public List<MemorySegment> allocate(AbstractInvokable owner, int numberOfSegments, int segmentSize)
	throws MemoryAllocationException
	{
		if (owner == null) {
			throw new IllegalArgumentException("The owner of a memory segment must not be null.");
		}
		if (segmentSize < 1) {
			throw new IllegalArgumentException();
		}
		if (numberOfSegments < 1) {
			throw new IllegalArgumentException();
		}

		// allocate the list to be returned
		ArrayList<MemorySegment> segments = new ArrayList<MemorySegment>(numberOfSegments);
		
		// get the list of other memory segments by this owner
		ArrayList<DefaultMemorySegment> segsByThisOwner = this.allocatedSegments.get(owner);
		if (segsByThisOwner == null) {
			segsByThisOwner = new ArrayList<DefaultMemorySegment>();
			this.allocatedSegments.put(owner, segsByThisOwner);
		}
		
		// -------------------- BEGIN CRITICAL SECTION -------------------
		synchronized (this.lock)
		{
			boolean allDone = false;
			try
			{
				// try to allocate all the segments
				for (int i = 0; i < numberOfSegments; i++) {
					DefaultMemorySegment segment = internalAllocateStrict(owner, segmentSize);
					segments.add(segment);
					segsByThisOwner.add(segment);
				}
				
				allDone = true;
			}
			finally {
				// if any form of exception occurred, we end here without the flag being set
				if (!allDone) {
					// free the memory
					for (int i = 0; i < segments.size(); i++) {
						internalRelease((DefaultMemorySegment) (segments.get(i)));
					}
					
					// remove the references of to the allocated segments
					for (int i = 0; i < segments.size(); i++) {
						segsByThisOwner.remove(segments.get(i));
					}
				}
			}
		}
		// -------------------- END CRITICAL SECTION -------------------
		
		return segments;
	}
	
	/**
	 * Allocates a given segment for a given owner. This method reserves the memory and created the descriptor
	 * and segment, but does not record the segment in the list of segments from the given owner.
	 * 
	 * WARNING: This method does not do any synchronization and hence requires synchronization by
	 *          its caller.
	 * 
	 * @param owner The owner for which to allocate the segment.
	 * @param segmentSize The size of the segment to be allocated.
	 * @return The allocated memory segment.
	 * @throws MemoryAllocationException Thrown, if not enough memory is available.
	 */
	private final DefaultMemorySegment internalAllocateStrict(AbstractInvokable owner, int segmentSize)
	throws MemoryAllocationException
	{
		// traverse the free list to find first fitting segment
		ListIterator<FreeSegmentEntry> freeSegs = this.freeSegments.listIterator();
		
		while(freeSegs.hasNext()) {
			FreeSegmentEntry entry = freeSegs.next();

			// check if there is enough space in the segment
			if (segmentSize <= entry.end - entry.start) {
				// construct a descriptor for the new segment
				int chunk = checkedDownCast(entry.start / chunkSize);
				int start = checkedDownCast(entry.start % chunkSize);
				
				MemorySegmentDescriptor descriptor = new MemorySegmentDescriptor(owner, memory[chunk], chunk, start, start + segmentSize);

				// adapt the free segment entry
				entry.start += segmentSize;

				// remove the free segment entry from the list if the remaining size is zero
				if (entry.start == entry.end) {
					freeSegs.remove();
				}
				
				// construct the new memory segment
				return factory(descriptor);
			}
		}
		
		throw new MemoryAllocationException();
	}
	

	/**
	 * 
	 * <p>
	 * WARNING: This method does not do any synchronization and hence requires synchronization by
	 *          its caller.
	 * 
	 * @param owner
	 * @param target
	 * @param segsByThisOwner
	 * @param totalMemory
	 * @param minNumSegments
	 * @param minSegmentSize
	 * @throws MemoryAllocationException Thrown, if the required amount of memory could no be gathered without
	 *                                   creating chunks smaller than <tt>minSegmentSize</tt>.
	 */
	private final void internalAllocateCompact(AbstractInvokable owner,
					ArrayList<MemorySegment> target,
					ArrayList<DefaultMemorySegment> segsByThisOwner,
					final long totalMemory, final int minNumSegments, final int minSegmentSize)
	throws MemoryAllocationException
	{	
		final int avgSegmentSize = checkedDownCast(Math.min(totalMemory / minNumSegments, this.chunkSize));
		final int minSizeForSegmentInFirstPass = Math.max(minSegmentSize, (int) (0.65f * avgSegmentSize));
		final long limitForMultipleSegs = 2L * minSizeForSegmentInFirstPass;
		
		if (avgSegmentSize < minSegmentSize) {
			throw new IllegalArgumentException("The requested memory cannot be distributed across the required " +
					"number of chunks without violating the requested minimal chunk size.");
		}
		
		long memoryLeft = totalMemory;
		int numMoreSegments = minNumSegments;
		
		// from now on, we need to make sure that every failure results in a release of the segments
		boolean success = false;
		try {
			// ----------------------------------------------------------------------------------------
			// We make two passes over the segments.
			// 1) In the first, we collect only larger buffers, which are larger than the average buffer
			//    size. We fit as many segments into them as possible without creating buffers that are
			//    less than two-third of the average size.
			// 2) The second one collects all segments above the minimal limit, if there is still
			//    space missing.
			// ----------------------------------------------------------------------------------------
			
			// ------------------ FIRST PASS ------------------
			
			// traverse the free list to find first fitting segment
			ListIterator<FreeSegmentEntry> freeSegs = this.freeSegments.listIterator();
			while (memoryLeft > 0 && freeSegs.hasNext())
			{
				FreeSegmentEntry entry = freeSegs.next();
				int size = checkedDownCast(entry.end - entry.start);
	
				// check if there is enough space in the segment to either
				// - fit all requested memory in
				// - allocate this complete segment to produce multiple result segments
				// - allocate one result segment of average size and 
				if (size >= memoryLeft) {
					// we can do it all in this segment.
					final int numSegmentsToFit = numMoreSegments > 0 ? numMoreSegments : 1;
					final int segSize = checkedDownCast(memoryLeft / numSegmentsToFit);
					
					// make sure the capacities are there in the array lists to hold the segments
					target.ensureCapacity(target.size() + numSegmentsToFit);
					
					// create all except the last result segment, because the last one gets in addition the
					// bytes that were lost due to rounding errors
					if (numSegmentsToFit > 1) {
						for (int i = 0; i < numSegmentsToFit - 1; i++) {
							final MemorySegmentDescriptor descr = createDescriptor(owner, entry, segSize);
							final DefaultMemorySegment newSeg = factory(descr);
							target.add(newSeg);
							segsByThisOwner.add(newSeg);
						}
						memoryLeft -= (numSegmentsToFit - 1) * segSize;
					}
					
					// the last one gets the remainder, including the bytes lost due to rounding errors.
					final MemorySegmentDescriptor descr = createDescriptor(owner, entry, checkedDownCast(memoryLeft));
					final DefaultMemorySegment newSeg = factory(descr);
					target.add(newSeg);
					segsByThisOwner.add(newSeg);
					
					// we are done
					memoryLeft = 0;
					numMoreSegments = 0;
				}
				else if (size >= limitForMultipleSegs) {
					// we use the complete remaining space and fit multiple segments in
					// check how many we will fit in and what size they will be
					int numSegmentsToFit = (int) Math.ceil(size / ((double) avgSegmentSize));
					int segSize = size / numSegmentsToFit;
					
					// if allocating the remainder of this chunk would leave a very small last segment, adjust the size
					if (memoryLeft < size + segSize) {
						segSize = checkedDownCast(memoryLeft / (numSegmentsToFit + 1));
					}
					target.ensureCapacity(target.size() + numSegmentsToFit);
					
					for (int i = 0; i < numSegmentsToFit - 1; i++) {
						final MemorySegmentDescriptor descr = createDescriptor(owner, entry, segSize);
						final DefaultMemorySegment newSeg = factory(descr);
						target.add(newSeg);
						segsByThisOwner.add(newSeg);
					}
					
					// the last one gets the remainder, including the bytes lost due to rounding errors.
					final MemorySegmentDescriptor descr = createDescriptor(owner, entry, checkedDownCast(entry.end - entry.start));
					final DefaultMemorySegment newSeg = factory(descr);
					target.add(newSeg);
					segsByThisOwner.add(newSeg);
					
					memoryLeft -= size;
					numMoreSegments -= numSegmentsToFit;
				}
				else if (size >= avgSegmentSize) {
					// we'll fit one segment in
					int sizeForSegment = (memoryLeft >= 2 * avgSegmentSize) ? avgSegmentSize : (int) (memoryLeft / 2);
					
					// allocate the segment and add it to the lists
					target.ensureCapacity(target.size() + 1);
					final MemorySegmentDescriptor descr = createDescriptor(owner, entry, sizeForSegment);
					final DefaultMemorySegment newSeg = factory(descr);
					target.add(newSeg);
					segsByThisOwner.add(newSeg);
					
					// decrease the memory size that we still need to allocate
					memoryLeft -= sizeForSegment;
					numMoreSegments--;
				}
	
				// remove the free segment entry from the list if the remaining size is zero
				if (entry.start == entry.end) {
					freeSegs.remove();
				}
			}
			
			// ------------------ SECOND PASS ------------------
			if (memoryLeft > 0) {
				// this pass only happens, if the first one did not get enough buffers.
				// the logic is simple: take whatever free segment remains that is larger than the
				// requested minimal segment size
				freeSegs = this.freeSegments.listIterator();
				while (memoryLeft > 0 && freeSegs.hasNext())
				{
					FreeSegmentEntry entry = freeSegs.next();
					int size = checkedDownCast(entry.end - entry.start);
		
					// check if there is enough space in the segment to either
					// - fit all requested memory in
					// - allocate this complete segment to produce multiple result segments
					// - allocate one result segment of average size and 
					if (size >= minSegmentSize) {
						// allocate everything as a segment.
						int segSize = checkedDownCast(Math.min(size, memoryLeft));
						
						target.ensureCapacity(target.size() + 1);
						final MemorySegmentDescriptor descr = createDescriptor(owner, entry, segSize);
						final DefaultMemorySegment newSeg = factory(descr);
						target.add(newSeg);
						segsByThisOwner.add(newSeg);
						
						// decrease the memory size that we still need to allocate
						memoryLeft -= segSize;
						
						// remove the free segment entry from the list if the remaining size is zero
						if (entry.start == entry.end) {
							freeSegs.remove();
						}
					}
				}
			}
			
			// check whether we got all we wanted
			if (memoryLeft > 0) {
				throw new MemoryAllocationException();
			}
			
			success = true;
			return;
		}
		finally {
			// check if we were successful. if not, release all yet allocated memory
			if (!success) {
				for (int i = 0; i < target.size(); i++) {
					internalRelease((DefaultMemorySegment) target.get(i));
				}
				
				segsByThisOwner.removeAll(target);
				target.clear();
			}
		}
	}

	private final MemorySegmentDescriptor createDescriptor(AbstractInvokable owner, FreeSegmentEntry entry, int size)
	{
		// compute chunk and offset
		final int chunk = checkedDownCast(entry.start / this.chunkSize);
		final int start = checkedDownCast(entry.start % this.chunkSize);

		// adapt the free segment entry
		entry.start += size;
		
		return new MemorySegmentDescriptor(owner, memory[chunk], chunk, start, start + size);
	}
	
	// ------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.memorymanager.MemoryManager#release(eu.stratosphere.nephele.services.memorymanager.MemorySegment)
	 */
	@Override
	public void release(MemorySegment segment) {
		// check if segment is null or has already been freed
		if (segment == null || segment.isFree()) {
			return;
		}
		
		final DefaultMemorySegment defSeg = (DefaultMemorySegment) segment;
		final AbstractInvokable owner = defSeg.getSegmentDescriptor().owner;
		
		// -------------------- BEGIN CRITICAL SECTION -------------------
		synchronized (this.lock)
		{
			if (isShutDown) {
				throw new IllegalStateException("Memory manager has been shut down.");
			}

			// remove the reference in the map for the owner
			try {
				ArrayList<DefaultMemorySegment> segsForOwner = this.allocatedSegments.get(owner);
				
				if (segsForOwner != null) {
					segsForOwner.remove(defSeg);
					if (segsForOwner.isEmpty()) {
						this.allocatedSegments.remove(owner);
					}
				}
			}
			catch (Throwable t) {
				LOG.error("Error removing book-keeping reference to allocated memory segment.", t);
			}
			finally {
				// release the memory in any case
				internalRelease(defSeg);
			}
		}
		// -------------------- END CRITICAL SECTION -------------------
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.memorymanager.MemoryManager#release(java.util.Collection)
	 */
	@Override
	public <T extends MemorySegment> void release(Collection<T> segments) {
		
		// sanity checks
		if (segments == null) {
			return;
		}
		
		Iterator<T> segmentsIterator = segments.iterator();
		
		// -------------------- BEGIN CRITICAL SECTION -------------------
		synchronized (this.lock)
		{
			if (isShutDown) {
				throw new IllegalStateException("Memory manager has been shut down.");
			}

			AbstractInvokable lastOwner = null;
			ArrayList<DefaultMemorySegment> segsForOwner = null;

			// go over all segments
			while (segmentsIterator.hasNext()) {
				
				final DefaultMemorySegment defSeg = (DefaultMemorySegment) segmentsIterator.next();
				if (defSeg.isFree()) {
					continue;
				}
				
				final AbstractInvokable owner = defSeg.getSegmentDescriptor().owner;
				
				try {
					// get the list of segments by this owner only if it is a different owner than for
					// the previous one (or it is the first segment)
					if (lastOwner == null || lastOwner != owner) {
						lastOwner = owner;
						segsForOwner = this.allocatedSegments.get(owner);
					}
					
					// remove the segment from the list
					if (segsForOwner != null) {
						segsForOwner.remove(defSeg);
						if (segsForOwner.isEmpty()) {
							this.allocatedSegments.remove(owner);
						}
					}
				}
				catch (Throwable t) {
					LOG.error("Error removing book-keeping reference to allocated memory segment.", t);
				}
				finally {
					// release the memory in any case
					internalRelease(defSeg);
				}
			}
		}
		// -------------------- END CRITICAL SECTION -------------------
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.memorymanager.MemoryManager#releaseAll(eu.stratosphere.nephele.template.AbstractInvokable)
	 */
	@Override
	public <T extends MemorySegment> void releaseAll(AbstractInvokable owner)
	{
		// -------------------- BEGIN CRITICAL SECTION -------------------
		synchronized (this.lock)
		{
			if (isShutDown) {
				throw new IllegalStateException("Memory manager has been shut down.");
			}
			
			// get all segments
			final ArrayList<DefaultMemorySegment> segments = this.allocatedSegments.remove(owner);
			
			// all segments may have been freed previously individually
			if (segments == null || segments.isEmpty()) {
				return;
			}
			
			// free each segment
			for (int i = 0; i < segments.size(); i++) {
				internalRelease(segments.get(i));
			}
			
			segments.clear();
		}
		// -------------------- END CRITICAL SECTION -------------------
	}
	
	/**
	 * Releases the memory segment, making the memory portion available again, clearing all references
	 * in the views. It does not remove the segment from the map of allocated segments.
	 * <p>
	 * WARNING: This method does not do any synchronization and hence requires synchronization by
	 *          its caller.
	 * 
	 * @param segment The segment to be released.
	 */
	private final void internalRelease(DefaultMemorySegment segment)
	{
		final MemorySegmentDescriptor descriptor = segment.getSegmentDescriptor();
		
		// mark the segment as freed and clear all internal references to the memory
		segment.free();
		segment.clearMemoryReferences();

		// re-integrate the memory into the free list
		
		long start = descriptor.chunk * ((long) chunkSize) + descriptor.start;
		long end = descriptor.chunk * ((long) chunkSize) + descriptor.end;

		// case 1: free list is empty
		if (freeSegments.size() == 0) {
			freeSegments.add(new FreeSegmentEntry(descriptor.chunk, start, end));
		}
		// case 2: segment ends before the first free segment entry
		else if (end <= freeSegments.getFirst().start) {
			FreeSegmentEntry firstEntry = freeSegments.get(0);
			if (end == firstEntry.start && descriptor.chunk == firstEntry.chunk) {
				firstEntry.start = start;
			} else {
				freeSegments.addFirst(new FreeSegmentEntry(descriptor.chunk, start, end));
			}
		}
		// case 3: segment starts after the last free segment entry
		else if (start >= freeSegments.getLast().end) {
			FreeSegmentEntry lastEntry = freeSegments.get(freeSegments.size() - 1);
			if (start == lastEntry.end && descriptor.chunk == lastEntry.chunk) {
				lastEntry.end = end;
			} else {
				freeSegments.addLast(new FreeSegmentEntry(descriptor.chunk, start, end));
			}
		}
		// case 4: segment is between two free segment entries (general case, at
		// least two freeSegments entries is ensured)
		else {
			// search for the pair of free segment entries bounding the segment
			// to be freed
			Iterator<FreeSegmentEntry> it = freeSegments.iterator();
			FreeSegmentEntry left = it.next(), right = it.next();
			
			int i = 1;
			while (!(start >= left.end && end <= right.start)) {
				left = right;
				right = it.next();
				i++;
			}

			if (start == left.end && end == right.start && left.chunk == right.chunk) {
				// extends left entry up to the end of right entry and remove
				// the right entry
				left.end = right.end;
				freeSegments.remove(i);
			} else if (start == left.end && descriptor.chunk == left.chunk) {
				// extend left entry up to the end of the segment
				left.end = end;
			} else if (end == right.start && descriptor.chunk == right.chunk) {
				// extend right entry up to the start of the segment
				right.start = start;
			} else {
				// add an extra entry between left and right
				freeSegments.add(i, new FreeSegmentEntry(descriptor.chunk, start, end));
			}
		}
	}
	
	// ------------------------------------------------------------------------

	/**
	 * Checks if the memory manager all memory available and the descriptors of the free segments
	 * describe a contiguous memory layout.
	 * 
	 * @return True, if the memory manager is empty and valid, false if it is not empty or corrupted.
	 */
	public boolean verifyEmpty()
	{
		long nextStart = 0;
		
		for (FreeSegmentEntry entry : this.freeSegments) {
			if (entry.start != nextStart) {
				return false;
			}
			nextStart = entry.end;
		}
		
		return nextStart == this.totalSize;
	}
	
	
	// ------------------------------------------------------------------------	
	
	/**
	 * Factory method for MemorySegment implementations. Creates the views and returns a 
	 * <tt>DefaultMemorySegment</tt> representation of the given memory segment.
	 * 
	 * @param descriptor The memory segment to create the representation for.
	 * @return A <tt>DefaultMemorySegment</tt> representation of the given memory segment.
	 */
	private static final DefaultMemorySegment factory(MemorySegmentDescriptor descriptor) {
		DefaultRandomAccessView randomAccessView = new DefaultRandomAccessView(descriptor);
		DefaultDataInputView inputView = new DefaultDataInputView(descriptor);
		DefaultDataOutputView outputView = new DefaultDataOutputView(descriptor);

		return new DefaultMemorySegment(descriptor, randomAccessView, inputView, outputView);
	}
	
	/**
	 * Casts the given value to an integer, if it can be safely done. If the cast would change the numeric
	 * value, this method raises an exception.
	 * <p>
	 * This method is a protection in places where one expects to be able to safely case, but where unexpected
	 * situations could make the cast unsafe and would cause hidden problems that are hard to track down.
	 * 
	 * @param value The value to be cast to an integer.
	 * @return The given value as an integer.
	 */
	private static final int checkedDownCast(long value) {
		if (value > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Cannot downcast long value " + value + " to integer.");
		}
		return (int) value;
	}

	/**
	 * Entries for the free segments list.
	 * 
	 * @author Alexander Alexandrov
	 */
	private static final class FreeSegmentEntry {
		private int chunk;

		private long start;

		private long end;

		public FreeSegmentEntry(int chunk, long start, long end) {
			this.chunk = chunk;
			this.start = start;
			this.end = end;
		}
	}

	/**
	 * MemorySegments wrapping a portion of the backing memory.
	 * 
	 * @author Alexander Alexandrov
	 */
	public static final class MemorySegmentDescriptor {
		
		protected final AbstractInvokable owner;
		
		protected final byte[] memory;

		protected final int chunk;

		protected final int start;

		protected final int end;

		protected final int size;

		protected MemorySegmentDescriptor(AbstractInvokable owner, byte[] memory, int chunk, int start, int end) {
			this.owner = owner;
			this.memory = memory;
			this.chunk = chunk;
			this.start = start;
			this.end = end;
			this.size = end - start;
		}
	}
}
