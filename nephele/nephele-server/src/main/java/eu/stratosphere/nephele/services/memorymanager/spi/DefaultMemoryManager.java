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
 * <li>allocation data is stored as an array list in a dedicated structure</li>
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
public class DefaultMemoryManager implements MemoryManager {
	
	/**
	 * Logging.
	 */
	private static final Log LOG = LogFactory.getLog(DefaultMemoryManager.class);

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
	 * Default chunk size constructor.
	 */
	public DefaultMemoryManager(long memorySize) {
		this(memorySize, MAX_CHUNK_SIZE);
	}

	/**
	 * Full options constructor.
	 * 
	 * @param memorySize
	 * @param chunkSize
	 */
	public DefaultMemoryManager(long memorySize, int chunkSize)
	{
		this.totalSize = memorySize;
		
		// initialize the free segments and allocated segments tracking structures
		this.freeSegments = new LinkedList<FreeSegmentEntry>();
		this.allocatedSegments = new HashMap<AbstractInvokable, ArrayList<DefaultMemorySegment>>();

		// calculate the number of required memory chunks and the length of the last chunk
		int numberOfFullChunks = (int) (memorySize / chunkSize);
		int lastChunkSize = (int) (memorySize % chunkSize);
		
		int numberOfChunks = lastChunkSize == 0 ? numberOfFullChunks : numberOfFullChunks + 1;

		// initialize memory chunks
		this.chunkSize = chunkSize;
		this.memory = new byte[numberOfChunks][];
		
		// add the full chunks
		for (int i = 0; i < numberOfFullChunks; i++) {
			// allocate memory of the specified size
			memory[i] = new byte[chunkSize];
			freeSegments.add(new FreeSegmentEntry(i, i * ((long) this.chunkSize), i * ((long) this.chunkSize) + memory[i].length));
		}
		
		// add the last chunk
		if (lastChunkSize > 0) {
			// allocate memory of the specified size
			memory[numberOfFullChunks] = new byte[lastChunkSize];
			freeSegments.add(new FreeSegmentEntry(numberOfFullChunks, numberOfFullChunks * ((long) this.chunkSize), numberOfFullChunks * ((long) this.chunkSize) + lastChunkSize));
		}
	}


	@Override
	public void shutdown() {
		synchronized (this.lock) {
			
			if (!isShutDown) {
				LOG.debug("Shutting down MemoryManager instance " + toString());
	
				// mark as shutdown and release memory
				isShutDown = true;
				memory = null;
				chunkSize = 0;
				
				// go over all allocated segments and release them
				for (ArrayList<DefaultMemorySegment> segments : allocatedSegments.values()) {
					for (DefaultMemorySegment seg : segments) {
						seg.clearMemoryReferences();
					}
				}
			}
			
		}
	}

	// ------------------------------------------------------------------------
	//                 MemoryManager interface implementation
	// ------------------------------------------------------------------------
	
	public List<MemorySegment> allocate(AbstractInvokable owner, int numDesiredSegments, int desiredSegmentSize, int minimalSegmentSize)
	throws MemoryAllocationException
	{
		ArrayList<MemorySegment> segments = new ArrayList<MemorySegment>(numDesiredSegments + 1);
		
		
		return segments;
	}
	
	/*
	 * @see eu.stratosphere.nephele.services.memorymanager.MemoryManager#allocate(eu.stratosphere.nephele.template.AbstractInvokable, int)
	 */
	@Override
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

			DefaultMemorySegment segment = internalAllocate(owner, segmentSize);
					
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

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.memorymanager.MemoryManager#allocate(eu.stratosphere.nephele.template.AbstractInvokable, int, int)
	 */
	@Override
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
					DefaultMemorySegment segment = internalAllocate(owner, segmentSize);
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
	private final DefaultMemorySegment internalAllocate(AbstractInvokable owner, int segmentSize)
	throws MemoryAllocationException
	{
		// traverse the free list to find first fitting segment
		ListIterator<FreeSegmentEntry> freeSegs = this.freeSegments.listIterator();
		
		while(freeSegs.hasNext()) {
			FreeSegmentEntry entry = freeSegs.next();

			// check if there is enough space in the segment
			if (segmentSize <= entry.end - entry.start) {
				// construct a descriptor for the new segment
				int chunk = (int) (entry.start / chunkSize);
				int start = (int) (entry.start % chunkSize);
				
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
	 * 
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
	 * Factory method for MemorySegment implementations.
	 * 
	 * @param descriptor
	 * @return
	 */
	private static DefaultMemorySegment factory(MemorySegmentDescriptor descriptor) {
		DefaultRandomAccessView randomAccessView = new DefaultRandomAccessView(descriptor);
		DefaultDataInputView inputView = new DefaultDataInputView(descriptor);
		DefaultDataOutputView outputView = new DefaultDataOutputView(descriptor);

		return new DefaultMemorySegment(descriptor, randomAccessView, inputView, outputView);
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


//	// ------------------------------------------------------------------------
//
//	/**
//	 * Instantiates a memory manager that allocates a given fraction of the total memory.
//	 * It will however only allocate so much memory such that at least a given amount of memory remains.
//	 * 
//	 * @param fraction
//	 *        The fraction of the current free space to allocate.
//	 * @param minUnreserved The amount of memory that should not be allocated by the memory manager.
//	 * @param minSize The minimal size the memory manager must have.
//	 * 
//	 * @return An instance of the MemoryManager with the given amount of memory.
//	 */
//	public static DefaultMemoryManager getWithHeapFraction(float fraction, long minUnreserved, long minSize) {
//		// first, get the basic memory statistics from the runtime
//		// the free memory is the currently free memory (in the current committed block), plus
//		// the memory that is currently virtual and uncommitted (max - total)
//		Runtime r = Runtime.getRuntime();
//		long maximum = r.maxMemory();
//		long free = maximum - r.totalMemory() + r.freeMemory();
//		long bytes = 0;
//		long tenuredFree = -1;
//
//		// in order to prevent allocations of arrays that are too big for the JVM's different memory pools,
//		// make sure that the maximum segment size is 70% of the currently free tenure heap
//		List<MemoryPoolMXBean> poolBeans = ManagementFactory.getMemoryPoolMXBeans();
//		for (MemoryPoolMXBean bean : poolBeans) {
//			if (bean.getName().equals("Tenured Gen")) {
//				// found the tenured pool
//				MemoryUsage usage = bean.getUsage();
//				tenuredFree = usage.getMax() - usage.getUsed();
//				break;
//			}
//		}
//
//		long maxSegSize = (tenuredFree == -1) ? Integer.MAX_VALUE : (long) (tenuredFree * 0.8);
//		if (maxSegSize > Integer.MAX_VALUE) {
//			maxSegSize = Integer.MAX_VALUE;
//		}
//
//		if (minUnreserved + minSize < free) {
//			LOG
//				.warn("System has low memory. Allocating the minimal memory manager will leave little memory. Expect performance degradation.");
//			bytes = MIN_MEMORY_SIZE;
//		} else {
//			bytes = (long) (maximum * fraction);
//
//			if (free - bytes < minUnreserved) {
//				bytes = free - minUnreserved;
//				LOG.warn("Memory manager attempt to allocate " + (long) (maximum * fraction)
//					+ " bytes would leave less than " + minUnreserved + " bytes free. Decreasing allocated memory to "
//					+ bytes + " bytes.");
//			} else if (bytes < MIN_MEMORY_SIZE) {
//				bytes = MIN_MEMORY_SIZE;
//				LOG.warn("Increasing memory manager space to the minimum of " + (MIN_MEMORY_SIZE / 1024) + "K.");
//			}
//		}
//
//		LOG.info("Instantiating memory manager with a size of " + bytes + " bytes and a maximal chunk size of "
//			+ maxSegSize + " bytes.");
//
//		return new DefaultMemoryManager(bytes, (int) maxSegSize);
//	}
}
