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

package org.apache.flink.runtime.memorymanager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;


public class DefaultMemoryManager implements MemoryManager {
	
	/**
	 * The default memory page size. Currently set to 32 KiBytes.
	 */
	public static final int DEFAULT_PAGE_SIZE = 32 * 1024;
	
	/**
	 * The minimal memory page size. Currently set to 4 KiBytes.
	 */
	public static final int MIN_PAGE_SIZE = 4 * 1024;
	
	/**
	 * The Logger.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(DefaultMemoryManager.class);
	
	// --------------------------------------------------------------------------------------------
	
	private final Object lock = new Object();	 	// The lock used on the shared structures.
	
	private final ArrayDeque<byte[]> freeSegments;	// the free memory segments
	
	private final HashMap<AbstractInvokable, Set<DefaultMemorySegment>> allocatedSegments;
	
	private final long roundingMask;		// mask used to round down sizes to multiples of the page size
	
	private final int pageSize;				// the page size, in bytes
	
	private final int pageSizeBits;			// the number of bits that the power-of-two page size corresponds to
	
	private final int totalNumPages;		// The initial total size, for verification.

	/** The total size of the memory managed by this memory manager */
	private final long memorySize;

	/** Number of slots of the task manager */
	private final int numberOfSlots;
	
	private final boolean isPreAllocated;
	
	/** The number of memory pages that have not been allocated and are available for lazy allocation */
	private int numNonAllocatedPages;
	
	/** flag whether the close() has already been invoked */
	private boolean isShutDown;

	// ------------------------------------------------------------------------
	// Constructors / Destructors
	// ------------------------------------------------------------------------

	/**
	 * Creates a memory manager with the given capacity, using the default page size.
	 * 
	 * @param memorySize The total size of the memory to be managed by this memory manager.
	 * @param numberOfSlots The number of slots of the task manager.
	 */
	public DefaultMemoryManager(long memorySize, int numberOfSlots) {
		this(memorySize, numberOfSlots, DEFAULT_PAGE_SIZE, true);
	}

	/**
	 * Creates a memory manager with the given capacity and given page size.
	 * 
	 * @param memorySize The total size of the memory to be managed by this memory manager.
	 * @param numberOfSlots The number of slots of the task manager.
	 * @param pageSize The size of the pages handed out by the memory manager.
	 * @param preAllocateMemory True, if the memory manaber should immediately allocate all memory, false
	 *                          if it should allocate and release the memory as needed.
	 */
	public DefaultMemoryManager(long memorySize, int numberOfSlots, int pageSize, boolean preAllocateMemory) {
		// sanity checks
		if (memorySize <= 0) {
			throw new IllegalArgumentException("Size of total memory must be positive.");
		}
		if (pageSize < MIN_PAGE_SIZE) {
			throw new IllegalArgumentException("The page size must be at least " + MIN_PAGE_SIZE + " bytes.");
		}
		if ((pageSize & (pageSize - 1)) != 0) {
			// not a power of two
			throw new IllegalArgumentException("The given page size is not a power of two.");
		}

		this.memorySize = memorySize;

		this.numberOfSlots = numberOfSlots;
		
		// assign page size and bit utilities
		this.pageSize = pageSize;
		this.roundingMask = ~((long) (pageSize - 1));
		int log = 0;
		while ((pageSize = pageSize >>> 1) != 0) {
			log++;
		}
		this.pageSizeBits = log;
		
		this.totalNumPages = getNumPages(memorySize);
		if (this.totalNumPages < 1) {
			throw new IllegalArgumentException("The given amount of memory amounted to less than one page.");
		}
		
		// initialize the free segments and allocated segments tracking structures
		this.freeSegments = new ArrayDeque<byte[]>(this.totalNumPages);
		this.allocatedSegments = new HashMap<AbstractInvokable, Set<DefaultMemorySegment>>();

		this.isPreAllocated = preAllocateMemory;
		
		if (preAllocateMemory) {
			// add the full chunks
			for (int i = 0; i < this.totalNumPages; i++) {
				// allocate memory of the specified size
				this.freeSegments.add(new byte[this.pageSize]);
			}
		}
		else {
			this.numNonAllocatedPages = this.totalNumPages;
		}
	}

	@Override
	public void shutdown() {
		// -------------------- BEGIN CRITICAL SECTION -------------------
		synchronized (this.lock)
		{
			if (!this.isShutDown) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Shutting down MemoryManager instance " + this);
				}
	
				// mark as shutdown and release memory
				this.isShutDown = true;
				
				this.freeSegments.clear();
				this.numNonAllocatedPages = 0;
				
				// go over all allocated segments and release them
				for (Set<DefaultMemorySegment> segments : this.allocatedSegments.values()) {
					for (DefaultMemorySegment seg : segments) {
						seg.destroy();
					}
				}
			}
		}
		// -------------------- END CRITICAL SECTION -------------------
	}

	@Override
	public boolean isShutdown() {
		return this.isShutDown;
	}

	@Override
	public boolean verifyEmpty() {
		synchronized (this.lock) {
			return isPreAllocated ?
					this.freeSegments.size() == this.totalNumPages :
					this.numNonAllocatedPages == this.totalNumPages;
		}
	}

	// ------------------------------------------------------------------------
	//                 MemoryManager interface implementation
	// ------------------------------------------------------------------------

	@Override
	public List<MemorySegment> allocatePages(AbstractInvokable owner, int numPages) throws MemoryAllocationException {
		final ArrayList<MemorySegment> segs = new ArrayList<MemorySegment>(numPages);
		allocatePages(owner, segs, numPages);
		return segs;
	}

	@Override
	public void allocatePages(AbstractInvokable owner, List<MemorySegment> target, int numPages)
			throws MemoryAllocationException
	{
		// sanity check
		if (owner == null) {
			throw new IllegalAccessError("The memory owner must not be null.");
		}
		
		// reserve array space, if applicable
		if (target instanceof ArrayList) {
			((ArrayList<MemorySegment>) target).ensureCapacity(numPages);
		}
		
		// -------------------- BEGIN CRITICAL SECTION -------------------
		synchronized (this.lock)
		{
			if (this.isShutDown) {
				throw new IllegalStateException("Memory manager has been shut down.");
			}
			
			// in the case of pre-allocated memory, the 'numNonAllocatedPages' is zero, in the
			// lazy case, the 'freeSegments.size()' is zero.
			if (numPages > (this.freeSegments.size() + numNonAllocatedPages)) {
				throw new MemoryAllocationException("Could not allocate " + numPages + " pages. Only " + 
					this.freeSegments.size() + " pages are remaining.");
			}
			
			Set<DefaultMemorySegment> segmentsForOwner = this.allocatedSegments.get(owner);
			if (segmentsForOwner == null) {
				segmentsForOwner = new HashSet<DefaultMemorySegment>(4 * numPages / 3 + 1);
				this.allocatedSegments.put(owner, segmentsForOwner);
			}
			
			if (isPreAllocated) {
				for (int i = numPages; i > 0; i--) {
					byte[] buffer = this.freeSegments.poll();
					final DefaultMemorySegment segment = new DefaultMemorySegment(owner, buffer);
					target.add(segment);
					segmentsForOwner.add(segment);
				}
			}
			else {
				for (int i = numPages; i > 0; i--) {
					byte[] buffer = new byte[pageSize];
					final DefaultMemorySegment segment = new DefaultMemorySegment(owner, buffer);
					target.add(segment);
					segmentsForOwner.add(segment);
				}
				numNonAllocatedPages -= numPages;
			}
		}
		// -------------------- END CRITICAL SECTION -------------------
	}
	
	// ------------------------------------------------------------------------

	@Override
	public void release(MemorySegment segment) {
		// check if segment is null or has already been freed
		if (segment == null || segment.isFreed() || !(segment instanceof DefaultMemorySegment)) {
			return;
		}
		
		final DefaultMemorySegment defSeg = (DefaultMemorySegment) segment;
		final AbstractInvokable owner = defSeg.owner;
		
		// -------------------- BEGIN CRITICAL SECTION -------------------
		synchronized (this.lock)
		{
			if (this.isShutDown) {
				throw new IllegalStateException("Memory manager has been shut down.");
			}

			// remove the reference in the map for the owner
			try {
				Set<DefaultMemorySegment> segsForOwner = this.allocatedSegments.get(owner);
				
				if (segsForOwner != null) {
					segsForOwner.remove(defSeg);
					if (segsForOwner.isEmpty()) {
						this.allocatedSegments.remove(owner);
					}
				}

				byte[] buffer = defSeg.destroy();
				
				if (isPreAllocated) {
					// release the memory in any case
					this.freeSegments.add(buffer);
				}
				else {
					numNonAllocatedPages++;
				}
			}
			catch (Throwable t) {
				throw new RuntimeException("Error removing book-keeping reference to allocated memory segment.", t);
			}
		}
		// -------------------- END CRITICAL SECTION -------------------
	}

	@Override
	public <T extends MemorySegment> void release(Collection<T> segments) {
		if (segments == null) {
			return;
		}
		
		// -------------------- BEGIN CRITICAL SECTION -------------------
		synchronized (this.lock)
		{
			if (this.isShutDown) {
				throw new IllegalStateException("Memory manager has been shut down.");
			}

			// since concurrent modifications to the collection
			// can disturb the release, we need to try potentially multiple times
			boolean successfullyReleased = false;
			do {
				final Iterator<T> segmentsIterator = segments.iterator();

				AbstractInvokable lastOwner = null;
				Set<DefaultMemorySegment> segsForOwner = null;

				try {
					// go over all segments
					while (segmentsIterator.hasNext()) {

						final MemorySegment seg = segmentsIterator.next();
						if (seg == null || seg.isFreed()) {
							continue;
						}

						final DefaultMemorySegment defSeg = (DefaultMemorySegment) seg;
						final AbstractInvokable owner = defSeg.owner;

						try {
							// get the list of segments by this owner only if it is a different owner than for
							// the previous one (or it is the first segment)
							if (lastOwner != owner) {
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

							// release the memory in any case
							byte[] buffer = defSeg.destroy();
							
							if (isPreAllocated) {
								this.freeSegments.add(buffer);
							}
							else {
								numNonAllocatedPages++;
							}
						}
						catch (Throwable t) {
							throw new RuntimeException(
									"Error removing book-keeping reference to allocated memory segment.", t);
						}
					}

					segments.clear();

					// the only way to exit the loop
					successfullyReleased = true;
				}
				catch (ConcurrentModificationException e) {
					// this may happen in the case where an asynchronous
					// call releases the memory. fall through the loop and try again
				}
			} while (!successfullyReleased);
		}
		// -------------------- END CRITICAL SECTION -------------------
	}

	@Override
	public void releaseAll(AbstractInvokable owner) {
		// -------------------- BEGIN CRITICAL SECTION -------------------
		synchronized (this.lock)
		{
			if (this.isShutDown) {
				throw new IllegalStateException("Memory manager has been shut down.");
			}

			// get all segments
			final Set<DefaultMemorySegment> segments = this.allocatedSegments.remove(owner);

			// all segments may have been freed previously individually
			if (segments == null || segments.isEmpty()) {
				return;
			}

			// free each segment
			if (isPreAllocated) {
				for (DefaultMemorySegment seg : segments) {
					final byte[] buffer = seg.destroy();
					this.freeSegments.add(buffer);
				}
			}
			else {
				for (DefaultMemorySegment seg : segments) {
					seg.destroy();
				}
				numNonAllocatedPages += segments.size();
			}

			segments.clear();
		}
		// -------------------- END CRITICAL SECTION -------------------
	}
	
	// ------------------------------------------------------------------------

	@Override
	public int getPageSize() {
		return this.pageSize;
	}

	@Override
	public long getMemorySize() {
		return this.memorySize;
	}

	@Override
	public int computeNumberOfPages(double fraction) {
		if (fraction <= 0 || fraction > 1) {
			throw new IllegalArgumentException("The fraction of memory to allocate must within (0, 1].");
		}

		return (int)(this.totalNumPages * fraction / this.numberOfSlots);
	}

	@Override
	public long computeMemorySize(double fraction) {
		return this.pageSize * computeNumberOfPages(fraction);
	}

	@Override
	public long roundDownToPageSizeMultiple(long numBytes) {
		return numBytes & this.roundingMask;
	}
	
	// ------------------------------------------------------------------------
	
	private int getNumPages(long numBytes) {
		if (numBytes < 0) {
			throw new IllegalArgumentException("The number of bytes to allocate must not be negative.");
		}
		
		final long numPages = numBytes >>> this.pageSizeBits;
		if (numPages <= Integer.MAX_VALUE) {
			return (int) numPages;
		} else {
			throw new IllegalArgumentException("The given number of bytes corresponds to more than MAX_INT pages.");
		}
	}
	
	// ------------------------------------------------------------------------
	
	private static final class DefaultMemorySegment extends MemorySegment {
		
		private AbstractInvokable owner;
		
		DefaultMemorySegment(AbstractInvokable owner, byte[] memory) {
			super(memory);
			this.owner = owner;
		}
		
		byte[] destroy() {
			final byte[] buffer = this.memory;
			this.memory = null;
			this.wrapper = null;
			return buffer;
		}
	}
}
