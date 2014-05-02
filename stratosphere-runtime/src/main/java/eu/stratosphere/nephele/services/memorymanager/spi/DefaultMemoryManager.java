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

package eu.stratosphere.nephele.services.memorymanager.spi;


import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;


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
	 * The Log.
	 */
	private static final Log LOG = LogFactory.getLog(DefaultMemoryManager.class);
	
	// --------------------------------------------------------------------------------------------
	
	private final Object lock = new Object();	 	// The lock used on the shared structures.
	
	private final ArrayDeque<byte[]> freeSegments;	// the free memory segments
	
	private final HashMap<AbstractInvokable, Set<DefaultMemorySegment>> allocatedSegments;
	
	private final long roundingMask;		// mask used to round down sizes to multiples of the page size
	
	private final int pageSize;				// the page size, in bytes
	
	private final int pageSizeBits;			// the number of bits that the power-of-two page size corresponds to
	
	private final int totalNumPages;		// The initial total size, for verification.
	
	private boolean isShutDown;				// flag whether the close() has already been invoked.

	// ------------------------------------------------------------------------
	// Constructors / Destructors
	// ------------------------------------------------------------------------

	/**
	 * Creates a memory manager with the given capacity, using the default page size.
	 * 
	 * @param memorySize The total size of the memory to be managed by this memory manager.
	 */
	public DefaultMemoryManager(long memorySize) {
		this(memorySize, DEFAULT_PAGE_SIZE);
	}

	/**
	 * Creates a memory manager with the given capacity and given page size.
	 * 
	 * @param memorySize The total size of the memory to be managed by this memory manager.
	 * @param pageSize The size of the pages handed out by the memory manager.
	 */
	public DefaultMemoryManager(long memorySize, int pageSize) {
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

		
		// add the full chunks
		for (int i = 0; i < this.totalNumPages; i++) {
			// allocate memory of the specified size
			this.freeSegments.add(new byte[this.pageSize]);
		}
	}


	@Override
	public void shutdown() {
		// -------------------- BEGIN CRITICAL SECTION -------------------
		synchronized (this.lock)
		{
			if (!this.isShutDown) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Shutting down MemoryManager instance " + toString());
				}
	
				// mark as shutdown and release memory
				this.isShutDown = true;
				this.freeSegments.clear();
				
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
	

	public boolean verifyEmpty() {
		synchronized (this.lock) {
			return this.freeSegments.size() == this.totalNumPages;
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
			
			if (numPages > this.freeSegments.size()) {
				throw new MemoryAllocationException("Could not allocate " + numPages + " pages. Only " + 
					this.freeSegments.size() + " pages are remaining.");
			}
			
			Set<DefaultMemorySegment> segmentsForOwner = this.allocatedSegments.get(owner);
			if (segmentsForOwner == null) {
				segmentsForOwner = new HashSet<DefaultMemorySegment>(4 * numPages / 3 + 1);
				this.allocatedSegments.put(owner, segmentsForOwner);
			}
			
			for (int i = numPages; i > 0; i--) {
				byte[] buffer = this.freeSegments.poll();
				final DefaultMemorySegment segment = new DefaultMemorySegment(owner, buffer);
				target.add(segment);
				segmentsForOwner.add(segment);
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
			}
			catch (Throwable t) {
				LOG.error("Error removing book-keeping reference to allocated memory segment.", t);
			}
			finally {
				// release the memory in any case
				byte[] buffer = defSeg.destroy();
				this.freeSegments.add(buffer);
			}
		}
		// -------------------- END CRITICAL SECTION -------------------
	}


	@Override
	public <T extends MemorySegment> void release(Collection<T> segments) {
		
		// sanity checks
		if (segments == null) {
			return;
		}
		
		// -------------------- BEGIN CRITICAL SECTION -------------------
		synchronized (this.lock)
		{
			if (this.isShutDown) {
				throw new IllegalStateException("Memory manager has been shut down.");
			}

			final Iterator<T> segmentsIterator = segments.iterator();
			
			AbstractInvokable lastOwner = null;
			Set<DefaultMemorySegment> segsForOwner = null;

			// go over all segments
			while (segmentsIterator.hasNext()) {
				
				final MemorySegment seg = segmentsIterator.next();
				if (seg.isFreed()) {
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
				}
				catch (Throwable t) {
					LOG.error("Error removing book-keeping reference to allocated memory segment.", t);
				}
				finally {
					// release the memory in any case
					byte[] buffer = defSeg.destroy();
					this.freeSegments.add(buffer);
				}
			}
			
			segments.clear();
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
			for (DefaultMemorySegment seg : segments) {
				final byte[] buffer = seg.destroy();
				this.freeSegments.add(buffer);
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
	public int computeNumberOfPages(long numBytes) {
		return getNumPages(numBytes);
	}

	@Override
	public long roundDownToPageSizeMultiple(long numBytes) {
		return numBytes & this.roundingMask;
	}
	
	// ------------------------------------------------------------------------
	
	private final int getNumPages(long numBytes) {
		if (numBytes < 0) {
			throw new IllegalArgumentException("The number of bytes to allocate must not be negative.");
		}
		
		final long numPages = numBytes >>> this.pageSizeBits;
		if (numPages <= Integer.MAX_VALUE) {
			return (int) numPages;
		} else {
			throw new IllegalArgumentException("The given number of bytes correstponds to more than MAX_INT pages.");
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
