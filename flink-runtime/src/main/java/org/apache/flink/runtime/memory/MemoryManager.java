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

package org.apache.flink.runtime.memory;

import org.apache.flink.core.memory.HybridMemorySegment;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.util.MathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * The memory manager governs the memory that Flink uses for sorting, hashing, and caching. Memory
 * is represented in segments of equal size. Operators allocate the memory by requesting a number
 * of memory segments.
 *
 * <p>The memory may be represented as on-heap byte arrays or as off-heap memory regions
 * (both via {@link HybridMemorySegment}). Which kind of memory the MemoryManager serves can
 * be passed as an argument to the initialization.
 *
 * <p>The memory manager can either pre-allocate all memory, or allocate the memory on demand. In the
 * former version, memory will be occupied and reserved from start on, which means that no OutOfMemoryError
 * can come while requesting memory. Released memory will also return to the MemoryManager's pool.
 * On-demand allocation means that the memory manager only keeps track how many memory segments are
 * currently allocated (bookkeeping only). Releasing a memory segment will not add it back to the pool,
 * but make it re-claimable by the garbage collector.
 */
public class MemoryManager {

	private static final Logger LOG = LoggerFactory.getLogger(MemoryManager.class);
	/** The default memory page size. Currently set to 32 KiBytes. */
	public static final int DEFAULT_PAGE_SIZE = 32 * 1024;

	/** The minimal memory page size. Currently set to 4 KiBytes. */
	public static final int MIN_PAGE_SIZE = 4 * 1024;

	// ------------------------------------------------------------------------

	/** The lock used on the shared structures. */
	private final Object lock = new Object();

	/** The memory pool from which we draw memory segments. Specific to on-heap or off-heap memory */
	private final MemoryPool memoryPool;

	/** Memory segments allocated per memory owner. */
	private final HashMap<Object, Set<MemorySegment>> allocatedSegments;

	/** The type of memory governed by this memory manager. */
	private final MemoryType memoryType;

	/** Mask used to round down sizes to multiples of the page size. */
	private final long roundingMask;

	/** The size of the memory segments. */
	private final int pageSize;

	/** The initial total size, for verification. */
	private final int totalNumPages;

	/** The total size of the memory managed by this memory manager. */
	private final long memorySize;

	/** Number of slots of the task manager. */
	private final int numberOfSlots;

	/** Flag marking whether the memory manager immediately allocates the memory. */
	private final boolean isPreAllocated;

	/** The number of memory pages that have not been allocated and are available for lazy allocation. */
	private int numNonAllocatedPages;

	/** Flag whether the close() has already been invoked. */
	private boolean isShutDown;


	/**
	 * Creates a memory manager with the given capacity, using the default page size.
	 *
	 * @param memorySize The total size of the memory to be managed by this memory manager.
	 * @param numberOfSlots The number of slots of the task manager.
	 */
	public MemoryManager(long memorySize, int numberOfSlots) {
		this(memorySize, numberOfSlots, DEFAULT_PAGE_SIZE, MemoryType.HEAP, true);
	}

	/**
	 * Creates a memory manager with the given capacity and given page size.
	 *
	 * @param memorySize The total size of the memory to be managed by this memory manager.
	 * @param numberOfSlots The number of slots of the task manager.
	 * @param pageSize The size of the pages handed out by the memory manager.
	 * @param memoryType The type of memory (heap / off-heap) that the memory manager should allocate.
	 * @param preAllocateMemory True, if the memory manager should immediately allocate all memory, false
	 *                          if it should allocate and release the memory as needed.
	 */
	public MemoryManager(long memorySize, int numberOfSlots, int pageSize,
							MemoryType memoryType, boolean preAllocateMemory) {
		// sanity checks
		if (memoryType == null) {
			throw new NullPointerException();
		}
		if (memorySize <= 0) {
			throw new IllegalArgumentException("Size of total memory must be positive.");
		}
		if (pageSize < MIN_PAGE_SIZE) {
			throw new IllegalArgumentException("The page size must be at least " + MIN_PAGE_SIZE + " bytes.");
		}
		if (!MathUtils.isPowerOf2(pageSize)) {
			throw new IllegalArgumentException("The given page size is not a power of two.");
		}

		this.memoryType = memoryType;
		this.memorySize = memorySize;
		this.numberOfSlots = numberOfSlots;

		// assign page size and bit utilities
		this.pageSize = pageSize;
		this.roundingMask = ~((long) (pageSize - 1));

		final long numPagesLong = memorySize / pageSize;
		if (numPagesLong > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("The given number of memory bytes (" + memorySize
					+ ") corresponds to more than MAX_INT pages.");
		}
		this.totalNumPages = (int) numPagesLong;
		if (this.totalNumPages < 1) {
			throw new IllegalArgumentException("The given amount of memory amounted to less than one page.");
		}

		this.allocatedSegments = new HashMap<Object, Set<MemorySegment>>();
		this.isPreAllocated = preAllocateMemory;

		this.numNonAllocatedPages = preAllocateMemory ? 0 : this.totalNumPages;
		final int memToAllocate = preAllocateMemory ? this.totalNumPages : 0;

		switch (memoryType) {
			case HEAP:
				this.memoryPool = new HybridHeapMemoryPool(memToAllocate, pageSize);
				break;
			case OFF_HEAP:
				if (!preAllocateMemory) {
					LOG.warn("It is advisable to set 'taskmanager.memory.preallocate' to true when" +
						" the memory type 'taskmanager.memory.off-heap' is set to true.");
				}
				this.memoryPool = new HybridOffHeapMemoryPool(memToAllocate, pageSize);
				break;
			default:
				throw new IllegalArgumentException("unrecognized memory type: " + memoryType);
		}
	}

	// ------------------------------------------------------------------------
	//  Shutdown
	// ------------------------------------------------------------------------

	/**
	 * Shuts the memory manager down, trying to release all the memory it managed. Depending
	 * on implementation details, the memory does not necessarily become reclaimable by the
	 * garbage collector, because there might still be references to allocated segments in the
	 * code that allocated them from the memory manager.
	 */
	public void shutdown() {
		// -------------------- BEGIN CRITICAL SECTION -------------------
		synchronized (lock) {
			if (!isShutDown) {
				// mark as shutdown and release memory
				isShutDown = true;
				numNonAllocatedPages = 0;

				// go over all allocated segments and release them
				for (Set<MemorySegment> segments : allocatedSegments.values()) {
					for (MemorySegment seg : segments) {
						seg.free();
					}
				}

				memoryPool.clear();
			}
		}
		// -------------------- END CRITICAL SECTION -------------------
	}

	/**
	 * Checks whether the MemoryManager has been shut down.
	 *
	 * @return True, if the memory manager is shut down, false otherwise.
	 */
	public boolean isShutdown() {
		return isShutDown;
	}

	/**
	 * Checks if the memory manager all memory available.
	 *
	 * @return True, if the memory manager is empty and valid, false if it is not empty or corrupted.
	 */
	public boolean verifyEmpty() {
		synchronized (lock) {
			return isPreAllocated ?
					memoryPool.getNumberOfAvailableMemorySegments() == totalNumPages :
					numNonAllocatedPages == totalNumPages;
		}
	}

	// ------------------------------------------------------------------------
	//  Memory allocation and release
	// ------------------------------------------------------------------------

	/**
	 * Allocates a set of memory segments from this memory manager. If the memory manager pre-allocated the
	 * segments, they will be taken from the pool of memory segments. Otherwise, they will be allocated
	 * as part of this call.
	 *
	 * @param owner The owner to associate with the memory segment, for the fallback release.
	 * @param numPages The number of pages to allocate.
	 * @return A list with the memory segments.
	 * @throws MemoryAllocationException Thrown, if this memory manager does not have the requested amount
	 *                                   of memory pages any more.
	 */
	public List<MemorySegment> allocatePages(Object owner, int numPages) throws MemoryAllocationException {
		final ArrayList<MemorySegment> segs = new ArrayList<MemorySegment>(numPages);
		allocatePages(owner, segs, numPages);
		return segs;
	}

	/**
	 * Allocates a set of memory segments from this memory manager. If the memory manager pre-allocated the
	 * segments, they will be taken from the pool of memory segments. Otherwise, they will be allocated
	 * as part of this call.
	 *
	 * @param owner The owner to associate with the memory segment, for the fallback release.
	 * @param target The list into which to put the allocated memory pages.
	 * @param numPages The number of pages to allocate.
	 * @throws MemoryAllocationException Thrown, if this memory manager does not have the requested amount
	 *                                   of memory pages any more.
	 */
	public void allocatePages(Object owner, List<MemorySegment> target, int numPages)
			throws MemoryAllocationException {
		// sanity check
		if (owner == null) {
			throw new IllegalArgumentException("The memory owner must not be null.");
		}

		// reserve array space, if applicable
		if (target instanceof ArrayList) {
			((ArrayList<MemorySegment>) target).ensureCapacity(numPages);
		}

		// -------------------- BEGIN CRITICAL SECTION -------------------
		synchronized (lock) {
			if (isShutDown) {
				throw new IllegalStateException("Memory manager has been shut down.");
			}

			// in the case of pre-allocated memory, the 'numNonAllocatedPages' is zero, in the
			// lazy case, the 'freeSegments.size()' is zero.
			if (numPages > (memoryPool.getNumberOfAvailableMemorySegments() + numNonAllocatedPages)) {
				throw new MemoryAllocationException("Could not allocate " + numPages + " pages. Only " +
						(memoryPool.getNumberOfAvailableMemorySegments() + numNonAllocatedPages)
						+ " pages are remaining.");
			}

			Set<MemorySegment> segmentsForOwner = allocatedSegments.get(owner);
			if (segmentsForOwner == null) {
				segmentsForOwner = new HashSet<MemorySegment>(numPages);
				allocatedSegments.put(owner, segmentsForOwner);
			}

			if (isPreAllocated) {
				for (int i = numPages; i > 0; i--) {
					MemorySegment segment = memoryPool.requestSegmentFromPool(owner);
					target.add(segment);
					segmentsForOwner.add(segment);
				}
			}
			else {
				for (int i = numPages; i > 0; i--) {
					MemorySegment segment = memoryPool.allocateNewSegment(owner);
					target.add(segment);
					segmentsForOwner.add(segment);
				}
				numNonAllocatedPages -= numPages;
			}
		}
		// -------------------- END CRITICAL SECTION -------------------
	}

	/**
	 * Tries to release the memory for the specified segment. If the segment has already been released or
	 * is null, the request is simply ignored.
	 *
	 * <p>If the memory manager manages pre-allocated memory, the memory segment goes back to the memory pool.
	 * Otherwise, the segment is only freed and made eligible for reclamation by the GC.
	 *
	 * @param segment The segment to be released.
	 * @throws IllegalArgumentException Thrown, if the given segment is of an incompatible type.
	 */
	public void release(MemorySegment segment) {
		// check if segment is null or has already been freed
		if (segment == null || segment.getOwner() == null) {
			return;
		}

		final Object owner = segment.getOwner();

		// -------------------- BEGIN CRITICAL SECTION -------------------
		synchronized (lock) {
			// prevent double return to this memory manager
			if (segment.isFreed()) {
				return;
			}
			if (isShutDown) {
				throw new IllegalStateException("Memory manager has been shut down.");
			}

			// remove the reference in the map for the owner
			try {
				Set<MemorySegment> segsForOwner = this.allocatedSegments.get(owner);

				if (segsForOwner != null) {
					segsForOwner.remove(segment);
					if (segsForOwner.isEmpty()) {
						this.allocatedSegments.remove(owner);
					}
				}

				if (isPreAllocated) {
					// release the memory in any case
					memoryPool.returnSegmentToPool(segment);
				}
				else {
					segment.free();
					numNonAllocatedPages++;
				}
			}
			catch (Throwable t) {
				throw new RuntimeException("Error removing book-keeping reference to allocated memory segment.", t);
			}
		}
		// -------------------- END CRITICAL SECTION -------------------
	}

	/**
	 * Tries to release many memory segments together.
	 *
	 * <p>If the memory manager manages pre-allocated memory, the memory segment goes back to the memory pool.
	 * Otherwise, the segment is only freed and made eligible for reclamation by the GC.
	 *
	 * @param segments The segments to be released.
	 * @throws NullPointerException Thrown, if the given collection is null.
	 * @throws IllegalArgumentException Thrown, id the segments are of an incompatible type.
	 */
	public void release(Collection<MemorySegment> segments) {
		if (segments == null) {
			return;
		}

		// -------------------- BEGIN CRITICAL SECTION -------------------
		synchronized (lock) {
			if (isShutDown) {
				throw new IllegalStateException("Memory manager has been shut down.");
			}

			// since concurrent modifications to the collection
			// can disturb the release, we need to try potentially multiple times
			boolean successfullyReleased = false;
			do {
				final Iterator<MemorySegment> segmentsIterator = segments.iterator();

				Object lastOwner = null;
				Set<MemorySegment> segsForOwner = null;

				try {
					// go over all segments
					while (segmentsIterator.hasNext()) {

						final MemorySegment seg = segmentsIterator.next();
						if (seg == null || seg.isFreed()) {
							continue;
						}

						final Object owner = seg.getOwner();

						try {
							// get the list of segments by this owner only if it is a different owner than for
							// the previous one (or it is the first segment)
							if (lastOwner != owner) {
								lastOwner = owner;
								segsForOwner = this.allocatedSegments.get(owner);
							}

							// remove the segment from the list
							if (segsForOwner != null) {
								segsForOwner.remove(seg);
								if (segsForOwner.isEmpty()) {
									this.allocatedSegments.remove(owner);
								}
							}

							if (isPreAllocated) {
								memoryPool.returnSegmentToPool(seg);
							}
							else {
								seg.free();
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

	/**
	 * Releases all memory segments for the given owner.
	 *
	 * @param owner The owner memory segments are to be released.
	 */
	public void releaseAll(Object owner) {
		if (owner == null) {
			return;
		}

		// -------------------- BEGIN CRITICAL SECTION -------------------
		synchronized (lock) {
			if (isShutDown) {
				throw new IllegalStateException("Memory manager has been shut down.");
			}

			// get all segments
			final Set<MemorySegment> segments = allocatedSegments.remove(owner);

			// all segments may have been freed previously individually
			if (segments == null || segments.isEmpty()) {
				return;
			}

			// free each segment
			if (isPreAllocated) {
				for (MemorySegment seg : segments) {
					memoryPool.returnSegmentToPool(seg);
				}
			}
			else {
				for (MemorySegment seg : segments) {
					seg.free();
				}
				numNonAllocatedPages += segments.size();
			}

			segments.clear();
		}
		// -------------------- END CRITICAL SECTION -------------------
	}

	// ------------------------------------------------------------------------
	//  Properties, sizes and size conversions
	// ------------------------------------------------------------------------

	/**
	 * Gets the type of memory (heap / off-heap) managed by this memory manager.
	 *
	 * @return The type of memory managed by this memory manager.
	 */
	public MemoryType getMemoryType() {
		return memoryType;
	}

	/**
	 * Checks whether this memory manager pre-allocates the memory.
	 *
	 * @return True if the memory manager pre-allocates the memory, false if it allocates as needed.
	 */
	public boolean isPreAllocated() {
		return isPreAllocated;
	}

	/**
	 * Gets the size of the pages handled by the memory manager.
	 *
	 * @return The size of the pages handled by the memory manager.
	 */
	public int getPageSize() {
		return pageSize;
	}

	/**
	 * Returns the total size of memory handled by this memory manager.
	 *
	 * @return The total size of memory.
	 */
	public long getMemorySize() {
		return memorySize;
	}

	/**
	 * Gets the total number of memory pages managed by this memory manager.
	 *
	 * @return The total number of memory pages managed by this memory manager.
	 */
	public int getTotalNumPages() {
		return totalNumPages;
	}

	/**
	 * Computes to how many pages the given number of bytes corresponds. If the given number of bytes is not an
	 * exact multiple of a page size, the result is rounded down, such that a portion of the memory (smaller
	 * than the page size) is not included.
	 *
	 * @param fraction the fraction of the total memory per slot
	 * @return The number of pages to which
	 */
	public int computeNumberOfPages(double fraction) {
		if (fraction <= 0 || fraction > 1) {
			throw new IllegalArgumentException("The fraction of memory to allocate must within (0, 1].");
		}

		return (int) (totalNumPages * fraction / numberOfSlots);
	}

	/**
	 * Computes the memory size of the fraction per slot.
	 *
	 * @param fraction The fraction of the memory of the task slot.
	 * @return The number of pages corresponding to the memory fraction.
	 */
	public long computeMemorySize(double fraction) {
		return pageSize * (long) computeNumberOfPages(fraction);
	}

	/**
	 * Rounds the given value down to a multiple of the memory manager's page size.
	 *
	 * @return The given value, rounded down to a multiple of the page size.
	 */
	public long roundDownToPageSizeMultiple(long numBytes) {
		return numBytes & roundingMask;
	}


	// ------------------------------------------------------------------------
	//  Memory Pools
	// ------------------------------------------------------------------------

	abstract static class MemoryPool {

		abstract int getNumberOfAvailableMemorySegments();

		abstract MemorySegment allocateNewSegment(Object owner);

		abstract MemorySegment requestSegmentFromPool(Object owner);

		abstract void returnSegmentToPool(MemorySegment segment);

		abstract void clear();
	}

	static final class HybridHeapMemoryPool extends MemoryPool {

		/** The collection of available memory segments. */
		private final ArrayDeque<byte[]> availableMemory;

		private final int segmentSize;

		HybridHeapMemoryPool(int numInitialSegments, int segmentSize) {
			this.availableMemory = new ArrayDeque<>(numInitialSegments);
			this.segmentSize = segmentSize;

			for (int i = 0; i < numInitialSegments; i++) {
				this.availableMemory.add(new byte[segmentSize]);
			}
		}

		@Override
		MemorySegment allocateNewSegment(Object owner) {
			return MemorySegmentFactory.allocateUnpooledSegment(segmentSize, owner);
		}

		@Override
		MemorySegment requestSegmentFromPool(Object owner) {
			byte[] buf = availableMemory.remove();
			return  MemorySegmentFactory.wrapPooledHeapMemory(buf, owner);
		}

		@Override
		void returnSegmentToPool(MemorySegment segment) {
			if (segment.getClass() == HybridMemorySegment.class) {
				HybridMemorySegment heapSegment = (HybridMemorySegment) segment;
				availableMemory.add(heapSegment.getArray());
				heapSegment.free();
			}
			else {
				throw new IllegalArgumentException("Memory segment is not a " + HybridMemorySegment.class.getSimpleName());
			}
		}

		@Override
		protected int getNumberOfAvailableMemorySegments() {
			return availableMemory.size();
		}

		@Override
		void clear() {
			availableMemory.clear();
		}
	}

	static final class HybridOffHeapMemoryPool extends MemoryPool {

		/** The collection of available memory segments. */
		private final ArrayDeque<ByteBuffer> availableMemory;

		private final int segmentSize;

		HybridOffHeapMemoryPool(int numInitialSegments, int segmentSize) {
			this.availableMemory = new ArrayDeque<>(numInitialSegments);
			this.segmentSize = segmentSize;

			for (int i = 0; i < numInitialSegments; i++) {
				this.availableMemory.add(ByteBuffer.allocateDirect(segmentSize));
			}
		}

		@Override
		MemorySegment allocateNewSegment(Object owner) {
			ByteBuffer memory = ByteBuffer.allocateDirect(segmentSize);
			return MemorySegmentFactory.wrapPooledOffHeapMemory(memory, owner);
		}

		@Override
		MemorySegment requestSegmentFromPool(Object owner) {
			ByteBuffer buf = availableMemory.remove();
			return MemorySegmentFactory.wrapPooledOffHeapMemory(buf, owner);
		}

		@Override
		void returnSegmentToPool(MemorySegment segment) {
			if (segment.getClass() == HybridMemorySegment.class) {
				HybridMemorySegment hybridSegment = (HybridMemorySegment) segment;
				ByteBuffer buf = hybridSegment.getOffHeapBuffer();
				availableMemory.add(buf);
				hybridSegment.free();
			}
			else {
				throw new IllegalArgumentException("Memory segment is not a " + HybridMemorySegment.class.getSimpleName());
			}
		}

		@Override
		protected int getNumberOfAvailableMemorySegments() {
			return availableMemory.size();
		}

		@Override
		void clear() {
			availableMemory.clear();
		}
	}
}
