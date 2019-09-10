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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.HybridMemorySegment;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.apache.flink.core.memory.MemorySegmentFactory.allocateUnpooledOffHeapMemory;
import static org.apache.flink.core.memory.MemorySegmentFactory.allocateUnpooledSegment;

/**
 * The memory manager governs the memory that Flink uses for sorting, hashing, and caching. Memory
 * is represented in segments of equal size. Operators allocate the memory by requesting a number
 * of memory segments.
 *
 * <p>The memory may be represented as on-heap byte arrays or as off-heap memory regions
 * (both via {@link HybridMemorySegment}). Which kind of memory the MemoryManager serves can
 * be passed as an argument to the initialization. Releasing a memory segment will make it re-claimable
 * by the garbage collector.
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

	/** Memory segments allocated per memory owner. */
	private final HashMap<Object, Set<MemorySegment>> allocatedSegments;

	/** The size of the memory segments. */
	private final int pageSize;

	/** The initial total size, for verification. */
	private final int totalNumPages;

	/** The total size of the memory managed by this memory manager. */
	private final long memorySize;

	/** Number of slots of the task manager. */
	private final int numberOfSlots;

	/** Type of the managed memory. */
	private final MemoryType memoryType;

	/** The number of memory pages that have not been allocated and are available for lazy allocation. */
	private int numNonAllocatedPages;

	/** Flag whether the close() has already been invoked. */
	private boolean isShutDown;

	/**
	 * Creates a memory manager with the given capacity and given page size.
	 *
	 * @param memorySize The total size of the memory to be managed by this memory manager.
	 * @param numberOfSlots The number of slots of the task manager.
	 * @param pageSize The size of the pages handed out by the memory manager.
	 * @param memoryType The type of memory (heap / off-heap) that the memory manager should allocate.
	 */
	public MemoryManager(
			long memorySize,
			int numberOfSlots,
			int pageSize,
			MemoryType memoryType) {
		sanityCheck(memorySize, pageSize, memoryType);

		this.allocatedSegments = new HashMap<>();
		this.memorySize = memorySize;
		this.numberOfSlots = numberOfSlots;
		this.pageSize = pageSize;
		this.totalNumPages = calculateTotalNumberOfPages(memorySize, pageSize);
		this.numNonAllocatedPages = this.totalNumPages;
		this.memoryType = memoryType;

		LOG.debug("Initialized MemoryManager with total memory size {}, number of slots {}, page size {}, " +
				"memory type {} and number of non allocated pages {}.",
			memorySize,
			numberOfSlots,
			pageSize,
			memoryType,
			numNonAllocatedPages);
	}

	private static void sanityCheck(long memorySize, int pageSize, MemoryType memoryType) {
		Preconditions.checkNotNull(memoryType);
		Preconditions.checkArgument(memorySize > 0L, "Size of total memory must be positive.");
		Preconditions.checkArgument(
			pageSize >= MIN_PAGE_SIZE,
			"The page size must be at least %d bytes.", MIN_PAGE_SIZE);
		Preconditions.checkArgument(
			MathUtils.isPowerOf2(pageSize),
			"The given page size is not a power of two.");
	}

	private static int calculateTotalNumberOfPages(long memorySize, int pageSize) {
		long numPagesLong = memorySize / pageSize;
		Preconditions.checkArgument(
			numPagesLong <= Integer.MAX_VALUE,
			"The given number of memory bytes (%s) corresponds to more than MAX_INT pages.", memorySize);

		@SuppressWarnings("NumericCastThatLosesPrecision")
		int totalNumPages = (int) numPagesLong;
		Preconditions.checkArgument(totalNumPages >= 1, "The given amount of memory amounted to less than one page.");

		return totalNumPages;
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
			}
		}
		// -------------------- END CRITICAL SECTION -------------------
	}

	/**
	 * Checks whether the MemoryManager has been shut down.
	 *
	 * @return True, if the memory manager is shut down, false otherwise.
	 */
	@VisibleForTesting
	public boolean isShutdown() {
		return isShutDown;
	}

	/**
	 * Checks if the memory manager all memory available.
	 *
	 * @return True, if the memory manager is empty and valid, false if it is not empty or corrupted.
	 */
	@VisibleForTesting
	public boolean verifyEmpty() {
		synchronized (lock) {
			return numNonAllocatedPages == totalNumPages;
		}
	}

	// ------------------------------------------------------------------------
	//  Memory allocation and release
	// ------------------------------------------------------------------------

	/**
	 * Allocates a set of memory segments from this memory manager.
	 *
	 * @param owner The owner to associate with the memory segment, for the fallback release.
	 * @param numPages The number of pages to allocate.
	 * @return A list with the memory segments.
	 * @throws MemoryAllocationException Thrown, if this memory manager does not have the requested amount
	 *                                   of memory pages any more.
	 */
	public List<MemorySegment> allocatePages(Object owner, int numPages) throws MemoryAllocationException {
		List<MemorySegment> segments = new ArrayList<>(numPages);
		allocatePages(owner, segments, numPages);
		return segments;
	}

	/**
	 * Allocates a set of memory segments from this memory manager.
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

			if (numPages > numNonAllocatedPages) {
				throw new MemoryAllocationException(
					String.format("Could not allocate %d pages. Only %d pages are remaining.", numPages, numNonAllocatedPages));
			}

			Set<MemorySegment> segmentsForOwner = allocatedSegments.get(owner);
			if (segmentsForOwner == null) {
				segmentsForOwner = new HashSet<MemorySegment>(numPages);
				allocatedSegments.put(owner, segmentsForOwner);
			}

			for (int i = numPages; i > 0; i--) {
				MemorySegment segment = allocateManagedSegment(memoryType, owner);
				target.add(segment);
				segmentsForOwner.add(segment);
			}
			numNonAllocatedPages -= numPages;
		}
		// -------------------- END CRITICAL SECTION -------------------
	}

	/**
	 * Tries to release the memory for the specified segment.
	 *
	 * <p>If the segment has already been released or is null, the request is simply ignored.
	 * The segment is only freed and made eligible for reclamation by the GC.
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

				segment.free();
				numNonAllocatedPages++;
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
	 * <p>The segment is only freed and made eligible for reclamation by the GC.
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

							seg.free();
							numNonAllocatedPages++;
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
				catch (ConcurrentModificationException | NoSuchElementException e) {
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
			for (MemorySegment seg : segments) {
				seg.free();
			}
			numNonAllocatedPages += segments.size();

			segments.clear();
		}
		// -------------------- END CRITICAL SECTION -------------------
	}

	// ------------------------------------------------------------------------
	//  Properties, sizes and size conversions
	// ------------------------------------------------------------------------

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

		//noinspection NumericCastThatLosesPrecision
		return (int) (totalNumPages * fraction / numberOfSlots);
	}

	private MemorySegment allocateManagedSegment(MemoryType memoryType, Object owner) {
		switch (memoryType) {
			case HEAP:
				return allocateUnpooledSegment(pageSize, owner);
			case OFF_HEAP:
				return allocateUnpooledOffHeapMemory(pageSize, owner);
			default:
				throw new IllegalArgumentException("unrecognized memory type: " + memoryType);
		}
	}
}
