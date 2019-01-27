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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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

import static org.apache.flink.util.Preconditions.checkNotNull;

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

	/** The wrapper of core memory segments allocated per memory owner. */
	private final AllocatedSegmentsWrapper allocatedCoreSegments;

	/** The wrapper of floating memory segments allocated per memory owner. */
	private final AllocatedSegmentsWrapper allocatedFloatingSegments;

	/** The size of the memory segments. */
	private final int pageSize;

	/** The initial total size, for verification. */
	private final int totalNumPages;

	/** The total size of the core memory managed by this memory manager. */
	private final long coreMemorySize;

	/** The total size of the floating memory managed by this memory manager. */
	private final long floatingMemorySize;

	/** Number of slots of the task manager. */
	private final int numberOfSlots;

	/** Flag marking whether the memory manager immediately allocates the memory. */
	private final boolean isPreAllocated;

	/** Flag whether the close() has already been invoked. */
	private boolean isShutDown;

	/**
	 * Creates a memory manager with the given core capacity, using the default page size.
	 *
	 * @param coreMemorySize The total size of the core memory to be managed by this memory manager.
	 * @param numberOfSlots The number of slots of the task manager.
	 */
	public MemoryManager(long coreMemorySize, int numberOfSlots) {
		this(coreMemorySize, 0, numberOfSlots, DEFAULT_PAGE_SIZE, MemoryType.HEAP, true);
	}

	/**
	 * Creates a memory manager with the given core capacity and given page size.
	 *
	 * @param coreMemorySize The total size of the memory to be managed by this memory manager.
	 * @param numberOfSlots The number of slots of the task manager.
	 * @param pageSize The size of the pages handed out by the memory manager.
	 * @param memoryType The type of memory (heap / off-heap) that the memory manager should allocate.
	 * @param preAllocateMemory True, if the memory manager should immediately allocate all memory, false
	 *                          if it should allocate and release the memory as needed.
	 */
	public MemoryManager(
			long coreMemorySize,
			int numberOfSlots,
			int pageSize,
			MemoryType memoryType,
			boolean preAllocateMemory) {
		this(coreMemorySize, 0, numberOfSlots, pageSize, memoryType, preAllocateMemory);
	}

	/**
	 * Creates a memory manager with the given core & floating capacities and given page size.
	 *
	 * @param coreMemorySize The total size of the core memory to be managed by this memory manager.
	 * @param floatingMemorySize The total size of the floating memory to be managed by this memory manager.
	 * @param numberOfSlots The number of slots of the task manager.
	 * @param pageSize The size of the pages handed out by the memory manager.
	 * @param memoryType The type of memory (heap / off-heap) that the memory manager should allocate.
	 * @param preAllocateMemory True, if the memory manager should immediately allocate all memory, false
	 *                          if it should allocate and release the memory as needed.
	 */
	public MemoryManager(
			long coreMemorySize,
			long floatingMemorySize,
			int numberOfSlots,
			int pageSize,
			MemoryType memoryType,
			boolean preAllocateMemory) {
		// sanity checks
		if (memoryType == null) {
			throw new NullPointerException();
		}
		if (coreMemorySize <= 0) {
			throw new IllegalArgumentException("Size of total core memory must be positive.");
		}
		if (floatingMemorySize < 0) {
			throw new IllegalArgumentException("Size of total floating memory must not be negative.");
		}
		if (pageSize < MIN_PAGE_SIZE) {
			throw new IllegalArgumentException("The page size must be at least " + MIN_PAGE_SIZE + " bytes.");
		}
		if (!MathUtils.isPowerOf2(pageSize)) {
			throw new IllegalArgumentException("The given page size is not a power of two.");
		}

		this.coreMemorySize = coreMemorySize;
		this.floatingMemorySize = floatingMemorySize;
		this.numberOfSlots = numberOfSlots;

		this.pageSize = pageSize;

		int numCorePages = calculateNumPages(coreMemorySize);
		int numFloatingPages = floatingMemorySize > 0 ? calculateNumPages(floatingMemorySize) : 0;
		this.totalNumPages = numCorePages + numFloatingPages;

		this.allocatedCoreSegments = new AllocatedSegmentsWrapper(numCorePages);
		this.allocatedFloatingSegments = new AllocatedSegmentsWrapper(numFloatingPages);

		this.isPreAllocated = preAllocateMemory;

		final int memToAllocate = preAllocateMemory ?  this.totalNumPages : 0;

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

	private int calculateNumPages(long memorySize) {
		final long numPagesLong = memorySize / pageSize;
		if (numPagesLong > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("The given number of memory bytes (" + memorySize
				+ ") corresponds to more than MAX_INT pages.");
		}
		int totalNumPages = (int) numPagesLong;
		if (totalNumPages < 1) {
			throw new IllegalArgumentException("The given amount of core memory amounted to less than one page.");
		}
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
		synchronized (lock) {
			if (!isShutDown) {
				// mark as shutdown and release memory
				isShutDown = true;

				// go over all allocated segments and release them
				allocatedCoreSegments.freeAllocatedSegments();
				allocatedFloatingSegments.freeAllocatedSegments();

				memoryPool.clear();
			}
		}
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
			int numTotalAvailablePages = allocatedCoreSegments.getNumAvailablePages() +
				allocatedFloatingSegments.getNumAvailablePages();
			return isPreAllocated ?
				memoryPool.getNumberOfAvailableMemorySegments() == totalNumPages &&
					numTotalAvailablePages == totalNumPages : numTotalAvailablePages == totalNumPages;
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
		final ArrayList<MemorySegment> segments = new ArrayList<>(numPages);
		allocatePages(owner, segments, numPages);
		return segments;
	}

	public void allocatePages(Object owner, List<MemorySegment> target, int numPages) throws MemoryAllocationException {
		allocatePages(owner, target, numPages, true);
	}

	public List<MemorySegment> allocatePages(Object owner, int numPages, boolean isCoreSegment) throws MemoryAllocationException {
		final ArrayList<MemorySegment> segments = new ArrayList<>(numPages);
		allocatePages(owner, segments, numPages, isCoreSegment);
		return segments;
	}

	/**
	 * Allocates a set of memory segments from this memory manager. If the memory manager pre-allocated the
	 * segments, they will be taken from the pool of memory segments. Otherwise, they will be allocated
	 * as part of this call.
	 *
	 * @param owner The owner to associate with the memory segment, for the fallback release.
	 * @param target The list into which to put the allocated memory pages.
	 * @param numPages The number of pages to allocate.
	 * @param isCoreSegment Whether to allocate core pages or not.
	 * @throws MemoryAllocationException Thrown, if this memory manager does not have the requested amount
	 *                                   of memory pages any more.
	 */
	public void allocatePages(Object owner, List<MemorySegment> target, int numPages, boolean isCoreSegment)
			throws MemoryAllocationException {
		// sanity check
		if (owner == null) {
			throw new IllegalArgumentException("The memory owner must not be null.");
		}

		// reserve array space, if applicable
		if (target instanceof ArrayList) {
			((ArrayList<MemorySegment>) target).ensureCapacity(numPages);
		}

		synchronized (lock) {
			if (isShutDown) {
				throw new IllegalStateException("Memory manager has been shut down.");
			}

			AllocatedSegmentsWrapper allocatedSegments = isCoreSegment ?
				allocatedCoreSegments : allocatedFloatingSegments;
			Set<MemorySegment> segmentsForOwner = allocatedSegments.allocateSegments(owner, numPages);

			if (isPreAllocated) {
				for (int i = numPages; i > 0; i--) {
					MemorySegment segment = memoryPool.requestSegmentFromPool(owner);
					target.add(segment);
					segmentsForOwner.add(segment);
				}
			} else {
				for (int i = numPages; i > 0; i--) {
					MemorySegment segment = memoryPool.allocateNewSegment(owner);
					target.add(segment);
					segmentsForOwner.add(segment);
				}
			}
		}
	}

	public void release(@Nullable MemorySegment segment) {
		release(segment, true);
	}

	/**
	 * Tries to release the memory for the specified segment. If the segment has already been released or
	 * is null, the request is simply ignored.
	 *
	 * <p>If the memory manager manages pre-allocated memory, the memory segment goes back to the memory pool.
	 * Otherwise, the segment is only freed and made eligible for reclamation by the GC.
	 *
	 * @param segment The segment to be released.
	 * @param isCoreSegment Whether the released segment is core or not.
	 * @return Whether the segment has been released.
	 * @throws IllegalArgumentException Thrown, if the given segment is of an incompatible type.
	 */
	public boolean release(@Nullable MemorySegment segment, boolean isCoreSegment) {
		// check if segment is null or has already been freed
		if (segment == null || segment.getOwner() == null) {
			return false;
		}

		synchronized (lock) {
			// prevent double return to this memory manager
			if (segment.isFreed()) {
				return false;
			}
			if (isShutDown) {
				throw new IllegalStateException("Memory manager has been shut down.");
			}

			// remove the reference in the map for the owner
			AllocatedSegmentsWrapper allocatedSegments = isCoreSegment ?
				allocatedCoreSegments : allocatedFloatingSegments;
			boolean hasRemoved = allocatedSegments.removeAllocatedSegments(segment);

			if (hasRemoved) {
				releaseMemorySegment(segment);
			}

			return hasRemoved;
		}
	}

	public void release(@Nullable Collection<MemorySegment> segments) {
		release(segments, true);
	}

	/**
	 * Tries to release many memory segments together.
	 *
	 * <p>If the memory manager manages pre-allocated memory, the memory segment goes back to the memory pool.
	 * Otherwise, the segment is only freed and made eligible for reclamation by the GC.
	 *
	 * @param segments The segments to be released.
	 * @param isCoreSegment Whether the released segments are core or not.
	 * @throws NullPointerException Thrown, if the given collection is null.
	 * @throws IllegalArgumentException Thrown, id the segments are of an incompatible type.
	 */
	public void release(@Nullable Collection<MemorySegment> segments, boolean isCoreSegment) {
		if (segments == null) {
			return;
		}

		List<MemorySegment> notReleased = new ArrayList<>();

		synchronized (lock) {
			if (isShutDown) {
				throw new IllegalStateException("Memory manager has been shut down.");
			}

			// since concurrent modifications to the collection
			// can disturb the release, we need to try potentially multiple times
			boolean successfullyReleased = false;
			do {
				final Iterator<MemorySegment> segmentsIterator = segments.iterator();
				AllocatedSegmentsWrapper allocatedSegments = isCoreSegment ?
					allocatedCoreSegments : allocatedFloatingSegments;

				try {
					// go over all segments
					while (segmentsIterator.hasNext()) {
						final MemorySegment seg = segmentsIterator.next();
						if (seg == null || seg.isFreed()) {
							continue;
						}

						boolean hasRemoved = allocatedSegments.removeAllocatedSegments(seg);

						if (hasRemoved) {
							releaseMemorySegment(seg);
						} else {
							notReleased.add(seg);
						}
					}

					segments.clear();
					segments.addAll(notReleased);

					// the only way to exit the loop
					successfullyReleased = true;
				} catch (ConcurrentModificationException e) {
					// this may happen in the case where an asynchronous
					// call releases the memory. fall through the loop and try again
				}
			} while (!successfullyReleased);
		}
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

		synchronized (lock) {
			if (isShutDown) {
				throw new IllegalStateException("Memory manager has been shut down.");
			}

			releaseMemorySegments(allocatedCoreSegments.removeAllocatedSegments(owner));
			releaseMemorySegments(allocatedFloatingSegments.removeAllocatedSegments(owner));
		}
	}

	private void releaseMemorySegments(Set<MemorySegment> segments) {
		if (segments == null || segments.isEmpty()) {
			return;
		}

		// free each segment
		if (isPreAllocated) {
			for (MemorySegment seg : segments) {
				memoryPool.returnSegmentToPool(seg);
			}
		} else {
			for (MemorySegment seg : segments) {
				seg.free();
			}
		}

		segments.clear();
	}

	private void releaseMemorySegment(MemorySegment seg) {
		if (isPreAllocated) {
			memoryPool.returnSegmentToPool(seg);
		} else {
			seg.free();
		}
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
		return coreMemorySize + floatingMemorySize;
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
	 * Manages the allocated segments for each memory owner.
	 */
	private static class AllocatedSegmentsWrapper {

		/** Memory segments allocated per memory owner. */
		private final HashMap<Object, Set<MemorySegment>> allocatedSegments;

		/** The number of available segments, for verification. */
		private int numAvailablePages;

		AllocatedSegmentsWrapper(int numPages) {
			this.numAvailablePages = numPages;
			this.allocatedSegments = new HashMap<>();
		}

		/**
		 * Allocates a number of pages for the specific owner.
		 *
		 * @param owner The memory owner.
		 * @param numRequiredPages How many pages are required for this owner.
		 * @return A set of allocated memory segments for this owner.
		 */
		@Nonnull
		Set<MemorySegment> allocateSegments(Object owner, int numRequiredPages) throws MemoryAllocationException {
			if (numRequiredPages > numAvailablePages) {
				throw new MemoryAllocationException("Could not allocate " + numRequiredPages + " pages. Only " + numAvailablePages
					+ " pages are remaining.");
			}

			Set<MemorySegment> segmentsForOwner = allocatedSegments.get(owner);
			if (segmentsForOwner == null) {
				segmentsForOwner = new HashSet<>(numRequiredPages);
				allocatedSegments.put(owner, segmentsForOwner);
			}
			numAvailablePages -= numRequiredPages;
			return segmentsForOwner;
		}

		/**
		 * Removes a specific segment from the allocated segments.
		 *
		 * @param segment The memory segment to be released.
		 * @return Whether the segment has been released.
		 */
		boolean removeAllocatedSegments(MemorySegment segment) {
			Object owner = checkNotNull(segment.getOwner());

			Set<MemorySegment> segmentsForOwner = allocatedSegments.get(owner);
			if (segmentsForOwner != null) {
				boolean hasRemoved = segmentsForOwner.remove(segment);

				if (hasRemoved) {
					if (segmentsForOwner.isEmpty()) {
						allocatedSegments.remove(owner);
					}
					numAvailablePages++;
				}

				return hasRemoved;
			}

			return false;
		}

		/**
		 * Releases all the segments for the specific memory owner from the allocated segments.
		 *
		 * @param owner The memory owner.
		 * @return All the memory segments for this owner.
		 */
		@Nullable
		Set<MemorySegment> removeAllocatedSegments(Object owner) {
			Set<MemorySegment> segmentsForOwner = allocatedSegments.remove(owner);
			if (segmentsForOwner != null) {
				numAvailablePages += segmentsForOwner.size();
			}
			return segmentsForOwner;
		}

		void freeAllocatedSegments() {
			for (Set<MemorySegment> segments : allocatedSegments.values()) {
				for (MemorySegment seg : segments) {
					seg.free();
				}
			}
			allocatedSegments.clear();
		}

		int getNumAvailablePages() {
			return numAvailablePages;
		}
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
