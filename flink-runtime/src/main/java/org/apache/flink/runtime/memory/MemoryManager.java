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
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.HybridMemorySegment;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.LongFunctionWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.apache.flink.core.memory.MemorySegmentFactory.allocateOffHeapUnsafeMemory;

/**
 * The memory manager governs the memory that Flink uses for sorting, hashing, caching or off-heap state backends
 * (e.g. RocksDB). Memory is represented either in {@link MemorySegment}s of equal size or in reserved chunks of certain
 * size. Operators allocate the memory either by requesting a number of memory segments or by reserving chunks.
 * Any allocated memory has to be released to be reused later.
 *
 * <p>The memory segments are represented as off-heap unsafe memory regions (both via {@link HybridMemorySegment}).
 * Releasing a memory segment will make it re-claimable by the garbage collector, but does not necessarily immediately
 * releases the underlying memory.
 */
public class MemoryManager {

	private static final Logger LOG = LoggerFactory.getLogger(MemoryManager.class);
	/** The default memory page size. Currently set to 32 KiBytes. */
	public static final int DEFAULT_PAGE_SIZE = 32 * 1024;

	/** The minimal memory page size. Currently set to 4 KiBytes. */
	public static final int MIN_PAGE_SIZE = 4 * 1024;

	// ------------------------------------------------------------------------

	/** Memory segments allocated per memory owner. */
	private final Map<Object, Set<MemorySegment>> allocatedSegments;

	/** Reserved memory per memory owner. */
	private final Map<Object, Long> reservedMemory;

	private final long pageSize;

	private final long totalNumberOfPages;

	private final UnsafeMemoryBudget memoryBudget;

	private final SharedResources sharedResources;

	/** Flag whether the close() has already been invoked. */
	private volatile boolean isShutDown;

	/**
	 * Creates a memory manager with the given capacity and given page size.
	 *
	 * @param memorySize The total size of the off-heap memory to be managed by this memory manager.
	 * @param pageSize The size of the pages handed out by the memory manager.
	 * @param verifyEmptyWaitGcMaxSleeps defines how long to wait for GC of all allocated memory to check for memory leaks,
	 *                                   see {@link UnsafeMemoryBudget} for details.
	 */
	MemoryManager(long memorySize, int pageSize, int verifyEmptyWaitGcMaxSleeps) {
		sanityCheck(memorySize, pageSize);

		this.pageSize = pageSize;
		this.memoryBudget = new UnsafeMemoryBudget(memorySize, verifyEmptyWaitGcMaxSleeps);
		this.totalNumberOfPages = memorySize / pageSize;
		this.allocatedSegments = new ConcurrentHashMap<>();
		this.reservedMemory = new ConcurrentHashMap<>();
		this.sharedResources = new SharedResources();
		verifyIntTotalNumberOfPages(memorySize, totalNumberOfPages);

		LOG.debug(
			"Initialized MemoryManager with total memory size {} and page size {}.",
			memorySize,
			pageSize);
	}

	private static void sanityCheck(long memorySize, int pageSize) {
		Preconditions.checkArgument(memorySize >= 0L, "Size of total memory must be non-negative.");
		Preconditions.checkArgument(
			pageSize >= MIN_PAGE_SIZE,
			"The page size must be at least %d bytes.", MIN_PAGE_SIZE);
		Preconditions.checkArgument(
			MathUtils.isPowerOf2(pageSize),
			"The given page size is not a power of two.");
	}

	private static void verifyIntTotalNumberOfPages(long memorySize, long numberOfPagesLong) {
		Preconditions.checkArgument(
			numberOfPagesLong <= Integer.MAX_VALUE,
			"The given number of memory bytes (%d) corresponds to more than MAX_INT pages (%d > %d).",
			memorySize,
			numberOfPagesLong,
			Integer.MAX_VALUE);
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
		if (!isShutDown) {
			// mark as shutdown and release memory
			isShutDown = true;
			reservedMemory.clear();

			// go over all allocated segments and release them
			for (Set<MemorySegment> segments : allocatedSegments.values()) {
				for (MemorySegment seg : segments) {
					seg.free();
				}
				segments.clear();
			}
			allocatedSegments.clear();
		}
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
	 * Checks if the memory manager's memory is completely available (nothing allocated at the moment).
	 *
	 * @return True, if the memory manager is empty and valid, false if it is not empty or corrupted.
	 */
	public boolean verifyEmpty() {
		return memoryBudget.verifyEmpty();
	}

	// ------------------------------------------------------------------------
	//  Memory allocation and release
	// ------------------------------------------------------------------------

	/**
	 * Allocates a set of memory segments from this memory manager.
	 *
	 * <p>The total allocated memory will not exceed its size limit, announced in the constructor.
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
	 * <p>The total allocated memory will not exceed its size limit, announced in the constructor.
	 *
	 * @param owner The owner to associate with the memory segment, for the fallback release.
	 * @param target The list into which to put the allocated memory pages.
	 * @param numberOfPages The number of pages to allocate.
	 * @throws MemoryAllocationException Thrown, if this memory manager does not have the requested amount
	 *                                   of memory pages any more.
	 */
	public void allocatePages(
			Object owner,
			Collection<MemorySegment> target,
			int numberOfPages) throws MemoryAllocationException {
		// sanity check
		Preconditions.checkNotNull(owner, "The memory owner must not be null.");
		Preconditions.checkState(!isShutDown, "Memory manager has been shut down.");
		Preconditions.checkArgument(
			numberOfPages <= totalNumberOfPages,
			"Cannot allocate more segments %d than the max number %d",
			numberOfPages,
			totalNumberOfPages);

		// reserve array space, if applicable
		if (target instanceof ArrayList) {
			((ArrayList<MemorySegment>) target).ensureCapacity(numberOfPages);
		}

		long memoryToReserve = numberOfPages * pageSize;
		try {
			memoryBudget.reserveMemory(memoryToReserve);
		} catch (MemoryReservationException e) {
			throw new MemoryAllocationException(String.format("Could not allocate %d pages", numberOfPages), e);
		}

		Runnable pageCleanup = this::releasePage;
		allocatedSegments.compute(owner, (o, currentSegmentsForOwner) -> {
			Set<MemorySegment> segmentsForOwner = currentSegmentsForOwner == null ?
				new HashSet<>(numberOfPages) : currentSegmentsForOwner;
			for (long i = numberOfPages; i > 0; i--) {
				MemorySegment segment = allocateOffHeapUnsafeMemory(getPageSize(), owner, pageCleanup);
				target.add(segment);
				segmentsForOwner.add(segment);
			}
			return segmentsForOwner;
		});

		Preconditions.checkState(!isShutDown, "Memory manager has been concurrently shut down.");
	}

	private void releasePage() {
		memoryBudget.releaseMemory(getPageSize());
	}

	/**
	 * Tries to release the memory for the specified segment.
	 *
	 * <p>If the segment has already been released, it is only freed. If it is null or has no owner, the request is simply ignored.
	 * The segment is only freed and made eligible for reclamation by the GC. The segment will be returned to
	 * the memory pool, increasing its available limit for the later allocations.
	 *
	 * @param segment The segment to be released.
	 */
	public void release(MemorySegment segment) {
		Preconditions.checkState(!isShutDown, "Memory manager has been shut down.");

		// check if segment is null or has already been freed
		if (segment == null || segment.getOwner() == null) {
			return;
		}

		// remove the reference in the map for the owner
		try {
			allocatedSegments.computeIfPresent(segment.getOwner(), (o, segsForOwner) -> {
				segment.free();
				segsForOwner.remove(segment);
				return segsForOwner.isEmpty() ? null : segsForOwner;
			});
		}
		catch (Throwable t) {
			throw new RuntimeException("Error removing book-keeping reference to allocated memory segment.", t);
		}
	}

	/**
	 * Tries to release many memory segments together.
	 *
	 * <p>The segment is only freed and made eligible for reclamation by the GC. Each segment will be returned to
	 * the memory pool, increasing its available limit for the later allocations.
	 *
	 * @param segments The segments to be released.
	 */
	public void release(Collection<MemorySegment> segments) {
		if (segments == null) {
			return;
		}

		Preconditions.checkState(!isShutDown, "Memory manager has been shut down.");

		// since concurrent modifications to the collection
		// can disturb the release, we need to try potentially multiple times
		boolean successfullyReleased = false;
		do {
			// We could just pre-sort the segments by owner and release them in a loop by owner.
			// It would simplify the code but require this additional step and memory for the sorted map of segments by owner.
			// Current approach is more complicated but it traverses the input segments only once w/o any additional buffer.
			// Later, we can check whether the simpler approach actually leads to any performance penalty and
			// if not, we can change it to the simpler approach for the better readability.
			Iterator<MemorySegment> segmentsIterator = segments.iterator();

			try {
				MemorySegment segment = null;
				while (segment == null && segmentsIterator.hasNext()) {
					segment = segmentsIterator.next();
				}
				while (segment != null) {
					segment = releaseSegmentsForOwnerUntilNextOwner(segment, segmentsIterator);
				}
				segments.clear();
				// the only way to exit the loop
				successfullyReleased = true;
			} catch (ConcurrentModificationException | NoSuchElementException e) {
				// this may happen in the case where an asynchronous
				// call releases the memory. fall through the loop and try again
			}
		} while (!successfullyReleased);
	}

	private MemorySegment releaseSegmentsForOwnerUntilNextOwner(
			MemorySegment firstSeg,
			Iterator<MemorySegment> segmentsIterator) {
		AtomicReference<MemorySegment> nextOwnerMemorySegment = new AtomicReference<>();
		Object owner = firstSeg.getOwner();
		allocatedSegments.compute(owner, (o, segsForOwner) -> {
			freeSegment(firstSeg, segsForOwner);
			while (segmentsIterator.hasNext()) {
				MemorySegment segment = segmentsIterator.next();
				try {
					if (segment == null || segment.isFreed()) {
						continue;
					}
					Object nextOwner = segment.getOwner();
					if (nextOwner != owner) {
						nextOwnerMemorySegment.set(segment);
						break;
					}
					freeSegment(segment, segsForOwner);
				} catch (Throwable t) {
					throw new RuntimeException(
						"Error removing book-keeping reference to allocated memory segment.", t);
				}
			}
			return segsForOwner == null || segsForOwner.isEmpty() ? null : segsForOwner;
		});
		return nextOwnerMemorySegment.get();
	}

	private static void freeSegment(MemorySegment segment, @Nullable Collection<MemorySegment> segments) {
		segment.free();
		if (segments != null) {
			segments.remove(segment);
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

		Preconditions.checkState(!isShutDown, "Memory manager has been shut down.");

		// get all segments
		Set<MemorySegment> segments = allocatedSegments.remove(owner);

		// all segments may have been freed previously individually
		if (segments == null || segments.isEmpty()) {
			return;
		}

		// free each segment
		for (MemorySegment segment: segments) {
			segment.free();
		}

		segments.clear();
	}

	/**
	 * Reserves a memory chunk of a certain size for an owner from this memory manager.
	 *
	 * @param owner The owner to associate with the memory reservation, for the fallback release.
	 * @param size size of memory to reserve.
	 * @throws MemoryReservationException Thrown, if this memory manager does not have the requested amount
	 *                                    of memory any more.
	 */
	public void reserveMemory(Object owner, long size) throws MemoryReservationException {
		checkMemoryReservationPreconditions(owner, size);
		if (size == 0L) {
			return;
		}

		memoryBudget.reserveMemory(size);

		reservedMemory.compute(owner, (o, memoryReservedForOwner) ->
			memoryReservedForOwner == null ? size : memoryReservedForOwner + size);

		Preconditions.checkState(!isShutDown, "Memory manager has been concurrently shut down.");
	}

	/**
	 * Releases a memory chunk of a certain size from an owner to this memory manager.
	 *
	 * @param owner The owner to associate with the memory reservation, for the fallback release.
	 * @param size size of memory to release.
	 */
	public void releaseMemory(Object owner, long size) {
		checkMemoryReservationPreconditions(owner, size);
		if (size == 0L) {
			return;
		}

		reservedMemory.compute(owner, (o, currentlyReserved) -> {
			long newReservedMemory = 0;
			if (currentlyReserved != null) {
				if (currentlyReserved < size) {
					LOG.warn(
						"Trying to release more memory {} than it was reserved {} so far for the owner {}",
						size,
						currentlyReserved,
						owner);
				}

				newReservedMemory = releaseAndCalculateReservedMemory(size, currentlyReserved);
			}

			return newReservedMemory == 0 ? null : newReservedMemory;
		});
	}

	private long releaseAndCalculateReservedMemory(long memoryToFree, long currentlyReserved) {
		final long effectiveMemoryToRelease = Math.min(currentlyReserved, memoryToFree);
		memoryBudget.releaseMemory(effectiveMemoryToRelease);

		return currentlyReserved - effectiveMemoryToRelease;
	}

	private void checkMemoryReservationPreconditions(Object owner, long size) {
		Preconditions.checkNotNull(owner, "The memory owner must not be null.");
		Preconditions.checkState(!isShutDown, "Memory manager has been shut down.");
		Preconditions.checkArgument(size >= 0L, "The memory size (%s) has to have non-negative size", size);
	}

	/**
	 * Releases all reserved memory chunks from an owner to this memory manager.
	 *
	 * @param owner The owner to associate with the memory reservation, for the fallback release.
	 */
	public void releaseAllMemory(Object owner) {
		checkMemoryReservationPreconditions(owner, 0L);
		Long memoryReservedForOwner = reservedMemory.remove(owner);
		if (memoryReservedForOwner != null) {
			memoryBudget.releaseMemory(memoryReservedForOwner);
		}
	}

	// ------------------------------------------------------------------------
	//  Shared opaque memory resources
	// ------------------------------------------------------------------------

	/**
	 * Acquires a shared memory resource, identified by a type string. If the resource already exists, this
	 * returns a descriptor to the resource. If the resource does not yet exist, the given memory fraction
	 * is reserved and the resource is initialized with that size.
	 *
	 * <p>The memory for the resource is reserved from the memory budget of this memory manager (thus
	 * determining the size of the resource), but resource itself is opaque, meaning the memory manager
	 * does not understand its structure.
	 *
	 * <p>The OpaqueMemoryResource object returned from this method must be closed once not used any further.
	 * Once all acquisitions have closed the object, the resource itself is closed.
	 *
	 * <p><b>Important:</b> The failure semantics are as follows: If the memory manager fails to reserve
	 * the memory, the external resource initializer will not be called. If an exception is thrown when the
	 * opaque resource is closed (last lease is released), the memory manager will still un-reserve the
	 * memory to make sure its own accounting is clean. The exception will need to be handled by the caller of
	 * {@link OpaqueMemoryResource#close()}. For example, if this indicates that native memory was not released
	 * and the process might thus have a memory leak, the caller can decide to kill the process as a result.
	 */
	public <T extends AutoCloseable> OpaqueMemoryResource<T> getSharedMemoryResourceForManagedMemory(
			String type,
			LongFunctionWithException<T, Exception> initializer,
			double fractionToInitializeWith) throws Exception {

		// if we need to allocate the resource (no shared resource allocated, yet), this would be the size to use
		final long numBytes = computeMemorySize(fractionToInitializeWith);

		// initializer and releaser as functions that are pushed into the SharedResources,
		// so that the SharedResources can decide in (thread-safely execute) when initialization
		// and release should happen
		final LongFunctionWithException<T, Exception> reserveAndInitialize = (size) -> {
			try {
				reserveMemory(type, size);
			} catch (MemoryReservationException e) {
				throw new MemoryAllocationException("Could not created the shared memory resource of size " + size +
					". Not enough memory left to reserve from the slot's managed memory.", e);
			}

			return initializer.apply(size);
		};

		final Consumer<Long> releaser = (size) -> releaseMemory(type, size);

		// This object identifies the lease in this request. It is used only to identify the release operation.
		// Using the object to represent the lease is a bit nicer safer than just using a reference counter.
		final Object leaseHolder = new Object();

		final SharedResources.ResourceAndSize<T> resource =
				sharedResources.getOrAllocateSharedResource(type, leaseHolder, reserveAndInitialize, numBytes);

		// the actual size may theoretically be different from what we requested, if allocated it was by
		// someone else before with a different value for fraction (should not happen in practice, though).
		final long size = resource.size();

		final ThrowingRunnable<Exception> disposer = () -> sharedResources.release(type, leaseHolder, releaser);

		return new OpaqueMemoryResource<>(resource.resourceHandle(), size, disposer);
	}

	/**
	 * Acquires a shared resource, identified by a type string. If the resource already exists, this
	 * returns a descriptor to the resource. If the resource does not yet exist, the method initializes
	 * a new resource using the initializer function and given size.
	 *
	 * <p>The resource opaque, meaning the memory manager does not understand its structure.
	 *
	 * <p>The OpaqueMemoryResource object returned from this method must be closed once not used any further.
	 * Once all acquisitions have closed the object, the resource itself is closed.
	 */
	public <T extends AutoCloseable> OpaqueMemoryResource<T> getExternalSharedMemoryResource(
			String type,
			LongFunctionWithException<T, Exception> initializer,
			long numBytes) throws Exception {

		// This object identifies the lease in this request. It is used only to identify the release operation.
		// Using the object to represent the lease is a bit nicer safer than just using a reference counter.
		final Object leaseHolder = new Object();

		final SharedResources.ResourceAndSize<T> resource =
				sharedResources.getOrAllocateSharedResource(type, leaseHolder, initializer, numBytes);

		final ThrowingRunnable<Exception> disposer = () -> sharedResources.release(type, leaseHolder);

		return new OpaqueMemoryResource<>(resource.resourceHandle(), resource.size(), disposer);
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
		return (int) pageSize;
	}

	/**
	 * Returns the total size of memory handled by this memory manager.
	 *
	 * @return The total size of memory.
	 */
	public long getMemorySize() {
		return memoryBudget.getTotalMemorySize();
	}

	/**
	 * Returns the available amount of memory handled by this memory manager.
	 *
	 * @return The available amount of memory.
	 */
	public long availableMemory() {
		return memoryBudget.getAvailableMemorySize();
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
		validateFraction(fraction);

		return (int) (totalNumberOfPages * fraction);
	}

	/**
	 * Computes the memory size corresponding to the fraction of all memory governed by this MemoryManager.
	 *
	 * @param fraction The fraction of all memory governed by this MemoryManager
	 * @return The memory size corresponding to the memory fraction
	 */
	public long computeMemorySize(double fraction) {
		validateFraction(fraction);

		return (long) Math.floor(memoryBudget.getTotalMemorySize() * fraction);
	}

	/**
	 * Creates a memory manager with the given capacity and given page size.
	 *
	 * <p>This is a production version of MemoryManager which waits for longest time
	 * to check for memory leaks ({@link #verifyEmpty()}) once the owner of the MemoryManager is ready to dispose.
	 *
	 * @param memorySize The total size of the off-heap memory to be managed by this memory manager.
	 * @param pageSize The size of the pages handed out by the memory manager.
	 */
	public static MemoryManager create(long memorySize, int pageSize) {
		return new MemoryManager(memorySize, pageSize, UnsafeMemoryBudget.MAX_SLEEPS_VERIFY_EMPTY);
	}

	private static void validateFraction(double fraction) {
		Preconditions.checkArgument(
			fraction != 0,
			"The fraction of memory to allocate should not be 0. Please make sure that all types of" +
				" managed memory consumers contained in the job are configured with a non-negative weight via `%s`.",
			TaskManagerOptions.MANAGED_MEMORY_CONSUMER_WEIGHTS.key());
		Preconditions.checkArgument(
			fraction > 0 && fraction <= 1,
			"The fraction of memory to allocate must within (0, 1], was: %s.",
			fraction);
	}
}
