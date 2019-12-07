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
import org.apache.flink.runtime.util.KeyedBudgetManager;
import org.apache.flink.runtime.util.KeyedBudgetManager.AcquisitionResult;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.LongFunctionWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.core.memory.MemorySegmentFactory.allocateOffHeapUnsafeMemory;
import static org.apache.flink.core.memory.MemorySegmentFactory.allocateUnpooledSegment;

/**
 * The memory manager governs the memory that Flink uses for sorting, hashing, and caching. Memory is represented
 * either in {@link MemorySegment}s of equal size and arbitrary type or in reserved chunks of certain size and {@link MemoryType}.
 * Operators allocate the memory either by requesting a number of memory segments or by reserving chunks.
 * Any allocated memory has to be released to be reused later.
 *
 * <p>Which {@link MemoryType}s the MemoryManager serves and their total sizes can be passed as an argument
 * to the constructor.
 *
 * <p>The memory segments may be represented as on-heap byte arrays or as off-heap memory regions
 * (both via {@link HybridMemorySegment}). Releasing a memory segment will make it re-claimable
 * by the garbage collector.
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
	private final Map<Object, Map<MemoryType, Long>> reservedMemory;

	private final KeyedBudgetManager<MemoryType> budgetByType;

	private final SharedResources sharedResources;

	/** Flag whether the close() has already been invoked. */
	private volatile boolean isShutDown;

	/**
	 * Creates a memory manager with the given memory types, capacity and given page size.
	 *
	 * @param memorySizeByType The total size of the memory to be managed by this memory manager for each type (heap / off-heap).
	 * @param pageSize The size of the pages handed out by the memory manager.
	 */
	public MemoryManager(Map<MemoryType, Long> memorySizeByType, int pageSize) {
		for (Entry<MemoryType, Long> sizeForType : memorySizeByType.entrySet()) {
			sanityCheck(sizeForType.getValue(), pageSize, sizeForType.getKey());
		}

		this.allocatedSegments = new ConcurrentHashMap<>();
		this.reservedMemory = new ConcurrentHashMap<>();
		this.budgetByType = new KeyedBudgetManager<>(memorySizeByType, pageSize);
		this.sharedResources = new SharedResources();
		verifyIntTotalNumberOfPages(memorySizeByType, budgetByType.maxTotalNumberOfPages());

		LOG.debug(
			"Initialized MemoryManager with total memory size {} ({}), page size {}.",
			budgetByType.totalAvailableBudget(),
			memorySizeByType,
			pageSize);
	}

	private static void sanityCheck(long memorySize, int pageSize, MemoryType memoryType) {
		Preconditions.checkNotNull(memoryType);
		Preconditions.checkArgument(memorySize >= 0L, "Size of total memory must be non-negative.");
		Preconditions.checkArgument(
			pageSize >= MIN_PAGE_SIZE,
			"The page size must be at least %d bytes.", MIN_PAGE_SIZE);
		Preconditions.checkArgument(
			MathUtils.isPowerOf2(pageSize),
			"The given page size is not a power of two.");
	}

	private static void verifyIntTotalNumberOfPages(Map<MemoryType, Long> memorySizeByType, long numberOfPagesLong) {
		Preconditions.checkArgument(
			numberOfPagesLong <= Integer.MAX_VALUE,
			"The given number of memory bytes (%d: %s) corresponds to more than MAX_INT pages.",
			numberOfPagesLong,
			memorySizeByType);
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
			budgetByType.releaseAll();

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
		return budgetByType.totalAvailableBudget() == budgetByType.maxTotalBudget();
	}

	// ------------------------------------------------------------------------
	//  Memory allocation and release
	// ------------------------------------------------------------------------

	/**
	 * Allocates a set of memory segments from this memory manager.
	 *
	 * <p>The returned segments can have any memory type. The total allocated memory for each type will not exceed its
	 * size limit, announced in the constructor.
	 *
	 * @param owner The owner to associate with the memory segment, for the fallback release.
	 * @param numPages The number of pages to allocate.
	 * @return A list with the memory segments.
	 * @throws MemoryAllocationException Thrown, if this memory manager does not have the requested amount
	 *                                   of memory pages any more.
	 * @deprecated use {@link #allocatePages(AllocationRequest)}
	 */
	@Deprecated
	public List<MemorySegment> allocatePages(Object owner, int numPages) throws MemoryAllocationException {
		List<MemorySegment> segments = new ArrayList<>(numPages);
		allocatePages(owner, segments, numPages);
		return segments;
	}

	/**
	 * Allocates a set of memory segments from this memory manager.
	 *
	 * <p>The allocated segments can have any memory type. The total allocated memory for each type will not exceed its
	 * size limit, announced in the constructor.
	 *
	 * @param owner The owner to associate with the memory segment, for the fallback release.
	 * @param target The list into which to put the allocated memory pages.
	 * @param numberOfPages The number of pages to allocate.
	 * @throws MemoryAllocationException Thrown, if this memory manager does not have the requested amount
	 *                                   of memory pages any more.
	 * @deprecated use {@link #allocatePages(AllocationRequest)}
	 */
	@Deprecated
	public void allocatePages(
			Object owner,
			Collection<MemorySegment> target,
			int numberOfPages) throws MemoryAllocationException {
		allocatePages(AllocationRequest
			.newBuilder(owner)
			.ofAllTypes()
			.numberOfPages(numberOfPages)
			.withOutput(target)
			.build());
	}

	/**
	 * Allocates a set of memory segments from this memory manager.
	 *
	 * <p>The allocated segments can have any memory type. The total allocated memory for each type will not exceed its
	 * size limit, announced in the constructor.
	 *
	 * @param request The allocation request which contains all the parameters.
	 * @return A collection with the allocated memory segments.
	 * @throws MemoryAllocationException Thrown, if this memory manager does not have the requested amount
	 *                                   of memory pages any more.
	 */
	public Collection<MemorySegment> allocatePages(AllocationRequest request) throws MemoryAllocationException {
		Object owner = request.getOwner();
		Collection<MemorySegment> target = request.output;
		int numberOfPages = request.getNumberOfPages();

		// sanity check
		Preconditions.checkNotNull(owner, "The memory owner must not be null.");
		Preconditions.checkState(!isShutDown, "Memory manager has been shut down.");

		// reserve array space, if applicable
		if (target instanceof ArrayList) {
			((ArrayList<MemorySegment>) target).ensureCapacity(numberOfPages);
		}

		AcquisitionResult<MemoryType> acquiredBudget = budgetByType.acquirePagedBudget(request.getTypes(), numberOfPages);
		if (acquiredBudget.isFailure()) {
			throw new MemoryAllocationException(
				String.format(
					"Could not allocate %d pages. Only %d pages are remaining.",
					numberOfPages,
					acquiredBudget.getTotalAvailableForAllQueriedKeys()));
		}

		allocatedSegments.compute(owner, (o, currentSegmentsForOwner) -> {
			Set<MemorySegment> segmentsForOwner = currentSegmentsForOwner == null ?
				new HashSet<>(numberOfPages) : currentSegmentsForOwner;
			for (MemoryType memoryType : acquiredBudget.getAcquiredPerKey().keySet()) {
				for (long i = acquiredBudget.getAcquiredPerKey().get(memoryType); i > 0; i--) {
					MemorySegment segment = allocateManagedSegment(memoryType, owner);
					target.add(segment);
					segmentsForOwner.add(segment);
				}
			}
			return segmentsForOwner;
		});

		Preconditions.checkState(!isShutDown, "Memory manager has been concurrently shut down.");

		return target;
	}

	/**
	 * Tries to release the memory for the specified segment.
	 *
	 * <p>If the segment has already been released, it is only freed. If it is null or has no owner, the request is simply ignored.
	 * The segment is only freed and made eligible for reclamation by the GC. The segment will be returned to
	 * the memory pool of its type, increasing its available limit for the later allocations.
	 *
	 * @param segment The segment to be released.
	 * @throws IllegalArgumentException Thrown, if the given segment is of an incompatible type.
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
				if (segsForOwner.remove(segment)) {
					budgetByType.releasePageForKey(getSegmentType(segment));
				}
				//noinspection ReturnOfNull
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
	 * the memory pool of its type, increasing its available limit for the later allocations.
	 *
	 * @param segments The segments to be released.
	 * @throws IllegalArgumentException Thrown, if the segments are of an incompatible type.
	 */
	public void release(Collection<MemorySegment> segments) {
		if (segments == null) {
			return;
		}

		Preconditions.checkState(!isShutDown, "Memory manager has been shut down.");

		EnumMap<MemoryType, Long> releasedMemory = new EnumMap<>(MemoryType.class);

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

			//noinspection ProhibitedExceptionCaught
			try {
				MemorySegment segment = null;
				while (segment == null && segmentsIterator.hasNext()) {
					segment = segmentsIterator.next();
				}
				while (segment != null) {
					segment = releaseSegmentsForOwnerUntilNextOwner(segment, segmentsIterator, releasedMemory);
				}
				segments.clear();
				// the only way to exit the loop
				successfullyReleased = true;
			} catch (ConcurrentModificationException | NoSuchElementException e) {
				// this may happen in the case where an asynchronous
				// call releases the memory. fall through the loop and try again
			}
		} while (!successfullyReleased);

		budgetByType.releaseBudgetForKeys(releasedMemory);
	}

	private MemorySegment releaseSegmentsForOwnerUntilNextOwner(
			MemorySegment firstSeg,
			Iterator<MemorySegment> segmentsIterator,
			EnumMap<MemoryType, Long> releasedMemory) {
		AtomicReference<MemorySegment> nextOwnerMemorySegment = new AtomicReference<>();
		Object owner = firstSeg.getOwner();
		allocatedSegments.compute(owner, (o, segsForOwner) -> {
			freeSegment(firstSeg, segsForOwner, releasedMemory);
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
					freeSegment(segment, segsForOwner, releasedMemory);
				} catch (Throwable t) {
					throw new RuntimeException(
						"Error removing book-keeping reference to allocated memory segment.", t);
				}
			}
			//noinspection ReturnOfNull
			return segsForOwner == null || segsForOwner.isEmpty() ? null : segsForOwner;
		});
		return nextOwnerMemorySegment.get();
	}

	private void freeSegment(
			MemorySegment segment,
			@Nullable Collection<MemorySegment> segments,
			EnumMap<MemoryType, Long> releasedMemory) {
		segment.free();
		if (segments != null && segments.remove(segment)) {
			releaseSegment(segment, releasedMemory);
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
		EnumMap<MemoryType, Long> releasedMemory = new EnumMap<>(MemoryType.class);
		for (MemorySegment segment : segments) {
			segment.free();
			releaseSegment(segment, releasedMemory);
		}
		budgetByType.releaseBudgetForKeys(releasedMemory);

		segments.clear();
	}

	/**
	 * Reserves memory of a certain type for an owner from this memory manager.
	 *
	 * @param owner The owner to associate with the memory reservation, for the fallback release.
	 * @param memoryType type of memory to reserve (heap / off-heap).
	 * @param size size of memory to reserve.
	 * @throws MemoryReservationException Thrown, if this memory manager does not have the requested amount
	 *                                    of memory any more.
	 */
	public void reserveMemory(Object owner, MemoryType memoryType, long size) throws MemoryReservationException {
		checkMemoryReservationPreconditions(owner, memoryType, size);
		if (size == 0L) {
			return;
		}

		long acquiredMemory = budgetByType.acquireBudgetForKey(memoryType, size);
		if (acquiredMemory < size) {
			throw new MemoryReservationException(
				String.format("Could not allocate %d bytes. Only %d bytes are remaining.", size, acquiredMemory));
		}

		reservedMemory.compute(owner, (o, reservations) -> {
			Map<MemoryType, Long> newReservations = reservations;
			if (reservations == null) {
				newReservations = new EnumMap<>(MemoryType.class);
				newReservations.put(memoryType, size);
			} else {
				reservations.compute(
					memoryType,
					(mt, currentlyReserved) -> currentlyReserved == null ? size : currentlyReserved + size);
			}
			return newReservations;
		});

		Preconditions.checkState(!isShutDown, "Memory manager has been concurrently shut down.");
	}

	/**
	 * Releases memory of a certain type from an owner to this memory manager.
	 *
	 * @param owner The owner to associate with the memory reservation, for the fallback release.
	 * @param memoryType type of memory to release (heap / off-heap).
	 * @param size size of memory to release.
	 */
	public void releaseMemory(Object owner, MemoryType memoryType, long size) {
		checkMemoryReservationPreconditions(owner, memoryType, size);
		if (size == 0L) {
			return;
		}

		reservedMemory.compute(owner, (o, reservations) -> {
			if (reservations != null) {
				reservations.compute(
					memoryType,
					(mt, currentlyReserved) -> {
						if (currentlyReserved == null || currentlyReserved < size) {
							LOG.warn(
								"Trying to release more memory {} than it was reserved {} so far for the owner {}",
								size,
								currentlyReserved == null ? 0 : currentlyReserved,
								owner);
							//noinspection ReturnOfNull
							return null;
						} else {
							return currentlyReserved - size;
						}
					});
			}
			//noinspection ReturnOfNull
			return reservations == null || reservations.isEmpty() ? null : reservations;
		});
		budgetByType.releaseBudgetForKey(memoryType, size);
	}

	private void checkMemoryReservationPreconditions(Object owner, MemoryType memoryType, long size) {
		Preconditions.checkNotNull(owner, "The memory owner must not be null.");
		Preconditions.checkNotNull(memoryType, "The memory type must not be null.");
		Preconditions.checkState(!isShutDown, "Memory manager has been shut down.");
		Preconditions.checkArgument(size >= 0L, "The memory size (%s) has to have non-negative size", size);
	}

	/**
	 * Releases all memory of a certain type from an owner to this memory manager.
	 *
	 * @param owner The owner to associate with the memory reservation, for the fallback release.
	 * @param memoryType type of memory to release (heap / off-heap).
	 */
	public void releaseAllMemory(Object owner, MemoryType memoryType) {
		checkMemoryReservationPreconditions(owner, memoryType, 0L);

		reservedMemory.compute(owner, (o, reservations) -> {
			if (reservations != null) {
				Long size = reservations.remove(memoryType);
				if (size != null) {
					budgetByType.releaseBudgetForKey(memoryType, size);
				}
			}
			//noinspection ReturnOfNull
			return reservations == null || reservations.isEmpty() ? null : reservations;
		});
	}

	// ------------------------------------------------------------------------
	//  Shared opaque memory resources
	// ------------------------------------------------------------------------

	/**
	 * Acquires a shared memory resource, that uses all the memory of this memory manager.
	 * This method behaves otherwise exactly as {@link #getSharedMemoryResourceForManagedMemory(String, LongFunctionWithException, double)}.
	 */
	public <T extends AutoCloseable> OpaqueMemoryResource<T> getSharedMemoryResourceForManagedMemory(
			String type,
			LongFunctionWithException<T, Exception> initializer) throws Exception {

		return getSharedMemoryResourceForManagedMemory(type, initializer, 1.0);
	}

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
	 */
	public <T extends AutoCloseable> OpaqueMemoryResource<T> getSharedMemoryResourceForManagedMemory(
			String type,
			LongFunctionWithException<T, Exception> initializer,
			double fractionToInitializeWith) throws Exception {

		// if we need to allocate the resource (no shared resource allocated, yet), this would be the size to use
		final long numBytes = computeMemorySize(fractionToInitializeWith);

		// the initializer attempt to reserve the memory before actual initialization
		final LongFunctionWithException<T, Exception> reserveAndInitialize = (size) -> {
			try {
				reserveMemory(type, MemoryType.OFF_HEAP, size);
			} catch (MemoryReservationException e) {
				throw new MemoryAllocationException("Could not created the shared memory resource of size " + size +
					". Not enough memory left to reserve from the slot's managed memory.", e);
			}

			return initializer.apply(size);
		};

		// This object identifies the lease in this request. It is used only to identify the release operation.
		// Using the object to represent the lease is a bit nicer safer than just using a reference counter.
		final Object leaseHolder = new Object();

		final SharedResources.ResourceAndSize<T> resource =
				sharedResources.getOrAllocateSharedResource(type, leaseHolder, reserveAndInitialize, numBytes);

		// the actual size may theoretically be different from what we requested, if allocated it was by
		// someone else before with a different value for fraction (should not happen in practice, though).
		final long size = resource.size();

		final ThrowingRunnable<Exception> disposer = () -> {
				final boolean allDisposed = sharedResources.release(type, leaseHolder);
				if (allDisposed) {
					releaseMemory(type, MemoryType.OFF_HEAP, size);
				}
			};

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
		//noinspection NumericCastThatLosesPrecision
		return (int) budgetByType.getDefaultPageSize();
	}

	/**
	 * Returns the total size of memory handled by this memory manager.
	 *
	 * @return The total size of memory.
	 */
	public long getMemorySize() {
		return budgetByType.maxTotalBudget();
	}

	/**
	 * Returns the total size of the certain type of memory handled by this memory manager.
	 *
	 * @param memoryType The type of memory.
	 * @return The total size of memory.
	 */
	public long getMemorySizeByType(MemoryType memoryType) {
		return budgetByType.maxTotalBudgetForKey(memoryType);
	}

	/**
	 * Returns the total size of the certain type of memory handled by this memory manager.
	 *
	 * @param memoryType The type of memory.
	 * @return The total size of memory.
	 */
	public long availableMemory(MemoryType memoryType) {
		return budgetByType.availableBudgetForKey(memoryType);
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
		return (int) (budgetByType.maxTotalNumberOfPages() * fraction);
	}

	/**
	 * Computes the memory size corresponding to the fraction of all memory governed by this MemoryManager.
	 *
	 * @param fraction The fraction of all memory governed by this MemoryManager
	 * @return The memory size corresponding to the memory fraction
	 */
	public long computeMemorySize(double fraction) {
		Preconditions.checkArgument(
			fraction > 0 && fraction <= 1,
			"The fraction of memory to allocate must within (0, 1], was: %s", fraction);

		return (long) (budgetByType.maxTotalBudget() * fraction);
	}

	private MemorySegment allocateManagedSegment(MemoryType memoryType, Object owner) {
		switch (memoryType) {
			case HEAP:
				return allocateUnpooledSegment(getPageSize(), owner);
			case OFF_HEAP:
				return allocateOffHeapUnsafeMemory(getPageSize(), owner);
			default:
				throw new IllegalArgumentException("unrecognized memory type: " + memoryType);
		}
	}

	private void releaseSegment(MemorySegment segment, EnumMap<MemoryType, Long> releasedMemory) {
		releasedMemory.compute(getSegmentType(segment), (t, v) -> v == null ? getPageSize() : v + getPageSize());
	}

	private static MemoryType getSegmentType(MemorySegment segment) {
		return segment.isOffHeap() ? MemoryType.OFF_HEAP : MemoryType.HEAP;
	}

	/** Memory segment allocation request. */
	@SuppressWarnings("WeakerAccess")
	public static class AllocationRequest {
		/** Owner of the segment to track by. */
		private final Object owner;

		/** Collection to add the allocated segments to. */
		private final Collection<MemorySegment> output;

		/** Number of pages to allocate. */
		private final int numberOfPages;

		/** Allowed types of memory to allocate. */
		private final Set<MemoryType> types;

		private AllocationRequest(
				Object owner,
				Collection<MemorySegment> output,
				int numberOfPages,
				Set<MemoryType> types) {
			this.owner = owner;
			this.output = output;
			this.numberOfPages = numberOfPages;
			this.types = types;
		}

		public Object getOwner() {
			return owner;
		}

		public int getNumberOfPages() {
			return numberOfPages;
		}

		public Set<MemoryType> getTypes() {
			return Collections.unmodifiableSet(types);
		}

		public static Builder newBuilder(Object owner) {
			return new Builder(owner);
		}

		public static AllocationRequest ofAllTypes(Object owner, int numberOfPages) {
			return newBuilder(owner).ofAllTypes().numberOfPages(numberOfPages).build();
		}

		public static AllocationRequest ofType(Object owner, int numberOfPages, MemoryType type) {
			return newBuilder(owner).ofType(type).numberOfPages(numberOfPages).build();
		}
	}

	/** A builder for the {@link AllocationRequest}. */
	@SuppressWarnings("WeakerAccess")
	public static class Builder {
		private final Object owner;
		private Collection<MemorySegment> output = new ArrayList<>();
		private int numberOfPages = 1;
		private Set<MemoryType> types = EnumSet.noneOf(MemoryType.class);

		public Builder(Object owner) {
			this.owner = owner;
		}

		public Builder withOutput(Collection<MemorySegment> output) {
			//noinspection AssignmentOrReturnOfFieldWithMutableType
			this.output = output;
			return this;
		}

		public Builder numberOfPages(int numberOfPages) {
			this.numberOfPages = numberOfPages;
			return this;
		}

		public Builder ofType(MemoryType type) {
			types.add(type);
			return this;
		}

		public Builder ofAllTypes() {
			types = EnumSet.allOf(MemoryType.class);
			return this;
		}

		public AllocationRequest build() {
			return new AllocationRequest(owner, output, numberOfPages, types);
		}
	}

	// ------------------------------------------------------------------------
	//  factories for testing
	// ------------------------------------------------------------------------

	public static MemoryManager forDefaultPageSize(long size) {
		final Map<MemoryType, Long> memorySizes = new HashMap<>();
		memorySizes.put(MemoryType.OFF_HEAP, size);
		return new MemoryManager(memorySizes, DEFAULT_PAGE_SIZE);
	}
}
