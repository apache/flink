/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.beam.vendor.grpc.v1p21p0.io.netty.buffer;


import static org.apache.beam.vendor.grpc.v1p21p0.io.netty.util.internal.ObjectUtil.checkPositiveOrZero;

import org.apache.beam.vendor.grpc.v1p21p0.io.netty.buffer.PoolArena.SizeClass;
import org.apache.beam.vendor.grpc.v1p21p0.io.netty.util.Recycler;
import org.apache.beam.vendor.grpc.v1p21p0.io.netty.util.Recycler.Handle;
import org.apache.beam.vendor.grpc.v1p21p0.io.netty.util.internal.MathUtil;
import org.apache.beam.vendor.grpc.v1p21p0.io.netty.util.internal.PlatformDependent;
import org.apache.beam.vendor.grpc.v1p21p0.io.netty.util.internal.logging.InternalLogger;
import org.apache.beam.vendor.grpc.v1p21p0.io.netty.util.internal.logging.InternalLoggerFactory;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

// This class is copied from Netty's io.netty.buffer.PoolThreadCache,
// can be removed after Beam bumps its shaded netty version to 1.22+ (BEAM-9030).
//
// Changed lines: 235, 242, 246~251, 268, 275, 280, 284, 426~427, 430, 435, 453, 458, 463~467, 469

/**
 * Acts a Thread cache for allocations. This implementation is moduled after
 * <a href="http://people.freebsd.org/~jasone/jemalloc/bsdcan2006/jemalloc.pdf">jemalloc</a> and the descripted
 * technics of
 * <a href="https://www.facebook.com/notes/facebook-engineering/scalable-memory-allocation-using-jemalloc/480222803919">
 * Scalable memory allocation using jemalloc</a>.
 */
final class PoolThreadCache {

	private static final InternalLogger logger = InternalLoggerFactory.getInstance(PoolThreadCache.class);

	final PoolArena<byte[]> heapArena;
	final PoolArena<ByteBuffer> directArena;

	// Hold the caches for the different size classes, which are tiny, small and normal.
	private final MemoryRegionCache<byte[]>[] tinySubPageHeapCaches;
	private final MemoryRegionCache<byte[]>[] smallSubPageHeapCaches;
	private final MemoryRegionCache<ByteBuffer>[] tinySubPageDirectCaches;
	private final MemoryRegionCache<ByteBuffer>[] smallSubPageDirectCaches;
	private final MemoryRegionCache<byte[]>[] normalHeapCaches;
	private final MemoryRegionCache<ByteBuffer>[] normalDirectCaches;

	// Used for bitshifting when calculate the index of normal caches later
	private final int numShiftsNormalDirect;
	private final int numShiftsNormalHeap;
	private final int freeSweepAllocationThreshold;
	private final AtomicBoolean freed = new AtomicBoolean();

	private int allocations;

	// TODO: Test if adding padding helps under contention
	//private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

	PoolThreadCache(PoolArena<byte[]> heapArena, PoolArena<ByteBuffer> directArena,
					int tinyCacheSize, int smallCacheSize, int normalCacheSize,
					int maxCachedBufferCapacity, int freeSweepAllocationThreshold) {
		checkPositiveOrZero(maxCachedBufferCapacity, "maxCachedBufferCapacity");
		this.freeSweepAllocationThreshold = freeSweepAllocationThreshold;
		this.heapArena = heapArena;
		this.directArena = directArena;
		if (directArena != null) {
			tinySubPageDirectCaches = createSubPageCaches(
				tinyCacheSize, PoolArena.numTinySubpagePools, SizeClass.Tiny);
			smallSubPageDirectCaches = createSubPageCaches(
				smallCacheSize, directArena.numSmallSubpagePools, SizeClass.Small);

			numShiftsNormalDirect = log2(directArena.pageSize);
			normalDirectCaches = createNormalCaches(
				normalCacheSize, maxCachedBufferCapacity, directArena);

			directArena.numThreadCaches.getAndIncrement();
		} else {
			// No directArea is configured so just null out all caches
			tinySubPageDirectCaches = null;
			smallSubPageDirectCaches = null;
			normalDirectCaches = null;
			numShiftsNormalDirect = -1;
		}
		if (heapArena != null) {
			// Create the caches for the heap allocations
			tinySubPageHeapCaches = createSubPageCaches(
				tinyCacheSize, PoolArena.numTinySubpagePools, SizeClass.Tiny);
			smallSubPageHeapCaches = createSubPageCaches(
				smallCacheSize, heapArena.numSmallSubpagePools, SizeClass.Small);

			numShiftsNormalHeap = log2(heapArena.pageSize);
			normalHeapCaches = createNormalCaches(
				normalCacheSize, maxCachedBufferCapacity, heapArena);

			heapArena.numThreadCaches.getAndIncrement();
		} else {
			// No heapArea is configured so just null out all caches
			tinySubPageHeapCaches = null;
			smallSubPageHeapCaches = null;
			normalHeapCaches = null;
			numShiftsNormalHeap = -1;
		}

		// Only check if there are caches in use.
		if ((tinySubPageDirectCaches != null || smallSubPageDirectCaches != null || normalDirectCaches != null
			|| tinySubPageHeapCaches != null || smallSubPageHeapCaches != null || normalHeapCaches != null)
			&& freeSweepAllocationThreshold < 1) {
			throw new IllegalArgumentException("freeSweepAllocationThreshold: "
				+ freeSweepAllocationThreshold + " (expected: > 0)");
		}
	}

	private static <T> MemoryRegionCache<T>[] createSubPageCaches(
		int cacheSize, int numCaches, SizeClass sizeClass) {
		if (cacheSize > 0 && numCaches > 0) {
			@SuppressWarnings("unchecked")
			MemoryRegionCache<T>[] cache = new MemoryRegionCache[numCaches];
			for (int i = 0; i < cache.length; i++) {
				// TODO: maybe use cacheSize / cache.length
				cache[i] = new SubPageMemoryRegionCache<T>(cacheSize, sizeClass);
			}
			return cache;
		} else {
			return null;
		}
	}

	private static <T> MemoryRegionCache<T>[] createNormalCaches(
		int cacheSize, int maxCachedBufferCapacity, PoolArena<T> area) {
		if (cacheSize > 0 && maxCachedBufferCapacity > 0) {
			int max = Math.min(area.chunkSize, maxCachedBufferCapacity);
			int arraySize = Math.max(1, log2(max / area.pageSize) + 1);

			@SuppressWarnings("unchecked")
			MemoryRegionCache<T>[] cache = new MemoryRegionCache[arraySize];
			for (int i = 0; i < cache.length; i++) {
				cache[i] = new NormalMemoryRegionCache<T>(cacheSize);
			}
			return cache;
		} else {
			return null;
		}
	}

	private static int log2(int val) {
		int res = 0;
		while (val > 1) {
			val >>= 1;
			res++;
		}
		return res;
	}

	/**
	 * Try to allocate a tiny buffer out of the cache. Returns {@code true} if successful {@code false} otherwise
	 */
	boolean allocateTiny(PoolArena<?> area, PooledByteBuf<?> buf, int reqCapacity, int normCapacity) {
		return allocate(cacheForTiny(area, normCapacity), buf, reqCapacity);
	}

	/**
	 * Try to allocate a small buffer out of the cache. Returns {@code true} if successful {@code false} otherwise
	 */
	boolean allocateSmall(PoolArena<?> area, PooledByteBuf<?> buf, int reqCapacity, int normCapacity) {
		return allocate(cacheForSmall(area, normCapacity), buf, reqCapacity);
	}

	/**
	 * Try to allocate a small buffer out of the cache. Returns {@code true} if successful {@code false} otherwise
	 */
	boolean allocateNormal(PoolArena<?> area, PooledByteBuf<?> buf, int reqCapacity, int normCapacity) {
		return allocate(cacheForNormal(area, normCapacity), buf, reqCapacity);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private boolean allocate(MemoryRegionCache<?> cache, PooledByteBuf buf, int reqCapacity) {
		if (cache == null) {
			// no cache found so just return false here
			return false;
		}
		boolean allocated = cache.allocate(buf, reqCapacity);
		if (++ allocations >= freeSweepAllocationThreshold) {
			allocations = 0;
			trim();
		}
		return allocated;
	}

	/**
	 * Add {@link PoolChunk} and {@code handle} to the cache if there is enough room.
	 * Returns {@code true} if it fit into the cache {@code false} otherwise.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	boolean add(PoolArena<?> area, PoolChunk chunk, ByteBuffer nioBuffer,
				long handle, int normCapacity, SizeClass sizeClass) {
		MemoryRegionCache<?> cache = cache(area, normCapacity, sizeClass);
		if (cache == null) {
			return false;
		}
		return cache.add(chunk, nioBuffer, handle);
	}

	private MemoryRegionCache<?> cache(PoolArena<?> area, int normCapacity, SizeClass sizeClass) {
		switch (sizeClass) {
			case Normal:
				return cacheForNormal(area, normCapacity);
			case Small:
				return cacheForSmall(area, normCapacity);
			case Tiny:
				return cacheForTiny(area, normCapacity);
			default:
				throw new Error();
		}
	}

	/// TODO: In the future when we move to Java9+ we should use java.lang.ref.Cleaner.
	@Override
	protected void finalize() throws Throwable {
		try {
			super.finalize();
		} finally {
			free(true);
		}
	}

	/**
	 *  Should be called if the Thread that uses this cache is about to exist to release resources out of the cache
	 */
	void free(boolean finalizer) {
		// As free() may be called either by the finalizer or by FastThreadLocal.onRemoval(...) we need to ensure
		// we only call this one time.
		if (freed.compareAndSet(false, true)) {
			int numFreed = free(tinySubPageDirectCaches, finalizer) +
				free(smallSubPageDirectCaches, finalizer) +
				free(normalDirectCaches, finalizer) +
				free(tinySubPageHeapCaches, finalizer) +
				free(smallSubPageHeapCaches, finalizer) +
				free(normalHeapCaches, finalizer);

			if (numFreed > 0 && logger.isDebugEnabled()) {
				logger.debug("Freed {} thread-local buffer(s) from thread: {}", numFreed,
					Thread.currentThread().getName());
			}

			if (directArena != null) {
				directArena.numThreadCaches.getAndDecrement();
			}

			if (heapArena != null) {
				heapArena.numThreadCaches.getAndDecrement();
			}
		}
	}

	private static int free(MemoryRegionCache<?>[] caches, boolean finalizer) {
		if (caches == null) {
			return 0;
		}

		int numFreed = 0;
		for (MemoryRegionCache<?> c: caches) {
			numFreed += free(c, finalizer);
		}
		return numFreed;
	}

	private static int free(MemoryRegionCache<?> cache, boolean finalizer) {
		if (cache == null) {
			return 0;
		}
		return cache.free(finalizer);
	}

	void trim() {
		trim(tinySubPageDirectCaches);
		trim(smallSubPageDirectCaches);
		trim(normalDirectCaches);
		trim(tinySubPageHeapCaches);
		trim(smallSubPageHeapCaches);
		trim(normalHeapCaches);
	}

	private static void trim(MemoryRegionCache<?>[] caches) {
		if (caches == null) {
			return;
		}
		for (MemoryRegionCache<?> c: caches) {
			trim(c);
		}
	}

	private static void trim(MemoryRegionCache<?> cache) {
		if (cache == null) {
			return;
		}
		cache.trim();
	}

	private MemoryRegionCache<?> cacheForTiny(PoolArena<?> area, int normCapacity) {
		int idx = PoolArena.tinyIdx(normCapacity);
		if (area.isDirect()) {
			return cache(tinySubPageDirectCaches, idx);
		}
		return cache(tinySubPageHeapCaches, idx);
	}

	private MemoryRegionCache<?> cacheForSmall(PoolArena<?> area, int normCapacity) {
		int idx = PoolArena.smallIdx(normCapacity);
		if (area.isDirect()) {
			return cache(smallSubPageDirectCaches, idx);
		}
		return cache(smallSubPageHeapCaches, idx);
	}

	private MemoryRegionCache<?> cacheForNormal(PoolArena<?> area, int normCapacity) {
		if (area.isDirect()) {
			int idx = log2(normCapacity >> numShiftsNormalDirect);
			return cache(normalDirectCaches, idx);
		}
		int idx = log2(normCapacity >> numShiftsNormalHeap);
		return cache(normalHeapCaches, idx);
	}

	private static <T> MemoryRegionCache<T> cache(MemoryRegionCache<T>[] cache, int idx) {
		if (cache == null || idx > cache.length - 1) {
			return null;
		}
		return cache[idx];
	}

	/**
	 * Cache used for buffers which are backed by TINY or SMALL size.
	 */
	private static final class SubPageMemoryRegionCache<T> extends MemoryRegionCache<T> {
		SubPageMemoryRegionCache(int size, SizeClass sizeClass) {
			super(size, sizeClass);
		}

		@Override
		protected void initBuf(
			PoolChunk<T> chunk, ByteBuffer nioBuffer, long handle, PooledByteBuf<T> buf, int reqCapacity) {
			chunk.initBufWithSubpage(buf, nioBuffer, handle, reqCapacity);
		}
	}

	/**
	 * Cache used for buffers which are backed by NORMAL size.
	 */
	private static final class NormalMemoryRegionCache<T> extends MemoryRegionCache<T> {
		NormalMemoryRegionCache(int size) {
			super(size, SizeClass.Normal);
		}

		@Override
		protected void initBuf(
			PoolChunk<T> chunk, ByteBuffer nioBuffer, long handle, PooledByteBuf<T> buf, int reqCapacity) {
			chunk.initBuf(buf, nioBuffer, handle, reqCapacity);
		}
	}

	private abstract static class MemoryRegionCache<T> {
		private final int size;
		private final Queue<Entry<T>> queue;
		private final SizeClass sizeClass;
		private int allocations;

		MemoryRegionCache(int size, SizeClass sizeClass) {
			this.size = MathUtil.safeFindNextPositivePowerOfTwo(size);
			queue = PlatformDependent.newFixedMpscQueue(this.size);
			this.sizeClass = sizeClass;
		}

		/**
		 * Init the {@link PooledByteBuf} using the provided chunk and handle with the capacity restrictions.
		 */
		protected abstract void initBuf(PoolChunk<T> chunk, ByteBuffer nioBuffer, long handle,
										PooledByteBuf<T> buf, int reqCapacity);

		/**
		 * Add to cache if not already full.
		 */
		@SuppressWarnings("unchecked")
		public final boolean add(PoolChunk<T> chunk, ByteBuffer nioBuffer, long handle) {
			Entry<T> entry = newEntry(chunk, nioBuffer, handle);
			boolean queued = queue.offer(entry);
			if (!queued) {
				// If it was not possible to cache the chunk, immediately recycle the entry
				entry.recycle();
			}

			return queued;
		}

		/**
		 * Allocate something out of the cache if possible and remove the entry from the cache.
		 */
		public final boolean allocate(PooledByteBuf<T> buf, int reqCapacity) {
			Entry<T> entry = queue.poll();
			if (entry == null) {
				return false;
			}
			initBuf(entry.chunk, entry.nioBuffer, entry.handle, buf, reqCapacity);
			entry.recycle();

			// allocations is not thread-safe which is fine as this is only called from the same thread all time.
			++ allocations;
			return true;
		}

		/**
		 * Clear out this cache and free up all previous cached {@link PoolChunk}s and {@code handle}s.
		 */
		public final int free(boolean finalizer) {
			return free(Integer.MAX_VALUE, finalizer);
		}

		private int free(int max, boolean finalizer) {
			int numFreed = 0;
			for (; numFreed < max; numFreed++) {
				Entry<T> entry = queue.poll();
				if (entry != null) {
					freeEntry(entry, finalizer);
				} else {
					// all cleared
					return numFreed;
				}
			}
			return numFreed;
		}

		/**
		 * Free up cached {@link PoolChunk}s if not allocated frequently enough.
		 */
		public final void trim() {
			int free = size - allocations;
			allocations = 0;

			// We not even allocated all the number that are
			if (free > 0) {
				free(free, false);
			}
		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		private  void freeEntry(Entry entry, boolean finalizer) {
			PoolChunk chunk = entry.chunk;
			long handle = entry.handle;
			ByteBuffer nioBuffer = entry.nioBuffer;

			if (!finalizer) {
				// recycle now so PoolChunk can be GC'ed. This will only be done if this is not freed because of
				// a finalizer.
				entry.recycle();
			}

			chunk.arena.freeChunk(chunk, handle, sizeClass, nioBuffer, finalizer);
		}

		static final class Entry<T> {
			final Handle<Entry<?>> recyclerHandle;
			PoolChunk<T> chunk;
			ByteBuffer nioBuffer;
			long handle = -1;

			Entry(Handle<Entry<?>> recyclerHandle) {
				this.recyclerHandle = recyclerHandle;
			}

			void recycle() {
				chunk = null;
				nioBuffer = null;
				handle = -1;
				recyclerHandle.recycle(this);
			}
		}

		@SuppressWarnings("rawtypes")
		private static Entry newEntry(PoolChunk<?> chunk, ByteBuffer nioBuffer, long handle) {
			Entry entry = RECYCLER.get();
			entry.chunk = chunk;
			entry.nioBuffer = nioBuffer;
			entry.handle = handle;
			return entry;
		}

		@SuppressWarnings("rawtypes")
		private static final Recycler<Entry> RECYCLER = new Recycler<Entry>() {
			@SuppressWarnings("unchecked")
			@Override
			protected Entry newObject(Handle<Entry> handle) {
				return new Entry(handle);
			}
		};
	}
}
