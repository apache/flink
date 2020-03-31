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

import org.apache.beam.vendor.grpc.v1p21p0.io.netty.util.NettyRuntime;
import org.apache.beam.vendor.grpc.v1p21p0.io.netty.util.concurrent.FastThreadLocal;
import org.apache.beam.vendor.grpc.v1p21p0.io.netty.util.concurrent.FastThreadLocalThread;
import org.apache.beam.vendor.grpc.v1p21p0.io.netty.util.internal.PlatformDependent;
import org.apache.beam.vendor.grpc.v1p21p0.io.netty.util.internal.StringUtil;
import org.apache.beam.vendor.grpc.v1p21p0.io.netty.util.internal.SystemPropertyUtil;
import org.apache.beam.vendor.grpc.v1p21p0.io.netty.util.internal.logging.InternalLogger;
import org.apache.beam.vendor.grpc.v1p21p0.io.netty.util.internal.logging.InternalLoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

// This class is copied from Netty's io.netty.buffer.PooledByteBufAllocator,
// can be removed after Beam bumps its shaded netty version to 1.22+ (BEAM-9030).
//
// Changed lines: 458

public class PooledByteBufAllocator extends AbstractByteBufAllocator implements ByteBufAllocatorMetricProvider {

	private static final InternalLogger logger = InternalLoggerFactory.getInstance(PooledByteBufAllocator.class);
	private static final int DEFAULT_NUM_HEAP_ARENA;
	private static final int DEFAULT_NUM_DIRECT_ARENA;

	private static final int DEFAULT_PAGE_SIZE;
	private static final int DEFAULT_MAX_ORDER; // 8192 << 11 = 16 MiB per chunk
	private static final int DEFAULT_TINY_CACHE_SIZE;
	private static final int DEFAULT_SMALL_CACHE_SIZE;
	private static final int DEFAULT_NORMAL_CACHE_SIZE;
	private static final int DEFAULT_MAX_CACHED_BUFFER_CAPACITY;
	private static final int DEFAULT_CACHE_TRIM_INTERVAL;
	private static final boolean DEFAULT_USE_CACHE_FOR_ALL_THREADS;
	private static final int DEFAULT_DIRECT_MEMORY_CACHE_ALIGNMENT;
	static final int DEFAULT_MAX_CACHED_BYTEBUFFERS_PER_CHUNK;

	private static final int MIN_PAGE_SIZE = 4096;
	private static final int MAX_CHUNK_SIZE = (int) (((long) Integer.MAX_VALUE + 1) / 2);

	static {
		int defaultPageSize = SystemPropertyUtil.getInt("io.netty.allocator.pageSize", 8192);
		Throwable pageSizeFallbackCause = null;
		try {
			validateAndCalculatePageShifts(defaultPageSize);
		} catch (Throwable t) {
			pageSizeFallbackCause = t;
			defaultPageSize = 8192;
		}
		DEFAULT_PAGE_SIZE = defaultPageSize;

		int defaultMaxOrder = SystemPropertyUtil.getInt("io.netty.allocator.maxOrder", 11);
		Throwable maxOrderFallbackCause = null;
		try {
			validateAndCalculateChunkSize(DEFAULT_PAGE_SIZE, defaultMaxOrder);
		} catch (Throwable t) {
			maxOrderFallbackCause = t;
			defaultMaxOrder = 11;
		}
		DEFAULT_MAX_ORDER = defaultMaxOrder;

		// Determine reasonable default for nHeapArena and nDirectArena.
		// Assuming each arena has 3 chunks, the pool should not consume more than 50% of max memory.
		final Runtime runtime = Runtime.getRuntime();

		/*
		 * We use 2 * available processors by default to reduce contention as we use 2 * available processors for the
		 * number of EventLoops in NIO and EPOLL as well. If we choose a smaller number we will run into hot spots as
		 * allocation and de-allocation needs to be synchronized on the PoolArena.
		 *
		 * See https://github.com/netty/netty/issues/3888.
		 */
		final int defaultMinNumArena = NettyRuntime.availableProcessors() * 2;
		final int defaultChunkSize = DEFAULT_PAGE_SIZE << DEFAULT_MAX_ORDER;
		DEFAULT_NUM_HEAP_ARENA = Math.max(0,
			SystemPropertyUtil.getInt(
				"io.netty.allocator.numHeapArenas",
				(int) Math.min(
					defaultMinNumArena,
					runtime.maxMemory() / defaultChunkSize / 2 / 3)));
		DEFAULT_NUM_DIRECT_ARENA = Math.max(0,
			SystemPropertyUtil.getInt(
				"io.netty.allocator.numDirectArenas",
				(int) Math.min(
					defaultMinNumArena,
					PlatformDependent.maxDirectMemory() / defaultChunkSize / 2 / 3)));

		// cache sizes
		DEFAULT_TINY_CACHE_SIZE = SystemPropertyUtil.getInt("io.netty.allocator.tinyCacheSize", 512);
		DEFAULT_SMALL_CACHE_SIZE = SystemPropertyUtil.getInt("io.netty.allocator.smallCacheSize", 256);
		DEFAULT_NORMAL_CACHE_SIZE = SystemPropertyUtil.getInt("io.netty.allocator.normalCacheSize", 64);

		// 32 kb is the default maximum capacity of the cached buffer. Similar to what is explained in
		// 'Scalable memory allocation using jemalloc'
		DEFAULT_MAX_CACHED_BUFFER_CAPACITY = SystemPropertyUtil.getInt(
			"io.netty.allocator.maxCachedBufferCapacity", 32 * 1024);

		// the number of threshold of allocations when cached entries will be freed up if not frequently used
		DEFAULT_CACHE_TRIM_INTERVAL = SystemPropertyUtil.getInt(
			"io.netty.allocator.cacheTrimInterval", 8192);

		DEFAULT_USE_CACHE_FOR_ALL_THREADS = SystemPropertyUtil.getBoolean(
			"io.netty.allocator.useCacheForAllThreads", true);

		DEFAULT_DIRECT_MEMORY_CACHE_ALIGNMENT = SystemPropertyUtil.getInt(
			"io.netty.allocator.directMemoryCacheAlignment", 0);

		// Use 1023 by default as we use an ArrayDeque as backing storage which will then allocate an internal array
		// of 1024 elements. Otherwise we would allocate 2048 and only use 1024 which is wasteful.
		DEFAULT_MAX_CACHED_BYTEBUFFERS_PER_CHUNK = SystemPropertyUtil.getInt(
			"io.netty.allocator.maxCachedByteBuffersPerChunk", 1023);

		if (logger.isDebugEnabled()) {
			logger.debug("-Dio.netty.allocator.numHeapArenas: {}", DEFAULT_NUM_HEAP_ARENA);
			logger.debug("-Dio.netty.allocator.numDirectArenas: {}", DEFAULT_NUM_DIRECT_ARENA);
			if (pageSizeFallbackCause == null) {
				logger.debug("-Dio.netty.allocator.pageSize: {}", DEFAULT_PAGE_SIZE);
			} else {
				logger.debug("-Dio.netty.allocator.pageSize: {}", DEFAULT_PAGE_SIZE, pageSizeFallbackCause);
			}
			if (maxOrderFallbackCause == null) {
				logger.debug("-Dio.netty.allocator.maxOrder: {}", DEFAULT_MAX_ORDER);
			} else {
				logger.debug("-Dio.netty.allocator.maxOrder: {}", DEFAULT_MAX_ORDER, maxOrderFallbackCause);
			}
			logger.debug("-Dio.netty.allocator.chunkSize: {}", DEFAULT_PAGE_SIZE << DEFAULT_MAX_ORDER);
			logger.debug("-Dio.netty.allocator.tinyCacheSize: {}", DEFAULT_TINY_CACHE_SIZE);
			logger.debug("-Dio.netty.allocator.smallCacheSize: {}", DEFAULT_SMALL_CACHE_SIZE);
			logger.debug("-Dio.netty.allocator.normalCacheSize: {}", DEFAULT_NORMAL_CACHE_SIZE);
			logger.debug("-Dio.netty.allocator.maxCachedBufferCapacity: {}", DEFAULT_MAX_CACHED_BUFFER_CAPACITY);
			logger.debug("-Dio.netty.allocator.cacheTrimInterval: {}", DEFAULT_CACHE_TRIM_INTERVAL);
			logger.debug("-Dio.netty.allocator.useCacheForAllThreads: {}", DEFAULT_USE_CACHE_FOR_ALL_THREADS);
			logger.debug("-Dio.netty.allocator.maxCachedByteBuffersPerChunk: {}",
				DEFAULT_MAX_CACHED_BYTEBUFFERS_PER_CHUNK);
		}
	}

	public static final PooledByteBufAllocator DEFAULT =
		new PooledByteBufAllocator(PlatformDependent.directBufferPreferred());

	private final PoolArena<byte[]>[] heapArenas;
	private final PoolArena<ByteBuffer>[] directArenas;
	private final int tinyCacheSize;
	private final int smallCacheSize;
	private final int normalCacheSize;
	private final List<PoolArenaMetric> heapArenaMetrics;
	private final List<PoolArenaMetric> directArenaMetrics;
	private final PoolThreadLocalCache threadCache;
	private final int chunkSize;
	private final PooledByteBufAllocatorMetric metric;

	public PooledByteBufAllocator() {
		this(false);
	}

	@SuppressWarnings("deprecation")
	public PooledByteBufAllocator(boolean preferDirect) {
		this(preferDirect, DEFAULT_NUM_HEAP_ARENA, DEFAULT_NUM_DIRECT_ARENA, DEFAULT_PAGE_SIZE, DEFAULT_MAX_ORDER);
	}

	@SuppressWarnings("deprecation")
	public PooledByteBufAllocator(int nHeapArena, int nDirectArena, int pageSize, int maxOrder) {
		this(false, nHeapArena, nDirectArena, pageSize, maxOrder);
	}

	/**
	 * @deprecated use
	 * {@link PooledByteBufAllocator#PooledByteBufAllocator(boolean, int, int, int, int, int, int, int, boolean)}
	 */
	@Deprecated
	public PooledByteBufAllocator(boolean preferDirect, int nHeapArena, int nDirectArena, int pageSize, int maxOrder) {
		this(preferDirect, nHeapArena, nDirectArena, pageSize, maxOrder,
			DEFAULT_TINY_CACHE_SIZE, DEFAULT_SMALL_CACHE_SIZE, DEFAULT_NORMAL_CACHE_SIZE);
	}

	/**
	 * @deprecated use
	 * {@link PooledByteBufAllocator#PooledByteBufAllocator(boolean, int, int, int, int, int, int, int, boolean)}
	 */
	@Deprecated
	public PooledByteBufAllocator(boolean preferDirect, int nHeapArena, int nDirectArena, int pageSize, int maxOrder,
								  int tinyCacheSize, int smallCacheSize, int normalCacheSize) {
		this(preferDirect, nHeapArena, nDirectArena, pageSize, maxOrder, tinyCacheSize, smallCacheSize,
			normalCacheSize, DEFAULT_USE_CACHE_FOR_ALL_THREADS, DEFAULT_DIRECT_MEMORY_CACHE_ALIGNMENT);
	}

	public PooledByteBufAllocator(boolean preferDirect, int nHeapArena,
								  int nDirectArena, int pageSize, int maxOrder, int tinyCacheSize,
								  int smallCacheSize, int normalCacheSize,
								  boolean useCacheForAllThreads) {
		this(preferDirect, nHeapArena, nDirectArena, pageSize, maxOrder,
			tinyCacheSize, smallCacheSize, normalCacheSize,
			useCacheForAllThreads, DEFAULT_DIRECT_MEMORY_CACHE_ALIGNMENT);
	}

	public PooledByteBufAllocator(boolean preferDirect, int nHeapArena, int nDirectArena, int pageSize, int maxOrder,
								  int tinyCacheSize, int smallCacheSize, int normalCacheSize,
								  boolean useCacheForAllThreads, int directMemoryCacheAlignment) {
		super(preferDirect);
		threadCache = new PoolThreadLocalCache(useCacheForAllThreads);
		this.tinyCacheSize = tinyCacheSize;
		this.smallCacheSize = smallCacheSize;
		this.normalCacheSize = normalCacheSize;
		chunkSize = validateAndCalculateChunkSize(pageSize, maxOrder);

		checkPositiveOrZero(nHeapArena, "nHeapArena");
		checkPositiveOrZero(nDirectArena, "nDirectArena");

		checkPositiveOrZero(directMemoryCacheAlignment, "directMemoryCacheAlignment");
		if (directMemoryCacheAlignment > 0 && !isDirectMemoryCacheAlignmentSupported()) {
			throw new IllegalArgumentException("directMemoryCacheAlignment is not supported");
		}

		if ((directMemoryCacheAlignment & -directMemoryCacheAlignment) != directMemoryCacheAlignment) {
			throw new IllegalArgumentException("directMemoryCacheAlignment: "
				+ directMemoryCacheAlignment + " (expected: power of two)");
		}

		int pageShifts = validateAndCalculatePageShifts(pageSize);

		if (nHeapArena > 0) {
			heapArenas = newArenaArray(nHeapArena);
			List<PoolArenaMetric> metrics = new ArrayList<PoolArenaMetric>(heapArenas.length);
			for (int i = 0; i < heapArenas.length; i ++) {
				PoolArena.HeapArena arena = new PoolArena.HeapArena(this,
					pageSize, maxOrder, pageShifts, chunkSize,
					directMemoryCacheAlignment);
				heapArenas[i] = arena;
				metrics.add(arena);
			}
			heapArenaMetrics = Collections.unmodifiableList(metrics);
		} else {
			heapArenas = null;
			heapArenaMetrics = Collections.emptyList();
		}

		if (nDirectArena > 0) {
			directArenas = newArenaArray(nDirectArena);
			List<PoolArenaMetric> metrics = new ArrayList<PoolArenaMetric>(directArenas.length);
			for (int i = 0; i < directArenas.length; i ++) {
				PoolArena.DirectArena arena = new PoolArena.DirectArena(
					this, pageSize, maxOrder, pageShifts, chunkSize, directMemoryCacheAlignment);
				directArenas[i] = arena;
				metrics.add(arena);
			}
			directArenaMetrics = Collections.unmodifiableList(metrics);
		} else {
			directArenas = null;
			directArenaMetrics = Collections.emptyList();
		}
		metric = new PooledByteBufAllocatorMetric(this);
	}

	@SuppressWarnings("unchecked")
	private static <T> PoolArena<T>[] newArenaArray(int size) {
		return new PoolArena[size];
	}

	private static int validateAndCalculatePageShifts(int pageSize) {
		if (pageSize < MIN_PAGE_SIZE) {
			throw new IllegalArgumentException("pageSize: " + pageSize + " (expected: " + MIN_PAGE_SIZE + ")");
		}

		if ((pageSize & pageSize - 1) != 0) {
			throw new IllegalArgumentException("pageSize: " + pageSize + " (expected: power of 2)");
		}

		// Logarithm base 2. At this point we know that pageSize is a power of two.
		return Integer.SIZE - 1 - Integer.numberOfLeadingZeros(pageSize);
	}

	private static int validateAndCalculateChunkSize(int pageSize, int maxOrder) {
		if (maxOrder > 14) {
			throw new IllegalArgumentException("maxOrder: " + maxOrder + " (expected: 0-14)");
		}

		// Ensure the resulting chunkSize does not overflow.
		int chunkSize = pageSize;
		for (int i = maxOrder; i > 0; i --) {
			if (chunkSize > MAX_CHUNK_SIZE / 2) {
				throw new IllegalArgumentException(String.format(
					"pageSize (%d) << maxOrder (%d) must not exceed %d", pageSize, maxOrder, MAX_CHUNK_SIZE));
			}
			chunkSize <<= 1;
		}
		return chunkSize;
	}

	@Override
	protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
		PoolThreadCache cache = threadCache.get();
		PoolArena<byte[]> heapArena = cache.heapArena;

		final ByteBuf buf;
		if (heapArena != null) {
			buf = heapArena.allocate(cache, initialCapacity, maxCapacity);
		} else {
			buf = PlatformDependent.hasUnsafe() ?
				new UnpooledUnsafeHeapByteBuf(this, initialCapacity, maxCapacity) :
				new UnpooledHeapByteBuf(this, initialCapacity, maxCapacity);
		}

		return toLeakAwareBuffer(buf);
	}

	@Override
	protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
		PoolThreadCache cache = threadCache.get();
		PoolArena<ByteBuffer> directArena = cache.directArena;

		final ByteBuf buf;
		if (directArena != null) {
			buf = directArena.allocate(cache, initialCapacity, maxCapacity);
		} else {
			buf = PlatformDependent.hasUnsafe() ?
				UnsafeByteBufUtil.newUnsafeDirectByteBuf(this, initialCapacity, maxCapacity) :
				new UnpooledDirectByteBuf(this, initialCapacity, maxCapacity);
		}

		return toLeakAwareBuffer(buf);
	}

	/**
	 * Default number of heap arenas - System Property: io.netty.allocator.numHeapArenas - default 2 * cores
	 */
	public static int defaultNumHeapArena() {
		return DEFAULT_NUM_HEAP_ARENA;
	}

	/**
	 * Default number of direct arenas - System Property: io.netty.allocator.numDirectArenas - default 2 * cores
	 */
	public static int defaultNumDirectArena() {
		return DEFAULT_NUM_DIRECT_ARENA;
	}

	/**
	 * Default buffer page size - System Property: io.netty.allocator.pageSize - default 8192
	 */
	public static int defaultPageSize() {
		return DEFAULT_PAGE_SIZE;
	}

	/**
	 * Default maximum order - System Property: io.netty.allocator.maxOrder - default 11
	 */
	public static int defaultMaxOrder() {
		return DEFAULT_MAX_ORDER;
	}

	/**
	 * Default thread caching behavior - System Property: io.netty.allocator.useCacheForAllThreads - default true
	 */
	public static boolean defaultUseCacheForAllThreads() {
		return DEFAULT_USE_CACHE_FOR_ALL_THREADS;
	}

	/**
	 * Default prefer direct - System Property: io.netty.noPreferDirect - default false
	 */
	public static boolean defaultPreferDirect() {
		return PlatformDependent.directBufferPreferred();
	}

	/**
	 * Default tiny cache size - System Property: io.netty.allocator.tinyCacheSize - default 512
	 */
	public static int defaultTinyCacheSize() {
		return DEFAULT_TINY_CACHE_SIZE;
	}

	/**
	 * Default small cache size - System Property: io.netty.allocator.smallCacheSize - default 256
	 */
	public static int defaultSmallCacheSize() {
		return DEFAULT_SMALL_CACHE_SIZE;
	}

	/**
	 * Default normal cache size - System Property: io.netty.allocator.normalCacheSize - default 64
	 */
	public static int defaultNormalCacheSize() {
		return DEFAULT_NORMAL_CACHE_SIZE;
	}

	/**
	 * Return {@code true} if direct memory cache alignment is supported, {@code false} otherwise.
	 */
	public static boolean isDirectMemoryCacheAlignmentSupported() {
		return PlatformDependent.hasUnsafe();
	}

	@Override
	public boolean isDirectBufferPooled() {
		return directArenas != null;
	}

	/**
	 * Returns {@code true} if the calling {@link Thread} has a {@link ThreadLocal} cache for the allocated
	 * buffers.
	 */
	@Deprecated
	public boolean hasThreadLocalCache() {
		return threadCache.isSet();
	}

	/**
	 * Free all cached buffers for the calling {@link Thread}.
	 */
	@Deprecated
	public void freeThreadLocalCache() {
		threadCache.remove();
	}

	final class PoolThreadLocalCache extends FastThreadLocal<PoolThreadCache> {
		private final boolean useCacheForAllThreads;

		PoolThreadLocalCache(boolean useCacheForAllThreads) {
			this.useCacheForAllThreads = useCacheForAllThreads;
		}

		@Override
		protected synchronized PoolThreadCache initialValue() {
			final PoolArena<byte[]> heapArena = leastUsedArena(heapArenas);
			final PoolArena<ByteBuffer> directArena = leastUsedArena(directArenas);

			Thread current = Thread.currentThread();
			if (useCacheForAllThreads || current instanceof FastThreadLocalThread) {
				return new PoolThreadCache(
					heapArena, directArena, tinyCacheSize, smallCacheSize, normalCacheSize,
					DEFAULT_MAX_CACHED_BUFFER_CAPACITY, DEFAULT_CACHE_TRIM_INTERVAL);
			}
			// No caching so just use 0 as sizes.
			return new PoolThreadCache(heapArena, directArena, 0, 0, 0, 0, 0);
		}

		@Override
		protected void onRemoval(PoolThreadCache threadCache) {
			threadCache.free(false);
		}

		private <T> PoolArena<T> leastUsedArena(PoolArena<T>[] arenas) {
			if (arenas == null || arenas.length == 0) {
				return null;
			}

			PoolArena<T> minArena = arenas[0];
			for (int i = 1; i < arenas.length; i++) {
				PoolArena<T> arena = arenas[i];
				if (arena.numThreadCaches.get() < minArena.numThreadCaches.get()) {
					minArena = arena;
				}
			}

			return minArena;
		}
	}

	@Override
	public PooledByteBufAllocatorMetric metric() {
		return metric;
	}

	/**
	 * Return the number of heap arenas.
	 *
	 * @deprecated use {@link PooledByteBufAllocatorMetric#numHeapArenas()}.
	 */
	@Deprecated
	public int numHeapArenas() {
		return heapArenaMetrics.size();
	}

	/**
	 * Return the number of direct arenas.
	 *
	 * @deprecated use {@link PooledByteBufAllocatorMetric#numDirectArenas()}.
	 */
	@Deprecated
	public int numDirectArenas() {
		return directArenaMetrics.size();
	}

	/**
	 * Return a {@link List} of all heap {@link PoolArenaMetric}s that are provided by this pool.
	 *
	 * @deprecated use {@link PooledByteBufAllocatorMetric#heapArenas()}.
	 */
	@Deprecated
	public List<PoolArenaMetric> heapArenas() {
		return heapArenaMetrics;
	}

	/**
	 * Return a {@link List} of all direct {@link PoolArenaMetric}s that are provided by this pool.
	 *
	 * @deprecated use {@link PooledByteBufAllocatorMetric#directArenas()}.
	 */
	@Deprecated
	public List<PoolArenaMetric> directArenas() {
		return directArenaMetrics;
	}

	/**
	 * Return the number of thread local caches used by this {@link PooledByteBufAllocator}.
	 *
	 * @deprecated use {@link PooledByteBufAllocatorMetric#numThreadLocalCaches()}.
	 */
	@Deprecated
	public int numThreadLocalCaches() {
		PoolArena<?>[] arenas = heapArenas != null ? heapArenas : directArenas;
		if (arenas == null) {
			return 0;
		}

		int total = 0;
		for (PoolArena<?> arena : arenas) {
			total += arena.numThreadCaches.get();
		}

		return total;
	}

	/**
	 * Return the size of the tiny cache.
	 *
	 * @deprecated use {@link PooledByteBufAllocatorMetric#tinyCacheSize()}.
	 */
	@Deprecated
	public int tinyCacheSize() {
		return tinyCacheSize;
	}

	/**
	 * Return the size of the small cache.
	 *
	 * @deprecated use {@link PooledByteBufAllocatorMetric#smallCacheSize()}.
	 */
	@Deprecated
	public int smallCacheSize() {
		return smallCacheSize;
	}

	/**
	 * Return the size of the normal cache.
	 *
	 * @deprecated use {@link PooledByteBufAllocatorMetric#normalCacheSize()}.
	 */
	@Deprecated
	public int normalCacheSize() {
		return normalCacheSize;
	}

	/**
	 * Return the chunk size for an arena.
	 *
	 * @deprecated use {@link PooledByteBufAllocatorMetric#chunkSize()}.
	 */
	@Deprecated
	public final int chunkSize() {
		return chunkSize;
	}

	final long usedHeapMemory() {
		return usedMemory(heapArenas);
	}

	final long usedDirectMemory() {
		return usedMemory(directArenas);
	}

	private static long usedMemory(PoolArena<?>[] arenas) {
		if (arenas == null) {
			return -1;
		}
		long used = 0;
		for (PoolArena<?> arena : arenas) {
			used += arena.numActiveBytes();
			if (used < 0) {
				return Long.MAX_VALUE;
			}
		}
		return used;
	}

	final PoolThreadCache threadCache() {
		PoolThreadCache cache =  threadCache.get();
		assert cache != null;
		return cache;
	}

	/**
	 * Returns the status of the allocator (which contains all metrics) as string. Be aware this may be expensive
	 * and so should not called too frequently.
	 */
	public String dumpStats() {
		int heapArenasLen = heapArenas == null ? 0 : heapArenas.length;
		StringBuilder buf = new StringBuilder(512)
			.append(heapArenasLen)
			.append(" heap arena(s):")
			.append(StringUtil.NEWLINE);
		if (heapArenasLen > 0) {
			for (PoolArena<byte[]> a: heapArenas) {
				buf.append(a);
			}
		}

		int directArenasLen = directArenas == null ? 0 : directArenas.length;

		buf.append(directArenasLen)
			.append(" direct arena(s):")
			.append(StringUtil.NEWLINE);
		if (directArenasLen > 0) {
			for (PoolArena<ByteBuffer> a: directArenas) {
				buf.append(a);
			}
		}

		return buf.toString();
	}
}
