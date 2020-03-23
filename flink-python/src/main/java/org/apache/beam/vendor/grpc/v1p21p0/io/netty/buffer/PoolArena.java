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

import org.apache.beam.vendor.grpc.v1p21p0.io.netty.util.internal.LongCounter;
import org.apache.beam.vendor.grpc.v1p21p0.io.netty.util.internal.PlatformDependent;
import org.apache.beam.vendor.grpc.v1p21p0.io.netty.util.internal.StringUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.beam.vendor.grpc.v1p21p0.io.netty.util.internal.ObjectUtil.checkPositiveOrZero;
import static java.lang.Math.max;

// This class is copied from Netty's io.netty.buffer.PoolArena,
// can be removed after Beam bumps its shaded netty version to 1.22+ (BEAM-9030).
//
// Changed lines: 284, 295, 297~300

abstract class PoolArena<T> implements PoolArenaMetric {
	static final boolean HAS_UNSAFE = PlatformDependent.hasUnsafe();

	enum SizeClass {
		Tiny,
		Small,
		Normal
	}

	static final int numTinySubpagePools = 512 >>> 4;

	final PooledByteBufAllocator parent;

	private final int maxOrder;
	final int pageSize;
	final int pageShifts;
	final int chunkSize;
	final int subpageOverflowMask;
	final int numSmallSubpagePools;
	final int directMemoryCacheAlignment;
	final int directMemoryCacheAlignmentMask;
	private final PoolSubpage<T>[] tinySubpagePools;
	private final PoolSubpage<T>[] smallSubpagePools;

	private final PoolChunkList<T> q050;
	private final PoolChunkList<T> q025;
	private final PoolChunkList<T> q000;
	private final PoolChunkList<T> qInit;
	private final PoolChunkList<T> q075;
	private final PoolChunkList<T> q100;

	private final List<PoolChunkListMetric> chunkListMetrics;

	// Metrics for allocations and deallocations
	private long allocationsNormal;
	// We need to use the LongCounter here as this is not guarded via synchronized block.
	private final LongCounter allocationsTiny = PlatformDependent.newLongCounter();
	private final LongCounter allocationsSmall = PlatformDependent.newLongCounter();
	private final LongCounter allocationsHuge = PlatformDependent.newLongCounter();
	private final LongCounter activeBytesHuge = PlatformDependent.newLongCounter();

	private long deallocationsTiny;
	private long deallocationsSmall;
	private long deallocationsNormal;

	// We need to use the LongCounter here as this is not guarded via synchronized block.
	private final LongCounter deallocationsHuge = PlatformDependent.newLongCounter();

	// Number of thread caches backed by this arena.
	final AtomicInteger numThreadCaches = new AtomicInteger();

	// TODO: Test if adding padding helps under contention
	//private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

	protected PoolArena(PooledByteBufAllocator parent, int pageSize,
						int maxOrder, int pageShifts, int chunkSize, int cacheAlignment) {
		this.parent = parent;
		this.pageSize = pageSize;
		this.maxOrder = maxOrder;
		this.pageShifts = pageShifts;
		this.chunkSize = chunkSize;
		directMemoryCacheAlignment = cacheAlignment;
		directMemoryCacheAlignmentMask = cacheAlignment - 1;
		subpageOverflowMask = ~(pageSize - 1);
		tinySubpagePools = newSubpagePoolArray(numTinySubpagePools);
		for (int i = 0; i < tinySubpagePools.length; i ++) {
			tinySubpagePools[i] = newSubpagePoolHead(pageSize);
		}

		numSmallSubpagePools = pageShifts - 9;
		smallSubpagePools = newSubpagePoolArray(numSmallSubpagePools);
		for (int i = 0; i < smallSubpagePools.length; i ++) {
			smallSubpagePools[i] = newSubpagePoolHead(pageSize);
		}

		q100 = new PoolChunkList<T>(this, null, 100, Integer.MAX_VALUE, chunkSize);
		q075 = new PoolChunkList<T>(this, q100, 75, 100, chunkSize);
		q050 = new PoolChunkList<T>(this, q075, 50, 100, chunkSize);
		q025 = new PoolChunkList<T>(this, q050, 25, 75, chunkSize);
		q000 = new PoolChunkList<T>(this, q025, 1, 50, chunkSize);
		qInit = new PoolChunkList<T>(this, q000, Integer.MIN_VALUE, 25, chunkSize);

		q100.prevList(q075);
		q075.prevList(q050);
		q050.prevList(q025);
		q025.prevList(q000);
		q000.prevList(null);
		qInit.prevList(qInit);

		List<PoolChunkListMetric> metrics = new ArrayList<PoolChunkListMetric>(6);
		metrics.add(qInit);
		metrics.add(q000);
		metrics.add(q025);
		metrics.add(q050);
		metrics.add(q075);
		metrics.add(q100);
		chunkListMetrics = Collections.unmodifiableList(metrics);
	}

	private PoolSubpage<T> newSubpagePoolHead(int pageSize) {
		PoolSubpage<T> head = new PoolSubpage<T>(pageSize);
		head.prev = head;
		head.next = head;
		return head;
	}

	@SuppressWarnings("unchecked")
	private PoolSubpage<T>[] newSubpagePoolArray(int size) {
		return new PoolSubpage[size];
	}

	abstract boolean isDirect();

	PooledByteBuf<T> allocate(PoolThreadCache cache, int reqCapacity, int maxCapacity) {
		PooledByteBuf<T> buf = newByteBuf(maxCapacity);
		allocate(cache, buf, reqCapacity);
		return buf;
	}

	static int tinyIdx(int normCapacity) {
		return normCapacity >>> 4;
	}

	static int smallIdx(int normCapacity) {
		int tableIdx = 0;
		int i = normCapacity >>> 10;
		while (i != 0) {
			i >>>= 1;
			tableIdx ++;
		}
		return tableIdx;
	}

	// capacity < pageSize
	boolean isTinyOrSmall(int normCapacity) {
		return (normCapacity & subpageOverflowMask) == 0;
	}

	// normCapacity < 512
	static boolean isTiny(int normCapacity) {
		return (normCapacity & 0xFFFFFE00) == 0;
	}

	private void allocate(PoolThreadCache cache, PooledByteBuf<T> buf, final int reqCapacity) {
		final int normCapacity = normalizeCapacity(reqCapacity);
		if (isTinyOrSmall(normCapacity)) { // capacity < pageSize
			int tableIdx;
			PoolSubpage<T>[] table;
			boolean tiny = isTiny(normCapacity);
			if (tiny) { // < 512
				if (cache.allocateTiny(this, buf, reqCapacity, normCapacity)) {
					// was able to allocate out of the cache so move on
					return;
				}
				tableIdx = tinyIdx(normCapacity);
				table = tinySubpagePools;
			} else {
				if (cache.allocateSmall(this, buf, reqCapacity, normCapacity)) {
					// was able to allocate out of the cache so move on
					return;
				}
				tableIdx = smallIdx(normCapacity);
				table = smallSubpagePools;
			}

			final PoolSubpage<T> head = table[tableIdx];

			/**
			 * Synchronize on the head. This is needed as {@link PoolChunk#allocateSubpage(int)} and
			 * {@link PoolChunk#free(long)} may modify the doubly linked list as well.
			 */
			synchronized (head) {
				final PoolSubpage<T> s = head.next;
				if (s != head) {
					assert s.doNotDestroy && s.elemSize == normCapacity;
					long handle = s.allocate();
					assert handle >= 0;
					s.chunk.initBufWithSubpage(buf, null, handle, reqCapacity);
					incTinySmallAllocation(tiny);
					return;
				}
			}
			synchronized (this) {
				allocateNormal(buf, reqCapacity, normCapacity);
			}

			incTinySmallAllocation(tiny);
			return;
		}
		if (normCapacity <= chunkSize) {
			if (cache.allocateNormal(this, buf, reqCapacity, normCapacity)) {
				// was able to allocate out of the cache so move on
				return;
			}
			synchronized (this) {
				allocateNormal(buf, reqCapacity, normCapacity);
				++allocationsNormal;
			}
		} else {
			// Huge allocations are never served via the cache so just call allocateHuge
			allocateHuge(buf, reqCapacity);
		}
	}

	// Method must be called inside synchronized(this) { ... } block
	private void allocateNormal(PooledByteBuf<T> buf, int reqCapacity, int normCapacity) {
		if (q050.allocate(buf, reqCapacity, normCapacity) || q025.allocate(buf, reqCapacity, normCapacity) ||
			q000.allocate(buf, reqCapacity, normCapacity) || qInit.allocate(buf, reqCapacity, normCapacity) ||
			q075.allocate(buf, reqCapacity, normCapacity)) {
			return;
		}

		// Add a new chunk.
		PoolChunk<T> c = newChunk(pageSize, maxOrder, pageShifts, chunkSize);
		boolean success = c.allocate(buf, reqCapacity, normCapacity);
		assert success;
		qInit.add(c);
	}

	private void incTinySmallAllocation(boolean tiny) {
		if (tiny) {
			allocationsTiny.increment();
		} else {
			allocationsSmall.increment();
		}
	}

	private void allocateHuge(PooledByteBuf<T> buf, int reqCapacity) {
		PoolChunk<T> chunk = newUnpooledChunk(reqCapacity);
		activeBytesHuge.add(chunk.chunkSize());
		buf.initUnpooled(chunk, reqCapacity);
		allocationsHuge.increment();
	}

	void free(PoolChunk<T> chunk, ByteBuffer nioBuffer, long handle, int normCapacity, PoolThreadCache cache) {
		if (chunk.unpooled) {
			int size = chunk.chunkSize();
			destroyChunk(chunk);
			activeBytesHuge.add(-size);
			deallocationsHuge.increment();
		} else {
			SizeClass sizeClass = sizeClass(normCapacity);
			if (cache != null && cache.add(this, chunk, nioBuffer, handle, normCapacity, sizeClass)) {
				// cached so not free it.
				return;
			}

			freeChunk(chunk, handle, sizeClass, nioBuffer, false);
		}
	}

	private SizeClass sizeClass(int normCapacity) {
		if (!isTinyOrSmall(normCapacity)) {
			return SizeClass.Normal;
		}
		return isTiny(normCapacity) ? SizeClass.Tiny : SizeClass.Small;
	}

	void freeChunk(PoolChunk<T> chunk, long handle, SizeClass sizeClass, ByteBuffer nioBuffer, boolean finalizer) {
		final boolean destroyChunk;
		synchronized (this) {
			// We only call this if freeChunk is not called because of the PoolThreadCache finalizer as otherwise this
			// may fail due lazy class-loading in for example tomcat.
			if (!finalizer) {
				switch (sizeClass) {
					case Normal:
						++deallocationsNormal;
						break;
					case Small:
						++deallocationsSmall;
						break;
					case Tiny:
						++deallocationsTiny;
						break;
					default:
						throw new Error();
				}
			}
			destroyChunk = !chunk.parent.free(chunk, handle, nioBuffer);
		}
		if (destroyChunk) {
			// destroyChunk not need to be called while holding the synchronized lock.
			destroyChunk(chunk);
		}
	}

	PoolSubpage<T> findSubpagePoolHead(int elemSize) {
		int tableIdx;
		PoolSubpage<T>[] table;
		if (isTiny(elemSize)) { // < 512
			tableIdx = elemSize >>> 4;
			table = tinySubpagePools;
		} else {
			tableIdx = 0;
			elemSize >>>= 10;
			while (elemSize != 0) {
				elemSize >>>= 1;
				tableIdx ++;
			}
			table = smallSubpagePools;
		}

		return table[tableIdx];
	}

	int normalizeCapacity(int reqCapacity) {
		checkPositiveOrZero(reqCapacity, "reqCapacity");

		if (reqCapacity >= chunkSize) {
			return directMemoryCacheAlignment == 0 ? reqCapacity : alignCapacity(reqCapacity);
		}

		if (!isTiny(reqCapacity)) { // >= 512
			// Doubled

			int normalizedCapacity = reqCapacity;
			normalizedCapacity --;
			normalizedCapacity |= normalizedCapacity >>>  1;
			normalizedCapacity |= normalizedCapacity >>>  2;
			normalizedCapacity |= normalizedCapacity >>>  4;
			normalizedCapacity |= normalizedCapacity >>>  8;
			normalizedCapacity |= normalizedCapacity >>> 16;
			normalizedCapacity ++;

			if (normalizedCapacity < 0) {
				normalizedCapacity >>>= 1;
			}
			assert directMemoryCacheAlignment == 0 || (normalizedCapacity & directMemoryCacheAlignmentMask) == 0;

			return normalizedCapacity;
		}

		if (directMemoryCacheAlignment > 0) {
			return alignCapacity(reqCapacity);
		}

		// Quantum-spaced
		if ((reqCapacity & 15) == 0) {
			return reqCapacity;
		}

		return (reqCapacity & ~15) + 16;
	}

	int alignCapacity(int reqCapacity) {
		int delta = reqCapacity & directMemoryCacheAlignmentMask;
		return delta == 0 ? reqCapacity : reqCapacity + directMemoryCacheAlignment - delta;
	}

	void reallocate(PooledByteBuf<T> buf, int newCapacity, boolean freeOldMemory) {
		if (newCapacity < 0 || newCapacity > buf.maxCapacity()) {
			throw new IllegalArgumentException("newCapacity: " + newCapacity);
		}

		int oldCapacity = buf.length;
		if (oldCapacity == newCapacity) {
			return;
		}

		PoolChunk<T> oldChunk = buf.chunk;
		ByteBuffer oldNioBuffer = buf.tmpNioBuf;
		long oldHandle = buf.handle;
		T oldMemory = buf.memory;
		int oldOffset = buf.offset;
		int oldMaxLength = buf.maxLength;
		int readerIndex = buf.readerIndex();
		int writerIndex = buf.writerIndex();

		allocate(parent.threadCache(), buf, newCapacity);
		if (newCapacity > oldCapacity) {
			memoryCopy(
				oldMemory, oldOffset,
				buf.memory, buf.offset, oldCapacity);
		} else if (newCapacity < oldCapacity) {
			if (readerIndex < newCapacity) {
				if (writerIndex > newCapacity) {
					writerIndex = newCapacity;
				}
				memoryCopy(
					oldMemory, oldOffset + readerIndex,
					buf.memory, buf.offset + readerIndex, writerIndex - readerIndex);
			} else {
				readerIndex = writerIndex = newCapacity;
			}
		}

		buf.setIndex(readerIndex, writerIndex);

		if (freeOldMemory) {
			free(oldChunk, oldNioBuffer, oldHandle, oldMaxLength, buf.cache);
		}
	}

	@Override
	public int numThreadCaches() {
		return numThreadCaches.get();
	}

	@Override
	public int numTinySubpages() {
		return tinySubpagePools.length;
	}

	@Override
	public int numSmallSubpages() {
		return smallSubpagePools.length;
	}

	@Override
	public int numChunkLists() {
		return chunkListMetrics.size();
	}

	@Override
	public List<PoolSubpageMetric> tinySubpages() {
		return subPageMetricList(tinySubpagePools);
	}

	@Override
	public List<PoolSubpageMetric> smallSubpages() {
		return subPageMetricList(smallSubpagePools);
	}

	@Override
	public List<PoolChunkListMetric> chunkLists() {
		return chunkListMetrics;
	}

	private static List<PoolSubpageMetric> subPageMetricList(PoolSubpage<?>[] pages) {
		List<PoolSubpageMetric> metrics = new ArrayList<PoolSubpageMetric>();
		for (PoolSubpage<?> head : pages) {
			if (head.next == head) {
				continue;
			}
			PoolSubpage<?> s = head.next;
			for (;;) {
				metrics.add(s);
				s = s.next;
				if (s == head) {
					break;
				}
			}
		}
		return metrics;
	}

	@Override
	public long numAllocations() {
		final long allocsNormal;
		synchronized (this) {
			allocsNormal = allocationsNormal;
		}
		return allocationsTiny.value() + allocationsSmall.value() + allocsNormal + allocationsHuge.value();
	}

	@Override
	public long numTinyAllocations() {
		return allocationsTiny.value();
	}

	@Override
	public long numSmallAllocations() {
		return allocationsSmall.value();
	}

	@Override
	public synchronized long numNormalAllocations() {
		return allocationsNormal;
	}

	@Override
	public long numDeallocations() {
		final long deallocs;
		synchronized (this) {
			deallocs = deallocationsTiny + deallocationsSmall + deallocationsNormal;
		}
		return deallocs + deallocationsHuge.value();
	}

	@Override
	public synchronized long numTinyDeallocations() {
		return deallocationsTiny;
	}

	@Override
	public synchronized long numSmallDeallocations() {
		return deallocationsSmall;
	}

	@Override
	public synchronized long numNormalDeallocations() {
		return deallocationsNormal;
	}

	@Override
	public long numHugeAllocations() {
		return allocationsHuge.value();
	}

	@Override
	public long numHugeDeallocations() {
		return deallocationsHuge.value();
	}

	@Override
	public  long numActiveAllocations() {
		long val = allocationsTiny.value() + allocationsSmall.value() + allocationsHuge.value()
			- deallocationsHuge.value();
		synchronized (this) {
			val += allocationsNormal - (deallocationsTiny + deallocationsSmall + deallocationsNormal);
		}
		return max(val, 0);
	}

	@Override
	public long numActiveTinyAllocations() {
		return max(numTinyAllocations() - numTinyDeallocations(), 0);
	}

	@Override
	public long numActiveSmallAllocations() {
		return max(numSmallAllocations() - numSmallDeallocations(), 0);
	}

	@Override
	public long numActiveNormalAllocations() {
		final long val;
		synchronized (this) {
			val = allocationsNormal - deallocationsNormal;
		}
		return max(val, 0);
	}

	@Override
	public long numActiveHugeAllocations() {
		return max(numHugeAllocations() - numHugeDeallocations(), 0);
	}

	@Override
	public long numActiveBytes() {
		long val = activeBytesHuge.value();
		synchronized (this) {
			for (int i = 0; i < chunkListMetrics.size(); i++) {
				for (PoolChunkMetric m: chunkListMetrics.get(i)) {
					val += m.chunkSize();
				}
			}
		}
		return max(0, val);
	}

	protected abstract PoolChunk<T> newChunk(int pageSize, int maxOrder, int pageShifts, int chunkSize);
	protected abstract PoolChunk<T> newUnpooledChunk(int capacity);
	protected abstract PooledByteBuf<T> newByteBuf(int maxCapacity);
	protected abstract void memoryCopy(T src, int srcOffset, T dst, int dstOffset, int length);
	protected abstract void destroyChunk(PoolChunk<T> chunk);

	@Override
	public synchronized String toString() {
		StringBuilder buf = new StringBuilder()
			.append("Chunk(s) at 0~25%:")
			.append(StringUtil.NEWLINE)
			.append(qInit)
			.append(StringUtil.NEWLINE)
			.append("Chunk(s) at 0~50%:")
			.append(StringUtil.NEWLINE)
			.append(q000)
			.append(StringUtil.NEWLINE)
			.append("Chunk(s) at 25~75%:")
			.append(StringUtil.NEWLINE)
			.append(q025)
			.append(StringUtil.NEWLINE)
			.append("Chunk(s) at 50~100%:")
			.append(StringUtil.NEWLINE)
			.append(q050)
			.append(StringUtil.NEWLINE)
			.append("Chunk(s) at 75~100%:")
			.append(StringUtil.NEWLINE)
			.append(q075)
			.append(StringUtil.NEWLINE)
			.append("Chunk(s) at 100%:")
			.append(StringUtil.NEWLINE)
			.append(q100)
			.append(StringUtil.NEWLINE)
			.append("tiny subpages:");
		appendPoolSubPages(buf, tinySubpagePools);
		buf.append(StringUtil.NEWLINE)
			.append("small subpages:");
		appendPoolSubPages(buf, smallSubpagePools);
		buf.append(StringUtil.NEWLINE);

		return buf.toString();
	}

	private static void appendPoolSubPages(StringBuilder buf, PoolSubpage<?>[] subpages) {
		for (int i = 0; i < subpages.length; i ++) {
			PoolSubpage<?> head = subpages[i];
			if (head.next == head) {
				continue;
			}

			buf.append(StringUtil.NEWLINE)
				.append(i)
				.append(": ");
			PoolSubpage<?> s = head.next;
			for (;;) {
				buf.append(s);
				s = s.next;
				if (s == head) {
					break;
				}
			}
		}
	}

	@Override
	protected final void finalize() throws Throwable {
		try {
			super.finalize();
		} finally {
			destroyPoolSubPages(smallSubpagePools);
			destroyPoolSubPages(tinySubpagePools);
			destroyPoolChunkLists(qInit, q000, q025, q050, q075, q100);
		}
	}

	private static void destroyPoolSubPages(PoolSubpage<?>[] pages) {
		for (PoolSubpage<?> page : pages) {
			page.destroy();
		}
	}

	private void destroyPoolChunkLists(PoolChunkList<T>... chunkLists) {
		for (PoolChunkList<T> chunkList: chunkLists) {
			chunkList.destroy(this);
		}
	}

	static final class HeapArena extends PoolArena<byte[]> {

		HeapArena(PooledByteBufAllocator parent, int pageSize, int maxOrder,
				  int pageShifts, int chunkSize, int directMemoryCacheAlignment) {
			super(parent, pageSize, maxOrder, pageShifts, chunkSize,
				directMemoryCacheAlignment);
		}

		private static byte[] newByteArray(int size) {
			return PlatformDependent.allocateUninitializedArray(size);
		}

		@Override
		boolean isDirect() {
			return false;
		}

		@Override
		protected PoolChunk<byte[]> newChunk(int pageSize, int maxOrder, int pageShifts, int chunkSize) {
			return new PoolChunk<byte[]>(this, newByteArray(chunkSize), pageSize, maxOrder, pageShifts, chunkSize, 0);
		}

		@Override
		protected PoolChunk<byte[]> newUnpooledChunk(int capacity) {
			return new PoolChunk<byte[]>(this, newByteArray(capacity), capacity, 0);
		}

		@Override
		protected void destroyChunk(PoolChunk<byte[]> chunk) {
			// Rely on GC.
		}

		@Override
		protected PooledByteBuf<byte[]> newByteBuf(int maxCapacity) {
			return HAS_UNSAFE ? PooledUnsafeHeapByteBuf.newUnsafeInstance(maxCapacity)
				: PooledHeapByteBuf.newInstance(maxCapacity);
		}

		@Override
		protected void memoryCopy(byte[] src, int srcOffset, byte[] dst, int dstOffset, int length) {
			if (length == 0) {
				return;
			}

			System.arraycopy(src, srcOffset, dst, dstOffset, length);
		}
	}

	static final class DirectArena extends PoolArena<ByteBuffer> {

		DirectArena(PooledByteBufAllocator parent, int pageSize, int maxOrder,
					int pageShifts, int chunkSize, int directMemoryCacheAlignment) {
			super(parent, pageSize, maxOrder, pageShifts, chunkSize,
				directMemoryCacheAlignment);
		}

		@Override
		boolean isDirect() {
			return true;
		}

		// mark as package-private, only for unit test
		int offsetCacheLine(ByteBuffer memory) {
			// We can only calculate the offset if Unsafe is present as otherwise directBufferAddress(...) will
			// throw an NPE.
			int remainder = HAS_UNSAFE
				? (int) (PlatformDependent.directBufferAddress(memory) & directMemoryCacheAlignmentMask)
				: 0;

			// offset = alignment - address & (alignment - 1)
			return directMemoryCacheAlignment - remainder;
		}

		@Override
		protected PoolChunk<ByteBuffer> newChunk(int pageSize, int maxOrder,
												 int pageShifts, int chunkSize) {
			if (directMemoryCacheAlignment == 0) {
				return new PoolChunk<ByteBuffer>(this,
					allocateDirect(chunkSize), pageSize, maxOrder,
					pageShifts, chunkSize, 0);
			}
			final ByteBuffer memory = allocateDirect(chunkSize
				+ directMemoryCacheAlignment);
			return new PoolChunk<ByteBuffer>(this, memory, pageSize,
				maxOrder, pageShifts, chunkSize,
				offsetCacheLine(memory));
		}

		@Override
		protected PoolChunk<ByteBuffer> newUnpooledChunk(int capacity) {
			if (directMemoryCacheAlignment == 0) {
				return new PoolChunk<ByteBuffer>(this,
					allocateDirect(capacity), capacity, 0);
			}
			final ByteBuffer memory = allocateDirect(capacity
				+ directMemoryCacheAlignment);
			return new PoolChunk<ByteBuffer>(this, memory, capacity,
				offsetCacheLine(memory));
		}

		private static ByteBuffer allocateDirect(int capacity) {
			return PlatformDependent.useDirectBufferNoCleaner() ?
				PlatformDependent.allocateDirectNoCleaner(capacity) : ByteBuffer.allocateDirect(capacity);
		}

		@Override
		protected void destroyChunk(PoolChunk<ByteBuffer> chunk) {
			if (PlatformDependent.useDirectBufferNoCleaner()) {
				PlatformDependent.freeDirectNoCleaner(chunk.memory);
			} else {
				PlatformDependent.freeDirectBuffer(chunk.memory);
			}
		}

		@Override
		protected PooledByteBuf<ByteBuffer> newByteBuf(int maxCapacity) {
			if (HAS_UNSAFE) {
				return PooledUnsafeDirectByteBuf.newInstance(maxCapacity);
			} else {
				return PooledDirectByteBuf.newInstance(maxCapacity);
			}
		}

		@Override
		protected void memoryCopy(ByteBuffer src, int srcOffset, ByteBuffer dst, int dstOffset, int length) {
			if (length == 0) {
				return;
			}

			if (HAS_UNSAFE) {
				PlatformDependent.copyMemory(
					PlatformDependent.directBufferAddress(src) + srcOffset,
					PlatformDependent.directBufferAddress(dst) + dstOffset, length);
			} else {
				// We must duplicate the NIO buffers because they may be accessed by other Netty buffers.
				src = src.duplicate();
				dst = dst.duplicate();
				src.position(srcOffset).limit(srcOffset + length);
				dst.position(dstOffset);
				dst.put(src);
			}
		}
	}
}
