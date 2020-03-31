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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.CompositeByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.PooledByteBufAllocator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;

import scala.Option;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Extends around Netty's {@link PooledByteBufAllocator} with strict control
 * over the number of created arenas.
 */
public class NettyBufferPool extends PooledByteBufAllocator {

	private static final Logger LOG = LoggerFactory.getLogger(NettyBufferPool.class);

	/** <tt>PoolArena&lt;ByteBuffer&gt;[]</tt> via Reflection. */
	private final Object[] directArenas;

	/** Configured number of arenas. */
	private final int numberOfArenas;

	/** Configured chunk size for the arenas. */
	private final int chunkSize;

	/** We strictly prefer direct buffers and disallow heap allocations. */
	private static final boolean PREFER_DIRECT = true;

	/**
	 * Arenas allocate chunks of pageSize << maxOrder bytes. With these defaults, this results in
	 * chunks of 16 MB.
	 *
	 * @see #MAX_ORDER
	 */
	private static final int PAGE_SIZE = 8192;

	/**
	 * Arenas allocate chunks of pageSize << maxOrder bytes. With these defaults, this results in
	 * chunks of 16 MB.
	 *
	 * @see #PAGE_SIZE
	 */
	private static final int MAX_ORDER = 11;

	/**
	 * Creates Netty's buffer pool with the specified number of direct arenas.
	 *
	 * @param numberOfArenas Number of arenas (recommended: 2 * number of task
	 *                       slots)
	 */
	public NettyBufferPool(int numberOfArenas) {
		super(
			PREFER_DIRECT,
			// No heap arenas, please.
			0,
			// Number of direct arenas. Each arena allocates a chunk of 16 MB, i.e.
			// we allocate numDirectArenas * 16 MB of direct memory. This can grow
			// to multiple chunks per arena during runtime, but this should only
			// happen with a large amount of connections per task manager. We
			// control the memory allocations with low/high watermarks when writing
			// to the TCP channels. Chunks are allocated lazily.
			numberOfArenas,
			PAGE_SIZE,
			MAX_ORDER);

		checkArgument(numberOfArenas >= 1, "Number of arenas");
		this.numberOfArenas = numberOfArenas;

		// Arenas allocate chunks of pageSize << maxOrder bytes. With these
		// defaults, this results in chunks of 16 MB.

		this.chunkSize = PAGE_SIZE << MAX_ORDER;

		Object[] allocDirectArenas = null;
		try {
			Field directArenasField = PooledByteBufAllocator.class
					.getDeclaredField("directArenas");
			directArenasField.setAccessible(true);

			allocDirectArenas = (Object[]) directArenasField.get(this);
		} catch (Exception ignored) {
			LOG.warn("Memory statistics not available");
		} finally {
			this.directArenas = allocDirectArenas;
		}
	}

	/**
	 * Returns the number of arenas.
	 *
	 * @return Number of arenas.
	 */
	int getNumberOfArenas() {
		return numberOfArenas;
	}

	/**
	 * Returns the chunk size.
	 *
	 * @return Chunk size.
	 */
	int getChunkSize() {
		return chunkSize;
	}

	// ------------------------------------------------------------------------
	// Direct pool arena stats via Reflection. This is not safe when upgrading
	// Netty versions, but we are currently bound to the version we have (see
	// commit d92e422). In newer Netty versions these statistics are exposed.
	// ------------------------------------------------------------------------

	/**
	 * Returns the number of currently allocated bytes.
	 *
	 * <p>The stats are gathered via Reflection and are mostly relevant for
	 * debugging purposes.
	 *
	 * @return Number of currently allocated bytes.
	 *
	 * @throws NoSuchFieldException   Error getting the statistics (should not
	 *                                happen when the Netty version stays the
	 *                                same).
	 * @throws IllegalAccessException Error getting the statistics (should not
	 *                                happen when the Netty version stays the
	 *                                same).
	 */
	public Option<Long> getNumberOfAllocatedBytes()
			throws NoSuchFieldException, IllegalAccessException {

		if (directArenas != null) {
			long numChunks = 0;
			for (Object arena : directArenas) {
				numChunks += getNumberOfAllocatedChunks(arena, "qInit");
				numChunks += getNumberOfAllocatedChunks(arena, "q000");
				numChunks += getNumberOfAllocatedChunks(arena, "q025");
				numChunks += getNumberOfAllocatedChunks(arena, "q050");
				numChunks += getNumberOfAllocatedChunks(arena, "q075");
				numChunks += getNumberOfAllocatedChunks(arena, "q100");
			}

			long allocatedBytes = numChunks * chunkSize;
			return Option.apply(allocatedBytes);
		} else {
			return Option.empty();
		}
	}

	/**
	 * Returns the number of allocated bytes of the given arena and chunk list.
	 *
	 * @param arena              Arena to gather statistics about.
	 * @param chunkListFieldName Chunk list to check.
	 *
	 * @return Number of total allocated bytes by this arena.
	 *
	 * @throws NoSuchFieldException   Error getting the statistics (should not
	 *                                happen when the Netty version stays the
	 *                                same).
	 * @throws IllegalAccessException Error getting the statistics (should not
	 *                                happen when the Netty version stays the
	 *                                same).
	 */
	private long getNumberOfAllocatedChunks(Object arena, String chunkListFieldName)
			throws NoSuchFieldException, IllegalAccessException {

		// Each PoolArena<ByteBuffer> stores its allocated PoolChunk<ByteBuffer>
		// instances grouped by usage (field qInit, q000, q025, etc.) in
		// PoolChunkList<ByteBuffer> lists. Each list has zero or more
		// PoolChunk<ByteBuffer> instances.

		// Chunk list of arena
		Field chunkListField = arena.getClass().getSuperclass()
				.getDeclaredField(chunkListFieldName);
		chunkListField.setAccessible(true);
		Object chunkList = chunkListField.get(arena);

		// Count the chunks in the list
		Field headChunkField = chunkList.getClass().getDeclaredField("head");
		headChunkField.setAccessible(true);
		Object headChunk = headChunkField.get(chunkList);

		if (headChunk == null) {
			return 0;
		} else {
			int numChunks = 0;

			Object current = headChunk;

			while (current != null) {
				Field nextChunkField = headChunk.getClass().getDeclaredField("next");
				nextChunkField.setAccessible(true);
				current = nextChunkField.get(current);
				numChunks++;
			}

			return numChunks;
		}
	}

	// ------------------------------------------------------------------------
	// Prohibit heap buffer allocations
	// ------------------------------------------------------------------------

	@Override
	public ByteBuf heapBuffer() {
		throw new UnsupportedOperationException("Heap buffer");
	}

	@Override
	public ByteBuf heapBuffer(int initialCapacity) {
		throw new UnsupportedOperationException("Heap buffer");
	}

	@Override
	public ByteBuf heapBuffer(int initialCapacity, int maxCapacity) {
		throw new UnsupportedOperationException("Heap buffer");
	}

	@Override
	public CompositeByteBuf compositeHeapBuffer() {
		throw new UnsupportedOperationException("Heap buffer");
	}

	@Override
	public CompositeByteBuf compositeHeapBuffer(int maxNumComponents) {
		throw new UnsupportedOperationException("Heap buffer");
	}
}
