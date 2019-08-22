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

package org.apache.flink.runtime.state.heap.space;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An abstract base implementation of the {@link ChunkAllocator} interface.
 */
public abstract class AbstractChunkAllocator implements ChunkAllocator {
	private static final Logger LOG = LoggerFactory.getLogger(AbstractChunkAllocator.class);

	private final int chunkSize;

	private ArrayList<ByteBuffer> buffers;

	private AtomicBoolean closed;

	AbstractChunkAllocator(SpaceConfiguration spaceConfiguration) {
		this.chunkSize = spaceConfiguration.getChunkSize();
		this.buffers = new ArrayList<>();
		this.closed = new AtomicBoolean(false);
	}

	@Override
	public Chunk createChunk(int chunkId, AllocateStrategy allocateStrategy) {
		ByteBuffer buffer = allocate(chunkSize);
		buffers.add(buffer);
		return new DefaultChunkImpl(chunkId, buffer, allocateStrategy);
	}

	@Override
	public void close() {
		if (closed.compareAndSet(false, true)) {
			buffers.forEach(this::release);
			buffers.clear();
		} else {
			LOG.warn("This chunk allocator {} has been already closed.", this);
		}
	}

	/**
	 * Allocate a buffer for the chunk.
	 *
	 * @param chunkSize the size of the chunk to allocate.
	 * @return the buffer for the chunk.
	 */
	abstract ByteBuffer allocate(int chunkSize);

	/**
	 * Release the given buffer, when necessary.
	 *
	 * @param buffer the buffer to release resource.
	 */
	abstract void release(ByteBuffer buffer);
}
