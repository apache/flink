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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.state.heap.space.SpaceConstants.BUCKET_SIZE;
import static org.apache.flink.runtime.state.heap.space.SpaceConstants.FOUR_BYTES_BITS;
import static org.apache.flink.runtime.state.heap.space.SpaceConstants.FOUR_BYTES_MARK;
import static org.apache.flink.runtime.state.heap.space.SpaceConstants.NO_SPACE;

/**
 * An implementation of {@link Allocator} which is not thread safety.
 */
public class SpaceAllocator implements Allocator {

	/** Configuration for space. */
	private final SpaceConfiguration spaceConfiguration;

	/**
	 * Array of all chunks.
	 */
	private volatile Chunk[] totalSpace = new Chunk[16];

	/**
	 * List of chunks used to allocate space for data less than bucket size.
	 */
	private final List<Chunk> totalSpaceForNormal = new ArrayList<>();

	/**
	 * List of chunks used to allocate space for data bigger than bucket size.
	 */
	private final List<Chunk> totalSpaceForHuge = new ArrayList<>();

	/**
	 * Generator for chunk id.
	 */
	private final AtomicInteger chunkIdGenerator = new AtomicInteger(0);

	/**
	 * The chunk allocator.
	 */
	private final ChunkAllocator chunkAllocator;

	public SpaceAllocator(SpaceConfiguration configuration) {
		this.spaceConfiguration = Preconditions.checkNotNull(configuration);
		this.chunkAllocator = ChunkAllocator.createChunkAllocator(spaceConfiguration);
		if (spaceConfiguration.isPreAllocate()) {
			init();
		}
	}

	@VisibleForTesting
	SpaceAllocator(SpaceConfiguration configuration, ChunkAllocator chunkAllocator) {
		this.spaceConfiguration = Preconditions.checkNotNull(configuration);
		this.chunkAllocator = Preconditions.checkNotNull(chunkAllocator);
	}

	private void init() {
		Chunk chunk = createChunk(AllocateStrategy.SmallBucket);
		totalSpaceForNormal.add(chunk);
		addTotalSpace(chunk, chunk.getChunkId());
	}

	@VisibleForTesting
	void addTotalSpace(Chunk chunk, int chunkId) {
		if (chunkId >= this.totalSpace.length) {
			Chunk[] chunkTemp = new Chunk[this.totalSpace.length * 2];
			System.arraycopy(this.totalSpace, 0, chunkTemp, 0, this.totalSpace.length);
			this.totalSpace = chunkTemp;
		}
		totalSpace[chunkId] = chunk;
	}

	@Override
	public long allocate(int len) {
		if (len >= BUCKET_SIZE) {
			return doAllocate(totalSpaceForHuge, len, AllocateStrategy.HugeBucket);
		} else {
			return doAllocate(totalSpaceForNormal, len, AllocateStrategy.SmallBucket);
		}
	}

	@Override
	public void free(long offset) {
		int chunkId = SpaceUtils.getChunkIdByAddress(offset);
		int interChunkOffset = SpaceUtils.getChunkOffsetByAddress(offset);
		getChunkById(chunkId).free(interChunkOffset);
	}

	@Override
	public Chunk getChunkById(int chunkId) {
		return totalSpace[chunkId];
	}

	private long doAllocate(List<Chunk> chunks, int len, AllocateStrategy allocateStrategy) {
		int offset;
		for (Chunk chunk : chunks) {
			offset = chunk.allocate(len);
			if (offset != NO_SPACE) {
				return ((chunk.getChunkId() & FOUR_BYTES_MARK) << FOUR_BYTES_BITS) | (offset & FOUR_BYTES_MARK);
			}
		}

		Chunk chunk = createChunk(allocateStrategy);
		chunks.add(chunk);
		addTotalSpace(chunk, chunk.getChunkId());
		offset = chunk.allocate(len);

		if (offset != NO_SPACE) {
			return ((chunk.getChunkId() & FOUR_BYTES_MARK) << FOUR_BYTES_BITS) | (offset & FOUR_BYTES_MARK);
		}

		throw new RuntimeException("There is no space to allocate");
	}

	private Chunk createChunk(AllocateStrategy allocateStrategy) {
		int chunkId = chunkIdGenerator.getAndIncrement();
		return chunkAllocator.createChunk(chunkId, allocateStrategy);
	}

	@Override
	public void close() throws IOException {
		chunkAllocator.close();
	}

	@VisibleForTesting
	ChunkAllocator getChunkAllocator() {
		return chunkAllocator;
	}

	@VisibleForTesting
	AtomicInteger getChunkIdGenerator() {
		return chunkIdGenerator;
	}
}
