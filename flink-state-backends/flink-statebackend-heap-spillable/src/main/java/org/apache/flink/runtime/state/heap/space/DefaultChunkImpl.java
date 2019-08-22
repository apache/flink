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

import java.nio.ByteBuffer;

import static org.apache.flink.runtime.state.heap.space.SpaceConstants.BUCKET_SIZE;

/**
 * Default implementation of {@link Chunk}.
 */
public class DefaultChunkImpl extends AbstractChunk {

	/**
	 * The backed byte buffer.
	 */
	private final ByteBuffer byteBuffer;

	/**
	 * Bucket allocator used for this chunk.
	 */
	private final BucketAllocator bucketAllocator;

	DefaultChunkImpl(int chunkId, ByteBuffer byteBuffer, AllocateStrategy allocateStrategy) {
		super(chunkId, byteBuffer.capacity());
		this.byteBuffer = byteBuffer;
		switch (allocateStrategy) {
			case SmallBucket:
				this.bucketAllocator = new PowerTwoBucketAllocator(capacity, BUCKET_SIZE);
				break;
			default:
				this.bucketAllocator = new DirectBucketAllocator(capacity);
		}
	}

	@Override
	public int allocate(int len) {
		return bucketAllocator.allocate(len);
	}

	@Override
	public void free(int interChunkOffset) {
		bucketAllocator.free(interChunkOffset);
	}

	@Override
	public int getOffsetInByteBuffer(int offsetInChunk) {
		return offsetInChunk;
	}

	@Override
	public ByteBuffer getByteBuffer(int chunkOffset) {
		return byteBuffer;
	}
}
