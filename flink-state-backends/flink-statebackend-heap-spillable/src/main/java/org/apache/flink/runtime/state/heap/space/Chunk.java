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

/**
 * Chunk is a contiguous byteBuffer. or logically contiguous space .
 * for example: a Chunk is 1G space, maybe it's one big file, or multi 4M on-heap ByteBuffer
 */
public interface Chunk {
	/**
	 * Try to allocate size bytes from the chunk. spaceSizeInfo will record occupied space size.
	 *
	 * @return the offset of the successful allocation, or -1 to indicate not-enough-space
	 */
	int allocate(int len);

	/**
	 * release the space addressed by interChunkOffset. spaceSizeInfo will record occupied space size.
	 *
	 * @param interChunkOffset offset of the chunk
	 */
	void free(int interChunkOffset);

	/**
	 * @return Id of this Chunk
	 */
	int getChunkId();

	int getChunkCapacity();

	/**
	 * @return This chunk's backing ByteBuffer described by chunkOffset.
	 */
	ByteBuffer getByteBuffer(int chunkOffset);

	/**
	 * @param offsetInChunk virtual and globle adrress in chunk
	 * @return chunk maybe compose of multi ByteBuffers, return the offset in certain ByteBuffer.
	 */
	int getOffsetInByteBuffer(int offsetInChunk);

	@SuppressWarnings("unused")
	long usedSize();
}
