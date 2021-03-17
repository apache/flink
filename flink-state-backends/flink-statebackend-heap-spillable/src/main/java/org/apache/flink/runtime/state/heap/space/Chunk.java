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

import org.apache.flink.core.memory.MemorySegment;

import java.nio.ByteBuffer;

/**
 * Chunk is a logically contiguous space backed by one or multiple {@link ByteBuffer}.
 *
 * <p>For example: a Chunk of 1G size may be backed by one {@link java.nio.MappedByteBuffer} from a
 * memory-mapped 1G file, or multiple {@link java.nio.HeapByteBuffer}/{@link
 * java.nio.DirectByteBuffer}.
 */
public interface Chunk {
    /**
     * Try to allocate size bytes from the chunk.
     *
     * @param len size of bytes to allocate.
     * @return the offset of the successful allocation, or -1 to indicate not-enough-space
     */
    int allocate(int len);

    /**
     * release the space addressed by interChunkOffset.
     *
     * @param interChunkOffset offset of the chunk
     */
    void free(int interChunkOffset);

    /** @return Id of this Chunk */
    int getChunkId();

    int getChunkCapacity();

    /** @return This chunk's backing MemorySegment described by chunkOffset. */
    MemorySegment getMemorySegment(int chunkOffset);

    /**
     * @param offsetInChunk virtual and global address in chunk
     * @return chunk maybe compose of multi {@link MemorySegment}s, return the offset in certain
     *     one.
     */
    int getOffsetInSegment(int offsetInChunk);
}
