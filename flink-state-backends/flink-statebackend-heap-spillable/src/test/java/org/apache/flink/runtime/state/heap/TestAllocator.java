/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.state.heap.space.Allocator;
import org.apache.flink.runtime.state.heap.space.Chunk;
import org.apache.flink.runtime.state.heap.space.SpaceUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.runtime.state.heap.space.Constants.FOUR_BYTES_BITS;
import static org.apache.flink.runtime.state.heap.space.Constants.FOUR_BYTES_MARK;

/**
 * Implementation of {@link Allocator} used for test. This allocator will create a chunk for each
 * allocation request. Size of a chunk is fixed, and only used by one space.
 */
public class TestAllocator extends TestLogger implements Allocator {

    /** Max allocate size supported by this allocator. */
    private final int maxAllocateSize;

    private Map<Integer, TestChunk> chunkMap;

    private volatile int chunkCounter;

    public TestAllocator() {
        this(256);
    }

    public TestAllocator(int maxAllocateSize) {
        this.maxAllocateSize = maxAllocateSize;
        this.chunkMap = new HashMap<>();
        this.chunkCounter = 0;
    }

    @Override
    public synchronized long allocate(int size) {
        Preconditions.checkArgument(
                size <= maxAllocateSize,
                "Can't allocate size of "
                        + size
                        + " larger than maxAllocateSize "
                        + maxAllocateSize);
        int chunkId = chunkCounter++;
        TestChunk testChunk = new TestChunk(chunkId, maxAllocateSize);
        chunkMap.put(chunkId, testChunk);
        int offset = testChunk.allocate(size);
        return ((chunkId & FOUR_BYTES_MARK) << FOUR_BYTES_BITS) | (offset & FOUR_BYTES_MARK);
    }

    @Override
    public synchronized void free(long address) {
        int chunkId = SpaceUtils.getChunkIdByAddress(address);
        int offset = SpaceUtils.getChunkOffsetByAddress(address);
        TestChunk chunk = chunkMap.remove(chunkId);
        if (chunk != null) {
            chunk.free(offset);
        }
    }

    @Override
    public synchronized Chunk getChunkById(int chunkId) {
        TestChunk chunk = chunkMap.get(chunkId);
        Preconditions.checkNotNull(chunk, "chunk " + chunkId + " doest not exist.");
        return chunk;
    }

    @Override
    public synchronized void close() {
        chunkMap.clear();
    }

    public int getMaxAllocateSize() {
        return maxAllocateSize;
    }

    /** Returns total size of used space. */
    public synchronized int getTotalSpaceSize() {
        return chunkMap.size() * maxAllocateSize;
    }

    /** Returns number of used space. */
    public synchronized int getTotalSpaceNumber() {
        return chunkMap.size();
    }

    /** Implementation of {@link Chunk} used for test. A chunk can only be used by one space. */
    public static class TestChunk implements Chunk {

        private final int chunkId;
        private final int size;
        private final MemorySegment segment;
        private final int offset;
        private volatile boolean used;

        TestChunk(int chunkId, int size) {
            this.offset = 14;
            this.chunkId = chunkId;
            this.size = size + offset;
            this.segment = MemorySegmentFactory.allocateUnpooledSegment(size);
        }

        @Override
        public synchronized int allocate(int len) {
            Preconditions.checkState(!used, "chunk has been allocated.");
            Preconditions.checkState(len <= size, "There is no enough size.");
            used = true;
            return offset;
        }

        @Override
        public synchronized void free(int interChunkOffset) {
            used = false;
        }

        @Override
        public int getChunkId() {
            return chunkId;
        }

        @Override
        public int getChunkCapacity() {
            return size;
        }

        @Override
        public MemorySegment getMemorySegment(int chunkOffset) {
            return segment;
        }

        @Override
        public int getOffsetInSegment(int offsetInChunk) {
            return offsetInChunk;
        }
    }
}
