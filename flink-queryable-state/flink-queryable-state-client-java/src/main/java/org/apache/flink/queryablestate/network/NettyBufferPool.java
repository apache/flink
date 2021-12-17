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

package org.apache.flink.queryablestate.network;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.buffer.CompositeByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.PooledByteBufAllocator;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Wrapper around Netty's {@link PooledByteBufAllocator} with strict control over the number of
 * created arenas.
 */
public class NettyBufferPool implements ByteBufAllocator {

    /** The wrapped buffer allocator. */
    private final PooledByteBufAllocator alloc;

    /**
     * Creates Netty's buffer pool with the specified number of direct arenas.
     *
     * @param numberOfArenas Number of arenas (recommended: 2 * number of task slots)
     */
    public NettyBufferPool(int numberOfArenas) {
        checkArgument(numberOfArenas >= 1, "Number of arenas");

        // We strictly prefer direct buffers and disallow heap allocations.
        boolean preferDirect = true;

        // Arenas allocate chunks of pageSize << maxOrder bytes. With these
        // defaults, this results in chunks of 16 MB.
        int pageSize = 8192;
        int maxOrder = 11;

        // Number of direct arenas. Each arena allocates a chunk of 16 MB, i.e.
        // we allocate numDirectArenas * 16 MB of direct memory. This can grow
        // to multiple chunks per arena during runtime, but this should only
        // happen with a large amount of connections per task manager. We
        // control the memory allocations with low/high watermarks when writing
        // to the TCP channels. Chunks are allocated lazily.
        int numDirectArenas = numberOfArenas;

        // No heap arenas, please.
        int numHeapArenas = 0;

        this.alloc =
                new PooledByteBufAllocator(
                        preferDirect, numHeapArenas, numDirectArenas, pageSize, maxOrder);
    }

    // ------------------------------------------------------------------------
    // Delegate calls to the allocated and prohibit heap buffer allocations
    // ------------------------------------------------------------------------

    @Override
    public ByteBuf buffer() {
        return alloc.buffer();
    }

    @Override
    public ByteBuf buffer(int initialCapacity) {
        return alloc.buffer(initialCapacity);
    }

    @Override
    public ByteBuf buffer(int initialCapacity, int maxCapacity) {
        return alloc.buffer(initialCapacity, maxCapacity);
    }

    @Override
    public ByteBuf ioBuffer() {
        return alloc.ioBuffer();
    }

    @Override
    public ByteBuf ioBuffer(int initialCapacity) {
        return alloc.ioBuffer(initialCapacity);
    }

    @Override
    public ByteBuf ioBuffer(int initialCapacity, int maxCapacity) {
        return alloc.ioBuffer(initialCapacity, maxCapacity);
    }

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
    public ByteBuf directBuffer() {
        return alloc.directBuffer();
    }

    @Override
    public ByteBuf directBuffer(int initialCapacity) {
        return alloc.directBuffer(initialCapacity);
    }

    @Override
    public ByteBuf directBuffer(int initialCapacity, int maxCapacity) {
        return alloc.directBuffer(initialCapacity, maxCapacity);
    }

    @Override
    public CompositeByteBuf compositeBuffer() {
        return alloc.compositeBuffer();
    }

    @Override
    public CompositeByteBuf compositeBuffer(int maxNumComponents) {
        return alloc.compositeBuffer(maxNumComponents);
    }

    @Override
    public CompositeByteBuf compositeHeapBuffer() {
        throw new UnsupportedOperationException("Heap buffer");
    }

    @Override
    public CompositeByteBuf compositeHeapBuffer(int maxNumComponents) {
        throw new UnsupportedOperationException("Heap buffer");
    }

    @Override
    public CompositeByteBuf compositeDirectBuffer() {
        return alloc.compositeDirectBuffer();
    }

    @Override
    public CompositeByteBuf compositeDirectBuffer(int maxNumComponents) {
        return alloc.compositeDirectBuffer(maxNumComponents);
    }

    @Override
    public boolean isDirectBufferPooled() {
        return alloc.isDirectBufferPooled();
    }

    @Override
    public int calculateNewCapacity(int minNewCapacity, int maxCapacity) {
        return alloc.calculateNewCapacity(minNewCapacity, maxCapacity);
    }
}
