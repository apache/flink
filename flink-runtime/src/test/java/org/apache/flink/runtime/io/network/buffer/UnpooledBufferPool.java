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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;

import java.util.concurrent.CompletableFuture;

/** Implementation of {@link BufferPool} that works on un-pooled memory segments. */
public class UnpooledBufferPool implements BufferPool {

    private static final int SEGMENT_SIZE = 1024;

    @Override
    public void lazyDestroy() {}

    @Override
    public Buffer requestBuffer() {
        return new NetworkBuffer(requestMemorySegment(), this);
    }

    @Override
    public BufferBuilder requestBufferBuilder() {
        return new BufferBuilder(requestMemorySegment(), this);
    }

    @Override
    public MemorySegment requestMemorySegment() {
        return MemorySegmentFactory.allocateUnpooledOffHeapMemory(SEGMENT_SIZE, null);
    }

    @Override
    public MemorySegment requestMemorySegmentBlocking() throws InterruptedException {
        return requestMemorySegment();
    }

    @Override
    public BufferBuilder requestBufferBuilderBlocking() throws InterruptedException {
        return requestBufferBuilder();
    }

    @Override
    public BufferBuilder requestBufferBuilder(int targetChannel) {
        return requestBufferBuilder();
    }

    @Override
    public BufferBuilder requestBufferBuilderBlocking(int targetChannel)
            throws InterruptedException {
        return requestBufferBuilder();
    }

    @Override
    public boolean addBufferListener(BufferListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDestroyed() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getNumberOfRequiredMemorySegments() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxNumberOfMemorySegments() {
        return -1;
    }

    @Override
    public int getNumBuffers() {
        return Integer.MAX_VALUE;
    }

    @Override
    public void setNumBuffers(int numBuffers) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getNumberOfAvailableMemorySegments() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int bestEffortGetNumOfUsedBuffers() {
        return 0;
    }

    @Override
    public void recycle(MemorySegment memorySegment) {
        memorySegment.free();
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return AVAILABLE;
    }
}
