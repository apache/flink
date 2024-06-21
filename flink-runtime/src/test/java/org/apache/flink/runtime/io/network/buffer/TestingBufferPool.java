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

// We have it in this package because we could not mock the methods otherwise

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;

import java.util.concurrent.CompletableFuture;

/** Implementation of {@link BufferPool} for testing. */
public class TestingBufferPool implements BufferPool {

    @Override
    public void reserveSegments(int numberOfSegmentsToReserve) {}

    @Override
    public void lazyDestroy() {}

    @Override
    public Buffer requestBuffer() {
        MemorySegment segment = requestMemorySegment();
        return new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);
    }

    @Override
    public BufferBuilder requestBufferBuilder() {
        throw new UnsupportedOperationException();
    }

    @Override
    public BufferBuilder requestBufferBuilderBlocking() throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BufferBuilder requestBufferBuilder(int targetChannel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BufferBuilder requestBufferBuilderBlocking(int targetChannel)
            throws InterruptedException {
        throw new UnsupportedOperationException();
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
    public MemorySegment requestMemorySegment() {
        return MemorySegmentFactory.allocateUnpooledSegment(1024);
    }

    @Override
    public MemorySegment requestMemorySegmentBlocking() throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getExpectedNumberOfMemorySegments() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMinNumberOfMemorySegments() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMaxNumberOfMemorySegments() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getNumBuffers() {
        return 0;
    }

    @Override
    public void setNumBuffers(int numBuffers) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setMaxOverdraftBuffersPerGate(int maxOverdraftBuffersPerGate) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMaxOverdraftBuffersPerGate() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getNumberOfAvailableMemorySegments() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int bestEffortGetNumOfUsedBuffers() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void recycle(MemorySegment memorySegment) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return AVAILABLE;
    }
}
