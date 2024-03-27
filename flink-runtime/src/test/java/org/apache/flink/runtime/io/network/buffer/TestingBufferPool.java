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

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/** Implementation of {@link BufferPool} for testing. */
public class TestingBufferPool implements BufferPool {

    public static final TestingBufferPool NO_OP = TestingBufferPool.builder().build();

    private Consumer<Integer> reserveSegmentsConsumer;

    private Runnable lazyDestroyRunnable;

    private Supplier<Buffer> requestBufferSupplier;

    private Supplier<BufferBuilder> requestBufferBuilderSupplier;

    private Supplier<BufferBuilder> requestBufferBuilderBlockingSupplier;

    private Function<Integer, BufferBuilder> requestBufferBuilderFunction;

    private Function<Integer, BufferBuilder> requestBufferBuilderBlockingFunction;

    private Function<BufferListener, Boolean> addBufferListenerFunction;

    private Supplier<Boolean> isDestroyedSupplier;

    private Supplier<MemorySegment> requestMemorySegmentSupplier;

    private Supplier<MemorySegment> requestMemorySegmentBlockingSupplier;

    private Supplier<Integer> getExpectedNumberOfMemorySegmentsSupplier;

    private Supplier<Integer> getMinNumberOfMemorySegmentsSupplier;

    private Supplier<Integer> getMaxNumberOfMemorySegmentsSupplier;

    private Supplier<Integer> getNumBuffersSupplier;

    private Consumer<Integer> setNumBuffersConsumer;

    private Consumer<Integer> setMaxOverdraftBuffersPerGateConsumer;

    private Supplier<Integer> getMaxOverdraftBuffersPerGateSupplier;

    private Supplier<Integer> getNumberOfAvailableMemorySegmentsSupplier;

    private Supplier<Integer> bestEffortGetNumOfUsedBuffersSupplier;

    private Consumer<MemorySegment> recycleConsumer;

    private Supplier<CompletableFuture<?>> getAvailableFutureSupplier;

    private TestingBufferPool(
            Consumer<Integer> reserveSegmentsConsumer,
            Runnable lazyDestroyRunnable,
            Supplier<Buffer> requestBufferSupplier,
            Supplier<BufferBuilder> requestBufferBuilderSupplier,
            Supplier<BufferBuilder> requestBufferBuilderBlockingSupplier,
            Function<Integer, BufferBuilder> requestBufferBuilderFunction,
            Function<Integer, BufferBuilder> requestBufferBuilderBlockingFunction,
            Function<BufferListener, Boolean> addBufferListenerFunction,
            Supplier<Boolean> isDestroyedSupplier,
            Supplier<MemorySegment> requestMemorySegmentSupplier,
            Supplier<MemorySegment> requestMemorySegmentBlockingSupplier,
            Supplier<Integer> getExpectedNumberOfMemorySegmentsSupplier,
            Supplier<Integer> getMinNumberOfMemorySegmentsSupplier,
            Supplier<Integer> getMaxNumberOfMemorySegmentsSupplier,
            Supplier<Integer> getNumBuffersSupplier,
            Consumer<Integer> setNumBuffersConsumer,
            Consumer<Integer> setMaxOverdraftBuffersPerGateConsumer,
            Supplier<Integer> getMaxOverdraftBuffersPerGateSupplier,
            Supplier<Integer> getNumberOfAvailableMemorySegmentsSupplier,
            Supplier<Integer> bestEffortGetNumOfUsedBuffersSupplier,
            Consumer<MemorySegment> recycleConsumer,
            Supplier<CompletableFuture<?>> getAvailableFutureSupplier) {
        this.reserveSegmentsConsumer = reserveSegmentsConsumer;
        this.lazyDestroyRunnable = lazyDestroyRunnable;
        this.requestBufferSupplier = requestBufferSupplier;
        this.requestBufferBuilderSupplier = requestBufferBuilderSupplier;
        this.requestBufferBuilderBlockingSupplier = requestBufferBuilderBlockingSupplier;
        this.requestBufferBuilderFunction = requestBufferBuilderFunction;
        this.requestBufferBuilderBlockingFunction = requestBufferBuilderBlockingFunction;
        this.addBufferListenerFunction = addBufferListenerFunction;
        this.isDestroyedSupplier = isDestroyedSupplier;
        this.requestMemorySegmentSupplier = requestMemorySegmentSupplier;
        this.requestMemorySegmentBlockingSupplier = requestMemorySegmentBlockingSupplier;
        this.getExpectedNumberOfMemorySegmentsSupplier = getExpectedNumberOfMemorySegmentsSupplier;
        this.getMinNumberOfMemorySegmentsSupplier = getMinNumberOfMemorySegmentsSupplier;
        this.getMaxNumberOfMemorySegmentsSupplier = getMaxNumberOfMemorySegmentsSupplier;
        this.getNumBuffersSupplier = getNumBuffersSupplier;
        this.setNumBuffersConsumer = setNumBuffersConsumer;
        this.setMaxOverdraftBuffersPerGateConsumer = setMaxOverdraftBuffersPerGateConsumer;
        this.getMaxOverdraftBuffersPerGateSupplier = getMaxOverdraftBuffersPerGateSupplier;
        this.getNumberOfAvailableMemorySegmentsSupplier =
                getNumberOfAvailableMemorySegmentsSupplier;
        this.bestEffortGetNumOfUsedBuffersSupplier = bestEffortGetNumOfUsedBuffersSupplier;
        this.recycleConsumer = recycleConsumer;
        this.getAvailableFutureSupplier = getAvailableFutureSupplier;
    }

    public static TestingBufferPool.Builder builder() {
        return new TestingBufferPool.Builder();
    }

    @Override
    public void reserveSegments(int numberOfSegmentsToReserve) throws IOException {
        reserveSegmentsConsumer.accept(numberOfSegmentsToReserve);
    }

    @Override
    public void lazyDestroy() {
        lazyDestroyRunnable.run();
    }

    @Nullable
    @Override
    public Buffer requestBuffer() {
        return requestBufferSupplier.get();
    }

    @Nullable
    @Override
    public BufferBuilder requestBufferBuilder() {
        return requestBufferBuilderSupplier.get();
    }

    @Nullable
    @Override
    public BufferBuilder requestBufferBuilder(int targetChannel) {
        return requestBufferBuilderFunction.apply(targetChannel);
    }

    @Override
    public BufferBuilder requestBufferBuilderBlocking() throws InterruptedException {
        return requestBufferBuilderBlockingSupplier.get();
    }

    @Override
    public BufferBuilder requestBufferBuilderBlocking(int targetChannel)
            throws InterruptedException {
        return requestBufferBuilderBlockingFunction.apply(targetChannel);
    }

    @Override
    public boolean addBufferListener(BufferListener listener) {
        return addBufferListenerFunction.apply(listener);
    }

    @Override
    public boolean isDestroyed() {
        return isDestroyedSupplier.get();
    }

    @Nullable
    @Override
    public MemorySegment requestMemorySegment() {
        return requestMemorySegmentSupplier.get();
    }

    @Override
    public MemorySegment requestMemorySegmentBlocking() throws InterruptedException {
        return requestMemorySegmentBlockingSupplier.get();
    }

    @Override
    public int getExpectedNumberOfMemorySegments() {
        return getExpectedNumberOfMemorySegmentsSupplier.get();
    }

    @Override
    public int getMinNumberOfMemorySegments() {
        return getMinNumberOfMemorySegmentsSupplier.get();
    }

    @Override
    public int getMaxNumberOfMemorySegments() {
        return getMaxNumberOfMemorySegmentsSupplier.get();
    }

    @Override
    public int getNumBuffers() {
        return getNumBuffersSupplier.get();
    }

    @Override
    public void setNumBuffers(int numBuffers) {
        setNumBuffersConsumer.accept(numBuffers);
    }

    @Override
    public void setMaxOverdraftBuffersPerGate(int maxOverdraftBuffersPerGate) {
        setMaxOverdraftBuffersPerGateConsumer.accept(maxOverdraftBuffersPerGate);
    }

    @Override
    public int getMaxOverdraftBuffersPerGate() {
        return getMaxOverdraftBuffersPerGateSupplier.get();
    }

    @Override
    public int getNumberOfAvailableMemorySegments() {
        return getNumberOfAvailableMemorySegmentsSupplier.get();
    }

    @Override
    public int bestEffortGetNumOfUsedBuffers() {
        return bestEffortGetNumOfUsedBuffersSupplier.get();
    }

    @Override
    public void recycle(MemorySegment memorySegment) {
        recycleConsumer.accept(memorySegment);
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return getAvailableFutureSupplier.get();
    }

    /** Builder for {@link TestingBufferPool}. */
    public static class Builder {

        private Consumer<Integer> reserveSegmentsConsumer = (ignore) -> {};

        private Runnable lazyDestroyRunnable = () -> {};

        private Supplier<Buffer> requestBufferSupplier =
                () -> {
                    throw new UnsupportedOperationException();
                };

        private Supplier<BufferBuilder> requestBufferBuilderSupplier =
                () -> {
                    throw new UnsupportedOperationException();
                };

        private Supplier<BufferBuilder> requestBufferBuilderBlockingSupplier =
                () -> {
                    throw new UnsupportedOperationException();
                };

        private Function<Integer, BufferBuilder> requestBufferBuilderFunction =
                (ignore) -> {
                    throw new UnsupportedOperationException();
                };

        private Function<Integer, BufferBuilder> requestBufferBuilderBlockingFunction =
                (ignore) -> {
                    throw new UnsupportedOperationException();
                };

        private Function<BufferListener, Boolean> addBufferListenerFunction =
                (ignore) -> {
                    throw new UnsupportedOperationException();
                };

        private Supplier<Boolean> isDestroyedSupplier =
                () -> {
                    throw new UnsupportedOperationException();
                };

        private Supplier<MemorySegment> requestMemorySegmentSupplier =
                () -> {
                    throw new UnsupportedOperationException();
                };

        private Supplier<MemorySegment> requestMemorySegmentBlockingSupplier =
                () -> {
                    throw new UnsupportedOperationException();
                };

        private Supplier<Integer> getExpectedNumberOfMemorySegmentsSupplier =
                () -> {
                    throw new UnsupportedOperationException();
                };

        private Supplier<Integer> getMinNumberOfMemorySegmentsSupplier =
                () -> {
                    throw new UnsupportedOperationException();
                };

        private Supplier<Integer> getMaxNumberOfMemorySegmentsSupplier =
                () -> {
                    throw new UnsupportedOperationException();
                };

        private Supplier<Integer> getNumBuffersSupplier = () -> 0;

        private Consumer<Integer> setNumBuffersConsumer =
                (ignore) -> {
                    throw new UnsupportedOperationException();
                };

        private Consumer<Integer> setMaxOverdraftBuffersPerGateConsumer =
                (ignore) -> {
                    throw new UnsupportedOperationException();
                };

        private Supplier<Integer> getMaxOverdraftBuffersPerGateSupplier =
                () -> {
                    throw new UnsupportedOperationException();
                };

        private Supplier<Integer> getNumberOfAvailableMemorySegmentsSupplier =
                () -> {
                    throw new UnsupportedOperationException();
                };

        private Supplier<Integer> bestEffortGetNumOfUsedBuffersSupplier =
                () -> {
                    throw new UnsupportedOperationException();
                };

        private Consumer<MemorySegment> recycleConsumer =
                (ignore) -> {
                    throw new UnsupportedOperationException();
                };

        private Supplier<CompletableFuture<?>> getAvailableFutureSupplier = () -> AVAILABLE;

        private Builder() {}

        public TestingBufferPool build() {
            return new TestingBufferPool(
                    reserveSegmentsConsumer,
                    lazyDestroyRunnable,
                    requestBufferSupplier,
                    requestBufferBuilderSupplier,
                    requestBufferBuilderBlockingSupplier,
                    requestBufferBuilderFunction,
                    requestBufferBuilderBlockingFunction,
                    addBufferListenerFunction,
                    isDestroyedSupplier,
                    requestMemorySegmentSupplier,
                    requestMemorySegmentBlockingSupplier,
                    getExpectedNumberOfMemorySegmentsSupplier,
                    getMinNumberOfMemorySegmentsSupplier,
                    getMaxNumberOfMemorySegmentsSupplier,
                    getNumBuffersSupplier,
                    setNumBuffersConsumer,
                    setMaxOverdraftBuffersPerGateConsumer,
                    getMaxOverdraftBuffersPerGateSupplier,
                    getNumberOfAvailableMemorySegmentsSupplier,
                    bestEffortGetNumOfUsedBuffersSupplier,
                    recycleConsumer,
                    getAvailableFutureSupplier);
        }

        public Builder setReserveSegmentsConsumer(Consumer<Integer> reserveSegmentsConsumer) {
            this.reserveSegmentsConsumer = reserveSegmentsConsumer;
            return this;
        }

        public Builder setLazyDestroyRunnable(Runnable lazyDestroyRunnable) {
            this.lazyDestroyRunnable = lazyDestroyRunnable;
            return this;
        }

        public Builder setRequestBufferSupplier(Supplier<Buffer> requestBufferSupplier) {
            this.requestBufferSupplier = requestBufferSupplier;
            return this;
        }

        public Builder setRequestBufferBuilderSupplier(
                Supplier<BufferBuilder> requestBufferBuilderSupplier) {
            this.requestBufferBuilderSupplier = requestBufferBuilderSupplier;
            return this;
        }

        public Builder setRequestBufferBuilderBlockingSupplier(
                Supplier<BufferBuilder> requestBufferBuilderBlockingSupplier) {
            this.requestBufferBuilderBlockingSupplier = requestBufferBuilderBlockingSupplier;
            return this;
        }

        public Builder setRequestBufferBuilderFunction(
                Function<Integer, BufferBuilder> requestBufferBuilderFunction) {
            this.requestBufferBuilderFunction = requestBufferBuilderFunction;
            return this;
        }

        public Builder setRequestBufferBuilderBlockingFunction(
                Function<Integer, BufferBuilder> requestBufferBuilderBlockingFunction) {
            this.requestBufferBuilderBlockingFunction = requestBufferBuilderBlockingFunction;
            return this;
        }

        public Builder setAddBufferListenerFunction(
                Function<BufferListener, Boolean> addBufferListenerFunction) {
            this.addBufferListenerFunction = addBufferListenerFunction;
            return this;
        }

        public Builder setIsDestroyedSupplier(Supplier<Boolean> isDestroyedSupplier) {
            this.isDestroyedSupplier = isDestroyedSupplier;
            return this;
        }

        public Builder setRequestMemorySegmentSupplier(
                Supplier<MemorySegment> requestMemorySegmentSupplier) {
            this.requestMemorySegmentSupplier = requestMemorySegmentSupplier;
            return this;
        }

        public Builder setRequestMemorySegmentBlockingSupplier(
                Supplier<MemorySegment> requestMemorySegmentBlockingSupplier) {
            this.requestMemorySegmentBlockingSupplier = requestMemorySegmentBlockingSupplier;
            return this;
        }

        public Builder setGetExpectedNumberOfMemorySegmentsSupplier(
                Supplier<Integer> getExpectedNumberOfMemorySegmentsSupplier) {
            this.getExpectedNumberOfMemorySegmentsSupplier =
                    getExpectedNumberOfMemorySegmentsSupplier;
            return this;
        }

        public Builder setGetMinNumberOfMemorySegmentsSupplier(
                Supplier<Integer> getMinNumberOfMemorySegmentsSupplier) {
            this.getMinNumberOfMemorySegmentsSupplier = getMinNumberOfMemorySegmentsSupplier;
            return this;
        }

        public Builder setGetMaxNumberOfMemorySegmentsSupplier(
                Supplier<Integer> getMaxNumberOfMemorySegmentsSupplier) {
            this.getMaxNumberOfMemorySegmentsSupplier = getMaxNumberOfMemorySegmentsSupplier;
            return this;
        }

        public Builder setGetNumBuffersSupplier(Supplier<Integer> getNumBuffersSupplier) {
            this.getNumBuffersSupplier = getNumBuffersSupplier;
            return this;
        }

        public Builder setSetNumBuffersConsumer(Consumer<Integer> setNumBuffersConsumer) {
            this.setNumBuffersConsumer = setNumBuffersConsumer;
            return this;
        }

        public Builder setSetMaxOverdraftBuffersPerGateConsumer(
                Consumer<Integer> setMaxOverdraftBuffersPerGateConsumer) {
            this.setMaxOverdraftBuffersPerGateConsumer = setMaxOverdraftBuffersPerGateConsumer;
            return this;
        }

        public Builder setGetMaxOverdraftBuffersPerGateSupplier(
                Supplier<Integer> getMaxOverdraftBuffersPerGateSupplier) {
            this.getMaxOverdraftBuffersPerGateSupplier = getMaxOverdraftBuffersPerGateSupplier;
            return this;
        }

        public Builder setGetNumberOfAvailableMemorySegmentsSupplier(
                Supplier<Integer> getNumberOfAvailableMemorySegmentsSupplier) {
            this.getNumberOfAvailableMemorySegmentsSupplier =
                    getNumberOfAvailableMemorySegmentsSupplier;
            return this;
        }

        public Builder setBestEffortGetNumOfUsedBuffersSupplier(
                Supplier<Integer> bestEffortGetNumOfUsedBuffersSupplier) {
            this.bestEffortGetNumOfUsedBuffersSupplier = bestEffortGetNumOfUsedBuffersSupplier;
            return this;
        }

        public Builder setRecycleConsumer(Consumer<MemorySegment> recycleConsumer) {
            this.recycleConsumer = recycleConsumer;
            return this;
        }

        public Builder setGetAvailableFutureSupplier(
                Supplier<CompletableFuture<?>> getAvailableFutureSupplier) {
            this.getAvailableFutureSupplier = getAvailableFutureSupplier;
            return this;
        }
    }
}
