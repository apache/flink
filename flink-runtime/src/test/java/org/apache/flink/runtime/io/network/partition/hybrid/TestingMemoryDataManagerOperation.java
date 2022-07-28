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

package org.apache.flink.runtime.io.network.partition.hybrid;

import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.util.function.SupplierWithException;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/** Mock {@link HsMemoryDataManagerOperation} for testing. */
public class TestingMemoryDataManagerOperation implements HsMemoryDataManagerOperation {
    private final SupplierWithException<BufferBuilder, InterruptedException>
            requestBufferFromPoolSupplier;

    private final BiConsumer<Integer, Integer> markBufferReadableConsumer;

    private final Consumer<BufferIndexAndChannel> onBufferConsumedConsumer;

    private final Runnable onBufferFinishedRunnable;

    private final Runnable onDataAvailableRunnable;

    private TestingMemoryDataManagerOperation(
            SupplierWithException<BufferBuilder, InterruptedException>
                    requestBufferFromPoolSupplier,
            BiConsumer<Integer, Integer> markBufferReadableConsumer,
            Consumer<BufferIndexAndChannel> onBufferConsumedConsumer,
            Runnable onBufferFinishedRunnable,
            Runnable onDataAvailableRunnable) {
        this.requestBufferFromPoolSupplier = requestBufferFromPoolSupplier;
        this.markBufferReadableConsumer = markBufferReadableConsumer;
        this.onBufferConsumedConsumer = onBufferConsumedConsumer;
        this.onBufferFinishedRunnable = onBufferFinishedRunnable;
        this.onDataAvailableRunnable = onDataAvailableRunnable;
    }

    @Override
    public BufferBuilder requestBufferFromPool() throws InterruptedException {
        return requestBufferFromPoolSupplier.get();
    }

    @Override
    public void markBufferReadableFromFile(int subpartitionId, int bufferIndex) {
        markBufferReadableConsumer.accept(subpartitionId, bufferIndex);
    }

    @Override
    public void onBufferConsumed(BufferIndexAndChannel consumedBuffer) {
        onBufferConsumedConsumer.accept(consumedBuffer);
    }

    @Override
    public void onBufferFinished() {
        onBufferFinishedRunnable.run();
    }

    @Override
    public void onDataAvailable(int subpartitionId) {
        onDataAvailableRunnable.run();
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for {@link TestingMemoryDataManagerOperation}. */
    public static class Builder {
        private SupplierWithException<BufferBuilder, InterruptedException>
                requestBufferFromPoolSupplier = () -> null;

        private BiConsumer<Integer, Integer> markBufferReadableConsumer = (ignore1, ignore2) -> {};

        private Consumer<BufferIndexAndChannel> onBufferConsumedConsumer = (ignore1) -> {};

        private Runnable onBufferFinishedRunnable = () -> {};

        private Runnable onDataAvailableRunnable = () -> {};

        public Builder setRequestBufferFromPoolSupplier(
                SupplierWithException<BufferBuilder, InterruptedException>
                        requestBufferFromPoolSupplier) {
            this.requestBufferFromPoolSupplier = requestBufferFromPoolSupplier;
            return this;
        }

        public Builder setMarkBufferReadableConsumer(
                BiConsumer<Integer, Integer> markBufferReadableConsumer) {
            this.markBufferReadableConsumer = markBufferReadableConsumer;
            return this;
        }

        public Builder setOnBufferConsumedConsumer(
                Consumer<BufferIndexAndChannel> onBufferConsumedConsumer) {
            this.onBufferConsumedConsumer = onBufferConsumedConsumer;
            return this;
        }

        public Builder setOnBufferFinishedRunnable(Runnable onBufferFinishedRunnable) {
            this.onBufferFinishedRunnable = onBufferFinishedRunnable;
            return this;
        }

        public Builder setOnDataAvailableRunnable(Runnable onDataAvailableRunnable) {
            this.onDataAvailableRunnable = onDataAvailableRunnable;
            return this;
        }

        private Builder() {}

        public TestingMemoryDataManagerOperation build() {
            return new TestingMemoryDataManagerOperation(
                    requestBufferFromPoolSupplier,
                    markBufferReadableConsumer,
                    onBufferConsumedConsumer,
                    onBufferFinishedRunnable,
                    onDataAvailableRunnable);
        }
    }
}
