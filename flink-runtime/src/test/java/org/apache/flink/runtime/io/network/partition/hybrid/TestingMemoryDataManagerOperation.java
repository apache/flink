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

    private final Consumer<BufferIndexAndChannel> markBufferConsumedConsumer;

    private final Runnable markBufferFinishedRunnable;

    private TestingMemoryDataManagerOperation(
            SupplierWithException<BufferBuilder, InterruptedException>
                    requestBufferFromPoolSupplier,
            BiConsumer<Integer, Integer> markBufferReadableConsumer,
            Consumer<BufferIndexAndChannel> markBufferConsumedConsumer,
            Runnable markBufferFinishedRunnable) {
        this.requestBufferFromPoolSupplier = requestBufferFromPoolSupplier;
        this.markBufferReadableConsumer = markBufferReadableConsumer;
        this.markBufferConsumedConsumer = markBufferConsumedConsumer;
        this.markBufferFinishedRunnable = markBufferFinishedRunnable;
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
        markBufferConsumedConsumer.accept(consumedBuffer);
    }

    @Override
    public void onBufferFinished() {
        markBufferFinishedRunnable.run();
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for {@link TestingMemoryDataManagerOperation}. */
    public static class Builder {
        private SupplierWithException<BufferBuilder, InterruptedException>
                requestBufferFromPoolSupplier = () -> null;

        private BiConsumer<Integer, Integer> markBufferReadableConsumer = (ignore1, ignore2) -> {};

        private Consumer<BufferIndexAndChannel> markBufferConsumedConsumer = (ignore1) -> {};

        private Runnable markBufferFinishedRunnable = () -> {};

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

        public Builder setMarkBufferConsumedConsumer(
                Consumer<BufferIndexAndChannel> markBufferConsumedConsumer) {
            this.markBufferConsumedConsumer = markBufferConsumedConsumer;
            return this;
        }

        public Builder setMarkBufferFinishedRunnable(Runnable markBufferFinishedRunnable) {
            this.markBufferFinishedRunnable = markBufferFinishedRunnable;
            return this;
        }

        private Builder() {}

        public TestingMemoryDataManagerOperation build() {
            return new TestingMemoryDataManagerOperation(
                    requestBufferFromPoolSupplier,
                    markBufferReadableConsumer,
                    markBufferConsumedConsumer,
                    markBufferFinishedRunnable);
        }
    }
}
