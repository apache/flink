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

package org.apache.flink.table.connector.source.lookup;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.functions.AsyncBatchLookupFunction;

import java.time.Duration;

/**
 * A provider for creating {@link AsyncBatchLookupFunction} with batch configuration.
 *
 * <p>This provider is used to create batch-oriented async lookup functions for AI/ML inference
 * scenarios and other high-latency lookup workloads. It allows configuring batch parameters such as
 * batch size and timeout.
 *
 * <p>Example usage in a custom {@link LookupTableSource}:
 *
 * <pre>{@code
 * public class MyLookupTableSource implements LookupTableSource {
 *
 *     @Override
 *     public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
 *         return AsyncBatchLookupFunctionProvider.of(
 *             new MyAsyncBatchLookupFunction(),
 *             32,  // maxBatchSize
 *             Duration.ofMillis(100)  // batchTimeout
 *         );
 *     }
 * }
 * }</pre>
 *
 * @see AsyncBatchLookupFunction
 * @see AsyncLookupFunctionProvider
 */
@PublicEvolving
public interface AsyncBatchLookupFunctionProvider extends LookupTableSource.LookupRuntimeProvider {

    /** Default batch size when not specified. */
    int DEFAULT_BATCH_SIZE = 32;

    /** Default batch timeout when not specified (100ms). */
    Duration DEFAULT_BATCH_TIMEOUT = Duration.ofMillis(100);

    /**
     * Creates a provider with the given function and default batch configuration.
     *
     * @param asyncBatchLookupFunction The batch lookup function
     * @return A new provider instance
     */
    static AsyncBatchLookupFunctionProvider of(AsyncBatchLookupFunction asyncBatchLookupFunction) {
        return of(asyncBatchLookupFunction, DEFAULT_BATCH_SIZE, DEFAULT_BATCH_TIMEOUT);
    }

    /**
     * Creates a provider with the given function and batch size (default timeout).
     *
     * @param asyncBatchLookupFunction The batch lookup function
     * @param maxBatchSize Maximum number of keys to batch together
     * @return A new provider instance
     */
    static AsyncBatchLookupFunctionProvider of(
            AsyncBatchLookupFunction asyncBatchLookupFunction, int maxBatchSize) {
        return of(asyncBatchLookupFunction, maxBatchSize, DEFAULT_BATCH_TIMEOUT);
    }

    /**
     * Creates a provider with full configuration.
     *
     * @param asyncBatchLookupFunction The batch lookup function
     * @param maxBatchSize Maximum number of keys to batch together
     * @param batchTimeout Maximum time to wait before flushing a partial batch
     * @return A new provider instance
     */
    static AsyncBatchLookupFunctionProvider of(
            AsyncBatchLookupFunction asyncBatchLookupFunction,
            int maxBatchSize,
            Duration batchTimeout) {
        return new DefaultAsyncBatchLookupFunctionProvider(
                asyncBatchLookupFunction, maxBatchSize, batchTimeout);
    }

    /**
     * Creates an {@link AsyncBatchLookupFunction} instance.
     *
     * @return The batch lookup function
     */
    AsyncBatchLookupFunction createAsyncBatchLookupFunction();

    /**
     * Returns the maximum batch size.
     *
     * @return Maximum number of keys to batch together
     */
    int getMaxBatchSize();

    /**
     * Returns the batch timeout.
     *
     * @return Maximum time to wait before flushing a partial batch
     */
    Duration getBatchTimeout();

    /**
     * Default implementation of {@link AsyncBatchLookupFunctionProvider}.
     *
     * <p>This is an internal implementation class.
     */
    class DefaultAsyncBatchLookupFunctionProvider implements AsyncBatchLookupFunctionProvider {

        private final AsyncBatchLookupFunction asyncBatchLookupFunction;
        private final int maxBatchSize;
        private final Duration batchTimeout;

        DefaultAsyncBatchLookupFunctionProvider(
                AsyncBatchLookupFunction asyncBatchLookupFunction,
                int maxBatchSize,
                Duration batchTimeout) {
            if (maxBatchSize <= 0) {
                throw new IllegalArgumentException(
                        "maxBatchSize must be positive: " + maxBatchSize);
            }
            if (batchTimeout == null || batchTimeout.isNegative()) {
                throw new IllegalArgumentException(
                        "batchTimeout must be non-negative: " + batchTimeout);
            }
            this.asyncBatchLookupFunction = asyncBatchLookupFunction;
            this.maxBatchSize = maxBatchSize;
            this.batchTimeout = batchTimeout;
        }

        @Override
        public AsyncBatchLookupFunction createAsyncBatchLookupFunction() {
            return asyncBatchLookupFunction;
        }

        @Override
        public int getMaxBatchSize() {
            return maxBatchSize;
        }

        @Override
        public Duration getBatchTimeout() {
            return batchTimeout;
        }
    }
}
