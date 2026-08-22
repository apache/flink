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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncBatchLookupFunction;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AsyncBatchLookupFunctionProvider}. */
class AsyncBatchLookupFunctionProviderTest {

    @Test
    void testCreateWithDefaults() {
        TestAsyncBatchLookupFunction function = new TestAsyncBatchLookupFunction();
        AsyncBatchLookupFunctionProvider provider = AsyncBatchLookupFunctionProvider.of(function);

        assertThat(provider.createAsyncBatchLookupFunction()).isSameAs(function);
        assertThat(provider.getMaxBatchSize())
                .isEqualTo(AsyncBatchLookupFunctionProvider.DEFAULT_BATCH_SIZE);
        assertThat(provider.getBatchTimeout())
                .isEqualTo(AsyncBatchLookupFunctionProvider.DEFAULT_BATCH_TIMEOUT);
    }

    @Test
    void testCreateWithCustomBatchSize() {
        TestAsyncBatchLookupFunction function = new TestAsyncBatchLookupFunction();
        AsyncBatchLookupFunctionProvider provider =
                AsyncBatchLookupFunctionProvider.of(function, 64);

        assertThat(provider.createAsyncBatchLookupFunction()).isSameAs(function);
        assertThat(provider.getMaxBatchSize()).isEqualTo(64);
        assertThat(provider.getBatchTimeout())
                .isEqualTo(AsyncBatchLookupFunctionProvider.DEFAULT_BATCH_TIMEOUT);
    }

    @Test
    void testCreateWithFullConfiguration() {
        TestAsyncBatchLookupFunction function = new TestAsyncBatchLookupFunction();
        Duration timeout = Duration.ofMillis(200);
        AsyncBatchLookupFunctionProvider provider =
                AsyncBatchLookupFunctionProvider.of(function, 128, timeout);

        assertThat(provider.createAsyncBatchLookupFunction()).isSameAs(function);
        assertThat(provider.getMaxBatchSize()).isEqualTo(128);
        assertThat(provider.getBatchTimeout()).isEqualTo(timeout);
    }

    @Test
    void testInvalidBatchSizeThrowsException() {
        TestAsyncBatchLookupFunction function = new TestAsyncBatchLookupFunction();

        assertThatThrownBy(() -> AsyncBatchLookupFunctionProvider.of(function, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxBatchSize must be positive");

        assertThatThrownBy(() -> AsyncBatchLookupFunctionProvider.of(function, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxBatchSize must be positive");
    }

    @Test
    void testNegativeTimeoutThrowsException() {
        TestAsyncBatchLookupFunction function = new TestAsyncBatchLookupFunction();

        assertThatThrownBy(
                        () ->
                                AsyncBatchLookupFunctionProvider.of(
                                        function, 32, Duration.ofMillis(-100)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batchTimeout must be non-negative");
    }

    @Test
    void testZeroTimeoutIsValid() {
        TestAsyncBatchLookupFunction function = new TestAsyncBatchLookupFunction();
        AsyncBatchLookupFunctionProvider provider =
                AsyncBatchLookupFunctionProvider.of(function, 32, Duration.ZERO);

        assertThat(provider.getBatchTimeout()).isEqualTo(Duration.ZERO);
    }

    /** Test implementation of AsyncBatchLookupFunction. */
    private static class TestAsyncBatchLookupFunction extends AsyncBatchLookupFunction {
        @Override
        public CompletableFuture<Collection<RowData>> asyncLookupBatch(List<RowData> keyRows) {
            return CompletableFuture.completedFuture(List.of());
        }
    }
}
