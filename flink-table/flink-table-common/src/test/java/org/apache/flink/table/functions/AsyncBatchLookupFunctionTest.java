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

package org.apache.flink.table.functions;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AsyncBatchLookupFunction}. */
class AsyncBatchLookupFunctionTest {

    @Test
    void testAsyncLookupBatch() throws Exception {
        // Create a test implementation
        TestAsyncBatchLookupFunction function = new TestAsyncBatchLookupFunction();

        // Create test keys
        List<RowData> keys = new ArrayList<>();
        keys.add(GenericRowData.of(1, "a"));
        keys.add(GenericRowData.of(2, "b"));
        keys.add(GenericRowData.of(3, "c"));

        // Call batch lookup
        CompletableFuture<Collection<RowData>> future = function.asyncLookupBatch(keys);

        // Wait for results
        Collection<RowData> results = future.get();

        // Verify results
        assertThat(results).hasSize(3);
        assertThat(function.getLastBatchSize()).isEqualTo(3);
    }

    @Test
    void testSingleKeyLookupDelegatesToBatch() throws Exception {
        TestAsyncBatchLookupFunction function = new TestAsyncBatchLookupFunction();

        RowData key = GenericRowData.of(1, "test");
        CompletableFuture<Collection<RowData>> future = function.asyncLookup(key);

        Collection<RowData> results = future.get();

        assertThat(results).hasSize(1);
        // Single key should be wrapped in a list
        assertThat(function.getLastBatchSize()).isEqualTo(1);
    }

    @Test
    void testEvalMethodBridgesToBatchLookup() throws Exception {
        TestAsyncBatchLookupFunction function = new TestAsyncBatchLookupFunction();

        CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();
        function.eval(future, 1, "test");

        Collection<RowData> results = future.get();

        assertThat(results).hasSize(1);
    }

    @Test
    void testEmptyBatch() throws Exception {
        TestAsyncBatchLookupFunction function = new TestAsyncBatchLookupFunction();

        CompletableFuture<Collection<RowData>> future = function.asyncLookupBatch(List.of());

        Collection<RowData> results = future.get();

        assertThat(results).isEmpty();
    }

    @Test
    void testExceptionPropagation() {
        FailingAsyncBatchLookupFunction function = new FailingAsyncBatchLookupFunction();

        List<RowData> keys = List.of(GenericRowData.of(1, "a"));
        CompletableFuture<Collection<RowData>> future = function.asyncLookupBatch(keys);

        assertThat(future).isCompletedExceptionally();
    }

    /** Test implementation of AsyncBatchLookupFunction. */
    private static class TestAsyncBatchLookupFunction extends AsyncBatchLookupFunction {

        private int lastBatchSize = 0;

        @Override
        public CompletableFuture<Collection<RowData>> asyncLookupBatch(List<RowData> keyRows) {
            lastBatchSize = keyRows.size();

            return CompletableFuture.supplyAsync(
                    () -> {
                        List<RowData> results = new ArrayList<>();
                        for (RowData key : keyRows) {
                            // Create a result row for each key
                            results.add(
                                    GenericRowData.of(
                                            key.getInt(0), key.getString(1).toString(), "result"));
                        }
                        return results;
                    });
        }

        public int getLastBatchSize() {
            return lastBatchSize;
        }
    }

    /** Test implementation that always fails. */
    private static class FailingAsyncBatchLookupFunction extends AsyncBatchLookupFunction {

        @Override
        public CompletableFuture<Collection<RowData>> asyncLookupBatch(List<RowData> keyRows) {
            CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();
            future.completeExceptionally(new RuntimeException("Test failure"));
            return future;
        }
    }
}
