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

package org.apache.flink.table.planner.plan.nodes.exec.common;

import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.CollectionSupplier;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.generated.GeneratedFunctionWrapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link BatchLookupFunctionWrapper}. */
class BatchLookupFunctionWrapperTest {

    private BatchLookupFunctionWrapper wrapper;
    private TestSingleAsyncFunction singleFunction;

    @BeforeEach
    void setUp() {
        singleFunction = new TestSingleAsyncFunction();
        GeneratedFunction<AsyncFunction<RowData, Object>> singleLookupFunction =
                new GeneratedFunctionWrapper<>(singleFunction);

        wrapper = new BatchLookupFunctionWrapper(singleLookupFunction);
    }

    @Test
    void testBatchToSingleAdaptation() throws Exception {
        AsyncFunction<List<RowData>, Object> batchFunction =
                wrapper.newInstance(getClass().getClassLoader());

        CountDownLatch latch = new CountDownLatch(1);
        List<Object> results = Collections.synchronizedList(new ArrayList<>());

        // Create batch input
        List<RowData> batchInput =
                Arrays.asList(
                        GenericRowData.of(1, StringData.fromString("Alice")),
                        GenericRowData.of(2, StringData.fromString("Bob")),
                        GenericRowData.of(3, StringData.fromString("Charlie")));

        // Process batch
        batchFunction.asyncInvoke(batchInput, new TestBatchResultFuture(results, latch));

        // Wait for completion
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

        // Verify that single function was called for each item in batch
        assertThat(singleFunction.getInvocationCount()).isEqualTo(batchInput.size());

        // Verify results
        assertThat(results).hasSize(1); // One batch result containing all individual results
        @SuppressWarnings("unchecked")
        List<Object> batchResult = (List<Object>) results.get(0);
        assertThat(batchResult).hasSize(batchInput.size());
    }

    @Test
    void testEmptyBatch() throws Exception {
        AsyncFunction<List<RowData>, Object> batchFunction =
                wrapper.newInstance(getClass().getClassLoader());

        CountDownLatch latch = new CountDownLatch(1);
        List<Object> results = Collections.synchronizedList(new ArrayList<>());

        // Process empty batch
        batchFunction.asyncInvoke(
                Collections.emptyList(), new TestBatchResultFuture(results, latch));

        // Wait for completion
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

        // Verify no single function calls
        assertThat(singleFunction.getInvocationCount()).isEqualTo(0);

        // Verify empty result
        assertThat(results).hasSize(1);
        @SuppressWarnings("unchecked")
        List<Object> batchResult = (List<Object>) results.get(0);
        assertThat(batchResult).isEmpty();
    }

    @Test
    void testSingleItemBatch() throws Exception {
        AsyncFunction<List<RowData>, Object> batchFunction =
                wrapper.newInstance(getClass().getClassLoader());

        CountDownLatch latch = new CountDownLatch(1);
        List<Object> results = Collections.synchronizedList(new ArrayList<>());

        // Create single item batch
        List<RowData> batchInput =
                Collections.singletonList(GenericRowData.of(1, StringData.fromString("Alice")));

        // Process batch
        batchFunction.asyncInvoke(batchInput, new TestBatchResultFuture(results, latch));

        // Wait for completion
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

        // Verify single function call
        assertThat(singleFunction.getInvocationCount()).isEqualTo(1);

        // Verify result
        assertThat(results).hasSize(1);
        @SuppressWarnings("unchecked")
        List<Object> batchResult = (List<Object>) results.get(0);
        assertThat(batchResult).hasSize(1);
    }

    @Test
    void testLargeBatch() throws Exception {
        AsyncFunction<List<RowData>, Object> batchFunction =
                wrapper.newInstance(getClass().getClassLoader());

        CountDownLatch latch = new CountDownLatch(1);
        List<Object> results = Collections.synchronizedList(new ArrayList<>());

        // Create large batch
        List<RowData> batchInput = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            batchInput.add(GenericRowData.of(i, StringData.fromString("User" + i)));
        }

        // Process batch
        batchFunction.asyncInvoke(batchInput, new TestBatchResultFuture(results, latch));

        // Wait for completion
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();

        // Verify function calls
        assertThat(singleFunction.getInvocationCount()).isEqualTo(batchInput.size());

        // Verify results
        assertThat(results).hasSize(1);
        @SuppressWarnings("unchecked")
        List<Object> batchResult = (List<Object>) results.get(0);
        assertThat(batchResult).hasSize(batchInput.size());
    }

    @Test
    void testErrorHandling() throws Exception {
        // Use a function that throws errors
        TestErrorAsyncFunction errorFunction = new TestErrorAsyncFunction();
        GeneratedFunction<AsyncFunction<RowData, Object>> errorLookupFunction =
                new GeneratedFunctionWrapper<>(errorFunction);

        BatchLookupFunctionWrapper errorWrapper =
                new BatchLookupFunctionWrapper(errorLookupFunction);
        AsyncFunction<List<RowData>, Object> batchFunction =
                errorWrapper.newInstance(getClass().getClassLoader());

        CountDownLatch latch = new CountDownLatch(1);
        List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());

        // Create batch input
        List<RowData> batchInput =
                Arrays.asList(
                        GenericRowData.of(1, StringData.fromString("Alice")),
                        GenericRowData.of(2, StringData.fromString("Bob")));

        // Process batch
        batchFunction.asyncInvoke(batchInput, new TestErrorResultFuture(errors, latch));

        // Wait for completion
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

        // Verify error was propagated
        assertThat(errors).hasSize(1);
        assertThat(errors.get(0)).isInstanceOf(RuntimeException.class);
        assertThat(errors.get(0).getMessage()).contains("Test error");
    }

    @Test
    void testWrapperMetadata() {
        // Test wrapper metadata
        assertThat(wrapper.getClassName()).isEqualTo("BatchLookupFunctionWrapper");
        assertThat(wrapper.getCode()).contains("BatchLookupFunctionWrapper");
        assertThat(wrapper.getReferences()).isNotEmpty();
    }

    // Test helper classes

    private static class TestSingleAsyncFunction implements AsyncFunction<RowData, Object> {
        private final AtomicInteger invocationCount = new AtomicInteger(0);

        @Override
        public void asyncInvoke(RowData input, ResultFuture<Object> resultFuture) throws Exception {
            invocationCount.incrementAndGet();

            // Simulate async processing
            CompletableFuture.runAsync(
                    () -> {
                        try {
                            Thread.sleep(1); // Minimal processing time
                            // Echo back the input as result
                            resultFuture.complete(Collections.singletonList(input));
                        } catch (Exception e) {
                            resultFuture.completeExceptionally(e);
                        }
                    });
        }

        public int getInvocationCount() {
            return invocationCount.get();
        }
    }

    private static class TestErrorAsyncFunction implements AsyncFunction<RowData, Object> {
        @Override
        public void asyncInvoke(RowData input, ResultFuture<Object> resultFuture) throws Exception {
            // Always throw an error
            CompletableFuture.runAsync(
                    () -> {
                        resultFuture.completeExceptionally(new RuntimeException("Test error"));
                    });
        }
    }

    private static class TestBatchResultFuture implements ResultFuture<Object> {
        private final List<Object> results;
        private final CountDownLatch latch;

        public TestBatchResultFuture(List<Object> results, CountDownLatch latch) {
            this.results = results;
            this.latch = latch;
        }

        @Override
        public void complete(Collection<Object> result) {
            results.addAll(result);
            latch.countDown();
        }

        @Override
        public void completeExceptionally(Throwable error) {
            latch.countDown();
        }

        @Override
        public void complete(CollectionSupplier<Object> supplier) {
            try {
                Collection<Object> result = supplier.get();
                results.addAll(result);
            } catch (Exception e) {
                // Ignore exception for testing
            } finally {
                latch.countDown();
            }
        }
    }

    private static class TestErrorResultFuture implements ResultFuture<Object> {
        private final List<Throwable> errors;
        private final CountDownLatch latch;

        public TestErrorResultFuture(List<Throwable> errors, CountDownLatch latch) {
            this.errors = errors;
            this.latch = latch;
        }

        @Override
        public void complete(Collection<Object> result) {
            latch.countDown();
        }

        @Override
        public void completeExceptionally(Throwable error) {
            errors.add(error);
            latch.countDown();
        }

        @Override
        public void complete(CollectionSupplier<Object> supplier) {
            try {
                supplier.get();
            } catch (Exception e) {
                errors.add(e);
            } finally {
                latch.countDown();
            }
        }
    }
}
