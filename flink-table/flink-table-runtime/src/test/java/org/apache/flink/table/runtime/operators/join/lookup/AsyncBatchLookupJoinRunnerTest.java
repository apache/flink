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

package org.apache.flink.table.runtime.operators.join.lookup;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.runtime.collector.TableFunctionResultFuture;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.generated.GeneratedFunctionWrapper;
import org.apache.flink.table.runtime.generated.GeneratedResultFutureWrapper;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.api.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.apache.flink.table.data.StringData.fromString;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link AsyncBatchLookupJoinRunner}. */
class AsyncBatchLookupJoinRunnerTest {

    private static final int BATCH_SIZE = 3;
    private static final long FLUSH_INTERVAL_MILLIS = 100L;
    private static final int ASYNC_BUFFER_CAPACITY = 10;

    private AsyncBatchLookupJoinRunner runner;
    private TestBatchAsyncFunction testAsyncFunction;
    private RowDataSerializer rightRowSerializer;

    @BeforeEach
    void setUp() throws Exception {
        testAsyncFunction = new TestBatchAsyncFunction();
        
        // Create serializer for right side rows
        LogicalType[] rightTypes = new LogicalType[] {
            DataTypes.INT().getLogicalType(),
            DataTypes.STRING().getLogicalType()
        };
        rightRowSerializer = new RowDataSerializer(rightTypes);

        // Create generated function wrapper
        GeneratedFunction<AsyncFunction<List<RowData>, Object>> generatedFetcher =
                new GeneratedFunctionWrapper<>(testAsyncFunction);

        // Create data structure converter
        DataType rightDataType = DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.INT()),
            DataTypes.FIELD("name", DataTypes.STRING())
        );
        DataStructureConverter<RowData, Object> fetcherConverter =
                DataStructureConverters.getConverter(rightDataType);

        // Create generated result future
        GeneratedResultFuture<TableFunctionResultFuture<List<RowData>>> generatedResultFuture =
                new GeneratedResultFutureWrapper<>(new TestBatchTableFunctionResultFuture());

        // Create runner
        runner = new AsyncBatchLookupJoinRunner(
                generatedFetcher,
                fetcherConverter,
                generatedResultFuture,
                rightRowSerializer,
                false, // not left outer join
                ASYNC_BUFFER_CAPACITY,
                BATCH_SIZE,
                FLUSH_INTERVAL_MILLIS);

        // Set runtime context
        runner.setRuntimeContext(new MockStreamingRuntimeContext(false, 1, 0));
        runner.open(DefaultOpenContext.INSTANCE);
    }

    @Test
    void testBatchProcessing() throws Exception {
        CountDownLatch latch = new CountDownLatch(BATCH_SIZE);
        List<RowData> results = Collections.synchronizedList(new ArrayList<>());

        // Create test input data
        List<RowData> inputRows = Arrays.asList(
            GenericRowData.of(1, StringData.fromString("Alice")),
            GenericRowData.of(2, StringData.fromString("Bob")),
            GenericRowData.of(3, StringData.fromString("Charlie"))
        );

        // Process inputs
        for (RowData input : inputRows) {
            runner.asyncInvoke(input, new TestBatchResultFuture(results, latch));
        }

        // Wait for batch processing
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

        // Verify that async function was called once with batch
        assertThat(testAsyncFunction.getInvocationCount()).isEqualTo(1);
        assertThat(testAsyncFunction.getLastBatchSize()).isEqualTo(BATCH_SIZE);

        // Verify results
        assertThat(results).hasSize(BATCH_SIZE);
    }

    @Test
    void testFlushOnTimeout() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        List<RowData> results = Collections.synchronizedList(new ArrayList<>());

        // Process single input (less than batch size)
        RowData input = GenericRowData.of(1, StringData.fromString("Alice"));
        runner.asyncInvoke(input, new TestBatchResultFuture(results, latch));

        // Wait for timeout flush
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

        // Verify that async function was called with single item
        assertThat(testAsyncFunction.getInvocationCount()).isEqualTo(1);
        assertThat(testAsyncFunction.getLastBatchSize()).isEqualTo(1);

        // Verify result
        assertThat(results).hasSize(1);
    }

    @Test
    void testMultipleBatches() throws Exception {
        int totalInputs = BATCH_SIZE * 2 + 1; // 7 inputs = 2 full batches + 1 partial
        CountDownLatch latch = new CountDownLatch(totalInputs);
        List<RowData> results = Collections.synchronizedList(new ArrayList<>());

        // Process inputs
        for (int i = 0; i < totalInputs; i++) {
            RowData input = GenericRowData.of(i, StringData.fromString("User" + i));
            runner.asyncInvoke(input, new TestBatchResultFuture(results, latch));
        }

        // Wait for all processing
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();

        // Verify that async function was called multiple times
        assertThat(testAsyncFunction.getInvocationCount()).isGreaterThanOrEqualTo(2);

        // Verify all results
        assertThat(results).hasSize(totalInputs);
    }

    @Test
    void testConcurrentProcessing() throws Exception {
        int numThreads = 5;
        int inputsPerThread = 10;
        int totalInputs = numThreads * inputsPerThread;
        
        CountDownLatch latch = new CountDownLatch(totalInputs);
        List<RowData> results = Collections.synchronizedList(new ArrayList<>());

        // Create threads to process inputs concurrently
        List<Thread> threads = new ArrayList<>();
        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            Thread thread = new Thread(() -> {
                try {
                    for (int i = 0; i < inputsPerThread; i++) {
                        RowData input = GenericRowData.of(
                            threadId * inputsPerThread + i, 
                            StringData.fromString("Thread" + threadId + "_User" + i));
                        runner.asyncInvoke(input, new TestBatchResultFuture(results, latch));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            threads.add(thread);
        }

        // Start all threads
        threads.forEach(Thread::start);

        // Wait for all processing
        assertThat(latch.await(15, TimeUnit.SECONDS)).isTrue();

        // Wait for threads to complete
        for (Thread thread : threads) {
            thread.join();
        }

        // Verify all results
        assertThat(results).hasSize(totalInputs);
    }

    // Test helper classes

    /**
     * Test async function that processes batches of RowData.
     * Based on existing TestingFetcherFunction pattern.
     */
    public static final class TestBatchAsyncFunction extends AbstractRichFunction
            implements AsyncFunction<List<RowData>, Object> {
        
        private static final long serialVersionUID = 1L;
        
        private final AtomicInteger invocationCount = new AtomicInteger(0);
        private volatile int lastBatchSize = 0;
        
        private static final Map<Integer, List<RowData>> data = new HashMap<>();
        
        static {
            data.put(1, Collections.singletonList(GenericRowData.of(1, fromString("Julian"))));
            data.put(2, Collections.singletonList(GenericRowData.of(2, fromString("Bob"))));
            data.put(3, Arrays.asList(
                    GenericRowData.of(3, fromString("Jark")),
                    GenericRowData.of(3, fromString("Jackson"))));
            data.put(4, Collections.singletonList(GenericRowData.of(4, fromString("Fabian"))));
        }
        
        private transient ExecutorService executor;

        @Override
        public void open(org.apache.flink.api.common.functions.OpenContext openContext) throws Exception {
            super.open(openContext);
            this.executor = Executors.newFixedThreadPool(2);
        }

        @Override
        public void asyncInvoke(List<RowData> input, ResultFuture<Object> resultFuture) throws Exception {
            invocationCount.incrementAndGet();
            lastBatchSize = input.size();
            
            // Simulate async batch processing
            CompletableFuture.supplyAsync(
                    (Supplier<Collection<Object>>) () -> {
                        try {
                            Thread.sleep(10); // Simulate processing time
                            List<Object> results = new ArrayList<>();
                            for (RowData row : input) {
                                int id = row.getInt(0);
                                List<RowData> lookupResults = data.get(id);
                                if (lookupResults != null) {
                                    results.addAll(lookupResults);
                                } else {
                                    // Echo back the input as result for unknown IDs
                                    results.add(row);
                                }
                            }
                            return results;
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    },
                    executor)
                    .thenAcceptAsync(resultFuture::complete, executor);
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (executor != null && !executor.isShutdown()) {
                executor.shutdown();
            }
        }

        public int getInvocationCount() {
            return invocationCount.get();
        }

        public int getLastBatchSize() {
            return lastBatchSize;
        }
    }

    private static class TestBatchResultFuture implements ResultFuture<RowData> {
        private final List<RowData> results;
        private final CountDownLatch latch;

        public TestBatchResultFuture(List<RowData> results, CountDownLatch latch) {
            this.results = results;
            this.latch = latch;
        }

        @Override
        public void complete(Iterable<RowData> result) {
            for (RowData row : result) {
                results.add(row);
                latch.countDown();
            }
        }

        @Override
        public void completeExceptionally(Throwable error) {
            // Handle error - for test purposes, just complete the latch
            latch.countDown();
        }
    }

    private static class TestBatchTableFunctionResultFuture extends TableFunctionResultFuture<List<RowData>> {
        @Override
        public void complete(List<RowData> result) {
            // Implementation for test
        }

        @Override
        public void completeExceptionally(Throwable error) {
            // Implementation for test
        }
    }
}