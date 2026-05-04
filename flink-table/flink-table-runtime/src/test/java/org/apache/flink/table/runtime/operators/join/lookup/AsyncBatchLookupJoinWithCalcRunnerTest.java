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

import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.runtime.collector.TableFunctionResultFuture;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.generated.GeneratedFunctionWrapper;
import org.apache.flink.table.runtime.generated.GeneratedResultFuture;
import org.apache.flink.table.runtime.generated.GeneratedResultFutureWrapper;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Collector;

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

/** Unit tests for {@link AsyncBatchLookupJoinWithCalcRunner}. */
class AsyncBatchLookupJoinWithCalcRunnerTest {

    private static final int BATCH_SIZE = 3;
    private static final long FLUSH_INTERVAL_MILLIS = 100L;
    private static final int ASYNC_BUFFER_CAPACITY = 10;

    private AsyncBatchLookupJoinWithCalcRunner runner;
    private TestAsyncFunction testAsyncFunction;
    private TestCalcFunction testCalcFunction;
    private RowDataSerializer rightRowSerializer;

    @BeforeEach
    void setUp() throws Exception {
        testAsyncFunction = new TestAsyncFunction();
        testCalcFunction = new TestCalcFunction();

        // Create serializer for right side rows
        LogicalType[] rightTypes =
                new LogicalType[] {
                    DataTypes.INT().getLogicalType(), DataTypes.STRING().getLogicalType()
                };
        rightRowSerializer = new RowDataSerializer(rightTypes);

        // Create generated function wrapper
        GeneratedFunction<AsyncFunction<List<RowData>, Object>> generatedFetcher =
                new GeneratedFunctionWrapper<>(testAsyncFunction);

        // Create data structure converter
        DataType rightDataType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("name", DataTypes.STRING()));
        @SuppressWarnings({"unchecked", "rawtypes"})
        DataStructureConverter<RowData, Object> fetcherConverter =
                (DataStructureConverter) DataStructureConverters.getConverter(rightDataType);

        // Create generated result future
        GeneratedResultFuture<TableFunctionResultFuture<List<RowData>>> generatedResultFuture =
                new GeneratedResultFutureWrapper<>(new TestTableFunctionResultFuture());

        // Create generated calc function
        GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalc =
                new GeneratedFunctionWrapper<>(testCalcFunction);

        // Create runner
        runner =
                new AsyncBatchLookupJoinWithCalcRunner(
                        generatedFetcher,
                        fetcherConverter,
                        generatedCalc,
                        generatedResultFuture,
                        rightRowSerializer,
                        false, // not left outer join
                        ASYNC_BUFFER_CAPACITY,
                        BATCH_SIZE,
                        FLUSH_INTERVAL_MILLIS);

        // Set runtime context
        runner.setRuntimeContext(new MockStreamingRuntimeContext(1, 0));
        runner.open(DefaultOpenContext.INSTANCE);
    }

    @Test
    void testBatchProcessingWithCalc() throws Exception {
        CountDownLatch latch = new CountDownLatch(BATCH_SIZE);
        List<RowData> results = Collections.synchronizedList(new ArrayList<>());

        // Create test input data
        List<RowData> inputRows =
                Arrays.asList(
                        GenericRowData.of(1, StringData.fromString("Alice")),
                        GenericRowData.of(2, StringData.fromString("Bob")),
                        GenericRowData.of(3, StringData.fromString("Charlie")));

        // Process inputs
        for (RowData input : inputRows) {
            runner.asyncInvoke(input, new TestResultFuture(results, latch));
        }

        // Wait for batch processing
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

        // Verify that async function was called once with batch
        assertThat(testAsyncFunction.getInvocationCount()).isEqualTo(1);
        assertThat(testAsyncFunction.getLastBatchSize()).isEqualTo(BATCH_SIZE);

        // Verify that calc function was called for each result
        assertThat(testCalcFunction.getInvocationCount()).isEqualTo(BATCH_SIZE);

        // Verify results (should be transformed by calc function)
        assertThat(results).hasSize(BATCH_SIZE);
        for (RowData result : results) {
            // Calc function adds 100 to the ID
            assertThat(result.getInt(0)).isGreaterThanOrEqualTo(101);
        }
    }

    @Test
    void testCalcFunctionFiltering() throws Exception {
        // Use a filtering calc function
        TestFilteringCalcFunction filteringCalc = new TestFilteringCalcFunction();
        GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedFilteringCalc =
                new GeneratedFunctionWrapper<>(filteringCalc);

        // Create new runner with filtering calc
        AsyncBatchLookupJoinWithCalcRunner filteringRunner =
                new AsyncBatchLookupJoinWithCalcRunner(
                        new GeneratedFunctionWrapper<>(testAsyncFunction),
                        (DataStructureConverter)
                                DataStructureConverters.getConverter(
                                        DataTypes.ROW(
                                                DataTypes.FIELD("id", DataTypes.INT()),
                                                DataTypes.FIELD("name", DataTypes.STRING()))),
                        generatedFilteringCalc,
                        new GeneratedResultFutureWrapper<>(new TestTableFunctionResultFuture()),
                        rightRowSerializer,
                        false,
                        ASYNC_BUFFER_CAPACITY,
                        BATCH_SIZE,
                        FLUSH_INTERVAL_MILLIS);

        filteringRunner.setRuntimeContext(new MockStreamingRuntimeContext(1, 0));
        filteringRunner.open(DefaultOpenContext.INSTANCE);

        CountDownLatch latch = new CountDownLatch(1); // Only expect 1 result after filtering
        List<RowData> results = Collections.synchronizedList(new ArrayList<>());

        // Create test input data (only even IDs will pass the filter)
        List<RowData> inputRows =
                Arrays.asList(
                        GenericRowData.of(1, StringData.fromString("Alice")), // filtered out
                        GenericRowData.of(2, StringData.fromString("Bob")), // passes filter
                        GenericRowData.of(3, StringData.fromString("Charlie")) // filtered out
                        );

        // Process inputs
        for (RowData input : inputRows) {
            filteringRunner.asyncInvoke(input, new TestResultFuture(results, latch));
        }

        // Wait for processing
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

        // Verify only one result passed the filter
        assertThat(results).hasSize(1);
        assertThat(results.get(0).getInt(0)).isEqualTo(2);
    }

    @Test
    void testCalcFunctionProjection() throws Exception {
        // Use a projecting calc function that only keeps the name field
        TestProjectingCalcFunction projectingCalc = new TestProjectingCalcFunction();
        GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedProjectingCalc =
                new GeneratedFunctionWrapper<>(projectingCalc);

        // Create new runner with projecting calc
        AsyncBatchLookupJoinWithCalcRunner projectingRunner =
                new AsyncBatchLookupJoinWithCalcRunner(
                        new GeneratedFunctionWrapper<>(testAsyncFunction),
                        (DataStructureConverter)
                                DataStructureConverters.getConverter(
                                        DataTypes.ROW(
                                                DataTypes.FIELD("id", DataTypes.INT()),
                                                DataTypes.FIELD("name", DataTypes.STRING()))),
                        generatedProjectingCalc,
                        new GeneratedResultFutureWrapper<>(new TestTableFunctionResultFuture()),
                        rightRowSerializer,
                        false,
                        ASYNC_BUFFER_CAPACITY,
                        BATCH_SIZE,
                        FLUSH_INTERVAL_MILLIS);

        projectingRunner.setRuntimeContext(new MockStreamingRuntimeContext(1, 0));
        projectingRunner.open(DefaultOpenContext.INSTANCE);

        CountDownLatch latch = new CountDownLatch(BATCH_SIZE);
        List<RowData> results = Collections.synchronizedList(new ArrayList<>());

        // Create test input data
        List<RowData> inputRows =
                Arrays.asList(
                        GenericRowData.of(1, StringData.fromString("Alice")),
                        GenericRowData.of(2, StringData.fromString("Bob")),
                        GenericRowData.of(3, StringData.fromString("Charlie")));

        // Process inputs
        for (RowData input : inputRows) {
            projectingRunner.asyncInvoke(input, new TestResultFuture(results, latch));
        }

        // Wait for processing
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

        // Verify results have only one field (name)
        assertThat(results).hasSize(BATCH_SIZE);
        for (RowData result : results) {
            assertThat(result.getArity()).isEqualTo(1);
            assertThat(result.getString(0)).isNotNull();
        }
    }

    // Test helper classes

    private static class TestAsyncFunction implements AsyncFunction<List<RowData>, Object> {
        private final AtomicInteger invocationCount = new AtomicInteger(0);
        private volatile int lastBatchSize = 0;

        @Override
        public void asyncInvoke(List<RowData> input, ResultFuture<Object> resultFuture)
                throws Exception {
            invocationCount.incrementAndGet();
            lastBatchSize = input.size();

            // Simulate async processing
            CompletableFuture.runAsync(
                    () -> {
                        try {
                            Thread.sleep(10); // Simulate some processing time
                            List<Object> results = new ArrayList<>();
                            for (RowData row : input) {
                                // Echo back the input as result
                                results.add(row);
                            }
                            resultFuture.complete(results);
                        } catch (Exception e) {
                            resultFuture.completeExceptionally(e);
                        }
                    });
        }

        public int getInvocationCount() {
            return invocationCount.get();
        }

        public int getLastBatchSize() {
            return lastBatchSize;
        }
    }

    private static class TestCalcFunction implements FlatMapFunction<RowData, RowData> {
        private final AtomicInteger invocationCount = new AtomicInteger(0);

        @Override
        public void flatMap(RowData value, Collector<RowData> out) throws Exception {
            invocationCount.incrementAndGet();
            // Transform: add 100 to the ID
            GenericRowData result = GenericRowData.of(value.getInt(0) + 100, value.getString(1));
            out.collect(result);
        }

        public int getInvocationCount() {
            return invocationCount.get();
        }
    }

    private static class TestFilteringCalcFunction implements FlatMapFunction<RowData, RowData> {
        @Override
        public void flatMap(RowData value, Collector<RowData> out) throws Exception {
            // Filter: only pass through even IDs
            if (value.getInt(0) % 2 == 0) {
                out.collect(value);
            }
        }
    }

    private static class TestProjectingCalcFunction implements FlatMapFunction<RowData, RowData> {
        @Override
        public void flatMap(RowData value, Collector<RowData> out) throws Exception {
            // Project: only keep the name field
            GenericRowData result = GenericRowData.of(value.getString(1));
            out.collect(result);
        }
    }

    private static class TestResultFuture implements ResultFuture<RowData> {
        private final List<RowData> results;
        private final CountDownLatch latch;

        public TestResultFuture(List<RowData> results, CountDownLatch latch) {
            this.results = results;
            this.latch = latch;
        }

        @Override
        public void complete(Collection<RowData> result) {
            for (RowData row : result) {
                results.add(row);
                latch.countDown();
            }
        }

        @Override
        public void complete(
                org.apache.flink.streaming.api.functions.async.CollectionSupplier<RowData>
                        supplier) {
            try {
                Collection<RowData> result = supplier.get();
                complete(result);
            } catch (Exception e) {
                completeExceptionally(e);
            }
        }

        @Override
        public void completeExceptionally(Throwable error) {
            // Handle error - for test purposes, just complete the latch
            latch.countDown();
        }
    }

    private static class TestTableFunctionResultFuture
            extends TableFunctionResultFuture<List<RowData>> {
        @Override
        public void complete(Collection<List<RowData>> result) {
            // Implementation for test
        }

        @Override
        public void completeExceptionally(Throwable error) {
            // Implementation for test
        }
    }
}
