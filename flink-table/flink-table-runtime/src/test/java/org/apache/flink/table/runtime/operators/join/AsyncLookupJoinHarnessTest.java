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

package org.apache.flink.table.runtime.operators.join;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperator;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorFactory;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.runtime.collector.TableFunctionCollector;
import org.apache.flink.table.runtime.collector.TableFunctionResultFuture;
import org.apache.flink.table.runtime.generated.GeneratedFunctionWrapper;
import org.apache.flink.table.runtime.generated.GeneratedProjectionWrapper;
import org.apache.flink.table.runtime.generated.GeneratedResultFutureWrapper;
import org.apache.flink.table.runtime.operators.join.lookup.AsyncLookupJoinRunner;
import org.apache.flink.table.runtime.operators.join.lookup.AsyncLookupJoinWithCalcRunner;
import org.apache.flink.table.runtime.operators.join.lookup.LookupCacheHandler;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Collector;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.apache.flink.streaming.api.datastream.AsyncDataStream.OutputMode.ORDERED;
import static org.apache.flink.streaming.api.datastream.AsyncDataStream.OutputMode.UNORDERED;
import static org.apache.flink.table.data.StringData.fromString;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Harness tests for {@link AsyncLookupJoinRunner} and {@link AsyncLookupJoinWithCalcRunner}. */
public class AsyncLookupJoinHarnessTest {

    private static final int ASYNC_BUFFER_CAPACITY = 100;
    private static final int ASYNC_TIMEOUT_MS = 3000;

    private final TypeSerializer<RowData> inSerializer =
            new RowDataSerializer(
                    DataTypes.INT().getLogicalType(), DataTypes.STRING().getLogicalType());

    private final RowDataHarnessAssertor assertor =
            new RowDataHarnessAssertor(
                    new LogicalType[] {
                        DataTypes.INT().getLogicalType(),
                        DataTypes.STRING().getLogicalType(),
                        DataTypes.INT().getLogicalType(),
                        DataTypes.STRING().getLogicalType()
                    });

    private final DataType rightRowDataType =
            DataTypes.ROW(
                            DataTypes.FIELD("f0", DataTypes.INT()),
                            DataTypes.FIELD("f1", DataTypes.STRING()))
                    .bridgedTo(RowData.class);

    @SuppressWarnings({"unchecked", "rawtypes"})
    private final DataStructureConverter<RowData, Object> fetcherConverter =
            (DataStructureConverter) DataStructureConverters.getConverter(rightRowDataType);

    private final RowDataSerializer rightRowSerializer =
            (RowDataSerializer)
                    InternalSerializers.<RowData>create(rightRowDataType.getLogicalType());

    private static Stream<Arguments> argumentsProvider() {
        return Stream.of(
                Arguments.of(ORDERED, Caching.ENABLE_CACHE),
                Arguments.of(UNORDERED, Caching.ENABLE_CACHE),
                Arguments.of(ORDERED, Caching.DISABLE_CACHE),
                Arguments.of(UNORDERED, Caching.DISABLE_CACHE));
    }

    private final List<CloseValidator> closeValidators = new ArrayList<>();

    @BeforeEach
    void beforeEach() {
        closeValidators.clear();
    }

    @AfterEach
    void afterEach() {
        for (CloseValidator closeValidator : closeValidators) {
            assertThat(closeValidator.isClosed())
                    .overridingErrorMessage(
                            "%s is not closed", closeValidator.getClass().getSimpleName())
                    .isTrue();
        }
    }

    @ParameterizedTest
    @MethodSource("argumentsProvider")
    void testTemporalInnerAsyncJoin(AsyncDataStream.OutputMode outputMode, Caching caching)
            throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createHarness(
                        JoinType.INNER_JOIN, FilterOnTable.WITHOUT_FILTER, outputMode, caching);

        testHarness.open();

        synchronized (testHarness.getCheckpointLock()) {
            testHarness.processElement(insertRecord(1, "a"));
            testHarness.processElement(insertRecord(2, "b"));
            testHarness.processElement(insertRecord(3, "c"));
            testHarness.processElement(insertRecord(3, "c"));
            testHarness.processElement(insertRecord(4, "d"));
            testHarness.processElement(insertRecord(5, "e"));
        }

        // wait until all async collectors in the buffer have been emitted out.
        synchronized (testHarness.getCheckpointLock()) {
            testHarness.endInput();
        }

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
        expectedOutput.add(insertRecord(3, "c", 3, "Jark"));
        expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
        expectedOutput.add(insertRecord(3, "c", 3, "Jark"));
        expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
        expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));

        checkResult(expectedOutput, testHarness.getOutput(), outputMode);

        if (caching.equals(Caching.ENABLE_CACHE)) {
            LookupCache cache = getRunner(testHarness).getLookupCache();
            Map<RowData, Collection<RowData>> expectedCachedEntries = new HashMap<>();
            expectedCachedEntries.put(row(1, "a"), Collections.singleton(row(1, "Julian")));
            expectedCachedEntries.put(row(2, "b"), Collections.emptySet());
            expectedCachedEntries.put(
                    row(3, "c"), Arrays.asList(row(3, "Jark"), row(3, "Jackson")));
            expectedCachedEntries.put(row(4, "d"), Collections.singleton(row(4, "Fabian")));
            expectedCachedEntries.put(row(5, "e"), Collections.emptySet());
            validateCache(cache, expectedCachedEntries);
        }

        testHarness.close();
    }

    @ParameterizedTest
    @MethodSource("argumentsProvider")
    void testTemporalInnerAsyncJoinWithFilter(
            AsyncDataStream.OutputMode outputMode, Caching caching) throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createHarness(JoinType.INNER_JOIN, FilterOnTable.WITH_FILTER, outputMode, caching);

        testHarness.open();

        synchronized (testHarness.getCheckpointLock()) {
            testHarness.processElement(insertRecord(1, "a"));
            testHarness.processElement(insertRecord(2, "b"));
            testHarness.processElement(insertRecord(3, "c"));
            testHarness.processElement(insertRecord(3, "c"));
            testHarness.processElement(insertRecord(4, "d"));
            testHarness.processElement(insertRecord(5, "e"));
        }

        // wait until all async collectors in the buffer have been emitted out.
        synchronized (testHarness.getCheckpointLock()) {
            testHarness.endInput();
        }

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
        expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
        expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
        expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));

        checkResult(expectedOutput, testHarness.getOutput(), outputMode);

        if (caching.equals(Caching.ENABLE_CACHE)) {
            LookupCache cache = getRunner(testHarness).getLookupCache();
            assertThat(cache).isNotNull();
            Map<RowData, Collection<RowData>> expectedCachedEntries = new HashMap<>();
            expectedCachedEntries.put(row(1, "a"), Collections.singleton(row(1, "Julian")));
            expectedCachedEntries.put(row(2, "b"), Collections.emptySet());
            expectedCachedEntries.put(row(3, "c"), Collections.singletonList(row(3, "Jackson")));
            expectedCachedEntries.put(row(4, "d"), Collections.singleton(row(4, "Fabian")));
            expectedCachedEntries.put(row(5, "e"), Collections.emptySet());
            validateCache(cache, expectedCachedEntries);
        }

        testHarness.close();
    }

    @ParameterizedTest
    @MethodSource("argumentsProvider")
    void testTemporalLeftAsyncJoin(AsyncDataStream.OutputMode outputMode, Caching caching)
            throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createHarness(
                        JoinType.LEFT_JOIN, FilterOnTable.WITHOUT_FILTER, outputMode, caching);

        testHarness.open();

        synchronized (testHarness.getCheckpointLock()) {
            testHarness.processElement(insertRecord(1, "a"));
            testHarness.processElement(insertRecord(2, "b"));
            testHarness.processElement(insertRecord(3, "c"));
            testHarness.processElement(insertRecord(3, "c"));
            testHarness.processElement(insertRecord(4, "d"));
            testHarness.processElement(insertRecord(5, "e"));
        }

        // wait until all async collectors in the buffer have been emitted out.
        synchronized (testHarness.getCheckpointLock()) {
            testHarness.endInput();
        }

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
        expectedOutput.add(insertRecord(2, "b", null, null));
        expectedOutput.add(insertRecord(3, "c", 3, "Jark"));
        expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
        expectedOutput.add(insertRecord(3, "c", 3, "Jark"));
        expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
        expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));
        expectedOutput.add(insertRecord(5, "e", null, null));

        checkResult(expectedOutput, testHarness.getOutput(), outputMode);

        if (caching.equals(Caching.ENABLE_CACHE)) {
            LookupCache cache = getRunner(testHarness).getLookupCache();
            assertThat(cache).isNotNull();
            Map<RowData, Collection<RowData>> expectedCachedEntries = new HashMap<>();
            expectedCachedEntries.put(row(1, "a"), Collections.singleton(row(1, "Julian")));
            expectedCachedEntries.put(row(2, "b"), Collections.emptySet());
            expectedCachedEntries.put(
                    row(3, "c"), Arrays.asList(row(3, "Jark"), row(3, "Jackson")));
            expectedCachedEntries.put(row(4, "d"), Collections.singleton(row(4, "Fabian")));
            expectedCachedEntries.put(row(5, "e"), Collections.emptySet());
            validateCache(cache, expectedCachedEntries);
        }

        testHarness.close();
    }

    @ParameterizedTest
    @MethodSource("argumentsProvider")
    void testTemporalLeftAsyncJoinWithFilter(AsyncDataStream.OutputMode outputMode, Caching caching)
            throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createHarness(JoinType.LEFT_JOIN, FilterOnTable.WITH_FILTER, outputMode, caching);

        testHarness.open();

        synchronized (testHarness.getCheckpointLock()) {
            testHarness.processElement(insertRecord(1, "a"));
            testHarness.processElement(insertRecord(2, "b"));
            testHarness.processElement(insertRecord(3, "c"));
            testHarness.processElement(insertRecord(3, "c"));
            testHarness.processElement(insertRecord(4, "d"));
            testHarness.processElement(insertRecord(5, "e"));
        }

        // wait until all async collectors in the buffer have been emitted out.
        synchronized (testHarness.getCheckpointLock()) {
            testHarness.endInput();
        }

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
        expectedOutput.add(insertRecord(2, "b", null, null));
        expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
        expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
        expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));
        expectedOutput.add(insertRecord(5, "e", null, null));

        checkResult(expectedOutput, testHarness.getOutput(), outputMode);

        if (caching.equals(Caching.ENABLE_CACHE)) {
            LookupCache cache = getRunner(testHarness).getLookupCache();
            assertThat(cache).isNotNull();
            Map<RowData, Collection<RowData>> expectedCachedEntries = new HashMap<>();
            expectedCachedEntries.put(row(1, "a"), Collections.singleton(row(1, "Julian")));
            expectedCachedEntries.put(row(2, "b"), Collections.emptySet());
            expectedCachedEntries.put(row(3, "c"), Collections.singletonList(row(3, "Jackson")));
            expectedCachedEntries.put(row(4, "d"), Collections.singleton(row(4, "Fabian")));
            expectedCachedEntries.put(row(5, "e"), Collections.emptySet());
            validateCache(cache, expectedCachedEntries);
        }

        testHarness.close();
    }

    // ---------------------------------------------------------------------------------

    private void checkResult(
            Collection<Object> expectedOutput,
            Collection<Object> actualOutput,
            AsyncDataStream.OutputMode outputMode) {
        if (outputMode.equals(ORDERED)) {
            assertor.assertOutputEquals("output wrong.", expectedOutput, actualOutput);
        } else {
            assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, actualOutput);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private OneInputStreamOperatorTestHarness<RowData, RowData> createHarness(
            JoinType joinType,
            FilterOnTable filterOnTable,
            AsyncDataStream.OutputMode outputMode,
            Caching caching)
            throws Exception {
        RichAsyncFunction<RowData, RowData> joinRunner;
        boolean isLeftJoin = joinType == JoinType.LEFT_JOIN;
        LookupCacheHandler cacheHandler = null;
        if (caching == Caching.ENABLE_CACHE) {
            cacheHandler =
                    new LookupCacheHandler(
                            DefaultLookupCache.newBuilder().maximumSize(Long.MAX_VALUE).build(),
                            RandomStringUtils.randomAlphabetic(10),
                            new GeneratedProjectionWrapper<>(
                                    new LookupJoinHarnessTest.CacheKeyProjection()));
        }
        if (filterOnTable == FilterOnTable.WITHOUT_FILTER) {
            joinRunner =
                    new AsyncLookupJoinRunner(
                            new GeneratedFunctionWrapper(
                                    new TestingFetcherFunction(),
                                    (function) -> closeValidators.add(((CloseValidator) function))),
                            fetcherConverter,
                            new GeneratedResultFutureWrapper<>(new TestingFetcherResultFuture()),
                            rightRowSerializer,
                            isLeftJoin,
                            ASYNC_BUFFER_CAPACITY,
                            cacheHandler);
        } else {
            joinRunner =
                    new AsyncLookupJoinWithCalcRunner(
                            new GeneratedFunctionWrapper(
                                    new TestingFetcherFunction(),
                                    (function) -> closeValidators.add(((CloseValidator) function))),
                            fetcherConverter,
                            new GeneratedFunctionWrapper<>(
                                    new CalculateOnTemporalTable(),
                                    (function) -> closeValidators.add(((CloseValidator) function))),
                            new GeneratedResultFutureWrapper<>(new TestingFetcherResultFuture()),
                            rightRowSerializer,
                            isLeftJoin,
                            ASYNC_BUFFER_CAPACITY,
                            cacheHandler);
        }

        return new OneInputStreamOperatorTestHarness<>(
                new AsyncWaitOperatorFactory<>(
                        joinRunner, ASYNC_TIMEOUT_MS, ASYNC_BUFFER_CAPACITY, outputMode),
                inSerializer);
    }

    @ParameterizedTest
    @EnumSource(Caching.class)
    public void testCloseAsyncLookupJoinRunner(Caching caching) throws Exception {
        LookupCacheHandler cacheHandler = null;
        if (caching == Caching.ENABLE_CACHE) {
            cacheHandler =
                    new LookupCacheHandler(
                            DefaultLookupCache.newBuilder().maximumSize(Long.MAX_VALUE).build(),
                            RandomStringUtils.randomAlphabetic(10),
                            new GeneratedProjectionWrapper<>(
                                    new LookupJoinHarnessTest.CacheKeyProjection()));
        }
        final AsyncLookupJoinRunner joinRunner =
                new AsyncLookupJoinRunner(
                        new GeneratedFunctionWrapper(new TestingFetcherFunction()),
                        fetcherConverter,
                        new GeneratedResultFutureWrapper<>(new TestingFetcherResultFuture()),
                        rightRowSerializer,
                        true,
                        100,
                        cacheHandler);
        assertThat(joinRunner.getAllResultFutures()).isNull();
        closeAsyncLookupJoinRunner(joinRunner);

        joinRunner.setRuntimeContext(new MockStreamingRuntimeContext(false, 1, 0));
        joinRunner.open(new Configuration());
        assertThat(joinRunner.getAllResultFutures()).isNotNull();
        closeAsyncLookupJoinRunner(joinRunner);

        joinRunner.open(new Configuration());
        joinRunner.asyncInvoke(row(1, "a"), new TestingFetcherResultFuture());
        assertThat(joinRunner.getAllResultFutures()).isNotNull();
        closeAsyncLookupJoinRunner(joinRunner);
    }

    private void closeAsyncLookupJoinRunner(AsyncLookupJoinRunner joinRunner) throws Exception {
        try {
            joinRunner.close();
        } catch (NullPointerException e) {
            fail("Unexpected close to fail with null pointer exception.", e);
        }
    }

    private AsyncLookupJoinRunner getRunner(
            OneInputStreamOperatorTestHarness<RowData, RowData> testHarness) {
        return ((AsyncLookupJoinRunner)
                ((AsyncWaitOperator<RowData, RowData>) testHarness.getOneInputOperator())
                        .getUserFunction());
    }

    private void validateCache(
            LookupCache cache, Map<RowData, Collection<RowData>> expectedCachedEntries) {
        for (Map.Entry<RowData, Collection<RowData>> entry : expectedCachedEntries.entrySet()) {
            assertThat(cache.getIfPresent(entry.getKey()))
                    .overridingErrorMessage("Unable to find '%s' in the cache", entry.getKey())
                    .isNotNull();
            assertThat(cache.getIfPresent(entry.getKey()))
                    .containsExactlyInAnyOrderElementsOf(entry.getValue());
        }
        assertThat(cache.size()).isEqualTo(expectedCachedEntries.size());
    }

    /** Whether this is a inner join or left join. */
    private enum JoinType {
        INNER_JOIN,
        LEFT_JOIN
    }

    /** Whether there is a filter on temporal table. */
    private enum FilterOnTable {
        WITH_FILTER,
        WITHOUT_FILTER
    }

    // ---------------------------------------------------------------------------------

    /**
     * The {@link TestingFetcherFunction} only accepts a single integer lookup key and returns zero
     * or one or more RowData.
     */
    public static final class TestingFetcherFunction extends AbstractRichFunction
            implements AsyncFunction<RowData, RowData>, CloseValidator {

        private static final long serialVersionUID = 4018474964018227081L;

        private static final Map<Integer, List<RowData>> data = new HashMap<>();

        private final Random random = new Random();

        private boolean isOpened = false;
        private boolean isClosed = false;

        static {
            data.put(1, Collections.singletonList(GenericRowData.of(1, fromString("Julian"))));
            data.put(
                    3,
                    Arrays.asList(
                            GenericRowData.of(3, fromString("Jark")),
                            GenericRowData.of(3, fromString("Jackson"))));
            data.put(4, Collections.singletonList(GenericRowData.of(4, fromString("Fabian"))));
        }

        private transient ExecutorService executor;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // generate unordered result for async lookup
            this.executor = Executors.newFixedThreadPool(2);
            isOpened = true;
        }

        @Override
        public void asyncInvoke(RowData input, ResultFuture<RowData> resultFuture)
                throws Exception {
            assertThat(isOpened)
                    .overridingErrorMessage("TestingFetcherFunction is not opened before invoke")
                    .isTrue();
            int id = input.getInt(0);
            CompletableFuture.supplyAsync(
                            (Supplier<Collection<RowData>>)
                                    () -> {
                                        try {
                                            Thread.sleep(random.nextInt(5));
                                        } catch (InterruptedException e) {
                                            throw new RuntimeException(e);
                                        }
                                        return data.get(id);
                                    },
                            executor)
                    .thenAcceptAsync(resultFuture::complete, executor);
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (null != executor && !executor.isShutdown()) {
                executor.shutdown();
            }
            isClosed = true;
        }

        @Override
        public boolean isClosed() {
            return isClosed;
        }
    }

    /**
     * The {@link TestingFetcherResultFuture} is a simple implementation of {@link
     * TableFunctionCollector} which forwards the collected collection.
     */
    public static final class TestingFetcherResultFuture
            extends TableFunctionResultFuture<RowData> {
        private static final long serialVersionUID = -312754413938303160L;

        @Override
        public void complete(Collection<RowData> result) {
            //noinspection unchecked
            getResultFuture().complete((Collection) result);
        }
    }

    /**
     * The {@link CalculateOnTemporalTable} is a filter on temporal table which only accepts length
     * of name greater than or equal to 6.
     */
    public static final class CalculateOnTemporalTable extends RichFlatMapFunction<RowData, RowData>
            implements CloseValidator {

        private static final long serialVersionUID = -1860345072157431136L;
        private boolean isOpened = false;
        private boolean isClosed = false;

        @Override
        public void open(Configuration parameters) throws Exception {
            isOpened = true;
        }

        @Override
        public void flatMap(RowData value, Collector<RowData> out) throws Exception {
            assertThat(isOpened)
                    .overridingErrorMessage("CalculateOnTemporalTable is not opened before invoke")
                    .isTrue();
            BinaryStringData name = (BinaryStringData) value.getString(1);
            if (name.getSizeInBytes() >= 6) {
                out.collect(value);
            }
        }

        @Override
        public void close() throws Exception {
            isClosed = true;
        }

        @Override
        public boolean isClosed() {
            return isClosed;
        }
    }

    private enum Caching {
        ENABLE_CACHE,
        DISABLE_CACHE
    }

    private interface CloseValidator {
        boolean isClosed();
    }
}
