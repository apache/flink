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

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.collector.TableFunctionCollector;
import org.apache.flink.table.runtime.generated.GeneratedCollectorWrapper;
import org.apache.flink.table.runtime.generated.GeneratedFunctionWrapper;
import org.apache.flink.table.runtime.generated.GeneratedProjectionWrapper;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.operators.join.lookup.LookupCacheHandler;
import org.apache.flink.table.runtime.operators.join.lookup.LookupJoinRunner;
import org.apache.flink.table.runtime.operators.join.lookup.LookupJoinWithCalcRunner;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Collector;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.apache.flink.table.data.StringData.fromString;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Harness tests for {@link LookupJoinRunner} and {@link LookupJoinWithCalcRunner}. */
public class LookupJoinHarnessTest {

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
    @EnumSource(Caching.class)
    void testTemporalInnerJoin(Caching caching) throws Exception {
        AtomicReference<TestingFetcherFunction> fetcher = new AtomicReference<>();
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createHarness(
                        JoinType.INNER_JOIN, FilterOnTable.WITHOUT_FILTER, caching, fetcher::set);

        testHarness.open();

        testHarness.processElement(insertRecord(1, "a"));
        testHarness.processElement(insertRecord(2, "b"));
        testHarness.processElement(insertRecord(3, "c"));
        testHarness.processElement(insertRecord(3, "c"));
        testHarness.processElement(insertRecord(4, "d"));
        testHarness.processElement(insertRecord(5, "e"));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
        expectedOutput.add(insertRecord(3, "c", 3, "Jark"));
        expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
        expectedOutput.add(insertRecord(3, "c", 3, "Jark"));
        expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
        expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));

        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());

        if (caching.equals(Caching.ENABLE_CACHE)) {
            LookupCache cache = getRunner(testHarness).getLookupCache();
            assertThat(cache).isNotNull();
            Map<RowData, Collection<RowData>> expectedCachedEntries = new HashMap<>();
            // Rows with
            expectedCachedEntries.put(row(1), Collections.singleton(row(1, "Julian")));
            expectedCachedEntries.put(row(2), Collections.emptySet());
            expectedCachedEntries.put(row(3), Arrays.asList(row(3, "Jark"), row(3, "Jackson")));
            expectedCachedEntries.put(row(4), Collections.singleton(row(4, "Fabian")));
            expectedCachedEntries.put(row(5), Collections.emptySet());
            validateCache(cache, expectedCachedEntries);
            assertThat(fetcher.get().fetchCounter).isEqualTo(5);
        }

        testHarness.close();
    }

    @ParameterizedTest
    @EnumSource(Caching.class)
    void testTemporalInnerJoinWithFilter(Caching caching) throws Exception {
        AtomicReference<TestingFetcherFunction> fetcher = new AtomicReference<>();
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createHarness(
                        JoinType.INNER_JOIN, FilterOnTable.WITH_FILTER, caching, fetcher::set);

        testHarness.open();

        testHarness.processElement(insertRecord(1, "a"));
        testHarness.processElement(insertRecord(2, "b"));
        testHarness.processElement(insertRecord(3, "c"));
        testHarness.processElement(insertRecord(3, "c"));
        testHarness.processElement(insertRecord(4, "d"));
        testHarness.processElement(insertRecord(5, "e"));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
        expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
        expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
        expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));

        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());

        if (caching.equals(Caching.ENABLE_CACHE)) {
            LookupCache cache = getRunner(testHarness).getLookupCache();
            assertThat(cache).isNotNull();
            Map<RowData, Collection<RowData>> expectedCachedEntries = new HashMap<>();
            expectedCachedEntries.put(row(1), Collections.singleton(row(1, "Julian")));
            expectedCachedEntries.put(row(2), Collections.emptySet());
            expectedCachedEntries.put(row(3), Arrays.asList(row(3, "Jark"), row(3, "Jackson")));
            expectedCachedEntries.put(row(4), Collections.singleton(row(4, "Fabian")));
            expectedCachedEntries.put(row(5), Collections.emptySet());
            validateCache(cache, expectedCachedEntries);
            assertThat(fetcher.get().fetchCounter).isEqualTo(5);
        }

        testHarness.close();
    }

    @ParameterizedTest
    @EnumSource(Caching.class)
    void testTemporalLeftJoin(Caching caching) throws Exception {
        AtomicReference<TestingFetcherFunction> fetcher = new AtomicReference<>();
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createHarness(
                        JoinType.LEFT_JOIN, FilterOnTable.WITHOUT_FILTER, caching, fetcher::set);

        testHarness.open();

        testHarness.processElement(insertRecord(1, "a"));
        testHarness.processElement(insertRecord(2, "b"));
        testHarness.processElement(insertRecord(3, "c"));
        testHarness.processElement(insertRecord(3, "c"));
        testHarness.processElement(insertRecord(4, "d"));
        testHarness.processElement(insertRecord(5, "e"));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
        expectedOutput.add(insertRecord(2, "b", null, null));
        expectedOutput.add(insertRecord(3, "c", 3, "Jark"));
        expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
        expectedOutput.add(insertRecord(3, "c", 3, "Jark"));
        expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
        expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));
        expectedOutput.add(insertRecord(5, "e", null, null));

        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());

        if (caching.equals(Caching.ENABLE_CACHE)) {
            LookupCache cache = getRunner(testHarness).getLookupCache();
            assertThat(cache).isNotNull();
            Map<RowData, Collection<RowData>> expectedCachedEntries = new HashMap<>();
            expectedCachedEntries.put(row(1), Collections.singleton(row(1, "Julian")));
            expectedCachedEntries.put(row(2), Collections.emptySet());
            expectedCachedEntries.put(row(3), Arrays.asList(row(3, "Jark"), row(3, "Jackson")));
            expectedCachedEntries.put(row(4), Collections.singleton(row(4, "Fabian")));
            expectedCachedEntries.put(row(5), Collections.emptySet());
            validateCache(cache, expectedCachedEntries);
            assertThat(fetcher.get().fetchCounter).isEqualTo(5);
        }

        testHarness.close();
    }

    @ParameterizedTest
    @EnumSource(Caching.class)
    void testTemporalLeftJoinWithFilter(Caching caching) throws Exception {
        AtomicReference<TestingFetcherFunction> fetcher = new AtomicReference<>();
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createHarness(JoinType.LEFT_JOIN, FilterOnTable.WITH_FILTER, caching, fetcher::set);

        testHarness.open();

        testHarness.processElement(insertRecord(1, "a"));
        testHarness.processElement(insertRecord(2, "b"));
        testHarness.processElement(insertRecord(3, "c"));
        testHarness.processElement(insertRecord(3, "c"));
        testHarness.processElement(insertRecord(4, "d"));
        testHarness.processElement(insertRecord(5, "e"));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
        expectedOutput.add(insertRecord(2, "b", null, null));
        expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
        expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
        expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));
        expectedOutput.add(insertRecord(5, "e", null, null));

        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());

        if (caching.equals(Caching.ENABLE_CACHE)) {
            LookupCache cache = getRunner(testHarness).getLookupCache();
            assertThat(cache).isNotNull();
            Map<RowData, Collection<RowData>> expectedCachedEntries = new HashMap<>();
            expectedCachedEntries.put(row(1), Collections.singleton(row(1, "Julian")));
            expectedCachedEntries.put(row(2), Collections.emptySet());
            expectedCachedEntries.put(row(3), Arrays.asList(row(3, "Jark"), row(3, "Jackson")));
            expectedCachedEntries.put(row(4), Collections.singleton(row(4, "Fabian")));
            expectedCachedEntries.put(row(5), Collections.emptySet());
            validateCache(cache, expectedCachedEntries);
            assertThat(fetcher.get().fetchCounter).isEqualTo(5);
        }

        testHarness.close();
    }

    @ParameterizedTest
    @EnumSource(JoinType.class)
    void testSharedCacheAcrossSubtasks(JoinType joinType) throws Exception {
        AtomicReference<TestingFetcherFunction> fetcherA = new AtomicReference<>();
        AtomicReference<TestingFetcherFunction> fetcherB = new AtomicReference<>();
        String tableIdentifier = "shared-cache-" + joinType;
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarnessA =
                createHarness(
                        joinType,
                        FilterOnTable.WITH_FILTER,
                        Caching.ENABLE_CACHE,
                        fetcherA::set,
                        tableIdentifier);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarnessB =
                createHarness(
                        joinType,
                        FilterOnTable.WITH_FILTER,
                        Caching.ENABLE_CACHE,
                        fetcherB::set,
                        tableIdentifier);

        testHarnessA.open();
        testHarnessB.open();

        List<StreamRecord<RowData>> records = new ArrayList<>();
        records.add(insertRecord(1, "a"));
        records.add(insertRecord(2, "b"));
        records.add(insertRecord(3, "c"));
        records.add(insertRecord(3, "c"));
        records.add(insertRecord(4, "d"));
        records.add(insertRecord(5, "e"));

        // Let runner B process elements after runner A
        for (StreamRecord<RowData> record : records) {
            testHarnessA.processElement(record);
            testHarnessB.processElement(record);
        }

        // Runner A and B should share the same cache
        LookupCache cache = getRunner(testHarnessA).getLookupCache();
        assertThat(cache).isNotNull();
        assertThat(getRunner(testHarnessB).getLookupCache()).isSameAs(cache);

        // Validate entries in the cache
        Map<RowData, Collection<RowData>> expectedCachedEntries = new HashMap<>();
        expectedCachedEntries.put(row(1), Collections.singleton(row(1, "Julian")));
        expectedCachedEntries.put(row(2), Collections.emptySet());
        expectedCachedEntries.put(row(3), Arrays.asList(row(3, "Jark"), row(3, "Jackson")));
        expectedCachedEntries.put(row(4), Collections.singleton(row(4, "Fabian")));
        expectedCachedEntries.put(row(5), Collections.emptySet());
        validateCache(cache, expectedCachedEntries);

        // Fetcher A should be invoked for 5 times
        assertThat(fetcherA.get().fetchCounter).isEqualTo(5);
        // Fetcher B should never be invoked because of the cache hit
        assertThat(fetcherB.get().fetchCounter).isZero();

        testHarnessA.close();
        testHarnessB.close();
    }

    // ---------------------------------------------------------------------------------

    private OneInputStreamOperatorTestHarness<RowData, RowData> createHarness(
            JoinType joinType,
            FilterOnTable filterOnTable,
            Caching caching,
            Consumer<TestingFetcherFunction> fetcherConsumer)
            throws Exception {
        return createHarness(
                joinType,
                filterOnTable,
                caching,
                fetcherConsumer,
                RandomStringUtils.randomAlphabetic(10));
    }

    @SuppressWarnings("unchecked")
    private OneInputStreamOperatorTestHarness<RowData, RowData> createHarness(
            JoinType joinType,
            FilterOnTable filterOnTable,
            Caching caching,
            Consumer<TestingFetcherFunction> fetcherConsumer,
            String tableIdentifier)
            throws Exception {
        boolean isLeftJoin = joinType == JoinType.LEFT_JOIN;
        ProcessFunction<RowData, RowData> joinRunner;
        LookupCacheHandler cacheHandler = null;
        if (caching == Caching.ENABLE_CACHE) {
            cacheHandler =
                    new LookupCacheHandler(
                            DefaultLookupCache.newBuilder().maximumSize(Long.MAX_VALUE).build(),
                            tableIdentifier,
                            new GeneratedProjectionWrapper<>(new CacheKeyProjection()));
        }
        if (filterOnTable == FilterOnTable.WITHOUT_FILTER) {
            joinRunner =
                    new LookupJoinRunner(
                            new GeneratedFunctionWrapper<>(
                                    new TestingFetcherFunction(),
                                    function -> {
                                        fetcherConsumer.accept((TestingFetcherFunction) function);
                                        closeValidators.add(((CloseValidator) function));
                                    }),
                            new GeneratedCollectorWrapper<TableFunctionCollector<RowData>>(
                                    new TestingFetcherCollector(),
                                    collector -> closeValidators.add(((CloseValidator) collector))),
                            isLeftJoin,
                            2,
                            cacheHandler);
        } else {
            joinRunner =
                    new LookupJoinWithCalcRunner(
                            new GeneratedFunctionWrapper<>(
                                    new TestingFetcherFunction(),
                                    function -> {
                                        fetcherConsumer.accept((TestingFetcherFunction) function);
                                        closeValidators.add(((CloseValidator) function));
                                    }),
                            new GeneratedFunctionWrapper<>(new CalculateOnTemporalTable()),
                            new GeneratedCollectorWrapper<TableFunctionCollector<RowData>>(
                                    new TestingFetcherCollector(),
                                    collector -> closeValidators.add(((CloseValidator) collector))),
                            isLeftJoin,
                            2,
                            cacheHandler);
        }

        ProcessOperator<RowData, RowData> operator = new ProcessOperator<>(joinRunner);
        return new OneInputStreamOperatorTestHarness<>(operator, inSerializer);
    }

    private void validateCache(
            LookupCache cache, Map<RowData, Collection<RowData>> expectedCachedEntries) {
        for (Map.Entry<RowData, Collection<RowData>> entry : expectedCachedEntries.entrySet()) {
            assertThat(cache.getIfPresent(entry.getKey())).isNotNull();
            assertThat(cache.getIfPresent(entry.getKey()))
                    .containsExactlyInAnyOrderElementsOf(entry.getValue());
        }
        assertThat(cache.size()).isEqualTo(expectedCachedEntries.size());
    }

    private LookupJoinRunner getRunner(
            OneInputStreamOperatorTestHarness<RowData, RowData> testHarness) {
        return ((LookupJoinRunner)
                ((ProcessOperator<RowData, RowData>) testHarness.getOneInputOperator())
                        .getUserFunction());
    }

    /** Whether this is an inner join or left join. */
    private enum JoinType {
        INNER_JOIN,
        LEFT_JOIN
    }

    /** Whether there is a filter on temporal table. */
    private enum FilterOnTable {
        WITH_FILTER,
        WITHOUT_FILTER
    }

    private enum Caching {
        ENABLE_CACHE,
        DISABLE_CACHE
    }

    // ---------------------------------------------------------------------------------

    /**
     * The {@link TestingFetcherFunction} only accepts a single integer lookup key and returns zero
     * or one or more RowData.
     */
    public static final class TestingFetcherFunction extends RichFlatMapFunction<RowData, RowData>
            implements CloseValidator {

        private static final long serialVersionUID = -494579552308003331L;
        private static final Map<Integer, List<GenericRowData>> data = new HashMap<>();
        private boolean isOpened = false;
        private boolean isClosed = false;
        private int fetchCounter;

        static {
            data.put(1, Collections.singletonList(GenericRowData.of(1, fromString("Julian"))));
            data.put(
                    3,
                    Arrays.asList(
                            GenericRowData.of(3, fromString("Jark")),
                            GenericRowData.of(3, fromString("Jackson"))));
            data.put(4, Collections.singletonList(GenericRowData.of(4, fromString("Fabian"))));
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            isOpened = true;
            fetchCounter = 0;
        }

        @Override
        public void flatMap(RowData value, Collector<RowData> out) throws Exception {
            assertThat(isOpened)
                    .overridingErrorMessage("TestingFetcherFunction is not opened before collect")
                    .isTrue();
            fetchCounter++;
            int id = value.getInt(0);
            List<GenericRowData> rows = data.get(id);
            if (rows != null) {
                for (GenericRowData row : rows) {
                    out.collect(row);
                }
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

    /**
     * The {@link TestingFetcherCollector} is a simple implementation of {@link
     * TableFunctionCollector} which combines left and right into a JoinedRowData.
     */
    public static final class TestingFetcherCollector extends TableFunctionCollector
            implements CloseValidator {
        private static final long serialVersionUID = -2361970630447540259L;
        private boolean isOpened = false;
        private boolean isClosed = false;

        @Override
        public void open(Configuration parameters) throws Exception {
            isOpened = true;
        }

        @Override
        public void collect(Object record) {
            assertThat(isOpened)
                    .overridingErrorMessage("TestingFetcherCollector is not opened before collect")
                    .isTrue();
            RowData left = (RowData) getInput();
            RowData right = (RowData) record;
            outputResult(new JoinedRowData(left, right));
        }

        @Override
        public void close() {
            isClosed = true;
        }

        @Override
        public boolean isClosed() {
            return isClosed;
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
                    .overridingErrorMessage("CalculateOnTemporalTable is not opened before collect")
                    .isTrue();
            BinaryStringData name = (BinaryStringData) value.getString(1);
            if (name.getSizeInBytes() >= 6) {
                out.collect(value);
            }
        }

        @Override
        public void close() {
            isClosed = true;
        }

        @Override
        public boolean isClosed() {
            return isClosed;
        }
    }

    public static final class CacheKeyProjection implements Projection<RowData, RowData> {

        @Override
        public RowData apply(RowData row) {
            return GenericRowData.of(row.getInt(0));
        }
    }

    private interface CloseValidator {
        boolean isClosed();
    }
}
