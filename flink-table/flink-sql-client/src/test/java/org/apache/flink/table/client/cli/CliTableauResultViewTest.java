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

package org.apache.flink.table.client.cli;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.client.config.ResultMode;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.client.gateway.result.ChangelogResult;
import org.apache.flink.table.client.util.CliClientTestUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.jline.terminal.Terminal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;
import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.client.config.SqlClientOptions.DISPLAY_MAX_COLUMN_WIDTH;
import static org.apache.flink.table.client.config.SqlClientOptions.EXECUTION_RESULT_MODE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for CliTableauResultView. */
class CliTableauResultViewTest {
    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    private ByteArrayOutputStream terminalOutput;
    private Terminal terminal;
    private ResolvedSchema schema;
    private List<RowData> data;
    private List<RowData> streamingData;

    @BeforeEach
    void setUp() {
        terminalOutput = new ByteArrayOutputStream();
        terminal = TerminalUtils.createDumbTerminal(terminalOutput);

        schema =
                ResolvedSchema.of(
                        Column.physical("boolean", DataTypes.BOOLEAN()),
                        Column.physical("int", DataTypes.INT()),
                        Column.physical("bigint", DataTypes.BIGINT()),
                        Column.physical("varchar", DataTypes.STRING()),
                        Column.physical("decimal(10, 5)", DataTypes.DECIMAL(10, 5)),
                        Column.physical(
                                "timestamp", DataTypes.TIMESTAMP(6).bridgedTo(Timestamp.class)),
                        Column.physical("binary", DataTypes.BYTES()));

        List<Row> rows =
                Arrays.asList(
                        Row.ofKind(
                                RowKind.INSERT,
                                null,
                                1,
                                2L,
                                "abc",
                                BigDecimal.valueOf(1.23),
                                Timestamp.valueOf("2020-03-01 18:39:14"),
                                new byte[] {50, 51, 52, -123, 54, 93, 115, 126}),
                        Row.ofKind(
                                RowKind.UPDATE_BEFORE,
                                false,
                                null,
                                0L,
                                "",
                                BigDecimal.valueOf(1),
                                Timestamp.valueOf("2020-03-01 18:39:14.1"),
                                new byte[] {100, -98, 32, 121, -125}),
                        Row.ofKind(
                                RowKind.UPDATE_AFTER,
                                true,
                                Integer.MAX_VALUE,
                                null,
                                "abcdefg",
                                BigDecimal.valueOf(12345),
                                Timestamp.valueOf("2020-03-01 18:39:14.12"),
                                new byte[] {-110, -23, 1, 2}),
                        Row.ofKind(
                                RowKind.DELETE,
                                false,
                                Integer.MIN_VALUE,
                                Long.MAX_VALUE,
                                null,
                                BigDecimal.valueOf(12345.06789),
                                Timestamp.valueOf("2020-03-01 18:39:14.123"),
                                new byte[] {50, 51, 52, -123, 54, 93, 115, 126}),
                        Row.ofKind(
                                RowKind.INSERT,
                                true,
                                100,
                                Long.MIN_VALUE,
                                "abcdefg111",
                                null,
                                Timestamp.valueOf("2020-03-01 18:39:14.123456"),
                                new byte[] {110, 23, -1, -2}),
                        Row.ofKind(
                                RowKind.DELETE,
                                null,
                                -1,
                                -1L,
                                "abcdefghijklmnopqrstuvwxyz12345",
                                BigDecimal.valueOf(-12345.06789),
                                null,
                                null),
                        Row.ofKind(
                                RowKind.INSERT,
                                null,
                                -1,
                                -1L,
                                "这是一段中文",
                                BigDecimal.valueOf(-12345.06789),
                                Timestamp.valueOf("2020-03-04 18:39:14"),
                                new byte[] {-3, -2, -1, 0, 1, 2, 3}),
                        Row.ofKind(
                                RowKind.DELETE,
                                null,
                                -1,
                                -1L,
                                "これは日本語をテストするための文です",
                                BigDecimal.valueOf(-12345.06789),
                                Timestamp.valueOf("2020-03-04 18:39:14"),
                                new byte[] {-3, -2, -1, 0, 1, 2, 3}));

        final DataStructureConverter<Object, Object> dataStructureConverter =
                DataStructureConverters.getConverter(schema.toPhysicalRowDataType());

        data =
                rows.stream()
                        .map(r -> (RowData) (dataStructureConverter.toInternal(r)))
                        .collect(Collectors.toList());
        streamingData =
                rows.stream()
                        .map(r -> (RowData) (dataStructureConverter.toInternal(r)))
                        .collect(Collectors.toList());
    }

    @Test
    void testBatchResult() {
        final Configuration testConfig = new Configuration();
        testConfig.set(EXECUTION_RESULT_MODE, ResultMode.TABLEAU);
        testConfig.set(RUNTIME_MODE, RuntimeExecutionMode.BATCH);

        ResultDescriptor resultDescriptor =
                new ResultDescriptor(CliClientTestUtils.createTestClient(schema), testConfig);
        TestChangelogResult collectResult =
                new TestChangelogResult(
                        () -> TypedResult.payload(data.subList(0, data.size() / 2)),
                        () -> TypedResult.payload(data.subList(data.size() / 2, data.size())),
                        TypedResult::endOfStream);
        CliTableauResultView view =
                new CliTableauResultView(terminal, resultDescriptor, collectResult);
        view.displayResults();
        assertThat(terminalOutput)
                .hasToString(
                        "+---------+-------------+----------------------+--------------------------------+----------------+----------------------------+---------------------+"
                                + System.lineSeparator()
                                + "| boolean |         int |               bigint |                        varchar | decimal(10, 5) |                  timestamp |              binary |"
                                + System.lineSeparator()
                                + "+---------+-------------+----------------------+--------------------------------+----------------+----------------------------+---------------------+"
                                + System.lineSeparator()
                                + "|  <NULL> |           1 |                    2 |                            abc |        1.23000 | 2020-03-01 18:39:14.000000 | x'32333485365d737e' |"
                                + System.lineSeparator()
                                + "|   FALSE |      <NULL> |                    0 |                                |        1.00000 | 2020-03-01 18:39:14.100000 |       x'649e207983' |"
                                + System.lineSeparator()
                                + "|    TRUE |  2147483647 |               <NULL> |                        abcdefg |    12345.00000 | 2020-03-01 18:39:14.120000 |         x'92e90102' |"
                                + System.lineSeparator()
                                + "|   FALSE | -2147483648 |  9223372036854775807 |                         <NULL> |    12345.06789 | 2020-03-01 18:39:14.123000 | x'32333485365d737e' |"
                                + System.lineSeparator()
                                + "|    TRUE |         100 | -9223372036854775808 |                     abcdefg111 |         <NULL> | 2020-03-01 18:39:14.123456 |         x'6e17fffe' |"
                                + System.lineSeparator()
                                + "|  <NULL> |          -1 |                   -1 | abcdefghijklmnopqrstuvwxyz1... |   -12345.06789 |                     <NULL> |              <NULL> |"
                                + System.lineSeparator()
                                + "|  <NULL> |          -1 |                   -1 |                   这是一段中文 |   -12345.06789 | 2020-03-04 18:39:14.000000 |   x'fdfeff00010203' |"
                                + System.lineSeparator()
                                + "|  <NULL> |          -1 |                   -1 |  これは日本語をテストするた... |   -12345.06789 | 2020-03-04 18:39:14.000000 |   x'fdfeff00010203' |"
                                + System.lineSeparator()
                                + "+---------+-------------+----------------------+--------------------------------+----------------+----------------------------+---------------------+"
                                + System.lineSeparator()
                                + "8 rows in set"
                                + System.lineSeparator());

        // adjust the max column width for printing
        testConfig.set(DISPLAY_MAX_COLUMN_WIDTH, 80);

        collectResult =
                new TestChangelogResult(
                        () -> TypedResult.payload(data.subList(0, data.size() / 2)),
                        () -> TypedResult.payload(data.subList(data.size() / 2, data.size())),
                        TypedResult::endOfStream);
        view = new CliTableauResultView(terminal, resultDescriptor, collectResult);
        view.displayResults();
        assertThat(terminalOutput.toString()).contains("abcdefghijklmnopqrstuvwxyz12345");

        view.close();
        // Job is finished. Don't need to close the job manually.
        assertThat(collectResult.closed).isFalse();
    }

    @Test
    void testCancelBatchResult() throws Exception {
        final Configuration testConfig = new Configuration();
        testConfig.set(EXECUTION_RESULT_MODE, ResultMode.TABLEAU);
        testConfig.set(RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        ResultDescriptor resultDescriptor =
                new ResultDescriptor(CliClientTestUtils.createTestClient(schema), testConfig);
        TestChangelogResult collectResult =
                new TestChangelogResult(
                        () -> TypedResult.payload(data.subList(0, data.size() / 2)),
                        TypedResult::empty);
        CliTableauResultView view =
                new CliTableauResultView(terminal, resultDescriptor, collectResult);

        // submit result display in another thread
        Future<?> furture = EXECUTOR_RESOURCE.getExecutor().submit(view::displayResults);

        // wait until we try to get blocking iterator
        CommonTestUtils.waitUntilCondition(() -> collectResult.fetchCount.get() > 1, 50L);

        // send signal to cancel
        terminal.raise(Terminal.Signal.INT);
        furture.get(5, TimeUnit.SECONDS);

        assertThat(terminalOutput)
                .hasToString(
                        "Query terminated, received a total of 0 row" + System.lineSeparator());

        assertThat(collectResult.closed).isTrue();
        view.close();
    }

    @Test
    void testEmptyBatchResult() {
        final Configuration testConfig = new Configuration();
        testConfig.set(EXECUTION_RESULT_MODE, ResultMode.TABLEAU);
        testConfig.set(RUNTIME_MODE, RuntimeExecutionMode.BATCH);

        ResultDescriptor resultDescriptor =
                new ResultDescriptor(CliClientTestUtils.createTestClient(schema), testConfig);
        TestChangelogResult testChangelogResult = new TestChangelogResult(TypedResult::endOfStream);
        CliTableauResultView view =
                new CliTableauResultView(terminal, resultDescriptor, testChangelogResult);

        view.displayResults();
        view.close();

        assertThat(terminalOutput).hasToString("Empty set" + System.lineSeparator());
        // Job is finished. Don't need to close the job manually.
        assertThat(testChangelogResult.closed).isFalse();
    }

    @Test
    void testFailedBatchResult() {
        final Configuration testConfig = new Configuration();
        testConfig.set(EXECUTION_RESULT_MODE, ResultMode.TABLEAU);
        testConfig.set(RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        ResultDescriptor resultDescriptor =
                new ResultDescriptor(CliClientTestUtils.createTestClient(schema), testConfig);
        TestChangelogResult changelogResult =
                new TestChangelogResult(
                        () -> {
                            throw new SqlExecutionException("query failed");
                        });

        CliTableauResultView view =
                new CliTableauResultView(terminal, resultDescriptor, changelogResult);

        assertThatThrownBy(view::displayResults)
                .satisfies(anyCauseMatches(SqlExecutionException.class, "query failed"));
        view.close();
        assertThat(changelogResult.closed).isTrue();
    }

    @Test
    void testStreamingResult() {
        final Configuration testConfig = new Configuration();
        testConfig.set(EXECUTION_RESULT_MODE, ResultMode.TABLEAU);
        testConfig.set(RUNTIME_MODE, RuntimeExecutionMode.STREAMING);

        ResultDescriptor resultDescriptor =
                new ResultDescriptor(CliClientTestUtils.createTestClient(schema), testConfig);
        TestChangelogResult collectResult =
                new TestChangelogResult(
                        () -> TypedResult.payload(data.subList(0, data.size() / 2)),
                        () -> TypedResult.payload(data.subList(data.size() / 2, data.size())),
                        TypedResult::endOfStream);
        CliTableauResultView view =
                new CliTableauResultView(terminal, resultDescriptor, collectResult);
        view.displayResults();

        view.close();
        // note: the expected result may look irregular because every CJK(Chinese/Japanese/Korean)
        // character's
        // width < 2 in IDE by default, every CJK character usually's width is 2, you can open this
        // source file
        // by vim or just cat the file to check the regular result.
        assertThat(terminalOutput)
                .hasToString(
                        "+----+---------+-------------+----------------------+--------------------------------+----------------+----------------------------+--------------------------------+"
                                + System.lineSeparator()
                                + "| op | boolean |         int |               bigint |                        varchar | decimal(10, 5) |                  timestamp |                         binary |"
                                + System.lineSeparator()
                                + "+----+---------+-------------+----------------------+--------------------------------+----------------+----------------------------+--------------------------------+"
                                + System.lineSeparator()
                                + "| +I |  <NULL> |           1 |                    2 |                            abc |        1.23000 | 2020-03-01 18:39:14.000000 |            x'32333485365d737e' |"
                                + System.lineSeparator()
                                + "| -U |   FALSE |      <NULL> |                    0 |                                |        1.00000 | 2020-03-01 18:39:14.100000 |                  x'649e207983' |"
                                + System.lineSeparator()
                                + "| +U |    TRUE |  2147483647 |               <NULL> |                        abcdefg |    12345.00000 | 2020-03-01 18:39:14.120000 |                    x'92e90102' |"
                                + System.lineSeparator()
                                + "| -D |   FALSE | -2147483648 |  9223372036854775807 |                         <NULL> |    12345.06789 | 2020-03-01 18:39:14.123000 |            x'32333485365d737e' |"
                                + System.lineSeparator()
                                + "| +I |    TRUE |         100 | -9223372036854775808 |                     abcdefg111 |         <NULL> | 2020-03-01 18:39:14.123456 |                    x'6e17fffe' |"
                                + System.lineSeparator()
                                + "| -D |  <NULL> |          -1 |                   -1 | abcdefghijklmnopqrstuvwxyz1... |   -12345.06789 |                     <NULL> |                         <NULL> |"
                                + System.lineSeparator()
                                + "| +I |  <NULL> |          -1 |                   -1 |                   这是一段中文 |   -12345.06789 | 2020-03-04 18:39:14.000000 |              x'fdfeff00010203' |"
                                + System.lineSeparator()
                                + "| -D |  <NULL> |          -1 |                   -1 |  これは日本語をテストするた... |   -12345.06789 | 2020-03-04 18:39:14.000000 |              x'fdfeff00010203' |"
                                + System.lineSeparator()
                                + "+----+---------+-------------+----------------------+--------------------------------+----------------+----------------------------+--------------------------------+"
                                + System.lineSeparator()
                                + "Received a total of 8 rows"
                                + System.lineSeparator());

        // adjust the max column width for printing
        testConfig.set(DISPLAY_MAX_COLUMN_WIDTH, 80);

        collectResult =
                new TestChangelogResult(
                        () -> TypedResult.payload(data.subList(0, data.size() / 2)),
                        () -> TypedResult.payload(data.subList(data.size() / 2, data.size())),
                        TypedResult::endOfStream);
        view = new CliTableauResultView(terminal, resultDescriptor, collectResult);
        view.displayResults();
        assertThat(terminalOutput.toString()).contains("abcdefghijklmnopqrstuvwxyz12345");

        // Job is finished. Don't need to close the job manually.
        assertThat(collectResult.closed).isFalse();
    }

    @Test
    void testEmptyStreamingResult() {
        final Configuration testConfig = new Configuration();
        testConfig.set(EXECUTION_RESULT_MODE, ResultMode.TABLEAU);
        testConfig.set(RUNTIME_MODE, RuntimeExecutionMode.STREAMING);
        ResultDescriptor resultDescriptor =
                new ResultDescriptor(CliClientTestUtils.createTestClient(schema), testConfig);
        TestChangelogResult collectResult = new TestChangelogResult(TypedResult::endOfStream);

        CliTableauResultView view =
                new CliTableauResultView(terminal, resultDescriptor, collectResult);

        view.displayResults();
        view.close();

        assertThat(terminalOutput)
                .hasToString(
                        "+----+---------+-------------+----------------------+--------------------------------+----------------+----------------------------+--------------------------------+"
                                + System.lineSeparator()
                                + "| op | boolean |         int |               bigint |                        varchar | decimal(10, 5) |                  timestamp |                         binary |"
                                + System.lineSeparator()
                                + "+----+---------+-------------+----------------------+--------------------------------+----------------+----------------------------+--------------------------------+"
                                + System.lineSeparator()
                                + "Received a total of 0 row"
                                + System.lineSeparator());
        // Job is finished. Don't need to close the job manually.
        assertThat(collectResult.closed).isFalse();
    }

    @Test
    void testCancelStreamingResult() throws Exception {
        final Configuration testConfig = new Configuration();
        testConfig.set(EXECUTION_RESULT_MODE, ResultMode.TABLEAU);
        testConfig.set(RUNTIME_MODE, RuntimeExecutionMode.STREAMING);

        TestChangelogResult collectResult =
                new TestChangelogResult(
                        () ->
                                TypedResult.payload(
                                        streamingData.subList(0, streamingData.size() / 2)),
                        TypedResult::empty);
        ResultDescriptor resultDescriptor =
                new ResultDescriptor(CliClientTestUtils.createTestClient(schema), testConfig);
        CliTableauResultView view =
                new CliTableauResultView(terminal, resultDescriptor, collectResult);

        // submit result display in another thread
        Future<?> furture = EXECUTOR_RESOURCE.getExecutor().submit(view::displayResults);

        // wait until we processed first result
        CommonTestUtils.waitUntilCondition(() -> collectResult.fetchCount.get() > 1, 50L);

        // send signal to cancel
        terminal.raise(Terminal.Signal.INT);
        furture.get(10, TimeUnit.SECONDS);
        view.close();

        assertThat(terminalOutput)
                .hasToString(
                        "+----+---------+-------------+----------------------+--------------------------------+----------------+----------------------------+--------------------------------+"
                                + System.lineSeparator()
                                + "| op | boolean |         int |               bigint |                        varchar | decimal(10, 5) |                  timestamp |                         binary |"
                                + System.lineSeparator()
                                + "+----+---------+-------------+----------------------+--------------------------------+----------------+----------------------------+--------------------------------+"
                                + System.lineSeparator()
                                + "| +I |  <NULL> |           1 |                    2 |                            abc |        1.23000 | 2020-03-01 18:39:14.000000 |            x'32333485365d737e' |"
                                + System.lineSeparator()
                                + "| -U |   FALSE |      <NULL> |                    0 |                                |        1.00000 | 2020-03-01 18:39:14.100000 |                  x'649e207983' |"
                                + System.lineSeparator()
                                + "| +U |    TRUE |  2147483647 |               <NULL> |                        abcdefg |    12345.00000 | 2020-03-01 18:39:14.120000 |                    x'92e90102' |"
                                + System.lineSeparator()
                                + "| -D |   FALSE | -2147483648 |  9223372036854775807 |                         <NULL> |    12345.06789 | 2020-03-01 18:39:14.123000 |            x'32333485365d737e' |"
                                + System.lineSeparator()
                                + "Query terminated, received a total of 4 rows"
                                + System.lineSeparator());
        // close job manually
        assertThat(collectResult.closed).isTrue();
    }

    @Test
    void testFailedStreamingResult() {
        final Configuration testConfig = new Configuration();
        testConfig.set(EXECUTION_RESULT_MODE, ResultMode.TABLEAU);
        testConfig.set(RUNTIME_MODE, RuntimeExecutionMode.STREAMING);
        ResultDescriptor resultDescriptor =
                new ResultDescriptor(CliClientTestUtils.createTestClient(schema), testConfig);
        TestChangelogResult changelogResult =
                new TestChangelogResult(
                        () -> {
                            throw new SqlExecutionException("query failed");
                        });
        CliTableauResultView view =
                new CliTableauResultView(terminal, resultDescriptor, changelogResult);

        assertThatThrownBy(view::displayResults)
                .satisfies(anyCauseMatches(SqlExecutionException.class, "query failed"));
        view.close();
        assertThat(changelogResult.closed).isTrue();
    }

    private static class TestChangelogResult implements ChangelogResult {

        volatile boolean closed;
        AtomicInteger fetchCount;
        private final Supplier<TypedResult<List<RowData>>>[] results;

        @SafeVarargs
        public TestChangelogResult(Supplier<TypedResult<List<RowData>>>... results) {
            this.results = results;
            this.closed = false;
            this.fetchCount = new AtomicInteger(0);
        }

        @Override
        public TypedResult<List<RowData>> retrieveChanges() {
            TypedResult<List<RowData>> result =
                    results[Math.min(fetchCount.getAndIncrement(), results.length - 1)].get();
            return result;
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
