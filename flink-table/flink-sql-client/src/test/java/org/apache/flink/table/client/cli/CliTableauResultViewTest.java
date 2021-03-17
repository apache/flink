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

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.client.cli.utils.TerminalUtils;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.jline.terminal.Terminal;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/** Tests for CliTableauResultView. */
public class CliTableauResultViewTest {

    private ByteArrayOutputStream terminalOutput;
    private Terminal terminal;
    private ResolvedSchema schema;
    private List<Row> data;
    private List<Row> streamingData;

    @Before
    public void setUp() {
        terminalOutput = new ByteArrayOutputStream();
        terminal = TerminalUtils.createDummyTerminal(terminalOutput);

        schema =
                ResolvedSchema.of(
                        Column.physical("boolean", DataTypes.BOOLEAN()),
                        Column.physical("int", DataTypes.INT()),
                        Column.physical("bigint", DataTypes.BIGINT()),
                        Column.physical("varchar", DataTypes.STRING()),
                        Column.physical("decimal(10, 5)", DataTypes.DECIMAL(10, 5)),
                        Column.physical("timestamp", DataTypes.TIMESTAMP(6)));

        data = new ArrayList<>();
        data.add(
                Row.ofKind(
                        RowKind.INSERT,
                        null,
                        1,
                        2,
                        "abc",
                        BigDecimal.valueOf(1.23),
                        Timestamp.valueOf("2020-03-01 18:39:14")));
        data.add(
                Row.ofKind(
                        RowKind.UPDATE_BEFORE,
                        false,
                        null,
                        0,
                        "",
                        BigDecimal.valueOf(1),
                        Timestamp.valueOf("2020-03-01 18:39:14.1")));
        data.add(
                Row.ofKind(
                        RowKind.UPDATE_AFTER,
                        true,
                        Integer.MAX_VALUE,
                        null,
                        "abcdefg",
                        BigDecimal.valueOf(1234567890),
                        Timestamp.valueOf("2020-03-01 18:39:14.12")));
        data.add(
                Row.ofKind(
                        RowKind.DELETE,
                        false,
                        Integer.MIN_VALUE,
                        Long.MAX_VALUE,
                        null,
                        BigDecimal.valueOf(12345.06789),
                        Timestamp.valueOf("2020-03-01 18:39:14.123")));
        data.add(
                Row.ofKind(
                        RowKind.INSERT,
                        true,
                        100,
                        Long.MIN_VALUE,
                        "abcdefg111",
                        null,
                        Timestamp.valueOf("2020-03-01 18:39:14.123456")));
        data.add(
                Row.ofKind(
                        RowKind.DELETE,
                        null,
                        -1,
                        -1,
                        "abcdefghijklmnopqrstuvwxyz",
                        BigDecimal.valueOf(-12345.06789),
                        null));

        data.add(
                Row.ofKind(
                        RowKind.INSERT,
                        null,
                        -1,
                        -1,
                        "这是一段中文",
                        BigDecimal.valueOf(-12345.06789),
                        Timestamp.valueOf("2020-03-04 18:39:14")));

        data.add(
                Row.ofKind(
                        RowKind.DELETE,
                        null,
                        -1,
                        -1,
                        "これは日本語をテストするための文です",
                        BigDecimal.valueOf(-12345.06789),
                        Timestamp.valueOf("2020-03-04 18:39:14")));

        streamingData = new ArrayList<>();
        for (Row datum : data) {
            Row row = Row.copy(datum);
            streamingData.add(row);
        }
    }

    @Test
    public void testBatchResult() {
        ResultDescriptor resultDescriptor = new ResultDescriptor("", schema, true, true, false);

        TestingExecutor mockExecutor =
                new TestingExecutorBuilder()
                        .setResultChangesSupplier(
                                () -> TypedResult.payload(data.subList(0, data.size() / 2)),
                                () ->
                                        TypedResult.payload(
                                                data.subList(data.size() / 2, data.size())),
                                TypedResult::endOfStream)
                        .build();

        CliTableauResultView view =
                new CliTableauResultView(terminal, mockExecutor, "session", resultDescriptor);

        view.displayResults();
        view.close();
        Assert.assertEquals(
                "+---------+-------------+----------------------+----------------------+----------------+----------------------------+"
                        + System.lineSeparator()
                        + "| boolean |         int |               bigint |              varchar | decimal(10, 5) |                  timestamp |"
                        + System.lineSeparator()
                        + "+---------+-------------+----------------------+----------------------+----------------+----------------------------+"
                        + System.lineSeparator()
                        + "|  (NULL) |           1 |                    2 |                  abc |           1.23 |      2020-03-01 18:39:14.0 |"
                        + System.lineSeparator()
                        + "|   false |      (NULL) |                    0 |                      |              1 |      2020-03-01 18:39:14.1 |"
                        + System.lineSeparator()
                        + "|    true |  2147483647 |               (NULL) |              abcdefg |     1234567890 |     2020-03-01 18:39:14.12 |"
                        + System.lineSeparator()
                        + "|   false | -2147483648 |  9223372036854775807 |               (NULL) |    12345.06789 |    2020-03-01 18:39:14.123 |"
                        + System.lineSeparator()
                        + "|    true |         100 | -9223372036854775808 |           abcdefg111 |         (NULL) | 2020-03-01 18:39:14.123456 |"
                        + System.lineSeparator()
                        + "|  (NULL) |          -1 |                   -1 | abcdefghijklmnopq... |   -12345.06789 |                     (NULL) |"
                        + System.lineSeparator()
                        + "|  (NULL) |          -1 |                   -1 |         这是一段中文 |   -12345.06789 |      2020-03-04 18:39:14.0 |"
                        + System.lineSeparator()
                        + "|  (NULL) |          -1 |                   -1 |  これは日本語をテ... |   -12345.06789 |      2020-03-04 18:39:14.0 |"
                        + System.lineSeparator()
                        + "+---------+-------------+----------------------+----------------------+----------------+----------------------------+"
                        + System.lineSeparator()
                        + "Received a total of 8 rows"
                        + System.lineSeparator(),
                terminalOutput.toString());
        assertThat(mockExecutor.getNumCancelCalls(), is(0));
    }

    @Test
    public void testCancelBatchResult() throws Exception {
        ResultDescriptor resultDescriptor = new ResultDescriptor("", schema, true, true, false);

        TestingExecutor mockExecutor =
                new TestingExecutorBuilder()
                        .setResultChangesSupplier(
                                () -> TypedResult.payload(data.subList(0, data.size() / 2)),
                                TypedResult::empty)
                        .build();

        CliTableauResultView view =
                new CliTableauResultView(terminal, mockExecutor, "session", resultDescriptor);

        // submit result display in another thread
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<?> furture = executorService.submit(view::displayResults);

        // wait until we trying to get batch result
        CommonTestUtils.waitUntilCondition(
                () -> mockExecutor.getNumRetrieveResultChancesCalls() > 1,
                Deadline.now().plus(Duration.ofSeconds(5)),
                50L);

        // send signal to cancel
        terminal.raise(Terminal.Signal.INT);
        furture.get(5, TimeUnit.SECONDS);

        Assert.assertEquals(
                "+---------+-------------+----------------------+----------------------+----------------+----------------------------+"
                        + System.lineSeparator()
                        + "| boolean |         int |               bigint |              varchar | decimal(10, 5) |                  timestamp |"
                        + System.lineSeparator()
                        + "+---------+-------------+----------------------+----------------------+----------------+----------------------------+"
                        + System.lineSeparator()
                        + "|  (NULL) |           1 |                    2 |                  abc |           1.23 |      2020-03-01 18:39:14.0 |"
                        + System.lineSeparator()
                        + "|   false |      (NULL) |                    0 |                      |              1 |      2020-03-01 18:39:14.1 |"
                        + System.lineSeparator()
                        + "|    true |  2147483647 |               (NULL) |              abcdefg |     1234567890 |     2020-03-01 18:39:14.12 |"
                        + System.lineSeparator()
                        + "|   false | -2147483648 |  9223372036854775807 |               (NULL) |    12345.06789 |    2020-03-01 18:39:14.123 |"
                        + System.lineSeparator()
                        + "Query terminated, received a total of 4 rows"
                        + System.lineSeparator(),
                terminalOutput.toString());

        // didn't have a chance to read page
        assertThat(mockExecutor.getNumRetrieveResultPageCalls(), is(0));
        // tried to cancel query
        assertThat(mockExecutor.getNumCancelCalls(), is(1));

        view.close();
    }

    @Test
    public void testEmptyBatchResult() {
        ResultDescriptor resultDescriptor = new ResultDescriptor("", schema, true, true, false);
        TestingExecutor mockExecutor =
                new TestingExecutorBuilder()
                        .setResultChangesSupplier(TypedResult::endOfStream)
                        .setResultPageSupplier(
                                () -> {
                                    throw new SqlExecutionException("query failed");
                                })
                        .build();

        CliTableauResultView view =
                new CliTableauResultView(terminal, mockExecutor, "session", resultDescriptor);

        view.displayResults();
        view.close();

        Assert.assertEquals(
                "+---------+-------------+----------------------+----------------------+----------------+----------------------------+"
                        + System.lineSeparator()
                        + "| boolean |         int |               bigint |              varchar | decimal(10, 5) |                  timestamp |"
                        + System.lineSeparator()
                        + "+---------+-------------+----------------------+----------------------+----------------+----------------------------+"
                        + System.lineSeparator()
                        + "Received a total of 0 row"
                        + System.lineSeparator(),
                terminalOutput.toString());
        assertThat(mockExecutor.getNumCancelCalls(), is(0));
    }

    @Test
    public void testFailedBatchResult() {
        ResultDescriptor resultDescriptor = new ResultDescriptor("", schema, true, true, false);

        TestingExecutor mockExecutor =
                new TestingExecutorBuilder()
                        .setResultChangesSupplier(
                                () -> {
                                    throw new SqlExecutionException("query failed");
                                },
                                TypedResult::endOfStream)
                        .build();

        CliTableauResultView view =
                new CliTableauResultView(terminal, mockExecutor, "session", resultDescriptor);

        try {
            view.displayResults();
            Assert.fail("Shouldn't get here");
        } catch (SqlExecutionException e) {
            Assert.assertEquals("query failed", e.getMessage());
        }
        view.close();

        assertThat(mockExecutor.getNumCancelCalls(), is(1));
    }

    @Test
    public void testStreamingResult() {
        ResultDescriptor resultDescriptor = new ResultDescriptor("", schema, true, true, true);

        TestingExecutor mockExecutor =
                new TestingExecutorBuilder()
                        .setResultChangesSupplier(
                                () ->
                                        TypedResult.payload(
                                                streamingData.subList(0, streamingData.size() / 2)),
                                () ->
                                        TypedResult.payload(
                                                streamingData.subList(
                                                        streamingData.size() / 2,
                                                        streamingData.size())),
                                TypedResult::endOfStream)
                        .build();

        CliTableauResultView view =
                new CliTableauResultView(terminal, mockExecutor, "session", resultDescriptor);

        view.displayResults();
        view.close();
        // note: the expected result may look irregular because every CJK(Chinese/Japanese/Korean)
        // character's
        // width < 2 in IDE by default, every CJK character usually's width is 2, you can open this
        // source file
        // by vim or just cat the file to check the regular result.
        Assert.assertEquals(
                "+----+---------+-------------+----------------------+----------------------+----------------+----------------------------+"
                        + System.lineSeparator()
                        + "| op | boolean |         int |               bigint |              varchar | decimal(10, 5) |                  timestamp |"
                        + System.lineSeparator()
                        + "+----+---------+-------------+----------------------+----------------------+----------------+----------------------------+"
                        + System.lineSeparator()
                        + "| +I |  (NULL) |           1 |                    2 |                  abc |           1.23 |      2020-03-01 18:39:14.0 |"
                        + System.lineSeparator()
                        + "| -U |   false |      (NULL) |                    0 |                      |              1 |      2020-03-01 18:39:14.1 |"
                        + System.lineSeparator()
                        + "| +U |    true |  2147483647 |               (NULL) |              abcdefg |     1234567890 |     2020-03-01 18:39:14.12 |"
                        + System.lineSeparator()
                        + "| -D |   false | -2147483648 |  9223372036854775807 |               (NULL) |    12345.06789 |    2020-03-01 18:39:14.123 |"
                        + System.lineSeparator()
                        + "| +I |    true |         100 | -9223372036854775808 |           abcdefg111 |         (NULL) | 2020-03-01 18:39:14.123456 |"
                        + System.lineSeparator()
                        + "| -D |  (NULL) |          -1 |                   -1 | abcdefghijklmnopq... |   -12345.06789 |                     (NULL) |"
                        + System.lineSeparator()
                        + "| +I |  (NULL) |          -1 |                   -1 |         这是一段中文 |   -12345.06789 |      2020-03-04 18:39:14.0 |"
                        + System.lineSeparator()
                        + "| -D |  (NULL) |          -1 |                   -1 |  これは日本語をテ... |   -12345.06789 |      2020-03-04 18:39:14.0 |"
                        + System.lineSeparator()
                        + "+----+---------+-------------+----------------------+----------------------+----------------+----------------------------+"
                        + System.lineSeparator()
                        + "Received a total of 8 rows"
                        + System.lineSeparator(),
                terminalOutput.toString());
        assertThat(mockExecutor.getNumCancelCalls(), is(0));
    }

    @Test
    public void testEmptyStreamingResult() {
        ResultDescriptor resultDescriptor = new ResultDescriptor("", schema, true, true, true);

        TestingExecutor mockExecutor =
                new TestingExecutorBuilder()
                        .setResultChangesSupplier(TypedResult::endOfStream)
                        .build();

        CliTableauResultView view =
                new CliTableauResultView(terminal, mockExecutor, "session", resultDescriptor);

        view.displayResults();
        view.close();

        Assert.assertEquals(
                "+----+---------+-------------+----------------------+----------------------+----------------+----------------------------+"
                        + System.lineSeparator()
                        + "| op | boolean |         int |               bigint |              varchar | decimal(10, 5) |                  timestamp |"
                        + System.lineSeparator()
                        + "+----+---------+-------------+----------------------+----------------------+----------------+----------------------------+"
                        + System.lineSeparator()
                        + "Received a total of 0 row"
                        + System.lineSeparator(),
                terminalOutput.toString());
        assertThat(mockExecutor.getNumCancelCalls(), is(0));
    }

    @Test
    public void testCancelStreamingResult() throws Exception {
        ResultDescriptor resultDescriptor = new ResultDescriptor("", schema, true, true, true);

        TestingExecutor mockExecutor =
                new TestingExecutorBuilder()
                        .setResultChangesSupplier(
                                () ->
                                        TypedResult.payload(
                                                streamingData.subList(0, streamingData.size() / 2)),
                                TypedResult::empty)
                        .build();

        CliTableauResultView view =
                new CliTableauResultView(terminal, mockExecutor, "session", resultDescriptor);

        // submit result display in another thread
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<?> furture = executorService.submit(view::displayResults);

        // wait until we processed first result
        CommonTestUtils.waitUntilCondition(
                () -> mockExecutor.getNumRetrieveResultChancesCalls() > 1,
                Deadline.now().plus(Duration.ofSeconds(5)),
                50L);

        // send signal to cancel
        terminal.raise(Terminal.Signal.INT);
        furture.get(5, TimeUnit.SECONDS);
        view.close();

        Assert.assertEquals(
                "+----+---------+-------------+----------------------+----------------------+----------------+----------------------------+"
                        + System.lineSeparator()
                        + "| op | boolean |         int |               bigint |              varchar | decimal(10, 5) |                  timestamp |"
                        + System.lineSeparator()
                        + "+----+---------+-------------+----------------------+----------------------+----------------+----------------------------+"
                        + System.lineSeparator()
                        + "| +I |  (NULL) |           1 |                    2 |                  abc |           1.23 |      2020-03-01 18:39:14.0 |"
                        + System.lineSeparator()
                        + "| -U |   false |      (NULL) |                    0 |                      |              1 |      2020-03-01 18:39:14.1 |"
                        + System.lineSeparator()
                        + "| +U |    true |  2147483647 |               (NULL) |              abcdefg |     1234567890 |     2020-03-01 18:39:14.12 |"
                        + System.lineSeparator()
                        + "| -D |   false | -2147483648 |  9223372036854775807 |               (NULL) |    12345.06789 |    2020-03-01 18:39:14.123 |"
                        + System.lineSeparator()
                        + "Query terminated, received a total of 4 rows"
                        + System.lineSeparator(),
                terminalOutput.toString());

        assertThat(mockExecutor.getNumCancelCalls(), is(1));
    }

    @Test
    public void testFailedStreamingResult() {
        ResultDescriptor resultDescriptor = new ResultDescriptor("", schema, true, true, true);

        TestingExecutor mockExecutor =
                new TestingExecutorBuilder()
                        .setResultChangesSupplier(
                                () ->
                                        TypedResult.payload(
                                                streamingData.subList(0, streamingData.size() / 2)),
                                () -> {
                                    throw new SqlExecutionException("query failed");
                                })
                        .build();

        CliTableauResultView view =
                new CliTableauResultView(terminal, mockExecutor, "session", resultDescriptor);

        try {
            view.displayResults();
            Assert.fail("Shouldn't get here");
        } catch (SqlExecutionException e) {
            Assert.assertEquals("query failed", e.getMessage());
        }
        view.close();

        Assert.assertEquals(
                "+----+---------+-------------+----------------------+----------------------+----------------+----------------------------+"
                        + System.lineSeparator()
                        + "| op | boolean |         int |               bigint |              varchar | decimal(10, 5) |                  timestamp |"
                        + System.lineSeparator()
                        + "+----+---------+-------------+----------------------+----------------------+----------------+----------------------------+"
                        + System.lineSeparator()
                        + "| +I |  (NULL) |           1 |                    2 |                  abc |           1.23 |      2020-03-01 18:39:14.0 |"
                        + System.lineSeparator()
                        + "| -U |   false |      (NULL) |                    0 |                      |              1 |      2020-03-01 18:39:14.1 |"
                        + System.lineSeparator()
                        + "| +U |    true |  2147483647 |               (NULL) |              abcdefg |     1234567890 |     2020-03-01 18:39:14.12 |"
                        + System.lineSeparator()
                        + "| -D |   false | -2147483648 |  9223372036854775807 |               (NULL) |    12345.06789 |    2020-03-01 18:39:14.123 |"
                        + System.lineSeparator(),
                terminalOutput.toString());
        assertThat(mockExecutor.getNumCancelCalls(), is(1));
    }
}
