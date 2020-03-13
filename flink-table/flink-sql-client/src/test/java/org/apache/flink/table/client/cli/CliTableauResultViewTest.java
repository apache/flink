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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.client.cli.utils.TerminalUtils;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.types.Row;

import org.jline.terminal.Terminal;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for CliTableauResultView.
 */
public class CliTableauResultViewTest {

	private ByteArrayOutputStream terminalOutput;
	private Terminal terminal;
	private TableSchema schema;
	private List<Row> data;
	private List<Tuple2<Boolean, Row>> streamingData;

	@Before
	public void setUp() {
		terminalOutput = new ByteArrayOutputStream();
		terminal = TerminalUtils.createDummyTerminal(terminalOutput);

		schema = TableSchema.builder()
				.field("boolean", DataTypes.BOOLEAN())
				.field("int", DataTypes.INT())
				.field("bigint", DataTypes.BIGINT())
				.field("varchar", DataTypes.STRING())
				.field("decimal(10, 5)", DataTypes.DECIMAL(10, 5))
				.field("timestamp", DataTypes.TIMESTAMP(6))
				.build();

		data = new ArrayList<>();
		data.add(
				Row.of(
						null,
						1,
						2,
						"abc",
						BigDecimal.valueOf(1.23),
						Timestamp.valueOf("2020-03-01 18:39:14"))
		);
		data.add(
				Row.of(
						false,
						null,
						0,
						"",
						BigDecimal.valueOf(1),
						Timestamp.valueOf("2020-03-01 18:39:14.1"))
		);
		data.add(
				Row.of(
						true,
						Integer.MAX_VALUE,
						null,
						"abcdefg",
						BigDecimal.valueOf(1234567890),
						Timestamp.valueOf("2020-03-01 18:39:14.12"))
		);
		data.add(
				Row.of(
						false,
						Integer.MIN_VALUE,
						Long.MAX_VALUE,
						null,
						BigDecimal.valueOf(12345.06789),
						Timestamp.valueOf("2020-03-01 18:39:14.123"))
		);
		data.add(
				Row.of(
						true,
						100,
						Long.MIN_VALUE,
						"abcdefg111",
						null,
						Timestamp.valueOf("2020-03-01 18:39:14.123456"))
		);
		data.add(
				Row.of(
						null,
						-1,
						-1,
						"abcdefghijklmnopqrstuvwxyz",
						BigDecimal.valueOf(-12345.06789),
						null)
		);

		data.add(
			Row.of(
				null,
				-1,
				-1,
				"这是一段中文",
				BigDecimal.valueOf(-12345.06789),
				Timestamp.valueOf("2020-03-04 18:39:14"))
		);

		data.add(
			Row.of(
				null,
				-1,
				-1,
				"これは日本語をテストするための文です",
				BigDecimal.valueOf(-12345.06789),
				Timestamp.valueOf("2020-03-04 18:39:14"))
		);

		streamingData = new ArrayList<>();
		for (int i = 0; i < data.size(); ++i) {
			streamingData.add(new Tuple2<>(i % 2 == 0, data.get(i)));
		}
	}

	@Test
	public void testBatchResult() {
		ResultDescriptor resultDescriptor = new ResultDescriptor("", schema, true, true);
		Executor mockExecutor = mock(Executor.class);
		CliTableauResultView view = new CliTableauResultView(
				terminal, mockExecutor, "session", resultDescriptor);

		when(mockExecutor.snapshotResult(anyString(), anyString(), anyInt()))
				.thenReturn(TypedResult.payload(1))
				.thenReturn(TypedResult.endOfStream());
		when(mockExecutor.retrieveResultPage(anyString(), anyInt()))
				.thenReturn(data);

		view.displayBatchResults();
		view.close();
		Assert.assertEquals(
				"+---------+-------------+----------------------+--------------------------------+----------------+----------------------------+\n" +
				"| boolean |         int |               bigint |                        varchar | decimal(10, 5) |                  timestamp |\n" +
				"+---------+-------------+----------------------+--------------------------------+----------------+----------------------------+\n" +
				"|  (NULL) |           1 |                    2 |                            abc |           1.23 |      2020-03-01 18:39:14.0 |\n" +
				"|   false |      (NULL) |                    0 |                                |              1 |      2020-03-01 18:39:14.1 |\n" +
				"|    true |  2147483647 |               (NULL) |                        abcdefg |     1234567890 |     2020-03-01 18:39:14.12 |\n" +
				"|   false | -2147483648 |  9223372036854775807 |                         (NULL) |    12345.06789 |    2020-03-01 18:39:14.123 |\n" +
				"|    true |         100 | -9223372036854775808 |                     abcdefg111 |         (NULL) | 2020-03-01 18:39:14.123456 |\n" +
				"|  (NULL) |          -1 |                   -1 |     abcdefghijklmnopqrstuvwxyz |   -12345.06789 |                     (NULL) |\n" +
				"|  (NULL) |          -1 |                   -1 |                   这是一段中文 |   -12345.06789 |      2020-03-04 18:39:14.0 |\n" +
				"|  (NULL) |          -1 |                   -1 |  これは日本語をテストするた... |   -12345.06789 |      2020-03-04 18:39:14.0 |\n" +
				"+---------+-------------+----------------------+--------------------------------+----------------+----------------------------+\n" +
				"8 row in set\n",
				terminalOutput.toString());
		verify(mockExecutor, times(0)).cancelQuery(anyString(), anyString());
	}

	@Test
	public void testCancelBatchResult() throws InterruptedException, ExecutionException, TimeoutException {
		ResultDescriptor resultDescriptor = new ResultDescriptor("", schema, true, true);
		Executor mockExecutor = mock(Executor.class);
		CliTableauResultView view = new CliTableauResultView(
				terminal, mockExecutor, "session", resultDescriptor);

		when(mockExecutor.snapshotResult(anyString(), anyString(), anyInt()))
				.thenReturn(TypedResult.empty());

		// submit result display in another thread
		ExecutorService executorService = Executors.newSingleThreadExecutor();
		Future<?> furture = executorService.submit(view::displayBatchResults);

		// wait until we trying to get batch result
		verify(mockExecutor, timeout(5000).atLeast(1)).snapshotResult(anyString(), anyString(), anyInt());

		// send signal to cancel
		terminal.raise(Terminal.Signal.INT);
		furture.get(5, TimeUnit.SECONDS);

		Assert.assertEquals("Query terminated\n", terminalOutput.toString());
		// didn't have a chance to read page
		verify(mockExecutor, times(0)).retrieveResultPage(anyString(), anyInt());
		// tried to cancel query
		verify(mockExecutor, times(1)).cancelQuery(anyString(), anyString());

		view.close();
	}

	@Test
	public void testEmptyBatchResult() {
		ResultDescriptor resultDescriptor = new ResultDescriptor("", schema, true, true);
		Executor mockExecutor = mock(Executor.class);
		CliTableauResultView view = new CliTableauResultView(
				terminal, mockExecutor, "session", resultDescriptor);

		when(mockExecutor.snapshotResult(anyString(), anyString(), anyInt()))
				.thenReturn(TypedResult.payload(1))
				.thenReturn(TypedResult.endOfStream());
		when(mockExecutor.retrieveResultPage(anyString(), anyInt()))
				.thenReturn(Collections.emptyList());

		view.displayBatchResults();
		view.close();

		Assert.assertEquals(
				"+---------+-----+--------+---------+----------------+-----------+\n" +
				"| boolean | int | bigint | varchar | decimal(10, 5) | timestamp |\n" +
				"+---------+-----+--------+---------+----------------+-----------+\n" +
				"0 row in set\n",
				terminalOutput.toString());
		verify(mockExecutor, times(0)).cancelQuery(anyString(), anyString());
	}

	@Test
	public void testFailedBatchResult() {
		ResultDescriptor resultDescriptor = new ResultDescriptor("", schema, true, true);
		Executor mockExecutor = mock(Executor.class);
		CliTableauResultView view = new CliTableauResultView(
				terminal, mockExecutor, "session", resultDescriptor);

		when(mockExecutor.snapshotResult(anyString(), anyString(), anyInt()))
				.thenReturn(TypedResult.payload(1))
				.thenReturn(TypedResult.endOfStream());
		when(mockExecutor.retrieveResultPage(anyString(), anyInt()))
				.thenThrow(new SqlExecutionException("query failed"));

		try {
			view.displayBatchResults();
			Assert.fail("Shouldn't get here");
		} catch (SqlExecutionException e) {
			Assert.assertEquals("query failed", e.getMessage());
		}
		view.close();

		verify(mockExecutor, times(1)).cancelQuery(anyString(), anyString());
	}

	@Test
	public void testStreamingResult() {
		ResultDescriptor resultDescriptor = new ResultDescriptor("", schema, true, true);
		Executor mockExecutor = mock(Executor.class);
		CliTableauResultView view = new CliTableauResultView(
				terminal, mockExecutor, "session", resultDescriptor);

		when(mockExecutor.retrieveResultChanges(anyString(), anyString()))
				.thenReturn(TypedResult.payload(streamingData.subList(0, streamingData.size() / 2)))
				.thenReturn(TypedResult.payload(streamingData.subList(streamingData.size() / 2, streamingData.size())))
				.thenReturn(TypedResult.endOfStream());

		view.displayStreamResults();
		view.close();
		// note: the expected result may look irregular because every CJK(Chinese/Japanese/Korean) character's
		// width < 2 in IDE by default, every CJK character usually's width is 2, you can open this source file
		// by vim or just cat the file to check the regular result.
		Assert.assertEquals(
				"+-----+---------+-------------+----------------------+----------------------+----------------+----------------------------+\n" +
				"| +/- | boolean |         int |               bigint |              varchar | decimal(10, 5) |                  timestamp |\n" +
				"+-----+---------+-------------+----------------------+----------------------+----------------+----------------------------+\n" +
				"|   + |  (NULL) |           1 |                    2 |                  abc |           1.23 |      2020-03-01 18:39:14.0 |\n" +
				"|   - |   false |      (NULL) |                    0 |                      |              1 |      2020-03-01 18:39:14.1 |\n" +
				"|   + |    true |  2147483647 |               (NULL) |              abcdefg |     1234567890 |     2020-03-01 18:39:14.12 |\n" +
				"|   - |   false | -2147483648 |  9223372036854775807 |               (NULL) |    12345.06789 |    2020-03-01 18:39:14.123 |\n" +
				"|   + |    true |         100 | -9223372036854775808 |           abcdefg111 |         (NULL) | 2020-03-01 18:39:14.123456 |\n" +
				"|   - |  (NULL) |          -1 |                   -1 |  abcdefghijklmnop... |   -12345.06789 |                     (NULL) |\n" +
				"|   + |  (NULL) |          -1 |                   -1 |         这是一段中文 |   -12345.06789 |      2020-03-04 18:39:14.0 |\n" +
				"|   - |  (NULL) |          -1 |                   -1 |  これは日本語をテ... |   -12345.06789 |      2020-03-04 18:39:14.0 |\n" +
				"+-----+---------+-------------+----------------------+----------------------+----------------+----------------------------+\n" +
				"Received a total of 8 rows\n",
				terminalOutput.toString());
		verify(mockExecutor, times(0)).cancelQuery(anyString(), anyString());
	}

	@Test
	public void testEmptyStreamingResult() {
		ResultDescriptor resultDescriptor = new ResultDescriptor("", schema, true, true);
		Executor mockExecutor = mock(Executor.class);
		CliTableauResultView view = new CliTableauResultView(
				terminal, mockExecutor, "session", resultDescriptor);

		when(mockExecutor.retrieveResultChanges(anyString(), anyString()))
				.thenReturn(TypedResult.endOfStream());

		view.displayStreamResults();
		view.close();

		Assert.assertEquals(
				"+-----+---------+-------------+----------------------+----------------------+----------------+----------------------------+\n" +
				"| +/- | boolean |         int |               bigint |              varchar | decimal(10, 5) |                  timestamp |\n" +
				"+-----+---------+-------------+----------------------+----------------------+----------------+----------------------------+\n" +
				"Received a total of 0 rows\n",
				terminalOutput.toString());
		verify(mockExecutor, times(0)).cancelQuery(anyString(), anyString());
	}

	@Test
	public void testCancelStreamingResult() throws InterruptedException, ExecutionException, TimeoutException {
		ResultDescriptor resultDescriptor = new ResultDescriptor("", schema, true, true);
		Executor mockExecutor = mock(Executor.class);
		CliTableauResultView view = new CliTableauResultView(
				terminal, mockExecutor, "session", resultDescriptor);

		when(mockExecutor.retrieveResultChanges(anyString(), anyString()))
				.thenReturn(TypedResult.payload(streamingData.subList(0, streamingData.size() / 2)))
				.thenReturn(TypedResult.empty());

		// submit result display in another thread
		ExecutorService executorService = Executors.newSingleThreadExecutor();
		Future<?> furture = executorService.submit(view::displayStreamResults);

		// wait until we processed first result
		verify(mockExecutor, timeout(5000).atLeast(2)).retrieveResultChanges(anyString(), anyString());

		// send signal to cancel
		terminal.raise(Terminal.Signal.INT);
		furture.get(5, TimeUnit.SECONDS);
		view.close();

		Assert.assertEquals(
				"+-----+---------+-------------+----------------------+----------------------+----------------+----------------------------+\n" +
				"| +/- | boolean |         int |               bigint |              varchar | decimal(10, 5) |                  timestamp |\n" +
				"+-----+---------+-------------+----------------------+----------------------+----------------+----------------------------+\n" +
				"|   + |  (NULL) |           1 |                    2 |                  abc |           1.23 |      2020-03-01 18:39:14.0 |\n" +
				"|   - |   false |      (NULL) |                    0 |                      |              1 |      2020-03-01 18:39:14.1 |\n" +
				"|   + |    true |  2147483647 |               (NULL) |              abcdefg |     1234567890 |     2020-03-01 18:39:14.12 |\n" +
				"|   - |   false | -2147483648 |  9223372036854775807 |               (NULL) |    12345.06789 |    2020-03-01 18:39:14.123 |\n" +
				"Query terminated, received a total of 4 rows\n",
				terminalOutput.toString());

		verify(mockExecutor, times(1)).cancelQuery(anyString(), anyString());
	}

	@Test
	public void testFailedStreamingResult() {
		ResultDescriptor resultDescriptor = new ResultDescriptor("", schema, true, true);
		Executor mockExecutor = mock(Executor.class);
		CliTableauResultView view = new CliTableauResultView(
				terminal, mockExecutor, "session", resultDescriptor);

		when(mockExecutor.retrieveResultChanges(anyString(), anyString()))
				.thenReturn(TypedResult.payload(streamingData.subList(0, streamingData.size() / 2)))
				.thenThrow(new SqlExecutionException("query failed"));

		try {
			view.displayStreamResults();
			Assert.fail("Shouldn't get here");
		} catch (SqlExecutionException e) {
			Assert.assertEquals("query failed", e.getMessage());
		}
		view.close();

		Assert.assertEquals(
				"+-----+---------+-------------+----------------------+----------------------+----------------+----------------------------+\n" +
				"| +/- | boolean |         int |               bigint |              varchar | decimal(10, 5) |                  timestamp |\n" +
				"+-----+---------+-------------+----------------------+----------------------+----------------+----------------------------+\n" +
				"|   + |  (NULL) |           1 |                    2 |                  abc |           1.23 |      2020-03-01 18:39:14.0 |\n" +
				"|   - |   false |      (NULL) |                    0 |                      |              1 |      2020-03-01 18:39:14.1 |\n" +
				"|   + |    true |  2147483647 |               (NULL) |              abcdefg |     1234567890 |     2020-03-01 18:39:14.12 |\n" +
				"|   - |   false | -2147483648 |  9223372036854775807 |               (NULL) |    12345.06789 |    2020-03-01 18:39:14.123 |\n",
				terminalOutput.toString());
		verify(mockExecutor, times(1)).cancelQuery(anyString(), anyString());
	}
}
