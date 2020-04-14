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

package org.apache.flink.table.api.internal;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link TableResultImpl}.
 */
public class TableResultImplTest {

	private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
	private final PrintStream originalOut = System.out;

	private TableSchema tableSchema = TableSchema.builder()
			.field("boolean", DataTypes.BOOLEAN())
			.field("int", DataTypes.INT())
			.field("bigint", DataTypes.BIGINT())
			.field("varchar", DataTypes.STRING())
			.field("decimal(10, 5)", DataTypes.DECIMAL(10, 5))
			.field("timestamp", DataTypes.TIMESTAMP(6))
			.build();

	@Before
	public void setUp() {
		System.setOut(new PrintStream(outContent));
	}

	@After
	public void restoreStreams() {
		System.setOut(originalOut);
	}

	@Test
	public void testPrintWithEmptyResult() {
		TableResult tableResult = TableResultImpl.builder()
				.resultKind(ResultKind.SUCCESS_WITH_CONTENT)
				.tableSchema(tableSchema)
				.data(Collections.emptyList())
				.build();

		tableResult.print();
		assertEquals(
				"+---------+-----+--------+---------+----------------+-----------+\n" +
				"| boolean | int | bigint | varchar | decimal(10, 5) | timestamp |\n" +
				"+---------+-----+--------+---------+----------------+-----------+\n" +
				"0 row(s) in result\n",
				outContent.toString());
	}

	@Test
	public void testPrintWithMultipleRows() {
		TableResult tableResult = TableResultImpl.builder()
				.resultKind(ResultKind.SUCCESS_WITH_CONTENT)
				.tableSchema(tableSchema)
				.data(getData())
				.build();

		tableResult.print();
		// note: the expected result may look irregular because every CJK(Chinese/Japanese/Korean) character's
		// width < 2 in IDE by default, every CJK character usually's width is 2, you can open this source file
		// by vim or just cat the file to check the regular result.
		assertEquals(
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
				"8 row(s) in result\n",
				outContent.toString());
	}

	private List<Row> getData() {
		List<Row> data = new ArrayList<>();
		data.add(Row.of(
				null,
				1,
				2,
				"abc",
				BigDecimal.valueOf(1.23),
				Timestamp.valueOf("2020-03-01 18:39:14"))
		);
		data.add(Row.of(
				false,
				null,
				0,
				"",
				BigDecimal.valueOf(1),
				Timestamp.valueOf("2020-03-01 18:39:14.1"))
		);
		data.add(Row.of(
				true,
				Integer.MAX_VALUE,
				null,
				"abcdefg",
				BigDecimal.valueOf(1234567890),
				Timestamp.valueOf("2020-03-01 18:39:14.12"))
		);
		data.add(Row.of(
				false,
				Integer.MIN_VALUE,
				Long.MAX_VALUE,
				null,
				BigDecimal.valueOf(12345.06789),
				Timestamp.valueOf("2020-03-01 18:39:14.123"))
		);
		data.add(Row.of(
				true,
				100,
				Long.MIN_VALUE,
				"abcdefg111",
				null,
				Timestamp.valueOf("2020-03-01 18:39:14.123456"))
		);
		data.add(Row.of(
				null,
				-1,
				-1,
				"abcdefghijklmnopqrstuvwxyz",
				BigDecimal.valueOf(-12345.06789),
				null)
		);
		data.add(Row.of(
				null,
				-1,
				-1,
				"这是一段中文",
				BigDecimal.valueOf(-12345.06789),
				Timestamp.valueOf("2020-03-04 18:39:14"))
		);
		data.add(Row.of(
				null,
				-1,
				-1,
				"これは日本語をテストするための文です",
				BigDecimal.valueOf(-12345.06789),
				Timestamp.valueOf("2020-03-04 18:39:14"))
		);
		return data;
	}

}
