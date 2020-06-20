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

package org.apache.flink.table.utils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link PrintUtils}.
 */
public class PrintUtilsTest {
	private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();

	@Test
	public void testArrayToString() {
		Row row = new Row(4);
		row.setField(0, new int[] { 1, 2 });
		row.setField(1, new Integer[] { 3, 4 });
		row.setField(2, new Object[] { new int[] { 5, 6 }, new int[] { 7, 8 } });
		row.setField(3, new Integer[][] { new Integer[] { 9, 10 }, new Integer[] { 11, 12 } });
		assertEquals("[[1, 2], [3, 4], [[5, 6], [7, 8]], [[9, 10], [11, 12]]]",
				Arrays.toString(PrintUtils.rowToString(row)));
	}

	@Test
	public void testCharFullWidth() {
		char[] chars = new char[] { 'A', 'a', ',', '中', '，', 'こ' };
		boolean[] expected = new boolean[] { false, false, false, true, true, true };

		for (int i = 0; i < chars.length; i++) {
			assertEquals(expected[i], PrintUtils.isFullWidth(Character.codePointAt(chars, i)));
		}
	}

	@Test
	public void testStringDisplayWidth() {
		List<String> data = Arrays.asList(
				"abcdefg,12345,ABC",
				"to be or not to be that's a question.",
				"这是一段中文",
				"これは日本語をテストするための文です");
		int[] expected = new int[] { 17, 37, 12, 36 };

		for (int i = 0; i < data.size(); i++) {
			assertEquals(expected[i], PrintUtils.getStringDisplayWidth(data.get(i)));
		}
	}

	@Test
	public void testPrintWithEmptyResult() {
		PrintUtils.printAsTableauForm(
				getSchema(),
				Collections.<Row>emptyList().iterator(),
				new PrintWriter(outContent));

		assertEquals(
				"+---------+-----+--------+---------+----------------+-----------+" + System.lineSeparator() +
				"| boolean | int | bigint | varchar | decimal(10, 5) | timestamp |" + System.lineSeparator() +
				"+---------+-----+--------+---------+----------------+-----------+" + System.lineSeparator() +
				"0 row in set" + System.lineSeparator(),
				outContent.toString());
	}

	@Test
	public void testPrintWithEmptyResultAndRowKind() {
		PrintUtils.printAsTableauForm(
				getSchema(),
				Collections.<Row>emptyList().iterator(),
				new PrintWriter(outContent),
				PrintUtils.MAX_COLUMN_WIDTH,
				"",
				true, // derive column width by type
				true);

		assertEquals(
				"+----+---------+-------------+----------------------+--------------------------------+----------------+----------------------------+" + System.lineSeparator() +
				"| op | boolean |         int |               bigint |                        varchar | decimal(10, 5) |                  timestamp |" + System.lineSeparator() +
				"+----+---------+-------------+----------------------+--------------------------------+----------------+----------------------------+" + System.lineSeparator() +
				"0 row in set" + System.lineSeparator(),
				outContent.toString());
	}

	@Test
	public void testPrintWithEmptyResultAndDeriveColumnWidthByContent() {
		PrintUtils.printAsTableauForm(
				getSchema(),
				Collections.<Row>emptyList().iterator(),
				new PrintWriter(outContent),
				PrintUtils.MAX_COLUMN_WIDTH,
				"",
				false, // derive column width by content
				false);

		assertEquals(
				"+---------+-----+--------+---------+----------------+-----------+" + System.lineSeparator() +
				"| boolean | int | bigint | varchar | decimal(10, 5) | timestamp |" + System.lineSeparator() +
				"+---------+-----+--------+---------+----------------+-----------+" + System.lineSeparator() +
				"0 row in set" + System.lineSeparator(),
				outContent.toString());
	}

	@Test
	public void testPrintWithMultipleRows() {
		PrintUtils.printAsTableauForm(
				getSchema(),
				getData().iterator(),
				new PrintWriter(outContent));

		// note: the expected result may look irregular because every CJK(Chinese/Japanese/Korean) character's
		// width < 2 in IDE by default, every CJK character usually's width is 2, you can open this source file
		// by vim or just cat the file to check the regular result.
		// The last row of `varchar` value will pad with two ' ' before the column.
		// Because the length of `これは日本語をテストするた` plus the length of `...` is 29,
		// no more Japanese character can be added to the line.
		assertEquals(
				"+---------+-------------+----------------------+--------------------------------+----------------+----------------------------+" + System.lineSeparator() +
				"| boolean |         int |               bigint |                        varchar | decimal(10, 5) |                  timestamp |" + System.lineSeparator() +
				"+---------+-------------+----------------------+--------------------------------+----------------+----------------------------+" + System.lineSeparator() +
				"|  (NULL) |           1 |                    2 |                            abc |           1.23 |      2020-03-01 18:39:14.0 |" + System.lineSeparator() +
				"|   false |      (NULL) |                    0 |                                |              1 |      2020-03-01 18:39:14.1 |" + System.lineSeparator() +
				"|    true |  2147483647 |               (NULL) |                        abcdefg |     1234567890 |     2020-03-01 18:39:14.12 |" + System.lineSeparator() +
				"|   false | -2147483648 |  9223372036854775807 |                         (NULL) |    12345.06789 |    2020-03-01 18:39:14.123 |" + System.lineSeparator() +
				"|    true |         100 | -9223372036854775808 |                     abcdefg111 |         (NULL) | 2020-03-01 18:39:14.123456 |" + System.lineSeparator() +
				"|  (NULL) |          -1 |                   -1 | abcdefghijklmnopqrstuvwxyza... |   -12345.06789 |                     (NULL) |" + System.lineSeparator() +
				"|  (NULL) |          -1 |                   -1 |                   这是一段中文 |   -12345.06789 |      2020-03-04 18:39:14.0 |" + System.lineSeparator() +
				"|  (NULL) |          -1 |                   -1 |  これは日本語をテストするた... |   -12345.06789 |      2020-03-04 18:39:14.0 |" + System.lineSeparator() +
				"+---------+-------------+----------------------+--------------------------------+----------------+----------------------------+" + System.lineSeparator() +
				"8 rows in set" + System.lineSeparator(),
				outContent.toString());
	}

	@Test
	public void testPrintWithMultipleRowsAndRowKind() {
		PrintUtils.printAsTableauForm(
				getSchema(),
				getData().iterator(),
				new PrintWriter(outContent),
				PrintUtils.MAX_COLUMN_WIDTH,
				"",
				true, // derive column width by type
				true);

		// note: the expected result may look irregular because every CJK(Chinese/Japanese/Korean) character's
		// width < 2 in IDE by default, every CJK character usually's width is 2, you can open this source file
		// by vim or just cat the file to check the regular result.
		// The last row of `varchar` value will pad with two ' ' before the column.
		// Because the length of `これは日本語をテストするた` plus the length of `...` is 29,
		// no more Japanese character can be added to the line.
		assertEquals(
				"+----+---------+-------------+----------------------+--------------------------------+----------------+----------------------------+" + System.lineSeparator() +
				"| op | boolean |         int |               bigint |                        varchar | decimal(10, 5) |                  timestamp |" + System.lineSeparator() +
				"+----+---------+-------------+----------------------+--------------------------------+----------------+----------------------------+" + System.lineSeparator() +
				"| +I |         |           1 |                    2 |                            abc |           1.23 |      2020-03-01 18:39:14.0 |" + System.lineSeparator() +
				"| +I |   false |             |                    0 |                                |              1 |      2020-03-01 18:39:14.1 |" + System.lineSeparator() +
				"| -D |    true |  2147483647 |                      |                        abcdefg |     1234567890 |     2020-03-01 18:39:14.12 |" + System.lineSeparator() +
				"| +I |   false | -2147483648 |  9223372036854775807 |                                |    12345.06789 |    2020-03-01 18:39:14.123 |" + System.lineSeparator() +
				"| +I |    true |         100 | -9223372036854775808 |                     abcdefg111 |                | 2020-03-01 18:39:14.123456 |" + System.lineSeparator() +
				"| -U |         |          -1 |                   -1 | abcdefghijklmnopqrstuvwxyza... |   -12345.06789 |                            |" + System.lineSeparator() +
				"| +U |         |          -1 |                   -1 |                   这是一段中文 |   -12345.06789 |      2020-03-04 18:39:14.0 |" + System.lineSeparator() +
				"| -D |         |          -1 |                   -1 |  これは日本語をテストするた... |   -12345.06789 |      2020-03-04 18:39:14.0 |" + System.lineSeparator() +
				"+----+---------+-------------+----------------------+--------------------------------+----------------+----------------------------+" + System.lineSeparator() +
				"8 rows in set" + System.lineSeparator(),
				outContent.toString());
	}

	@Test
	public void testPrintWithMultipleRowsAndDeriveColumnWidthByContent() {
		PrintUtils.printAsTableauForm(
				getSchema(),
				getData().subList(0, 3).iterator(),
				new PrintWriter(outContent),
				PrintUtils.MAX_COLUMN_WIDTH,
				"",
				false, // derive column width by content
				true);

		assertEquals(
				"+----+---------+------------+--------+---------+----------------+------------------------+" + System.lineSeparator() +
				"| op | boolean |        int | bigint | varchar | decimal(10, 5) |              timestamp |" + System.lineSeparator() +
				"+----+---------+------------+--------+---------+----------------+------------------------+" + System.lineSeparator() +
				"| +I |         |          1 |      2 |     abc |           1.23 |  2020-03-01 18:39:14.0 |" + System.lineSeparator() +
				"| +I |   false |            |      0 |         |              1 |  2020-03-01 18:39:14.1 |" + System.lineSeparator() +
				"| -D |    true | 2147483647 |        | abcdefg |     1234567890 | 2020-03-01 18:39:14.12 |" + System.lineSeparator() +
				"+----+---------+------------+--------+---------+----------------+------------------------+" + System.lineSeparator() +
				"3 rows in set" + System.lineSeparator(),
				outContent.toString());
	}

	private TableSchema getSchema() {
		return TableSchema.builder()
				.field("boolean", DataTypes.BOOLEAN())
				.field("int", DataTypes.INT())
				.field("bigint", DataTypes.BIGINT())
				.field("varchar", DataTypes.STRING())
				.field("decimal(10, 5)", DataTypes.DECIMAL(10, 5))
				.field("timestamp", DataTypes.TIMESTAMP(6))
				.build();
	}

	private List<Row> getData() {
		List<Row> data = new ArrayList<>();
		data.add(Row.ofKind(
				RowKind.INSERT,
				null,
				1,
				2,
				"abc",
				BigDecimal.valueOf(1.23),
				Timestamp.valueOf("2020-03-01 18:39:14"))
		);
		data.add(Row.ofKind(
				RowKind.INSERT,
				false,
				null,
				0,
				"",
				BigDecimal.valueOf(1),
				Timestamp.valueOf("2020-03-01 18:39:14.1"))
		);
		data.add(Row.ofKind(
				RowKind.DELETE,
				true,
				Integer.MAX_VALUE,
				null,
				"abcdefg",
				BigDecimal.valueOf(1234567890),
				Timestamp.valueOf("2020-03-01 18:39:14.12"))
		);
		data.add(Row.ofKind(
				RowKind.INSERT,
				false,
				Integer.MIN_VALUE,
				Long.MAX_VALUE,
				null,
				BigDecimal.valueOf(12345.06789),
				Timestamp.valueOf("2020-03-01 18:39:14.123"))
		);
		data.add(Row.ofKind(
				RowKind.INSERT,
				true,
				100,
				Long.MIN_VALUE,
				"abcdefg111",
				null,
				Timestamp.valueOf("2020-03-01 18:39:14.123456"))
		);
		data.add(Row.ofKind(
				RowKind.UPDATE_BEFORE,
				null,
				-1,
				-1,
				"abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz",
				BigDecimal.valueOf(-12345.06789),
				null)
		);
		data.add(Row.ofKind(
				RowKind.UPDATE_AFTER,
				null,
				-1,
				-1,
				"这是一段中文",
				BigDecimal.valueOf(-12345.06789),
				Timestamp.valueOf("2020-03-04 18:39:14"))
		);
		data.add(Row.ofKind(
				RowKind.DELETE,
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
