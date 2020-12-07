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

package org.apache.flink.formats.csv;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.flink.formats.csv.RowCsvInputFormatTest.PATH;
import static org.apache.flink.formats.csv.RowCsvInputFormatTest.createTempFile;
import static org.junit.Assert.assertEquals;

/**
 * Test split logic for {@link RowCsvInputFormat}.
 */
public class RowCsvInputFormatSplitTest {

	@Test
	public void readAll() throws Exception {
		test("11$\n1,222\n" + "22$2,333\n", 0, -1, '$', asList(Row.of("11\n1", "222"), Row.of("222", "333")));
	}

	@Test
	public void readStartOffset() throws Exception {
		test("11$\n1,222\n" + "22$2,333\n", 1, -1, '$', singletonList(Row.of("222", "333")));
	}

	@Test
	public void readStartOffsetWithSeparator() throws Exception {
		test("11$\n1,222\n" + "22$2,333\n", 3, -1, '$', singletonList(Row.of("222", "333")));
	}

	@Test
	public void readLengthWithSeparator() throws Exception {
		test("11$\n1,222\n" + "22$\n2,333\n", 0, 13, '$', asList(Row.of("11\n1", "222"), Row.of("22\n2", "333")));
	}

	@Test
	public void readLengthWithMultiBytesEscapeChar() throws Exception {
		test("11好\n1,222\n" + "22好\n2,333\n", 0, 13, '好', asList(Row.of("11\n1", "222"), Row.of("22\n2", "333")));
	}

	@Test
	public void readLengthWithMultiBytesEscapeChar2() throws Exception {
		test("11好\n1,222\n" + "22好\n2,333\n", 0, 16, '好', asList(Row.of("11\n1", "222"), Row.of("22\n2", "333")));
	}

	@Test
	public void readLengthWithMultiBytesEscapeChar3() throws Exception {
		test("11好\n1,222\n" + "22好\n2,333\n", 0, 18, '好', asList(Row.of("11\n1", "222"), Row.of("22\n2", "333")));
	}

	@Test
	public void readStartOffsetAndLength() throws Exception {
		test("11好\n1,222\n" + "22好\n2,333\n", 3, 18, '好', singletonList(Row.of("22\n2", "333")));
	}

	@Test
	public void readMultiLineSeparator() throws Exception {
		test("111,222\r\n" + "222,333\r\n", 3, 18, '好', singletonList(Row.of("222", "333")));
	}

	@Test
	public void readRLineSeparator() throws Exception {
		test("111,222\r" + "222,333\r", 3, 18, '好', singletonList(Row.of("222", "333")));
	}

	@Test
	public void testQuotationMark() throws Exception {
		test("\"111\",222\r" + "222,333\r", 0, 18, '$', asList(Row.of("111", "222"), Row.of("222", "333")));
		test("\"111\",222\r" + "222,333\r", 3, 18, '$', singletonList(Row.of("222", "333")));
		test("\"111\",222\r" + "222,333\r", 5, 18, '$', singletonList(Row.of("222", "333")));
		test("\"111\",222\r" + "222,333\r", 6, 18, '$', singletonList(Row.of("222", "333")));

		testOneField("\"111\"\r" + "222\r", 0, 18, '$', asList(Row.of("111"), Row.of("222")));
		testOneField("\"111\"\r" + "222\r", 3, 18, '$', singletonList(Row.of("222")));
		testOneField("\"111\"\r" + "222\r", 5, 18, '$', singletonList(Row.of("222")));
	}

	@Test
	public void testSurroundEscapedDelimiter() throws Exception {
		test("$11$1,222\r" + "222,333\r", 0, 18, '$', asList(Row.of("111", "222"), Row.of("222", "333")));
		test("$11$1,222\r" + "222,333\r", 3, 18, '$', singletonList(Row.of("222", "333")));
		test("$11$1,222\r" + "222,333\r", 5, 18, '$', singletonList(Row.of("222", "333")));
		test("$11$1,222\r" + "222,333\r", 6, 18, '$', singletonList(Row.of("222", "333")));

		testOneField("123*'4**\r" + "123*'4**\n", 0, 18, '*', asList(Row.of("123'4*"), Row.of("123'4*")));
		testOneField("123*'4**\r" + "123*'4**\n", 3, 18, '*', singletonList(Row.of("123'4*")));
		testOneField("123*'4**\r" + "123*'4**\n", 4, 18, '*', singletonList(Row.of("123'4*")));
		testOneField("123*'4**\r" + "123*'4**\n", 5, 18, '*', singletonList(Row.of("123'4*")));

		testOneField("'123''4**'\r" + "'123''4**'\n", 0, 18, '*', asList(Row.of("'123''4*'"), Row.of("'123''4*'")));
		testOneField("'123''4**'\r" + "'123''4**'\n", 3, 18, '*', singletonList(Row.of("'123''4*'")));
		testOneField("'123''4**'\r" + "'123''4**'\n", 4, 18, '*', singletonList(Row.of("'123''4*'")));
		testOneField("'123''4**'\r" + "'123''4**'\n", 5, 18, '*', singletonList(Row.of("'123''4*'")));
		testOneField("'123''4**'\r" + "'123''4**'\n", 6, 18, '*', singletonList(Row.of("'123''4*'")));
	}

	private void test(String content, long offset, long length, char escapeChar, List<Row> expected) throws Exception {
		test(
				content,
				offset,
				length,
				escapeChar,
				expected,
				new TypeInformation[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO});
	}

	private void testOneField(String content, long offset, long length, char escapeChar, List<Row> expected) throws Exception {
		test(
				content,
				offset,
				length,
				escapeChar,
				expected,
				new TypeInformation[]{BasicTypeInfo.STRING_TYPE_INFO});
	}

	private void test(
			String content,
			long offset, long length,
			char escapeChar,
			List<Row> expected,
			TypeInformation[] fieldTypes) throws Exception {
		FileInputSplit split = createTempFile(content, offset, length);

		RowCsvInputFormat.Builder builder = RowCsvInputFormat.builder(new RowTypeInfo(fieldTypes), PATH)
				.setEscapeCharacter(escapeChar);

		RowCsvInputFormat format = builder.build();
		format.configure(new Configuration());
		format.open(split);

		List<Row> rows = new ArrayList<>();
		while (!format.reachedEnd()) {
			Row result = new Row(3);
			result = format.nextRecord(result);
			if (result == null) {
				break;
			} else {
				rows.add(result);
			}
		}

		assertEquals(expected, rows);
	}
}
