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

package org.apache.flink.api.java.io;

import org.apache.flink.api.common.io.ParseException;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.types.parser.StringParser;

import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link RowCsvInputFormat}.
 */
public class RowCsvInputFormatTest {

	private static final Path PATH = new Path("an/ignored/file/");

	// static variables for testing the removal of \r\n to \n
	private static final String FIRST_PART = "That is the first part";
	private static final String SECOND_PART = "That is the second part";

	@Test
	public void ignoreInvalidLines() throws Exception {
		String fileContent =
			"#description of the data\n" +
				"header1|header2|header3|\n" +
				"this is|1|2.0|\n" +
				"//a comment\n" +
				"a test|3|4.0|\n" +
				"#next|5|6.0|\n";

		FileInputSplit split = createTempFile(fileContent);

		TypeInformation[] fieldTypes = new TypeInformation[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO};

		RowCsvInputFormat format = new RowCsvInputFormat(PATH, fieldTypes, "\n", "|");
		format.setLenient(false);
		Configuration parameters = new Configuration();
		format.configure(new Configuration());
		format.open(split);

		Row result = new Row(3);
		try {
			result = format.nextRecord(result);
			fail("Parse Exception was not thrown! (Row too short)");
		} catch (ParseException ignored) {
		} // => ok

		try {
			result = format.nextRecord(result);
			fail("Parse Exception was not thrown! (Invalid int value)");
		} catch (ParseException ignored) {
		} // => ok

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals("this is", result.getField(0));
		assertEquals(1, result.getField(1));
		assertEquals(2.0, result.getField(2));

		try {
			result = format.nextRecord(result);
			fail("Parse Exception was not thrown! (Row too short)");
		} catch (ParseException ignored) {
		} // => ok

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals("a test", result.getField(0));
		assertEquals(3, result.getField(1));
		assertEquals(4.0, result.getField(2));

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals("#next", result.getField(0));
		assertEquals(5, result.getField(1));
		assertEquals(6.0, result.getField(2));

		result = format.nextRecord(result);
		assertNull(result);

		// re-open with lenient = true
		format.setLenient(true);
		format.configure(parameters);
		format.open(split);

		result = new Row(3);

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals("header1", result.getField(0));
		assertNull(result.getField(1));
		assertNull(result.getField(2));

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals("this is", result.getField(0));
		assertEquals(1, result.getField(1));
		assertEquals(2.0, result.getField(2));

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals("a test", result.getField(0));
		assertEquals(3, result.getField(1));
		assertEquals(4.0, result.getField(2));

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals("#next", result.getField(0));
		assertEquals(5, result.getField(1));
		assertEquals(6.0, result.getField(2));
		result = format.nextRecord(result);
		assertNull(result);
	}

	@Test
	public void ignoreSingleCharPrefixComments() throws Exception {
		String fileContent =
			"#description of the data\n" +
				"#successive commented line\n" +
				"this is|1|2.0|\n" +
				"a test|3|4.0|\n" +
				"#next|5|6.0|\n";

		FileInputSplit split = createTempFile(fileContent);

		TypeInformation[] fieldTypes = new TypeInformation[]{
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.DOUBLE_TYPE_INFO};

		RowCsvInputFormat format = new RowCsvInputFormat(PATH, fieldTypes, "\n", "|");
		format.setCommentPrefix("#");
		format.configure(new Configuration());
		format.open(split);

		Row result = new Row(3);

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals("this is", result.getField(0));
		assertEquals(1, result.getField(1));
		assertEquals(2.0, result.getField(2));

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals("a test", result.getField(0));
		assertEquals(3, result.getField(1));
		assertEquals(4.0, result.getField(2));

		result = format.nextRecord(result);
		assertNull(result);
	}

	@Test
	public void ignoreMultiCharPrefixComments() throws Exception {
		String fileContent =
			"//description of the data\n" +
				"//successive commented line\n" +
				"this is|1|2.0|\n" +
				"a test|3|4.0|\n" +
				"//next|5|6.0|\n";

		FileInputSplit split = createTempFile(fileContent);

		TypeInformation[] fieldTypes = new TypeInformation[]{
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.DOUBLE_TYPE_INFO};

		RowCsvInputFormat format = new RowCsvInputFormat(PATH, fieldTypes, "\n", "|");
		format.setCommentPrefix("//");
		format.configure(new Configuration());
		format.open(split);

		Row result = new Row(3);

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals("this is", result.getField(0));
		assertEquals(1, result.getField(1));
		assertEquals(2.0, result.getField(2));

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals("a test", result.getField(0));
		assertEquals(3, result.getField(1));
		assertEquals(4.0, result.getField(2));
		result = format.nextRecord(result);
		assertNull(result);
	}

	@Test
	public void readStringFields() throws Exception {
		String fileContent = "abc|def|ghijk\nabc||hhg\n|||\n||";

		FileInputSplit split = createTempFile(fileContent);

		TypeInformation[] fieldTypes = new TypeInformation[]{
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO};

		RowCsvInputFormat format = new RowCsvInputFormat(PATH, fieldTypes, "\n", "|");
		format.configure(new Configuration());
		format.open(split);

		Row result = new Row(3);

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals("abc", result.getField(0));
		assertEquals("def", result.getField(1));
		assertEquals("ghijk", result.getField(2));

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals("abc", result.getField(0));
		assertEquals("", result.getField(1));
		assertEquals("hhg", result.getField(2));

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals("", result.getField(0));
		assertEquals("", result.getField(1));
		assertEquals("", result.getField(2));

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals("", result.getField(0));
		assertEquals("", result.getField(1));
		assertEquals("", result.getField(2));

		result = format.nextRecord(result);
		assertNull(result);
		assertTrue(format.reachedEnd());
	}

	@Test
	public void readMixedQuotedStringFields() throws Exception {
		String fileContent = "@a|b|c@|def|@ghijk@\nabc||@|hhg@\n|||\n";

		FileInputSplit split = createTempFile(fileContent);

		TypeInformation[] fieldTypes = new TypeInformation[]{
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO};

		RowCsvInputFormat format = new RowCsvInputFormat(PATH, fieldTypes, "\n", "|");
		format.configure(new Configuration());
		format.enableQuotedStringParsing('@');
		format.open(split);

		Row result = new Row(3);

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals("a|b|c", result.getField(0));
		assertEquals("def", result.getField(1));
		assertEquals("ghijk", result.getField(2));

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals("abc", result.getField(0));
		assertEquals("", result.getField(1));
		assertEquals("|hhg", result.getField(2));

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals("", result.getField(0));
		assertEquals("", result.getField(1));
		assertEquals("", result.getField(2));

		result = format.nextRecord(result);
		assertNull(result);
		assertTrue(format.reachedEnd());
	}

	@Test
	public void readStringFieldsWithTrailingDelimiters() throws Exception {
		String fileContent = "abc|-def|-ghijk\nabc|-|-hhg\n|-|-|-\n";

		FileInputSplit split = createTempFile(fileContent);

		TypeInformation[] fieldTypes = new TypeInformation[]{
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO};

		RowCsvInputFormat format = new RowCsvInputFormat(PATH, fieldTypes, "\n", "|");
		format.setFieldDelimiter("|-");
		format.configure(new Configuration());
		format.open(split);

		Row result = new Row(3);

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals("abc", result.getField(0));
		assertEquals("def", result.getField(1));
		assertEquals("ghijk", result.getField(2));

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals("abc", result.getField(0));
		assertEquals("", result.getField(1));
		assertEquals("hhg", result.getField(2));

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals("", result.getField(0));
		assertEquals("", result.getField(1));
		assertEquals("", result.getField(2));

		result = format.nextRecord(result);
		assertNull(result);
		assertTrue(format.reachedEnd());
	}

	@Test
	public void testTailingEmptyFields() throws Exception {
		String fileContent = "abc|-def|-ghijk\n" +
				"abc|-def|-\n" +
				"abc|-|-\n" +
				"|-|-|-\n" +
				"|-|-\n" +
				"abc|-def\n";

		FileInputSplit split = createTempFile(fileContent);

		TypeInformation[] fieldTypes = new TypeInformation[]{
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO};

		RowCsvInputFormat format = new RowCsvInputFormat(PATH, fieldTypes, "\n", "|");
		format.setFieldDelimiter("|-");
		format.configure(new Configuration());
		format.open(split);

		Row result = new Row(3);

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals("abc", result.getField(0));
		assertEquals("def", result.getField(1));
		assertEquals("ghijk", result.getField(2));

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals("abc", result.getField(0));
		assertEquals("def", result.getField(1));
		assertEquals("", result.getField(2));

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals("abc", result.getField(0));
		assertEquals("", result.getField(1));
		assertEquals("", result.getField(2));

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals("", result.getField(0));
		assertEquals("", result.getField(1));
		assertEquals("", result.getField(2));

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals("", result.getField(0));
		assertEquals("", result.getField(1));
		assertEquals("", result.getField(2));

		try {
			format.nextRecord(result);
			fail("Parse Exception was not thrown! (Row too short)");
		} catch (ParseException e) {}
	}

	@Test
	public void testIntegerFields() throws Exception {
		String fileContent = "111|222|333|444|555\n666|777|888|999|000|\n";

		FileInputSplit split = createTempFile(fileContent);

		TypeInformation[] fieldTypes = new TypeInformation[]{
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO};

		RowCsvInputFormat format = new RowCsvInputFormat(PATH, fieldTypes, "\n", "|");

		format.setFieldDelimiter("|");
		format.configure(new Configuration());
		format.open(split);

		Row result = new Row(5);

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals(111, result.getField(0));
		assertEquals(222, result.getField(1));
		assertEquals(333, result.getField(2));
		assertEquals(444, result.getField(3));
		assertEquals(555, result.getField(4));

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals(666, result.getField(0));
		assertEquals(777, result.getField(1));
		assertEquals(888, result.getField(2));
		assertEquals(999, result.getField(3));
		assertEquals(0, result.getField(4));

		result = format.nextRecord(result);
		assertNull(result);
		assertTrue(format.reachedEnd());
	}

	@Test
	public void testEmptyFields() throws Exception {
		String fileContent =
			",,,,,,,,\n" +
				",,,,,,,\n" +
				",,,,,,,,\n" +
				",,,,,,,\n" +
				",,,,,,,,\n" +
				",,,,,,,,\n" +
				",,,,,,,\n" +
				",,,,,,,,\n";

		FileInputSplit split = createTempFile(fileContent);

		TypeInformation[] fieldTypes = new TypeInformation[]{
			BasicTypeInfo.BOOLEAN_TYPE_INFO,
			BasicTypeInfo.BYTE_TYPE_INFO,
			BasicTypeInfo.DOUBLE_TYPE_INFO,
			BasicTypeInfo.FLOAT_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.LONG_TYPE_INFO,
			BasicTypeInfo.SHORT_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO};

		RowCsvInputFormat format = new RowCsvInputFormat(PATH, fieldTypes, true);
		format.setFieldDelimiter(",");
		format.configure(new Configuration());
		format.open(split);

		Row result = new Row(8);
		int linesCnt = fileContent.split("\n").length;

		for (int i = 0; i < linesCnt; i++) {
			result = format.nextRecord(result);
			assertNull(result.getField(i));
		}

		// ensure no more rows
		assertNull(format.nextRecord(result));
		assertTrue(format.reachedEnd());
	}

	@Test
	public void testDoubleFields() throws Exception {
		String fileContent = "11.1|22.2|33.3|44.4|55.5\n66.6|77.7|88.8|99.9|00.0|\n";

		FileInputSplit split = createTempFile(fileContent);

		TypeInformation[] fieldTypes = new TypeInformation[]{
			BasicTypeInfo.DOUBLE_TYPE_INFO,
			BasicTypeInfo.DOUBLE_TYPE_INFO,
			BasicTypeInfo.DOUBLE_TYPE_INFO,
			BasicTypeInfo.DOUBLE_TYPE_INFO,
			BasicTypeInfo.DOUBLE_TYPE_INFO};

		RowCsvInputFormat format = new RowCsvInputFormat(PATH, fieldTypes);
		format.setFieldDelimiter("|");
		format.configure(new Configuration());
		format.open(split);

		Row result = new Row(5);

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals(11.1, result.getField(0));
		assertEquals(22.2, result.getField(1));
		assertEquals(33.3, result.getField(2));
		assertEquals(44.4, result.getField(3));
		assertEquals(55.5, result.getField(4));

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals(66.6, result.getField(0));
		assertEquals(77.7, result.getField(1));
		assertEquals(88.8, result.getField(2));
		assertEquals(99.9, result.getField(3));
		assertEquals(0.0, result.getField(4));

		result = format.nextRecord(result);
		assertNull(result);
		assertTrue(format.reachedEnd());
	}

	@Test
	public void testReadFirstN() throws Exception {
		String fileContent = "111|222|333|444|555|\n666|777|888|999|000|\n";

		FileInputSplit split = createTempFile(fileContent);

		TypeInformation[] fieldTypes = new TypeInformation[]{
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO};

		RowCsvInputFormat format = new RowCsvInputFormat(PATH, fieldTypes);
		format.setFieldDelimiter("|");
		format.configure(new Configuration());
		format.open(split);

		Row result = new Row(2);

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals(111, result.getField(0));
		assertEquals(222, result.getField(1));

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals(666, result.getField(0));
		assertEquals(777, result.getField(1));

		result = format.nextRecord(result);
		assertNull(result);
		assertTrue(format.reachedEnd());
	}

	@Test
	public void testReadSparseWithNullFieldsForTypes() throws Exception {
		String fileContent = "111|x|222|x|333|x|444|x|555|x|666|x|777|x|888|x|999|x|000|x|\n" +
			"000|x|999|x|888|x|777|x|666|x|555|x|444|x|333|x|222|x|111|x|";

		FileInputSplit split = createTempFile(fileContent);

		TypeInformation[] fieldTypes = new TypeInformation[]{
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO};

		RowCsvInputFormat format = new RowCsvInputFormat(
			PATH,
			fieldTypes,
			new int[]{0, 3, 7});
		format.setFieldDelimiter("|x|");
		format.configure(new Configuration());
		format.open(split);

		Row result = new Row(3);

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals(111, result.getField(0));
		assertEquals(444, result.getField(1));
		assertEquals(888, result.getField(2));

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals(0, result.getField(0));
		assertEquals(777, result.getField(1));
		assertEquals(333, result.getField(2));

		result = format.nextRecord(result);
		assertNull(result);
		assertTrue(format.reachedEnd());
	}

	@Test
	public void testReadSparseWithPositionSetter() throws Exception {
		String fileContent = "111|222|333|444|555|666|777|888|999|000|\n" +
			"000|999|888|777|666|555|444|333|222|111|";

		FileInputSplit split = createTempFile(fileContent);

		TypeInformation[] fieldTypes = new TypeInformation[]{
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO};

		RowCsvInputFormat format = new RowCsvInputFormat(
			PATH,
			fieldTypes,
			new int[]{0, 3, 7});
		format.setFieldDelimiter("|");
		format.configure(new Configuration());
		format.open(split);

		Row result = new Row(3);
		result = format.nextRecord(result);

		assertNotNull(result);
		assertEquals(111, result.getField(0));
		assertEquals(444, result.getField(1));
		assertEquals(888, result.getField(2));

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals(0, result.getField(0));
		assertEquals(777, result.getField(1));
		assertEquals(333, result.getField(2));

		result = format.nextRecord(result);
		assertNull(result);
		assertTrue(format.reachedEnd());
	}

	@Test
	public void testReadSparseWithMask() throws Exception {
		String fileContent = "111&&222&&333&&444&&555&&666&&777&&888&&999&&000&&\n" +
			"000&&999&&888&&777&&666&&555&&444&&333&&222&&111&&";

		FileInputSplit split = RowCsvInputFormatTest.createTempFile(fileContent);

		TypeInformation[] fieldTypes = new TypeInformation[]{
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO};

		RowCsvInputFormat format = new RowCsvInputFormat(
			PATH,
			fieldTypes,
			new int[]{0, 3, 7});
		format.setFieldDelimiter("&&");
		format.configure(new Configuration());
		format.open(split);

		Row result = new Row(3);

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals(111, result.getField(0));
		assertEquals(444, result.getField(1));
		assertEquals(888, result.getField(2));

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals(0, result.getField(0));
		assertEquals(777, result.getField(1));
		assertEquals(333, result.getField(2));

		result = format.nextRecord(result);
		assertNull(result);
		assertTrue(format.reachedEnd());
	}

	@Test
	public void testParseStringErrors() throws Exception {
		StringParser stringParser = new StringParser();

		stringParser.enableQuotedStringParsing((byte) '"');

		Map<String, StringParser.ParseErrorState> failures = new HashMap<>();
		failures.put("\"string\" trailing", FieldParser.ParseErrorState.UNQUOTED_CHARS_AFTER_QUOTED_STRING);
		failures.put("\"unterminated ", FieldParser.ParseErrorState.UNTERMINATED_QUOTED_STRING);

		for (Map.Entry<String, StringParser.ParseErrorState> failure : failures.entrySet()) {
			int result = stringParser.parseField(
				failure.getKey().getBytes(ConfigConstants.DEFAULT_CHARSET),
				0,
				failure.getKey().length(),
				new byte[]{(byte) '|'},
				null);
			assertEquals(-1, result);
			assertEquals(failure.getValue(), stringParser.getErrorState());
		}
	}

	@Test
	@Ignore("Test disabled because we do not support double-quote escaped quotes right now.")
	public void testParserCorrectness() throws Exception {
		// RFC 4180 Compliance Test content
		// Taken from http://en.wikipedia.org/wiki/Comma-separated_values#Example
		String fileContent = "Year,Make,Model,Description,Price\n" +
			"1997,Ford,E350,\"ac, abs, moon\",3000.00\n" +
			"1999,Chevy,\"Venture \"\"Extended Edition\"\"\",\"\",4900.00\n" +
			"1996,Jeep,Grand Cherokee,\"MUST SELL! air, moon roof, loaded\",4799.00\n" +
			"1999,Chevy,\"Venture \"\"Extended Edition, Very Large\"\"\",,5000.00\n" +
			",,\"Venture \"\"Extended Edition\"\"\",\"\",4900.00";

		FileInputSplit split = createTempFile(fileContent);

		TypeInformation[] fieldTypes = new TypeInformation[]{
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.DOUBLE_TYPE_INFO};

		RowCsvInputFormat format = new RowCsvInputFormat(PATH, fieldTypes);
		format.setSkipFirstLineAsHeader(true);
		format.setFieldDelimiter(",");
		format.configure(new Configuration());
		format.open(split);

		Row result = new Row(5);
		Row r1 = new Row(5);
		r1.setField(0, 1997);
		r1.setField(1, "Ford");
		r1.setField(2, "E350");
		r1.setField(3, "ac, abs, moon");
		r1.setField(4, 3000.0);

		Row r2 = new Row(5);
		r2.setField(0, 1999);
		r2.setField(1, "Chevy");
		r2.setField(2, "Venture \"Extended Edition\"");
		r2.setField(3, "");
		r2.setField(4, 4900.0);

		Row r3 = new Row(5);
		r3.setField(0, 1996);
		r3.setField(1, "Jeep");
		r3.setField(2, "Grand Cherokee");
		r3.setField(3, "MUST SELL! air, moon roof, loaded");
		r3.setField(4, 4799.0);

		Row r4 = new Row(5);
		r4.setField(0, 1999);
		r4.setField(1, "Chevy");
		r4.setField(2, "Venture \"Extended Edition, Very Large\"");
		r4.setField(3, "");
		r4.setField(4, 5000.0);

		Row r5 = new Row(5);
		r5.setField(0, 0);
		r5.setField(1, "");
		r5.setField(2, "Venture \"Extended Edition\"");
		r5.setField(3, "");
		r5.setField(4, 4900.0);

		Row[] expectedLines = new Row[]{r1, r2, r3, r4, r5};
		for (Row expected : expectedLines) {
			result = format.nextRecord(result);
			assertEquals(expected, result);
		}
		assertNull(format.nextRecord(result));
		assertTrue(format.reachedEnd());
	}

	@Test
	public void testWindowsLineEndRemoval() throws Exception {

		// check typical use case -- linux file is correct and it is set up to linux(\n)
		testRemovingTrailingCR("\n", "\n");

		// check typical windows case -- windows file endings and file has windows file endings set up
		testRemovingTrailingCR("\r\n", "\r\n");

		// check problematic case windows file -- windows file endings(\r\n)
		// but linux line endings (\n) set up
		testRemovingTrailingCR("\r\n", "\n");

		// check problematic case linux file -- linux file endings (\n)
		// but windows file endings set up (\r\n)
		// specific setup for windows line endings will expect \r\n because
		// it has to be set up and is not standard.
	}

	@Test
	public void testQuotedStringParsingWithIncludeFields() throws Exception {
		String fileContent = "\"20:41:52-1-3-2015\"|\"Re: Taskmanager memory error in Eclipse\"|" +
			"\"Blahblah <blah@blahblah.org>\"|\"blaaa|\"blubb\"";
		File tempFile = File.createTempFile("CsvReaderQuotedString", "tmp");
		tempFile.deleteOnExit();
		tempFile.setWritable(true);

		OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(tempFile));
		writer.write(fileContent);
		writer.close();

		TypeInformation[] fieldTypes = new TypeInformation[]{
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO};

		RowCsvInputFormat inputFormat = new RowCsvInputFormat(
			new Path(tempFile.toURI().toString()),
			fieldTypes,
			new int[]{0, 2});
		inputFormat.enableQuotedStringParsing('"');
		inputFormat.setFieldDelimiter("|");
		inputFormat.setDelimiter('\n');
		inputFormat.configure(new Configuration());

		FileInputSplit[] splits = inputFormat.createInputSplits(1);
		inputFormat.open(splits[0]);

		Row record = inputFormat.nextRecord(new Row(2));
		assertEquals("20:41:52-1-3-2015", record.getField(0));
		assertEquals("Blahblah <blah@blahblah.org>", record.getField(1));
	}

	@Test
	public void testQuotedStringParsingWithEscapedQuotes() throws Exception {
		String fileContent = "\"\\\"Hello\\\" World\"|\"We are\\\" young\"";
		File tempFile = File.createTempFile("CsvReaderQuotedString", "tmp");
		tempFile.deleteOnExit();
		tempFile.setWritable(true);

		OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(tempFile));
		writer.write(fileContent);
		writer.close();

		TypeInformation[] fieldTypes = new TypeInformation[]{
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO};

		RowCsvInputFormat inputFormat = new RowCsvInputFormat(new Path(tempFile.toURI().toString()), fieldTypes);
		inputFormat.enableQuotedStringParsing('"');
		inputFormat.setFieldDelimiter("|");
		inputFormat.setDelimiter('\n');
		inputFormat.configure(new Configuration());

		FileInputSplit[] splits = inputFormat.createInputSplits(1);
		inputFormat.open(splits[0]);

		Row record = inputFormat.nextRecord(new Row(2));
		assertEquals("\\\"Hello\\\" World", record.getField(0));
		assertEquals("We are\\\" young", record.getField(1));
	}

	@Test
	public void testSqlTimeFields() throws Exception {
		String fileContent = "1990-10-14|02:42:25|1990-10-14 02:42:25.123|1990-1-4 2:2:5\n" +
			"1990-10-14|02:42:25|1990-10-14 02:42:25.123|1990-1-4 2:2:5.3\n";

		FileInputSplit split = createTempFile(fileContent);

		TypeInformation[] fieldTypes = new TypeInformation[]{
			SqlTimeTypeInfo.DATE,
			SqlTimeTypeInfo.TIME,
			SqlTimeTypeInfo.TIMESTAMP,
			SqlTimeTypeInfo.TIMESTAMP};

		RowCsvInputFormat format = new RowCsvInputFormat(PATH, fieldTypes);
		format.setFieldDelimiter("|");
		format.configure(new Configuration());
		format.open(split);

		Row result = new Row(4);

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals(Date.valueOf("1990-10-14"), result.getField(0));
		assertEquals(Time.valueOf("02:42:25"), result.getField(1));
		assertEquals(Timestamp.valueOf("1990-10-14 02:42:25.123"), result.getField(2));
		assertEquals(Timestamp.valueOf("1990-01-04 02:02:05"), result.getField(3));

		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals(Date.valueOf("1990-10-14"), result.getField(0));
		assertEquals(Time.valueOf("02:42:25"), result.getField(1));
		assertEquals(Timestamp.valueOf("1990-10-14 02:42:25.123"), result.getField(2));
		assertEquals(Timestamp.valueOf("1990-01-04 02:02:05.3"), result.getField(3));

		result = format.nextRecord(result);
		assertNull(result);
		assertTrue(format.reachedEnd());
	}

	@Test
	public void testScanOrder() throws Exception {
		String fileContent =
			// first row
			"111|222|333|444|555|666|777|888|999|000|\n" +
			// second row
			"000|999|888|777|666|555|444|333|222|111|";
		FileInputSplit split = createTempFile(fileContent);

		TypeInformation[] fieldTypes = new TypeInformation[]{
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO};

		int[] order = new int[]{7, 3, 0};
		RowCsvInputFormat format = new RowCsvInputFormat(
			PATH,
			fieldTypes,
			order);

		format.setFieldDelimiter("|");
		format.configure(new Configuration());
		format.open(split);

		Row result = new Row(3);

		// check first row
		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals(888, result.getField(0));
		assertEquals(444, result.getField(1));
		assertEquals(111, result.getField(2));

		// check second row
		result = format.nextRecord(result);
		assertNotNull(result);
		assertEquals(333, result.getField(0));
		assertEquals(777, result.getField(1));
		assertEquals(0, result.getField(2));
	}

	@Test
	public void testEmptyProjection() throws Exception {
		String fileContent =
				"111|222|333\n" +
				"000|999|888";
		FileInputSplit split = createTempFile(fileContent);

		RowCsvInputFormat format = new RowCsvInputFormat(
			PATH,
			new TypeInformation[0],
			new int[0]);

		format.setFieldDelimiter("|");
		format.configure(new Configuration());
		format.open(split);

		Row result = new Row(0);

		// check first row
		result = format.nextRecord(result);
		assertNotNull(result);

		// check second row
		result = format.nextRecord(result);
		assertNotNull(result);
	}

	private static FileInputSplit createTempFile(String content) throws IOException {
		File tempFile = File.createTempFile("test_contents", "tmp");
		tempFile.deleteOnExit();
		OutputStreamWriter wrt = new OutputStreamWriter(new FileOutputStream(tempFile), StandardCharsets.UTF_8);
		wrt.write(content);
		wrt.close();
		return new FileInputSplit(0, new Path(tempFile.toURI().toString()), 0, tempFile.length(), new String[]{"localhost"});
	}

	private static void testRemovingTrailingCR(String lineBreakerInFile, String lineBreakerSetup) throws IOException {
		String fileContent = FIRST_PART + lineBreakerInFile + SECOND_PART + lineBreakerInFile;

		// create input file
		File tempFile = File.createTempFile("CsvInputFormatTest", "tmp");
		tempFile.deleteOnExit();
		tempFile.setWritable(true);

		OutputStreamWriter wrt = new OutputStreamWriter(new FileOutputStream(tempFile));
		wrt.write(fileContent);
		wrt.close();

		TypeInformation[] fieldTypes = new TypeInformation[]{BasicTypeInfo.STRING_TYPE_INFO};

		RowCsvInputFormat inputFormat = new RowCsvInputFormat(new Path(tempFile.toURI().toString()), fieldTypes);
		inputFormat.configure(new Configuration());
		inputFormat.setDelimiter(lineBreakerSetup);

		FileInputSplit[] splits = inputFormat.createInputSplits(1);
		inputFormat.open(splits[0]);

		Row result = inputFormat.nextRecord(new Row(1));
		assertNotNull("Expecting to not return null", result);
		assertEquals(FIRST_PART, result.getField(0));

		result = inputFormat.nextRecord(result);
		assertNotNull("Expecting to not return null", result);
		assertEquals(SECOND_PART, result.getField(0));
	}
}
