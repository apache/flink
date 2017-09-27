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

package org.apache.flink.table.runtime.batch.io;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.ParseException;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.types.Row;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.types.parser.StringParser;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link RFCRowCsvInputFormat}.
 */
public class RFCRowCsvInputFormatTest {

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

		RFCRowCsvInputFormat format = new RFCRowCsvInputFormat(PATH, fieldTypes, "\n", '|');
		format.enableLenientParsing(false);
		Configuration parameters = new Configuration();
		format.configure(new Configuration());
		format.open(split);

		Row result = new Row(3);
		try {
			result = format.nextRecord(result);
			Assert.fail("Parse Exception was not thrown! (Row too short)");
		} catch (ParseException ignored) {
		} // => ok

		try {
			result = format.nextRecord(result);
			Assert.fail("Parse Exception was not thrown! (Invalid int value)");
		} catch (ParseException ignored) {
		} // => ok

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals("this is", result.getField(0));
		Assert.assertEquals(1, result.getField(1));
		Assert.assertEquals(2.0, result.getField(2));

		try {
			result = format.nextRecord(result);
			Assert.fail("Parse Exception was not thrown! (Row too short)");
		} catch (ParseException ignored) {
		} // => ok

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals("a test", result.getField(0));
		Assert.assertEquals(3, result.getField(1));
		Assert.assertEquals(4.0, result.getField(2));

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals("#next", result.getField(0));
		Assert.assertEquals(5, result.getField(1));
		Assert.assertEquals(6.0, result.getField(2));

		result = format.nextRecord(result);
		Assert.assertNull(result);

		// re-open with lenient = true
		format.enableLenientParsing(true);
		format.configure(parameters);
		format.open(split);

		result = new Row(3);

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals("this is", result.getField(0));
		Assert.assertEquals(1, result.getField(1));
		Assert.assertEquals(2.0, result.getField(2));

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals("a test", result.getField(0));
		Assert.assertEquals(3, result.getField(1));
		Assert.assertEquals(4.0, result.getField(2));

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals("#next", result.getField(0));
		Assert.assertEquals(5, result.getField(1));
		Assert.assertEquals(6.0, result.getField(2));
		result = format.nextRecord(result);
		Assert.assertNull(result);
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

		RFCRowCsvInputFormat format = new RFCRowCsvInputFormat(PATH, fieldTypes, "\n", '|');
		format.enableSkipComment(true);
		format.configure(new Configuration());
		format.open(split);

		Row result = new Row(3);

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals("this is", result.getField(0));
		Assert.assertEquals(1, result.getField(1));
		Assert.assertEquals(2.0, result.getField(2));

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals("a test", result.getField(0));
		Assert.assertEquals(3, result.getField(1));
		Assert.assertEquals(4.0, result.getField(2));

		result = format.nextRecord(result);
		Assert.assertNull(result);
	}

	// Test disabled because we do not support multi char prefix comments right now for RFC compliant CSV parser
	@Test
	@Ignore
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

		RFCRowCsvInputFormat format = new RFCRowCsvInputFormat(PATH, fieldTypes, "\n", '|');
		format.enableSkipComment(true);
		format.configure(new Configuration());
		format.open(split);

		Row result = new Row(3);

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals("this is", result.getField(0));
		Assert.assertEquals(1, result.getField(1));
		Assert.assertEquals(2.0, result.getField(2));

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals("a test", result.getField(0));
		Assert.assertEquals(3, result.getField(1));
		Assert.assertEquals(4.0, result.getField(2));
		result = format.nextRecord(result);
		Assert.assertNull(result);
	}

	@Test
	public void readStringFields() throws Exception {
		String fileContent = "abc|def|ghijk\nabc||hhg\n|||\n||";

		FileInputSplit split = createTempFile(fileContent);

		TypeInformation[] fieldTypes = new TypeInformation[]{
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO};

		RFCRowCsvInputFormat format = new RFCRowCsvInputFormat(PATH, fieldTypes, "\n", '|');
		format.configure(new Configuration());
		format.open(split);

		Row result = new Row(3);

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals("abc", result.getField(0));
		Assert.assertEquals("def", result.getField(1));
		Assert.assertEquals("ghijk", result.getField(2));

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals("abc", result.getField(0));
		Assert.assertEquals("", result.getField(1));
		Assert.assertEquals("hhg", result.getField(2));

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals("", result.getField(0));
		Assert.assertEquals("", result.getField(1));
		Assert.assertEquals("", result.getField(2));

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals("", result.getField(0));
		Assert.assertEquals("", result.getField(1));
		Assert.assertEquals("", result.getField(2));

		result = format.nextRecord(result);
		Assert.assertNull(result);
		Assert.assertTrue(format.reachedEnd());
	}

	@Test
	public void readMixedQuotedStringFields() throws Exception {
		String fileContent = "@a|b|c@|def|@ghijk@\nabc||@|hhg@\n|||\n";

		FileInputSplit split = createTempFile(fileContent);

		TypeInformation[] fieldTypes = new TypeInformation[]{
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO};

		RFCRowCsvInputFormat format = new RFCRowCsvInputFormat(PATH, fieldTypes, "\n", '|');
		format.setRuntimeContext(runtimeContext);
		format.configure(new Configuration());
		format.enableQuotedStringParsing('@');
		format.open(split);

		Row result = new Row(3);

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals("a|b|c", result.getField(0));
		Assert.assertEquals("def", result.getField(1));
		Assert.assertEquals("ghijk", result.getField(2));

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals("abc", result.getField(0));
		Assert.assertEquals("", result.getField(1));
		Assert.assertEquals("|hhg", result.getField(2));

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals("", result.getField(0));
		Assert.assertEquals("", result.getField(1));
		Assert.assertEquals("", result.getField(2));

		result = format.nextRecord(result);
		Assert.assertNull(result);
		Assert.assertTrue(format.reachedEnd());
	}

	@Test
	public void readStringFieldsWithTrailingDelimiters() throws Exception {
		String fileContent = "abc|def|ghijk\nabc||hhg\n|||\n";

		FileInputSplit split = createTempFile(fileContent);

		TypeInformation[] fieldTypes = new TypeInformation[]{
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO};

		RFCRowCsvInputFormat format = new RFCRowCsvInputFormat(PATH, fieldTypes, "\n", '|');
		format.setFieldDelimiter("|");
		format.configure(new Configuration());
		format.open(split);

		Row result = new Row(3);

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals("abc", result.getField(0));
		Assert.assertEquals("def", result.getField(1));
		Assert.assertEquals("ghijk", result.getField(2));

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals("abc", result.getField(0));
		Assert.assertEquals("", result.getField(1));
		Assert.assertEquals("hhg", result.getField(2));

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals("", result.getField(0));
		Assert.assertEquals("", result.getField(1));
		Assert.assertEquals("", result.getField(2));

		result = format.nextRecord(result);
		Assert.assertNull(result);
		Assert.assertTrue(format.reachedEnd());
	}

	@Test
	public void testTailingEmptyFields() throws Exception {
		String fileContent = "abc|def|ghijk\n" +
				"abc|def|\n" +
				"abc||\n" +
				"|||\n" +
				"||\n" +
				"abc|def\n";

		FileInputSplit split = createTempFile(fileContent);

		TypeInformation[] fieldTypes = new TypeInformation[]{
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO};

		RFCRowCsvInputFormat format = new RFCRowCsvInputFormat(PATH, fieldTypes, "\n", '|');
		format.setFieldDelimiter("|");
		format.configure(new Configuration());
		format.open(split);

		Row result = new Row(3);

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals("abc", result.getField(0));
		Assert.assertEquals("def", result.getField(1));
		Assert.assertEquals("ghijk", result.getField(2));

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals("abc", result.getField(0));
		Assert.assertEquals("def", result.getField(1));
		Assert.assertEquals("", result.getField(2));

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals("abc", result.getField(0));
		Assert.assertEquals("", result.getField(1));
		Assert.assertEquals("", result.getField(2));

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals("", result.getField(0));
		Assert.assertEquals("", result.getField(1));
		Assert.assertEquals("", result.getField(2));

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals("", result.getField(0));
		Assert.assertEquals("", result.getField(1));
		Assert.assertEquals("", result.getField(2));

		try {
			format.nextRecord(result);
			Assert.fail("Parse Exception was not thrown! (Row too short)");
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

		RFCRowCsvInputFormat format = new RFCRowCsvInputFormat(PATH, fieldTypes, "\n", '|');

		format.setFieldDelimiter("|");
		format.configure(new Configuration());
		format.open(split);

		Row result = new Row(5);

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals(111, result.getField(0));
		Assert.assertEquals(222, result.getField(1));
		Assert.assertEquals(333, result.getField(2));
		Assert.assertEquals(444, result.getField(3));
		Assert.assertEquals(555, result.getField(4));

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals(666, result.getField(0));
		Assert.assertEquals(777, result.getField(1));
		Assert.assertEquals(888, result.getField(2));
		Assert.assertEquals(999, result.getField(3));
		Assert.assertEquals(0, result.getField(4));

		result = format.nextRecord(result);
		Assert.assertNull(result);
		Assert.assertTrue(format.reachedEnd());
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

		RFCRowCsvInputFormat format = new RFCRowCsvInputFormat(PATH, fieldTypes);
		format.setFieldDelimiter(",");
		format.enableLenientParsing(true);
		format.configure(new Configuration());
		format.open(split);

		Row result = new Row(8);
		int linesCnt = fileContent.split("\n").length;

		for (int i = 0; i < linesCnt; i++) {
			result = format.nextRecord(result);
			if (i != 7) { // skip for string data type
				Assert.assertNull(result.getField(i));
			}
		}

		// ensure no more rows
		Assert.assertNull(format.nextRecord(result));
		Assert.assertTrue(format.reachedEnd());
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

		RFCRowCsvInputFormat format = new RFCRowCsvInputFormat(PATH, fieldTypes);
		format.setFieldDelimiter("|");
		format.configure(new Configuration());
		format.open(split);

		Row result = new Row(5);

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals(11.1, result.getField(0));
		Assert.assertEquals(22.2, result.getField(1));
		Assert.assertEquals(33.3, result.getField(2));
		Assert.assertEquals(44.4, result.getField(3));
		Assert.assertEquals(55.5, result.getField(4));

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals(66.6, result.getField(0));
		Assert.assertEquals(77.7, result.getField(1));
		Assert.assertEquals(88.8, result.getField(2));
		Assert.assertEquals(99.9, result.getField(3));
		Assert.assertEquals(0.0, result.getField(4));

		result = format.nextRecord(result);
		Assert.assertNull(result);
		Assert.assertTrue(format.reachedEnd());
	}

	@Test
	public void testReadFirstN() throws Exception {
		String fileContent = "111|222|333|444|555|\n666|777|888|999|000|\n";

		FileInputSplit split = createTempFile(fileContent);

		TypeInformation[] fieldTypes = new TypeInformation[]{
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO};

		RFCRowCsvInputFormat format = new RFCRowCsvInputFormat(PATH, fieldTypes);
		format.setFieldDelimiter("|");
		format.configure(new Configuration());
		format.open(split);

		Row result = new Row(2);

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals(111, result.getField(0));
		Assert.assertEquals(222, result.getField(1));

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals(666, result.getField(0));
		Assert.assertEquals(777, result.getField(1));

		result = format.nextRecord(result);
		Assert.assertNull(result);
		Assert.assertTrue(format.reachedEnd());
	}

	@Test
	public void testReadSparseWithNullFieldsForTypes() throws Exception {
		final String fileContent = "111x222x333x444x555x666x777x888x999x000\n" +
			"000x999x888x777x666x555x444x333x222x111";

		FileInputSplit split = createTempFile(fileContent);

		TypeInformation[] fieldTypes = new TypeInformation[]{
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO};

		RFCRowCsvInputFormat format = new RFCRowCsvInputFormat(
			PATH,
			fieldTypes,
			new int[]{0, 3, 7});
		format.setFieldDelimiter("x");
		format.configure(new Configuration());
		format.open(split);

		Row result = new Row(3);

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals(111, result.getField(0));
		Assert.assertEquals(444, result.getField(1));
		Assert.assertEquals(888, result.getField(2));

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals(0, result.getField(0));
		Assert.assertEquals(777, result.getField(1));
		Assert.assertEquals(333, result.getField(2));

		result = format.nextRecord(result);
		Assert.assertNull(result);
		Assert.assertTrue(format.reachedEnd());
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

		RFCRowCsvInputFormat format = new RFCRowCsvInputFormat(
			PATH,
			fieldTypes,
			new int[]{0, 3, 7});
		format.setFieldDelimiter("|");
		format.configure(new Configuration());
		format.open(split);

		Row result = new Row(3);
		result = format.nextRecord(result);

		Assert.assertNotNull(result);
		Assert.assertEquals(111, result.getField(0));
		Assert.assertEquals(444, result.getField(1));
		Assert.assertEquals(888, result.getField(2));

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals(0, result.getField(0));
		Assert.assertEquals(777, result.getField(1));
		Assert.assertEquals(333, result.getField(2));

		result = format.nextRecord(result);
		Assert.assertNull(result);
		Assert.assertTrue(format.reachedEnd());
	}

	@Test
	public void testReadSparseWithMask() throws Exception {
		String fileContent = "111&222&333&444&555&666&777&888&999&000&\n" +
			"000&999&888&777&666&555&444&333&222&111&";

		FileInputSplit split = RFCRowCsvInputFormatTest.createTempFile(fileContent);

		TypeInformation[] fieldTypes = new TypeInformation[]{
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO};

		RFCRowCsvInputFormat format = new RFCRowCsvInputFormat(
			PATH,
			fieldTypes,
			new int[]{0, 3, 7});
		format.setFieldDelimiter("&");
		format.configure(new Configuration());
		format.open(split);

		Row result = new Row(3);

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals(111, result.getField(0));
		Assert.assertEquals(444, result.getField(1));
		Assert.assertEquals(888, result.getField(2));

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals(0, result.getField(0));
		Assert.assertEquals(777, result.getField(1));
		Assert.assertEquals(333, result.getField(2));

		result = format.nextRecord(result);
		Assert.assertNull(result);
		Assert.assertTrue(format.reachedEnd());
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
			Assert.assertEquals(-1, result);
			Assert.assertEquals(failure.getValue(), stringParser.getErrorState());
		}
	}

	@Test
	public void testParserCorrectness() throws Exception {
		// RFC 4180 Compliance Test content
		// Taken from http://en.wikipedia.org/wiki/Comma-separated_values#Example
		String fileContent = "Year,Make,Model,Description,Price\n" +
			"1997,Ford,E350,\"ac, abs, moon\",3000.00\n" +
			"1999,Chevy,\"Venture \"\"Extended Edition\"\"\",\"\",4900.00\n" +
			"1996,Jeep,Grand Cherokee,\"MUST SELL! air, moon roof, loaded\",4799.00\n" +
			"1999,Chevy,\"Venture \"\"Extended Edition, Very Large\"\"\",,5000.00\n" +
			"1999,,\"Venture \"\"Extended Edition\"\"\",\"\",4900.00";

		FileInputSplit split = createTempFile(fileContent);

		TypeInformation[] fieldTypes = new TypeInformation[]{
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.DOUBLE_TYPE_INFO};

		RFCRowCsvInputFormat format = new RFCRowCsvInputFormat(PATH, fieldTypes);
		format.setRuntimeContext(runtimeContext);
		format.enableSkipFirstLine(true);
		format.setFieldDelimiter(",");
		format.enableQuotedStringParsing('\"');
		format.enableLenientParsing(true);
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
		r5.setField(0, 1999);
		r5.setField(1, "");
		r5.setField(2, "Venture \"Extended Edition\"");
		r5.setField(3, "");
		r5.setField(4, 4900.0);

		Row[] expectedLines = new Row[]{r1, r2, r3, r4, r5};
		for (Row expected : expectedLines) {
			result = format.nextRecord(result);
			Assert.assertEquals(expected, result);
		}
		Assert.assertNull(format.nextRecord(result));
		Assert.assertTrue(format.reachedEnd());
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
		final String fileContent = "\"20:41:52-1-3-2015\"|\"Re: Taskmanager memory error in Eclipse\"|" +
			"\"Blahblah <blah@blahblah.org>\"|\"blaaa|\"\"blubb\"";
		File tempFile = File.createTempFile("CsvReaderQuotedString", "tmp");
		tempFile.deleteOnExit();
		tempFile.setWritable(true);

		OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(tempFile));
		writer.write(fileContent);
		writer.close();

		TypeInformation[] fieldTypes = new TypeInformation[]{
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO};

		RFCRowCsvInputFormat inputFormat = new RFCRowCsvInputFormat(
			new Path(tempFile.toURI().toString()),
			fieldTypes,
			new int[]{0, 2});
		inputFormat.setRuntimeContext(runtimeContext);
		inputFormat.enableQuotedStringParsing('"');
		inputFormat.setFieldDelimiter("|");
		inputFormat.setRecordDelimiter('\n');
		inputFormat.configure(new Configuration());

		FileInputSplit[] splits = inputFormat.createInputSplits(1);
		inputFormat.open(splits[0]);

		Row record = inputFormat.nextRecord(new Row(2));
		Assert.assertEquals("20:41:52-1-3-2015", record.getField(0));
		Assert.assertEquals("Blahblah <blah@blahblah.org>", record.getField(1));
	}

	@Test
	public void testQuotedStringParsingWithEscapedQuotes() throws Exception {
		String fileContent = "\"\\\"\"Hello\\\"\" World\"|\"We are\\\"\" young\"";
		File tempFile = File.createTempFile("CsvReaderQuotedString", "tmp");
		tempFile.deleteOnExit();
		tempFile.setWritable(true);

		OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(tempFile));
		writer.write(fileContent);
		writer.close();

		TypeInformation[] fieldTypes = new TypeInformation[]{
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO};

		RFCRowCsvInputFormat inputFormat = new RFCRowCsvInputFormat(new Path(tempFile.toURI().toString()), fieldTypes);
		inputFormat.setRuntimeContext(runtimeContext);

		inputFormat.enableQuotedStringParsing('"');
		inputFormat.setFieldDelimiter("|");
		inputFormat.setRecordDelimiter('\n');
		inputFormat.configure(new Configuration());

		FileInputSplit[] splits = inputFormat.createInputSplits(1);
		inputFormat.open(splits[0]);

		Row record = inputFormat.nextRecord(new Row(2));
		Assert.assertEquals("\\\"Hello\\\" World", record.getField(0));
		Assert.assertEquals("We are\\\" young", record.getField(1));
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

		RFCRowCsvInputFormat format = new RFCRowCsvInputFormat(PATH, fieldTypes);
		format.setFieldDelimiter("|");
		format.configure(new Configuration());
		format.open(split);

		Row result = new Row(4);

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals(Date.valueOf("1990-10-14"), result.getField(0));
		Assert.assertEquals(Time.valueOf("02:42:25"), result.getField(1));
		Assert.assertEquals(Timestamp.valueOf("1990-10-14 02:42:25.123"), result.getField(2));
		Assert.assertEquals(Timestamp.valueOf("1990-01-04 02:02:05"), result.getField(3));

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals(Date.valueOf("1990-10-14"), result.getField(0));
		Assert.assertEquals(Time.valueOf("02:42:25"), result.getField(1));
		Assert.assertEquals(Timestamp.valueOf("1990-10-14 02:42:25.123"), result.getField(2));
		Assert.assertEquals(Timestamp.valueOf("1990-01-04 02:02:05.3"), result.getField(3));

		result = format.nextRecord(result);
		Assert.assertNull(result);
		Assert.assertTrue(format.reachedEnd());
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
		RFCRowCsvInputFormat format = new RFCRowCsvInputFormat(
			PATH,
			fieldTypes,
			order);

		format.setFieldDelimiter("|");
		format.configure(new Configuration());
		format.open(split);

		Row result = new Row(3);

		// check first row
		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals(888, result.getField(0));
		Assert.assertEquals(444, result.getField(1));
		Assert.assertEquals(111, result.getField(2));

		// check second row
		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals(333, result.getField(0));
		Assert.assertEquals(777, result.getField(1));
		Assert.assertEquals(0, result.getField(2));

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

		RFCRowCsvInputFormat inputFormat = new RFCRowCsvInputFormat(new Path(tempFile.toURI().toString()), fieldTypes);
		inputFormat.configure(new Configuration());
		inputFormat.setRecordDelimiter(lineBreakerSetup);

		FileInputSplit[] splits = inputFormat.createInputSplits(1);
		inputFormat.open(splits[0]);

		Row result = inputFormat.nextRecord(new Row(1));
		Assert.assertNotNull("Expecting to not return null", result);
		Assert.assertEquals(FIRST_PART, result.getField(0));

		result = inputFormat.nextRecord(result);
		Assert.assertNotNull("Expecting to not return null", result);
		Assert.assertEquals(SECOND_PART, result.getField(0));
	}

	RuntimeContext runtimeContext = new RuntimeContext() {
		@Override
		public String getTaskName() {
			return null;
		}

		@Override
		public MetricGroup getMetricGroup() {
			return null;
		}

		@Override
		public int getNumberOfParallelSubtasks() {
			return 0;
		}

		@Override
		public int getMaxNumberOfParallelSubtasks() {
			return 0;
		}

		@Override
		public int getIndexOfThisSubtask() {
			return 0;
		}

		@Override
		public int getAttemptNumber() {
			return 0;
		}

		@Override
		public String getTaskNameWithSubtasks() {
			return null;
		}

		@Override
		public ExecutionConfig getExecutionConfig() {
			return null;
		}

		@Override
		public ClassLoader getUserCodeClassLoader() {
			return null;
		}

		@Override
		public <V, A extends Serializable> void addAccumulator(String name, Accumulator<V, A> accumulator) {

		}

		@Override
		public <V, A extends Serializable> Accumulator<V, A> getAccumulator(String name) {
			return null;
		}

		@Override
		public Map<String, Accumulator<?, ?>> getAllAccumulators() {
			return null;
		}

		@Override
		public IntCounter getIntCounter(String name) {
			return null;
		}

		@Override
		public LongCounter getLongCounter(String name) {
			return null;
		}

		@Override
		public DoubleCounter getDoubleCounter(String name) {
			return null;
		}

		@Override
		public Histogram getHistogram(String name) {
			return null;
		}

		@Override
		public boolean hasBroadcastVariable(String name) {
			return false;
		}

		@Override
		public <RT> List<RT> getBroadcastVariable(String name) {
			return null;
		}

		@Override
		public <T, C> C getBroadcastVariableWithInitializer(String name, BroadcastVariableInitializer<T, C> initializer) {
			return null;
		}

		@Override
		public DistributedCache getDistributedCache() {
			return null;
		}

		@Override
		public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
			return null;
		}

		@Override
		public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
			return null;
		}

		@Override
		public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
			return null;
		}

		@Override
		public <T, ACC> FoldingState<T, ACC> getFoldingState(FoldingStateDescriptor<T, ACC> stateProperties) {
			return null;
		}

		@Override
		public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
			return null;
		}
	};
}
