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
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.MetricGroup;
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
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;

/**
 * Tests for @{@link RFCCsvInputFormat}.
 *
 */
public class RFCCsvInputFormatTest {

	private static final Path PATH = new Path("an/ignored/file/");
	//Static variables for testing the removal of \r\n to \n
	private static final String FIRST_PART = "That is the first part";
	private static final String SECOND_PART = "That is the second part";

	@Test
	public void ignoreSingleCharPrefixComments() {
		try {
			final String fileContent = "#description of the data\n" +
				"#successive commented line\n" +
				"this is|1|2.0|\n" +
				"a test|3|4.0|\n" +
				"#next|5|6.0|\n";

			final FileInputSplit split = createTempFile(fileContent);

			final TupleTypeInfo<Tuple3<String, Integer, Double>> typeInfo = TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class, Double.class);
			final RFCCsvInputFormat<Tuple3<String, Integer, Double>> format = new RFCTupleCsvInputFormat<Tuple3<String, Integer, Double>>(PATH, "\n", '|', typeInfo);
			format.enableSkipComment(true);

			final Configuration parameters = new Configuration();
			format.configure(parameters);
			format.open(split);

			Tuple3<String, Integer, Double> result = new Tuple3<String, Integer, Double>();

			result = format.nextRecord(result);
			Assert.assertNotNull(result);
			Assert.assertEquals("this is", result.f0);
			Assert.assertEquals(Integer.valueOf(1), result.f1);
			Assert.assertEquals(new Double(2.0), result.f2);

			result = format.nextRecord(result);
			Assert.assertNotNull(result);
			Assert.assertEquals("a test", result.f0);
			Assert.assertEquals(Integer.valueOf(3), result.f1);
			Assert.assertEquals(new Double(4.0), result.f2);

			result = format.nextRecord(result);
			Assert.assertNull(result);
		}
		catch (Exception ex) {
			ex.printStackTrace();
			Assert.fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
		}
	}

	// Test disabled because we do not support multi char prefix comments right now for RFC compliant CSV parser
	@Test
	@Ignore
	public void ignoreMultiCharPrefixComments() {
		try {

			final String fileContent = "//description of the data\n" +
				"//successive commented line\n" +
				"this is|1|2.0|\n" +
				"a test|3|4.0|\n" +
				"//next|5|6.0|\n";

			final FileInputSplit split = createTempFile(fileContent);

			final TupleTypeInfo<Tuple3<String, Integer, Double>> typeInfo = TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class, Double.class);
			final RFCCsvInputFormat<Tuple3<String, Integer, Double>> format = new RFCTupleCsvInputFormat<Tuple3<String, Integer, Double>>(PATH, "\n", '|', typeInfo);
			format.enableSkipComment(true);

			final Configuration parameters = new Configuration();
			format.configure(parameters);
			format.open(split);

			Tuple3<String, Integer, Double> result = new Tuple3<String, Integer, Double>();

			result = format.nextRecord(result);
			Assert.assertNotNull(result);
			Assert.assertEquals("this is", result.f0);
			Assert.assertEquals(Integer.valueOf(1), result.f1);
			Assert.assertEquals(new Double(2.0), result.f2);

			result = format.nextRecord(result);
			Assert.assertNotNull(result);
			Assert.assertEquals("a test", result.f0);
			Assert.assertEquals(Integer.valueOf(3), result.f1);
			Assert.assertEquals(new Double(4.0), result.f2);

			result = format.nextRecord(result);
			Assert.assertNull(result);
		}
		catch (Exception ex) {
			ex.printStackTrace();
			Assert.fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
		}
	}

	@Test
	public void readStringFields() {
		try {
			final String fileContent = "abc|def|ghijk\nabc||hhg\n|||";
			final FileInputSplit split = createTempFile(fileContent);

			final TupleTypeInfo<Tuple3<String, String, String>> typeInfo = TupleTypeInfo.getBasicTupleTypeInfo(String.class, String.class, String.class);
			final RFCCsvInputFormat<Tuple3<String, String, String>> format = new RFCTupleCsvInputFormat<Tuple3<String, String, String>>(PATH, "\n", '|', typeInfo);

			final Configuration parameters = new Configuration();
			format.configure(parameters);
			format.open(split);

			Tuple3<String, String, String> result = new Tuple3<String, String, String>();

			result = format.nextRecord(result);
			Assert.assertNotNull(result);
			Assert.assertEquals("abc", result.f0);
			Assert.assertEquals("def", result.f1);
			Assert.assertEquals("ghijk", result.f2);

			result = format.nextRecord(result);
			Assert.assertNotNull(result);
			Assert.assertEquals("abc", result.f0);
			Assert.assertEquals("", result.f1);
			Assert.assertEquals("hhg", result.f2);

			result = format.nextRecord(result);
			Assert.assertNotNull(result);
			Assert.assertEquals("", result.f0);
			Assert.assertEquals("", result.f1);
			Assert.assertEquals("", result.f2);

			result = format.nextRecord(result);
			Assert.assertNull(result);
			Assert.assertTrue(format.reachedEnd());
		}
		catch (Exception ex) {
			ex.printStackTrace();
			Assert.fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
		}
	}

	@Test
	public void readMixedQuotedStringFields() {
		try {
			final String fileContent = "@a|b|c@|def|@ghijk@\nabc||@|hhg@\n|||";
			final FileInputSplit split = createTempFile(fileContent);

			final TupleTypeInfo<Tuple3<String, String, String>> typeInfo = TupleTypeInfo.getBasicTupleTypeInfo(String.class, String.class, String.class);
			final RFCCsvInputFormat<Tuple3<String, String, String>> format = new RFCTupleCsvInputFormat<Tuple3<String, String, String>>(PATH, "\n", '|', typeInfo);
			format.setRuntimeContext(runtimeContext);

			final Configuration parameters = new Configuration();
			format.configure(parameters);
			format.enableQuotedStringParsing('@');
			format.open(split);

			Tuple3<String, String, String> result = new Tuple3<String, String, String>();

			result = format.nextRecord(result);
			Assert.assertNotNull(result);
			Assert.assertEquals("a|b|c", result.f0);
			Assert.assertEquals("def", result.f1);
			Assert.assertEquals("ghijk", result.f2);

			result = format.nextRecord(result);
			Assert.assertNotNull(result);
			Assert.assertEquals("abc", result.f0);
			Assert.assertEquals("", result.f1);
			Assert.assertEquals("|hhg", result.f2);

			result = format.nextRecord(result);
			Assert.assertNotNull(result);
			Assert.assertEquals("", result.f0);
			Assert.assertEquals("", result.f1);
			Assert.assertEquals("", result.f2);

			result = format.nextRecord(result);
			Assert.assertNull(result);
			Assert.assertTrue(format.reachedEnd());
		}
		catch (Exception ex) {
			ex.printStackTrace();
			Assert.fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
		}
	}

	@Test
	public void readStringFieldsWithTrailingDelimiters() {
		try {
			final String fileContent = "abc-def-ghijk\nabc--hhg\n---\n";
			final FileInputSplit split = createTempFile(fileContent);

			final TupleTypeInfo<Tuple3<String, String, String>> typeInfo = TupleTypeInfo.getBasicTupleTypeInfo(String.class, String.class, String.class);
			final RFCCsvInputFormat<Tuple3<String, String, String>> format = new RFCTupleCsvInputFormat<Tuple3<String, String, String>>(PATH, typeInfo);

			format.setFieldDelimiter("-");

			format.configure(new Configuration());
			format.open(split);

			Tuple3<String, String, String> result = new Tuple3<String, String, String>();

			result = format.nextRecord(result);
			Assert.assertNotNull(result);
			Assert.assertEquals("abc", result.f0);
			Assert.assertEquals("def", result.f1);
			Assert.assertEquals("ghijk", result.f2);

			result = format.nextRecord(result);
			Assert.assertNotNull(result);
			Assert.assertEquals("abc", result.f0);
			Assert.assertEquals("", result.f1);
			Assert.assertEquals("hhg", result.f2);

			result = format.nextRecord(result);
			Assert.assertNotNull(result);
			Assert.assertEquals("", result.f0);
			Assert.assertEquals("", result.f1);
			Assert.assertEquals("", result.f2);

			result = format.nextRecord(result);
			Assert.assertNull(result);
			Assert.assertTrue(format.reachedEnd());
		}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
		}
	}

	@Test
	public void testTailingEmptyFields() throws Exception {
		final String fileContent = "aa,bb,cc\n" + // ok
				"aa,bb,\n" +  // the last field is empty
				"aa,,\n" +    // the last two fields are empty
				",,\n" +      // all fields are empty
				"aa,bb";      // row too short
		final FileInputSplit split = createTempFile(fileContent);

		final TupleTypeInfo<Tuple3<String, String, String>> typeInfo =
				TupleTypeInfo.getBasicTupleTypeInfo(String.class, String.class, String.class);
		final RFCCsvInputFormat<Tuple3<String, String, String>> format = new RFCTupleCsvInputFormat<Tuple3<String, String, String>>(PATH, typeInfo);

		format.setFieldDelimiter(",");

		format.configure(new Configuration());
		format.open(split);

		Tuple3<String, String, String> result = new Tuple3<String, String, String>();

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals("aa", result.f0);
		Assert.assertEquals("bb", result.f1);
		Assert.assertEquals("cc", result.f2);

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals("aa", result.f0);
		Assert.assertEquals("bb", result.f1);
		Assert.assertEquals("", result.f2);

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals("aa", result.f0);
		Assert.assertEquals("", result.f1);
		Assert.assertEquals("", result.f2);

		result = format.nextRecord(result);
		Assert.assertNotNull(result);
		Assert.assertEquals("", result.f0);
		Assert.assertEquals("", result.f1);
		Assert.assertEquals("", result.f2);

		try {
			format.nextRecord(result);
			Assert.fail("Parse Exception was not thrown! (Row too short)");
		} catch (ParseException e) {}
	}

	@Test
	public void testIntegerFields() throws IOException {
		try {
			final String fileContent = "111|222|333|444|555\n666|777|888|999|000|\n";
			final FileInputSplit split = createTempFile(fileContent);

			final TupleTypeInfo<Tuple5<Integer, Integer, Integer, Integer, Integer>> typeInfo =
				TupleTypeInfo.getBasicTupleTypeInfo(Integer.class, Integer.class, Integer.class, Integer.class, Integer.class);
			final RFCCsvInputFormat<Tuple5<Integer, Integer, Integer, Integer, Integer>> format = new RFCTupleCsvInputFormat<Tuple5<Integer, Integer, Integer, Integer, Integer>>(PATH, typeInfo);

			format.setFieldDelimiter("|");

			format.configure(new Configuration());
			format.open(split);

			Tuple5<Integer, Integer, Integer, Integer, Integer> result = new Tuple5<Integer, Integer, Integer, Integer, Integer>();

			result = format.nextRecord(result);
			Assert.assertNotNull(result);
			Assert.assertEquals(Integer.valueOf(111), result.f0);
			Assert.assertEquals(Integer.valueOf(222), result.f1);
			Assert.assertEquals(Integer.valueOf(333), result.f2);
			Assert.assertEquals(Integer.valueOf(444), result.f3);
			Assert.assertEquals(Integer.valueOf(555), result.f4);

			result = format.nextRecord(result);
			Assert.assertNotNull(result);
			Assert.assertEquals(Integer.valueOf(666), result.f0);
			Assert.assertEquals(Integer.valueOf(777), result.f1);
			Assert.assertEquals(Integer.valueOf(888), result.f2);
			Assert.assertEquals(Integer.valueOf(999), result.f3);
			Assert.assertEquals(Integer.valueOf(000), result.f4);

			result = format.nextRecord(result);
			Assert.assertNull(result);
			Assert.assertTrue(format.reachedEnd());
		}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
		}
	}

	@Test
	public void testEmptyFields() throws IOException {
		try {
			final String fileContent = " |0|0|0|0|0\n" +
				"1| |1|1|1|1\n" +
				"2|2| |2|2|2\n" +
				"3|3|3| |3|3\n" +
				"4|4|4|4| |4\n" +
				"5|5|5|5|5\n";
			final FileInputSplit split = createTempFile(fileContent);

			final TupleTypeInfo<Tuple6<Short, Integer, Long, Float, Double, Byte>> typeInfo =
				TupleTypeInfo.getBasicTupleTypeInfo(Short.class, Integer.class, Long.class, Float.class, Double.class, Byte.class);
			final RFCCsvInputFormat<Tuple6<Short, Integer, Long, Float, Double, Byte>> format = new RFCTupleCsvInputFormat<Tuple6<Short, Integer, Long, Float, Double, Byte>>(PATH, typeInfo);

			format.setFieldDelimiter("|");

			format.configure(new Configuration());
			format.open(split);

			Tuple6<Short, Integer, Long, Float, Double, Byte> result = new Tuple6<Short, Integer, Long, Float, Double, Byte>();

			try {
				result = format.nextRecord(result);
				Assert.fail("Empty String Parse Exception was not thrown! (ShortParser)");
			} catch (ParseException e) {}
			try {
				result = format.nextRecord(result);
				Assert.fail("Empty String Parse Exception was not thrown! (IntegerParser)");
			} catch (ParseException e) {}
			try {
				result = format.nextRecord(result);
				Assert.fail("Empty String Parse Exception was not thrown! (LongParser)");
			} catch (ParseException e) {}
			try {
				result = format.nextRecord(result);
				Assert.fail("Empty String Parse Exception was not thrown! (FloatParser)");
			} catch (ParseException e) {}
			try {
				result = format.nextRecord(result);
				Assert.fail("Empty String Parse Exception was not thrown! (DoubleParser)");
			} catch (ParseException e) {}
			try {
				result = format.nextRecord(result);
				Assert.fail("Empty String Parse Exception was not thrown! (ByteParser)");
			} catch (ParseException e) {}

			result = format.nextRecord(result);
			Assert.assertNull(result);
			Assert.assertTrue(format.reachedEnd());
		}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
		}
	}

	@Test
	public void testDoubleFields() throws IOException {
		try {
			final String fileContent = "11.1|22.2|33.3|44.4|55.5\n66.6|77.7|88.8|99.9|00.0|\n";
			final FileInputSplit split = createTempFile(fileContent);

			final TupleTypeInfo<Tuple5<Double, Double, Double, Double, Double>> typeInfo =
					TupleTypeInfo.getBasicTupleTypeInfo(Double.class, Double.class, Double.class, Double.class, Double.class);
			final RFCCsvInputFormat<Tuple5<Double, Double, Double, Double, Double>> format = new RFCTupleCsvInputFormat<Tuple5<Double, Double, Double, Double, Double>>(PATH, typeInfo);

			format.setFieldDelimiter("|");

			format.configure(new Configuration());
			format.open(split);

			Tuple5<Double, Double, Double, Double, Double> result = new Tuple5<Double, Double, Double, Double, Double>();

			result = format.nextRecord(result);
			Assert.assertNotNull(result);
			Assert.assertEquals(Double.valueOf(11.1), result.f0);
			Assert.assertEquals(Double.valueOf(22.2), result.f1);
			Assert.assertEquals(Double.valueOf(33.3), result.f2);
			Assert.assertEquals(Double.valueOf(44.4), result.f3);
			Assert.assertEquals(Double.valueOf(55.5), result.f4);

			result = format.nextRecord(result);
			Assert.assertNotNull(result);
			Assert.assertEquals(Double.valueOf(66.6), result.f0);
			Assert.assertEquals(Double.valueOf(77.7), result.f1);
			Assert.assertEquals(Double.valueOf(88.8), result.f2);
			Assert.assertEquals(Double.valueOf(99.9), result.f3);
			Assert.assertEquals(Double.valueOf(00.0), result.f4);

			result = format.nextRecord(result);
			Assert.assertNull(result);
			Assert.assertTrue(format.reachedEnd());
		}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
		}
	}

	@Test
	public void testReadFirstN() throws IOException {
		try {
			final String fileContent = "111|222|333|444|555|\n666|777|888|999|000|\n";
			final FileInputSplit split = createTempFile(fileContent);

			final TupleTypeInfo<Tuple2<Integer, Integer>> typeInfo = TupleTypeInfo.getBasicTupleTypeInfo(Integer.class, Integer.class);
			final RFCCsvInputFormat<Tuple2<Integer, Integer>> format = new RFCTupleCsvInputFormat<Tuple2<Integer, Integer>>(PATH, typeInfo);

			format.setFieldDelimiter("|");

			format.configure(new Configuration());
			format.open(split);

			Tuple2<Integer, Integer> result = new Tuple2<Integer, Integer>();

			result = format.nextRecord(result);
			Assert.assertNotNull(result);
			Assert.assertEquals(Integer.valueOf(111), result.f0);
			Assert.assertEquals(Integer.valueOf(222), result.f1);

			result = format.nextRecord(result);
			Assert.assertNotNull(result);
			Assert.assertEquals(Integer.valueOf(666), result.f0);
			Assert.assertEquals(Integer.valueOf(777), result.f1);

			result = format.nextRecord(result);
			Assert.assertNull(result);
			Assert.assertTrue(format.reachedEnd());
		}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
		}

	}

	@Test
	public void testReadSparseWithNullFieldsForTypes() throws IOException {
		try {

			final String fileContent = "111x222x333x444x555x666x777x888x999x000\n" +
				"000x999x888x777x666x555x444x333x222x111";
			final FileInputSplit split = createTempFile(fileContent);

			final TupleTypeInfo<Tuple3<Integer, Integer, Integer>> typeInfo = TupleTypeInfo.getBasicTupleTypeInfo(Integer.class, Integer.class, Integer.class);
			final RFCCsvInputFormat<Tuple3<Integer, Integer, Integer>> format = new RFCTupleCsvInputFormat<Tuple3<Integer, Integer, Integer>>(PATH, typeInfo, new boolean[]{true, false, false, true, false, false, false, true});

			format.setFieldDelimiter("x");

			format.configure(new Configuration());
			format.open(split);

			Tuple3<Integer, Integer, Integer> result = new Tuple3<Integer, Integer, Integer>();

			result = format.nextRecord(result);
			Assert.assertNotNull(result);
			Assert.assertEquals(Integer.valueOf(111), result.f0);
			Assert.assertEquals(Integer.valueOf(444), result.f1);
			Assert.assertEquals(Integer.valueOf(888), result.f2);

			result = format.nextRecord(result);
			Assert.assertNotNull(result);
			Assert.assertEquals(Integer.valueOf(000), result.f0);
			Assert.assertEquals(Integer.valueOf(777), result.f1);
			Assert.assertEquals(Integer.valueOf(333), result.f2);

			result = format.nextRecord(result);
			Assert.assertNull(result);
			Assert.assertTrue(format.reachedEnd());
		}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
		}
	}

	@Test
	public void testReadSparseWithPositionSetter() throws IOException {
		try {
			final String fileContent = "111|222|333|444|555|666|777|888|999|000|\n000|999|888|777|666|555|444|333|222|111|";
			final FileInputSplit split = createTempFile(fileContent);

			final TupleTypeInfo<Tuple3<Integer, Integer, Integer>> typeInfo = TupleTypeInfo.getBasicTupleTypeInfo(Integer.class, Integer.class, Integer.class);
			final RFCCsvInputFormat<Tuple3<Integer, Integer, Integer>> format = new RFCTupleCsvInputFormat<Tuple3<Integer, Integer, Integer>>(PATH, typeInfo, new int[]{0, 3, 7});

			format.setFieldDelimiter("|");

			format.configure(new Configuration());
			format.open(split);

			Tuple3<Integer, Integer, Integer> result = new Tuple3<Integer, Integer, Integer>();

			result = format.nextRecord(result);
			Assert.assertNotNull(result);
			Assert.assertEquals(Integer.valueOf(111), result.f0);
			Assert.assertEquals(Integer.valueOf(444), result.f1);
			Assert.assertEquals(Integer.valueOf(888), result.f2);

			result = format.nextRecord(result);
			Assert.assertNotNull(result);
			Assert.assertEquals(Integer.valueOf(000), result.f0);
			Assert.assertEquals(Integer.valueOf(777), result.f1);
			Assert.assertEquals(Integer.valueOf(333), result.f2);

			result = format.nextRecord(result);
			Assert.assertNull(result);
			Assert.assertTrue(format.reachedEnd());
		}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
		}
	}

	@Test
	public void testReadSparseWithMask() throws IOException {
		try {

			final String fileContent = "111&222&333&444&555&666&777&888&999&000\n" +
				"000&999&888&777&666&555&444&333&222&111";

			final FileInputSplit split = createTempFile(fileContent);

			final TupleTypeInfo<Tuple3<Integer, Integer, Integer>> typeInfo = TupleTypeInfo.getBasicTupleTypeInfo(Integer.class, Integer.class, Integer.class);
			final RFCCsvInputFormat<Tuple3<Integer, Integer, Integer>> format = new RFCTupleCsvInputFormat<Tuple3<Integer, Integer, Integer>>(PATH, typeInfo, new boolean[]{true, false, false, true, false, false, false, true});

			format.setFieldDelimiter("&");

			format.configure(new Configuration());
			format.open(split);

			Tuple3<Integer, Integer, Integer> result = new Tuple3<Integer, Integer, Integer>();

			result = format.nextRecord(result);
			Assert.assertNotNull(result);
			Assert.assertEquals(Integer.valueOf(111), result.f0);
			Assert.assertEquals(Integer.valueOf(444), result.f1);
			Assert.assertEquals(Integer.valueOf(888), result.f2);

			result = format.nextRecord(result);
			Assert.assertNotNull(result);
			Assert.assertEquals(Integer.valueOf(000), result.f0);
			Assert.assertEquals(Integer.valueOf(777), result.f1);
			Assert.assertEquals(Integer.valueOf(333), result.f2);

			result = format.nextRecord(result);
			Assert.assertNull(result);
			Assert.assertTrue(format.reachedEnd());
		}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
		}
	}

	@Test
	public void testParseStringErrors() throws Exception {
		StringParser stringParser = new StringParser();
		stringParser.enableQuotedStringParsing((byte) '"');

		Object[][] failures = {
				{"\"string\" trailing", FieldParser.ParseErrorState.UNQUOTED_CHARS_AFTER_QUOTED_STRING},
				{"\"unterminated ", FieldParser.ParseErrorState.UNTERMINATED_QUOTED_STRING}
		};

		for (Object[] failure : failures) {
			String input = (String) failure[0];

			int result = stringParser.parseField(input.getBytes(ConfigConstants.DEFAULT_CHARSET), 0,
				input.length(), new byte[]{'|'}, null);

			Assert.assertThat(result, is(-1));
			Assert.assertThat(stringParser.getErrorState(), is(failure[1]));
		}

	}

	@Test
	public void testParserCorrectness() throws Exception {
		// RFC 4180 Compliance Test content
		// Taken from http://en.wikipedia.org/wiki/Comma-separated_values#Example
		final String fileContent =
				"Year,Make,Model,Description,Price\n" +
				"1997,Ford,E350,\"ac, abs, moon\",3000.00\n" +
				"1999,Chevy,\"Venture \"\"Extended Edition\"\"\",\"\",4900.00\n" +
				"1996,Jeep,Grand Cherokee,\"MUST SELL! air, moon roof, loaded\",4799.00\n" +
				"1999,Chevy,\"Venture \"\"Extended Edition, Very Large\"\"\",,5000.00\n" +
				"1999,,\"Venture \"\"Extended Edition\"\"\",\"\",4900.00";

		final FileInputSplit split = createTempFile(fileContent);

		final TupleTypeInfo<Tuple5<Integer, String, String, String, Double>> typeInfo =
				TupleTypeInfo.getBasicTupleTypeInfo(Integer.class, String.class, String.class, String.class, Double.class);
		final RFCCsvInputFormat<Tuple5<Integer, String, String, String, Double>> format = new RFCTupleCsvInputFormat<Tuple5<Integer, String, String, String, Double>>(PATH, typeInfo);
		format.setRuntimeContext(runtimeContext);

		format.enableSkipFirstLine(true);
		format.setFieldDelimiter(",");
		format.enableQuotedStringParsing('\"');
		format.enableLenientParsing(true);

		format.configure(new Configuration());
		format.open(split);

		Tuple5<Integer, String, String, String, Double> result = new Tuple5<Integer, String, String, String, Double>();

		@SuppressWarnings("unchecked")
		Tuple5<Integer, String, String, String, Double>[] expectedLines = new Tuple5[] {
				new Tuple5<Integer, String, String, String, Double>(1997, "Ford", "E350", "ac, abs, moon", 3000.0),
				new Tuple5<Integer, String, String, String, Double>(1999, "Chevy", "Venture \"Extended Edition\"", "", 4900.0),
				new Tuple5<Integer, String, String, String, Double>(1996, "Jeep", "Grand Cherokee", "MUST SELL! air, moon roof, loaded", 4799.00),
				new Tuple5<Integer, String, String, String, Double>(1999, "Chevy", "Venture \"Extended Edition, Very Large\"", "", 5000.00),
				new Tuple5<Integer, String, String, String, Double>(1999, "", "Venture \"Extended Edition\"", "", 4900.0)
		};

		try {
			for (Tuple5<Integer, String, String, String, Double> expected : expectedLines) {
				result = format.nextRecord(result);
				Assert.assertEquals(expected, result);
			}

			Assert.assertNull(format.nextRecord(result));
			Assert.assertTrue(format.reachedEnd());

		} catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
		}

	}

	@Test
	public void testQuotedStringParsingWithIncludeFields() throws Exception {

		final String fileContent = "\"20:41:52-1-3-2015\"|\"Re: Taskmanager memory error in Eclipse\"|" +
			"\"Blahblah <blah@blahblah.org>\"|\"blaaa|\"\"blubb\"";

		final File tempFile = File.createTempFile("CsvReaderQuotedString", "tmp");
		tempFile.deleteOnExit();
		tempFile.setWritable(true);

		OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(tempFile));
		writer.write(fileContent);
		writer.close();

		TupleTypeInfo<Tuple2<String, String>> typeInfo = TupleTypeInfo.getBasicTupleTypeInfo(String.class, String.class);
		RFCCsvInputFormat<Tuple2<String, String>> inputFormat = new RFCTupleCsvInputFormat<Tuple2<String, String>>(new Path(tempFile.toURI().toString()), typeInfo, new boolean[]{true, false, true});
		inputFormat.setRuntimeContext(runtimeContext);

		inputFormat.enableQuotedStringParsing('"');
		inputFormat.setFieldDelimiter("|");
		inputFormat.setRecordDelimiter('\n');

		inputFormat.configure(new Configuration());
		FileInputSplit[] splits = inputFormat.createInputSplits(1);

		inputFormat.open(splits[0]);

		Tuple2<String, String> record = inputFormat.nextRecord(new Tuple2<String, String>());

		Assert.assertEquals("20:41:52-1-3-2015", record.f0);
		Assert.assertEquals("Blahblah <blah@blahblah.org>", record.f1);
	}

	@Test
	public void testQuotedStringParsingWithEscapedQuotes() throws Exception {

		final String fileContent = "\"\\\"\"Hello\\\"\" World\"|\"We are\\\"\" young\"";

		final File tempFile = File.createTempFile("CsvReaderQuotedString", "tmp");
		tempFile.deleteOnExit();
		tempFile.setWritable(true);

		OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(tempFile));
		writer.write(fileContent);
		writer.close();

		TupleTypeInfo<Tuple2<String, String>> typeInfo = TupleTypeInfo.getBasicTupleTypeInfo(String.class, String.class);
		RFCCsvInputFormat<Tuple2<String, String>> inputFormat = new RFCTupleCsvInputFormat<>(new Path(tempFile.toURI().toString()), typeInfo);
		inputFormat.setRuntimeContext(runtimeContext);

		inputFormat.enableQuotedStringParsing('"');
		inputFormat.setFieldDelimiter("|");
		inputFormat.setRecordDelimiter('\n');

		inputFormat.configure(new Configuration());
		FileInputSplit[] splits = inputFormat.createInputSplits(1);

		inputFormat.open(splits[0]);

		Tuple2<String, String> record = inputFormat.nextRecord(new Tuple2<String, String>());

		Assert.assertEquals("\\\"Hello\\\" World", record.f0);
		Assert.assertEquals("We are\\\" young", record.f1);
	}

	private FileInputSplit createTempFile(String content) throws IOException {
		File tempFile = File.createTempFile("test_contents", "tmp");
		tempFile.deleteOnExit();

		OutputStreamWriter wrt = new OutputStreamWriter(
				new FileOutputStream(tempFile), StandardCharsets.UTF_8
		);
		wrt.write(content);
		wrt.close();

		return new FileInputSplit(0, new Path(tempFile.toURI().toString()), 0, tempFile.length(), new String[] {"localhost"});
	}

	@Test
	public void testWindowsLineEndRemoval() {

		//Check typical use case -- linux file is correct and it is set up to linuc(\n)
		this.testRemovingTrailingCR("\n", "\n");

		//Check typical windows case -- windows file endings and file has windows file endings set up
		this.testRemovingTrailingCR("\r\n", "\r\n");

		//Check problematic case windows file -- windows file endings(\r\n) but linux line endings (\n) set up
		this.testRemovingTrailingCR("\r\n", "\n");

		//Check problematic case linux file -- linux file endings (\n) but windows file endings set up (\r\n)
		//Specific setup for windows line endings will expect \r\n because it has to be set up and is not standard.
	}

	private void testRemovingTrailingCR(String lineBreakerInFile, String lineBreakerSetup) {
		File tempFile = null;

		String fileContent = RFCCsvInputFormatTest.FIRST_PART + lineBreakerInFile + RFCCsvInputFormatTest.SECOND_PART + lineBreakerInFile;

		try {
			// create input file
			tempFile = File.createTempFile("RFCCsvInputFormatTest", "tmp");
			tempFile.deleteOnExit();
			tempFile.setWritable(true);

			OutputStreamWriter wrt = new OutputStreamWriter(new FileOutputStream(tempFile));
			wrt.write(fileContent);
			wrt.close();

			final TupleTypeInfo<Tuple1<String>> typeInfo = TupleTypeInfo.getBasicTupleTypeInfo(String.class);
			final RFCCsvInputFormat<Tuple1<String>> inputFormat = new RFCTupleCsvInputFormat<Tuple1<String>>(new Path(tempFile.toURI().toString()), typeInfo);

			Configuration parameters = new Configuration();
			inputFormat.configure(parameters);

			inputFormat.setRecordDelimiter(lineBreakerSetup);

			FileInputSplit[] splits = inputFormat.createInputSplits(1);

			inputFormat.open(splits[0]);

			Tuple1<String> result = inputFormat.nextRecord(new Tuple1<String>());

			Assert.assertNotNull("Expecting to not return null", result);

			Assert.assertEquals(FIRST_PART, result.f0);

			result = inputFormat.nextRecord(result);

			Assert.assertNotNull("Expecting to not return null", result);
			Assert.assertEquals(SECOND_PART, result.f0);

		}
		catch (Throwable t) {
			System.err.println("test failed with exception: " + t.getMessage());
			t.printStackTrace(System.err);
			Assert.fail("Test erroneous");
		}
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
