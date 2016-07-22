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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.io.ParseException;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.types.parser.StringParser;
import org.junit.Test;

public class RowCsvInputFormatTest {

	private static final Path PATH = new Path("an/ignored/file/");

	//Static variables for testing the removal of \r\n to \n
	private static final String FIRST_PART = "That is the first part";

	private static final String SECOND_PART = "That is the second part";

	@Test
	public void ignoreInvalidLines() {
		try {
			String fileContent = "#description of the data\n" +
					"header1|header2|header3|\n"+
					"this is|1|2.0|\n"+
					"//a comment\n" +
					"a test|3|4.0|\n" +
					"#next|5|6.0|\n";

			FileInputSplit split = createTempFile(fileContent);

			RowTypeInfo typeInfo = new RowTypeInfo(new TypeInformation<?>[] {
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.DOUBLE_TYPE_INFO
			});
			CsvInputFormat<Row> format = new RowCsvInputFormat(PATH, "\n", "|", typeInfo);
			format.setLenient(false);

			Configuration parameters = new Configuration();
			format.configure(parameters);
			format.open(split);

			Row result = new Row(3);

			try {
				result = format.nextRecord(result);
				fail("Parse Exception was not thrown! (Row too short)");
			} catch (ParseException ex) {
			}

			try {
				result = format.nextRecord(result);
				fail("Parse Exception was not thrown! (Invalid int value)");
			} catch (ParseException ex) {
			}

			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals("this is", result.productElement(0));
			assertEquals(new Integer(1), result.productElement(1));
			assertEquals(new Double(2.0), result.productElement(2));

			try {
				result = format.nextRecord(result);
				fail("Parse Exception was not thrown! (Row too short)");
			} catch (ParseException ex) {
			}

			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals("a test", result.productElement(0));
			assertEquals(new Integer(3), result.productElement(1));
			assertEquals(new Double(4.0), result.productElement(2));

			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals("#next", result.productElement(0));
			assertEquals(new Integer(5), result.productElement(1));
			assertEquals(new Double(6.0), result.productElement(2));

			result = format.nextRecord(result);
			assertNull(result);

			//re-open with lenient = true
			format.setLenient(true);
			format.configure(parameters);
			format.open(split);

			result = new Row(3);

			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals("header1", result.productElement(0));
			assertNull(result.productElement(1));
			assertNull(result.productElement(2));

			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals("this is", result.productElement(0));
			assertEquals(new Integer(1), result.productElement(1));
			assertEquals(new Double(2.0), result.productElement(2));

			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals("a test", result.productElement(0));
			assertEquals(new Integer(3), result.productElement(1));
			assertEquals(new Double(4.0), result.productElement(2));

			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals("#next", result.productElement(0));
			assertEquals(new Integer(5), result.productElement(1));
			assertEquals(new Double(6.0), result.productElement(2));

			result = format.nextRecord(result);
			assertNull(result);
		}
		catch (Exception ex) {
			ex.printStackTrace();
			fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
		}
	}

	@Test
	public void ignoreSingleCharPrefixComments() {
		try {
			final String fileContent =
					"#description of the data\n" +
					"#successive commented line\n" +
					"this is|1|2.0|\n" +
					"a test|3|4.0|\n" +
					"#next|5|6.0|\n";

			FileInputSplit split = createTempFile(fileContent);

			RowTypeInfo typeInfo = new RowTypeInfo(new TypeInformation<?>[] {
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.DOUBLE_TYPE_INFO });
			CsvInputFormat<Row> format = new RowCsvInputFormat(PATH, "\n", "|", typeInfo);
			format.setCommentPrefix("#");

			Configuration parameters = new Configuration();
			format.configure(parameters);
			format.open(split);

			Row result = new Row(3);

			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals("this is", result.productElement(0));
			assertEquals(new Integer(1), result.productElement(1));
			assertEquals(new Double(2.0), result.productElement(2));

			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals("a test", result.productElement(0));
			assertEquals(new Integer(3), result.productElement(1));
			assertEquals(new Double(4.0), result.productElement(2));

			result = format.nextRecord(result);
			assertNull(result);
		}
		catch (Exception ex) {
			ex.printStackTrace();
			fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
		}
	}

	@Test
	public void ignoreMultiCharPrefixComments() {
		try {


			final String fileContent = "//description of the data\n" +
					"//successive commented line\n" +
					"this is|1|2.0|\n"+
					"a test|3|4.0|\n" +
					"//next|5|6.0|\n";

			final FileInputSplit split = createTempFile(fileContent);

			final RowTypeInfo typeInfo = new RowTypeInfo(new TypeInformation<?>[] {
				BasicTypeInfo.STRING_TYPE_INFO,	BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO });
			final CsvInputFormat<Row> format = new RowCsvInputFormat(PATH, "\n", "|", typeInfo);
			format.setCommentPrefix("//");

			final Configuration parameters = new Configuration();
			format.configure(parameters);
			format.open(split);

			Row result = new Row(3);

			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals("this is", result.productElement(0));
			assertEquals(new Integer(1), result.productElement(1));
			assertEquals(new Double(2.0), result.productElement(2));

			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals("a test", result.productElement(0));
			assertEquals(new Integer(3), result.productElement(1));
			assertEquals(new Double(4.0), result.productElement(2));

			result = format.nextRecord(result);
			assertNull(result);
		}
		catch (Exception ex) {
			ex.printStackTrace();
			fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
		}
	}

	@Test
	public void readStringFields() {
		try {
			String fileContent = "abc|def|ghijk\nabc||hhg\n|||";
			FileInputSplit split = createTempFile(fileContent);

			RowTypeInfo typeInfo = new RowTypeInfo(new TypeInformation<?>[] {
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO
			});
			CsvInputFormat<Row> format = new RowCsvInputFormat(PATH, "\n", "|", typeInfo);

			final Configuration parameters = new Configuration();
			format.configure(parameters);
			format.open(split);

			Row result = new Row(3);

			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals("abc", result.productElement(0));
			assertEquals("def", result.productElement(1));
			assertEquals("ghijk", result.productElement(2));

			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals("abc", result.productElement(0));
			assertEquals("", result.productElement(1));
			assertEquals("hhg", result.productElement(2));

			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals("", result.productElement(0));
			assertEquals("", result.productElement(1));
			assertEquals("", result.productElement(2));

			result = format.nextRecord(result);
			assertNull(result);
			assertTrue(format.reachedEnd());
		}
		catch (Exception ex) {
			ex.printStackTrace();
			fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
		}
	}

	@Test
	public void readMixedQuotedStringFields() {
		try {
			String fileContent = "@a|b|c@|def|@ghijk@\nabc||@|hhg@\n|||";
			FileInputSplit split = createTempFile(fileContent);

			RowTypeInfo typeInfo = new RowTypeInfo(new TypeInformation<?>[] {
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO
			});
			CsvInputFormat<Row> format = new RowCsvInputFormat(PATH, "\n", "|", typeInfo);

			Configuration parameters = new Configuration();
			format.configure(parameters);
			format.enableQuotedStringParsing('@');
			format.open(split);

			Row result = new Row(3);

			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals("a|b|c", result.productElement(0));
			assertEquals("def", result.productElement(1));
			assertEquals("ghijk", result.productElement(2));

			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals("abc", result.productElement(0));
			assertEquals("", result.productElement(1));
			assertEquals("|hhg", result.productElement(2));

			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals("", result.productElement(0));
			assertEquals("", result.productElement(1));
			assertEquals("", result.productElement(2));

			result = format.nextRecord(result);
			assertNull(result);
			assertTrue(format.reachedEnd());
		}
		catch (Exception ex) {
			ex.printStackTrace();
			fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
		}
	}

	@Test
	public void readStringFieldsWithTrailingDelimiters() {
		try {
			String fileContent = "abc|-def|-ghijk\nabc|-|-hhg\n|-|-|-\n";
			FileInputSplit split = createTempFile(fileContent);

			RowTypeInfo typeInfo = new RowTypeInfo(new TypeInformation<?>[] {
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO
			});
			CsvInputFormat<Row> format = new RowCsvInputFormat(PATH, "\n", "|", typeInfo);

			format.setFieldDelimiter("|-");

			format.configure(new Configuration());
			format.open(split);

			Row result = new Row(3);

			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals("abc", result.productElement(0));
			assertEquals("def", result.productElement(1));
			assertEquals("ghijk", result.productElement(2));

			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals("abc", result.productElement(0));
			assertEquals("", result.productElement(1));
			assertEquals("hhg", result.productElement(2));

			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals("", result.productElement(0));
			assertEquals("", result.productElement(1));
			assertEquals("", result.productElement(2));

			result = format.nextRecord(result);
			assertNull(result);
			assertTrue(format.reachedEnd());
		}
		catch (Exception ex) {
			fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
		}
	}

	@Test
	public void testIntegerFields() throws IOException {
		try {
			String fileContent = "111|222|333|444|555\n666|777|888|999|000|\n";
			FileInputSplit split = createTempFile(fileContent);

			RowTypeInfo typeInfo = new RowTypeInfo(
					new TypeInformation<?>[] {
						BasicTypeInfo.INT_TYPE_INFO,
						BasicTypeInfo.INT_TYPE_INFO,
						BasicTypeInfo.INT_TYPE_INFO,
						BasicTypeInfo.INT_TYPE_INFO,
						BasicTypeInfo.INT_TYPE_INFO
					});
			CsvInputFormat<Row> format = new RowCsvInputFormat(PATH, "\n", "|", typeInfo);

			format.setFieldDelimiter("|");

			format.configure(new Configuration());
			format.open(split);

			Row result = new Row(5);

			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals(Integer.valueOf(111), result.productElement(0));
			assertEquals(Integer.valueOf(222), result.productElement(1));
			assertEquals(Integer.valueOf(333), result.productElement(2));
			assertEquals(Integer.valueOf(444), result.productElement(3));
			assertEquals(Integer.valueOf(555), result.productElement(4));

			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals(Integer.valueOf(666), result.productElement(0));
			assertEquals(Integer.valueOf(777), result.productElement(1));
			assertEquals(Integer.valueOf(888), result.productElement(2));
			assertEquals(Integer.valueOf(999), result.productElement(3));
			assertEquals(Integer.valueOf(000), result.productElement(4));

			result = format.nextRecord(result);
			assertNull(result);
			assertTrue(format.reachedEnd());
		}
		catch (Exception ex) {
			fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
		}
	}

	@Test
	public void testEmptyFields() throws IOException {
		try{
			String fileContent =
					"|0|0|0|0|0|\n" +
					"1||1|1|1|1|\n" +
					"2|2||2|2|2|\n" +
					"3|3|3||3|3|\n" +
					"4|4|4|4||4|\n" +
					"5|5|5|5|5||\n";

			FileInputSplit split = createTempFile(fileContent);

			//TODO: FLOAT_TYPE_INFO and DOUBLE_TYPE_INFO don't handle correctly null values
			RowTypeInfo typeInfo = new RowTypeInfo(new TypeInformation<?>[] {
				BasicTypeInfo.SHORT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.LONG_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO,
//				BasicTypeInfo.FLOAT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO,
//				BasicTypeInfo.DOUBLE_TYPE_INFO,
				BasicTypeInfo.BYTE_TYPE_INFO
			});
			CsvInputFormat<Row> format = new RowCsvInputFormat(PATH, typeInfo);

			format.setFieldDelimiter("|");

			format.configure(new Configuration());
			format.open(split);

			Row result = new Row(6);
			int linesCnt = fileContent.split("\n").length;

			for (int i = 0; i < linesCnt; i++) {
				result = format.nextRecord(result);
				assertNull(result.productElement(i));
			}

			//ensure no more rows
			assertNull(format.nextRecord(result));
			assertTrue(format.reachedEnd());
		} catch (Exception ex) {
			fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
		}
	}

	@Test
	public void testDoubleFields() throws IOException {
		try {
			String fileContent = "11.1|22.2|33.3|44.4|55.5\n66.6|77.7|88.8|99.9|00.0|\n";
			FileInputSplit split = createTempFile(fileContent);

			RowTypeInfo typeInfo = new RowTypeInfo(new TypeInformation<?>[] {
				BasicTypeInfo.DOUBLE_TYPE_INFO,
				BasicTypeInfo.DOUBLE_TYPE_INFO,
				BasicTypeInfo.DOUBLE_TYPE_INFO,
				BasicTypeInfo.DOUBLE_TYPE_INFO,
				BasicTypeInfo.DOUBLE_TYPE_INFO
			});
			CsvInputFormat<Row> format = new RowCsvInputFormat(PATH, typeInfo);

			format.setFieldDelimiter("|");

			format.configure(new Configuration());
			format.open(split);

			Row result = new Row(5);

			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals(Double.valueOf(11.1), result.productElement(0));
			assertEquals(Double.valueOf(22.2), result.productElement(1));
			assertEquals(Double.valueOf(33.3), result.productElement(2));
			assertEquals(Double.valueOf(44.4), result.productElement(3));
			assertEquals(Double.valueOf(55.5), result.productElement(4));

			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals(Double.valueOf(66.6), result.productElement(0));
			assertEquals(Double.valueOf(77.7), result.productElement(1));
			assertEquals(Double.valueOf(88.8), result.productElement(2));
			assertEquals(Double.valueOf(99.9), result.productElement(3));
			assertEquals(Double.valueOf(00.0), result.productElement(4));

			result = format.nextRecord(result);
			assertNull(result);
			assertTrue(format.reachedEnd());
		}
		catch (Exception ex) {
			fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
		}
	}

	@Test
	public void testReadFirstN() throws IOException {
		try {
			final String fileContent = "111|222|333|444|555|\n666|777|888|999|000|\n";
			final FileInputSplit split = createTempFile(fileContent);

			final RowTypeInfo typeInfo = new RowTypeInfo(
					new TypeInformation<?>[] {
						BasicTypeInfo.INT_TYPE_INFO,
						BasicTypeInfo.INT_TYPE_INFO
					});
			CsvInputFormat<Row> format = new RowCsvInputFormat(PATH, typeInfo);

			format.setFieldDelimiter("|");

			format.configure(new Configuration());
			format.open(split);

			Row result = new Row(2);

			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals(Integer.valueOf(111), result.productElement(0));
			assertEquals(Integer.valueOf(222), result.productElement(1));

			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals(Integer.valueOf(666), result.productElement(0));
			assertEquals(Integer.valueOf(777), result.productElement(1));

			result = format.nextRecord(result);
			assertNull(result);
			assertTrue(format.reachedEnd());
		}
		catch (Exception ex) {
			fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
		}

	}

	@Test
	public void testReadSparseWithNullFieldsForTypes() throws IOException {
		try {
			String fileContent = "111|x|222|x|333|x|444|x|555|x|666|x|777|x|888|x|999|x|000|x|\n" +
					"000|x|999|x|888|x|777|x|666|x|555|x|444|x|333|x|222|x|111|x|";
			FileInputSplit split = createTempFile(fileContent);

			RowTypeInfo typeInfo = new RowTypeInfo(new TypeInformation<?>[] {
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO
			});
			CsvInputFormat<Row> format = new RowCsvInputFormat(PATH, typeInfo,
					new boolean[] { true, false, false, true, false, false, false, true });

			format.setFieldDelimiter("|x|");

			format.setFieldDelimiter("|x|");

			format.configure(new Configuration());
			format.open(split);

			Row result = new Row(3);

			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals(Integer.valueOf(111), result.productElement(0));
			assertEquals(Integer.valueOf(444), result.productElement(1));
			assertEquals(Integer.valueOf(888), result.productElement(2));

			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals(Integer.valueOf(000), result.productElement(0));
			assertEquals(Integer.valueOf(777), result.productElement(1));
			assertEquals(Integer.valueOf(333), result.productElement(2));

			result = format.nextRecord(result);
			assertNull(result);
			assertTrue(format.reachedEnd());
		}
		catch (Exception ex) {
			fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
		}
	}

	@Test
	public void testReadSparseWithPositionSetter() throws IOException {
		try {
			String fileContent = "111|222|333|444|555|666|777|888|999|000|\n000|999|888|777|666|555|444|333|222|111|";
			FileInputSplit split = createTempFile(fileContent);

			RowTypeInfo typeInfo = new RowTypeInfo(new TypeInformation<?>[] {
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO
			});
			CsvInputFormat<Row> format = new RowCsvInputFormat(PATH, typeInfo, new int[] { 0, 3, 7 });

			format.setFieldDelimiter("|");

			format.configure(new Configuration());
			format.open(split);

			Row result = new Row(3);

			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals(Integer.valueOf(111), result.productElement(0));
			assertEquals(Integer.valueOf(444), result.productElement(1));
			assertEquals(Integer.valueOf(888), result.productElement(2));

			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals(Integer.valueOf(000), result.productElement(0));
			assertEquals(Integer.valueOf(777), result.productElement(1));
			assertEquals(Integer.valueOf(333), result.productElement(2));

			result = format.nextRecord(result);
			assertNull(result);
			assertTrue(format.reachedEnd());
		}
		catch (Exception ex) {
			fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
		}
	}

	@Test
	public void testReadSparseWithMask() throws IOException {
		try {
			String fileContent =
					"111&&222&&333&&444&&555&&666&&777&&888&&999&&000&&\n" +
					"000&&999&&888&&777&&666&&555&&444&&333&&222&&111&&";
			FileInputSplit split = createTempFile(fileContent);

			RowTypeInfo typeInfo = new RowTypeInfo(new TypeInformation<?>[] {
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO
			});
			CsvInputFormat<Row> format = new RowCsvInputFormat(PATH, typeInfo,
					new boolean[] { true, false, false, true, false, false, false, true });

			format.setFieldDelimiter("&&");

			format.configure(new Configuration());
			format.open(split);

			Row result = new Row(3);

			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals(Integer.valueOf(111), result.productElement(0));
			assertEquals(Integer.valueOf(444), result.productElement(1));
			assertEquals(Integer.valueOf(888), result.productElement(2));

			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals(Integer.valueOf(000), result.productElement(0));
			assertEquals(Integer.valueOf(777), result.productElement(1));
			assertEquals(Integer.valueOf(333), result.productElement(2));

			result = format.nextRecord(result);
			assertNull(result);
			assertTrue(format.reachedEnd());
		}
		catch (Exception ex) {
			fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
		}
	}

	@Test
	public void testParseStringErrors() throws Exception {
		StringParser stringParser = new StringParser();
		stringParser.enableQuotedStringParsing((byte)'"');

		Object[][] failures = {
				{"\"string\" trailing", FieldParser.ParseErrorState.UNQUOTED_CHARS_AFTER_QUOTED_STRING},
				{"\"unterminated ", FieldParser.ParseErrorState.UNTERMINATED_QUOTED_STRING}
		};

		for (Object[] failure : failures) {
			String input = (String) failure[0];

			int result = stringParser.parseField(input.getBytes(), 0, input.length(), new byte[]{'|'}, null);

			assertThat(result, is(-1));
			assertThat(stringParser.getErrorState(), is(failure[1]));
		}


	}

	// Test disabled becase we do not support double-quote escaped quotes right now.
	// @Test
	public void testParserCorrectness() throws Exception {
		// RFC 4180 Compliance Test content
		// Taken from http://en.wikipedia.org/wiki/Comma-separated_values#Example
		String fileContent =
			"Year,Make,Model,Description,Price\n" +
			"1997,Ford,E350,\"ac, abs, moon\",3000.00\n" +
			"1999,Chevy,\"Venture \"\"Extended Edition\"\"\",\"\",4900.00\n" +
			"1996,Jeep,Grand Cherokee,\"MUST SELL! air, moon roof, loaded\",4799.00\n" +
			"1999,Chevy,\"Venture \"\"Extended Edition, Very Large\"\"\",,5000.00\n" +
			",,\"Venture \"\"Extended Edition\"\"\",\"\",4900.00";

		FileInputSplit split = createTempFile(fileContent);

		RowTypeInfo typeInfo = new RowTypeInfo(new TypeInformation<?>[] {
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.DOUBLE_TYPE_INFO
		});
		CsvInputFormat<Row> format = new RowCsvInputFormat(PATH, typeInfo);

		format.setSkipFirstLineAsHeader(true);
		format.setFieldDelimiter(',');

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

		Row[] expectedLines = new Row[] { r1, r2, r3, r4, r5 };
		try {
			for (Row expected : expectedLines) {
				result = format.nextRecord(result);
				assertEquals(expected, result);
			}

			assertNull(format.nextRecord(result));
			assertTrue(format.reachedEnd());

		} catch (Exception ex) {
			fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
		}

	}

	private FileInputSplit createTempFile(String content) throws IOException {
		File tempFile = File.createTempFile("test_contents", "tmp");
		tempFile.deleteOnExit();

		OutputStreamWriter wrt = new OutputStreamWriter(new FileOutputStream(tempFile), StandardCharsets.UTF_8);
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
		File tempFile=null;

		String fileContent = FIRST_PART + lineBreakerInFile + SECOND_PART + lineBreakerInFile;

		try {
			// create input file
			tempFile = File.createTempFile("CsvInputFormatTest", "tmp");
			tempFile.deleteOnExit();
			tempFile.setWritable(true);

			OutputStreamWriter wrt = new OutputStreamWriter(new FileOutputStream(tempFile));
			wrt.write(fileContent);
			wrt.close();

			RowTypeInfo typeInfo = new RowTypeInfo(new TypeInformation<?>[] {BasicTypeInfo.STRING_TYPE_INFO });
			CsvInputFormat<Row> inputFormat = new RowCsvInputFormat(new Path(tempFile.toURI().toString()), typeInfo);

			Configuration parameters = new Configuration();
			inputFormat.configure(parameters);

			inputFormat.setDelimiter(lineBreakerSetup);

			FileInputSplit[] splits = inputFormat.createInputSplits(1);

			inputFormat.open(splits[0]);

			Row result = inputFormat.nextRecord(new Row(1));

			assertNotNull("Expecting to not return null", result);

			assertEquals(FIRST_PART, result.productElement(0));

			result = inputFormat.nextRecord(result);

			assertNotNull("Expecting to not return null", result);
			assertEquals(SECOND_PART, result.productElement(0));

		}
		catch (Throwable t) {
			System.err.println("test failed with exception: " + t.getMessage());
			t.printStackTrace(System.err);
			fail("Test erroneous");
		}
	}

	@Test
	public void testQuotedStringParsingWithIncludeFields() throws Exception {
		final String fileContent = "\"20:41:52-1-3-2015\"|\"Re: Taskmanager memory error in Eclipse\"|" +
				"\"Blahblah <blah@blahblah.org>\"|\"blaaa|\"blubb\"";

		final File tempFile = File.createTempFile("CsvReaderQuotedString", "tmp");
		tempFile.deleteOnExit();
		tempFile.setWritable(true);

		OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(tempFile));
		writer.write(fileContent);
		writer.close();

		final RowTypeInfo typeInfo = new RowTypeInfo(
				new TypeInformation<?>[] { BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO });
		CsvInputFormat<Row> inputFormat = new RowCsvInputFormat(
				new Path(tempFile.toURI().toString()), typeInfo, new boolean[] { true, false, true });

		inputFormat.enableQuotedStringParsing('"');
		inputFormat.setFieldDelimiter('|');
		inputFormat.setDelimiter('\n');

		inputFormat.configure(new Configuration());
		FileInputSplit[] splits = inputFormat.createInputSplits(1);

		inputFormat.open(splits[0]);

		Row record = inputFormat.nextRecord(new Row(2));

		assertEquals("20:41:52-1-3-2015", record.productElement(0));
		assertEquals("Blahblah <blah@blahblah.org>", record.productElement(1));
	}

	@Test
	public void testQuotedStringParsingWithEscapedQuotes() throws Exception {
		final String fileContent = "\"\\\"Hello\\\" World\"|\"We are\\\" young\"";

		final File tempFile = File.createTempFile("CsvReaderQuotedString", "tmp");
		tempFile.deleteOnExit();
		tempFile.setWritable(true);

		OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(tempFile));
		writer.write(fileContent);
		writer.close();

		RowTypeInfo typeInfo = new RowTypeInfo(new TypeInformation<?>[] { BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO });
		CsvInputFormat<Row> inputFormat = new RowCsvInputFormat(new Path(tempFile.toURI().toString()), typeInfo);

		inputFormat.enableQuotedStringParsing('"');
		inputFormat.setFieldDelimiter('|');
		inputFormat.setDelimiter('\n');

		inputFormat.configure(new Configuration());
		FileInputSplit[] splits = inputFormat.createInputSplits(1);

		inputFormat.open(splits[0]);

		Row record = inputFormat.nextRecord(new Row(2));

		assertEquals("\\\"Hello\\\" World", record.productElement(0));
		assertEquals("We are\\\" young", record.productElement(1));
	}

	/**
	 * Tests that the CSV input format can deal with POJOs which are subclasses.
	 *
	 * @throws Exception
	 */
	@Test
	public void testPojoSubclassType() throws Exception {
		final String fileContent = "t1,foobar,tweet2\nt2,barfoo,tweet2";

		final File tempFile = File.createTempFile("CsvReaderPOJOSubclass", "tmp");
		tempFile.deleteOnExit();

		OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(tempFile));
		writer.write(fileContent);
		writer.close();

		PojoTypeInfo<TwitterPOJO> typeInfo = (PojoTypeInfo<TwitterPOJO>)TypeExtractor.createTypeInfo(TwitterPOJO.class);
		CsvInputFormat<TwitterPOJO> inputFormat = new PojoCsvInputFormat<>(new Path(tempFile.toURI().toString()), typeInfo);

		inputFormat.configure(new Configuration());
		FileInputSplit[] splits = inputFormat.createInputSplits(1);

		inputFormat.open(splits[0]);

		List<TwitterPOJO> expected = new ArrayList<>();

		for (String line: fileContent.split("\n")) {
			String[] elements = line.split(",");
			expected.add(new TwitterPOJO(elements[0], elements[1], elements[2]));
		}

		List<TwitterPOJO> actual = new ArrayList<>();

		TwitterPOJO pojo;

		while((pojo = inputFormat.nextRecord(new TwitterPOJO())) != null) {
			actual.add(pojo);
		}

		assertEquals(expected, actual);
	}

	// --------------------------------------------------------------------------------------------
	// Custom types for testing
	// --------------------------------------------------------------------------------------------

	public static class PojoItem {
		public int field1;
		public String field2;
		public Double field3;
		public String field4;
	}

	public static class PrivatePojoItem {
		private int field1;
		private String field2;
		private Double field3;
		private String field4;

		public int getField1() {
			return field1;
		}

		public void setField1(int field1) {
			this.field1 = field1;
		}

		public String getField2() {
			return field2;
		}

		public void setField2(String field2) {
			this.field2 = field2;
		}

		public Double getField3() {
			return field3;
		}

		public void setField3(Double field3) {
			this.field3 = field3;
		}

		public String getField4() {
			return field4;
		}

		public void setField4(String field4) {
			this.field4 = field4;
		}
	}

	public static class POJO {
		public String table;
		public String time;

		public POJO() {
			this("", "");
		}

		public POJO(String table, String time) {
			this.table = table;
			this.time = time;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof POJO) {
				POJO other = (POJO) obj;
				return table.equals(other.table) && time.equals(other.time);
			} else {
				return false;
			}
		}
	}

	public static class TwitterPOJO extends POJO {
		public String tweet;

		public TwitterPOJO() {
			this("", "", "");
		}

		public TwitterPOJO(String table, String time, String tweet) {
			super(table, time);
			this.tweet = tweet;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof TwitterPOJO) {
				TwitterPOJO other = (TwitterPOJO) obj;
				return super.equals(other) && tweet.equals(other.tweet);
			} else {
				return false;
			}
		}
	}

}
