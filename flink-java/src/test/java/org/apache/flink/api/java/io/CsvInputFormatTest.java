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

import com.google.common.base.Charsets;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.types.parser.StringParser;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CsvInputFormatTest {
	
	private static final Path PATH = new Path("an/ignored/file/");
	
	//Static variables for testing the removal of \r\n to \n
	private static final String FIRST_PART = "That is the first part";
	
	private static final String SECOND_PART = "That is the second part";
	
	@Test
	public void ignoreInvalidLines() {
		try {
			
			
			final String fileContent =  "#description of the data\n" + 
										"header1|header2|header3|\n"+
										"this is|1|2.0|\n"+
										"//a comment\n" +
										"a test|3|4.0|\n" +
										"#next|5|6.0|\n";
			
			final FileInputSplit split = createTempFile(fileContent);
			
			CsvInputFormat<Tuple3<String, Integer, Double>> format = 
					new CsvInputFormat<Tuple3<String, Integer, Double>>(PATH, "\n", '|',  String.class, Integer.class, Double.class);
			format.setLenient(true);
		
			final Configuration parameters = new Configuration();
			format.configure(parameters);
			format.open(split);
			
			
			Tuple3<String, Integer, Double> result = new Tuple3<String, Integer, Double>();
			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals("this is", result.f0);
			assertEquals(new Integer(1), result.f1);
			assertEquals(new Double(2.0), result.f2);
			
			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals("a test", result.f0);
			assertEquals(new Integer(3), result.f1);
			assertEquals(new Double(4.0), result.f2);
			
			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals("#next", result.f0);
			assertEquals(new Integer(5), result.f1);
			assertEquals(new Double(6.0), result.f2);

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
			final String fileContent = "#description of the data\n" +
									   "#successive commented line\n" +
									   "this is|1|2.0|\n" +
									   "a test|3|4.0|\n" +
									   "#next|5|6.0|\n";
			
			final FileInputSplit split = createTempFile(fileContent);
			
			CsvInputFormat<Tuple3<String, Integer, Double>> format = 
					new CsvInputFormat<Tuple3<String, Integer, Double>>(PATH, "\n", '|', String.class, Integer.class, Double.class);
			format.setCommentPrefix("#");
		
			final Configuration parameters = new Configuration();
			format.configure(parameters);
			format.open(split);
			
			Tuple3<String, Integer, Double> result = new Tuple3<String, Integer, Double>();
			
			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals("this is", result.f0);
			assertEquals(new Integer(1), result.f1);
			assertEquals(new Double(2.0), result.f2);
			
			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals("a test", result.f0);
			assertEquals(new Integer(3), result.f1);
			assertEquals(new Double(4.0), result.f2);

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
			
			CsvInputFormat<Tuple3<String, Integer, Double>> format = 
					new CsvInputFormat<Tuple3<String, Integer, Double>>(PATH, "\n", '|', String.class, Integer.class, Double.class);
			format.setCommentPrefix("//");
		
			final Configuration parameters = new Configuration();
			format.configure(parameters);
			format.open(split);
			
			Tuple3<String, Integer, Double> result = new Tuple3<String, Integer, Double>();
			
			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals("this is", result.f0);
			assertEquals(new Integer(1), result.f1);
			assertEquals(new Double(2.0), result.f2);
			
			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals("a test", result.f0);
			assertEquals(new Integer(3), result.f1);
			assertEquals(new Double(4.0), result.f2);
			
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
			final String fileContent = "abc|def|ghijk\nabc||hhg\n|||";
			final FileInputSplit split = createTempFile(fileContent);
			
			final CsvInputFormat<Tuple3<String, String, String>> format = new CsvInputFormat<Tuple3<String, String, String>>(PATH, "\n", '|', String.class, String.class, String.class);
		
			final Configuration parameters = new Configuration();
			format.configure(parameters);
			format.open(split);
			
			Tuple3<String, String, String> result = new Tuple3<String, String, String>();
			
			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals("abc", result.f0);
			assertEquals("def", result.f1);
			assertEquals("ghijk", result.f2);
			
			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals("abc", result.f0);
			assertEquals("", result.f1);
			assertEquals("hhg", result.f2);
			
			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals("", result.f0);
			assertEquals("", result.f1);
			assertEquals("", result.f2);
			
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
			final String fileContent = "abc|def|ghijk\nabc||hhg\n|||\n";
			final FileInputSplit split = createTempFile(fileContent);
		
			final CsvInputFormat<Tuple3<String, String, String>> format = new CsvInputFormat<Tuple3<String, String, String>>(PATH);
			
			format.setFieldDelimiter('|');
			format.setFieldTypes(String.class, String.class, String.class);
			
			format.configure(new Configuration());
			format.open(split);

			Tuple3<String, String, String> result = new Tuple3<String, String, String>();
			
			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals("abc", result.f0);
			assertEquals("def", result.f1);
			assertEquals("ghijk", result.f2);
			
			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals("abc", result.f0);
			assertEquals("", result.f1);
			assertEquals("hhg", result.f2);
			
			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals("", result.f0);
			assertEquals("", result.f1);
			assertEquals("", result.f2);
			
			result = format.nextRecord(result);
			assertNull(result);
			assertTrue(format.reachedEnd());
		}
		catch (Exception ex) {
			fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
		}
	}
	
	@Test
	public void testIntegerFieldsl() throws IOException {
		try {
			final String fileContent = "111|222|333|444|555\n666|777|888|999|000|\n";
			final FileInputSplit split = createTempFile(fileContent);	
		
			final CsvInputFormat<Tuple5<Integer, Integer, Integer, Integer, Integer>> format = new CsvInputFormat<Tuple5<Integer, Integer, Integer, Integer, Integer>>(PATH);
			
			format.setFieldDelimiter('|');
			format.setFieldTypes(Integer.class, Integer.class, Integer.class, Integer.class, Integer.class);
			
			format.configure(new Configuration());
			format.open(split);
			
			Tuple5<Integer, Integer, Integer, Integer, Integer> result = new Tuple5<Integer, Integer, Integer, Integer, Integer>();
			
			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals(Integer.valueOf(111), result.f0);
			assertEquals(Integer.valueOf(222), result.f1);
			assertEquals(Integer.valueOf(333), result.f2);
			assertEquals(Integer.valueOf(444), result.f3);
			assertEquals(Integer.valueOf(555), result.f4);
			
			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals(Integer.valueOf(666), result.f0);
			assertEquals(Integer.valueOf(777), result.f1);
			assertEquals(Integer.valueOf(888), result.f2);
			assertEquals(Integer.valueOf(999), result.f3);
			assertEquals(Integer.valueOf(000), result.f4);
			
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
		
			final CsvInputFormat<Tuple2<Integer, Integer>> format = new CsvInputFormat<Tuple2<Integer, Integer>>(PATH);
			
			format.setFieldDelimiter('|');
			format.setFieldTypes(Integer.class, Integer.class);
			
			format.configure(new Configuration());
			format.open(split);
			
			Tuple2<Integer, Integer> result = new Tuple2<Integer, Integer>();
			
			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals(Integer.valueOf(111), result.f0);
			assertEquals(Integer.valueOf(222), result.f1);
			
			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals(Integer.valueOf(666), result.f0);
			assertEquals(Integer.valueOf(777), result.f1);
			
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
			final String fileContent = "111|222|333|444|555|666|777|888|999|000|\n000|999|888|777|666|555|444|333|222|111|";
			final FileInputSplit split = createTempFile(fileContent);	
			
			final CsvInputFormat<Tuple3<Integer, Integer, Integer>> format = new CsvInputFormat<Tuple3<Integer, Integer, Integer>>(PATH);
			
			format.setFieldDelimiter('|');
			format.setFieldTypes(Integer.class, null, null, Integer.class, null, null, null, Integer.class);
			
			format.configure(new Configuration());
			format.open(split);
			
			Tuple3<Integer, Integer, Integer> result = new Tuple3<Integer, Integer, Integer>();
			
			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals(Integer.valueOf(111), result.f0);
			assertEquals(Integer.valueOf(444), result.f1);
			assertEquals(Integer.valueOf(888), result.f2);
			
			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals(Integer.valueOf(000), result.f0);
			assertEquals(Integer.valueOf(777), result.f1);
			assertEquals(Integer.valueOf(333), result.f2);
			
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
			final String fileContent = "111|222|333|444|555|666|777|888|999|000|\n000|999|888|777|666|555|444|333|222|111|";
			final FileInputSplit split = createTempFile(fileContent);	
			
			final CsvInputFormat<Tuple3<Integer, Integer, Integer>> format = new CsvInputFormat<Tuple3<Integer, Integer, Integer>>(PATH);
			
			format.setFieldDelimiter('|');
			
			format.setFields(new int[] {0, 3, 7}, new Class<?>[] {Integer.class, Integer.class, Integer.class});
			
			
			format.configure(new Configuration());
			format.open(split);
			
			Tuple3<Integer, Integer, Integer> result = new Tuple3<Integer, Integer, Integer>();
			
			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals(Integer.valueOf(111), result.f0);
			assertEquals(Integer.valueOf(444), result.f1);
			assertEquals(Integer.valueOf(888), result.f2);
			
			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals(Integer.valueOf(000), result.f0);
			assertEquals(Integer.valueOf(777), result.f1);
			assertEquals(Integer.valueOf(333), result.f2);
			
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
			final String fileContent = "111|222|333|444|555|666|777|888|999|000|\n000|999|888|777|666|555|444|333|222|111|";
			final FileInputSplit split = createTempFile(fileContent);	
			
			final CsvInputFormat<Tuple3<Integer, Integer, Integer>> format = new CsvInputFormat<Tuple3<Integer, Integer, Integer>>(PATH);
			
			format.setFieldDelimiter('|');

			format.setFields(new boolean[] { true, false, false, true, false, false, false, true }, new Class<?>[] { Integer.class,
					Integer.class, Integer.class });
			
			format.configure(new Configuration());
			format.open(split);
			
			Tuple3<Integer, Integer, Integer> result = new Tuple3<Integer, Integer, Integer>();
			
			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals(Integer.valueOf(111), result.f0);
			assertEquals(Integer.valueOf(444), result.f1);
			assertEquals(Integer.valueOf(888), result.f2);
			
			result = format.nextRecord(result);
			assertNotNull(result);
			assertEquals(Integer.valueOf(000), result.f0);
			assertEquals(Integer.valueOf(777), result.f1);
			assertEquals(Integer.valueOf(333), result.f2);
			
			result = format.nextRecord(result);
			assertNull(result);
			assertTrue(format.reachedEnd());
		}
		catch (Exception ex) {
			fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
		}
	}
	
	@Test
	public void testReadSparseWithShuffledPositions() throws IOException {
		try {
			final CsvInputFormat<Tuple3<Integer, Integer, Integer>> format = new CsvInputFormat<Tuple3<Integer, Integer, Integer>>(PATH);
			
			format.setFieldDelimiter('|');
			
			try {
				format.setFields(new int[] {8, 1, 3}, new Class<?>[] {Integer.class, Integer.class, Integer.class});
				fail("Input sequence should have been rejected.");
			}
			catch (IllegalArgumentException e) {
				// that is what we want
			}
		}
		catch (Exception ex) {
			fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
		}
	}

	@Test
	public void testParseStringErrors() throws Exception {
		StringParser stringParser = new StringParser();

		Object[][] failures = {
				{"\"string\" trailing", FieldParser.ParseErrorState.UNQUOTED_CHARS_AFTER_QUOTED_STRING},
				{"\"unterminated ", 		FieldParser.ParseErrorState.UNTERMINATED_QUOTED_STRING}
		};

		for (Object[] failure : failures) {
			String input = (String) failure[0];

			int result = stringParser.parseField(input.getBytes(), 0, input.length(), '|', null);

			assertThat(result, is(-1));
			assertThat(stringParser.getErrorState(), is(failure[1]));
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
				",,\"Venture \"\"Extended Edition\"\"\",\"\",4900.00";

		final FileInputSplit split = createTempFile(fileContent);

		final CsvInputFormat<Tuple5<Integer, String, String, String, Double>> format =
				new CsvInputFormat<Tuple5<Integer, String, String, String, Double>>(PATH);

		format.setSkipFirstLineAsHeader(true);
		format.setFieldDelimiter(',');

		format.setFields(new boolean[] { true, true, true, true, true }, new Class<?>[] {
				Integer.class, String.class, String.class, String.class, Double.class });

		format.configure(new Configuration());
		format.open(split);

		Tuple5<Integer, String, String, String, Double> result = new Tuple5<Integer, String, String, String, Double>();

		@SuppressWarnings("unchecked")
		Tuple5<Integer, String, String, String, Double>[] expectedLines = new Tuple5[] {
				new Tuple5<Integer, String, String, String, Double>(1997, "Ford", "E350", "ac, abs, moon", 3000.0),
				new Tuple5<Integer, String, String, String, Double>(1999, "Chevy", "Venture \"Extended Edition\"", "", 4900.0),
				new Tuple5<Integer, String, String, String, Double>(1996, "Jeep", "Grand Cherokee", "MUST SELL! air, moon roof, loaded", 4799.00),
				new Tuple5<Integer, String, String, String, Double>(1999, "Chevy", "Venture \"Extended Edition, Very Large\"", "", 5000.00	),
				new Tuple5<Integer, String, String, String, Double>(0, "", "Venture \"Extended Edition\"", "", 4900.0)
		};

		try {
			for (Tuple5<Integer, String, String, String, Double> expected : expectedLines) {
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

		OutputStreamWriter wrt = new OutputStreamWriter(
				new FileOutputStream(tempFile), Charsets.UTF_8
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
		File tempFile=null;
		
		String fileContent = CsvInputFormatTest.FIRST_PART + lineBreakerInFile + CsvInputFormatTest.SECOND_PART + lineBreakerInFile;
		
		try {
			// create input file
			tempFile = File.createTempFile("CsvInputFormatTest", "tmp");
			tempFile.deleteOnExit();
			tempFile.setWritable(true);
			
			OutputStreamWriter wrt = new OutputStreamWriter(new FileOutputStream(tempFile));
			wrt.write(fileContent);
			wrt.close();
			
			CsvInputFormat<Tuple1<String>> inputFormat = new CsvInputFormat<Tuple1<String>>(new Path(tempFile.toURI().toString()),String.class);
			
			Configuration parameters = new Configuration(); 
			inputFormat.configure(parameters);
			
			inputFormat.setDelimiter(lineBreakerSetup);
			
			FileInputSplit[] splits = inputFormat.createInputSplits(1);
						
			inputFormat.open(splits[0]);
			
			Tuple1<String> result = inputFormat.nextRecord(new Tuple1<String>());
			
			assertNotNull("Expecting to not return null", result);
			
			
			
			assertEquals(FIRST_PART, result.f0);
			
			result = inputFormat.nextRecord(result);
			
			assertNotNull("Expecting to not return null", result);
			assertEquals(SECOND_PART, result.f0);
			
		}
		catch (Throwable t) {
			System.err.println("test failed with exception: " + t.getMessage());
			t.printStackTrace(System.err);
			fail("Test erroneous");
		}
	}

}
