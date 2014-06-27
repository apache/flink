/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.api.java.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.log4j.Level;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.tuple.Tuple5;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.FileInputSplit;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.util.LogUtils;

public class CsvInputFormatTest {
	
	private static final Path PATH = new Path("an/ignored/file/");
	
	//Static variables for testing the removal of \r\n to \n
	private static final String FIRST_PART = "That is the first part";
	
	private static final String SECOND_PART = "That is the second part";
	
	
	@BeforeClass
	public static void initialize() {
		LogUtils.initializeDefaultConsoleLogger(Level.WARN);
	}
	
	@Test
	public void readStringFields() {
		try {
			final String fileContent = "abc|def|ghijk\nabc||hhg\n|||";
			final FileInputSplit split = createInputSplit(fileContent);
			
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
			final FileInputSplit split = createInputSplit(fileContent);
		
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
			final FileInputSplit split = createInputSplit(fileContent);
		
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
			final FileInputSplit split = createInputSplit(fileContent);
		
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
			final FileInputSplit split = createInputSplit(fileContent);
			
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
			final FileInputSplit split = createInputSplit(fileContent);
			
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
			final FileInputSplit split = createInputSplit(fileContent);
			
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

	private FileInputSplit createInputSplit(String content) throws IOException {
		File tempFile = File.createTempFile("test_contents", "tmp");
		tempFile.deleteOnExit();

		FileWriter wrt = new FileWriter(tempFile);
		wrt.write(content);
		wrt.close();

		return new FileInputSplit(0, new Path(tempFile.toURI().toString()), 0, tempFile.length(), new String[] {"localhost"});
	}

}
