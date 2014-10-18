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


package org.apache.flink.api.java.record.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.junit.Assert;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CsvInputFormatTest {
	
	protected File tempFile;
	
	private final CsvInputFormat format = new CsvInputFormat();
	
	//Static variables for testing the removal of \r\n to \n
	private static final String FIRST_PART = "That is the first part";
		
	private static final String SECOND_PART = "That is the second part";
	
	// --------------------------------------------------------------------------------------------
	@Before
	public void setup() {
		format.setFilePath("file:///some/file/that/will/not/be/read");
	}
	
	@After
	public void setdown() throws Exception {
		if (this.format != null) {
			this.format.close();
		}
		if (this.tempFile != null) {
			this.tempFile.delete();
		}
	}

	@Test
	public void testConfigureEmptyConfig() {
		try {
			Configuration config = new Configuration();
			
			// empty configuration, plus no fields on the format itself is not valid
			try {
				format.configure(config);
				fail(); // should give an error
			} catch (IllegalConfigurationException e) {
				; // okay
			}
		}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void readWithEmptyFieldInstanceParameters() {
		try {
			final String fileContent = "abc|def|ghijk\nabc||hhg\n|||";
			final FileInputSplit split = createTempFile(fileContent);	
		
			final Configuration parameters = new Configuration();

			format.setFieldDelimiter('|');
			format.setFieldTypes(StringValue.class, StringValue.class, StringValue.class);
			
			format.configure(parameters);
			format.open(split);
			
			Record record = new Record();
			
			assertNotNull(format.nextRecord(record));
			assertEquals("abc", record.getField(0, StringValue.class).getValue());
			assertEquals("def", record.getField(1, StringValue.class).getValue());
			assertEquals("ghijk", record.getField(2, StringValue.class).getValue());
			
			assertNotNull(format.nextRecord(record));
			assertEquals("abc", record.getField(0, StringValue.class).getValue());
			assertEquals("", record.getField(1, StringValue.class).getValue());
			assertEquals("hhg", record.getField(2, StringValue.class).getValue());
			
			assertNotNull(format.nextRecord(record));
			assertEquals("", record.getField(0, StringValue.class).getValue());
			assertEquals("", record.getField(1, StringValue.class).getValue());
			assertEquals("", record.getField(2, StringValue.class).getValue());
		}
		catch (Exception ex) {
			ex.printStackTrace();
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
	}
	
	@Test
	public void readWithEmptyFieldConfigParameters() {
		try {
			final String fileContent = "abc|def|ghijk\nabc||hhg\n|||";
			final FileInputSplit split = createTempFile(fileContent);	
		
			final Configuration parameters = new Configuration();
			new CsvInputFormat.ConfigBuilder(null, parameters)
				.field(StringValue.class, 0).field(StringValue.class, 1).field(StringValue.class, 2);
			
			format.setFieldDelimiter('|');
			
			format.configure(parameters);
			format.open(split);
			
			Record record = new Record();
			
			assertNotNull(format.nextRecord(record));
			assertEquals("abc", record.getField(0, StringValue.class).getValue());
			assertEquals("def", record.getField(1, StringValue.class).getValue());
			assertEquals("ghijk", record.getField(2, StringValue.class).getValue());
			
			assertNotNull(format.nextRecord(record));
			assertEquals("abc", record.getField(0, StringValue.class).getValue());
			assertEquals("", record.getField(1, StringValue.class).getValue());
			assertEquals("hhg", record.getField(2, StringValue.class).getValue());
			
			assertNotNull(format.nextRecord(record));
			assertEquals("", record.getField(0, StringValue.class).getValue());
			assertEquals("", record.getField(1, StringValue.class).getValue());
			assertEquals("", record.getField(2, StringValue.class).getValue());
		}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
	}
	
	@Test
	public void testReadAll() throws IOException {
		try {
			final String fileContent = "111|222|333|444|555\n666|777|888|999|000|";
			final FileInputSplit split = createTempFile(fileContent);	
		
			final Configuration parameters = new Configuration();
			
			new CsvInputFormat.ConfigBuilder(null, parameters)
				.fieldDelimiter('|')
				.field(IntValue.class, 0).field(IntValue.class, 1).field(IntValue.class, 2)
				.field(IntValue.class, 3).field(IntValue.class, 4);
			
			format.configure(parameters);
			format.open(split);
			
			Record record = new Record();
			
			assertNotNull(format.nextRecord(record));
			assertEquals(111, record.getField(0, IntValue.class).getValue());
			assertEquals(222, record.getField(1, IntValue.class).getValue());
			assertEquals(333, record.getField(2, IntValue.class).getValue());
			assertEquals(444, record.getField(3, IntValue.class).getValue());
			assertEquals(555, record.getField(4, IntValue.class).getValue());
			
			assertNotNull(format.nextRecord(record));
			assertEquals(666, record.getField(0, IntValue.class).getValue());
			assertEquals(777, record.getField(1, IntValue.class).getValue());
			assertEquals(888, record.getField(2, IntValue.class).getValue());
			assertEquals(999, record.getField(3, IntValue.class).getValue());
			assertEquals(000, record.getField(4, IntValue.class).getValue());
			
			assertNull(format.nextRecord(record));
			assertTrue(format.reachedEnd());
		}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
	}
	
	@Test
	public void testReadFirstN() throws IOException {
		try {
			final String fileContent = "111|222|333|444|555|\n666|777|888|999|000|";
			final FileInputSplit split = createTempFile(fileContent);	
		
			final Configuration parameters = new Configuration();
			
			new CsvInputFormat.ConfigBuilder(null, parameters)
			.fieldDelimiter('|')
			.field(IntValue.class, 0).field(IntValue.class, 1);
			
			format.configure(parameters);
			format.open(split);
			
			Record record = new Record();
			
			assertNotNull(format.nextRecord(record));
			assertEquals(111, record.getField(0, IntValue.class).getValue());
			assertEquals(222, record.getField(1, IntValue.class).getValue());
			boolean notParsed = false;
			try {
				record.getField(2, IntValue.class);
			} catch (IndexOutOfBoundsException ioo) {
				notParsed = true;
			}
			assertTrue(notParsed);
			
			assertNotNull(format.nextRecord(record));
			assertEquals(666, record.getField(0, IntValue.class).getValue());
			assertEquals(777, record.getField(1, IntValue.class).getValue());
			notParsed = false;
			try {
				record.getField(2, IntValue.class);
			} catch (IndexOutOfBoundsException ioo) {
				notParsed = true;
			}
			assertTrue(notParsed);
			
			assertNull(format.nextRecord(record));
			assertTrue(format.reachedEnd());
		}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
		
	}
	
	@Test
	public void testReadSparse() throws IOException {
		try {
			final String fileContent = "111|222|333|444|555|666|777|888|999|000|\n000|999|888|777|666|555|444|333|222|111|";
			final FileInputSplit split = createTempFile(fileContent);	
		
			final Configuration parameters = new Configuration();
			
			new CsvInputFormat.ConfigBuilder(null, parameters)
				.fieldDelimiter('|')
				.field(IntValue.class, 0).field(IntValue.class, 3).field(IntValue.class, 7);
			
			format.configure(parameters);
			format.open(split);
			
			Record record = new Record();
			
			assertNotNull(format.nextRecord(record));
			assertEquals(111, record.getField(0, IntValue.class).getValue());
			assertEquals(444, record.getField(1, IntValue.class).getValue());
			assertEquals(888, record.getField(2, IntValue.class).getValue());
			
			assertNotNull(format.nextRecord(record));
			assertEquals(000, record.getField(0, IntValue.class).getValue());
			assertEquals(777, record.getField(1, IntValue.class).getValue());
			assertEquals(333, record.getField(2, IntValue.class).getValue());
			
			assertNull(format.nextRecord(record));
			assertTrue(format.reachedEnd());
		}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
	}
	
	@Test
	public void testReadSparseShufflePosition() throws IOException {
		try {
			final String fileContent = "111|222|333|444|555|666|777|888|999|000|\n000|999|888|777|666|555|444|333|222|111|";
			final FileInputSplit split = createTempFile(fileContent);	
		
			final Configuration parameters = new Configuration();
			
			new CsvInputFormat.ConfigBuilder(null, parameters)
				.fieldDelimiter('|')
				.field(IntValue.class, 8).field(IntValue.class, 1).field(IntValue.class, 3);
			
			format.configure(parameters);
			format.open(split);
			
			Record record = new Record();
			
			assertNotNull(format.nextRecord(record));
			assertEquals(999, record.getField(0, IntValue.class).getValue());
			assertEquals(222, record.getField(1, IntValue.class).getValue());
			assertEquals(444, record.getField(2, IntValue.class).getValue());
			
			assertNotNull(format.nextRecord(record));
			assertEquals(222, record.getField(0, IntValue.class).getValue());
			assertEquals(999, record.getField(1, IntValue.class).getValue());
			assertEquals(777, record.getField(2, IntValue.class).getValue());
			
			assertNull(format.nextRecord(record));
			assertTrue(format.reachedEnd());
		}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
	}
	
	private FileInputSplit createTempFile(String content) throws IOException {
		this.tempFile = File.createTempFile("test_contents", "tmp");
		this.tempFile.deleteOnExit();
		
		DataOutputStream dos = new DataOutputStream(new FileOutputStream(tempFile));
		dos.writeBytes(content);
		dos.close();
			
		return new FileInputSplit(0, new Path(this.tempFile.toURI().toString()), 0, this.tempFile.length(), new String[] {"localhost"});
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
			
			//Instantiate input format
			CsvInputFormat inputFormat = new CsvInputFormat();
			
			Configuration parameters = new Configuration();
			new CsvInputFormat.ConfigBuilder(null, parameters)
			.field(StringValue.class, 0).filePath(tempFile.toURI().toString());
			
			
			inputFormat.configure(parameters);
			
			inputFormat.setDelimiter(lineBreakerSetup);
			
			FileInputSplit[] splits = inputFormat.createInputSplits(1);
						
			inputFormat.open(splits[0]);
			
			Record record = new Record();
			
			Record result = inputFormat.nextRecord(record);
			
			assertNotNull("Expecting to not return null", result);
			
			
			
			assertEquals(FIRST_PART, result.getField(0, StringValue.class).getValue());
			
			result = inputFormat.nextRecord(record);
			
			assertNotNull("Expecting to not return null", result);
			assertEquals(SECOND_PART, result.getField(0, StringValue.class).getValue());
			
		}
		catch (Throwable t) {
			System.err.println("test failed with exception: " + t.getMessage());
			t.printStackTrace(System.err);
			fail("Test erroneous");
		}
	}

}
