/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.common.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.configuration.IllegalConfigurationException;
import eu.stratosphere.core.fs.FileInputSplit;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.types.PactInteger;
import eu.stratosphere.types.PactRecord;
import eu.stratosphere.types.PactString;
import eu.stratosphere.util.LogUtils;

public class RecordInputFormatTest {
	
	protected File tempFile;
	
	private final RecordInputFormat format = new RecordInputFormat();
	
	// --------------------------------------------------------------------------------------------

	@BeforeClass
	public static void initialize() {
		LogUtils.initializeDefaultConsoleLogger(Level.WARN);
	}
	
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

			format.setFieldDelim('|');
			format.setFieldTypes(PactString.class, PactString.class, PactString.class);
			
			format.configure(parameters);
			format.open(split);
			
			PactRecord record = new PactRecord();
			
			assertTrue(format.nextRecord(record));
			assertEquals("abc", record.getField(0, PactString.class).getValue());
			assertEquals("def", record.getField(1, PactString.class).getValue());
			assertEquals("ghijk", record.getField(2, PactString.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals("abc", record.getField(0, PactString.class).getValue());
			assertEquals("", record.getField(1, PactString.class).getValue());
			assertEquals("hhg", record.getField(2, PactString.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals("", record.getField(0, PactString.class).getValue());
			assertEquals("", record.getField(1, PactString.class).getValue());
			assertEquals("", record.getField(2, PactString.class).getValue());
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
			new RecordInputFormat.ConfigBuilder(null, parameters)
				.field(PactString.class, 0).field(PactString.class, 1).field(PactString.class, 2);
			
			format.setFieldDelim('|');
			
			format.configure(parameters);
			format.open(split);
			
			PactRecord record = new PactRecord();
			
			assertTrue(format.nextRecord(record));
			assertEquals("abc", record.getField(0, PactString.class).getValue());
			assertEquals("def", record.getField(1, PactString.class).getValue());
			assertEquals("ghijk", record.getField(2, PactString.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals("abc", record.getField(0, PactString.class).getValue());
			assertEquals("", record.getField(1, PactString.class).getValue());
			assertEquals("hhg", record.getField(2, PactString.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals("", record.getField(0, PactString.class).getValue());
			assertEquals("", record.getField(1, PactString.class).getValue());
			assertEquals("", record.getField(2, PactString.class).getValue());
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
			
			new RecordInputFormat.ConfigBuilder(null, parameters)
				.fieldDelimiter('|')
				.field(PactInteger.class, 0).field(PactInteger.class, 1).field(PactInteger.class, 2)
				.field(PactInteger.class, 3).field(PactInteger.class, 4);
			
			format.configure(parameters);
			format.open(split);
			
			PactRecord record = new PactRecord();
			
			assertTrue(format.nextRecord(record));
			assertEquals(111, record.getField(0, PactInteger.class).getValue());
			assertEquals(222, record.getField(1, PactInteger.class).getValue());
			assertEquals(333, record.getField(2, PactInteger.class).getValue());
			assertEquals(444, record.getField(3, PactInteger.class).getValue());
			assertEquals(555, record.getField(4, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(666, record.getField(0, PactInteger.class).getValue());
			assertEquals(777, record.getField(1, PactInteger.class).getValue());
			assertEquals(888, record.getField(2, PactInteger.class).getValue());
			assertEquals(999, record.getField(3, PactInteger.class).getValue());
			assertEquals(000, record.getField(4, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
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
			
			new RecordInputFormat.ConfigBuilder(null, parameters)
			.fieldDelimiter('|')
			.field(PactInteger.class, 0).field(PactInteger.class, 1);
			
			format.configure(parameters);
			format.open(split);
			
			PactRecord record = new PactRecord();
			
			assertTrue(format.nextRecord(record));
			assertEquals(111, record.getField(0, PactInteger.class).getValue());
			assertEquals(222, record.getField(1, PactInteger.class).getValue());
			boolean notParsed = false;
			try {
				record.getField(2, PactInteger.class);
			} catch (IndexOutOfBoundsException ioo) {
				notParsed = true;
			}
			assertTrue(notParsed);
			
			assertTrue(format.nextRecord(record));
			assertEquals(666, record.getField(0, PactInteger.class).getValue());
			assertEquals(777, record.getField(1, PactInteger.class).getValue());
			notParsed = false;
			try {
				record.getField(2, PactInteger.class);
			} catch (IndexOutOfBoundsException ioo) {
				notParsed = true;
			}
			assertTrue(notParsed);
			
			assertFalse(format.nextRecord(record));
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
			
			new RecordInputFormat.ConfigBuilder(null, parameters)
				.fieldDelimiter('|')
				.field(PactInteger.class, 0).field(PactInteger.class, 3).field(PactInteger.class, 7);
			
			format.configure(parameters);
			format.open(split);
			
			PactRecord record = new PactRecord();
			
			assertTrue(format.nextRecord(record));
			assertEquals(111, record.getField(0, PactInteger.class).getValue());
			assertEquals(444, record.getField(1, PactInteger.class).getValue());
			assertEquals(888, record.getField(2, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(000, record.getField(0, PactInteger.class).getValue());
			assertEquals(777, record.getField(1, PactInteger.class).getValue());
			assertEquals(333, record.getField(2, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
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
			
			new RecordInputFormat.ConfigBuilder(null, parameters)
				.fieldDelimiter('|')
				.field(PactInteger.class, 8).field(PactInteger.class, 1).field(PactInteger.class, 3);
			
			format.configure(parameters);
			format.open(split);
			
			PactRecord record = new PactRecord();
			
			assertTrue(format.nextRecord(record));
			assertEquals(999, record.getField(0, PactInteger.class).getValue());
			assertEquals(222, record.getField(1, PactInteger.class).getValue());
			assertEquals(444, record.getField(2, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(222, record.getField(0, PactInteger.class).getValue());
			assertEquals(999, record.getField(1, PactInteger.class).getValue());
			assertEquals(777, record.getField(2, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
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

}
