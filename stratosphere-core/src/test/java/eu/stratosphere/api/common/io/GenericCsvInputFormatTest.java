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

package eu.stratosphere.api.common.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

import junit.framework.Assert;

import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.api.common.io.GenericCsvInputFormat;
import eu.stratosphere.api.common.io.ParseException;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.FileInputSplit;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.LogUtils;

public class GenericCsvInputFormatTest {

	private File tempFile;
	
	private TestCsvInputFormat format;
	
	// --------------------------------------------------------------------------------------------

	@BeforeClass
	public static void initialize() {
		LogUtils.initializeDefaultConsoleLogger(Level.WARN);
	}
	
	@Before
	public void setup() {
		format = new TestCsvInputFormat();
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
	public void testSparseFieldArray() {
		
		@SuppressWarnings("unchecked")
		Class<? extends Value>[] originalTypes = new Class[] { IntValue.class, null, null, StringValue.class, null, DoubleValue.class };
		
		format.setFieldTypes(originalTypes);
		assertEquals(3, format.getNumberOfNonNullFields());
		assertEquals(6, format.getNumberOfFieldsTotal());
		
		assertTrue(Arrays.equals(originalTypes, format.getFieldTypes()));
	}
	
	@Test
	@SuppressWarnings("unchecked")
	public void testReadNoPosAll() throws IOException {
		try {
			final String fileContent = "111|222|333|444|555\n666|777|888|999|000|";
			final FileInputSplit split = createTempFile(fileContent);	
		
			final Configuration parameters = new Configuration();
			
			format.setFieldDelim('|');
			format.setFieldTypes(IntValue.class, IntValue.class, IntValue.class, IntValue.class, IntValue.class);
			
			format.configure(parameters);
			format.open(split);
			
			Value[] values = createIntValues(5);
			
			assertTrue(format.nextRecord(values));
			assertEquals(111, ((IntValue) values[0]).getValue());
			assertEquals(222, ((IntValue) values[1]).getValue());
			assertEquals(333, ((IntValue) values[2]).getValue());
			assertEquals(444, ((IntValue) values[3]).getValue());
			assertEquals(555, ((IntValue) values[4]).getValue());
			
			assertTrue(format.nextRecord(values));
			assertEquals(666, ((IntValue) values[0]).getValue());
			assertEquals(777, ((IntValue) values[1]).getValue());
			assertEquals(888, ((IntValue) values[2]).getValue());
			assertEquals(999, ((IntValue) values[3]).getValue());
			assertEquals(000, ((IntValue) values[4]).getValue());
			
			assertFalse(format.nextRecord(values));
			assertTrue(format.reachedEnd());
		}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testReadNoPosFirstN() throws IOException {
		try {
			final String fileContent = "111|222|333|444|555|\n666|777|888|999|000|";
			final FileInputSplit split = createTempFile(fileContent);	
		
			final Configuration parameters = new Configuration();
			
			format.setFieldDelim('|');
			format.setFieldTypes(IntValue.class, IntValue.class);
			
			format.configure(parameters);
			format.open(split);
			
			Value[] values = createIntValues(2);

			// if this would parse all, we would get an index out of bounds exception
			assertTrue(format.nextRecord(values));
			assertEquals(111, ((IntValue) values[0]).getValue());
			assertEquals(222, ((IntValue) values[1]).getValue());
			
			assertTrue(format.nextRecord(values));
			assertEquals(666, ((IntValue) values[0]).getValue());
			assertEquals(777, ((IntValue) values[1]).getValue());

			
			assertFalse(format.nextRecord(values));
			assertTrue(format.reachedEnd());
		}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
		
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testSparseParse() {
		try {
			final String fileContent = "111|222|333|444|555|666|777|888|999|000|\n000|999|888|777|666|555|444|333|222|111|";
			final FileInputSplit split = createTempFile(fileContent);	
		
			final Configuration parameters = new Configuration();
			
			format.setFieldDelim('|');
			format.setFieldTypes(IntValue.class, null, null, IntValue.class, null, null, null, IntValue.class);
			
			format.configure(parameters);
			format.open(split);
			
			Value[] values = createIntValues(3);
			
			assertTrue(format.nextRecord(values));
			assertEquals(111, ((IntValue) values[0]).getValue());
			assertEquals(444, ((IntValue) values[1]).getValue());
			assertEquals(888, ((IntValue) values[2]).getValue());
			
			assertTrue(format.nextRecord(values));
			assertEquals(000, ((IntValue) values[0]).getValue());
			assertEquals(777, ((IntValue) values[1]).getValue());
			assertEquals(333, ((IntValue) values[2]).getValue());
			
			assertFalse(format.nextRecord(values));
			assertTrue(format.reachedEnd());
		}
		catch (Exception ex) {
			System.err.println(ex.getMessage());
			ex.printStackTrace();
			Assert.fail("Test erroneous");
		}
	}

    @SuppressWarnings("unchecked")
    @Test
    public void testSparseParseWithIndices() {
        try {
            final String fileContent = "111|222|333|444|555|666|777|888|999|000|\n000|999|888|777|666|555|444|333|222|111|";
            final FileInputSplit split = createTempFile(fileContent);

            final Configuration parameters = new Configuration();

            format.setFieldDelim('|');
            format.setFields(new int[]{0, 3, 7},
                    (Class<? extends Value>[]) new Class[]{IntValue.class, IntValue.class, IntValue.class});
            format.configure(parameters);
            format.open(split);

            Value[] values = createIntValues(3);

            assertTrue(format.nextRecord(values));
            assertEquals(111, ((IntValue) values[0]).getValue());
            assertEquals(444, ((IntValue) values[1]).getValue());
            assertEquals(888, ((IntValue) values[2]).getValue());

            assertTrue(format.nextRecord(values));
            assertEquals(000, ((IntValue) values[0]).getValue());
            assertEquals(777, ((IntValue) values[1]).getValue());
            assertEquals(333, ((IntValue) values[2]).getValue());

            assertFalse(format.nextRecord(values));
            assertTrue(format.reachedEnd());
        }
        catch (Exception ex) {
            System.err.println(ex.getMessage());
            ex.printStackTrace();
            Assert.fail("Test erroneous");
        }
    }
	
	@SuppressWarnings("unchecked")
	@Test
	public void testReadTooShortInput() throws IOException {
		try {
			final String fileContent = "111|222|333|444\n666|777|888|999";
			final FileInputSplit split = createTempFile(fileContent);	
		
			final Configuration parameters = new Configuration();
			format.setFieldDelim('|');
			format.setFieldTypes(IntValue.class, IntValue.class, IntValue.class, IntValue.class, IntValue.class);
			
			format.configure(parameters);
			format.open(split);
			
			Value[] values = createIntValues(5);
			
			try {
				format.nextRecord(values);
				Assert.fail("Should have thrown a parse exception on too short input.");
			}
			catch (ParseException e) {
				// all is well
			}
		}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testReadTooShortInputLenient() throws IOException {
		try {
			final String fileContent = "666|777|888|999|555\n111|222|333|444\n666|777|888|999|555";
			final FileInputSplit split = createTempFile(fileContent);	
		
			final Configuration parameters = new Configuration();
			format.setFieldDelim('|');
			format.setFieldTypes(IntValue.class, IntValue.class, IntValue.class, IntValue.class, IntValue.class);
			format.setLenient(true);
			
			format.configure(parameters);
			format.open(split);
			
			Value[] values = createIntValues(5);
			
			Assert.assertTrue(format.nextRecord(values));	// line okay
			Assert.assertFalse(format.nextRecord(values));	// line too short
			Assert.assertTrue(format.nextRecord(values));	// line okay
		}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testReadInvalidContents() throws IOException {
		try {
			final String fileContent = "abc|222|def|444\nkkz|777|888|hhg";
			final FileInputSplit split = createTempFile(fileContent);	
		
			final Configuration parameters = new Configuration();

			format.setFieldDelim('|');
			format.setFieldTypes(StringValue.class, IntValue.class, StringValue.class, IntValue.class);
			
			format.configure(parameters);
			format.open(split);
			
			Value[] values = new Value[] { new StringValue(), new IntValue(), new StringValue(), new IntValue() };
			
			assertTrue(format.nextRecord(values));
			
			try {
				format.nextRecord(values);
				Assert.fail("Input format accepted on invalid input.");
			}
			catch (ParseException e) {
				; // all good
			}
		}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testReadInvalidContentsLenient() {
		try {
			final String fileContent = "abc|222|def|444\nkkz|777|888|hhg";
			final FileInputSplit split = createTempFile(fileContent);	
		
			final Configuration parameters = new Configuration();

			format.setFieldDelim('|');
			format.setFieldTypes(StringValue.class, IntValue.class, StringValue.class, IntValue.class);
			format.setLenient(true);
			
			format.configure(parameters);
			format.open(split);
			
			Value[] values = new Value[] { new StringValue(), new IntValue(), new StringValue(), new IntValue() };
			
			assertTrue(format.nextRecord(values));
			assertFalse(format.nextRecord(values));
		}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void readWithEmptyField() {
		try {
			final String fileContent = "abc|def|ghijk\nabc||hhg\n|||";
			final FileInputSplit split = createTempFile(fileContent);	
		
			final Configuration parameters = new Configuration();

			format.setFieldDelim('|');
			format.setFieldTypes(StringValue.class, StringValue.class, StringValue.class);
			
			format.configure(parameters);
			format.open(split);
			
			Value[] values = new Value[] { new StringValue(), new StringValue(), new StringValue()};
			
			assertTrue(format.nextRecord(values));
			assertEquals("abc", ((StringValue) values[0]).getValue());
			assertEquals("def", ((StringValue) values[1]).getValue());
			assertEquals("ghijk", ((StringValue) values[2]).getValue());
			
			assertTrue(format.nextRecord(values));
			assertEquals("abc", ((StringValue) values[0]).getValue());
			assertEquals("", ((StringValue) values[1]).getValue());
			assertEquals("hhg", ((StringValue) values[2]).getValue());
			
			assertTrue(format.nextRecord(values));
			assertEquals("", ((StringValue) values[0]).getValue());
			assertEquals("", ((StringValue) values[1]).getValue());
			assertEquals("", ((StringValue) values[2]).getValue());
			
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
	
	private final Value[] createIntValues(int num) {
		Value[] v = new Value[num];
		
		for (int i = 0; i < num; i++) {
			v[i] = new IntValue();
		}
		
		return v;
	}
	
	private static final class TestCsvInputFormat extends GenericCsvInputFormat<Value[]> {

		private static final long serialVersionUID = 2653609265252951059L;

		@Override
		public boolean readRecord(Value[] target, byte[] bytes, int offset, int numBytes) {
			return parseRecord(target, bytes, offset, numBytes);
		}
	}
}
