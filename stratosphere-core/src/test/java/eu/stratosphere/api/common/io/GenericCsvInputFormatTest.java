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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.FileInputSplit;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.LongValue;
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
		
		format.setFieldTypesGeneric(originalTypes);
		assertEquals(3, format.getNumberOfNonNullFields());
		assertEquals(6, format.getNumberOfFieldsTotal());
		
		assertTrue(Arrays.equals(originalTypes, format.getGenericFieldTypes()));
	}
	
	@Test
	public void testReadNoPosAll() throws IOException {
		try {
			final String fileContent = "111|222|333|444|555\n666|777|888|999|000|";
			final FileInputSplit split = createTempFile(fileContent);	
		
			final Configuration parameters = new Configuration();
			
			format.setFieldDelimiter('|');
			format.setFieldTypesGeneric(IntValue.class, IntValue.class, IntValue.class, IntValue.class, IntValue.class);
			
			format.configure(parameters);
			format.open(split);
			
			Value[] values = createIntValues(5);
			
			values = format.nextRecord(values);
			assertNotNull(values);
			assertEquals(111, ((IntValue) values[0]).getValue());
			assertEquals(222, ((IntValue) values[1]).getValue());
			assertEquals(333, ((IntValue) values[2]).getValue());
			assertEquals(444, ((IntValue) values[3]).getValue());
			assertEquals(555, ((IntValue) values[4]).getValue());
			
			values = format.nextRecord(values);
			assertNotNull(values);
			assertEquals(666, ((IntValue) values[0]).getValue());
			assertEquals(777, ((IntValue) values[1]).getValue());
			assertEquals(888, ((IntValue) values[2]).getValue());
			assertEquals(999, ((IntValue) values[3]).getValue());
			assertEquals(000, ((IntValue) values[4]).getValue());
			
			assertNull(format.nextRecord(values));
			assertTrue(format.reachedEnd());
		}
		catch (Exception ex) {
			fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
	}
	
	@Test
	public void testReadNoPosFirstN() throws IOException {
		try {
			final String fileContent = "111|222|333|444|555|\n666|777|888|999|000|";
			final FileInputSplit split = createTempFile(fileContent);	
		
			final Configuration parameters = new Configuration();
			
			format.setFieldDelimiter('|');
			format.setFieldTypesGeneric(IntValue.class, IntValue.class);
			
			format.configure(parameters);
			format.open(split);
			
			Value[] values = createIntValues(2);

			// if this would parse all, we would get an index out of bounds exception
			values = format.nextRecord(values);
			assertNotNull(values);
			assertEquals(111, ((IntValue) values[0]).getValue());
			assertEquals(222, ((IntValue) values[1]).getValue());
			
			values = format.nextRecord(values);
			assertNotNull(values);
			assertEquals(666, ((IntValue) values[0]).getValue());
			assertEquals(777, ((IntValue) values[1]).getValue());

			
			assertNull(format.nextRecord(values));
			assertTrue(format.reachedEnd());
		}
		catch (Exception ex) {
			fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
		
	}
	
	@Test
	public void testSparseParse() {
		try {
			final String fileContent = "111|222|333|444|555|666|777|888|999|000|\n000|999|888|777|666|555|444|333|222|111|";
			final FileInputSplit split = createTempFile(fileContent);	
		
			final Configuration parameters = new Configuration();
			
			format.setFieldDelimiter('|');
			format.setFieldTypesGeneric(IntValue.class, null, null, IntValue.class, null, null, null, IntValue.class);
			
			format.configure(parameters);
			format.open(split);
			
			Value[] values = createIntValues(3);
			
			values = format.nextRecord(values);
			assertNotNull(values);
			assertEquals(111, ((IntValue) values[0]).getValue());
			assertEquals(444, ((IntValue) values[1]).getValue());
			assertEquals(888, ((IntValue) values[2]).getValue());
			
			values = format.nextRecord(values);
			assertNotNull(values);
			assertEquals(000, ((IntValue) values[0]).getValue());
			assertEquals(777, ((IntValue) values[1]).getValue());
			assertEquals(333, ((IntValue) values[2]).getValue());
			
			assertNull(format.nextRecord(values));
			assertTrue(format.reachedEnd());
		}
		catch (Exception ex) {
			System.err.println(ex.getMessage());
			ex.printStackTrace();
			fail("Test erroneous");
		}
	}

	@Test
	public void testLongLongLong() {
		try {
			final String fileContent = "1,2,3\n3,2,1";
			final FileInputSplit split = createTempFile(fileContent);

			final Configuration parameters = new Configuration();

			format.setFieldDelimiter(',');
			format.setFieldTypesGeneric(LongValue.class, LongValue.class, LongValue.class);
			format.configure(parameters);
			format.open(split);

			Value[] values = createLongValues(3);

			values = format.nextRecord(values);
			assertNotNull(values);
			assertEquals(1L, ((LongValue) values[0]).getValue());
			assertEquals(2L, ((LongValue) values[1]).getValue());
			assertEquals(3L, ((LongValue) values[2]).getValue());
			
			values = format.nextRecord(values);
			assertNotNull(values);
			assertEquals(3L, ((LongValue) values[0]).getValue());
			assertEquals(2L, ((LongValue) values[1]).getValue());
			assertEquals(1L, ((LongValue) values[2]).getValue());

			assertNull(format.nextRecord(values));
			assertTrue(format.reachedEnd());
		} catch (Exception ex) {
			System.err.println(ex.getMessage());
			ex.printStackTrace();
			fail("Test erroneous");
		}
	}
	
	
	@SuppressWarnings("unchecked")
	@Test
	public void testSparseParseWithIndices() {
		try {
			final String fileContent = "111|222|333|444|555|666|777|888|999|000|\n000|999|888|777|666|555|444|333|222|111|";
			final FileInputSplit split = createTempFile(fileContent);

			final Configuration parameters = new Configuration();

			format.setFieldDelimiter('|');
			format.setFieldsGeneric(new int[] { 0, 3, 7 },
				(Class<? extends Value>[]) new Class[] { IntValue.class, IntValue.class, IntValue.class });
			format.configure(parameters);
			format.open(split);

			Value[] values = createIntValues(3);

			values = format.nextRecord(values);
			assertNotNull(values);
			assertEquals(111, ((IntValue) values[0]).getValue());
			assertEquals(444, ((IntValue) values[1]).getValue());
			assertEquals(888, ((IntValue) values[2]).getValue());

			values = format.nextRecord(values);
			assertNotNull(values);
			assertEquals(000, ((IntValue) values[0]).getValue());
			assertEquals(777, ((IntValue) values[1]).getValue());
			assertEquals(333, ((IntValue) values[2]).getValue());

			assertNull(format.nextRecord(values));
			assertTrue(format.reachedEnd());
		} catch (Exception ex) {
			System.err.println(ex.getMessage());
			ex.printStackTrace();
			fail("Test erroneous");
		}
	}
	
	@Test
	public void testReadTooShortInput() throws IOException {
		try {
			final String fileContent = "111|222|333|444\n666|777|888|999";
			final FileInputSplit split = createTempFile(fileContent);	
		
			final Configuration parameters = new Configuration();
			format.setFieldDelimiter('|');
			format.setFieldTypesGeneric(IntValue.class, IntValue.class, IntValue.class, IntValue.class, IntValue.class);
			
			format.configure(parameters);
			format.open(split);
			
			Value[] values = createIntValues(5);
			
			try {
				format.nextRecord(values);
				fail("Should have thrown a parse exception on too short input.");
			}
			catch (ParseException e) {
				// all is well
			}
		}
		catch (Exception ex) {
			fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
	}
	
	@Test
	public void testReadTooShortInputLenient() throws IOException {
		try {
			final String fileContent = "666|777|888|999|555\n111|222|333|444\n666|777|888|999|555";
			final FileInputSplit split = createTempFile(fileContent);	
		
			final Configuration parameters = new Configuration();
			format.setFieldDelimiter('|');
			format.setFieldTypesGeneric(IntValue.class, IntValue.class, IntValue.class, IntValue.class, IntValue.class);
			format.setLenient(true);
			
			format.configure(parameters);
			format.open(split);
			
			Value[] values = createIntValues(5);
			
			assertNotNull(format.nextRecord(values));	// line okay
			assertNull(format.nextRecord(values));	// line too short
			assertNotNull(format.nextRecord(values));	// line okay
		}
		catch (Exception ex) {
			fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
	}
	
	@Test
	public void testReadInvalidContents() throws IOException {
		try {
			final String fileContent = "abc|222|def|444\nkkz|777|888|hhg";
			final FileInputSplit split = createTempFile(fileContent);
		
			final Configuration parameters = new Configuration();

			format.setFieldDelimiter('|');
			format.setFieldTypesGeneric(StringValue.class, IntValue.class, StringValue.class, IntValue.class);
			
			format.configure(parameters);
			format.open(split);
			
			Value[] values = new Value[] { new StringValue(), new IntValue(), new StringValue(), new IntValue() };
			
			assertNotNull(format.nextRecord(values));
			
			try {
				format.nextRecord(values);
				fail("Input format accepted on invalid input.");
			}
			catch (ParseException e) {
				; // all good
			}
		}
		catch (Exception ex) {
			fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
	}
	
	@Test
	public void testReadInvalidContentsLenient() {
		try {
			final String fileContent = "abc|222|def|444\nkkz|777|888|hhg";
			final FileInputSplit split = createTempFile(fileContent);	
		
			final Configuration parameters = new Configuration();

			format.setFieldDelimiter('|');
			format.setFieldTypesGeneric(StringValue.class, IntValue.class, StringValue.class, IntValue.class);
			format.setLenient(true);
			
			format.configure(parameters);
			format.open(split);
			
			Value[] values = new Value[] { new StringValue(), new IntValue(), new StringValue(), new IntValue() };
			
			assertNotNull(format.nextRecord(values));
			assertNull(format.nextRecord(values));
		}
		catch (Exception ex) {
			fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
	}
	
	@Test
	public void testReadInvalidContentsLenientWithSkipping() {
		try {
			final String fileContent = "abc|dfgsdf|777|444\n" +  // good line
									"kkz|777|foobar|hhg\n" +  // wrong data type in field
									"kkz|777foobarhhg  \n" +  // too short, a skipped field never ends
									"xyx|ignored|42|\n";      // another good line
			final FileInputSplit split = createTempFile(fileContent);	
		
			final Configuration parameters = new Configuration();

			format.setFieldDelimiter('|');
			format.setFieldTypesGeneric(StringValue.class, null, IntValue.class);
			format.setLenient(true);
			
			format.configure(parameters);
			format.open(split);
			
			Value[] values = new Value[] { new StringValue(), new IntValue()};
			
			assertNotNull(format.nextRecord(values));
			assertNull(format.nextRecord(values));
			assertNull(format.nextRecord(values));
			assertNotNull(format.nextRecord(values));
		}
		catch (Exception ex) {
			fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
	}
	
	@Test
	public void readWithEmptyField() {
		try {
			final String fileContent = "abc|def|ghijk\nabc||hhg\n|||";
			final FileInputSplit split = createTempFile(fileContent);	
		
			final Configuration parameters = new Configuration();

			format.setFieldDelimiter('|');
			format.setFieldTypesGeneric(StringValue.class, StringValue.class, StringValue.class);
			
			format.configure(parameters);
			format.open(split);
			
			Value[] values = new Value[] { new StringValue(), new StringValue(), new StringValue()};
			
			values = format.nextRecord(values);
			assertNotNull(values);
			assertEquals("abc", ((StringValue) values[0]).getValue());
			assertEquals("def", ((StringValue) values[1]).getValue());
			assertEquals("ghijk", ((StringValue) values[2]).getValue());
			
			values = format.nextRecord(values);
			assertNotNull(values);
			assertEquals("abc", ((StringValue) values[0]).getValue());
			assertEquals("", ((StringValue) values[1]).getValue());
			assertEquals("hhg", ((StringValue) values[2]).getValue());
			
			values = format.nextRecord(values);
			assertNotNull(values);
			assertEquals("", ((StringValue) values[0]).getValue());
			assertEquals("", ((StringValue) values[1]).getValue());
			assertEquals("", ((StringValue) values[2]).getValue());
			
		}
		catch (Exception ex) {
			fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
	}
	
	@Test
	public void readWithHeaderLine() {
		try {
			final String fileContent = "colname-1|colname-2|some name 3|column four|\n" +
									"123|abc|456|def|\n"+
									"987|xyz|654|pqr|\n";
			
			final FileInputSplit split = createTempFile(fileContent);
		
			final Configuration parameters = new Configuration();

			format.setFieldDelimiter('|');
			format.setFieldTypesGeneric(IntValue.class, StringValue.class, IntValue.class, StringValue.class);
			format.setSkipFirstLineAsHeader(true);
			
			format.configure(parameters);
			format.open(split);
			
			Value[] values = new Value[] { new IntValue(), new StringValue(), new IntValue(), new StringValue()};
			
			// first line is skipped as header
			assertNotNull(format.nextRecord(values));   //  first row (= second line)
			assertNotNull(format.nextRecord(values));   // second row (= third line) 
			assertNull(format.nextRecord(values));  // exhausted
			assertTrue(format.reachedEnd());         // exhausted
		}
		catch (Exception ex) {
			fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
	}
	
	@Test
	public void readWithHeaderLineAndInvalidIntermediate() {
		try {
			final String fileContent = "colname-1|colname-2|some name 3|column four|\n" +
									"123|abc|456|def|\n"+
									"colname-1|colname-2|some name 3|column four|\n" +  // repeated header in the middle
									"987|xyz|654|pqr|\n";
			
			final FileInputSplit split = createTempFile(fileContent);
		
			final Configuration parameters = new Configuration();

			format.setFieldDelimiter('|');
			format.setFieldTypesGeneric(IntValue.class, StringValue.class, IntValue.class, StringValue.class);
			format.setSkipFirstLineAsHeader(true);
			
			format.configure(parameters);
			format.open(split);
			
			Value[] values = new Value[] { new IntValue(), new StringValue(), new IntValue(), new StringValue()};
			
			// first line is skipped as header
			assertNotNull(format.nextRecord(values));   //  first row (= second line)
			
			try {
				format.nextRecord(values);
				fail("Format accepted invalid line.");
			}
			catch (ParseException e) {
				// as we expected
			}
		}
		catch (Exception ex) {
			fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
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
	
	private final Value[] createLongValues(int num) {
		Value[] v = new Value[num];
		
		for (int i = 0; i < num; i++) {
			v[i] = new LongValue();
		}
		
		return v;
	}
	
	private static final class TestCsvInputFormat extends GenericCsvInputFormat<Value[]> {

		private static final long serialVersionUID = 2653609265252951059L;

		@Override
		public Value[] readRecord(Value[] target, byte[] bytes, int offset, int numBytes) {
			return parseRecord(target, bytes, offset, numBytes) ? target : null;
		}
	}
}
