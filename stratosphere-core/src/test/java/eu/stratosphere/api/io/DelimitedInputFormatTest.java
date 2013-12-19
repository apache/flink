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

package eu.stratosphere.api.io;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;

import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.api.io.DelimitedInputFormat;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.FileInputSplit;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.LogUtils;

public class DelimitedInputFormatTest {
	
	protected Configuration config;
	
	protected File tempFile;
	
	private final DelimitedInputFormat<Record> format = new MyTextInputFormat();
	
	// --------------------------------------------------------------------------------------------
	
	@BeforeClass
	public static void initialize() {
		LogUtils.initializeDefaultConsoleLogger(Level.WARN);
	}
	
	@Before
	public void setup() {
		this.format.setFilePath(new Path("file:///some/file/that/will/not/be/read"));
		this.config = new Configuration();
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

	// --------------------------------------------------------------------------------------------
	// --------------------------------------------------------------------------------------------
	@Test
	public void testConfigure() {
		this.config.setString("delimited-format.delimiter", "\n");
		
		format.configure(this.config);
		assertEquals("\n", new String(format.getDelimiter()));

		this.config.setString("delimited-format.delimiter", "&-&");
		format.configure(this.config);
		assertEquals("&-&", new String(format.getDelimiter()));
	}
	
	@Test
	public void testSerialization() throws Exception {
		final byte[] DELIMITER = new byte[] {1, 2, 3, 4};
		final int NUM_LINE_SAMPLES = 7;
		final int LINE_LENGTH_LIMIT = 12345;
		final int BUFFER_SIZE = 178;
		
		DelimitedInputFormat<Record> format = new MyTextInputFormat();
		format.setDelimiter(DELIMITER);
		format.setNumLineSamples(NUM_LINE_SAMPLES);
		format.setLineLengthLimit(LINE_LENGTH_LIMIT);
		format.setBufferSize(BUFFER_SIZE);
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(format);
		oos.flush();
		oos.close();
		
		ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
		@SuppressWarnings("unchecked")
		DelimitedInputFormat<Record> deserialized = (DelimitedInputFormat<Record>) ois.readObject();
		
		assertEquals(NUM_LINE_SAMPLES, deserialized.getNumLineSamples());
		assertEquals(LINE_LENGTH_LIMIT, deserialized.getLineLengthLimit());
		assertEquals(BUFFER_SIZE, deserialized.getBufferSize());
		assertArrayEquals(DELIMITER, deserialized.getDelimiter());
	}

	@Test
	public void testOpen() throws IOException {
		final String myString = "my mocked line 1\nmy mocked line 2\n";
		final FileInputSplit split = createTempFile(myString);	
		
		int bufferSize = 5;
		format.setBufferSize(bufferSize);
		format.open(split);
		assertEquals(0, format.splitStart);
		assertEquals(myString.length() - bufferSize, format.splitLength);
		assertEquals(bufferSize, format.getBufferSize());
	}

	@Test
	public void testRead() throws IOException {
		final String myString = "my key|my val$$$my key2\n$$ctd.$$|my value2";
		final FileInputSplit split = createTempFile(myString);
		
		final Configuration parameters = new Configuration();
		
		format.setDelimiter("$$$");
		format.configure(parameters);
		format.open(split);
		
		Record theRecord = new Record();

		assertTrue(format.nextRecord(theRecord));
		assertEquals("my key", theRecord.getField(0, StringValue.class).getValue());
		assertEquals("my val", theRecord.getField(1, StringValue.class).getValue());
		
		assertTrue(format.nextRecord(theRecord));
		assertEquals("my key2\n$$ctd.$$", theRecord.getField(0, StringValue.class).getValue());
		assertEquals("my value2", theRecord.getField(1, StringValue.class).getValue());
		
		assertFalse(format.nextRecord(theRecord));
		assertTrue(format.reachedEnd());
	}
	
	@Test
	public void testRead2() throws IOException {
		// 2. test case
		final String myString = "my key|my val$$$my key2\n$$ctd.$$|my value2";
		final FileInputSplit split = createTempFile(myString);
		
		final Configuration parameters = new Configuration();
		// default delimiter = '\n'
		
		format.configure(parameters);
		format.open(split);

		Record theRecord = new Record();

		assertTrue(format.nextRecord(theRecord));
		assertEquals("my key", theRecord.getField(0, StringValue.class).getValue());
		assertEquals("my val$$$my key2", theRecord.getField(1, StringValue.class).getValue());
		
		assertTrue(format.nextRecord(theRecord));
		assertEquals("$$ctd.$$", theRecord.getField(0, StringValue.class).getValue());
		assertEquals("my value2", theRecord.getField(1, StringValue.class).getValue());
		
		assertFalse(format.nextRecord(theRecord));
		assertTrue(format.reachedEnd());
	}
	
	private FileInputSplit createTempFile(String contents) throws IOException {
		this.tempFile = File.createTempFile("test_contents", "tmp");
		this.tempFile.deleteOnExit();
		
		OutputStreamWriter wrt = new OutputStreamWriter(new FileOutputStream(this.tempFile));
		wrt.write(contents);
		wrt.close();
		
		return new FileInputSplit(0, new Path(this.tempFile.toURI().toString()), 0, this.tempFile.length(), new String[] {"localhost"});
	}
	
	protected static final class MyTextInputFormat extends eu.stratosphere.api.io.DelimitedInputFormat<Record> {
		private static final long serialVersionUID = 1L;
		
		private final StringValue str1 = new StringValue();
		private final StringValue str2 = new StringValue();
		
		@Override
		public boolean readRecord(Record target, byte[] bytes, int offset, int numBytes) {
			String theRecord = new String(bytes, offset, numBytes);
			
			str1.setValue(theRecord.substring(0, theRecord.indexOf('|')));
			str2.setValue(theRecord.substring(theRecord.indexOf('|') + 1));
			
			target.setField(0, str1);
			target.setField(1, str2);
			return true;
		}
	}
}
