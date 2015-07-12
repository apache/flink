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


package org.apache.flink.api.common.io;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DelimitedInputFormatTest {
	
	protected Configuration config;
	
	protected File tempFile;
	
	private final DelimitedInputFormat<String> format = new MyTextInputFormat();
	
	// --------------------------------------------------------------------------------------------

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
		
		DelimitedInputFormat<String> format = new MyTextInputFormat();
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
		DelimitedInputFormat<String> deserialized = (DelimitedInputFormat<String>) ois.readObject();
		
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

	/**
	 * Tests simple delimited parsing with a custom delimiter.
	 */
	@Test
	public void testRead() {
		try {
			final String myString = "my key|my val$$$my key2\n$$ctd.$$|my value2";
			final FileInputSplit split = createTempFile(myString);
			
			final Configuration parameters = new Configuration();
			
			format.setDelimiter("$$$");
			format.configure(parameters);
			format.open(split);
	
			String first = format.nextRecord(null);
			assertNotNull(first);
			assertEquals("my key|my val", first);

			String second = format.nextRecord(null);
			assertNotNull(second);
			assertEquals("my key2\n$$ctd.$$|my value2", second);
			
			assertNull(format.nextRecord(null));
			assertTrue(format.reachedEnd());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testRead2() throws IOException {
		// 2. test case
		final String myString = "my key|my val$$$my key2\n$$ctd.$$|my value2\n";
		final FileInputSplit split = createTempFile(myString);
		
		final Configuration parameters = new Configuration();
		// default delimiter = '\n'
		
		format.configure(parameters);
		format.open(split);

		String first = format.nextRecord(null);
		String second = format.nextRecord(null);
		
		assertNotNull(first);
		assertNotNull(second);
		
		assertEquals("my key|my val$$$my key2", first);
		assertEquals("$$ctd.$$|my value2", second);
		
		assertNull(format.nextRecord(null));
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
	
	
	protected static final class MyTextInputFormat extends DelimitedInputFormat<String> {
		private static final long serialVersionUID = 1L;
		
		@Override
		public String readRecord(String reuse, byte[] bytes, int offset, int numBytes) {
			return new String(bytes, offset, numBytes);
		}
	}
}
