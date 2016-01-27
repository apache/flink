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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DelimitedInputFormatTest {
	
	private final DelimitedInputFormat<String> format = new MyTextInputFormat();
	
	// --------------------------------------------------------------------------------------------

	@Before
	public void setup() {
		this.format.setFilePath(new Path("file:///some/file/that/will/not/be/read"));
	}
	
	@After
	public void setdown() throws Exception {
		if (this.format != null) {
			this.format.close();
		}
	}

	// --------------------------------------------------------------------------------------------
	// --------------------------------------------------------------------------------------------
	@Test
	public void testConfigure() {
		Configuration cfg = new Configuration();
		cfg.setString("delimited-format.delimiter", "\n");
		
		format.configure(cfg);
		assertEquals("\n", new String(format.getDelimiter()));

		cfg.setString("delimited-format.delimiter", "&-&");
		format.configure(cfg);
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

	@Test
	public void testReadWithoutTrailingDelimiter() throws IOException {
		// 2. test case
		final String myString = "my key|my val$$$my key2\n$$ctd.$$|my value2";
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
	
	@Test
	public void testReadWithTrailingDelimiter() throws IOException {
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
	
	@Test
	public void testReadCustomDelimiter() {
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

	/**
	 * Tests that the records are read correctly when the split boundary is in the middle of a record.
	 */
	@Test
	public void testReadOverSplitBoundariesUnaligned() {
		try {
			final String myString = "value1\nvalue2\nvalue3";
			final FileInputSplit split = createTempFile(myString);
			
			FileInputSplit split1 = new FileInputSplit(0, split.getPath(), 0, split.getLength() / 2, split.getHostnames());
			FileInputSplit split2 = new FileInputSplit(1, split.getPath(), split1.getLength(), split.getLength(), split.getHostnames());

			final Configuration parameters = new Configuration();
			
			format.configure(parameters);
			format.open(split1);
			
			assertEquals("value1", format.nextRecord(null));
			assertEquals("value2", format.nextRecord(null));
			assertNull(format.nextRecord(null));
			assertTrue(format.reachedEnd());
			
			format.close();
			format.open(split2);

			assertEquals("value3", format.nextRecord(null));
			assertNull(format.nextRecord(null));
			assertTrue(format.reachedEnd());
			
			format.close();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * Tests that the correct number of records is read when the split boundary is exact at the record boundary.
	 */
	@Test
	public void testReadWithBufferSizeIsMultple() {
		try {
			final String myString = "aaaaaaa\nbbbbbbb\nccccccc\nddddddd\n";
			final FileInputSplit split = createTempFile(myString);

			FileInputSplit split1 = new FileInputSplit(0, split.getPath(), 0, split.getLength() / 2, split.getHostnames());
			FileInputSplit split2 = new FileInputSplit(1, split.getPath(), split1.getLength(), split.getLength(), split.getHostnames());

			final Configuration parameters = new Configuration();

			format.setBufferSize(2 * ((int) split1.getLength()));
			format.configure(parameters);

			String next;
			int count = 0;

			// read split 1
			format.open(split1);
			while ((next = format.nextRecord(null)) != null) {
				assertEquals(7, next.length());
				count++;
			}
			assertNull(format.nextRecord(null));
			assertTrue(format.reachedEnd());
			format.close();
			
			// this one must have read one too many, because the next split will skipp the trailing remainder
			// which happens to be one full record
			assertEquals(3, count);

			// read split 2
			format.open(split2);
			while ((next = format.nextRecord(null)) != null) {
				assertEquals(7, next.length());
				count++;
			}
			format.close();

			assertEquals(4, count);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testReadExactlyBufferSize() {
		try {
			final String myString = "aaaaaaa\nbbbbbbb\nccccccc\nddddddd\n";
			
			final FileInputSplit split = createTempFile(myString);
			final Configuration parameters = new Configuration();
			
			format.setBufferSize((int) split.getLength());
			format.configure(parameters);
			format.open(split);

			String next;
			int count = 0;
			while ((next = format.nextRecord(null)) != null) {
				assertEquals(7, next.length());
				count++;
			}
			assertNull(format.nextRecord(null));
			assertTrue(format.reachedEnd());

			format.close();
			
			assertEquals(4, count);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testReadRecordsLargerThanBuffer() {
		try {
			final String myString = "aaaaaaaaaaaaaaaaaaaaa\n" +
									"bbbbbbbbbbbbbbbbbbbbbbbbb\n" +
									"ccccccccccccccccccc\n" +
									"ddddddddddddddddddddddddddddddddddd\n";

			final FileInputSplit split = createTempFile(myString);
			FileInputSplit split1 = new FileInputSplit(0, split.getPath(), 0, split.getLength() / 2, split.getHostnames());
			FileInputSplit split2 = new FileInputSplit(1, split.getPath(), split1.getLength(), split.getLength(), split.getHostnames());
			
			final Configuration parameters = new Configuration();

			format.setBufferSize(8);
			format.configure(parameters);

			String next;
			List<String> result = new ArrayList<String>();
			
			
			format.open(split1);
			while ((next = format.nextRecord(null)) != null) {
				result.add(next);
			}
			assertNull(format.nextRecord(null));
			assertTrue(format.reachedEnd());
			format.close();

			format.open(split2);
			while ((next = format.nextRecord(null)) != null) {
				result.add(next);
			}
			assertNull(format.nextRecord(null));
			assertTrue(format.reachedEnd());
			format.close();
			
			assertEquals(4, result.size());
			assertEquals(Arrays.asList(myString.split("\n")), result);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	private static FileInputSplit createTempFile(String contents) throws IOException {
		File tempFile = File.createTempFile("test_contents", "tmp");
		tempFile.deleteOnExit();
		
		OutputStreamWriter wrt = new OutputStreamWriter(new FileOutputStream(tempFile));
		wrt.write(contents);
		wrt.close();
		
		return new FileInputSplit(0, new Path(tempFile.toURI().toString()), 0, tempFile.length(), new String[] {"localhost"});
	}
	
	
	protected static final class MyTextInputFormat extends DelimitedInputFormat<String> {
		private static final long serialVersionUID = 1L;
		
		@Override
		public String readRecord(String reuse, byte[] bytes, int offset, int numBytes) {
			return new String(bytes, offset, numBytes);
		}
	}
}
