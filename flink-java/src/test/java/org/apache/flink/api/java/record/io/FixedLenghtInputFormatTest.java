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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FixedLenghtInputFormatTest {

	protected Configuration config;
	
	protected File tempFile;
	
	private final FixedLengthInputFormat format = new MyFixedLengthInputFormat();
	
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
	public void testOpen() throws IOException {
		final int[] fileContent = {1,2,3,4,5,6,7,8};
		final FileInputSplit split = createTempFile(fileContent);	
	
		final Configuration parameters = new Configuration();
		parameters.setInteger(FixedLengthInputFormat.RECORDLENGTH_PARAMETER_KEY, 8);
		
		format.configure(parameters);
		format.open(split);
		assertEquals(0, format.getSplitStart());
		assertEquals(0, format.getReadBufferSize() % 8);
		format.close();

		parameters.setInteger(FixedLengthInputFormat.RECORDLENGTH_PARAMETER_KEY, 13);
		format.configure(parameters);
		format.close();
		format.open(split);
		assertEquals(0, format.getReadBufferSize() % 13);
		format.close();
		
		parameters.setInteger(FixedLengthInputFormat.RECORDLENGTH_PARAMETER_KEY, 27);
		format.configure(parameters);
		format.close();
		format.open(split);
		assertEquals(0, format.getReadBufferSize() % 27);
		format.close();
		
	}
	
	@Test
	public void testRead() throws IOException {
		final int[] fileContent = {1,2,3,4,5,6,7,8};
		final FileInputSplit split = createTempFile(fileContent);
		
		final Configuration parameters = new Configuration();
		
		parameters.setInteger(FixedLengthInputFormat.RECORDLENGTH_PARAMETER_KEY, 8);
		
		format.configure(parameters);
		format.open(split);
		
		Record record = new Record();
		
		assertNotNull(format.nextRecord(record));
		assertEquals(1, record.getField(0, IntValue.class).getValue());
		assertEquals(2, record.getField(1, IntValue.class).getValue());	
		
		assertNotNull(format.nextRecord(record));
		assertEquals(3, record.getField(0, IntValue.class).getValue());
		assertEquals(4, record.getField(1, IntValue.class).getValue());
		
		assertNotNull(format.nextRecord(record));
		assertEquals(5, record.getField(0, IntValue.class).getValue());
		assertEquals(6, record.getField(1, IntValue.class).getValue());
		
		assertNotNull(format.nextRecord(record));
		assertEquals(7, record.getField(0, IntValue.class).getValue());
		assertEquals(8, record.getField(1, IntValue.class).getValue());
		
		assertNull(format.nextRecord(record));
		assertTrue(format.reachedEnd());
	}
	
	
	@Test
	public void testReadFail() throws IOException {
		final int[] fileContent = {1,2,3,4,5,6,7,8,9};
		final FileInputSplit split = createTempFile(fileContent);
		
		final Configuration parameters = new Configuration();
		parameters.setInteger(FixedLengthInputFormat.RECORDLENGTH_PARAMETER_KEY, 8);
		
		format.configure(parameters);
		format.open(split);
		
		Record record = new Record();

		try {
			assertNotNull(format.nextRecord(record));
			assertEquals(1, record.getField(0, IntValue.class).getValue());
			assertEquals(2, record.getField(1, IntValue.class).getValue());
			
			assertNotNull(format.nextRecord(record));
			assertEquals(3, record.getField(0, IntValue.class).getValue());
			assertEquals(4, record.getField(1, IntValue.class).getValue());
			
			assertNotNull(format.nextRecord(record));
			assertEquals(5, record.getField(0, IntValue.class).getValue());
			assertEquals(6, record.getField(1, IntValue.class).getValue());
			
			assertNotNull(format.nextRecord(record));
			assertEquals(7, record.getField(0, IntValue.class).getValue());
			assertEquals(8, record.getField(1, IntValue.class).getValue());
			
			assertNull(format.nextRecord(record));
		} catch(IOException ioe) {
			assertTrue(ioe.getMessage().equals("Unable to read full record"));
		}
	}
	
	
	private FileInputSplit createTempFile(int[] contents) throws IOException {
		this.tempFile = File.createTempFile("test_contents", "tmp");
		this.tempFile.deleteOnExit();
		
		DataOutputStream dos = new DataOutputStream(new FileOutputStream(tempFile));
		
		for(int i : contents) {
			dos.writeInt(i);
		}
		
		dos.close();
			
		return new FileInputSplit(0, new Path(this.tempFile.toURI().toString()), 0, this.tempFile.length(), new String[] {"localhost"});
	}
		
	
	private final class MyFixedLengthInputFormat extends FixedLengthInputFormat {
		private static final long serialVersionUID = 1L;

		IntValue p1 = new IntValue();
		IntValue p2 = new IntValue();
		
		@Override
		public boolean readBytes(Record target, byte[] buffer, int startPos) {
			int v1 = 0;
			v1 = (v1 | buffer[startPos+0]) << 8;
			v1 = (v1 | buffer[startPos+1]) << 8;
			v1 = (v1 | buffer[startPos+2]) << 8;
			v1 = (v1 | buffer[startPos+3]);
			p1.setValue(v1);
			
			int v2 = 0;
			v2 = (v2 | buffer[startPos+4]) << 8;
			v2 = (v2 | buffer[startPos+5]) << 8;
			v2 = (v2 | buffer[startPos+6]) << 8;
			v2 = (v2 | buffer[startPos+7]);
			p2.setValue(v2);
			
			target.setField(0, p1);
			target.setField(1, p2);
			
			return true;
		}
	}
}
