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

package eu.stratosphere.nephele.io.channels;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.nephele.types.IntegerRecord;

/**
 * This class checks the functionality of the {@link SerializationBuffer} class and the {@link DefaultDeserializer}
 * class
 * 
 * @author marrus
 */
public class BufferTest
{
	private File file = new File("./tmp");

	private FileInputStream fileinstream;

	private FileOutputStream filestream;

	private FileChannel writeable;

	private FileChannel readable;

	/**
	 * Set up files and stream for testing
	 * 
	 * @throws IOException
	 */
	@Before
	public void before() throws IOException {
		file.createNewFile();
		filestream = new FileOutputStream(file);
		fileinstream = new FileInputStream(file);
		writeable = filestream.getChannel();
		readable = fileinstream.getChannel();

	}

	/**
	 * clean up. Remove file close streams and channels
	 * 
	 * @throws IOException
	 */
	@After
	public void after() throws IOException {
		fileinstream.close();
		writeable.close();
		readable.close();
		filestream.close();
		file.delete();
	}

	/**
	 * Tests serialization and deserialization of an {@link IntegerRecord}
	 */
	@Test
	public void testIntSerialize()
	{
		final SerializationBuffer<IntegerRecord> intSerializationBuffer = new SerializationBuffer<IntegerRecord>();
		final int NUM = 0xab627ef;
		
		IntegerRecord intRecord = new IntegerRecord(NUM);
		// Serialize a record.
		try {
			intSerializationBuffer.serialize(intRecord);
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
		// Last record is still in buffer, serializing another should throw IOException
		try {
			intSerializationBuffer.serialize(intRecord);
			fail();
		} catch (IOException e) {
		}
		
		// Read from buffer (written in file)
		try {
			intSerializationBuffer.read(writeable);
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
		// Now a new Record can be serialized
		try {
			intSerializationBuffer.serialize(intRecord);
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}

		DefaultDeserializer<IntegerRecord> intDeserialitionBuffer = new DefaultDeserializer<IntegerRecord>(IntegerRecord.class, true);
		IntegerRecord record = new IntegerRecord();
		// Deserialize a Record
		try {
			record = intDeserialitionBuffer.readData(record, readable);
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
		// Check it contains the right value
		assertEquals(NUM, record.getValue());
		// File empty, another read should throw IOException
		try {
			record = intDeserialitionBuffer.readData(record, readable);
			fail();
		} catch (IOException e) {
		}

	}

	/**
	 * Tests serialization and deserialization of an {@link StringRecord}
	 */
	@Test
	public void testStringSerialize()
	{
		final SerializationBuffer<StringRecord> stringSerializationBuffer = new SerializationBuffer<StringRecord>();
		final String str = "abc";
		
		StringRecord stringrecord = new StringRecord(str);
		
		// Serialize a record.
		try {
			stringSerializationBuffer.serialize(stringrecord);
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
		// Read from buffer (write in file)
		try {
			stringSerializationBuffer.read(writeable);
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
		// Serialize next Record.
		// Read from buffer (write in file)
		final String str2 = "abcdef";
		stringrecord = new StringRecord(str2);
		try {
			stringSerializationBuffer.serialize(stringrecord);
			stringSerializationBuffer.read(writeable);
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}

		final DefaultDeserializer<StringRecord> stringDeserialitionBuffer = new DefaultDeserializer<StringRecord>(StringRecord.class, true);
		StringRecord record = new StringRecord();
		// Deserialize and check record are correct
		try {
			record = stringDeserialitionBuffer.readData(record, readable);
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
		assertEquals(str, record.toString());
		try {
			record = stringDeserialitionBuffer.readData(record, readable);
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
		assertEquals(str2, record.toString());
	}
}
