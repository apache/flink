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

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.io.DefaultRecordDeserializer;
import eu.stratosphere.nephele.types.IntegerRecord;
import eu.stratosphere.nephele.types.StringRecord;

/**
 * This class checks the functionality of the {@link SerializationBuffer} class and the {@link DeserializationBuffer}
 * class
 * 
 * @author marrus
 */
public class BufferTest {
	static File file = new File("./tmp");

	static FileInputStream fileinstream;

	static FileOutputStream filestream;

	static FileChannel writeable;

	static FileChannel readable;

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
	@AfterClass
	public static void after() throws IOException {
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
	public void testIntSerialize() {
		SerializationBuffer<IntegerRecord> intSerializationBuffer = new SerializationBuffer<IntegerRecord>();
		int i = 0;
		IntegerRecord intRecord = new IntegerRecord(i);
		//Serialize a record.
		try {
			intSerializationBuffer.serialize(intRecord);
		} catch (IOException e) {
			fail();
			e.printStackTrace();
		}
		//Last record is still in buffer, serializing another should throw IOException
		try {
			intSerializationBuffer.serialize(intRecord);
			fail();
		} catch (IOException e) {
			e.printStackTrace();
		}
		//Read from buffer (written in file)
		try {
			intSerializationBuffer.read(writeable);
		} catch (IOException e) {
			fail();
			e.printStackTrace();
		}
		//Now a new Record can be serialized
		try {
			intSerializationBuffer.serialize(intRecord);
		} catch (IOException e) {
			fail();
			e.printStackTrace();
		}
		
		DeserializationBuffer<IntegerRecord> intDeserialitionBuffer = new DeserializationBuffer<IntegerRecord>(
			new DefaultRecordDeserializer<IntegerRecord>(IntegerRecord.class), true);
		IntegerRecord record = new IntegerRecord();
		//Deserialze a Record 
		try {
			record = intDeserialitionBuffer.readData(readable);
		} catch (IOException e) {
			fail();
			e.printStackTrace();
		}
		//Check it contains the right value
		assertEquals(i, record.getValue());
		//File empty, another read should throw IOException
		try {
			record = intDeserialitionBuffer.readData(readable);
			fail();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Tests serialization and deserialization of an {@link StringRecord}
	 */
	@Test
	public void testStringSerialize() {
		SerializationBuffer<StringRecord> stringSerializationBuffer = new SerializationBuffer<StringRecord>();
		String str = "abc";
		StringRecord stringrecord = new StringRecord(str);
		//Serialize a record.
		try {
			stringSerializationBuffer.serialize(stringrecord);
		} catch (IOException e) {
			fail();
			e.printStackTrace();
		}
		//Read from buffer (write in file)
		try {
			stringSerializationBuffer.read(writeable);
		} catch (IOException e) {
			fail();
			e.printStackTrace();
		}
		//Serialize next Record. 
		//Read from buffer (write in file)
		String str2 = "abcdef";
		stringrecord = new StringRecord(str2);
		try {
			stringSerializationBuffer.serialize(stringrecord);
			stringSerializationBuffer.read(writeable);
		} catch (IOException e) {
			fail();
			e.printStackTrace();
		}

		DeserializationBuffer<StringRecord> stringDeserialitionBuffer = new DeserializationBuffer<StringRecord>(
			new DefaultRecordDeserializer<StringRecord>(StringRecord.class), true);
		StringRecord record = new StringRecord();
		//Deserialize and check record are correct
		try {
			record = stringDeserialitionBuffer.readData(readable);
		} catch (IOException e) {
			fail();
			e.printStackTrace();
		}
		assertEquals(str, record.toString());
		try {
			record = stringDeserialitionBuffer.readData(readable);
		} catch (IOException e) {
			fail();
			e.printStackTrace();
		}
		assertEquals(str2, record.toString());
	}
}
