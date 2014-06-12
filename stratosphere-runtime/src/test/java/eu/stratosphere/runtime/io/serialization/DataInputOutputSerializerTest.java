/***********************************************************************************************************************
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.runtime.io.serialization;

import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.runtime.io.serialization.types.SerializationTestType;
import eu.stratosphere.runtime.io.serialization.types.SerializationTestTypeFactory;
import eu.stratosphere.runtime.io.serialization.types.Util;
import junit.framework.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;

public class DataInputOutputSerializerTest {

	@Test
	public void testWrapAsByteBuffer() {
		SerializationTestType randomInt = Util.randomRecord(SerializationTestTypeFactory.INT);

		DataOutputSerializer serializer = new DataOutputSerializer(randomInt.length());
		MemorySegment segment = new MemorySegment(new byte[randomInt.length()]);

		try {
			// empty buffer, read buffer should be empty
			ByteBuffer wrapper = serializer.wrapAsByteBuffer();

			Assert.assertEquals(0, wrapper.position());
			Assert.assertEquals(0, wrapper.limit());

			// write to data output, read buffer should still be empty
			randomInt.write(serializer);

			Assert.assertEquals(0, wrapper.position());
			Assert.assertEquals(0, wrapper.limit());

			// get updated read buffer, read buffer should contain written data
			wrapper = serializer.wrapAsByteBuffer();

			Assert.assertEquals(0, wrapper.position());
			Assert.assertEquals(randomInt.length(), wrapper.limit());

			// clear data output, read buffer should still contain written data
			serializer.clear();

			Assert.assertEquals(0, wrapper.position());
			Assert.assertEquals(randomInt.length(), wrapper.limit());

			// get updated read buffer, should be empty
			wrapper = serializer.wrapAsByteBuffer();

			Assert.assertEquals(0, wrapper.position());
			Assert.assertEquals(0, wrapper.limit());

			// write to data output and read back to memory
			randomInt.write(serializer);
			wrapper = serializer.wrapAsByteBuffer();

			segment.put(0, wrapper, randomInt.length());

			Assert.assertEquals(randomInt.length(), wrapper.position());
			Assert.assertEquals(randomInt.length(), wrapper.limit());
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail("Test encountered an unexpected exception.");
		}
	}

	@Test
	public void testRandomValuesWriteRead() {
		final int numElements = 100000;
		final ArrayDeque<SerializationTestType> reference = new ArrayDeque<SerializationTestType>();

		DataOutputSerializer serializer = new DataOutputSerializer(1);

		for (SerializationTestType value : Util.randomRecords(numElements)) {
			reference.add(value);

			try {
				value.write(serializer);
			} catch (IOException e) {
				e.printStackTrace();
				Assert.fail("Test encountered an unexpected exception.");
			}
		}

		DataInputDeserializer deserializer = new DataInputDeserializer(serializer.wrapAsByteBuffer());

		for (SerializationTestType expected : reference) {
			try {
				SerializationTestType actual = expected.getClass().newInstance();
				actual.read(deserializer);

				Assert.assertEquals(expected, actual);
			} catch (Exception e) {
				e.printStackTrace();
				Assert.fail("Test encountered an unexpected exception.");
			}
		}

		reference.clear();
	}
}
