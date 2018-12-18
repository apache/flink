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

package org.apache.flink.util;

import org.apache.flink.api.common.typeutils.base.DoubleValueSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.StringValue;
import org.apache.flink.types.Value;

import org.junit.Test;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Objects;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link InstantiationUtil}.
 */
public class InstantiationUtilTest extends TestLogger {

	@Test
	public void testInstantiationOfStringValue() {
		StringValue stringValue = InstantiationUtil.instantiate(
				StringValue.class, null);
		assertNotNull(stringValue);
	}

	@Test
	public void testInstantiationOfStringValueAndCastToValue() {
		StringValue stringValue = InstantiationUtil.instantiate(
				StringValue.class, Value.class);
		assertNotNull(stringValue);
	}

	@Test
	public void testHasNullaryConstructor() {
		assertTrue(InstantiationUtil
				.hasPublicNullaryConstructor(StringValue.class));
	}

	@Test
	public void testClassIsProper() {
		assertTrue(InstantiationUtil.isProperClass(StringValue.class));
	}

	@Test
	public void testClassIsNotProper() {
		assertFalse(InstantiationUtil.isProperClass(Value.class));
	}

	@Test(expected = RuntimeException.class)
	public void testCheckForInstantiationOfPrivateClass() {
		InstantiationUtil.checkForInstantiation(TestClass.class);
	}

	@Test
	public void testSerializationToByteArray() throws IOException {
		final DoubleValue toSerialize = new DoubleValue(Math.random());
		final DoubleValueSerializer serializer = new DoubleValueSerializer();

		byte[] serialized = InstantiationUtil.serializeToByteArray(serializer, toSerialize);

		DoubleValue deserialized = InstantiationUtil.deserializeFromByteArray(serializer, serialized);

		assertEquals("Serialized record is not equal after serialization.", toSerialize, deserialized);
	}

	@Test
	public void testWriteToConfigFailingSerialization() {
		try {
			final String key1 = "testkey1";
			final String key2 = "testkey2";
			final Configuration config = new Configuration();

			try {
				InstantiationUtil.writeObjectToConfig(new TestClassWriteFails(), config, "irgnored");
				fail("should throw an exception");
			}
			catch (TestException e) {
				// expected
			}
			catch (Exception e) {
				fail("Wrong exception type - exception not properly forwarded");
			}

			InstantiationUtil.writeObjectToConfig(new TestClassReadFails(), config, key1);
			InstantiationUtil.writeObjectToConfig(new TestClassReadFailsCNF(), config, key2);

			try {
				InstantiationUtil.readObjectFromConfig(config, key1, getClass().getClassLoader());
				fail("should throw an exception");
			}
			catch (TestException e) {
				// expected
			}
			catch (Exception e) {
				fail("Wrong exception type - exception not properly forwarded");
			}

			try {
				InstantiationUtil.readObjectFromConfig(config, key2, getClass().getClassLoader());
				fail("should throw an exception");
			}
			catch (ClassNotFoundException e) {
				// expected
			}
			catch (Exception e) {
				fail("Wrong exception type - exception not properly forwarded");
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testCopyWritable() throws Exception {
		WritableType original = new WritableType();
		WritableType copy = InstantiationUtil.createCopyWritable(original);

		assertTrue(original != copy);
		assertTrue(original.equals(copy));
	}

	// --------------------------------------------------------------------------------------------

	private class TestClass {}

	private static class TestException extends IOException {
		private static final long serialVersionUID = 1L;
	}

	private static class TestClassWriteFails implements java.io.Serializable {

		private static final long serialVersionUID = 1L;

		private void writeObject(ObjectOutputStream out) throws IOException {
			throw new TestException();
		}
	}

	private static class TestClassReadFails implements java.io.Serializable {

		private static final long serialVersionUID = 1L;

		private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
			throw new TestException();
		}
	}

	private static class TestClassReadFailsCNF implements java.io.Serializable {

		private static final long serialVersionUID = 1L;

		private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
			throw new ClassNotFoundException("test exception");
		}
	}

	/** A simple test type. */
	public static final class WritableType implements IOReadableWritable {

		private int aInt;
		private long aLong;

		public WritableType() {
			Random rnd = new Random();
			this.aInt = rnd.nextInt();
			this.aLong = rnd.nextLong();
		}

		@Override
		public int hashCode() {
			return Objects.hash(aInt, aLong);
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == this) {
				return true;
			}
			else if (obj != null && obj.getClass() == WritableType.class) {
				WritableType that = (WritableType) obj;
				return this.aLong == that.aLong && this.aInt == that.aInt;
			}
			else {
				return false;
			}
		}

		@Override
		public void write(DataOutputView out) throws IOException {
			out.writeInt(aInt);
			out.writeLong(aLong);
		}

		@Override
		public void read(DataInputView in) throws IOException {
			this.aInt = in.readInt();
			this.aLong = in.readLong();
		}
	}
}
