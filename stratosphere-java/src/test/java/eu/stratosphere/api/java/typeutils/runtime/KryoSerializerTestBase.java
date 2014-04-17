/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java.typeutils.runtime;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;

import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;

/**
 * Abstract test base for serializers.
 */
public abstract class KryoSerializerTestBase<T> {

	protected abstract TypeSerializer<String> createSerializer();

	protected abstract int getLength();

	protected abstract Class<T> getTypeClass();

	protected abstract String getTestData();

	// --------------------------------------------------------------------------------------------

	@Test
	public void testInstantiate() {
		try {
			TypeSerializer<String> serializer = getSerializer();

			String instance = serializer.createInstance();
			assertNotNull("The created instance must not be null.", instance);

			Class<T> type = getTypeClass();
			assertNotNull("The test is corrupt: type class is null.", type);

			assertEquals("Type of the instantiated object is wrong.", type,
					instance.getClass());
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception in test: " + e.getMessage());
		}
	}

	@Test
	public void testGetLength() {
		try {
			TypeSerializer<String> serializer = getSerializer();
			assertEquals(getLength(), serializer.getLength());
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception in test: " + e.getMessage());
		}
	}

	@Test
	public void testCopy() {
		try {
			TypeSerializer<String> serializer = getSerializer();
			String testData = getData();
			String copy = serializer
					.copy(testData, serializer.createInstance());
			assertEquals(
					"Copied element is not equal to the original element.",
					testData, copy);

		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception in test: " + e.getMessage());
		}
	}

	@Test
	public void testSerialize() {
		try {
			TypeSerializer<String> serializer = getSerializer();
			String testData = getData();

			TestOutputView out = new TestOutputView();
			serializer.serialize(testData, out);
			TestInputView in = out.getInputView();

			assertTrue("No data available during deserialization.",
			in.available() > 0);

			String deserialized = serializer.deserialize(
					serializer.createInstance(), in);
			assertEquals("Deserialized value if wrong.", testData, deserialized);

			assertTrue("Trailing data available after deserialization.",
			in.available() == 0);

		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception in test: " + e.getMessage());
		}
	}

	// --------------------------------------------------------------------------------------------

	private TypeSerializer<String> getSerializer() {
		TypeSerializer<String> serializer = createSerializer();
		if (serializer == null) {
			throw new RuntimeException(
					"Test case corrupt. Returns null as serializer.");
		}
		return serializer;
	}

	private String getData() {
		String data = getTestData();
		if (data == null) {
			throw new RuntimeException(
					"Test case corrupt. Returns null as test data.");
		}
		return data;
	}

	// --------------------------------------------------------------------------------------------

	private static final class TestOutputView extends DataOutputStream
			implements DataOutputView {

		public TestOutputView() {
			super(new ByteArrayOutputStream(4096));
		}

		public TestInputView getInputView() {
			ByteArrayOutputStream baos = (ByteArrayOutputStream) out;
			return new TestInputView(baos.toByteArray());
		}

		@Override
		public void skipBytesToWrite(int numBytes) throws IOException {
			for (int i = 0; i < numBytes; i++) {
				write(0);
			}
		}

		@Override
		public void write(DataInputView source, int numBytes)
				throws IOException {
			byte[] buffer = new byte[numBytes];
			source.readFully(buffer);
			write(buffer);
		}
	}

	private static final class TestInputView extends DataInputStream implements
			DataInputView {

		public TestInputView(byte[] data) {
			super(new ByteArrayInputStream(data));
		}

		@Override
		public void skipBytesToRead(int numBytes) throws IOException {
			while (numBytes > 0) {
				int skipped = skipBytes(numBytes);
				numBytes -= skipped;
			}
		}
	}
}
