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

package org.apache.flink.types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertArrayEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import java.util.Random;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.apache.flink.util.StringUtils;
import org.junit.Test;

/**
 * Test for the serialization of Strings through the StringValue class.
 */
public class StringSerializationTest {

	private final Random rnd = new Random(2093486528937460234L);
	
	
	@Test
	public void testNonNullValues() {
		try {
			String[] testStrings = new String[] {
				"a", "", "bcd", "jbmbmner8 jhk hj \n \t üäßß@µ", "", "non-empty"
			};
			
			testSerialization(testStrings);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception in test: " + e.getMessage());
		}
	}

	@Test
	public void testUnicodeValues() {
		try {
			String[] testStrings = new String[] {
				StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2, (char) 1, (char) 127),
				StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2, (char) 128, (char) 16383),
				StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2, (char) 16384, (char) 65535),
				StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2, (char) 1, (char) 16383),
				StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2, (char) 1, (char) 65535),
				StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2, (char) 128, (char) 65535)
			};
			testSerialization(testStrings);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception in test: " + e.getMessage());
		}
	}

	@Test
	public void testUnicodeSurrogatePairs() {
		try {
			String[] symbols = new String[] {
				"\uD800\uDF30", "\uD800\uDF31", "\uD800\uDF32", "\uD834\uDF08", "\uD834\uDF56",
				"\uD834\uDD20", "\uD802\uDC01", "\uD800\uDC09", "\uD87E\uDC9E", "\uD864\uDDF8",
				"\uD840\uDC0E", "\uD801\uDC80", "\uD801\uDC56", "\uD801\uDC05", "\uD800\uDF01"
			};
			String[] buffer = new String[100];
			Random random = new Random();
			for (int i = 0; i < 100; i++) {
				StringBuilder builder = new StringBuilder();
				for (int j = 0; j < 100; j++) {
					builder.append(symbols[random.nextInt(symbols.length)]);
				}
				buffer[i] = builder.toString();
			}
			testSerialization(buffer);
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception in test: " + e.getMessage());
		}

	}

	@Test
	public void testStringBinaryCompatibility() {
		try {
			String[] testStrings = new String[] {
				StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2, (char) 1, (char) 127),
				StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2, (char) 128, (char) 16383),
				StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2, (char) 16384, (char) 65535),
				StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2, (char) 1, (char) 16383),
				StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2, (char) 1, (char) 65535),
				StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2, (char) 128, (char) 65535)
			};

			for (String testString: testStrings) {
				// new and old impl should produce the same binary result
				byte[] oldBytes = serializeBytes(testString, StringSerializationTest::oldWriteString);
				byte[] newBytes = serializeBytes(testString, StringSerializationTest::newWriteString);
				assertArrayEquals(oldBytes, newBytes);
				// old impl should read bytes from new one
				String oldString = deserializeBytes(newBytes, StringSerializationTest::oldReadString);
				assertEquals(oldString, testString);
				// new impl should read bytes from old one
				String newString = deserializeBytes(oldBytes, StringSerializationTest::newReadString);
				assertEquals(newString, testString);
				// it should roundtrip over new impl
				String roundtrip = deserializeBytes(newBytes, StringSerializationTest::newReadString);
				assertEquals(roundtrip, testString);
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception in test: " + e.getMessage());
		}
	}
	
	@Test
	public void testNullValues() {
		try {
			String[] testStrings = new String[] {
				"a", null, "", null, "bcd", null, "jbmbmner8 jhk hj \n \t üäßß@µ", null, "", null, "non-empty"
			};
			
			testSerialization(testStrings);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception in test: " + e.getMessage());
		}
	}
	
	@Test
	public void testLongValues() {
		try {
			String[] testStrings = new String[] {
				StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2),
				StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2),
				StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2),
				StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2)
			};
			
			testSerialization(testStrings);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception in test: " + e.getMessage());
		}
	}
	
	@Test
	public void testMixedValues() {
		try {
			String[] testStrings = new String[] {
				StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2),
				"",
				StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2),
				null,
				StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2),
				null,
				"",
				StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2),
				"",
				null
			};
			
			testSerialization(testStrings);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception in test: " + e.getMessage());
		}
	}
	
	@Test
	public void testBinaryCopyOfLongStrings() {
		try {
			String[] testStrings = new String[] {
				StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2),
				"",
				StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2),
				null,
				StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2),
				null,
				"",
				StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2),
				"",
				null
			};
			
			testCopy(testStrings);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception in test: " + e.getMessage());
		}
	}

	public static final byte[] serializeBytes(String value, BiConsumer<String, DataOutput> writer) throws IOException {
		ByteArrayOutputStream buffer = new ByteArrayOutputStream();
		DataOutputStream stream = new DataOutputStream(buffer);
		writer.accept(value, stream);
		stream.close();
		return buffer.toByteArray();
	}

	public static final String deserializeBytes(byte[] value, Function<DataInput, String> reader) throws IOException {
		ByteArrayInputStream buffer = new ByteArrayInputStream(value);
		DataInputStream stream = new DataInputStream(buffer);
		String result = reader.apply(stream);
		stream.close();
		return result;
	}
	
	public static final void testSerialization(String[] values) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
		DataOutputStream serializer = new DataOutputStream(baos);
		
		for (String value : values) {
			StringValue.writeString(value, serializer);
		}
		
		serializer.close();
		baos.close();
		
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		DataInputStream deserializer = new DataInputStream(bais);
		
		int num = 0;
		while (deserializer.available() > 0) {
			String deser = StringValue.readString(deserializer);
			
			assertEquals("DeserializedString differs from original string.", values[num], deser);
			num++;
		}
		
		assertEquals("Wrong number of deserialized values", values.length, num);
	}

	public static final void testCopy(String[] values) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
		DataOutputStream serializer = new DataOutputStream(baos);
		
		for (String value : values) {
			StringValue.writeString(value, serializer);
		}
		
		serializer.close();
		baos.close();
		
		ByteArrayInputStream sourceInput = new ByteArrayInputStream(baos.toByteArray());
		DataInputStream source = new DataInputStream(sourceInput);
		ByteArrayOutputStream targetOutput = new ByteArrayOutputStream(4096);
		DataOutputStream target = new DataOutputStream(targetOutput);
		
		for (int i = 0; i < values.length; i++) {
			StringValue.copyString(source, target);
		}
		
		ByteArrayInputStream validateInput = new ByteArrayInputStream(targetOutput.toByteArray());
		DataInputStream validate = new DataInputStream(validateInput);
		
		int num = 0;
		while (validate.available() > 0) {
			String deser = StringValue.readString(validate);
			
			assertEquals("DeserializedString differs from original string.", values[num], deser);
			num++;
		}
		
		assertEquals("Wrong number of deserialized values", values.length, num);
	}

	// needed to test the binary compatibility for new/old string serialization code
	private static final int HIGH_BIT = 0x1 << 7;
	private static String oldReadString(DataInput in) {
		try {
			// the length we read is offset by one, because a length of zero indicates a null value
			int len = in.readUnsignedByte();

			if (len == 0) {
				return null;
			}

			if (len >= HIGH_BIT) {
				int shift = 7;
				int curr;
				len = len & 0x7f;
				while ((curr = in.readUnsignedByte()) >= HIGH_BIT) {
					len |= (curr & 0x7f) << shift;
					shift += 7;
				}
				len |= curr << shift;
			}

			// subtract one for the null length
			len -= 1;

			final char[] data = new char[len];

			for (int i = 0; i < len; i++) {
				int c = in.readUnsignedByte();
				if (c < HIGH_BIT) {
					data[i] = (char) c;
				} else {
					int shift = 7;
					int curr;
					c = c & 0x7f;
					while ((curr = in.readUnsignedByte()) >= HIGH_BIT) {
						c |= (curr & 0x7f) << shift;
						shift += 7;
					}
					c |= curr << shift;
					data[i] = (char) c;
				}
			}

			return new String(data, 0, len);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static final void oldWriteString(CharSequence cs, DataOutput out) {
		try {
			if (cs != null) {
				// the length we write is offset by one, because a length of zero indicates a null value
				int lenToWrite = cs.length()+1;
				if (lenToWrite < 0) {
					throw new IllegalArgumentException("CharSequence is too long.");
				}

				// write the length, variable-length encoded
				while (lenToWrite >= HIGH_BIT) {
					out.write(lenToWrite | HIGH_BIT);
					lenToWrite >>>= 7;
				}
				out.write(lenToWrite);

				// write the char data, variable length encoded
				for (int i = 0; i < cs.length(); i++) {
					int c = cs.charAt(i);

					while (c >= HIGH_BIT) {
						out.write(c | HIGH_BIT);
						c >>>= 7;
					}
					out.write(c);
				}
			} else {
				out.write(0);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static String newReadString(DataInput in) {
		try {
			return StringValue.readString(in);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static void newWriteString(CharSequence cs, DataOutput out) {
		try {
			StringValue.writeString(cs, out);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}



}
