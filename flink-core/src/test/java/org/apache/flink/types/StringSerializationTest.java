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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;

import org.apache.flink.types.StringValue;
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
	
}
