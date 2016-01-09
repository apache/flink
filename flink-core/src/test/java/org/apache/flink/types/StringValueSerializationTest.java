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
import java.io.IOException;
import java.util.Random;

import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.StringUtils;

import org.junit.Test;

/**
 * Test for the serialization of StringValue.
 */
public class StringValueSerializationTest {

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
				StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2),
				"",
				StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2),
				""
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
				StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2),
				"",
				StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2),
				""
			};
			
			testCopy(testStrings);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception in test: " + e.getMessage());
		}
	}
	
	public static void testSerialization(String[] values) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
		DataOutputViewStreamWrapper serializer = new DataOutputViewStreamWrapper(baos);
		
		for (String value : values) {
			StringValue sv = new StringValue(value);
			sv.write(serializer);
		}
		
		serializer.close();
		baos.close();
		
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		DataInputViewStreamWrapper deserializer = new DataInputViewStreamWrapper(bais);
		
		int num = 0;
		while (bais.available() > 0) {
			StringValue deser = new StringValue();
			deser.read(deserializer);
			
			assertEquals("DeserializedString differs from original string.", values[num], deser.getValue());
			num++;
		}
		
		assertEquals("Wrong number of deserialized values", values.length, num);
	}

	public static void testCopy(String[] values) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
		DataOutputViewStreamWrapper serializer = new DataOutputViewStreamWrapper(baos);
		
		StringValue sValue = new StringValue();
		
		for (String value : values) {
			sValue.setValue(value);
			sValue.write(serializer);
		}
		
		serializer.close();
		baos.close();
		
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		DataInputViewStreamWrapper source = new DataInputViewStreamWrapper(bais);
		
		ByteArrayOutputStream targetOutput = new ByteArrayOutputStream(4096);
		DataOutputViewStreamWrapper target = new DataOutputViewStreamWrapper(targetOutput);

		for (String value : values) {
			sValue.copy(source, target);
		}
		
		ByteArrayInputStream validateInput = new ByteArrayInputStream(targetOutput.toByteArray());
		DataInputViewStreamWrapper validate = new DataInputViewStreamWrapper(validateInput);
		
		int num = 0;
		while (validateInput.available() > 0) {
			sValue.read(validate);
			
			assertEquals("DeserializedString differs from original string.", values[num], sValue.getValue());
			num++;
		}
		
		assertEquals("Wrong number of deserialized values", values.length, num);
	}
	
}
