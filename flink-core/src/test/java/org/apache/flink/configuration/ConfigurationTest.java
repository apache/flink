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

package org.apache.flink.configuration;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.flink.core.testutils.CommonTestUtils;

import org.apache.flink.util.TestLogger;
import org.junit.Test;

/**
 * This class contains test for the configuration package. In particular, the serialization of {@link Configuration}
 * objects is tested.
 */
public class ConfigurationTest extends TestLogger {
	
	private static final byte[] EMPTY_BYTES = new byte[0];
	private static final long TOO_LONG = Integer.MAX_VALUE + 10L;
	private static final double TOO_LONG_DOUBLE = Double.MAX_VALUE;

	/**
	 * This test checks the serialization/deserialization of configuration objects.
	 */
	@Test
	public void testConfigurationSerializationAndGetters() {
		try {
			final Configuration orig = new Configuration();
			orig.setString("mykey", "myvalue");
			orig.setInteger("mynumber", 100);
			orig.setLong("longvalue", 478236947162389746L);
			orig.setFloat("PI", 3.1415926f);
			orig.setDouble("E", Math.E);
			orig.setBoolean("shouldbetrue", true);
			orig.setBytes("bytes sequence", new byte[] { 1, 2, 3, 4, 5 } );
			orig.setClass("myclass", this.getClass());
	
			final Configuration copy = CommonTestUtils.createCopyWritable(orig);
			assertEquals("myvalue", copy.getString("mykey", "null"));
			assertEquals(100, copy.getInteger("mynumber", 0));
			assertEquals(478236947162389746L, copy.getLong("longvalue", 0L));
			assertEquals(3.1415926f, copy.getFloat("PI", 3.1415926f), 0.0);
			assertEquals(Math.E, copy.getDouble("E", 0.0), 0.0);
			assertEquals(true, copy.getBoolean("shouldbetrue", false));
			assertArrayEquals(new byte[] { 1, 2, 3, 4, 5 }, copy.getBytes("bytes sequence", null));
			assertEquals(getClass(), copy.getClass("myclass", null, getClass().getClassLoader()));
			
			assertEquals(orig, copy);
			assertEquals(orig.keySet(), copy.keySet());
			assertEquals(orig.hashCode(), copy.hashCode());

		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testConversions() {
		try {
			Configuration pc = new Configuration();
			
			pc.setInteger("int", 5);
			pc.setLong("long", 15);
			pc.setLong("too_long", TOO_LONG);
			pc.setFloat("float", 2.1456775f);
			pc.setDouble("double", Math.PI);
			pc.setDouble("too_long_double", TOO_LONG_DOUBLE);
			pc.setString("string", "42");
			pc.setString("non_convertible_string", "bcdefg&&");
			pc.setBoolean("boolean", true);
			
			// as integer
			assertEquals(5, pc.getInteger("int", 0));
			assertEquals(5L, pc.getLong("int", 0));
			assertEquals(5f, pc.getFloat("int", 0), 0.0);
			assertEquals(5.0, pc.getDouble("int", 0), 0.0);
			assertEquals(false, pc.getBoolean("int", true));
			assertEquals("5", pc.getString("int", "0"));
			assertArrayEquals(EMPTY_BYTES, pc.getBytes("int", EMPTY_BYTES));
			
			// as long
			assertEquals(15, pc.getInteger("long", 0));
			assertEquals(15L, pc.getLong("long", 0));
			assertEquals(15f, pc.getFloat("long", 0), 0.0);
			assertEquals(15.0, pc.getDouble("long", 0), 0.0);
			assertEquals(false, pc.getBoolean("long", true));
			assertEquals("15", pc.getString("long", "0"));
			assertArrayEquals(EMPTY_BYTES, pc.getBytes("long", EMPTY_BYTES));
			
			// as too long
			assertEquals(0, pc.getInteger("too_long", 0));
			assertEquals(TOO_LONG, pc.getLong("too_long", 0));
			assertEquals((float) TOO_LONG, pc.getFloat("too_long", 0), 10.0);
			assertEquals((double) TOO_LONG, pc.getDouble("too_long", 0), 10.0);
			assertEquals(false, pc.getBoolean("too_long", true));
			assertEquals(String.valueOf(TOO_LONG), pc.getString("too_long", "0"));
			assertArrayEquals(EMPTY_BYTES, pc.getBytes("too_long", EMPTY_BYTES));
			
			// as float
			assertEquals(0, pc.getInteger("float", 0));
			assertEquals(0L, pc.getLong("float", 0));
			assertEquals(2.1456775f, pc.getFloat("float", 0), 0.0);
			assertEquals(2.1456775, pc.getDouble("float", 0), 0.0000001);
			assertEquals(false, pc.getBoolean("float", true));
			assertTrue(pc.getString("float", "0").startsWith("2.145677"));
			assertArrayEquals(EMPTY_BYTES, pc.getBytes("float", EMPTY_BYTES));
			
			// as double
			assertEquals(0, pc.getInteger("double", 0));
			assertEquals(0L, pc.getLong("double", 0));
			assertEquals(3.141592f, pc.getFloat("double", 0), 0.000001);
			assertEquals(Math.PI, pc.getDouble("double", 0), 0.0);
			assertEquals(false, pc.getBoolean("double", true));
			assertTrue(pc.getString("double", "0").startsWith("3.1415926535"));
			assertArrayEquals(EMPTY_BYTES, pc.getBytes("double", EMPTY_BYTES));
			
			// as too long double
			assertEquals(0, pc.getInteger("too_long_double", 0));
			assertEquals(0L, pc.getLong("too_long_double", 0));
			assertEquals(0f, pc.getFloat("too_long_double", 0f), 0.000001);
			assertEquals(TOO_LONG_DOUBLE, pc.getDouble("too_long_double", 0), 0.0);
			assertEquals(false, pc.getBoolean("too_long_double", true));
			assertEquals(String.valueOf(TOO_LONG_DOUBLE), pc.getString("too_long_double", "0"));
			assertArrayEquals(EMPTY_BYTES, pc.getBytes("too_long_double", EMPTY_BYTES));
			
			// as string
			assertEquals(42, pc.getInteger("string", 0));
			assertEquals(42L, pc.getLong("string", 0));
			assertEquals(42f, pc.getFloat("string", 0f), 0.000001);
			assertEquals(42.0, pc.getDouble("string", 0), 0.0);
			assertEquals(false, pc.getBoolean("string", true));
			assertEquals("42", pc.getString("string", "0"));
			assertArrayEquals(EMPTY_BYTES, pc.getBytes("string", EMPTY_BYTES));
			
			// as non convertible string
			assertEquals(0, pc.getInteger("non_convertible_string", 0));
			assertEquals(0L, pc.getLong("non_convertible_string", 0));
			assertEquals(0f, pc.getFloat("non_convertible_string", 0f), 0.000001);
			assertEquals(0.0, pc.getDouble("non_convertible_string", 0), 0.0);
			assertEquals(false, pc.getBoolean("non_convertible_string", true));
			assertEquals("bcdefg&&", pc.getString("non_convertible_string", "0"));
			assertArrayEquals(EMPTY_BYTES, pc.getBytes("non_convertible_string", EMPTY_BYTES));
			
			// as boolean
			assertEquals(0, pc.getInteger("boolean", 0));
			assertEquals(0L, pc.getLong("boolean", 0));
			assertEquals(0f, pc.getFloat("boolean", 0f), 0.000001);
			assertEquals(0.0, pc.getDouble("boolean", 0), 0.0);
			assertEquals(true, pc.getBoolean("boolean", false));
			assertEquals("true", pc.getString("boolean", "0"));
			assertArrayEquals(EMPTY_BYTES, pc.getBytes("boolean", EMPTY_BYTES));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testCopyConstructor() {
		try {
			final String key = "theKey";
			
			Configuration cfg1 = new Configuration();
			cfg1.setString(key, "value");
			
			Configuration cfg2 = new Configuration(cfg1);
			cfg2.setString(key, "another value");
			
			assertEquals("value", cfg1.getString(key, ""));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
