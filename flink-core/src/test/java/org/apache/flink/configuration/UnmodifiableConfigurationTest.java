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


import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * This class verifies that the Unmodifiable Configuration class overrides all setter methods in
 * Configuration.
 */
public class UnmodifiableConfigurationTest {

	private static Configuration pc = new Configuration();
	private static UnmodifiableConfiguration unConf = new UnmodifiableConfiguration(pc);
	private static Class clazz = unConf.getClass();

	@Test
	public void testOverride() throws Exception{
		unConf.setBoolean("a", false);
		assertEquals(clazz, clazz.getMethod("setClass", String.class, Class.class).getDeclaringClass());
		assertEquals(clazz, clazz.getMethod("setString", String.class, String.class).getDeclaringClass());
		assertEquals(clazz, clazz.getMethod("setInteger", String.class, int.class).getDeclaringClass());
		assertEquals(clazz, clazz.getMethod("setLong", String.class, long.class).getDeclaringClass());
		assertEquals(clazz, clazz.getMethod("setFloat", String.class, float.class).getDeclaringClass());
		assertEquals(clazz, clazz.getMethod("setDouble", String.class, double.class).getDeclaringClass());
		assertEquals(clazz, clazz.getMethod("setBoolean", String.class, boolean.class).getDeclaringClass());
		assertEquals(clazz, clazz.getMethod("setBytes", String.class, byte[].class).getDeclaringClass());
		assertEquals(clazz, clazz.getMethod("addAll", Configuration.class).getDeclaringClass());
		assertEquals(clazz, clazz.getMethod("addAll", Configuration.class, String.class).getDeclaringClass());
	}
}
