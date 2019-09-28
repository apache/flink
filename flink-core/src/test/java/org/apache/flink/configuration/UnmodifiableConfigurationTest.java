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

import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This class verifies that the Unmodifiable Configuration class overrides all setter methods in
 * Configuration.
 */
public class UnmodifiableConfigurationTest extends TestLogger {

	@Test
	public void testOverrideAddMethods() {
		try {
			Class<UnmodifiableConfiguration> clazz = UnmodifiableConfiguration.class;
			for (Method m : clazz.getMethods()) {
				if (m.getName().startsWith("add")) {
					assertEquals(clazz, m.getDeclaringClass());
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testExceptionOnSet() {
		try {
			@SuppressWarnings("rawtypes")
			final ConfigOption rawOption = ConfigOptions.key("testkey").defaultValue("value");

			Map<Class<?>, Object> parameters = new HashMap<>();
			parameters.put(byte[].class, new byte[0]);
			parameters.put(Class.class, Object.class);
			parameters.put(int.class, 0);
			parameters.put(long.class, 0L);
			parameters.put(float.class, 0.0f);
			parameters.put(double.class, 0.0);
			parameters.put(String.class, "");
			parameters.put(boolean.class, false);

			Class<UnmodifiableConfiguration> clazz = UnmodifiableConfiguration.class;
			UnmodifiableConfiguration config = new UnmodifiableConfiguration(new Configuration());

			for (Method m : clazz.getMethods()) {
				if (m.getName().startsWith("set")) {

					Class<?> keyClass = m.getParameterTypes()[0];
					Class<?> parameterClass = m.getParameterTypes()[1];
					Object key = keyClass == String.class ? "key" : rawOption;

					Object parameter = parameters.get(parameterClass);
					assertNotNull("method " + m + " not covered by test", parameter);

					try {
						m.invoke(config, key, parameter);
						fail("should fail with an exception");
					}
					catch (InvocationTargetException e) {
						assertTrue(e.getTargetException() instanceof UnsupportedOperationException);
					}
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
