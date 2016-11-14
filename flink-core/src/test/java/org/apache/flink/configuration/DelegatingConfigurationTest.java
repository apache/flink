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

import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Set;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;


public class DelegatingConfigurationTest {

	@Test
	public void testIfDelegatesImplementAllMethods() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {

		// For each method in the Configuration class...
		Method[] confMethods = Configuration.class.getDeclaredMethods();
		Method[] delegateMethods = DelegatingConfiguration.class.getDeclaredMethods();

		for (Method configurationMethod : confMethods) {
			if (!Modifier.isPublic(configurationMethod.getModifiers()) ) {
				continue;
			}

			boolean hasMethod = false;

			// Find matching method in wrapper class and call it
			lookForWrapper: for (Method wrapperMethod : delegateMethods) {
				if (configurationMethod.getName().equals(wrapperMethod.getName())) {

					// Get parameters for method
					Class<?>[] wrapperMethodParams = wrapperMethod.getParameterTypes();
					Class<?>[] configMethodParams = configurationMethod.getParameterTypes();
					if (wrapperMethodParams.length != configMethodParams.length) {
						continue;
					}

					for (int i = 0; i < wrapperMethodParams.length; i++) {
						if (wrapperMethodParams[i] != configMethodParams[i]) {
							continue lookForWrapper;
						}
					}
					hasMethod = true;
					break;
				}
			}

			assertTrue("Configuration method '" + configurationMethod.getName() + 
					"' has not been wrapped correctly in DelegatingConfiguration wrapper", hasMethod);
		}
	}
	
	@Test
	public void testDelegationConfigurationWithNullPrefix() {
		Configuration backingConf = new Configuration();
		backingConf.setValueInternal("test-key", "value");

		DelegatingConfiguration configuration = new DelegatingConfiguration(
				backingConf, null);
		Set<String> keySet = configuration.keySet();

		assertEquals(keySet, backingConf.keySet());

	}

	@Test
	public void testDelegationConfigurationWithPrefix() {
		String prefix = "pref-";
		String expectedKey = "key";

		/*
		 * Key matches the prefix
		 */
		Configuration backingConf = new Configuration();
		backingConf.setValueInternal(prefix + expectedKey, "value");

		DelegatingConfiguration configuration = new DelegatingConfiguration(backingConf, prefix);
		Set<String> keySet = configuration.keySet();
		

		assertEquals(keySet.size(), 1);
		assertEquals(keySet.iterator().next(), expectedKey);

		/*
		 * Key does not match the prefix
		 */
		backingConf = new Configuration();
		backingConf.setValueInternal("test-key", "value");

		configuration = new DelegatingConfiguration(backingConf, prefix);
		keySet = configuration.keySet();

		assertTrue(keySet.isEmpty());
	}
}
