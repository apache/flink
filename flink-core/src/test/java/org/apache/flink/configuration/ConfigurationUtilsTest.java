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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link ConfigurationUtils}.
 */
public class ConfigurationUtilsTest extends TestLogger {

	@Test
	public void testPropertiesToConfiguration() {
		final Properties properties = new Properties();
		final int entries = 10;

		for (int i = 0; i < entries; i++) {
			properties.setProperty("key" + i, "value" + i);
		}

		final Configuration configuration = ConfigurationUtils.createConfiguration(properties);

		for (String key : properties.stringPropertyNames()) {
			assertThat(configuration.getString(key, ""), is(equalTo(properties.getProperty(key))));
		}

		assertThat(configuration.toMap().size(), is(properties.size()));
	}

	@Test
		public void testHideSensitiveValues() {
		final Map<String, String> keyValuePairs = new HashMap<>();
		keyValuePairs.put("foobar", "barfoo");
		final String secretKey1 = "secret.key";
		keyValuePairs.put(secretKey1, "12345");
		final String secretKey2 = "my.password";
		keyValuePairs.put(secretKey2, "12345");

		final Map<String, String> expectedKeyValuePairs = new HashMap<>(keyValuePairs);

		for (String secretKey : Arrays.asList(secretKey1, secretKey2)) {
			expectedKeyValuePairs.put(secretKey, GlobalConfiguration.HIDDEN_CONTENT);
		}

		final Map<String, String> hiddenSensitiveValues = ConfigurationUtils.hideSensitiveValues(keyValuePairs);

		assertThat(hiddenSensitiveValues, is(equalTo(expectedKeyValuePairs)));
	}

}
