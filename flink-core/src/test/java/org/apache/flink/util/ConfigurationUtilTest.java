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

import org.apache.flink.configuration.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ConfigurationUtilTest {

	/**
	 * Tests getInteger without any deprecated keys.
	 */
	@Test
	public void testGetIntegerNoDeprecatedKeys() throws Exception {
		Configuration config = new Configuration();
		String key = "asdasd";
		int value = 1223239;
		int defaultValue = 272770;

		assertEquals(defaultValue, ConfigurationUtil.getIntegerWithDeprecatedKeys(config, key, defaultValue));

		config.setInteger(key, value);
		assertEquals(value, ConfigurationUtil.getIntegerWithDeprecatedKeys(config, key, defaultValue));
	}

	/**
	 * Tests getInteger with deprecated keys and checks precedence.
	 */
	@Test
	public void testGetIntegerWithDeprecatedKeys() throws Exception {
		Configuration config = new Configuration();
		String key = "asdasd";
		int value = 1223239;
		int defaultValue = 272770;

		String[] deprecatedKey = new String[] { "deprecated-0", "deprecated-1" };
		int[] deprecatedValue = new int[] { 99192, 7727 };

		assertEquals(defaultValue, ConfigurationUtil.getIntegerWithDeprecatedKeys(config, key, defaultValue));

		// Set 2nd deprecated key
		config.setInteger(deprecatedKey[1], deprecatedValue[1]);
		assertEquals(deprecatedValue[1], ConfigurationUtil.getIntegerWithDeprecatedKeys(config, key, defaultValue, deprecatedKey[1]));

		// Set 1st deprecated key (precedence)
		config.setInteger(deprecatedKey[0], deprecatedValue[0]);
		assertEquals(deprecatedValue[0], ConfigurationUtil.getIntegerWithDeprecatedKeys(config, key, defaultValue, deprecatedKey[0]));

		// Set current key
		config.setInteger(key, value);
		assertEquals(value, ConfigurationUtil.getIntegerWithDeprecatedKeys(config, key, defaultValue));
	}

	/**
	 * Tests getString without any deprecated keys.
	 */
	@Test
	public void testGetStringNoDeprecatedKeys() throws Exception {
		Configuration config = new Configuration();
		String key = "asdasd";
		String value = "1223239";
		String defaultValue = "272770";

		assertEquals(defaultValue, ConfigurationUtil.getStringWithDeprecatedKeys(config, key, defaultValue));

		config.setString(key, value);
		assertEquals(value, ConfigurationUtil.getStringWithDeprecatedKeys(config, key, defaultValue));
	}

	/**
	 * Tests getString with deprecated keys and checks precedence.
	 */
	@Test
	public void testGetStringWithDeprecatedKeys() throws Exception {
		Configuration config = new Configuration();
		String key = "asdasd";
		String value = "1223239";
		String defaultValue = "272770";

		String[] deprecatedKey = new String[] { "deprecated-0", "deprecated-1" };
		String[] deprecatedValue = new String[] { "99192", "7727" };

		assertEquals(defaultValue, ConfigurationUtil.getStringWithDeprecatedKeys(config, key, defaultValue));

		// Set 2nd deprecated key
		config.setString(deprecatedKey[1], deprecatedValue[1]);
		assertEquals(deprecatedValue[1], ConfigurationUtil.getStringWithDeprecatedKeys(config, key, defaultValue, deprecatedKey[1]));

		// Set 1st deprecated key (precedence)
		config.setString(deprecatedKey[0], deprecatedValue[0]);
		assertEquals(deprecatedValue[0], ConfigurationUtil.getStringWithDeprecatedKeys(config, key, defaultValue, deprecatedKey[0]));

		// Set current key
		config.setString(key, value);
		assertEquals(value, ConfigurationUtil.getStringWithDeprecatedKeys(config, key, defaultValue));
	}
}
