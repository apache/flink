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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility for accessing deprecated {@link Configuration} values.
 */
@Internal
public class ConfigurationUtil {

	private static final Logger LOG = LoggerFactory.getLogger(ConfigurationUtil.class);

	/**
	 * Returns the value associated with the given key as an Integer in the
	 * given Configuration.
	 *
	 * <p>The regular key has precedence over any deprecated keys. The
	 * precedence of deprecated keys depends on the argument order, first
	 * deprecated keys have higher precedence than later ones.
	 *
	 * @param config Configuration to access
	 * @param key Configuration key (highest precedence)
	 * @param defaultValue Default value (if no key is set)
	 * @param deprecatedKeys Optional deprecated keys in precedence order
	 * @return Integer value associated with first found key or the default value
	 */
	public static int getIntegerWithDeprecatedKeys(
			Configuration config,
			String key,
			int defaultValue,
			String... deprecatedKeys) {

		if (config.containsKey(key)) {
			return config.getInteger(key, defaultValue);
		} else {
			// Check deprecated keys
			for (String deprecatedKey : deprecatedKeys) {
				if (config.containsKey(deprecatedKey)) {
					LOG.warn("Configuration key '{}' has been deprecated. Please use '{}' instead.", deprecatedKey, key);
					return config.getInteger(deprecatedKey, defaultValue);
				}
			}
			return defaultValue;
		}
	}

	/**
	 * Returns the value associated with the given key as a String in the
	 * given Configuration.
	 *
	 * <p>The regular key has precedence over any deprecated keys. The
	 * precedence of deprecated keys depends on the argument order, first
	 * deprecated keys have higher precedence than later ones.
	 *
	 * @param config Configuration to access
	 * @param key Configuration key (highest precedence)
	 * @param defaultValue Default value (if no key is set)
	 * @param deprecatedKeys Optional deprecated keys in precedence order
	 * @return String associated with first found key or the default value
	 */
	public static String getStringWithDeprecatedKeys(
			Configuration config,
			String key,
			String defaultValue,
			String... deprecatedKeys) {

		if (config.containsKey(key)) {
			return config.getString(key, defaultValue);
		} else {
			// Check deprecated keys
			for (String deprecatedKey : deprecatedKeys) {
				if (config.containsKey(deprecatedKey)) {
					LOG.warn("Configuration key {} has been deprecated. Please use {} instead.", deprecatedKey, key);
					return config.getString(deprecatedKey, defaultValue);
				}
			}
			return defaultValue;
		}
	}
}
