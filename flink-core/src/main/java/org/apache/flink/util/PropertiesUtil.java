/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util;

import org.slf4j.Logger;

import java.util.Properties;

/**
 * Simple utilities for getting typed values from Properties.
 */
public class PropertiesUtil {

	/**
	 * Get integer from properties.
	 * This method throws an exception if the integer is not valid.
	 *
	 * @param config Properties
	 * @param key key in Properties
	 * @param defaultValue default value if value is not set
	 * @return default or value of key
	 * @throws IllegalArgumentException
	 */
	public static int getInt(Properties config, String key, int defaultValue) {
		String val = config.getProperty(key);
		if (val == null) {
			return defaultValue;
		} else {
			try {
				return Integer.parseInt(val);
			} catch (NumberFormatException nfe) {
				throw new IllegalArgumentException("Value for configuration key='" + key + "' is not set correctly. " +
						"Entered value='" + val + "'. Default value='" + defaultValue + "'");
			}
		}
	}

	/**
	 * Get long from properties.
	 * This method throws an exception if the long is not valid.
	 *
	 * @param config Properties
	 * @param key key in Properties
	 * @param defaultValue default value if value is not set
	 * @return default or value of key
	 * @throws IllegalArgumentException
	 */
	public static long getLong(Properties config, String key, long defaultValue) {
		String val = config.getProperty(key);
		if (val == null) {
			return defaultValue;
		} else {
			try {
				return Long.parseLong(val);
			} catch (NumberFormatException nfe) {
				throw new IllegalArgumentException("Value for configuration key='" + key + "' is not set correctly. " +
						"Entered value='" + val + "'. Default value='" + defaultValue + "'");
			}
		}
	}

	/**
	 * Get long from properties.
	 * This method only logs if the long is not valid.
	 *
	 * @param config Properties
	 * @param key key in Properties
	 * @param defaultValue default value if value is not set
	 * @return default or value of key
	 * @throws IllegalArgumentException
	 */
	public static long getLong(Properties config, String key, long defaultValue, Logger logger) {
		try {
			return getLong(config, key, defaultValue);
		} catch(IllegalArgumentException iae) {
			logger.warn(iae.getMessage());
			return defaultValue;
		}
	}

	// ------------------------------------------------------------------------
	
	/** Private default constructor to prevent instantiation */
	private PropertiesUtil() {}
}
