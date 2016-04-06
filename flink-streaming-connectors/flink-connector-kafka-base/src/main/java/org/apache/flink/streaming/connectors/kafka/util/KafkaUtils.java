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

package org.apache.flink.streaming.connectors.kafka.util;

import java.util.Properties;

public class KafkaUtils {

	public static int getIntFromConfig(Properties config, String key, int defaultValue) {
		try {
			return Integer.parseInt(config.getProperty(key, Integer.toString(defaultValue)));
		} catch(NumberFormatException nfe) {
			throw new IllegalArgumentException("Value for configuration key='" + key + "' is not set correctly. " +
					"Entered value='" + config.getProperty(key) + "'. Default value='" + defaultValue + "'");
		}
	}

	public static long getLongFromConfig(Properties config, String key, long defaultValue) {
		try {
			return Long.parseLong(config.getProperty(key, Long.toString(defaultValue)));
		} catch(NumberFormatException nfe) {
			throw new IllegalArgumentException("Value for configuration key='" + key + "' is not set correctly. " +
					"Entered value='" + config.getProperty(key) + "'. Default value='" + defaultValue + "'");
		}
	}
	
	// ------------------------------------------------------------------------
	
	/** Private default constructor to prevent instantiation */
	private KafkaUtils() {}
}
