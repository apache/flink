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

package org.apache.flink.streaming.connectors.twitter;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Provides utility methods to use when working with properties files.
 */
public final class PropertiesUtil {

	private PropertiesUtil() {}

	/**
	 * Loads properties from the given file path.
	 *
	 * @param path the path to the properties file.
	 *
	 * @return the loaded properties.
	 */
	public static Properties load(final String path) {
		InputStream input = null;
		try {
			input = new FileInputStream(path);
			Properties properties = new Properties();
			properties.load(input);
			return properties;
		} catch (Exception e) {
			throw new RuntimeException("Cannot load the properties file with path [" + path + "].", e);
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {}				
			}
		}
	}	
	
}
