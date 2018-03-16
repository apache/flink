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

package org.apache.flink.mesos.cli;

import org.apache.flink.configuration.Configuration;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

/**
 * Class handling the command line interface to the Mesos session.
 */
public class FlinkMesosSessionCli {

	private static final ObjectMapper mapper = new ObjectMapper();

	/**
	 * Decode encoded dynamic properties.
	 *
	 * @param dynamicPropertiesEncoded encoded properties produced by the encoding method.
	 * @return a configuration instance to be merged with the static configuration.
	 */
	public static Configuration decodeDynamicProperties(String dynamicPropertiesEncoded) {
		try {
			Configuration configuration = new Configuration();
			if (dynamicPropertiesEncoded != null) {
				TypeReference<Map<String, String>> typeRef = new TypeReference<Map<String, String>>() {
				};
				Map<String, String> props = mapper.readValue(dynamicPropertiesEncoded, typeRef);
				for (Map.Entry<String, String> property : props.entrySet()) {
					configuration.setString(property.getKey(), property.getValue());
				}
			}
			return configuration;
		} catch (IOException ex) {
			throw new IllegalArgumentException("unreadable encoded properties", ex);
		}
	}

	/**
	 * Encode dynamic properties as a string to be transported as an environment variable.
	 *
	 * @param configuration the dynamic properties to encode.
	 * @return a string to be decoded later.
	 */
	public static String encodeDynamicProperties(Configuration configuration) {
		try {
			String dynamicPropertiesEncoded = mapper.writeValueAsString(configuration.toMap());
			return dynamicPropertiesEncoded;
		}
		catch (JsonProcessingException ex) {
			throw new IllegalArgumentException("unwritable properties", ex);
		}
	}
}
