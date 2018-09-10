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

package org.apache.flink.table.client.config;

import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FunctionDescriptor;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.client.config.ConfigUtil.extractEarlyStringProperty;

/**
 * Descriptor for user-defined functions.
 */
public class UserDefinedFunction extends FunctionDescriptor {

	private String name;
	private Map<String, String> properties;

	private static final String FUNCTION_NAME = "name";

	private UserDefinedFunction(String name, Map<String, String> properties) {
		this.name = name;
		this.properties = properties;
	}

	public String getName() {
		return name;
	}

	public Map<String, String> getProperties() {
		return properties;
	}

	/**
	 * Creates a user-defined function descriptor with the given config.
	 */
	public static UserDefinedFunction create(Map<String, Object> config) {
		final String name = extractEarlyStringProperty(config, FUNCTION_NAME, "function");
		final Map<String, Object> properties = new HashMap<>(config);
		properties.remove(FUNCTION_NAME);
		return new UserDefinedFunction(name, ConfigUtil.normalizeYaml(properties));
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public void addProperties(DescriptorProperties properties) {
		this.properties.forEach(properties::putString);
	}
}
