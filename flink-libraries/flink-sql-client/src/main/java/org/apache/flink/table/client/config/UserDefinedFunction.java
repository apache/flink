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

import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FunctionDescriptor;
import org.apache.flink.table.descriptors.FunctionValidator;

import java.util.Map;

import static org.apache.flink.table.client.config.UserDefinedFunction.From.CLASS;

/**
 * Descriptor for user-defined functions.
 */
public class UserDefinedFunction extends FunctionDescriptor {

	private static final String FROM = "from";

	private From from;

	private Map<String, String> properties;

	private UserDefinedFunction(String name, From from, Map<String, String> properties) {
		super(name);
		this.from = from;
		this.properties = properties;
	}

	/**
	 * Gets where the user-defined function should be created from.
	 */
	public From getFrom() {
		return from;
	}

	/**
	 * Creates a UDF descriptor with the given config.
	 */
	public static UserDefinedFunction create(Map<String, Object> config) {
		Map<String, String> udfConfig = ConfigUtil.normalizeYaml(config);
		if (!udfConfig.containsKey(FunctionValidator.FUNCTION_NAME())) {
			throw new SqlClientException("The 'name' attribute of a function is missing.");
		}

		final String name = udfConfig.get(FunctionValidator.FUNCTION_NAME());
		if (name.trim().length() <= 0) {
			throw new SqlClientException("Invalid function name '" + name + "'.");
		}

		// the default value is "CLASS"
		From fromValue = CLASS;

		if (udfConfig.containsKey(FROM)) {
			final String from = udfConfig.get(FROM);
			try {
				fromValue = From.valueOf(from.toUpperCase());
			} catch (IllegalArgumentException ex) {
				throw new SqlClientException("Unknown 'from' value '" + from + "'.");
			}
		}

		switch (fromValue) {
			case CLASS:
				return new UserDefinedFunction(name, fromValue, udfConfig);
			default:
				throw new SqlClientException("The from attribute can only be \"class\" now.");
		}
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public void addProperties(DescriptorProperties properties) {
		this.properties.forEach(properties::putString);
	}

	enum From {
		CLASS
	}
}
