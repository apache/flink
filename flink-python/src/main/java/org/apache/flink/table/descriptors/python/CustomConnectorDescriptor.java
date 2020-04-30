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

package org.apache.flink.table.descriptors.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;

import java.util.Map;

/**
 * Describes a custom connector to an other system.
 */
@Internal
public class CustomConnectorDescriptor extends ConnectorDescriptor {

	private final DescriptorProperties properties;

	/**
	 * Constructs a {@link CustomConnectorDescriptor}.
	 *
	 * @param type String that identifies this connector.
	 * @param version Property version for backwards compatibility.
	 * @param formatNeeded Flag for basic validation of a needed format descriptor.
	 */
	public CustomConnectorDescriptor(String type, int version, boolean formatNeeded) {
		super(type, version, formatNeeded);
		properties = new DescriptorProperties();
	}

	/**
	 * Adds a configuration property for the connector.
	 *
	 * @param key The property key to be set.
	 * @param value The property value to be set.
	 */
	public CustomConnectorDescriptor property(String key, String value) {
		properties.putString(key, value);
		return this;
	}

	/**
	 * Adds a set of properties for the connector.
	 *
	 * @param properties The properties to add.
	 */
	public CustomConnectorDescriptor properties(Map<String, String> properties) {
		this.properties.putProperties(properties);
		return this;
	}

	@Override
	protected Map<String, String> toConnectorProperties() {
		return properties.asMap();
	}
}
