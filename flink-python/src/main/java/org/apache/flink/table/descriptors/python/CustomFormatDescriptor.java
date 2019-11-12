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
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FormatDescriptor;

import java.util.Map;

/**
 * Describes the custom format of data.
 */
@Internal
public class CustomFormatDescriptor extends FormatDescriptor {

	private final DescriptorProperties properties;

	/**
	 * Constructs a {@link CustomFormatDescriptor}.
	 *
	 * @param type String that identifies this format.
	 * @param version Property version for backwards compatibility.
	 */
	public CustomFormatDescriptor(String type, int version) {
		super(type, version);
		properties = new DescriptorProperties();
	}

	/**
	 * Adds a configuration property for the format.
	 *
	 * @param key The property key to be set.
	 * @param value The property value to be set.
	 */
	public CustomFormatDescriptor property(String key, String value) {
		properties.putString(key, value);
		return this;
	}

	/**
	 * Adds a set of properties for the format.
	 *
	 * @param properties The properties to add.
	 */
	public CustomFormatDescriptor properties(Map<String, String> properties) {
		this.properties.putProperties(properties);
		return this;
	}

	@Override
	protected Map<String, String> toFormatProperties() {
		return properties.asMap();
	}
}
