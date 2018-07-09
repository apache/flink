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
import org.apache.flink.table.descriptors.TableDescriptor;
import org.apache.flink.table.descriptors.TableDescriptorValidator;

import java.util.HashMap;
import java.util.Map;

/**
 * Common class for all descriptors describing a table source and sink together.
 */
public class SourceSink extends TableDescriptor {
	private String name;
	private Map<String, String> properties;

	protected SourceSink(String name, Map<String, String> properties) {
		this.name = name;
		this.properties = properties;
	}

	public String getName() {
		return name;
	}

	public Map<String, String> getProperties() {
		return properties;
	}

	@Override
	public void addProperties(DescriptorProperties properties) {
		this.properties.forEach(properties::putString);
	}

	public Source toSource() {
		final Map<String, String> newProperties = new HashMap<>(properties);
		newProperties.replace(TableDescriptorValidator.TABLE_TYPE(),
				TableDescriptorValidator.TABLE_TYPE_VALUE_SOURCE());
		return new Source(name, newProperties);
	}

	public Sink toSink() {
		final Map<String, String> newProperties = new HashMap<>(properties);
		newProperties.replace(TableDescriptorValidator.TABLE_TYPE(),
				TableDescriptorValidator.TABLE_TYPE_VALUE_SINK());
		return new Sink(name, newProperties);
	}
}
