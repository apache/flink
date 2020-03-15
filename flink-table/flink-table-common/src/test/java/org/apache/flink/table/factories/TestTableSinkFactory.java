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

package org.apache.flink.table.factories;

import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_TYPE;

/**
 * Test table sink factory.
 */
public class TestTableSinkFactory implements TableFactory, TableSinkFactory<Row> {

	public static final String CONNECTOR_TYPE_VALUE_TEST = "test";
	public static final String FORMAT_TYPE_VALUE_TEST = "test";
	public static final String FORMAT_PATH = "format.path";
	public static final String REQUIRED_TEST = "required.test";
	public static final String REQUIRED_TEST_VALUE = "required-0";

	@Override
	public TableSink<Row> createTableSink(Map<String, String> properties) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_TEST);
		context.put(FORMAT_TYPE, FORMAT_TYPE_VALUE_TEST);
		context.put(REQUIRED_TEST, REQUIRED_TEST_VALUE);
		context.put(CONNECTOR_PROPERTY_VERSION, "1");
		context.put(FORMAT_PROPERTY_VERSION, "1");
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();
		// connector
		properties.add(FORMAT_PATH);
		properties.add("schema.#.name");
		properties.add("schema.#.field.#.name");
		return properties;
	}
}
