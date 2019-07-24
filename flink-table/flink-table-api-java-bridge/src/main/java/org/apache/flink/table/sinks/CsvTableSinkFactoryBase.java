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

package org.apache.flink.table.sinks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FileSystemValidator;
import org.apache.flink.table.descriptors.OldCsvValidator;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.TableFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.FileSystemValidator.CONNECTOR_PATH;
import static org.apache.flink.table.descriptors.FileSystemValidator.CONNECTOR_TYPE_VALUE;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_TYPE;
import static org.apache.flink.table.descriptors.OldCsvValidator.FORMAT_FIELDS;
import static org.apache.flink.table.descriptors.OldCsvValidator.FORMAT_FIELD_DELIMITER;
import static org.apache.flink.table.descriptors.OldCsvValidator.FORMAT_TYPE_VALUE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;

/**
 * Factory base for creating configured instances of {@link CsvTableSink}.
 */
@Internal
public abstract class CsvTableSinkFactoryBase implements TableFactory {

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE);
		context.put(FORMAT_TYPE, FORMAT_TYPE_VALUE);
		context.put(CONNECTOR_PROPERTY_VERSION, "1");
		context.put(FORMAT_PROPERTY_VERSION, "1");
		return context;
	}

	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();
		// connector
		properties.add(CONNECTOR_PATH);
		// format
		properties.add(FORMAT_FIELDS + ".#." + DescriptorProperties.TABLE_SCHEMA_TYPE);
		properties.add(FORMAT_FIELDS + ".#." + DescriptorProperties.TABLE_SCHEMA_NAME);
		properties.add(FORMAT_FIELD_DELIMITER);
		properties.add(CONNECTOR_PATH);
		// schema
		properties.add(SCHEMA + ".#." + DescriptorProperties.TABLE_SCHEMA_TYPE);
		properties.add(SCHEMA + ".#." + DescriptorProperties.TABLE_SCHEMA_NAME);
		return properties;
	}

	protected CsvTableSink createTableSink(
			Boolean isStreaming,
			Map<String, String> properties) {

		DescriptorProperties params = new DescriptorProperties();
		params.putProperties(properties);

		// validate
		new FileSystemValidator().validate(params);
		new OldCsvValidator().validate(params);
		new SchemaValidator(isStreaming, false, false).validate(params);

		// build
		TableSchema formatSchema = params.getTableSchema(FORMAT_FIELDS);
		TableSchema tableSchema = params.getTableSchema(SCHEMA);

		if (!formatSchema.equals(tableSchema)) {
			throw new TableException(
					"Encodings that differ from the schema are not supported yet for CsvTableSink.");
		}

		String path = params.getString(CONNECTOR_PATH);
		String fieldDelimiter = params.getOptionalString(FORMAT_FIELD_DELIMITER).orElse(",");

		CsvTableSink csvTableSink = new CsvTableSink(path, fieldDelimiter);

		return (CsvTableSink) csvTableSink.configure(formatSchema.getFieldNames(), formatSchema.getFieldTypes());
	}

}
