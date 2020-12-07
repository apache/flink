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
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FileSystemValidator;
import org.apache.flink.table.descriptors.FormatDescriptorValidator;
import org.apache.flink.table.descriptors.OldCsvValidator;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.FileSystemValidator.CONNECTOR_PATH;
import static org.apache.flink.table.descriptors.FileSystemValidator.CONNECTOR_TYPE_VALUE;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_TYPE;
import static org.apache.flink.table.descriptors.OldCsvValidator.FORMAT_FIELDS;
import static org.apache.flink.table.descriptors.OldCsvValidator.FORMAT_FIELD_DELIMITER;
import static org.apache.flink.table.descriptors.OldCsvValidator.FORMAT_NUM_FILES;
import static org.apache.flink.table.descriptors.OldCsvValidator.FORMAT_TYPE_VALUE;
import static org.apache.flink.table.descriptors.OldCsvValidator.FORMAT_WRITE_MODE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.sources.CsvTableSourceFactoryBase.getFieldLogicalTypes;

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
		properties.add(FORMAT_FIELDS + ".#." + DescriptorProperties.TYPE);
		properties.add(FORMAT_FIELDS + ".#." + DescriptorProperties.DATA_TYPE);
		properties.add(FORMAT_FIELDS + ".#." + DescriptorProperties.NAME);
		properties.add(FormatDescriptorValidator.FORMAT_DERIVE_SCHEMA);
		properties.add(FORMAT_FIELD_DELIMITER);
		properties.add(CONNECTOR_PATH);
		properties.add(FORMAT_WRITE_MODE);
		properties.add(FORMAT_NUM_FILES);

		// schema
		properties.add(SCHEMA + ".#." + DescriptorProperties.TYPE);
		properties.add(SCHEMA + ".#." + DescriptorProperties.DATA_TYPE);
		properties.add(SCHEMA + ".#." + DescriptorProperties.NAME);
		properties.add(SCHEMA + ".#." + DescriptorProperties.EXPR);
		// schema watermark
		properties.add(SCHEMA + "." + DescriptorProperties.WATERMARK + ".*");
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
		TableSchema tableSchema = TableSchemaUtils.getPhysicalSchema(params.getTableSchema(SCHEMA));

		// if a schema is defined, no matter derive schema is set or not, will use the defined schema
		final boolean hasSchema = params.hasPrefix(FORMAT_FIELDS);
		if (hasSchema) {
			TableSchema formatSchema = params.getTableSchema(FORMAT_FIELDS);
			if (!getFieldLogicalTypes(formatSchema).equals(getFieldLogicalTypes(tableSchema))) {
				throw new TableException(String.format(
						"Encodings that differ from the schema are not supported yet for" +
								" CsvTableSink, format schema is '%s', but table schema is '%s'.",
						formatSchema,
						tableSchema));
			}
		}

		String path = params.getString(CONNECTOR_PATH);
		String fieldDelimiter = params.getOptionalString(FORMAT_FIELD_DELIMITER).orElse(",");
		Optional<String> writeModeParam = params.getOptionalString(FORMAT_WRITE_MODE);
		FileSystem.WriteMode writeMode =
				(writeModeParam.isPresent()) ? FileSystem.WriteMode.valueOf(writeModeParam.get()) : null;
		int numFiles = params.getOptionalInt(FORMAT_NUM_FILES).orElse(-1);

		// bridge to java.sql.Timestamp/Time/Date
		DataType[] dataTypes = Arrays.stream(tableSchema.getFieldDataTypes())
			.map(dt -> {
				switch (dt.getLogicalType().getTypeRoot()) {
					case TIMESTAMP_WITHOUT_TIME_ZONE:
						return dt.bridgedTo(Timestamp.class);
					case TIME_WITHOUT_TIME_ZONE:
						return dt.bridgedTo(Time.class);
					case DATE:
						return dt.bridgedTo(Date.class);
					default:
						return dt;
				}
			})
			.toArray(DataType[]::new);

		return new CsvTableSink(
			path,
			fieldDelimiter,
			numFiles,
			writeMode,
			tableSchema.getFieldNames(),
			dataTypes);
	}

}
