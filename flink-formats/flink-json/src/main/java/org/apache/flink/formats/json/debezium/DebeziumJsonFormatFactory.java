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

package org.apache.flink.formats.json.debezium;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Format factory for providing configured instances of Debezium JSON to RowData {@link DeserializationSchema}.
 */
public class DebeziumJsonFormatFactory implements DeserializationFormatFactory, SerializationFormatFactory {

	public static final String IDENTIFIER = "debezium-json";

	public static final ConfigOption<Boolean> SCHEMA_INCLUDE = ConfigOptions
		.key("schema-include")
		.booleanType()
		.defaultValue(false)
		.withDescription("When setting up a Debezium Kafka Connect, users can enable " +
			"a Kafka configuration 'value.converter.schemas.enable' to include schema in the message. " +
			"This option indicates the Debezium JSON data include the schema in the message or not. " +
			"Default is false.");

	public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS = JsonOptions.IGNORE_PARSE_ERRORS;

	public static final ConfigOption<String> TIMESTAMP_FORMAT = JsonOptions.TIMESTAMP_FORMAT;

	@Override
	public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
			DynamicTableFactory.Context context,
			ReadableConfig formatOptions) {

		FactoryUtil.validateFactoryOptions(this, formatOptions);

		final boolean schemaInclude = formatOptions.get(SCHEMA_INCLUDE);

		final boolean ignoreParseErrors = formatOptions.get(IGNORE_PARSE_ERRORS);

		final TimestampFormat timestampFormat = JsonOptions.getTimestampFormat(formatOptions);

		return new DebeziumJsonDecodingFormat(schemaInclude, ignoreParseErrors, timestampFormat);
	}

	@Override
	public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
			DynamicTableFactory.Context context,
			ReadableConfig formatOptions) {

		FactoryUtil.validateFactoryOptions(this, formatOptions);
		TimestampFormat timestampFormat = JsonOptions.getTimestampFormat(formatOptions);
		if (formatOptions.get(SCHEMA_INCLUDE)) {
			throw new ValidationException(String.format(
				"Debezium JSON serialization doesn't support '%s.%s' option been set to true.",
				IDENTIFIER,
				SCHEMA_INCLUDE.key()
			));
		}

		return new EncodingFormat<SerializationSchema<RowData>>() {

			@Override
			public ChangelogMode getChangelogMode() {
				return ChangelogMode.newBuilder()
					.addContainedKind(RowKind.INSERT)
					.addContainedKind(RowKind.UPDATE_BEFORE)
					.addContainedKind(RowKind.UPDATE_AFTER)
					.addContainedKind(RowKind.DELETE)
					.build();
			}

			@Override
			public SerializationSchema<RowData> createRuntimeEncoder(DynamicTableSink.Context context, DataType consumedDataType) {
				final RowType rowType = (RowType) consumedDataType.getLogicalType();
				return new DebeziumJsonSerializationSchema(rowType, timestampFormat);
			}
		};
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		return Collections.emptySet();
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> options = new HashSet<>();
		options.add(SCHEMA_INCLUDE);
		options.add(IGNORE_PARSE_ERRORS);
		options.add(TIMESTAMP_FORMAT);
		return options;
	}
}
