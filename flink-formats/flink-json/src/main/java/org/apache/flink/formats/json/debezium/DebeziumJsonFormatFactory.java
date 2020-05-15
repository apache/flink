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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.ScanFormat;
import org.apache.flink.table.connector.format.SinkFormat;
import org.apache.flink.table.connector.source.ScanTableSource;
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

	public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS = ConfigOptions
		.key("ignore-parse-errors")
		.booleanType()
		.defaultValue(false)
		.withDescription("Optional flag to skip fields and rows with parse errors instead of failing, " +
			"fields are set to null in case of errors. Default is false.");

	@SuppressWarnings("unchecked")
	@Override
	public ScanFormat<DeserializationSchema<RowData>> createScanFormat(
			DynamicTableFactory.Context context,
			ReadableConfig formatOptions) {
		FactoryUtil.validateFactoryOptions(this, formatOptions);
		final boolean schemaInclude = formatOptions.get(SCHEMA_INCLUDE);
		final boolean ignoreParseErrors = formatOptions.get(IGNORE_PARSE_ERRORS);

		return new ScanFormat<DeserializationSchema<RowData>>() {
			@Override
			public DeserializationSchema<RowData> createScanFormat(ScanTableSource.Context context, DataType producedDataType) {
				final RowType rowType = (RowType) producedDataType.getLogicalType();
				final TypeInformation<RowData> rowDataTypeInfo =
					(TypeInformation<RowData>) context.createTypeInformation(producedDataType);
				return new DebeziumJsonDeserializationSchema(
					rowType,
					rowDataTypeInfo,
					schemaInclude,
					ignoreParseErrors);
			}

			@Override
			public ChangelogMode getChangelogMode() {
				return ChangelogMode.newBuilder()
					.addContainedKind(RowKind.INSERT)
					.addContainedKind(RowKind.UPDATE_BEFORE)
					.addContainedKind(RowKind.UPDATE_AFTER)
					.addContainedKind(RowKind.DELETE)
					.build();
			}
		};
	}

	@Override
	public SinkFormat<SerializationSchema<RowData>> createSinkFormat(
			DynamicTableFactory.Context context,
			ReadableConfig formatOptions) {
		throw new UnsupportedOperationException("Debezium format doesn't support as a sink format yet.");
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
		return options;
	}

}
