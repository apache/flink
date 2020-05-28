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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FormatDescriptorValidator;
import org.apache.flink.table.types.DataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.DescriptorProperties.EXPR;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_ROWTIME;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_DATA_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_EXPR;

/**
 * Base class for {@link TableFormatFactory}s.
 *
 * @param <T> record type that the format produces or consumes.
 */
@PublicEvolving
public abstract class TableFormatFactoryBase<T> implements TableFormatFactory<T> {

	// Constants for schema derivation
	// TODO drop constants once SchemaValidator has been ported to flink-table-common
	private static final String SCHEMA = "schema";
	private static final String SCHEMA_NAME = "name";
	private static final String SCHEMA_DATA_TYPE = "data-type";
	/**
	 * @deprecated {@link #SCHEMA_TYPE} will be removed in future version as it uses old type system.
	 *  Please use {@link #SCHEMA_DATA_TYPE} instead.
	 */
	@Deprecated
	private static final String SCHEMA_TYPE = "type";
	private static final String SCHEMA_PROCTIME = "proctime";
	private static final String SCHEMA_FROM = "from";
	private static final String ROWTIME_TIMESTAMPS_TYPE = "rowtime.timestamps.type";
	private static final String ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_FIELD = "from-field";
	private static final String ROWTIME_TIMESTAMPS_FROM = "rowtime.timestamps.from";
	private static final String ROWTIME_TIMESTAMPS_CLASS = "rowtime.timestamps.class";
	private static final String ROWTIME_TIMESTAMPS_SERIALIZED = "rowtime.timestamps.serialized";
	private static final String ROWTIME_WATERMARKS_TYPE = "rowtime.watermarks.type";
	private static final String ROWTIME_WATERMARKS_CLASS = "rowtime.watermarks.class";
	private static final String ROWTIME_WATERMARKS_SERIALIZED = "rowtime.watermarks.serialized";
	private static final String ROWTIME_WATERMARKS_DELAY = "rowtime.watermarks.delay";

	private String type;

	private String version;

	private boolean supportsSchemaDerivation;

	public TableFormatFactoryBase(String type, int version, boolean supportsSchemaDerivation) {
		this.type = type;
		this.version = Integer.toString(version);
		this.supportsSchemaDerivation = supportsSchemaDerivation;
	}

	@Override
	public final Map<String, String> requiredContext() {
		final Map<String, String> context = new HashMap<>();
		context.put(FormatDescriptorValidator.FORMAT_TYPE, type);
		context.put(FormatDescriptorValidator.FORMAT_PROPERTY_VERSION, version);
		context.putAll(requiredFormatContext());
		return context;
	}

	@Override
	public final boolean supportsSchemaDerivation() {
		return supportsSchemaDerivation;
	}

	@Override
	public final List<String> supportedProperties() {
		final List<String> properties = new ArrayList<>();
		if (supportsSchemaDerivation) {
			properties.add(FormatDescriptorValidator.FORMAT_DERIVE_SCHEMA);
			// schema
			properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
			properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
			properties.add(SCHEMA + ".#." + SCHEMA_NAME);
			properties.add(SCHEMA + ".#." + SCHEMA_FROM);
			// computed column
			properties.add(SCHEMA + ".#." + EXPR);
			// time attributes
			properties.add(SCHEMA + ".#." + SCHEMA_PROCTIME);
			properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_TYPE);
			properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_FROM);
			properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_CLASS);
			properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_SERIALIZED);
			properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_TYPE);
			properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_CLASS);
			properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_SERIALIZED);
			properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_DELAY);
			// watermark
			properties.add(SCHEMA + "." + WATERMARK + ".#."  + WATERMARK_ROWTIME);
			properties.add(SCHEMA + "." + WATERMARK + ".#."  + WATERMARK_STRATEGY_EXPR);
			properties.add(SCHEMA + "." + WATERMARK + ".#."  + WATERMARK_STRATEGY_DATA_TYPE);
			// table constraint
			properties.add(SCHEMA + "." + DescriptorProperties.PRIMARY_KEY_NAME);
			properties.add(SCHEMA + "." + DescriptorProperties.PRIMARY_KEY_COLUMNS);
		}
		properties.addAll(supportedFormatProperties());
		return properties;
	}

	/**
	 * Format specific context.
	 *
	 * <p>This method can be used if format type and a property version is not enough.
	 */
	protected Map<String, String> requiredFormatContext() {
		return Collections.emptyMap();
	}

	/**
	 * Format specific supported properties.
	 *
	 * <p>This method can be used if schema derivation is not enough.
	 */
	protected List<String> supportedFormatProperties() {
		return Collections.emptyList();
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Finds the table schema that can be used for a format schema (without time attributes and generated columns).
	 */
	public static TableSchema deriveSchema(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties();
		descriptorProperties.putProperties(properties);

		final TableSchema.Builder builder = TableSchema.builder();

		final TableSchema tableSchema = descriptorProperties.getTableSchema(SCHEMA);
		for (int i = 0; i < tableSchema.getFieldCount(); i++) {
			final TableColumn tableColumn = tableSchema.getTableColumns().get(i);
			final String fieldName = tableColumn.getName();
			final DataType dataType = tableColumn.getType();
			final boolean isGeneratedColumn = tableColumn.isGenerated();
			if (isGeneratedColumn) {
				//skip generated column
				continue;
			}
			final boolean isProctime = descriptorProperties
				.getOptionalBoolean(SCHEMA + '.' + i + '.' + SCHEMA_PROCTIME)
				.orElse(false);
			final String timestampKey = SCHEMA + '.' + i + '.' + ROWTIME_TIMESTAMPS_TYPE;
			final boolean isRowtime = descriptorProperties.containsKey(timestampKey);
			if (!isProctime && !isRowtime) {
				// check for aliasing
				final String aliasName = descriptorProperties
					.getOptionalString(SCHEMA + '.' + i + '.' + SCHEMA_FROM)
					.orElse(fieldName);
				builder.field(aliasName, dataType);
			}
			// only use the rowtime attribute if it references a field
			else if (isRowtime &&
					descriptorProperties.isValue(timestampKey, ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_FIELD)) {
				final String aliasName = descriptorProperties
					.getString(SCHEMA + '.' + i + '.' + ROWTIME_TIMESTAMPS_FROM);
				builder.field(aliasName, dataType);
			}
		}

		return builder.build();
	}
}
