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

package org.apache.flink.table.sources;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.TABLE_SCHEMA_EXPR;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_ROWTIME;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_DATA_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_EXPR;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;

/**
 * Factory for creating configured instances of {@link CsvTableSource} in a stream environment.
 */
@PublicEvolving
public class SequenceTableSourceFactory implements TableSourceFactory<Long> {

	public static final String ID_FIELD_NAME = "id";
	public static final String CONNECTOR_TYPE_VALUE = "sequence";
	public static final String CONNECTOR_START = "connector.start";
	public static final String CONNECTOR_END = "connector.end";
	public static final String CONNECTOR_ROWS_PER_SECOND = "connector.rows-per-second";

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE);
		context.put(CONNECTOR_PROPERTY_VERSION, "1");
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();
		// connector
		properties.add(CONNECTOR_START);
		properties.add(CONNECTOR_END);
		properties.add(CONNECTOR_ROWS_PER_SECOND);
		// schema
		properties.add(SCHEMA + ".#." + DescriptorProperties.TABLE_SCHEMA_TYPE);
		properties.add(SCHEMA + ".#." + DescriptorProperties.TABLE_SCHEMA_DATA_TYPE);
		properties.add(SCHEMA + ".#." + DescriptorProperties.TABLE_SCHEMA_NAME);
		properties.add(SCHEMA + ".#." + TABLE_SCHEMA_EXPR);
		// watermark
		properties.add(SCHEMA + "." + WATERMARK + ".#."  + WATERMARK_ROWTIME);
		properties.add(SCHEMA + "." + WATERMARK + ".#."  + WATERMARK_STRATEGY_EXPR);
		properties.add(SCHEMA + "." + WATERMARK + ".#."  + WATERMARK_STRATEGY_DATA_TYPE);
		return properties;
	}

	@Override
	public TableSource<Long> createTableSource(Context context) {
		validateSchema(context.getTable().getSchema());

		DescriptorProperties params = new DescriptorProperties();
		params.putProperties(context.getTable().getProperties());

		return new SequenceTableSource(
				params.getLong(CONNECTOR_START),
				params.getLong(CONNECTOR_END),
				params.getLong(CONNECTOR_ROWS_PER_SECOND));
	}

	private void validateSchema(TableSchema schema) {
		DataType idType = schema.getFieldDataType(ID_FIELD_NAME).orElseThrow(
				() -> new ValidationException("SequenceTableSource should contains id field."));
		if (idType.getLogicalType().getTypeRoot() != LogicalTypeRoot.BIGINT) {
			throw new ValidationException("SequenceTableSource id field should be bigint.");
		}

		schema.getTableColumns().forEach(tableColumn -> {
			if (!tableColumn.getName().equals(ID_FIELD_NAME) && !tableColumn.isGenerated()) {
				throw new ValidationException(
						"SequenceTableSource fields except id should all be generated field.");
			}
		});
	}
}
