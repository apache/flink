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

package org.apache.flink.table.utils;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FormatDescriptorValidator;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.descriptors.StreamTableDescriptorValidator;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.DescriptorProperties.TABLE_SCHEMA_EXPR;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_ROWTIME;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_DATA_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_EXPR;

/**
 * Mocking {@link TableSourceFactory} for tests.
 */
public class TableSourceFactoryMock implements TableSourceFactory<Row> {

	public static final String CONNECTOR_TYPE_VALUE = "table-source-factory-mock";

	@Override
	public TableSource<Row> createTableSource(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties();
		descriptorProperties.putProperties(properties);
		final TableSchema schema = TableSchemaUtils.getPhysicalSchema(
			descriptorProperties.getTableSchema(Schema.SCHEMA));
		return new TableSourceMock(schema);
	}

	@Override
	public TableSource<Row> createTableSource(ObjectPath tablePath, CatalogTable table) {
		return new TableSourceMock(TableSchemaUtils.getPhysicalSchema(table.getSchema()));
	}

	@Override
	public Map<String, String> requiredContext() {
		final Map<String, String> context = new HashMap<>();
		context.put(ConnectorDescriptorValidator.CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE);
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		final List<String> supportedProperties = new ArrayList<>();
		supportedProperties.add(StreamTableDescriptorValidator.UPDATE_MODE);
		supportedProperties.add(ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION);
		supportedProperties.add(FormatDescriptorValidator.FORMAT + ".*");
		supportedProperties.add(Schema.SCHEMA + ".#." + Schema.SCHEMA_NAME);
		supportedProperties.add(Schema.SCHEMA + ".#." + Schema.SCHEMA_DATA_TYPE);
		supportedProperties.add(Schema.SCHEMA + ".#." + Schema.SCHEMA_TYPE);
		// computed column
		supportedProperties.add(Schema.SCHEMA + ".#." + TABLE_SCHEMA_EXPR);
		// watermark
		supportedProperties.add(Schema.SCHEMA + "." + WATERMARK + ".#."  + WATERMARK_ROWTIME);
		supportedProperties.add(Schema.SCHEMA + "." + WATERMARK + ".#."  + WATERMARK_STRATEGY_EXPR);
		supportedProperties.add(Schema.SCHEMA + "." + WATERMARK + ".#."  + WATERMARK_STRATEGY_DATA_TYPE);
		return supportedProperties;
	}
}
