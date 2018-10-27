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

package org.apache.flink.table.client.gateway.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME_TIMESTAMPS_FROM;
import static org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME_TIMESTAMPS_TYPE;
import static org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME_WATERMARKS_TYPE;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA_TYPE;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_APPEND;

/**
 * Table source factory for testing.
 */
public abstract class TestTableSourceFactoryBase implements StreamTableSourceFactory<Row> {

	private String type;
	private String testProperty;

	public TestTableSourceFactoryBase(String type, String testProperty) {
		this.type = type;
		this.testProperty = testProperty;
	}

	@Override
	public Map<String, String> requiredContext() {
		final Map<String, String> context = new HashMap<>();
		context.put(UPDATE_MODE(), UPDATE_MODE_VALUE_APPEND());
		context.put(CONNECTOR_TYPE, type);
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		final List<String> properties = new ArrayList<>();
		properties.add("connector." + testProperty);
		properties.add(SCHEMA() + ".#." + SCHEMA_TYPE());
		properties.add(SCHEMA() + ".#." + SCHEMA_NAME());
		properties.add(SCHEMA() + ".#." + ROWTIME_TIMESTAMPS_TYPE());
		properties.add(SCHEMA() + ".#." + ROWTIME_TIMESTAMPS_FROM());
		properties.add(SCHEMA() + ".#." + ROWTIME_WATERMARKS_TYPE());
		return properties;
	}

	@Override
	public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
		final DescriptorProperties params = new DescriptorProperties(true);
		params.putProperties(properties);
		final Optional<String> proctime = SchemaValidator.deriveProctimeAttribute(params);
		final List<RowtimeAttributeDescriptor> rowtime = SchemaValidator.deriveRowtimeAttributes(params);
		return new TestTableSource(
			params.getTableSchema(SCHEMA()),
			properties.get(testProperty),
			proctime.orElse(null),
			rowtime);
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Test table source.
	 */
	public static class TestTableSource implements StreamTableSource<Row>, DefinedRowtimeAttributes, DefinedProctimeAttribute {

		private final TableSchema schema;
		private final String property;
		private final String proctime;
		private final List<RowtimeAttributeDescriptor> rowtime;

		public TestTableSource(TableSchema schema, String property, String proctime, List<RowtimeAttributeDescriptor> rowtime) {
			this.schema = schema;
			this.property = property;
			this.proctime = proctime;
			this.rowtime = rowtime;
		}

		public String getProperty() {
			return property;
		}

		@Override
		public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
			return null;
		}

		@Override
		public TypeInformation<Row> getReturnType() {
			return Types.ROW(schema.getFieldNames(), schema.getFieldTypes());
		}

		@Override
		public TableSchema getTableSchema() {
			return schema;
		}

		@Override
		public String explainSource() {
			return "TestTableSource";
		}

		@Override
		public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
			return rowtime;
		}

		@Override
		public String getProctimeAttribute() {
			return proctime;
		}
	}
}
