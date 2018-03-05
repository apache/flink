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
import org.apache.flink.table.client.gateway.local.DependencyTest;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceFactory;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA_TYPE;

/**
 * Table source factory for testing the classloading in {@link DependencyTest}.
 */
public class TestTableSourceFactory implements TableSourceFactory<Row> {

	@Override
	public Map<String, String> requiredContext() {
		final Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE(), "test-table-source-factory");
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		final List<String> properties = new ArrayList<>();
		properties.add("connector.test-property");
		properties.add(SCHEMA() + ".#." + SCHEMA_TYPE());
		properties.add(SCHEMA() + ".#." + SCHEMA_NAME());
		return properties;
	}

	@Override
	public TableSource<Row> create(Map<String, String> properties) {
		final DescriptorProperties params = new DescriptorProperties(true);
		params.putProperties(properties);
		return new TestTableSource(
			params.getTableSchema(SCHEMA()),
			properties.get("connector.test-property"));
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Test table source.
	 */
	public static class TestTableSource implements StreamTableSource<Row> {

		private final TableSchema schema;
		private final String property;

		public TestTableSource(TableSchema schema, String property) {
			this.schema = schema;
			this.property = property;
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
			return Types.ROW(schema.getColumnNames(), schema.getTypes());
		}

		@Override
		public TableSchema getTableSchema() {
			return schema;
		}

		@Override
		public String explainSource() {
			return "TestTableSource";
		}
	}
}
