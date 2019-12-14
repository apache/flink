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

package org.apache.flink.table.catalog;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;

/**
 * Table source factory for testing. It creates a dummy {@link TableSource}
 * that returns an empty {@link TableSchema}.
 */
public class TestExternalTableSourceFactory implements TableSourceFactory<Row> {

	static final String TEST_EXTERNAL_CONNECTOR_TYPE = "test-external-connector";

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> properties = new HashMap<>();
		properties.put(CONNECTOR_TYPE, TEST_EXTERNAL_CONNECTOR_TYPE);
		return properties;
	}

	@Override
	public List<String> supportedProperties() {
		return Collections.emptyList();
	}

	@Override
	public TableSource<Row> createTableSource(Map<String, String> properties) {
		return new TestExternalTableSource();
	}

	/**
	 * Dummy table source for tests.
	 */
	public static class TestExternalTableSource implements StreamTableSource<Row>, BatchTableSource<Row> {
		private final TableSchema tableSchema = new TableSchema(new String[0], new TypeInformation[0]);

		@Override
		public DataSet<Row> getDataSet(ExecutionEnvironment execEnv) {
			return null;
		}

		@Override
		public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
			return null;
		}

		@Override
		public TypeInformation<Row> getReturnType() {
			return tableSchema.toRowType();
		}

		@Override
		public TableSchema getTableSchema() {
			return tableSchema;
		}

		@Override
		public String explainSource() {
			return "()";
		}
	}
}
