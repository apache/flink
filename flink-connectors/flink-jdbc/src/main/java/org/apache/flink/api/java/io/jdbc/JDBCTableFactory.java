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

package org.apache.flink.api.java.io.jdbc;

import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.factories.BatchTableSinkFactory;
import org.apache.flink.table.factories.BatchTableSourceFactory;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.util.TableProperties;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;

/**
 * JDBC table factory.
 */
public class JDBCTableFactory implements StreamTableSinkFactory<Object>, BatchTableSinkFactory<Row>,
		StreamTableSourceFactory<Row>, BatchTableSourceFactory<Row> {

	private TableSink createTableSink(Map<String, String> properties) {
		TableProperties prop = new TableProperties();
		prop.putProperties(properties);
		RichTableSchema schema = prop.readSchemaFromProperties(Thread.currentThread().getContextClassLoader());

		String username = prop.getString(JDBCOptions.USER_NAME);
		String tablename = prop.getString(JDBCOptions.TABLE_NAME);
		String password = prop.getString(JDBCOptions.PASSWORD);
		String drivername = prop.getString(JDBCOptions.DRIVER_NAME);
		String dbURL = prop.getString(JDBCOptions.DB_URL);

		List<String> pks = schema.getPrimaryKeys();
		if (pks == null || pks.isEmpty()) {

			StringBuilder fields = new StringBuilder();
			StringBuilder question = new StringBuilder();
			String[] columnNames = schema.getColumnNames();
			for (int i = 0; i < columnNames.length; i++) {
				if (i != 0) {
					fields.append(", ");
					question.append(", ");
				}
				fields.append(columnNames[i]);
				question.append("?");
			}

			return JDBCAppendTableSink.builder()
					.setDrivername(drivername)
					.setUsername(username)
					.setPassword(password)
					.setDBUrl(dbURL)
					.setQuery(String.format("insert into %s (%s) values (%s)",
							tablename, fields, question))
					.setParameterTypes(schema.getColumnTypes())
					.build();
		} else {
			throw new RuntimeException("JDBC sink with primary keys is not support yet!");
		}
	}

	@Override
	public BatchTableSink<Row> createBatchTableSink(Map<String, String> properties) {
		return (BatchTableSink<Row>) createTableSink(properties);
	}

	@Override
	public BatchTableSource<Row> createBatchTableSource(Map<String, String> props) {
		return null;
	}

	@Override
	public StreamTableSink<Object> createStreamTableSink(Map<String, String> properties) {
		return (StreamTableSink) createTableSink(properties);
	}

	@Override
	public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
		return null;
	}

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, "JDBC");
		context.put(CONNECTOR_PROPERTY_VERSION, "1");
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		return JDBCOptions.SUPPORTED_KEYS;
	}
}
