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

package org.apache.flink.connectors.hbase.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.TypeConverters;
import org.apache.flink.table.factories.BatchTableSinkFactory;
import org.apache.flink.table.factories.BatchTableSourceFactory;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.util.TableProperties;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connectors.hbase.table.HBaseValidator.COLUMNFAMILY_QUALIFIER_DELIMITER_PATTERN;
import static org.apache.flink.connectors.hbase.table.HBaseValidator.CONNECTOR_HBASE_CLIENT_PARAM_PREFIX;
import static org.apache.flink.connectors.hbase.table.HBaseValidator.CONNECTOR_HBASE_TABLE_NAME;
import static org.apache.flink.connectors.hbase.table.HBaseValidator.CONNECTOR_TYPE_VALUE_HBASE;
import static org.apache.flink.connectors.hbase.table.HBaseValidator.DEFAULT_ROW_KEY;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;

/**
 * Base class for HBaseTableFactory, the subclass will implement concrete table source/sink creation.
 */
public abstract class HBaseTableFactoryBase
		implements StreamTableSinkFactory<Object>, BatchTableSinkFactory<Row>, StreamTableSourceFactory<Row>, BatchTableSourceFactory<Row> {

	abstract String hbaseVersion();

	abstract TableSink createTableSink(Map<String, String> properties);

	abstract TableSource createTableSource(Map<String, String> properties, boolean isBounded);

	@Override
	public StreamTableSink<Object> createStreamTableSink(Map<String, String> properties) {
		return (StreamTableSink<Object>) createTableSink(properties);
	}

	@Override
	public BatchTableSink<Row> createBatchTableSink(Map<String, String> properties) {
		return (BatchTableSink<Row>) createTableSink(properties);
	}

	@Override
	public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
		return (StreamTableSource<Row>) createTableSource(properties, false);
	}

	@Override
	public BatchTableSource<Row> createBatchTableSource(Map<String, String> properties) {
		return (BatchTableSource<Row>) createTableSource(properties, true);
	}

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_HBASE); // hbase
		context.put(CONNECTOR_VERSION, hbaseVersion()); // version
		context.put(CONNECTOR_PROPERTY_VERSION, "1"); // backwards compatibility
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();

		properties.add(CONNECTOR_HBASE_TABLE_NAME);

		// HBase's param wildcard
		properties.add(CONNECTOR_HBASE_CLIENT_PARAM_PREFIX);

		// TODO add more support, e.g., async, batch, ...
		return properties;
	}

	protected void preCheck(Map<String, String> properties) {
		String tableName = properties.get(CONNECTOR_HBASE_TABLE_NAME);
		if (StringUtils.isNullOrWhitespaceOnly(tableName)) {
			throw new RuntimeException(CONNECTOR_HBASE_TABLE_NAME + " should not be empty!");
		}
		String hbaseZk = properties.get(HConstants.ZOOKEEPER_QUORUM);
		if (StringUtils.isNullOrWhitespaceOnly(hbaseZk)) {
			Configuration defaultConf = HBaseConfiguration.create();
			String zkQuorum = defaultConf.get(HConstants.ZOOKEEPER_QUORUM);
			if (StringUtils.isNullOrWhitespaceOnly(zkQuorum)) {
				throw new RuntimeException(HConstants.ZOOKEEPER_QUORUM + " should not be empty! " +
					"Pls specify it or ensure a default hbase-site.xml is valid in current class path.");
			}
		}
	}

	protected RichTableSchema getTableSchemaFromProperties(Map<String, String> properties) {
		TableProperties tableProperties = new TableProperties();
		tableProperties.putProperties(properties);
		return tableProperties.readSchemaFromProperties(null);
	}

	protected Configuration createClientConfiguration(Map<String, String> userParams) {
		// create default configuration from current runtime env (`hbase-site.xml` in classpath) first,
		Configuration clientConfiguration = HBaseConfiguration.create();

		// and append or overwrite configuration using user params from client-side which has highest priority
		if (null != userParams) {
			for (Map.Entry<String, String> entry : userParams.entrySet()) {
				clientConfiguration.set(entry.getKey(), entry.getValue());
			}
		}
		return  clientConfiguration;
	}

	/**
	 * Return a Tuple3 structure: (hTableSchema, rowKeySourceIndex, qualifierSourceIndexes).
	 */
	protected Tuple3<HBaseTableSchemaV2, Integer, List<Integer>> extractHBaseSchemaAndIndexMapping(RichTableSchema schema) {
		String[] columnNames = schema.getColumnNames();
		List<String> pkColmns = schema.getPrimaryKeys();
		// case sensitive
		Preconditions.checkArgument(
			null != pkColmns && pkColmns.size() == 1,
			"The HBase table schema must have a single column primary key, for example:'" + DEFAULT_ROW_KEY + "'");

		String rowKey = pkColmns.get(0);
		int rowKeySourceIndex = -1;
		for (int idx = 0; idx < columnNames.length; idx++) {
			if (rowKey.equals(columnNames[idx])) {
				rowKeySourceIndex = idx;
				break;
			}
		}
		// this should never happen!
		Preconditions.checkArgument(-1 != rowKeySourceIndex , "Primary key column'" + rowKey + "' invalid!");

		InternalType[] columnTypes = schema.getColumnTypes();
		TypeInformation rowKeyType = TypeConverters.createExternalTypeInfoFromDataType(columnTypes[rowKeySourceIndex]);

		// convert richTableSchema to HBaseTableSchemaV2
		HBaseTableSchemaV2.Builder hTableSchemaBuilder = new HBaseTableSchemaV2.Builder(rowKey, rowKeyType);
		// HBase qualifier index (based on list element index) -> source column index (in RichTableSchema)
		List<Integer> qualifierSourceIndexes = new ArrayList<>();
		for (int idx = 0; idx < columnNames.length; idx++) {
			if (idx != rowKeySourceIndex) {
				String[] cfQ = columnNames[idx].split(COLUMNFAMILY_QUALIFIER_DELIMITER_PATTERN);
				Preconditions.checkArgument(
						2 == cfQ.length,
						"invalid column name'" + columnNames[idx] + "' for HBase qualifier name pattern: `columnFamily.qualifier`!");
				TypeInformation columnType = TypeConverters.createExternalTypeInfoFromDataType(columnTypes[idx]);
				hTableSchemaBuilder.addColumn(cfQ[0], cfQ[1], columnType);
				qualifierSourceIndexes.add(idx);
			}
		}
		HBaseTableSchemaV2 hTableSchema = hTableSchemaBuilder.build();
		return new Tuple3<>(hTableSchema, rowKeySourceIndex, qualifierSourceIndexes);
	}
}
