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

import org.apache.flink.addons.hbase.HBaseTableSource;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connectors.hbase.table.HBaseValidator.COLUMNFAMILY_QUALIFIER_DELIMITER_PATTERN;
import static org.apache.flink.connectors.hbase.table.HBaseValidator.CONNECTOR_HBASE_CLIENT_PARAM_PREFIX;
import static org.apache.flink.connectors.hbase.table.HBaseValidator.CONNECTOR_HBASE_TABLE_NAME;
import static org.apache.flink.connectors.hbase.table.HBaseValidator.CONNECTOR_QUALIFIER_DELIMITER;
import static org.apache.flink.connectors.hbase.table.HBaseValidator.CONNECTOR_ROW_KEY;
import static org.apache.flink.connectors.hbase.table.HBaseValidator.CONNECTOR_TYPE_VALUE_HBASE;
import static org.apache.flink.connectors.hbase.table.HBaseValidator.CONNECTOR_VERSION_VALUE_143;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;

/**
 * Factory for creating configured instances of {@link HBaseTableSource} or sink.
 */
public class HBaseTableFactory implements StreamTableSourceFactory<Row>, StreamTableSinkFactory<Row> {
	@Override
	public StreamTableSink<Row> createStreamTableSink(Map<String, String> properties) {
		//TODO NEED to wait FLINK-10206
		throw new UnsupportedOperationException("not support createStreamTableSink now.");
	}

	@Override
	public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		Configuration hbaseClientConf = createClientConfiguration(properties);
		//TODO , now only for the first version, it's to hack to get the rowkey from TableSchema.
		hackRowkey(descriptorProperties);
		HBaseTableContext hBaseTableContext = HBaseTableContext.create(descriptorProperties, hbaseClientConf);
		return new HBaseStreamTableSource(hBaseTableContext);
	}

	/**
	 * now it's a hack. in the future, we will remove it.
	 */
	private void hackRowkey(DescriptorProperties descriptorProperties) {
		TableSchema tableSchema = descriptorProperties.getTableSchema(SCHEMA);
		String[] columnNames = tableSchema.getFieldNames();
		String delimiter = descriptorProperties.getOptionalString(CONNECTOR_QUALIFIER_DELIMITER).orElse(
			COLUMNFAMILY_QUALIFIER_DELIMITER_PATTERN);
		String rowkey = null;
		int rowkeyCount = 0;
		for (String column : columnNames) {
			if (!column.contains(delimiter)) {
				rowkey = column;
				rowkeyCount++;
			}
		}
		Preconditions.checkArgument(rowkeyCount == 1,
			"a column which doesn't contain delimiter(" + delimiter + ") is regarded as rowkey, now only support 1 rowkey, but now has " + rowkeyCount);
		descriptorProperties.putString(CONNECTOR_ROW_KEY, rowkey);
	}

	private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);

		new HBaseValidator().validate(descriptorProperties);

		return descriptorProperties;
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
		properties.add(CONNECTOR_QUALIFIER_DELIMITER);
		properties.add(CONNECTOR_ROW_KEY);

		properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_NAME);

		// HBase's param wildcard
		properties.add(CONNECTOR_HBASE_CLIENT_PARAM_PREFIX);

		// TODO add more support
		return properties;
	}

	private Configuration createClientConfiguration(Map<String, String> userParams) {
		// create default configuration from current runtime env (`hbase-site.xml` in classpath) first,
		Configuration clientConfiguration = HBaseConfiguration.create();

		// and append or overwrite configuration using user params from client-side which has highest priority
		if (null != userParams) {
			for (Map.Entry<String, String> entry : userParams.entrySet()) {
				clientConfiguration.set(entry.getKey(), entry.getValue());
			}
		}
		return clientConfiguration;
	}

	String hbaseVersion() {
		return CONNECTOR_VERSION_VALUE_143;
	}
}
