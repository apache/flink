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

package org.apache.flink.connector.hbase;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.hbase.options.HBaseOptions;
import org.apache.flink.connector.hbase.options.HBaseWriteOptions;
import org.apache.flink.connector.hbase.sink.HBaseDynamicTableSink;
import org.apache.flink.connector.hbase.source.HBaseDynamicTableSource;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

/**
 * HBase connector factory.
 */
public class HBaseDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

	private static final String IDENTIFIER = "hbase-1.4";

	private static final ConfigOption<String> TABLE_NAME = ConfigOptions
		.key("table-name")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. It defines the HBase table name.");

	private static final ConfigOption<String> ZOOKEEPER_QUORUM = ConfigOptions
		.key("zookeeper.quorum")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. It defines HBase Zookeeper quorum.");

	private static final ConfigOption<String> ZOOKEEPER_ZNODE_PARENT = ConfigOptions
		.key("zookeeper.znode-parent")
		.stringType()
		.defaultValue("/hbase")
		.withDescription("Optional. The root dir in Zookeeper for HBase cluster, default value is '/hbase'");

	private static final ConfigOption<String> NULL_STRING_LITERAL = ConfigOptions
		.key("null-string-literal")
		.stringType()
		.defaultValue("null")
		.withDescription("Optional. Representation for null values for string fields. (\"null\" by default). " +
			"HBase connector encode/decode empty bytes as null values except string types.");

	private static final ConfigOption<MemorySize> SINK_BUFFER_FLUSH_MAX_SIZE = ConfigOptions
		.key("sink.buffer-flush.max-size")
		.memoryType()
		.defaultValue(MemorySize.parse("2mb"))
		.withDescription("Optional. Writing option, determines how many size in memory of " +
			"buffered rows to insert per round trip. This can help performance on writing " +
			"to JDBC database. The default value is '2mb'.");

	private static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS = ConfigOptions
		.key("sink.buffer-flush.max-rows")
		.intType()
		.noDefaultValue()
		.withDescription("Optional. Writing option, determines how many rows to insert " +
			"per round trip. This can help performance on writing to JDBC database. " +
			"No default value, i.e. the default flushing is not depends on the number of buffered rows.");

	private static final ConfigOption<Duration> SINK_BUFFER_FLUSH_INTERVAL = ConfigOptions
		.key("sink.buffer-flush.interval")
		.durationType()
		.noDefaultValue()
		.withDescription("Optional. Writing option, sets a flush interval flushing " +
			"buffered requesting if the interval passes, in milliseconds. Default value is '0s', " +
			"which means no asynchronous flush thread will be scheduled.");

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		TableFactoryHelper helper = createTableFactoryHelper(this, context);
		helper.validate();
		String hTableName = helper.getOptions().get(TABLE_NAME);
		// create default configuration from current runtime env (`hbase-site.xml` in classpath) first,
		Configuration hbaseClientConf = HBaseConfiguration.create();
		hbaseClientConf.set(HConstants.ZOOKEEPER_QUORUM, helper.getOptions().get(ZOOKEEPER_QUORUM));
		hbaseClientConf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, helper.getOptions().get(ZOOKEEPER_ZNODE_PARENT));

		String nullStringLiteral = helper.getOptions().get(NULL_STRING_LITERAL);

		TableSchema tableSchema = context.getCatalogTable().getSchema();
		HBaseTableSchema hbaseSchema = HBaseTableSchema.fromTableSchema(tableSchema);

		return new HBaseDynamicTableSource(
			hbaseClientConf,
			hTableName,
			hbaseSchema,
			nullStringLiteral);
	}

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		TableFactoryHelper helper = createTableFactoryHelper(this, context);
		helper.validate();
		HBaseOptions.Builder hbaseOptionsBuilder = HBaseOptions.builder();
		hbaseOptionsBuilder.setTableName(helper.getOptions().get(TABLE_NAME));
		hbaseOptionsBuilder.setZkQuorum(helper.getOptions().get(ZOOKEEPER_QUORUM));
		hbaseOptionsBuilder.setZkNodeParent(helper.getOptions().get(ZOOKEEPER_ZNODE_PARENT));

		HBaseWriteOptions.Builder writeBuilder = HBaseWriteOptions.builder();
		writeBuilder.setBufferFlushMaxSizeInBytes(helper.getOptions().get(SINK_BUFFER_FLUSH_MAX_SIZE).getBytes());
		helper.getOptions().getOptional(SINK_BUFFER_FLUSH_INTERVAL)
			.ifPresent(v -> writeBuilder.setBufferFlushIntervalMillis(v.toMillis()));
		helper.getOptions().getOptional(SINK_BUFFER_FLUSH_MAX_ROWS)
			.ifPresent(writeBuilder::setBufferFlushMaxRows);

		String nullStringLiteral = helper.getOptions().get(NULL_STRING_LITERAL);

		TableSchema tableSchema = context.getCatalogTable().getSchema();
		HBaseTableSchema hbaseSchema = HBaseTableSchema.fromTableSchema(tableSchema);

		return new HBaseDynamicTableSink(
			hbaseSchema,
			hbaseOptionsBuilder.build(),
			writeBuilder.build(),
			nullStringLiteral);
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> set = new HashSet<>();
		set.add(TABLE_NAME);
		set.add(ZOOKEEPER_QUORUM);
		return set;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> set = new HashSet<>();
		set.add(ZOOKEEPER_ZNODE_PARENT);
		set.add(NULL_STRING_LITERAL);
		set.add(SINK_BUFFER_FLUSH_MAX_SIZE);
		set.add(SINK_BUFFER_FLUSH_MAX_ROWS);
		set.add(SINK_BUFFER_FLUSH_INTERVAL);
		return set;
	}
}
