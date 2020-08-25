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

package org.apache.flink.connector.hbase1;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.hbase.options.HBaseWriteOptions;
import org.apache.flink.connector.hbase.util.HBaseConfigurationUtil;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.connector.hbase1.options.HBaseOptions;
import org.apache.flink.connector.hbase1.sink.HBaseUpsertTableSink;
import org.apache.flink.connector.hbase1.source.HBaseTableSource;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connector.hbase1.HBaseValidator.CONNECTOR_VERSION_VALUE_143;
import static org.apache.flink.table.descriptors.AbstractHBaseValidator.CONNECTOR_TABLE_NAME;
import static org.apache.flink.table.descriptors.AbstractHBaseValidator.CONNECTOR_TYPE_VALUE_HBASE;
import static org.apache.flink.table.descriptors.AbstractHBaseValidator.CONNECTOR_WRITE_BUFFER_FLUSH_INTERVAL;
import static org.apache.flink.table.descriptors.AbstractHBaseValidator.CONNECTOR_WRITE_BUFFER_FLUSH_MAX_ROWS;
import static org.apache.flink.table.descriptors.AbstractHBaseValidator.CONNECTOR_WRITE_BUFFER_FLUSH_MAX_SIZE;
import static org.apache.flink.table.descriptors.AbstractHBaseValidator.CONNECTOR_ZK_NODE_PARENT;
import static org.apache.flink.table.descriptors.AbstractHBaseValidator.CONNECTOR_ZK_QUORUM;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;
import static org.apache.flink.table.descriptors.DescriptorProperties.EXPR;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_ROWTIME;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_DATA_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_EXPR;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_DATA_TYPE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;

/**
 * Factory for creating configured instances of {@link HBaseTableSource} or sink.
 */
@Internal
public class HBase1TableFactory implements StreamTableSourceFactory<Row>, StreamTableSinkFactory<Tuple2<Boolean, Row>> {

	@Override
	public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		// create default configuration from current runtime env (`hbase-site.xml` in classpath) first,
		Configuration hbaseClientConf = HBaseConfigurationUtil.getHBaseConfiguration();
		String hbaseZk = descriptorProperties.getString(CONNECTOR_ZK_QUORUM);
		hbaseClientConf.set(HConstants.ZOOKEEPER_QUORUM, hbaseZk);
		descriptorProperties
			.getOptionalString(CONNECTOR_ZK_NODE_PARENT)
			.ifPresent(v -> hbaseClientConf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, v));

		String hTableName = descriptorProperties.getString(CONNECTOR_TABLE_NAME);
		TableSchema tableSchema = TableSchemaUtils.getPhysicalSchema(
			descriptorProperties.getTableSchema(SCHEMA));
		HBaseTableSchema hbaseSchema = validateTableSchema(tableSchema);
		return new HBaseTableSource(hbaseClientConf, hTableName, hbaseSchema, null);
	}

	@Override
	public StreamTableSink<Tuple2<Boolean, Row>> createStreamTableSink(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		HBaseOptions.Builder hbaseOptionsBuilder = HBaseOptions.builder();
		hbaseOptionsBuilder.setZkQuorum(descriptorProperties.getString(CONNECTOR_ZK_QUORUM));
		hbaseOptionsBuilder.setTableName(descriptorProperties.getString(CONNECTOR_TABLE_NAME));
		descriptorProperties
			.getOptionalString(CONNECTOR_ZK_NODE_PARENT)
			.ifPresent(hbaseOptionsBuilder::setZkNodeParent);

		TableSchema tableSchema = TableSchemaUtils.getPhysicalSchema(
			descriptorProperties.getTableSchema(SCHEMA));
		HBaseTableSchema hbaseSchema = validateTableSchema(tableSchema);

		HBaseWriteOptions.Builder writeBuilder = HBaseWriteOptions.builder();
		descriptorProperties
			.getOptionalInt(CONNECTOR_WRITE_BUFFER_FLUSH_MAX_ROWS)
			.ifPresent(writeBuilder::setBufferFlushMaxRows);
		descriptorProperties
			.getOptionalMemorySize(CONNECTOR_WRITE_BUFFER_FLUSH_MAX_SIZE)
			.ifPresent(v -> writeBuilder.setBufferFlushMaxSizeInBytes(v.getBytes()));
		descriptorProperties
			.getOptionalDuration(CONNECTOR_WRITE_BUFFER_FLUSH_INTERVAL)
			.ifPresent(v -> writeBuilder.setBufferFlushIntervalMillis(v.toMillis()));

		return new HBaseUpsertTableSink(
			hbaseSchema,
			hbaseOptionsBuilder.build(),
			writeBuilder.build()
		);
	}

	private HBaseTableSchema validateTableSchema(TableSchema schema) {
		HBaseTableSchema hbaseSchema = new HBaseTableSchema();
		String[] fieldNames = schema.getFieldNames();
		TypeInformation[] fieldTypes = schema.getFieldTypes();
		for (int i = 0; i < fieldNames.length; i++) {
			String name = fieldNames[i];
			TypeInformation<?> type = fieldTypes[i];
			if (type instanceof RowTypeInfo) {
				RowTypeInfo familyType = (RowTypeInfo) type;
				String[] qualifierNames = familyType.getFieldNames();
				TypeInformation[] qualifierTypes = familyType.getFieldTypes();
				for (int j = 0; j < familyType.getArity(); j++) {
					// HBase connector doesn't support LocalDateTime
					// use Timestamp as conversion class for now.
					Class clazz = qualifierTypes[j].getTypeClass();
					if (LocalDateTime.class.equals(clazz)) {
						clazz = Timestamp.class;
					} else if (LocalDate.class.equals(clazz)) {
						clazz = Date.class;
					} else if (LocalTime.class.equals(clazz)) {
						clazz = Time.class;
					}
					hbaseSchema.addColumn(name, qualifierNames[j], clazz);
				}
			} else {
				hbaseSchema.setRowKey(name, type.getTypeClass());
			}
		}
		return hbaseSchema;
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

		properties.add(CONNECTOR_TABLE_NAME);
		properties.add(CONNECTOR_ZK_QUORUM);
		properties.add(CONNECTOR_ZK_NODE_PARENT);
		properties.add(CONNECTOR_WRITE_BUFFER_FLUSH_MAX_SIZE);
		properties.add(CONNECTOR_WRITE_BUFFER_FLUSH_MAX_ROWS);
		properties.add(CONNECTOR_WRITE_BUFFER_FLUSH_INTERVAL);

		// schema
		properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_NAME);
		// computed column
		properties.add(SCHEMA + ".#." + EXPR);

		// watermark
		properties.add(SCHEMA + "." + WATERMARK + ".#."  + WATERMARK_ROWTIME);
		properties.add(SCHEMA + "." + WATERMARK + ".#."  + WATERMARK_STRATEGY_EXPR);
		properties.add(SCHEMA + "." + WATERMARK + ".#."  + WATERMARK_STRATEGY_DATA_TYPE);

		// table constraint
		properties.add(SCHEMA + "." + DescriptorProperties.PRIMARY_KEY_NAME);
		properties.add(SCHEMA + "." + DescriptorProperties.PRIMARY_KEY_COLUMNS);

		return properties;
	}

	private String hbaseVersion() {
		return CONNECTOR_VERSION_VALUE_143;
	}
}
