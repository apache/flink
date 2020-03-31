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

import org.apache.flink.api.java.io.jdbc.dialect.JDBCDialects;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.JDBCValidator;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.JDBCValidator.CONNECTOR_DRIVER;
import static org.apache.flink.table.descriptors.JDBCValidator.CONNECTOR_LOOKUP_CACHE_MAX_ROWS;
import static org.apache.flink.table.descriptors.JDBCValidator.CONNECTOR_LOOKUP_CACHE_TTL;
import static org.apache.flink.table.descriptors.JDBCValidator.CONNECTOR_LOOKUP_MAX_RETRIES;
import static org.apache.flink.table.descriptors.JDBCValidator.CONNECTOR_PASSWORD;
import static org.apache.flink.table.descriptors.JDBCValidator.CONNECTOR_READ_FETCH_SIZE;
import static org.apache.flink.table.descriptors.JDBCValidator.CONNECTOR_READ_PARTITION_COLUMN;
import static org.apache.flink.table.descriptors.JDBCValidator.CONNECTOR_READ_PARTITION_LOWER_BOUND;
import static org.apache.flink.table.descriptors.JDBCValidator.CONNECTOR_READ_PARTITION_NUM;
import static org.apache.flink.table.descriptors.JDBCValidator.CONNECTOR_READ_PARTITION_UPPER_BOUND;
import static org.apache.flink.table.descriptors.JDBCValidator.CONNECTOR_TABLE;
import static org.apache.flink.table.descriptors.JDBCValidator.CONNECTOR_TYPE_VALUE_JDBC;
import static org.apache.flink.table.descriptors.JDBCValidator.CONNECTOR_URL;
import static org.apache.flink.table.descriptors.JDBCValidator.CONNECTOR_USERNAME;
import static org.apache.flink.table.descriptors.JDBCValidator.CONNECTOR_WRITE_FLUSH_INTERVAL;
import static org.apache.flink.table.descriptors.JDBCValidator.CONNECTOR_WRITE_FLUSH_MAX_ROWS;
import static org.apache.flink.table.descriptors.JDBCValidator.CONNECTOR_WRITE_MAX_RETRIES;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;

/**
 * Factory for creating configured instances of {@link JDBCTableSource} and {@link JDBCUpsertTableSink}.
 */
public class JDBCTableSourceSinkFactory implements
	StreamTableSourceFactory<Row>,
	StreamTableSinkFactory<Tuple2<Boolean, Row>> {

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_JDBC); // jdbc
		context.put(CONNECTOR_PROPERTY_VERSION, "1"); // backwards compatibility
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();

		// common options
		properties.add(CONNECTOR_DRIVER);
		properties.add(CONNECTOR_URL);
		properties.add(CONNECTOR_TABLE);
		properties.add(CONNECTOR_USERNAME);
		properties.add(CONNECTOR_PASSWORD);

		// scan options
		properties.add(CONNECTOR_READ_PARTITION_COLUMN);
		properties.add(CONNECTOR_READ_PARTITION_NUM);
		properties.add(CONNECTOR_READ_PARTITION_LOWER_BOUND);
		properties.add(CONNECTOR_READ_PARTITION_UPPER_BOUND);
		properties.add(CONNECTOR_READ_FETCH_SIZE);

		// lookup options
		properties.add(CONNECTOR_LOOKUP_CACHE_MAX_ROWS);
		properties.add(CONNECTOR_LOOKUP_CACHE_TTL);
		properties.add(CONNECTOR_LOOKUP_MAX_RETRIES);

		// sink options
		properties.add(CONNECTOR_WRITE_FLUSH_MAX_ROWS);
		properties.add(CONNECTOR_WRITE_FLUSH_INTERVAL);
		properties.add(CONNECTOR_WRITE_MAX_RETRIES);

		// schema
		properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_NAME);

		return properties;
	}

	@Override
	public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

		return JDBCTableSource.builder()
			.setOptions(getJDBCOptions(descriptorProperties))
			.setReadOptions(getJDBCReadOptions(descriptorProperties))
			.setLookupOptions(getJDBCLookupOptions(descriptorProperties))
			.setSchema(descriptorProperties.getTableSchema(SCHEMA))
			.build();
	}

	@Override
	public StreamTableSink<Tuple2<Boolean, Row>> createStreamTableSink(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

		final JDBCUpsertTableSink.Builder builder = JDBCUpsertTableSink.builder()
			.setOptions(getJDBCOptions(descriptorProperties))
			.setTableSchema(descriptorProperties.getTableSchema(SCHEMA));

		descriptorProperties.getOptionalInt(CONNECTOR_WRITE_FLUSH_MAX_ROWS).ifPresent(builder::setFlushMaxSize);
		descriptorProperties.getOptionalDuration(CONNECTOR_WRITE_FLUSH_INTERVAL).ifPresent(
			s -> builder.setFlushIntervalMills(s.toMillis()));
		descriptorProperties.getOptionalInt(CONNECTOR_WRITE_MAX_RETRIES).ifPresent(builder::setMaxRetryTimes);

		return builder.build();
	}

	private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);

		new SchemaValidator(true, false, false).validate(descriptorProperties);
		new JDBCValidator().validate(descriptorProperties);

		return descriptorProperties;
	}

	private JDBCOptions getJDBCOptions(DescriptorProperties descriptorProperties) {
		final String url = descriptorProperties.getString(CONNECTOR_URL);
		final JDBCOptions.Builder builder = JDBCOptions.builder()
			.setDBUrl(url)
			.setTableName(descriptorProperties.getString(CONNECTOR_TABLE))
			.setDialect(JDBCDialects.get(url).get());

		descriptorProperties.getOptionalString(CONNECTOR_DRIVER).ifPresent(builder::setDriverName);
		descriptorProperties.getOptionalString(CONNECTOR_USERNAME).ifPresent(builder::setUsername);
		descriptorProperties.getOptionalString(CONNECTOR_PASSWORD).ifPresent(builder::setPassword);

		return builder.build();
	}

	private JDBCReadOptions getJDBCReadOptions(DescriptorProperties descriptorProperties) {
		final Optional<String> partitionColumnName =
			descriptorProperties.getOptionalString(CONNECTOR_READ_PARTITION_COLUMN);
		final Optional<Long> partitionLower = descriptorProperties.getOptionalLong(CONNECTOR_READ_PARTITION_LOWER_BOUND);
		final Optional<Long> partitionUpper = descriptorProperties.getOptionalLong(CONNECTOR_READ_PARTITION_UPPER_BOUND);
		final Optional<Integer> numPartitions = descriptorProperties.getOptionalInt(CONNECTOR_READ_PARTITION_NUM);

		final JDBCReadOptions.Builder builder = JDBCReadOptions.builder();
		if (partitionColumnName.isPresent()) {
			builder.setPartitionColumnName(partitionColumnName.get());
			builder.setPartitionLowerBound(partitionLower.get());
			builder.setPartitionUpperBound(partitionUpper.get());
			builder.setNumPartitions(numPartitions.get());
		}
		descriptorProperties.getOptionalInt(CONNECTOR_READ_FETCH_SIZE).ifPresent(builder::setFetchSize);

		return builder.build();
	}

	private JDBCLookupOptions getJDBCLookupOptions(DescriptorProperties descriptorProperties) {
		final JDBCLookupOptions.Builder builder = JDBCLookupOptions.builder();

		descriptorProperties.getOptionalLong(CONNECTOR_LOOKUP_CACHE_MAX_ROWS).ifPresent(builder::setCacheMaxSize);
		descriptorProperties.getOptionalDuration(CONNECTOR_LOOKUP_CACHE_TTL).ifPresent(
			s -> builder.setCacheExpireMs(s.toMillis()));
		descriptorProperties.getOptionalInt(CONNECTOR_LOOKUP_MAX_RETRIES).ifPresent(builder::setMaxRetryTimes);

		return builder.build();
	}
}
