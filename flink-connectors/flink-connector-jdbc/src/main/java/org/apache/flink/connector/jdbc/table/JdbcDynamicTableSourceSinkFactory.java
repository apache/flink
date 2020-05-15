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

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.dialect.JdbcDialects;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcReadOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.time.Duration;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Factory for creating configured instances of {@link JdbcDynamicTableSource}
 * and {@link JdbcDynamicTableSink}.
 */
@Internal
public class JdbcDynamicTableSourceSinkFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

	public static final ConfigOption<String> IDENTIFIER = ConfigOptions
		.key("connector")
		.stringType()
		.defaultValue("jdbc")
		.withDescription("-- required: specify this table type is jdbc.");
	public static final ConfigOption<String> URL = ConfigOptions
		.key("url")
		.stringType()
		.noDefaultValue()
		.withDescription("-- required: the jdbc database url.");
	public static final ConfigOption<String> TABLE = ConfigOptions
		.key("table")
		.stringType()
		.noDefaultValue()
		.withDescription("-- required: the jdbc table name.");
	public static final ConfigOption<String> USERNAME = ConfigOptions
		.key("username")
		.stringType()
		.noDefaultValue()
		.withDescription("-- optional: the jdbc user name.");
	public static final ConfigOption<String> PASSWORD = ConfigOptions
		.key("password")
		.stringType()
		.noDefaultValue()
		.withDescription("-- optional: the jdbc password.");
	private static final ConfigOption<String> DRIVER = ConfigOptions
		.key("driver")
		.stringType()
		.noDefaultValue()
		.withDescription("-- optional: the class name of the JDBC driver to use to connect to this URL. " +
			"If not set, it will automatically be derived from the URL.");

	// read config options
	private static final ConfigOption<String> READ_PARTITION_COLUMN = ConfigOptions
		.key("read.partition.column")
		.stringType()
		.noDefaultValue()
		.withDescription("-- optional: the column name used for partitioning the input.");
	private static final ConfigOption<Integer> READ_PARTITION_NUM = ConfigOptions
		.key("read.partition.num")
		.intType()
		.noDefaultValue()
		.withDescription("-- optional: the number of partitions.");
	private static final ConfigOption<Long> READ_PARTITION_LOWER_BOUND = ConfigOptions
		.key("read.partition.lower-bound")
		.longType()
		.noDefaultValue()
		.withDescription("-- optional: the smallest value of the first partition.");
	private static final ConfigOption<Long> READ_PARTITION_UPPER_BOUND = ConfigOptions
		.key("read.partition.upper-bound")
		.longType()
		.noDefaultValue()
		.withDescription("-- optional: the largest value of the last partition.");
	private static final ConfigOption<Integer> READ_FETCH_SIZE = ConfigOptions
		.key("read.fetch-size")
		.intType()
		.defaultValue(0)
		.withDescription("-- optional, Gives the reader a hint as to the number of rows that should be fetched, from" +
			" the database when reading per round trip. If the value specified is zero, then the hint is ignored. The" +
			" default value is zero.");

	// look up config options
	private static final ConfigOption<Long> LOOKUP_CACHE_MAX_ROWS = ConfigOptions
		.key("lookup.cache.max-rows")
		.longType()
		.defaultValue(-1L)
		.withDescription("-- optional, max number of rows of lookup cache, over this value, the oldest rows will " +
			"be eliminated. \"cache.max-rows\" and \"cache.ttl\" options must all be specified if any of them is " +
			"specified. Cache is not enabled as default.");
	private static final ConfigOption<Duration> LOOKUP_CACHE_TTL = ConfigOptions
		.key("lookup.cache.ttl")
		.durationType()
		.defaultValue(Duration.ofSeconds(10))
		.withDescription("-- optional, the cache time to live.");
	private static final ConfigOption<Integer> LOOKUP_MAX_RETRIES = ConfigOptions
		.key("lookup.max-retries")
		.intType()
		.defaultValue(3)
		.withDescription("-- optional, max retry times if lookup database failed.");

	// write config options
	private static final ConfigOption<Integer> WRITE_FLUSH_MAX_ROWS = ConfigOptions
		.key("write.flush.max-rows")
		.intType()
		.defaultValue(5000)
		.withDescription("-- optional, flush max size (includes all append, upsert and delete records), over this number" +
			" of records, will flush data. The default value is 5000.");
	private static final ConfigOption<Long> WRITE_FLUSH_INTERVAL = ConfigOptions
		.key("write.flush.interval")
		.longType()
		.defaultValue(0L)
		.withDescription("-- optional, flush interval mills, over this time, asynchronous threads will flush data. The " +
			"default value is 0, which means no asynchronous flush thread will be scheduled.");
	private static final ConfigOption<Integer> WRITE_MAX_RETRIES = ConfigOptions
		.key("write.max-retries")
		.intType()
		.defaultValue(3)
		.withDescription("-- optional, max retry times if writing records to database failed.");

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		helper.validate();
		final JdbcOptions jdbcOptions = getJdbcOptions(helper.getOptions());
		final DataType rowDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();
		final TableSchema formatSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
		final DataType[] fieldDataTypes = formatSchema.getFieldDataTypes();

		return new JdbcDynamicTableSink(
			jdbcOptions,
			getJdbcExecutionOptions(helper.getOptions()),
			getJdbcDmlOptions(jdbcOptions, context.getCatalogTable().getSchema()),
			rowDataType,
			fieldDataTypes);
	}

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		helper.validate();
		TableSchema formatSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

		int[] selectFields = new int[formatSchema.getFieldNames().length];
		for (int i = 0; i < selectFields.length; i++) {
			selectFields[i] = i;
		}
		return new JdbcDynamicTableSource(
			getJdbcOptions(helper.getOptions()),
			getJdbcReadOptions(helper.getOptions()),
			getJdbcLookupOptions(helper.getOptions()),
			formatSchema,
			selectFields);
	}

	private JdbcOptions getJdbcOptions(ReadableConfig readableConfig) {
		final String url = readableConfig.get(URL);
		final JdbcOptions.Builder builder = JdbcOptions.builder()
			.setDBUrl(url)
			.setTableName(readableConfig.get(TABLE))
			.setDialect(JdbcDialects.get(url).get());

		readableConfig.getOptional(DRIVER).ifPresent(builder::setDriverName);
		readableConfig.getOptional(USERNAME).ifPresent(builder::setUsername);
		readableConfig.getOptional(PASSWORD).ifPresent(builder::setPassword);
		return builder.build();
	}

	private JdbcReadOptions getJdbcReadOptions(ReadableConfig readableConfig) {
		final Optional<String> partitionColumnName = readableConfig.getOptional(READ_PARTITION_COLUMN);
		final Optional<Long> partitionLower = readableConfig.getOptional(READ_PARTITION_LOWER_BOUND);
		final Optional<Long> partitionUpper = readableConfig.getOptional(READ_PARTITION_UPPER_BOUND);
		final Optional<Integer> numPartitions = readableConfig.getOptional(READ_PARTITION_NUM);

		final JdbcReadOptions.Builder builder = JdbcReadOptions.builder();
		if (partitionColumnName.isPresent()) {
			builder.setPartitionColumnName(partitionColumnName.get());
			builder.setPartitionLowerBound(partitionLower.get());
			builder.setPartitionUpperBound(partitionUpper.get());
			builder.setNumPartitions(numPartitions.get());
		}
		readableConfig.getOptional(READ_FETCH_SIZE).ifPresent(builder::setFetchSize);
		return builder.build();
	}

	private JdbcLookupOptions getJdbcLookupOptions(ReadableConfig readableConfig) {
		final JdbcLookupOptions.Builder builder = JdbcLookupOptions.builder();

		readableConfig.getOptional(LOOKUP_CACHE_MAX_ROWS).ifPresent(builder::setCacheMaxSize);
		readableConfig.getOptional(LOOKUP_CACHE_TTL).ifPresent(
			s -> builder.setCacheExpireMs(s.toMillis()));
		readableConfig.getOptional(LOOKUP_MAX_RETRIES).ifPresent(builder::setMaxRetryTimes);

		return builder.build();
	}

	private JdbcExecutionOptions getJdbcExecutionOptions(ReadableConfig readableConfig) {
		final JdbcExecutionOptions.Builder builder = new JdbcExecutionOptions.Builder();
		readableConfig.getOptional(WRITE_FLUSH_MAX_ROWS)
			.ifPresent(builder::withBatchSize);
		readableConfig.getOptional(WRITE_FLUSH_INTERVAL)
			.ifPresent(builder::withBatchIntervalMs);
		readableConfig.getOptional(WRITE_MAX_RETRIES)
			.ifPresent(builder::withMaxRetries);
		return builder.build();
	}

	private JdbcDmlOptions getJdbcDmlOptions(JdbcOptions jdbcOptions, TableSchema schema) {
		String[] keyFields = schema.getPrimaryKey()
			.map(pk -> pk.getColumns().toArray(new String[0]))
			.orElse(null);

		return JdbcDmlOptions.builder()
			.withTableName(jdbcOptions.getTableName())
			.withDialect(jdbcOptions.getDialect())
			.withFieldNames(schema.getFieldNames())
			.withKeyFields(keyFields)
			.build();
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER.defaultValue();
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> requiredOptions = new HashSet<>();
		requiredOptions.add(URL);
		requiredOptions.add(TABLE);
		return requiredOptions;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> optionalOptions = new HashSet<>();
		optionalOptions.add(DRIVER);
		optionalOptions.add(USERNAME);
		optionalOptions.add(PASSWORD);
		optionalOptions.add(READ_PARTITION_COLUMN);
		optionalOptions.add(READ_PARTITION_LOWER_BOUND);
		optionalOptions.add(READ_PARTITION_UPPER_BOUND);
		optionalOptions.add(READ_PARTITION_NUM);
		optionalOptions.add(READ_FETCH_SIZE);
		optionalOptions.add(LOOKUP_CACHE_MAX_ROWS);
		optionalOptions.add(LOOKUP_CACHE_TTL);
		optionalOptions.add(LOOKUP_MAX_RETRIES);
		optionalOptions.add(WRITE_FLUSH_MAX_ROWS);
		optionalOptions.add(WRITE_FLUSH_INTERVAL);
		optionalOptions.add(WRITE_MAX_RETRIES);
		return optionalOptions;
	}
}
