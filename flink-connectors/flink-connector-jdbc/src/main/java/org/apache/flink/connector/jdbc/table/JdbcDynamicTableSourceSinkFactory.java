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
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
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
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Factory for creating configured instances of {@link JdbcDynamicTableSource}
 * and {@link JdbcDynamicTableSink}.
 */
@Internal
public class JdbcDynamicTableSourceSinkFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

	public static final String IDENTIFIER = "jdbc";
	public static final ConfigOption<String> URL = ConfigOptions
		.key("url")
		.stringType()
		.noDefaultValue()
		.withDescription("the jdbc database url.");
	public static final ConfigOption<String> TABLE_NAME = ConfigOptions
		.key("table-name")
		.stringType()
		.noDefaultValue()
		.withDescription("the jdbc table name.");
	public static final ConfigOption<String> USERNAME = ConfigOptions
		.key("username")
		.stringType()
		.noDefaultValue()
		.withDescription("the jdbc user name.");
	public static final ConfigOption<String> PASSWORD = ConfigOptions
		.key("password")
		.stringType()
		.noDefaultValue()
		.withDescription("the jdbc password.");
	private static final ConfigOption<String> DRIVER = ConfigOptions
		.key("driver")
		.stringType()
		.noDefaultValue()
		.withDescription("the class name of the JDBC driver to use to connect to this URL. " +
			"If not set, it will automatically be derived from the URL.");

	// read config options
	private static final ConfigOption<String> SCAN_PARTITION_COLUMN = ConfigOptions
		.key("scan.partition.column")
		.stringType()
		.noDefaultValue()
		.withDescription("the column name used for partitioning the input.");
	private static final ConfigOption<Integer> SCAN_PARTITION_NUM = ConfigOptions
		.key("scan.partition.num")
		.intType()
		.noDefaultValue()
		.withDescription("the number of partitions.");
	private static final ConfigOption<Long> SCAN_PARTITION_LOWER_BOUND = ConfigOptions
		.key("scan.partition.lower-bound")
		.longType()
		.noDefaultValue()
		.withDescription("the smallest value of the first partition.");
	private static final ConfigOption<Long> SCAN_PARTITION_UPPER_BOUND = ConfigOptions
		.key("scan.partition.upper-bound")
		.longType()
		.noDefaultValue()
		.withDescription("the largest value of the last partition.");
	private static final ConfigOption<Integer> SCAN_FETCH_SIZE = ConfigOptions
		.key("scan.fetch-size")
		.intType()
		.defaultValue(0)
		.withDescription("gives the reader a hint as to the number of rows that should be fetched, from" +
			" the database when reading per round trip. If the value specified is zero, then the hint is ignored. The" +
			" default value is zero.");

	// look up config options
	private static final ConfigOption<Long> LOOKUP_CACHE_MAX_ROWS = ConfigOptions
		.key("lookup.cache.max-rows")
		.longType()
		.defaultValue(-1L)
		.withDescription("the max number of rows of lookup cache, over this value, the oldest rows will " +
			"be eliminated. \"cache.max-rows\" and \"cache.ttl\" options must all be specified if any of them is " +
			"specified. Cache is not enabled as default.");
	private static final ConfigOption<Duration> LOOKUP_CACHE_TTL = ConfigOptions
		.key("lookup.cache.ttl")
		.durationType()
		.defaultValue(Duration.ofSeconds(10))
		.withDescription("the cache time to live.");
	private static final ConfigOption<Integer> LOOKUP_MAX_RETRIES = ConfigOptions
		.key("lookup.max-retries")
		.intType()
		.defaultValue(3)
		.withDescription("the max retry times if lookup database failed.");

	// write config options
	private static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS = ConfigOptions
		.key("sink.buffer-flush.max-rows")
		.intType()
		.defaultValue(5000)
		.withDescription("the flush max size (includes all append, upsert and delete records), over this number" +
			" of records, will flush data. The default value is 5000.");
	private static final ConfigOption<Long> SINK_BUFFER_FLUSH_INTERVAL = ConfigOptions
		.key("sink.buffer-flush.interval")
		.longType()
		.defaultValue(0L)
		.withDescription("the flush interval mills, over this time, asynchronous threads will flush data. The " +
			"default value is 0, which means no asynchronous flush thread will be scheduled.");
	private static final ConfigOption<Integer> SINK_MAX_RETRIES = ConfigOptions
		.key("sink.max-retries")
		.intType()
		.defaultValue(3)
		.withDescription("the max retry times if writing records to database failed.");

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		final ReadableConfig config = helper.getOptions();

		helper.validate();
		validateConfigOptions(config);
		JdbcOptions jdbcOptions = getJdbcOptions(config);
		TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

		return new JdbcDynamicTableSink(
			jdbcOptions,
			getJdbcExecutionOptions(config),
			getJdbcDmlOptions(jdbcOptions, physicalSchema),
			physicalSchema);
	}

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		final ReadableConfig config = helper.getOptions();

		helper.validate();
		validateConfigOptions(config);
		TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
		return new JdbcDynamicTableSource(
			getJdbcOptions(helper.getOptions()),
			getJdbcReadOptions(helper.getOptions()),
			getJdbcLookupOptions(helper.getOptions()),
			physicalSchema);
	}

	private JdbcOptions getJdbcOptions(ReadableConfig readableConfig) {
		final String url = readableConfig.get(URL);
		final JdbcOptions.Builder builder = JdbcOptions.builder()
			.setDBUrl(url)
			.setTableName(readableConfig.get(TABLE_NAME))
			.setDialect(JdbcDialects.get(url).get());

		readableConfig.getOptional(DRIVER).ifPresent(builder::setDriverName);
		readableConfig.getOptional(USERNAME).ifPresent(builder::setUsername);
		readableConfig.getOptional(PASSWORD).ifPresent(builder::setPassword);
		return builder.build();
	}

	private JdbcReadOptions getJdbcReadOptions(ReadableConfig readableConfig) {
		final Optional<String> partitionColumnName = readableConfig.getOptional(SCAN_PARTITION_COLUMN);
		final JdbcReadOptions.Builder builder = JdbcReadOptions.builder();
		if (partitionColumnName.isPresent()) {
			builder.setPartitionColumnName(partitionColumnName.get());
			builder.setPartitionLowerBound(readableConfig.get(SCAN_PARTITION_LOWER_BOUND));
			builder.setPartitionUpperBound(readableConfig.get(SCAN_PARTITION_UPPER_BOUND));
			builder.setNumPartitions(readableConfig.get(SCAN_PARTITION_NUM));
		}
		readableConfig.getOptional(SCAN_FETCH_SIZE).ifPresent(builder::setFetchSize);
		return builder.build();
	}

	private JdbcLookupOptions getJdbcLookupOptions(ReadableConfig readableConfig) {
		return new JdbcLookupOptions(
			readableConfig.get(LOOKUP_CACHE_MAX_ROWS),
			readableConfig.get(LOOKUP_CACHE_TTL).toMillis(),
			readableConfig.get(LOOKUP_MAX_RETRIES));
	}

	private JdbcExecutionOptions getJdbcExecutionOptions(ReadableConfig config) {
		final JdbcExecutionOptions.Builder builder = new JdbcExecutionOptions.Builder();
		builder.withBatchSize(config.get(SINK_BUFFER_FLUSH_MAX_ROWS));
		builder.withBatchIntervalMs(config.get(SINK_BUFFER_FLUSH_INTERVAL));
		builder.withMaxRetries(config.get(SINK_MAX_RETRIES));
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
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> requiredOptions = new HashSet<>();
		requiredOptions.add(URL);
		requiredOptions.add(TABLE_NAME);
		return requiredOptions;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> optionalOptions = new HashSet<>();
		optionalOptions.add(DRIVER);
		optionalOptions.add(USERNAME);
		optionalOptions.add(PASSWORD);
		optionalOptions.add(SCAN_PARTITION_COLUMN);
		optionalOptions.add(SCAN_PARTITION_LOWER_BOUND);
		optionalOptions.add(SCAN_PARTITION_UPPER_BOUND);
		optionalOptions.add(SCAN_PARTITION_NUM);
		optionalOptions.add(SCAN_FETCH_SIZE);
		optionalOptions.add(LOOKUP_CACHE_MAX_ROWS);
		optionalOptions.add(LOOKUP_CACHE_TTL);
		optionalOptions.add(LOOKUP_MAX_RETRIES);
		optionalOptions.add(SINK_BUFFER_FLUSH_MAX_ROWS);
		optionalOptions.add(SINK_BUFFER_FLUSH_INTERVAL);
		optionalOptions.add(SINK_MAX_RETRIES);
		return optionalOptions;
	}

	private void validateConfigOptions(ReadableConfig config) {
		config.getOptional(URL).orElseThrow(() -> new IllegalArgumentException(
			String.format("Could not find required option: %s", URL.key())));
		config.getOptional(TABLE_NAME).orElseThrow(() -> new IllegalArgumentException(
			String.format("Could not find required option: %s", TABLE_NAME.key())));

		String jdbcUrl = config.get(URL);
		final Optional<JdbcDialect> dialect = JdbcDialects.get(jdbcUrl);
		checkState(dialect.isPresent(), "Cannot handle such jdbc url: " + jdbcUrl);

		if (config.getOptional(USERNAME).isPresent()) {
			checkState(config.getOptional(PASSWORD).isPresent(),
				"Database username must be provided when database password is provided");
		}

		checkAllOrNone(config, new ConfigOption[]{
			SCAN_PARTITION_COLUMN,
			SCAN_PARTITION_NUM,
			SCAN_PARTITION_LOWER_BOUND,
			SCAN_PARTITION_UPPER_BOUND
		});

		if (config.getOptional(SCAN_PARTITION_LOWER_BOUND).isPresent() &&
			config.getOptional(SCAN_PARTITION_UPPER_BOUND).isPresent()) {
			checkState(config.get(SCAN_PARTITION_LOWER_BOUND) <= config.get(SCAN_PARTITION_UPPER_BOUND),
				String.format("%s must not be larger than %s", SCAN_PARTITION_LOWER_BOUND.key(), SCAN_PARTITION_UPPER_BOUND.key()));
		}

		checkAllOrNone(config, new ConfigOption[]{
			LOOKUP_CACHE_MAX_ROWS,
			LOOKUP_CACHE_TTL
		});
	}

	private void checkAllOrNone(ReadableConfig config, ConfigOption<?>[] configOptions) {
		int presentCount = 0;
		for (ConfigOption configOption : configOptions) {
			if (config.getOptional(configOption).isPresent()) {
				presentCount++;
			}
		}
		String[] propertyNames = Arrays.stream(configOptions).map(ConfigOption::key).toArray(String[]::new);
		Preconditions.checkArgument(configOptions.length == presentCount || presentCount == 0,
			"Either all or none of the following options should be provided:\n" + String.join("\n", propertyNames));
	}
}
