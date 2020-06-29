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

package org.apache.flink.streaming.connectors.elasticsearch.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.BULK_FLASH_MAX_SIZE_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.BULK_FLUSH_BACKOFF_DELAY_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.BULK_FLUSH_BACKOFF_TYPE_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.BULK_FLUSH_INTERVAL_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.BULK_FLUSH_MAX_ACTIONS_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.CONNECTION_MAX_RETRY_TIMEOUT_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.CONNECTION_PATH_PREFIX;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.DOCUMENT_TYPE_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.FAILURE_HANDLER_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.FLUSH_ON_CHECKPOINT_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.FORMAT_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.HOSTS_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.INDEX_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.KEY_DELIMITER_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.LOOKUP_CACHE_MAX_ROWS;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.LOOKUP_CACHE_TTL;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.LOOKUP_MAX_RETRIES;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.SCROLL_MAX_SIZE_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.SCROLL_TIMEOUT_OPTION;

/**
 * Factory for creating configured instances of {@link Elasticsearch6DynamicSource}
 * and {@link Elasticsearch6DynamicSink}.
 */
@Internal
public class Elasticsearch6DynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {
	private static final Set<ConfigOption<?>> requiredOptions = Stream.of(
		HOSTS_OPTION,
		INDEX_OPTION,
		DOCUMENT_TYPE_OPTION
	).collect(Collectors.toSet());
	private static final Set<ConfigOption<?>> optionalOptions = Stream.of(
		SCROLL_MAX_SIZE_OPTION,
		SCROLL_TIMEOUT_OPTION,
		KEY_DELIMITER_OPTION,
		FAILURE_HANDLER_OPTION,
		FLUSH_ON_CHECKPOINT_OPTION,
		BULK_FLASH_MAX_SIZE_OPTION,
		BULK_FLUSH_MAX_ACTIONS_OPTION,
		BULK_FLUSH_INTERVAL_OPTION,
		BULK_FLUSH_BACKOFF_TYPE_OPTION,
		BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION,
		BULK_FLUSH_BACKOFF_DELAY_OPTION,
		CONNECTION_MAX_RETRY_TIMEOUT_OPTION,
		CONNECTION_PATH_PREFIX,
		FORMAT_OPTION,
		LOOKUP_CACHE_MAX_ROWS,
		LOOKUP_CACHE_TTL,
		LOOKUP_MAX_RETRIES
	).collect(Collectors.toSet());

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		TableSchema tableSchema = context.getCatalogTable().getSchema();
		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		final DecodingFormat<DeserializationSchema<RowData>> format = helper.discoverDecodingFormat(
			DeserializationFormatFactory.class,
			FORMAT_OPTION);
		helper.validate();
		Configuration configuration = new Configuration();
		context.getCatalogTable()
			.getOptions()
			.forEach(configuration::setString);
		Elasticsearch6Configuration config = new Elasticsearch6Configuration(configuration, context.getClassLoader());

		validateSource(config, configuration);

		return new Elasticsearch6DynamicSource(
			format,
			config,
			TableSchemaUtils.getPhysicalSchema(tableSchema),
			new ElasticsearchLookupOptions.Builder()
				.setCacheExpireMs(config.getCacheExpiredMs().toMillis())
				.setCacheMaxSize(config.getCacheMaxSize())
				.setMaxRetryTimes(config.getMaxRetryTimes())
				.build()
		);
	}

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		TableSchema tableSchema = context.getCatalogTable().getSchema();
		ElasticsearchValidationUtils.validatePrimaryKey(tableSchema);
		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

		final EncodingFormat<SerializationSchema<RowData>> format = helper.discoverEncodingFormat(
			SerializationFormatFactory.class,
			FORMAT_OPTION);

		helper.validate();
		Configuration configuration = new Configuration();
		context.getCatalogTable()
			.getOptions()
			.forEach(configuration::setString);
		Elasticsearch6Configuration config = new Elasticsearch6Configuration(configuration, context.getClassLoader());

		validateSink(config, configuration);

		return new Elasticsearch6DynamicSink(
			format,
			config,
			TableSchemaUtils.getPhysicalSchema(tableSchema));
	}

	private void validateSource(Elasticsearch6Configuration config, Configuration originalConfiguration) {
		config.getHosts(); // validate hosts
		validate(
			config.getIndex().length() >= 1,
			() -> String.format("'%s' must not be empty", INDEX_OPTION.key()));
		validate(
			config.getScrollMaxSize().map(scrollMaxSize -> scrollMaxSize >= 1).orElse(true),
			() -> String.format(
				"'%s' must be at least 1. Got: %s",
				SCROLL_MAX_SIZE_OPTION.key(),
				config.getScrollMaxSize().get())
		);
		validate(config.getScrollTimeout().map(scrollTimeout -> scrollTimeout >= 1).orElse(true),
			() -> String.format(
				"'%s' must be at least 1. Got: %s",
				SCROLL_TIMEOUT_OPTION.key(),
				config.getScrollTimeout().get())
		);
		long cacheMaxSize = config.getCacheMaxSize();
		validate(
			cacheMaxSize == -1 || cacheMaxSize >= 1,
			() -> String.format(
				"'%s' must be at least 1. Got: %s",
				LOOKUP_CACHE_MAX_ROWS.key(),
				cacheMaxSize)
		);
		validate(
			config.getCacheExpiredMs().toSeconds() >= 1,
			() -> String.format(
				"'%s' must be at least 1. Got: %s",
				LOOKUP_CACHE_TTL.key(),
				config.getCacheExpiredMs().toSeconds())
		);
		validate(
			config.getMaxRetryTimes() >= 1,
			() -> String.format(
				"'%s' must be at least 1. Got: %s",
				LOOKUP_MAX_RETRIES.key(),
				config.getMaxRetryTimes())
		);
	}

	private void validateSink(Elasticsearch6Configuration config, Configuration originalConfiguration) {
		config.getFailureHandler(); // checks if we can instantiate the custom failure handler
		config.getHosts(); // validate hosts
		validate(
			config.getIndex().length() >= 1,
			() -> String.format("'%s' must not be empty", INDEX_OPTION.key()));
		int maxActions = config.getBulkFlushMaxActions();
		validate(
			maxActions == -1 || maxActions >= 1,
			() -> String.format(
				"'%s' must be at least 1 character. Got: %s",
				BULK_FLUSH_MAX_ACTIONS_OPTION.key(),
				maxActions)
		);
		long maxSize = config.getBulkFlushMaxByteSize();
		long mb1 = 1024 * 1024;
		validate(
			maxSize == -1 || (maxSize >= mb1 && maxSize % mb1 == 0),
			() -> String.format(
				"'%s' must be in MB granularity. Got: %s",
				BULK_FLASH_MAX_SIZE_OPTION.key(),
				originalConfiguration.get(BULK_FLASH_MAX_SIZE_OPTION).toHumanReadableString())
		);
		validate(
			config.getBulkFlushBackoffRetries().map(retries -> retries >= 1).orElse(true),
			() -> String.format(
				"'%s' must be at least 1. Got: %s",
				BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION.key(),
				config.getBulkFlushBackoffRetries().get())
		);
	}

	private static void validate(boolean condition, Supplier<String> message) {
		if (!condition) {
			throw new ValidationException(message.get());
		}
	}

	@Override
	public String factoryIdentifier() {
		return "elasticsearch-6";
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		return requiredOptions;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		return optionalOptions;
	}

}
