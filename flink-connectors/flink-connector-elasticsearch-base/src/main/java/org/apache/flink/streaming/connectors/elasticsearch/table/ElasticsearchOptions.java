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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;

import java.time.Duration;
import java.util.List;

import static org.apache.flink.configuration.description.TextElement.text;

/**
 * Options for {@link org.apache.flink.table.factories.DynamicTableSinkFactory} for Elasticsearch.
 */
public class ElasticsearchOptions {
	/**
	 * Backoff strategy. Extends {@link ElasticsearchSinkBase.FlushBackoffType} with
	 * {@code DISABLED} option.
	 */
	public enum BackOffType {
		DISABLED,
		CONSTANT,
		EXPONENTIAL
	}

	public static final ConfigOption<List<String>> HOSTS_OPTION =
		ConfigOptions.key("hosts")
			.stringType()
			.asList()
			.noDefaultValue()
			.withDescription("Elasticseatch hosts to connect to.");
	public static final ConfigOption<String> INDEX_OPTION =
		ConfigOptions.key("index")
			.stringType()
			.noDefaultValue()
			.withDescription("Elasticsearch index for every record.");
	public static final ConfigOption<String> DOCUMENT_TYPE_OPTION =
		ConfigOptions.key("document-type")
			.stringType()
			.noDefaultValue()
			.withDescription("Elasticsearch document type.");
	public static final ConfigOption<String> KEY_DELIMITER_OPTION =
		ConfigOptions.key("document-id.key-delimiter")
			.stringType()
			.defaultValue("_")
			.withDescription("Delimiter for composite keys e.g., \"$\" would result in IDs \"KEY1$KEY2$KEY3\".");
	public static final ConfigOption<String> FAILURE_HANDLER_OPTION =
		ConfigOptions.key("failure-handler")
			.stringType()
			.defaultValue("fail")
			.withDescription(Description.builder()
				.text("Failure handling strategy in case a request to Elasticsearch fails")
				.list(
					text("\"fail\" (throws an exception if a request fails and thus causes a job failure),"),
					text("\"ignore\" (ignores failures and drops the request),"),
					text("\"retry_rejected\" (re-adds requests that have failed due to queue capacity saturation),"),
					text("\"class name\" for failure handling with a ActionRequestFailureHandler subclass"))
				.build());
	public static final ConfigOption<Boolean> FLUSH_ON_CHECKPOINT_OPTION =
		ConfigOptions.key("sink.flush-on-checkpoint")
			.booleanType()
			.defaultValue(true)
			.withDescription("Disables flushing on checkpoint");
	public static final ConfigOption<Integer> BULK_FLUSH_MAX_ACTIONS_OPTION =
		ConfigOptions.key("sink.bulk-flush.max-actions")
			.intType()
			.defaultValue(1000)
			.withDescription("Maximum number of actions to buffer for each bulk request.");
	public static final ConfigOption<MemorySize> BULK_FLASH_MAX_SIZE_OPTION =
		ConfigOptions.key("sink.bulk-flush.max-size")
			.memoryType()
			.defaultValue(MemorySize.parse("2mb"))
			.withDescription("Maximum size of buffered actions per bulk request");
	public static final ConfigOption<Duration> BULK_FLUSH_INTERVAL_OPTION =
		ConfigOptions.key("sink.bulk-flush.interval")
			.durationType()
			.defaultValue(Duration.ofSeconds(1))
			.withDescription("Bulk flush interval");
	public static final ConfigOption<BackOffType> BULK_FLUSH_BACKOFF_TYPE_OPTION =
		ConfigOptions.key("sink.bulk-flush.backoff.strategy")
			.enumType(BackOffType.class)
			.defaultValue(BackOffType.DISABLED)
			.withDescription("Backoff strategy");
	public static final ConfigOption<Integer> BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION =
		ConfigOptions.key("sink.bulk-flush.backoff.max-retries")
			.intType()
			.noDefaultValue()
			.withDescription("Maximum number of retries.");
	public static final ConfigOption<Duration> BULK_FLUSH_BACKOFF_DELAY_OPTION =
		ConfigOptions.key("sink.bulk-flush.backoff.delay")
			.durationType()
			.noDefaultValue()
			.withDescription("Delay between each backoff attempt.");
	public static final ConfigOption<Duration> CONNECTION_MAX_RETRY_TIMEOUT_OPTION =
		ConfigOptions.key("connection.max-retry-timeout")
			.durationType()
			.noDefaultValue()
			.withDescription("Maximum timeout between retries.");
	public static final ConfigOption<String> CONNECTION_PATH_PREFIX =
		ConfigOptions.key("connection.path-prefix")
			.stringType()
			.noDefaultValue()
			.withDescription("Prefix string to be added to every REST communication.");
	public static final ConfigOption<String> FORMAT_OPTION =
		ConfigOptions.key("format")
			.stringType()
			.defaultValue("json")
			.withDescription("Elasticsearch connector requires to specify a format.\n" +
				"The format must produce a valid json document. \n" +
				"By default uses built-in 'json' format. Please refer to Table Formats section for more details.");

	private ElasticsearchOptions() {
	}
}
