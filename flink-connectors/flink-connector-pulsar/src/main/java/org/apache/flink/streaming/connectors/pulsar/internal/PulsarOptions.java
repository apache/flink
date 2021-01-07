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

package org.apache.flink.streaming.connectors.pulsar.internal;

import org.apache.flink.annotation.PublicEvolving;

import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.shade.com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * A collection of constants to talk to Pulsar.
 */
@PublicEvolving
public class PulsarOptions {

	// option key prefix for different modules
	public static final String PULSAR_CLIENT_OPTION_KEY_PREFIX = "pulsar.client.";
	public static final String PULSAR_PRODUCER_OPTION_KEY_PREFIX = "pulsar.producer.";
	public static final String PULSAR_READER_OPTION_KEY_PREFIX = "pulsar.reader.";

	// topic options
	public static final String TOPIC_SINGLE_OPTION_KEY = "topic";
	public static final String TOPIC_MULTI_OPTION_KEY = "topics";
	public static final String TOPIC_PATTERN_OPTION_KEY = "topicspattern";

	public static final String PARTITION_SUFFIX = TopicName.PARTITIONED_TOPIC_SUFFIX;

	public static final Set<String> TOPIC_OPTION_KEYS = ImmutableSet.of(
		TOPIC_SINGLE_OPTION_KEY,
		TOPIC_MULTI_OPTION_KEY,
		TOPIC_PATTERN_OPTION_KEY);

	public static final String SERVICE_URL_OPTION_KEY = "service-url";
	public static final String ADMIN_URL_OPTION_KEY = "admin-url";
	public static final String STARTUP_MODE_OPTION_KEY = "startup-mode";

	public static final String PARTITION_DISCOVERY_INTERVAL_MS_OPTION_KEY = "partition.discovery.interval-millis";
	public static final String CLIENT_CACHE_SIZE_OPTION_KEY = "client-cache-size";
	public static final String FLUSH_ON_CHECKPOINT_OPTION_KEY = "flush-on-checkpoint";
	public static final String FAIL_ON_WRITE_OPTION_KEY = "fail-on-write";
	public static final String TRANSACTION_TIMEOUT = "transaction-timeout";
	public static final String MAX_BLOCK_TIME_MS = "max-block-time-ms";
	public static final String POLL_TIMEOUT_MS_OPTION_KEY = "poll-timeout-ms";
	public static final String SEND_TIMEOUT_MS = "send-timeout-ms";
	public static final String SUBSCRIPTION_ROLE_OPTION_KEY = "subscription-role-prefix";
	public static final String COMMIT_MAX_RETRIES = "commit-max-retries";
	public static final String FAIL_ON_DATA_LOSS_OPTION_KEY = "fail-on-data-loss";
	public static final String ENABLE_KEY_HASH_RANGE_KEY = "enable-key-hash-range";
	public static final String KEY_DISABLED_METRICS = "key-disable-metrics";

	public static final String INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_FALSE =
		"Some data may have been lost because they are not available in Pulsar any more; either the\n"
			+
			" data was aged out by Pulsar or the topic may have been deleted before all the data in the\n"
			+
			" topic was processed. If you want your streaming query to fail on such cases, set the source\n"
			+
			" option \"failOnDataLoss\" to \"true\".";
	public static final String INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_TRUE =
		"Some data may have been lost because they are not available in Pulsar any more; either the\n"
			+
			" data was aged out by Pulsar or the topic may have been deleted before all the data in the\n"
			+
			" topic was processed. If you don't want your streaming query to fail on such cases, set the\n"
			+
			" source option \"failOnDataLoss\" to \"false\".";

	public static final String USE_EXTEND_FIELD = "use-extend-field";
	public static final String TOPIC_ATTRIBUTE_NAME = "__topic";
	public static final String KEY_ATTRIBUTE_NAME = "__key";
	public static final String MESSAGE_ID_NAME = "__messageId";
	public static final String PUBLISH_TIME_NAME = "__publishTime";
	public static final String EVENT_TIME_NAME = "__eventTime";

	public static final Set<String> META_FIELD_NAMES = ImmutableSet.of(
		TOPIC_ATTRIBUTE_NAME,
		KEY_ATTRIBUTE_NAME,
		MESSAGE_ID_NAME,
		PUBLISH_TIME_NAME,
		EVENT_TIME_NAME);

	public static final String DEFAULT_PARTITIONS = "table-default-partitions";
	public static final String AUTH_PARAMS_KEY = "auth-params";
	public static final String AUTH_PLUGIN_CLASSNAME_KEY = "auth-plugin-classname";
}
