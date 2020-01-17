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

import com.google.common.collect.ImmutableList;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.shade.com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

/**
 * Multiple options keys to work with the Pulsar connector.
 */
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

	public static final String PARTITION_DISCOVERY_INTERVAL_MS_OPTION_KEY = "partitiondiscoveryintervalmillis";
	public static final String CLIENT_CACHE_SIZE_OPTION_KEY = "clientcachesize";
	public static final String FLUSH_ON_CHECKPOINT_OPTION_KEY = "flushoncheckpoint";
	public static final String FAIL_ON_WRITE_OPTION_KEY = "failonwrite";
	public static final String POLL_TIMEOUT_MS_OPTION_KEY = "polltimeoutms";
	public static final String FAIL_ON_DATA_LOSS_OPTION_KEY = "failondataloss";

	public static final String TOPIC_ATTRIBUTE_NAME = "__topic";
	public static final String KEY_ATTRIBUTE_NAME = "__key";
	public static final String MESSAGE_ID_NAME = "__messageId";
	public static final String PUBLISH_TIME_NAME = "__publishTime";
	public static final String EVENT_TIME_NAME = "__eventTime";

	public static final List<String> META_FIELD_NAMES = ImmutableList.of(
		TOPIC_ATTRIBUTE_NAME,
		KEY_ATTRIBUTE_NAME,
		MESSAGE_ID_NAME,
		PUBLISH_TIME_NAME,
		EVENT_TIME_NAME);

	public static final String DEFAULT_PARTITIONS = "table-default-partitions";
}
