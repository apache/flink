/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.v2;

import org.apache.flink.configuration.ConfigOption;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.configuration.ConfigOptions.key;

/** KAFKA options. */
public class KafkaOptions {
	public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

	public static final ConfigOption<Long> START_TIME_MILLS = key("startTimeMs".toLowerCase()).defaultValue(-1L);
	public static final ConfigOption<String> TIME_ZONE = key("timeZone".toLowerCase()).noDefaultValue();


	public static final String KAFKA_KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS = "flink.partition-discovery.interval-millis";
	public static final ConfigOption<String> TOPIC = key("topic".toLowerCase()).noDefaultValue();
	public static final ConfigOption<String> TOPIC_PATTERN = key("topicPattern".toLowerCase()).noDefaultValue();
	public static final ConfigOption<String> EXTRA_CONFIG = key("extraConfig".toLowerCase()).noDefaultValue();
	public static final ConfigOption<String> STARTUP_MODE = key("startupMode".toLowerCase()).noDefaultValue();
	public static final ConfigOption<String> OPTIONAL_CONVERTER_CLASS = key("converterClass".toLowerCase())
		.noDefaultValue();
	public static final ConfigOption<String> OPTIONAL_START_TIME = key("startTime".toLowerCase())
		.noDefaultValue();
	public static final ConfigOption<Long> PARTITION_DISCOVERY_INTERVAL_MS =
		key("partitionDiscoveryIntervalMS".toLowerCase()).defaultValue(60000L);

	public static final List<String> SUPPORTED_KEYS = Arrays.asList(TOPIC.key(), TOPIC_PATTERN.key(),
		EXTRA_CONFIG.key(), STARTUP_MODE.key(), OPTIONAL_CONVERTER_CLASS.key(), OPTIONAL_START_TIME.key(),
		PARTITION_DISCOVERY_INTERVAL_MS.key(), START_TIME_MILLS.key(), TIME_ZONE.key());
}
