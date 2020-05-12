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

package org.apache.flink.table.filesystem;

import org.apache.flink.configuration.ConfigOption;

import java.time.Duration;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * This class holds configuration constants used by filesystem(Including hive) connector.
 */
public class FileSystemOptions {

	public static final ConfigOption<Boolean> STREAMING_SOURCE_ENABLE =
			key("streaming-source.enable")
					.booleanType()
					.defaultValue(false)
					.withDescription("Enable streaming source or not.");

	public static final ConfigOption<Duration> STREAMING_SOURCE_MONITOR_INTERVAL =
			key("streaming-source.monitor-interval")
					.durationType()
					.defaultValue(Duration.ofMinutes(1))
					.withDescription("Time interval for consecutively monitoring partition/file.");

	public static final ConfigOption<String> STREAMING_SOURCE_CONSUME_ORDER =
			key("streaming-source.consume-order")
					.stringType()
					.defaultValue("create-time")
					.withDescription("The consume order of streaming source," +
							" support create-time and partition-time." +
							" create-time compare partition/file creation time, this is not the" +
							" partition create time in Hive metaStore, but the folder/file create" +
							" time in filesystem;" +
							" partition-time compare time represented by partition name.");

	public static final ConfigOption<String> STREAMING_SOURCE_CONSUME_START_OFFSET =
			key("streaming-source.consume-start-offset")
					.stringType()
					.defaultValue("1970-00-00")
					.withDescription("Start offset for streaming consuming." +
							" How to parse and compare offsets depends on your order." +
							" For create-time and partition-time, should be a timestamp string." +
							" For partition-time, will use partition time extractor to" +
							" extract time from partition.");

	public static final ConfigOption<String> PARTITION_TIME_EXTRACTOR_KIND =
			key("partition.time-extractor.kind")
					.stringType()
					.defaultValue("default")
					.withDescription("Time extractor to extract time from partition values." +
							" Support default and custom." +
							" For default, can configure timestamp pattern." +
							" For custom, should configure extractor class.");

	public static final ConfigOption<String> PARTITION_TIME_EXTRACTOR_CLASS =
			key("partition.time-extractor.class")
					.stringType()
					.noDefaultValue()
					.withDescription("The extractor class for implement PartitionTimeExtractor interface.");

	public static final ConfigOption<String> PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN =
			key("partition.time-extractor.timestamp-pattern")
					.stringType()
					.noDefaultValue()
					.withDescription("The 'default' construction way allows users to use partition" +
							" fields to get a legal timestamp pattern." +
							" Default support 'yyyy-mm-dd hh:mm:ss' from first field." +
							" If timestamp in partition is single field 'dt', can configure: '$dt'." +
							" If timestamp in partition is year, month, day, hour," +
							" can configure: '$year-$month-$day $hour:00:00'." +
							" If timestamp in partition is dt and hour, can configure: '$dt $hour:00:00'.");
}
