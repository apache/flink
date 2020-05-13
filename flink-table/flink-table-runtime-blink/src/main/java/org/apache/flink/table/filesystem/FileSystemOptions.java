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
					.withDescription("Enable streaming source or not.\n" +
							"NOTES: For non-partition table, please make sure that " +
							"each file should be put atomically into the target directory, " +
							"otherwise the reader may get incomplete data.");

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
							" partition-time compare time represented by partition name.\n" +
							"For non-partition table, this value should always be 'create-time'.");

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

	public static final ConfigOption<Duration> LOOKUP_JOIN_CACHE_TTL =
			key("lookup.join.cache.ttl")
					.durationType()
					.defaultValue(Duration.ofMinutes(60))
					.withDescription("The cache TTL (e.g. 10min) for the build table in lookup join. " +
							"By default the TTL is 60 minutes.");

	public static final ConfigOption<String> SINK_PARTITION_COMMIT_TRIGGER =
			key("sink.partition-commit.trigger")
					.stringType()
					.defaultValue("partition-time")
					.withDescription("Trigger type for partition commit:" +
							" 'partition-time': extract time from partition," +
							" if 'watermark' > 'partition-time' + 'delay', will commit the partition." +
							" 'process-time': use processing time, if 'current processing time' > " +
							"'partition directory creation time' + 'delay', will commit the partition.");

	public static final ConfigOption<Duration> SINK_PARTITION_COMMIT_DELAY =
			key("sink.partition-commit.delay")
					.durationType()
					.defaultValue(Duration.ofMillis(0))
					.withDescription("The partition will not commit until the delay time." +
							" if it is a day partition, should be '1 d'," +
							" if it is a hour partition, should be '1 h'");

	public static final ConfigOption<String> SINK_PARTITION_COMMIT_POLICY_KIND =
			key("sink.partition-commit.policy.kind")
					.stringType()
					.noDefaultValue()
					.withDescription("Policy to commit a partition is to notify the downstream" +
							" application that the partition has finished writing, the partition" +
							" is ready to be read." +
							" metastore: add partition to metastore. Only work with hive table," +
							" it is empty implementation for file system table." +
							" success-file: add '_success' file to directory." +
							" Both can be configured at the same time: 'metastore,success-file'." +
							" custom: use policy class to create a commit policy." +
							" Support to configure multiple policies: 'metastore,success-file'.");

	public static final ConfigOption<String> SINK_PARTITION_COMMIT_POLICY_CLASS =
			key("sink.partition-commit.policy.class")
					.stringType()
					.noDefaultValue()
					.withDescription("The partition commit policy class for implement" +
							" PartitionCommitPolicy interface. Only work in custom commit policy");

	public static final ConfigOption<String> SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME =
			key("sink.partition-commit.success-file.name")
					.stringType()
					.defaultValue("_SUCCESS")
					.withDescription("The file name for success-file partition commit policy," +
							" default is '_SUCCESS'.");
}
