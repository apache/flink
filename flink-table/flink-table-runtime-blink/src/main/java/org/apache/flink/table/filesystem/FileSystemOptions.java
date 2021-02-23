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
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.table.factories.FactoryUtil;

import java.time.Duration;

import static org.apache.flink.configuration.ConfigOptions.key;

/** This class holds configuration constants used by filesystem(Including hive) connector. */
public class FileSystemOptions {

    public static final ConfigOption<String> PATH =
            key("path").stringType().noDefaultValue().withDescription("The path of a directory");

    public static final ConfigOption<String> PARTITION_DEFAULT_NAME =
            key("partition.default-name")
                    .stringType()
                    .defaultValue("__DEFAULT_PARTITION__")
                    .withDescription(
                            "The default partition name in case the dynamic partition"
                                    + " column value is null/empty string");

    public static final ConfigOption<MemorySize> SINK_ROLLING_POLICY_FILE_SIZE =
            key("sink.rolling-policy.file-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(128))
                    .withDescription(
                            "The maximum part file size before rolling (by default 128MB).");

    public static final ConfigOption<Duration> SINK_ROLLING_POLICY_ROLLOVER_INTERVAL =
            key("sink.rolling-policy.rollover-interval")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(30))
                    .withDescription(
                            "The maximum time duration a part file can stay open before rolling"
                                    + " (by default 30 min to avoid to many small files). The frequency at which"
                                    + " this is checked is controlled by the 'sink.rolling-policy.check-interval' option.");

    public static final ConfigOption<Duration> SINK_ROLLING_POLICY_CHECK_INTERVAL =
            key("sink.rolling-policy.check-interval")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(1))
                    .withDescription(
                            "The interval for checking time based rolling policies. "
                                    + "This controls the frequency to check whether a part file should rollover based on 'sink.rolling-policy.rollover-interval'.");

    public static final ConfigOption<Boolean> SINK_SHUFFLE_BY_PARTITION =
            key("sink.shuffle-by-partition.enable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "The option to enable shuffle data by dynamic partition fields in sink"
                                    + " phase, this can greatly reduce the number of file for filesystem sink but may"
                                    + " lead data skew, the default value is disabled.");

    public static final ConfigOption<Boolean> STREAMING_SOURCE_ENABLE =
            key("streaming-source.enable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Enable streaming source or not.\n"
                                    + " NOTES: Please make sure that each partition/file should be written"
                                    + " atomically, otherwise the reader may get incomplete data.");

    public static final ConfigOption<String> STREAMING_SOURCE_PARTITION_INCLUDE =
            key("streaming-source.partition.include")
                    .stringType()
                    .defaultValue("all")
                    .withDescription(
                            "Option to set the partitions to read, the supported values "
                                    + "are \"all\" and \"latest\","
                                    + " the \"all\" means read all partitions; the \"latest\" means read latest "
                                    + "partition in order of streaming-source.partition.order, the \"latest\" only works"
                                    + " when the streaming hive source table used as temporal table. "
                                    + "By default the option is \"all\".\n.");

    public static final ConfigOption<Duration> STREAMING_SOURCE_MONITOR_INTERVAL =
            key("streaming-source.monitor-interval")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("Time interval for consecutively monitoring partition/file.");

    public static final ConfigOption<String> STREAMING_SOURCE_PARTITION_ORDER =
            key("streaming-source.partition-order")
                    .stringType()
                    .defaultValue("partition-name")
                    .withDeprecatedKeys("streaming-source.consume-order")
                    .withDescription(
                            "The partition order of streaming source,"
                                    + " support \"create-time\", \"partition-time\" and \"partition-name\"."
                                    + " \"create-time\" compares partition/file creation time, this is not the"
                                    + " partition create time in Hive metaStore, but the folder/file modification"
                                    + " time in filesystem, if the partition folder somehow gets updated,"
                                    + " e.g. add new file into folder, it can affect how the data is consumed."
                                    + " \"partition-time\" compares the time extracted from partition name."
                                    + " \"partition-name\" compares partition name's alphabetical order."
                                    + " This option is equality with deprecated option \"streaming-source.consume-order\".");

    public static final ConfigOption<String> STREAMING_SOURCE_CONSUME_START_OFFSET =
            key("streaming-source.consume-start-offset")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Start offset for streaming consuming."
                                    + " How to parse and compare offsets depends on your order."
                                    + " For create-time and partition-time, should be a timestamp"
                                    + " string (yyyy-[m]m-[d]d [hh:mm:ss])."
                                    + " For partition-time, will use partition time extractor to"
                                    + " extract time from partition."
                                    + " For partition-name, is the partition name string, e.g.:"
                                    + " pt_year=2020/pt_mon=10/pt_day=01");

    public static final ConfigOption<String> PARTITION_TIME_EXTRACTOR_KIND =
            key("partition.time-extractor.kind")
                    .stringType()
                    .defaultValue("default")
                    .withDescription(
                            "Time extractor to extract time from partition values."
                                    + " Support default and custom."
                                    + " For default, can configure timestamp pattern."
                                    + " For custom, should configure extractor class.");

    public static final ConfigOption<String> PARTITION_TIME_EXTRACTOR_CLASS =
            key("partition.time-extractor.class")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The extractor class for implement PartitionTimeExtractor interface.");

    public static final ConfigOption<String> PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN =
            key("partition.time-extractor.timestamp-pattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The 'default' construction way allows users to use partition"
                                    + " fields to get a legal timestamp pattern."
                                    + " Default support 'yyyy-mm-dd hh:mm:ss' from first field."
                                    + " If timestamp in partition is single field 'dt', can configure: '$dt'."
                                    + " If timestamp in partition is year, month, day, hour,"
                                    + " can configure: '$year-$month-$day $hour:00:00'."
                                    + " If timestamp in partition is dt and hour, can configure: '$dt $hour:00:00'.");

    public static final ConfigOption<Duration> LOOKUP_JOIN_CACHE_TTL =
            key("lookup.join.cache.ttl")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(60))
                    .withDescription(
                            "The cache TTL (e.g. 10min) for the build table in lookup join. "
                                    + "By default the TTL is 60 minutes.");

    public static final ConfigOption<String> SINK_PARTITION_COMMIT_TRIGGER =
            key("sink.partition-commit.trigger")
                    .stringType()
                    .defaultValue("process-time")
                    .withDescription(
                            "Trigger type for partition commit:\n"
                                    + " 'process-time': based on the time of the machine, it neither requires"
                                    + " partition time extraction nor watermark generation. Commit partition"
                                    + " once the 'current system time' passes 'partition creation system time' plus 'delay'.\n"
                                    + " 'partition-time': based on the time that extracted from partition values,"
                                    + " it requires watermark generation. Commit partition once the 'watermark'"
                                    + " passes 'time extracted from partition values' plus 'delay'.");

    public static final ConfigOption<Duration> SINK_PARTITION_COMMIT_DELAY =
            key("sink.partition-commit.delay")
                    .durationType()
                    .defaultValue(Duration.ofMillis(0))
                    .withDescription(
                            "The partition will not commit until the delay time."
                                    + " if it is a day partition, should be '1 d',"
                                    + " if it is a hour partition, should be '1 h'");

    public static final ConfigOption<String> SINK_PARTITION_COMMIT_POLICY_KIND =
            key("sink.partition-commit.policy.kind")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Policy to commit a partition is to notify the downstream"
                                    + " application that the partition has finished writing, the partition"
                                    + " is ready to be read."
                                    + " metastore: add partition to metastore. Only hive table supports metastore"
                                    + " policy, file system manages partitions through directory structure."
                                    + " success-file: add '_success' file to directory."
                                    + " Both can be configured at the same time: 'metastore,success-file'."
                                    + " custom: use policy class to create a commit policy."
                                    + " Support to configure multiple policies: 'metastore,success-file'.");

    public static final ConfigOption<String> SINK_PARTITION_COMMIT_POLICY_CLASS =
            key("sink.partition-commit.policy.class")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The partition commit policy class for implement"
                                    + " PartitionCommitPolicy interface. Only work in custom commit policy");

    public static final ConfigOption<String> SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME =
            key("sink.partition-commit.success-file.name")
                    .stringType()
                    .defaultValue("_SUCCESS")
                    .withDescription(
                            "The file name for success-file partition commit policy,"
                                    + " default is '_SUCCESS'.");

    public static final ConfigOption<Boolean> AUTO_COMPACTION =
            key("auto-compaction")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to enable automatic compaction in streaming sink or not.\n"
                                    + "The data will be written to temporary files. After the checkpoint is"
                                    + " completed, the temporary files generated by a checkpoint will be compacted.\n"
                                    + "The temporary files are invisible before compaction.");

    public static final ConfigOption<MemorySize> COMPACTION_FILE_SIZE =
            key("compaction.file-size")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "The compaction target file size, the default value is the rolling file size.");

    public static final ConfigOption<Integer> SINK_PARALLELISM = FactoryUtil.SINK_PARALLELISM;
}
