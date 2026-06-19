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

package org.apache.flink.connector.file.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.InlineElement;
import org.apache.flink.table.factories.FactoryUtil;

import java.time.Duration;
import java.util.List;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.TextElement.text;

/** Options for the filesystem connector. */
@PublicEvolving
public class FileSystemConnectorOptions {

    public static final ConfigOption<String> PATH =
            key("path").stringType().noDefaultValue().withDescription("The path of a directory");

    public static final ConfigOption<String> PARTITION_DEFAULT_NAME =
            key("partition.default-name")
                    .stringType()
                    .defaultValue("__DEFAULT_PARTITION__")
                    .withDescription(
                            "The default partition name in case the dynamic partition"
                                    + " column value is null/empty string.");

    public static final ConfigOption<Duration> SOURCE_MONITOR_INTERVAL =
            key("source.monitor-interval")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "The interval in which the source checks for new files. The interval must be greater than 0. "
                                    + "Each file is uniquely identified by its path, and will be processed once, as soon as it's discovered. "
                                    + "The set of files already processed is kept in state during the whole lifecycle of the source, "
                                    + "so it's persisted in checkpoints and savepoints together with the source state. "
                                    + "Shorter intervals mean that files are discovered more quickly, "
                                    + "but also imply more frequent listing or directory traversal of the file system / object store. "
                                    + "If this config option is not set, the provided path will be scanned once, hence the source will be bounded.");

    public static final ConfigOption<FileStatisticsType> SOURCE_REPORT_STATISTICS =
            key("source.report-statistics")
                    .enumType(FileStatisticsType.class)
                    .defaultValue(FileStatisticsType.ALL)
                    .withDescription(
                            "The file statistics type which the source could provide. "
                                    + "The statistics reporting is a heavy operation in some cases,"
                                    + "this config allows users to choose the statistics type according to different situations.");

    public static final ConfigOption<String> SOURCE_PATH_REGEX_PATTERN =
            key("source.path.regex-pattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The regex pattern to filter files or directories in the directory of the `path` option. "
                                    + "This regex pattern should be matched with the absolute file path."
                                    + "For example, if we want to get all files under some path like '/dir', "
                                    + "the table should set 'path'='/dir' and 'source.regex-pattern'='/dir/.*'."
                                    + "The hidden files and directories will not be matched.");

    public static final ConfigOption<MemorySize> SINK_ROLLING_POLICY_FILE_SIZE =
            key("sink.rolling-policy.file-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(128))
                    .withDescription("The maximum part file size before rolling.");

    public static final ConfigOption<Duration> SINK_ROLLING_POLICY_ROLLOVER_INTERVAL =
            key("sink.rolling-policy.rollover-interval")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(30))
                    .withDescription(
                            "The maximum time duration a part file can stay open before rolling"
                                    + " (by default long enough to avoid too many small files). The frequency at which"
                                    + " this is checked is controlled by the 'sink.rolling-policy.check-interval' option.");

    public static final ConfigOption<Duration> SINK_ROLLING_POLICY_INACTIVITY_INTERVAL =
            key("sink.rolling-policy.inactivity-interval")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(30))
                    .withDescription(
                            "The maximum time duration a part file can stay inactive before rolling"
                                    + " (by default long enough to avoid too many small files). The frequency at which"
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
                                    + " lead data skew.");

    public static final ConfigOption<String> PARTITION_TIME_EXTRACTOR_KIND =
            key("partition.time-extractor.kind")
                    .stringType()
                    .defaultValue("default")
                    .withDescription(
                            "Time extractor to extract time from partition values. "
                                    + "This can either be 'default' or a custom extractor class. "
                                    + "For 'default', you can configure a timestamp pattern.");

    public static final ConfigOption<String> PARTITION_TIME_EXTRACTOR_CLASS =
            key("partition.time-extractor.class")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The extractor class for implement PartitionTimeExtractor interface.");

    public static final ConfigOption<String> PARTITION_TIME_EXTRACTOR_TIMESTAMP_FORMATTER =
            key("partition.time-extractor.timestamp-formatter")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The formatter to format timestamp from string, it can be used with 'partition.time-extractor.timestamp-pattern', "
                                                    + "creates a formatter using the specified value. "
                                                    + "Supports multiple partition fields like '$year-$month-$day $hour:00:00'.")
                                    .list(
                                            text(
                                                    "The timestamp-formatter is compatible with "
                                                            + "Java's DateTimeFormatter."))
                                    .build());

    public static final ConfigOption<String> PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN =
            key("partition.time-extractor.timestamp-pattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "When 'partition.time-extractor.kind' is set to 'default', "
                                                    + "you can specify a pattern to get a timestamp from partitions. "
                                                    + "the formatter pattern is defined by 'partition.time-extractor.timestamp-formatter'.")
                                    .list(
                                            text(
                                                    "By default, a format of 'yyyy-MM-dd hh:mm:ss' is read from the first field."),
                                            text(
                                                    "If the timestamp in the partition is a single field called 'dt', you can use '$dt'."),
                                            text(
                                                    "If it is spread across multiple fields for year, month, day, and hour, you can use '$year-$month-$day $hour:00:00'."),
                                            text(
                                                    "If the timestamp is in fields dt and hour, you can use '$dt "
                                                            + "$hour:00:00'."),
                                            text(
                                                    "By basicDate, a format of 'yyyyMMdd' is read from the first field."),
                                            text(
                                                    "If the timestamp in the partition is a single field called 'dt', you can use '$dt'."))
                                    .build());

    public static final ConfigOption<PartitionCommitTriggerType> SINK_PARTITION_COMMIT_TRIGGER =
            key("sink.partition-commit.trigger")
                    .enumType(PartitionCommitTriggerType.class)
                    .defaultValue(PartitionCommitTriggerType.PROCESS_TIME)
                    .withDescription("Trigger type for partition commit.");

    public static final ConfigOption<Duration> SINK_PARTITION_COMMIT_DELAY =
            key("sink.partition-commit.delay")
                    .durationType()
                    .defaultValue(Duration.ofMillis(0))
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The partition will not commit until the delay time. "
                                                    + "The value should be '1 d' for day partitions and '1 h' for hour partitions.")
                                    .build());

    public static final ConfigOption<String> SINK_PARTITION_COMMIT_WATERMARK_TIME_ZONE =
            key("sink.partition-commit.watermark-time-zone")
                    .stringType()
                    .defaultValue("UTC")
                    .withDescription(
                            "The time zone to parse the long watermark value to TIMESTAMP value,"
                                    + " the parsed watermark timestamp is used to compare with partition time"
                                    + " to decide the partition should commit or not."
                                    + " The default value is 'UTC', which means the watermark is defined on TIMESTAMP column or not defined."
                                    + " If the watermark is defined on TIMESTAMP_LTZ column, the time zone of watermark is user configured time zone,"
                                    + " the value should be the user configured local time zone. The option value is either a full name"
                                    + " such as 'America/Los_Angeles', or a custom timezone id such as 'GMT-08:00'.");

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

    public static final ConfigOption<List<String>> SINK_PARTITION_COMMIT_POLICY_CLASS_PARAMETERS =
            key("sink.partition-commit.policy.class.parameters")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "The parameters passed to the constructor of the custom commit policy, "
                                    + " with multiple parameters separated by semicolons, such as 'param1;param2'."
                                    + " The configuration value will be split into a list (['param1', 'param2'])"
                                    + " and passed to the constructor of the custom commit policy class."
                                    + " This option is optional, if not configured, default constructor will be used.");

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

    public static final ConfigOption<Integer> COMPACTION_PARALLELISM =
            key("compaction.parallelism")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines a custom parallelism for the compaction operator in batch mode. By default, if this option is not define, "
                                    + "the planner will use the parallelism of the sink as the parallelism. ");

    public static final ConfigOption<Integer> SINK_PARALLELISM = FactoryUtil.SINK_PARALLELISM;

    // --------------------------------------------------------------------------------------------
    // Enums
    // --------------------------------------------------------------------------------------------

    /** Trigger types for partition commit, see {@link #SINK_PARTITION_COMMIT_TRIGGER}. */
    public enum PartitionCommitTriggerType implements DescribedEnum {
        PROCESS_TIME(
                "process-time",
                text(
                        "Based on the time of the machine, requires neither partition time extraction nor watermark generation. "
                                + "Commits partition once the current system time passes partition creation system time plus delay.")),
        PARTITION_TIME(
                "partition-time",
                text(
                        "Based on the time extracted from partition values, requires watermark generation. "
                                + "Commits partition once the watermark passes the time extracted from partition values plus delay."));

        private final String value;
        private final InlineElement description;

        PartitionCommitTriggerType(String value, InlineElement description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return description;
        }
    }

    /** Statistics types for file system, see {@link #SOURCE_REPORT_STATISTICS}. */
    public enum FileStatisticsType implements DescribedEnum {
        NONE("NONE", text("Do not report any file statistics.")),
        ALL("ALL", text("Report all file statistics that the format can provide."));

        private final String value;
        private final InlineElement description;

        FileStatisticsType(String value, InlineElement description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return description;
        }
    }

    private FileSystemConnectorOptions() {}
}
