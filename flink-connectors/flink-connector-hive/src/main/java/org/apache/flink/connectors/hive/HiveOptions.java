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

package org.apache.flink.connectors.hive;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.InlineElement;
import org.apache.flink.connector.file.table.FileSystemConnectorOptions;

import java.time.Duration;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.TextElement.text;

/** This class holds configuration constants used by hive connector. */
public class HiveOptions {

    public static final ConfigOption<Boolean> TABLE_EXEC_HIVE_FALLBACK_MAPRED_READER =
            key("table.exec.hive.fallback-mapred-reader")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If it is false, using flink native vectorized reader to read orc files; "
                                    + "If it is true, using hadoop mapred record reader to read orc files.");

    public static final ConfigOption<Boolean>
            TABLE_EXEC_HIVE_READ_PARTITION_WITH_SUBDIRECTORY_ENABLED =
                    key("table.exec.hive.read-partition-with-subdirectory.enabled")
                            .booleanType()
                            .defaultValue(true)
                            .withDescription(
                                    "If it is true, flink will read the files of partitioned hive table from subdirectories under the partition directory to be read.\n"
                                            + "If it is false, an exception that 'not a file: xxx' will be thrown when the partition directory contains any sub-directory.");

    public static final ConfigOption<Boolean> TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM =
            key("table.exec.hive.infer-source-parallelism")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "If is false, parallelism of source are set by config.\n"
                                    + "If is true, source parallelism is inferred according to splits number.\n");

    public static final ConfigOption<Integer> TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM_MAX =
            key("table.exec.hive.infer-source-parallelism.max")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Sets max infer parallelism for source operator.");

    public static final ConfigOption<Boolean> TABLE_EXEC_HIVE_FALLBACK_MAPRED_WRITER =
            key("table.exec.hive.fallback-mapred-writer")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "If it is false, using flink native writer to write parquet and orc files; "
                                    + "If it is true, using hadoop mapred record writer to write parquet and orc files.");

    public static final ConfigOption<Integer> TABLE_EXEC_HIVE_LOAD_PARTITION_SPLITS_THREAD_NUM =
            key("table.exec.hive.load-partition-splits.thread-num")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "The thread number to split hive's partitions to splits. It should be bigger than 0.");

    public static final ConfigOption<MemorySize> TABLE_EXEC_HIVE_SPLIT_MAX_BYTES =
            key("table.exec.hive.split-max-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("128mb"))
                    .withDescription(
                            "The maximum number of bytes (default is 128MB) to pack into a split while reading Hive table. A split will be assigned to a reader. "
                                    + "It only works for the Hive table stored as ORC format.");

    public static final ConfigOption<MemorySize> TABLE_EXEC_HIVE_FILE_OPEN_COST =
            key("table.exec.hive.file-open-cost")
                    .memoryType()
                    .defaultValue(MemorySize.parse("4mb"))
                    .withDescription(
                            "The estimated cost (default is 4MB) to open a file. Used to split Hive's files to splits."
                                    + " When the value is over estimated, Flink will tend to pack Hive's data into less splits, which will be helpful when Hive's table contains many small files."
                                    + " And vice versa. It only works for the Hive table stored as ORC format.");

    public static final ConfigOption<Integer> TABLE_EXEC_HIVE_CALCULATE_PARTITION_SIZE_THREAD_NUM =
            key("table.exec.hive.calculate-partition-size.thread-num")
                    .intType()
                    .defaultValue(3)
                    .withDescription("The thread number to calculate partition's size.");

    public static final ConfigOption<Boolean> TABLE_EXEC_HIVE_DYNAMIC_GROUPING_ENABLED =
            key("table.exec.hive.sink.sort-by-dynamic-partition.enable")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to enable sorting data by dynamic partition column or not when it's for dynamic partition writing."
                                    + " The default is to enable sorting."
                                    + " If it's enabled, the date will be sorted additionally by the dynamic partition columns"
                                    + " before writing into sink table."
                                    + " If it's disabled, there won't be extra sorting,"
                                    + " but it may throw OutOfMemory exception if there are too many dynamic partitions fall into same sink node."
                                    + " Note: it only works in batch mode.");

    /**
     * Hive users usually commit partition for metastore and a _SUCCESS file. That's why we create a
     * same option with {@link FileSystemConnectorOptions#SINK_PARTITION_COMMIT_POLICY_KIND} with
     * different default value.
     */
    public static final ConfigOption<String> SINK_PARTITION_COMMIT_POLICY_KIND =
            key("sink.partition-commit.policy.kind")
                    .stringType()
                    .defaultValue("metastore,success-file")
                    .withDescription(
                            "Policy to commit a partition is to notify the downstream"
                                    + " application that the partition has finished writing, the partition"
                                    + " is ready to be read."
                                    + " metastore: add partition to metastore. "
                                    + " success-file: add a success file to the partition directory. The success file name can be configured by the 'sink.partition-commit.success-file.name' option."
                                    + " Both can be configured at the same time: 'metastore,success-file'."
                                    + " custom: use policy class to create a commit policy."
                                    + " Support to configure multiple policies: 'metastore,success-file'.");

    public static final ConfigOption<Integer> TABLE_EXEC_HIVE_READ_STATISTICS_THREAD_NUM =
            key("table.exec.hive.read-statistics.thread-num")
                    .intType()
                    .defaultValue(Runtime.getRuntime().availableProcessors())
                    .withDescription(
                            "The thread number to read input format statistics. It should be bigger than 0.");

    public static final ConfigOption<MemorySize> COMPACT_SMALL_FILES_AVG_SIZE =
            key("compaction.small-files.avg-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(16))
                    .withDescription(
                            "When it's for writing Hive in batch mode and `auto-compaction` is configured to be true, if the average written file size is less this number,"
                                    + " Flink will start to compact theses files to bigger files with target size which is configured by `compaction.file-size`."
                                    + " If the `compaction.file-size` is not configured, it will use `sink.rolling-policy.file-size` as the target size.");

    public static final ConfigOption<Boolean> TABLE_EXEC_HIVE_SINK_STATISTIC_AUTO_GATHER_ENABLE =
            key("table.exec.hive.sink.statistic-auto-gather.enable")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "If it's true, Flink will gather statistic automatically during writing Hive Table."
                                    + " If it's false, no any statistic will be gathered. The default value is true."
                                    + " For ORC and Parquet format, numFiles/totalSize/numRows/rawDataSize can be gathered."
                                    + " For other format, only numFiles/totalSize can be gathered."
                                    + " Note: only batch mode supports auto gather statistic, stream mode doesn't support it yet.");

    public static final ConfigOption<Integer>
            TABLE_EXEC_HIVE_SINK_STATISTIC_AUTO_GATHER_THREAD_NUM =
                    key("table.exec.hive.sink.statistic-auto-gather.thread-num")
                            .intType()
                            .defaultValue(3)
                            .withDescription(
                                    "The number of threads used to gather statistic during writing Hive Table"
                                            + " when the table is stored as ORC or Parquet format."
                                            + " The default value is 3.");

    public static final ConfigOption<Boolean> STREAMING_SOURCE_ENABLE =
            key("streaming-source.enable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text("Enable streaming source or not.")
                                    .linebreak()
                                    .text(
                                            " NOTES: Please make sure that each partition/file should be written"
                                                    + " atomically, otherwise the reader may get incomplete data.")
                                    .build());

    public static final ConfigOption<String> STREAMING_SOURCE_PARTITION_INCLUDE =
            key("streaming-source.partition.include")
                    .stringType()
                    .defaultValue("all")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Option to set the partitions to read, supported values are")
                                    .list(
                                            text("all (read all partitions)"),
                                            text(
                                                    "latest (read latest partition in order of 'streaming-source.partition.order', this only works when a streaming Hive source table is used as a temporal table)"))
                                    .build());

    public static final ConfigOption<Duration> STREAMING_SOURCE_MONITOR_INTERVAL =
            key("streaming-source.monitor-interval")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("Time interval for consecutively monitoring partition/file.");

    public static final ConfigOption<String> STREAMING_SOURCE_CONSUME_START_OFFSET =
            key("streaming-source.consume-start-offset")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Start offset for streaming consuming. How to parse and compare offsets depends on 'streaming-source.partition-order'.")
                                    .list(
                                            text(
                                                    "For 'create-time' and 'partition-time' it should be a timestamp string (yyyy-[m]m-[d]d [hh:mm:ss])."),
                                            text(
                                                    "For 'partition-time' it will use a partition time extractor to extract the time from the partition."),
                                            text(
                                                    "For 'partition-name' it is the name of the partition, e.g. 'pt_year=2020/pt_mon=10/pt_day=01'."))
                                    .build());

    public static final ConfigOption<PartitionOrder> STREAMING_SOURCE_PARTITION_ORDER =
            key("streaming-source.partition-order")
                    .enumType(PartitionOrder.class)
                    .defaultValue(PartitionOrder.PARTITION_NAME)
                    .withDeprecatedKeys("streaming-source.consume-order")
                    .withDescription(
                            Description.builder()
                                    .text("The partition order of the streaming source.")
                                    .text(
                                            "This is a synonym for the deprecated 'streaming-source.consume-order' option.")
                                    .build());

    public static final ConfigOption<Duration> LOOKUP_JOIN_CACHE_TTL =
            key("lookup.join.cache.ttl")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(60))
                    .withDescription(
                            "The cache TTL (e.g. 10min) for the build table in lookup join.");

    public static final ConfigOption<Boolean> TABLE_EXEC_HIVE_NATIVE_AGG_FUNCTION_ENABLED =
            key("table.exec.hive.native-agg-function.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Enabling native aggregate function for hive dialect to use hash-agg strategy that can improve the aggregation performance.");

    // --------------------------------------------------------------------------------------------
    // Enums
    // --------------------------------------------------------------------------------------------

    /** Partition order used for {@link #STREAMING_SOURCE_PARTITION_ORDER}. */
    public enum PartitionOrder implements DescribedEnum {
        CREATE_TIME(
                "create-time",
                text(
                        "Compares partition / file creation time, which is not the partition creation time in the Hive metastore, "
                                + "but the folder / file modification time in the filesystem; e.g., adding a new file into "
                                + "the folder may affect how the data is consumed.")),
        PARTITION_TIME(
                "partition-time", text("Compares the time extracted from the partition name.")),
        PARTITION_NAME("partition-name", text("Compares partition names lexicographically."));

        private final String value;
        private final InlineElement description;

        PartitionOrder(String value, InlineElement description) {
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
}
