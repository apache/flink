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
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.InlineElement;

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

    public static final ConfigOption<Boolean> TABLE_EXEC_HIVE_BUCKETING_ENABLE =
            key("table.exec.hive.bucketing.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "If it's true, for Hive bucketed table, all data files belonged to same bucket will be read by same reader."
                                    + " Otherwise, these files can be read by different reader");

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
