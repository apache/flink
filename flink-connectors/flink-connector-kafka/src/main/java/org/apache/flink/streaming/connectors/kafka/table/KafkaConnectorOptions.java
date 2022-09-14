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

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.InlineElement;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.table.factories.FactoryUtil;

import java.time.Duration;
import java.util.List;

import static org.apache.flink.configuration.description.TextElement.text;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT_SUFFIX;

/** Options for the Kafka connector. */
@PublicEvolving
public class KafkaConnectorOptions {

    // --------------------------------------------------------------------------------------------
    // Format options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<String> KEY_FORMAT =
            ConfigOptions.key("key" + FORMAT_SUFFIX)
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines the format identifier for encoding key data. "
                                    + "The identifier is used to discover a suitable format factory.");

    public static final ConfigOption<String> VALUE_FORMAT =
            ConfigOptions.key("value" + FORMAT_SUFFIX)
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines the format identifier for encoding value data. "
                                    + "The identifier is used to discover a suitable format factory.");

    public static final ConfigOption<List<String>> KEY_FIELDS =
            ConfigOptions.key("key.fields")
                    .stringType()
                    .asList()
                    .defaultValues()
                    .withDescription(
                            "Defines an explicit list of physical columns from the table schema "
                                    + "that configure the data type for the key format. By default, this list is "
                                    + "empty and thus a key is undefined.");

    public static final ConfigOption<ValueFieldsStrategy> VALUE_FIELDS_INCLUDE =
            ConfigOptions.key("value.fields-include")
                    .enumType(ValueFieldsStrategy.class)
                    .defaultValue(ValueFieldsStrategy.ALL)
                    .withDescription(
                            String.format(
                                    "Defines a strategy how to deal with key columns in the data type "
                                            + "of the value format. By default, '%s' physical columns of the table schema "
                                            + "will be included in the value format which means that the key columns "
                                            + "appear in the data type for both the key and value format.",
                                    ValueFieldsStrategy.ALL));

    public static final ConfigOption<String> KEY_FIELDS_PREFIX =
            ConfigOptions.key("key.fields-prefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Defines a custom prefix for all fields of the key format to avoid "
                                                    + "name clashes with fields of the value format. "
                                                    + "By default, the prefix is empty.")
                                    .linebreak()
                                    .text(
                                            String.format(
                                                    "If a custom prefix is defined, both the table schema and '%s' will work with prefixed names.",
                                                    KEY_FIELDS.key()))
                                    .linebreak()
                                    .text(
                                            "When constructing the data type of the key format, the prefix "
                                                    + "will be removed and the non-prefixed names will be used within the key format.")
                                    .linebreak()
                                    .text(
                                            String.format(
                                                    "Please note that this option requires that '%s' must be '%s'.",
                                                    VALUE_FIELDS_INCLUDE.key(),
                                                    ValueFieldsStrategy.EXCEPT_KEY))
                                    .build());

    public static final ConfigOption<Integer> SINK_PARALLELISM = FactoryUtil.SINK_PARALLELISM;

    // --------------------------------------------------------------------------------------------
    // Kafka specific options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<List<String>> TOPIC =
            ConfigOptions.key("topic")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "Topic names from which the table is read. Either 'topic' or 'topic-pattern' must be set for source. "
                                    + "Option 'topic' is required for sink.");

    public static final ConfigOption<String> TOPIC_PATTERN =
            ConfigOptions.key("topic-pattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional topic pattern from which the table is read for source. Either 'topic' or 'topic-pattern' must be set.");

    public static final ConfigOption<String> PROPS_BOOTSTRAP_SERVERS =
            ConfigOptions.key("properties.bootstrap.servers")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Required Kafka server connection string");

    public static final ConfigOption<String> PROPS_GROUP_ID =
            ConfigOptions.key("properties.group.id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Required consumer group in Kafka consumer, no need for Kafka producer");

    // --------------------------------------------------------------------------------------------
    // Scan specific options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<ScanStartupMode> SCAN_STARTUP_MODE =
            ConfigOptions.key("scan.startup.mode")
                    .enumType(ScanStartupMode.class)
                    .defaultValue(ScanStartupMode.GROUP_OFFSETS)
                    .withDescription("Startup mode for Kafka consumer.");

    public static final ConfigOption<String> SCAN_STARTUP_SPECIFIC_OFFSETS =
            ConfigOptions.key("scan.startup.specific-offsets")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional offsets used in case of \"specific-offsets\" startup mode");

    public static final ConfigOption<Long> SCAN_STARTUP_TIMESTAMP_MILLIS =
            ConfigOptions.key("scan.startup.timestamp-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional timestamp used in case of \"timestamp\" startup mode");

    public static final ConfigOption<Duration> SCAN_TOPIC_PARTITION_DISCOVERY =
            ConfigOptions.key("scan.topic-partition-discovery.interval")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional interval for consumer to discover dynamically created Kafka partitions periodically.");

    // --------------------------------------------------------------------------------------------
    // Sink specific options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<String> SINK_PARTITIONER =
            ConfigOptions.key("sink.partitioner")
                    .stringType()
                    .defaultValue("default")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Optional output partitioning from Flink's partitions into Kafka's partitions. Valid enumerations are")
                                    .list(
                                            text(
                                                    "'default' (use kafka default partitioner to partition records)"),
                                            text(
                                                    "'fixed' (each Flink partition ends up in at most one Kafka partition)"),
                                            text(
                                                    "'round-robin' (a Flink partition is distributed to Kafka partitions round-robin when 'key.fields' is not specified)"),
                                            text(
                                                    "custom class name (use custom FlinkKafkaPartitioner subclass)"))
                                    .build());

    // Disable this feature by default
    public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS =
            ConfigOptions.key("sink.buffer-flush.max-rows")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The max size of buffered records before flushing. "
                                                    + "When the sink receives many updates on the same key, "
                                                    + "the buffer will retain the last records of the same key. "
                                                    + "This can help to reduce data shuffling and avoid possible tombstone messages to the Kafka topic.")
                                    .linebreak()
                                    .text("Can be set to '0' to disable it.")
                                    .linebreak()
                                    .text(
                                            "Note both 'sink.buffer-flush.max-rows' and 'sink.buffer-flush.interval' "
                                                    + "must be set to be greater than zero to enable sink buffer flushing.")
                                    .build());

    // Disable this feature by default
    public static final ConfigOption<Duration> SINK_BUFFER_FLUSH_INTERVAL =
            ConfigOptions.key("sink.buffer-flush.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(0))
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The flush interval millis. Over this time, asynchronous threads "
                                                    + "will flush data. When the sink receives many updates on the same key, "
                                                    + "the buffer will retain the last record of the same key.")
                                    .linebreak()
                                    .text("Can be set to '0' to disable it.")
                                    .linebreak()
                                    .text(
                                            "Note both 'sink.buffer-flush.max-rows' and 'sink.buffer-flush.interval' "
                                                    + "must be set to be greater than zero to enable sink buffer flushing.")
                                    .build());

    public static final ConfigOption<DeliveryGuarantee> DELIVERY_GUARANTEE =
            ConfigOptions.key("sink.delivery-guarantee")
                    .enumType(DeliveryGuarantee.class)
                    .defaultValue(DeliveryGuarantee.AT_LEAST_ONCE)
                    .withDescription("Optional delivery guarantee when committing.");

    public static final ConfigOption<String> TRANSACTIONAL_ID_PREFIX =
            ConfigOptions.key("sink.transactional-id-prefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "If the delivery guarantee is configured as "
                                    + DeliveryGuarantee.EXACTLY_ONCE
                                    + " this value is used a prefix for the identifier of all opened Kafka transactions.");

    // --------------------------------------------------------------------------------------------
    // Enums
    // --------------------------------------------------------------------------------------------

    /** Strategies to derive the data type of a value format by considering a key format. */
    public enum ValueFieldsStrategy {
        ALL,
        EXCEPT_KEY
    }

    /** Startup mode for the Kafka consumer, see {@link #SCAN_STARTUP_MODE}. */
    public enum ScanStartupMode implements DescribedEnum {
        EARLIEST_OFFSET("earliest-offset", text("Start from the earliest offset possible.")),
        LATEST_OFFSET("latest-offset", text("Start from the latest offset.")),
        GROUP_OFFSETS(
                "group-offsets",
                text(
                        "Start from committed offsets in ZooKeeper / Kafka brokers of a specific consumer group.")),
        TIMESTAMP("timestamp", text("Start from user-supplied timestamp for each partition.")),
        SPECIFIC_OFFSETS(
                "specific-offsets",
                text("Start from user-supplied specific offsets for each partition."));

        private final String value;
        private final InlineElement description;

        ScanStartupMode(String value, InlineElement description) {
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

    private KafkaConnectorOptions() {}
}
