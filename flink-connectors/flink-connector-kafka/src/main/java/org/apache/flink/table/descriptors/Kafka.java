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

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_PROPERTIES;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SINK_PARTITIONER;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SINK_PARTITIONER_CLASS;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SINK_PARTITIONER_VALUE_CUSTOM;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SINK_PARTITIONER_VALUE_FIXED;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SINK_PARTITIONER_VALUE_ROUND_ROBIN;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SPECIFIC_OFFSETS;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SPECIFIC_OFFSETS_OFFSET;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SPECIFIC_OFFSETS_PARTITION;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_STARTUP_MODE;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_STARTUP_TIMESTAMP_MILLIS;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_TOPIC;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_TYPE_VALUE_KAFKA;

/** Connector descriptor for the Apache Kafka message queue. */
@PublicEvolving
public class Kafka extends ConnectorDescriptor {

    private String version;
    private String topic;
    private StartupMode startupMode;
    private Map<Integer, Long> specificOffsets;
    private long startTimestampMillis;
    private Map<String, String> kafkaProperties;
    private String sinkPartitionerType;
    private Class<? extends FlinkKafkaPartitioner> sinkPartitionerClass;

    /** Connector descriptor for the Apache Kafka message queue. */
    public Kafka() {
        super(CONNECTOR_TYPE_VALUE_KAFKA, 1, true);
    }

    /**
     * Sets the Kafka version to be used.
     *
     * @param version Kafka version. E.g., "0.8", "0.11", etc.
     */
    public Kafka version(String version) {
        Preconditions.checkNotNull(version);
        this.version = version;
        return this;
    }

    /**
     * Sets the topic from which the table is read.
     *
     * @param topic The topic from which the table is read.
     */
    public Kafka topic(String topic) {
        Preconditions.checkNotNull(topic);
        this.topic = topic;
        return this;
    }

    /**
     * Sets the configuration properties for the Kafka consumer. Resets previously set properties.
     *
     * @param properties The configuration properties for the Kafka consumer.
     */
    public Kafka properties(Properties properties) {
        Preconditions.checkNotNull(properties);
        if (this.kafkaProperties == null) {
            this.kafkaProperties = new HashMap<>();
        }
        this.kafkaProperties.clear();
        properties.forEach((k, v) -> this.kafkaProperties.put((String) k, (String) v));
        return this;
    }

    /**
     * Adds a configuration properties for the Kafka consumer.
     *
     * @param key property key for the Kafka consumer
     * @param value property value for the Kafka consumer
     */
    public Kafka property(String key, String value) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(value);
        if (this.kafkaProperties == null) {
            this.kafkaProperties = new HashMap<>();
        }
        kafkaProperties.put(key, value);
        return this;
    }

    /**
     * Configures to start reading from the earliest offset for all partitions.
     *
     * @see FlinkKafkaConsumerBase#setStartFromEarliest()
     */
    public Kafka startFromEarliest() {
        this.startupMode = StartupMode.EARLIEST;
        this.specificOffsets = null;
        return this;
    }

    /**
     * Configures to start reading from the latest offset for all partitions.
     *
     * @see FlinkKafkaConsumerBase#setStartFromLatest()
     */
    public Kafka startFromLatest() {
        this.startupMode = StartupMode.LATEST;
        this.specificOffsets = null;
        return this;
    }

    /**
     * Configures to start reading from any committed group offsets found in Zookeeper / Kafka
     * brokers.
     *
     * @see FlinkKafkaConsumerBase#setStartFromGroupOffsets()
     */
    public Kafka startFromGroupOffsets() {
        this.startupMode = StartupMode.GROUP_OFFSETS;
        this.specificOffsets = null;
        return this;
    }

    /**
     * Configures to start reading partitions from specific offsets, set independently for each
     * partition. Resets previously set offsets.
     *
     * @param specificOffsets the specified offsets for partitions
     * @see FlinkKafkaConsumerBase#setStartFromSpecificOffsets(Map)
     */
    public Kafka startFromSpecificOffsets(Map<Integer, Long> specificOffsets) {
        this.startupMode = StartupMode.SPECIFIC_OFFSETS;
        this.specificOffsets = Preconditions.checkNotNull(specificOffsets);
        return this;
    }

    /**
     * Configures to start reading partitions from specific offsets and specifies the given offset
     * for the given partition.
     *
     * @param partition partition index
     * @param specificOffset partition offset to start reading from
     * @see FlinkKafkaConsumerBase#setStartFromSpecificOffsets(Map)
     */
    public Kafka startFromSpecificOffset(int partition, long specificOffset) {
        this.startupMode = StartupMode.SPECIFIC_OFFSETS;
        if (this.specificOffsets == null) {
            this.specificOffsets = new HashMap<>();
        }
        this.specificOffsets.put(partition, specificOffset);
        return this;
    }

    /**
     * Configures to start reading from partition offsets of the specified timestamp.
     *
     * @param startTimestampMillis timestamp to start reading from
     * @see FlinkKafkaConsumerBase#setStartFromTimestamp(long)
     */
    public Kafka startFromTimestamp(long startTimestampMillis) {
        this.startupMode = StartupMode.TIMESTAMP;
        this.specificOffsets = null;
        this.startTimestampMillis = startTimestampMillis;
        return this;
    }

    /**
     * Configures how to partition records from Flink's partitions into Kafka's partitions.
     *
     * <p>This strategy ensures that each Flink partition ends up in one Kafka partition.
     *
     * <p>Note: One Kafka partition can contain multiple Flink partitions. Examples:
     *
     * <p>More Flink partitions than Kafka partitions. Some (or all) Kafka partitions contain the
     * output of more than one flink partition:
     *
     * <pre>
     *     Flink Sinks            Kafka Partitions
     *         1    ----------------&gt;    1
     *         2    --------------/
     *         3    -------------/
     *         4    ------------/
     * </pre>
     *
     * <p>Fewer Flink partitions than Kafka partitions:
     *
     * <pre>
     *     Flink Sinks            Kafka Partitions
     *         1    ----------------&gt;    1
     *         2    ----------------&gt;    2
     *                                      3
     *                                      4
     *                                      5
     * </pre>
     *
     * @see org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner
     */
    public Kafka sinkPartitionerFixed() {
        sinkPartitionerType = CONNECTOR_SINK_PARTITIONER_VALUE_FIXED;
        sinkPartitionerClass = null;
        return this;
    }

    /**
     * Configures how to partition records from Flink's partitions into Kafka's partitions.
     *
     * <p>This strategy ensures that records will be distributed to Kafka partitions in a
     * round-robin fashion.
     *
     * <p>Note: This strategy is useful to avoid an unbalanced partitioning. However, it will cause
     * a lot of network connections between all the Flink instances and all the Kafka brokers.
     */
    public Kafka sinkPartitionerRoundRobin() {
        sinkPartitionerType = CONNECTOR_SINK_PARTITIONER_VALUE_ROUND_ROBIN;
        sinkPartitionerClass = null;
        return this;
    }

    /**
     * Configures how to partition records from Flink's partitions into Kafka's partitions.
     *
     * <p>This strategy allows for a custom partitioner by providing an implementation of {@link
     * FlinkKafkaPartitioner}.
     */
    public Kafka sinkPartitionerCustom(Class<? extends FlinkKafkaPartitioner> partitionerClass) {
        sinkPartitionerType = CONNECTOR_SINK_PARTITIONER_VALUE_CUSTOM;
        sinkPartitionerClass = Preconditions.checkNotNull(partitionerClass);
        return this;
    }

    @Override
    protected Map<String, String> toConnectorProperties() {
        final DescriptorProperties properties = new DescriptorProperties();

        if (version != null) {
            properties.putString(CONNECTOR_VERSION, version);
        }

        if (topic != null) {
            properties.putString(CONNECTOR_TOPIC, topic);
        }

        if (startupMode != null) {
            properties.putString(
                    CONNECTOR_STARTUP_MODE, KafkaValidator.normalizeStartupMode(startupMode));
        }

        if (specificOffsets != null) {
            final StringBuilder stringBuilder = new StringBuilder();
            int i = 0;
            for (Map.Entry<Integer, Long> specificOffset : specificOffsets.entrySet()) {
                if (i != 0) {
                    stringBuilder.append(';');
                }
                stringBuilder
                        .append(CONNECTOR_SPECIFIC_OFFSETS_PARTITION)
                        .append(':')
                        .append(specificOffset.getKey())
                        .append(',')
                        .append(CONNECTOR_SPECIFIC_OFFSETS_OFFSET)
                        .append(':')
                        .append(specificOffset.getValue());
                i++;
            }
            properties.putString(CONNECTOR_SPECIFIC_OFFSETS, stringBuilder.toString());
        }

        if (startTimestampMillis > 0) {
            properties.putString(
                    CONNECTOR_STARTUP_TIMESTAMP_MILLIS, String.valueOf(startTimestampMillis));
        }

        if (kafkaProperties != null) {
            this.kafkaProperties.forEach(
                    (key, value) -> properties.putString(CONNECTOR_PROPERTIES + '.' + key, value));
        }

        if (sinkPartitionerType != null) {
            properties.putString(CONNECTOR_SINK_PARTITIONER, sinkPartitionerType);
            if (sinkPartitionerClass != null) {
                properties.putClass(CONNECTOR_SINK_PARTITIONER_CLASS, sinkPartitionerClass);
            }
        }

        return properties.asMap();
    }
}
