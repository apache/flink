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

package org.apache.flink.streaming.connectors.kinesis.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.streaming.connectors.kinesis.FixedKinesisPartitioner;
import org.apache.flink.streaming.connectors.kinesis.KinesisPartitioner;
import org.apache.flink.streaming.connectors.kinesis.RandomKinesisPartitioner;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.configuration.description.TextElement.text;

/**
 * Options for Kinesis tables supported by the {@code CREATE TABLE ... WITH ...} clause of the Flink
 * SQL dialect and the Flink Table API.
 */
@Internal
public class KinesisOptions {

    private KinesisOptions() {}

    // -----------------------------------------------------------------------------------------
    // Kinesis specific options
    // -----------------------------------------------------------------------------------------

    /**
     * Prefix for properties defined in {@link
     * org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants} that are delegated
     * to {@link org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer} and {@link
     * org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer}.
     */
    public static final String AWS_PROPERTIES_PREFIX = "aws.";

    /**
     * Prefix for properties defined in {@link
     * org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants} that are
     * delegated to {@link org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer}.
     */
    public static final String CONSUMER_PREFIX = "scan.";

    /**
     * Prefix for properties defined in {@link
     * com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration} that are delegated to
     * {@link org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer}.
     */
    public static final String PRODUCER_PREFIX = "sink.producer.";

    /**
     * Prefixes of properties that are validated by downstream components and should not be
     * validated by the Table API infrastructure.
     */
    public static final String[] NON_VALIDATED_PREFIXES =
            new String[] {AWS_PROPERTIES_PREFIX, CONSUMER_PREFIX, PRODUCER_PREFIX};

    public static final ConfigOption<String> STREAM =
            ConfigOptions.key("stream")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Name of the Kinesis stream backing this table.");

    // -----------------------------------------------------------------------------------------
    // Sink specific options
    // -----------------------------------------------------------------------------------------

    public static final ConfigOption<String> SINK_PARTITIONER =
            ConfigOptions.key("sink.partitioner")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Optional output partitioning from Flink's partitions into Kinesis shards. "
                                                    + "Sinks that write to tables defined with the %s clause always use a "
                                                    + "field-based partitioner and cannot define this option.",
                                            code("PARTITION BY"))
                                    .linebreak()
                                    .text("Valid enumerations are")
                                    .list(
                                            text("random (use a random partition key)"),
                                            text(
                                                    "fixed (each Flink partition ends up in at most one Kinesis shard)"),
                                            text(
                                                    "custom class name (use a custom %s subclass)",
                                                    text(KinesisPartitioner.class.getName())))
                                    .build());

    public static final ConfigOption<String> SINK_PARTITIONER_FIELD_DELIMITER =
            ConfigOptions.key("sink.partitioner-field-delimiter")
                    .stringType()
                    .defaultValue("|")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Optional field delimiter for fields-based partitioner derived from a %s clause",
                                            code("PARTITION BY"))
                                    .build());

    // -----------------------------------------------------------------------------------------
    // Option enumerations
    // -----------------------------------------------------------------------------------------

    public static final String SINK_PARTITIONER_VALUE_FIXED = "fixed";

    public static final String SINK_PARTITIONER_VALUE_RANDOM = "random";

    // -----------------------------------------------------------------------------------------
    // Utilities
    // -----------------------------------------------------------------------------------------

    /** Options handled and validated by the table-level layer. */
    public static final Set<String> TABLE_LEVEL_OPTIONS =
            new HashSet<>(
                    Arrays.asList(
                            STREAM.key(),
                            FactoryUtil.FORMAT.key(),
                            SINK_PARTITIONER.key(),
                            SINK_PARTITIONER_FIELD_DELIMITER.key()));

    /** Derive properties to be passed to the {@code FlinkKinesisConsumer}. */
    public static Properties getConsumerProperties(Map<String, String> tableOptions) {
        Properties properties = new Properties();

        for (Map.Entry<String, String> entry : tableOptions.entrySet()) {
            String sourceKey = entry.getKey();
            String sourceVal = entry.getValue();

            if (!TABLE_LEVEL_OPTIONS.contains(sourceKey)) {
                if (sourceKey.startsWith(AWS_PROPERTIES_PREFIX)) {
                    properties.put(translateAwsKey(sourceKey), sourceVal);
                } else if (sourceKey.startsWith(CONSUMER_PREFIX)) {
                    properties.put(translateConsumerKey(sourceKey), sourceVal);
                }
            }
        }

        return properties;
    }

    /** Derive properties to be passed to the {@code FlinkKinesisProducer}. */
    public static Properties getProducerProperties(Map<String, String> tableOptions) {
        Properties properties = new Properties();

        for (Map.Entry<String, String> entry : tableOptions.entrySet()) {
            String sourceKey = entry.getKey();
            String sourceVal = entry.getValue();

            if (!TABLE_LEVEL_OPTIONS.contains(sourceKey)) {
                if (sourceKey.startsWith(AWS_PROPERTIES_PREFIX)) {
                    properties.put(translateAwsKey(sourceKey), sourceVal);
                } else if (sourceKey.startsWith(PRODUCER_PREFIX)) {
                    properties.put(translateProducerKey(sourceKey), sourceVal);
                }
            }
        }

        return properties;
    }

    /** Map {@code scan.foo.bar} to {@code flink.foo.bar}. */
    private static String translateAwsKey(String key) {
        if (!key.endsWith("credentials.provider")) {
            return key.replace("credentials.", "credentials.provider.");
        } else {
            return key;
        }
    }

    /** Map {@code scan.foo.bar} to {@code flink.foo.bar}. */
    private static String translateConsumerKey(String key) {
        String result = "flink." + key.substring(CONSUMER_PREFIX.length());

        if (result.endsWith("initpos-timestamp-format")) {
            return result.replace("initpos-timestamp-format", "initpos.timestamp.format");
        } else if (result.endsWith("initpos-timestamp")) {
            return result.replace("initpos-timestamp", "initpos.timestamp");
        } else {
            return result;
        }
    }

    /** Map {@code sink.foo-bar} to {@code FooBar}. */
    private static String translateProducerKey(String key) {
        String suffix = key.substring(PRODUCER_PREFIX.length());
        return Arrays.stream(suffix.split("-"))
                .map(s -> s.substring(0, 1).toUpperCase() + s.substring(1))
                .collect(Collectors.joining(""));
    }

    /**
     * Constructs the kinesis partitioner for a {@code targetTable} based on the currently set
     * {@code tableOptions}.
     *
     * <p>The following rules are applied with decreasing precedence order.
     *
     * <ul>
     *   <li>If {@code targetTable} is partitioned, return a {@code RowDataKinesisPartitioner}.
     *   <li>If the partitioner type is not set, return a {@link RandomKinesisPartitioner}.
     *   <li>If a specific partitioner type alias is used, instantiate the corresponding type
     *   <li>Interpret the partitioner type as a classname of a user-defined partitioner.
     * </ul>
     *
     * @param tableOptions A read-only set of config options that determines the partitioner type.
     * @param physicalType Physical type for partitioning.
     * @param partitionKeys Partitioning keys in physical type.
     * @param classLoader A {@link ClassLoader} to use for loading user-defined partitioner classes.
     */
    public static KinesisPartitioner<RowData> getKinesisPartitioner(
            ReadableConfig tableOptions,
            RowType physicalType,
            List<String> partitionKeys,
            ClassLoader classLoader) {

        if (!partitionKeys.isEmpty()) {
            String delimiter = tableOptions.get(SINK_PARTITIONER_FIELD_DELIMITER);
            return new RowDataFieldsKinesisPartitioner(physicalType, partitionKeys, delimiter);
        } else if (!tableOptions.getOptional(SINK_PARTITIONER).isPresent()) {
            return new RandomKinesisPartitioner<>();
        } else {
            String partitioner = tableOptions.getOptional(SINK_PARTITIONER).get();
            if (SINK_PARTITIONER_VALUE_FIXED.equals(partitioner)) {
                return new FixedKinesisPartitioner<>();
            } else if (SINK_PARTITIONER_VALUE_RANDOM.equals(partitioner)) {
                return new RandomKinesisPartitioner<>();
            } else { // interpret the option value as a fully-qualified class name
                return initializePartitioner(partitioner, classLoader);
            }
        }
    }

    /** Returns a class value with the given class name. */
    private static <T> KinesisPartitioner<T> initializePartitioner(
            String name, ClassLoader classLoader) {
        try {
            Class<?> clazz = Class.forName(name, true, classLoader);
            if (!KinesisPartitioner.class.isAssignableFrom(clazz)) {
                throw new ValidationException(
                        String.format(
                                "Partitioner class '%s' should have %s in its parents chain",
                                name, KinesisPartitioner.class.getName()));
            }
            @SuppressWarnings("unchecked")
            final KinesisPartitioner<T> partitioner =
                    InstantiationUtil.instantiate(name, KinesisPartitioner.class, classLoader);

            return partitioner;
        } catch (ClassNotFoundException | FlinkException e) {
            throw new ValidationException(
                    String.format("Could not find and instantiate partitioner class '%s'", name),
                    e);
        }
    }
}
