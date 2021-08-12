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

package org.apache.flink.connector.kafka.table.options;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.OffsetMode;
import org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.ValueFieldsStrategy;
import org.apache.flink.connector.kafka.table.source.KafkaDynamicTableSource;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.format.Format;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.BOUNDEDNESS;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.KEY_FIELDS;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.KEY_FIELDS_PREFIX;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.KEY_FORMAT;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.PARTITIONS;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.PROPS_GROUP_ID;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.SCAN_STARTUP_SPECIFIC_OFFSETS;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.SCAN_STOP_MODE;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.SCAN_STOP_SPECIFIC_OFFSETS;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.SCAN_STOP_TIMESTAMP_MILLIS;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.SINK_PARTITIONER;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.TOPIC;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.TOPIC_PATTERN;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.VALUE_FIELDS_INCLUDE;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.VALUE_FORMAT;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;

/** Utilities for {@link KafkaConnectorOptions}. */
@Internal
public class KafkaConnectorOptionsUtil {

    private static final ConfigOption<String> SCHEMA_REGISTRY_SUBJECT =
            ConfigOptions.key("schema-registry.subject").stringType().noDefaultValue();

    // --------------------------------------------------------------------------------------------
    // Option enumerations
    // --------------------------------------------------------------------------------------------

    // Sink partitioner.
    public static final String SINK_PARTITIONER_VALUE_DEFAULT = "default";
    public static final String SINK_PARTITIONER_VALUE_FIXED = "fixed";
    public static final String SINK_PARTITIONER_VALUE_ROUND_ROBIN = "round-robin";

    // Prefix for Kafka specific properties.
    public static final String PROPERTIES_PREFIX = "properties.";

    // Other keywords.
    private static final String PARTITION = "partition";
    private static final String OFFSET = "offset";
    public static final String AVRO_CONFLUENT = "avro-confluent";
    public static final String DEBEZIUM_AVRO_CONFLUENT = "debezium-avro-confluent";
    private static final List<String> SCHEMA_REGISTRY_FORMATS =
            Arrays.asList(AVRO_CONFLUENT, DEBEZIUM_AVRO_CONFLUENT);

    // --------------------------------------------------------------------------------------------
    // Validation
    // --------------------------------------------------------------------------------------------

    public static void validateTableSourceOptions(ReadableConfig tableOptions) {
        validateConsumerGroupId(tableOptions);
        validateSourceTopicPartitionSubscription(tableOptions);
        validateOffsetMode(tableOptions, StartOrStop.START);
        validateOffsetMode(tableOptions, StartOrStop.STOP);
    }

    public static void validateTableSinkOptions(ReadableConfig tableOptions) {
        validateSinkTopic(tableOptions);
        validateSinkPartitioner(tableOptions);
    }

    private static void validateConsumerGroupId(ReadableConfig tableOptions) {
        if (!tableOptions.getOptional(PROPS_GROUP_ID).isPresent()) {
            throw new ValidationException(
                    String.format(
                            "Option '%s' is required for Kafka source table",
                            PROPS_GROUP_ID.key()));
        }
    }

    public static void validateSourceTopicPartitionSubscription(ReadableConfig tableOptions) {
        final List<String> subscriptionTypes = new ArrayList<>();

        tableOptions.getOptional(TOPIC).ifPresent((ignore) -> subscriptionTypes.add(TOPIC.key()));
        tableOptions
                .getOptional(TOPIC_PATTERN)
                .ifPresent((ignore) -> subscriptionTypes.add(TOPIC_PATTERN.key()));
        tableOptions
                .getOptional(PARTITIONS)
                .ifPresent((ignore) -> subscriptionTypes.add(PARTITIONS.key()));

        if (subscriptionTypes.size() == 0) {
            throw new ValidationException(
                    String.format(
                            "At least one topic partition subscription pattern is required: "
                                    + "'%s', '%s' or '%s'.",
                            TOPIC.key(), TOPIC_PATTERN.key(), PARTITIONS.key()));
        }
        if (subscriptionTypes.size() > 1) {
            throw new ValidationException(
                    String.format(
                            "Multiple topic partition subscription patterns shouldn't be set together: "
                                    + "%s",
                            subscriptionTypes.stream()
                                    .collect(Collectors.joining("', '", "'", "'"))));
        }
    }

    public static void validatePKConstraints(
            ObjectIdentifier tableName, CatalogTable catalogTable, Format format) {
        if (catalogTable.getSchema().getPrimaryKey().isPresent()
                && format.getChangelogMode().containsOnly(RowKind.INSERT)) {
            Configuration options = Configuration.fromMap(catalogTable.getOptions());
            String formatName =
                    options.getOptional(FactoryUtil.FORMAT).orElse(options.get(VALUE_FORMAT));
            throw new ValidationException(
                    String.format(
                            "The Kafka table '%s' with '%s' format doesn't support defining PRIMARY KEY constraint"
                                    + " on the table, because it can't guarantee the semantic of primary key.",
                            tableName.asSummaryString(), formatName));
        }
    }

    public static void validateSinkTopic(ReadableConfig tableOptions) {
        String errorMessageTemp =
                "Flink Kafka sink currently only supports single topic, but got %s: %s.";
        if (!isSingleTopic(tableOptions)) {
            if (tableOptions.getOptional(TOPIC_PATTERN).isPresent()) {
                throw new ValidationException(
                        String.format(
                                errorMessageTemp,
                                "'topic-pattern'",
                                tableOptions.get(TOPIC_PATTERN)));
            } else if (tableOptions.getOptional(PARTITIONS).isPresent()) {
                throw new ValidationException(
                        String.format(
                                errorMessageTemp, PARTITIONS.key(), tableOptions.get(PARTITIONS)));
            } else {
                throw new ValidationException(
                        String.format(errorMessageTemp, "'topic'", tableOptions.get(TOPIC)));
            }
        }
    }

    private static void validateOffsetMode(ReadableConfig tableOptions, StartOrStop startOrStop) {
        // Validate if stopping offsets are specified when running in bounded mode
        if (startOrStop.equals(StartOrStop.STOP)) {
            tableOptions
                    .getOptional(BOUNDEDNESS)
                    .ifPresent(
                            boundedness -> {
                                if (boundedness.equals(Boundedness.BOUNDED)
                                        && !tableOptions.getOptional(SCAN_STOP_MODE).isPresent()) {
                                    throw new ValidationException(
                                            String.format(
                                                    "'%s' should be specified when Kafka source is running in %s mode. ",
                                                    SCAN_STOP_MODE.key(), boundedness));
                                }
                            });
        }

        // Validate offset mode
        final ConfigOption<OffsetMode> option =
                startOrStop.equals(StartOrStop.START) ? SCAN_STARTUP_MODE : SCAN_STOP_MODE;
        tableOptions
                .getOptional(option)
                .ifPresent(
                        mode -> {
                            switch (mode) {
                                case TIMESTAMP:
                                    final ConfigOption<Long> timestampOption =
                                            startOrStop.equals(StartOrStop.START)
                                                    ? SCAN_STARTUP_TIMESTAMP_MILLIS
                                                    : SCAN_STOP_TIMESTAMP_MILLIS;
                                    if (!tableOptions.getOptional(timestampOption).isPresent()) {
                                        throw new ValidationException(
                                                String.format(
                                                        "'%s' is required in '%s' startup mode"
                                                                + " but missing.",
                                                        timestampOption.key(),
                                                        OffsetMode.TIMESTAMP));
                                    }

                                    break;
                                case SPECIFIC_OFFSETS:
                                    final ConfigOption<String> specificOffsetsOption =
                                            startOrStop.equals(StartOrStop.START)
                                                    ? SCAN_STARTUP_SPECIFIC_OFFSETS
                                                    : SCAN_STOP_SPECIFIC_OFFSETS;
                                    if (!tableOptions
                                            .getOptional(specificOffsetsOption)
                                            .isPresent()) {
                                        throw new ValidationException(
                                                String.format(
                                                        "'%s' is required in '%s' startup mode"
                                                                + " but missing.",
                                                        specificOffsetsOption.key(),
                                                        OffsetMode.SPECIFIC_OFFSETS));
                                    }
                                    if (!isSingleTopic(tableOptions)) {
                                        throw new ValidationException(
                                                "Currently Kafka source only supports specific offset for single topic.");
                                    }
                                    String specificOffsets =
                                            tableOptions.get(specificOffsetsOption);
                                    parseSpecificOffsets(
                                            specificOffsets, specificOffsetsOption.key());

                                    break;
                            }
                        });
    }

    private static void validateSinkPartitioner(ReadableConfig tableOptions) {
        tableOptions
                .getOptional(SINK_PARTITIONER)
                .ifPresent(
                        partitioner -> {
                            if (partitioner.equals(SINK_PARTITIONER_VALUE_ROUND_ROBIN)
                                    && tableOptions.getOptional(KEY_FIELDS).isPresent()) {
                                throw new ValidationException(
                                        "Currently 'round-robin' partitioner only works when option 'key.fields' is not specified.");
                            } else if (partitioner.isEmpty()) {
                                throw new ValidationException(
                                        String.format(
                                                "Option '%s' should be a non-empty string.",
                                                SINK_PARTITIONER.key()));
                            }
                        });
    }

    // --------------------------------------------------------------------------------------------
    // Validations for legacy source
    // --------------------------------------------------------------------------------------------

    public static void validateLegacyTableSourceOptions(ReadableConfig tableOptions) {
        validateSourceTopic(tableOptions);
        validateScanStartupMode(tableOptions);
    }

    private static void validateSourceTopic(ReadableConfig tableOptions) {
        Optional<List<String>> topic = tableOptions.getOptional(TOPIC);
        Optional<String> pattern = tableOptions.getOptional(TOPIC_PATTERN);

        if (topic.isPresent() && pattern.isPresent()) {
            throw new ValidationException(
                    "Option 'topic' and 'topic-pattern' shouldn't be set together.");
        }

        if (!topic.isPresent() && !pattern.isPresent()) {
            throw new ValidationException("Either 'topic' or 'topic-pattern' must be set.");
        }
    }

    private static void validateScanStartupMode(ReadableConfig tableOptions) {
        tableOptions
                .getOptional(SCAN_STARTUP_MODE)
                .ifPresent(
                        mode -> {
                            switch (mode) {
                                case TIMESTAMP:
                                    if (!tableOptions
                                            .getOptional(SCAN_STARTUP_TIMESTAMP_MILLIS)
                                            .isPresent()) {
                                        throw new ValidationException(
                                                String.format(
                                                        "'%s' is required in '%s' startup mode"
                                                                + " but missing.",
                                                        SCAN_STARTUP_TIMESTAMP_MILLIS.key(),
                                                        OffsetMode.TIMESTAMP));
                                    }

                                    break;
                                case SPECIFIC_OFFSETS:
                                    if (!tableOptions
                                            .getOptional(SCAN_STARTUP_SPECIFIC_OFFSETS)
                                            .isPresent()) {
                                        throw new ValidationException(
                                                String.format(
                                                        "'%s' is required in '%s' startup mode"
                                                                + " but missing.",
                                                        SCAN_STARTUP_SPECIFIC_OFFSETS.key(),
                                                        OffsetMode.SPECIFIC_OFFSETS));
                                    }
                                    if (!isSingleTopic(tableOptions)) {
                                        throw new ValidationException(
                                                "Currently Kafka source only supports specific offset for single topic.");
                                    }
                                    String specificOffsets =
                                            tableOptions.get(SCAN_STARTUP_SPECIFIC_OFFSETS);
                                    parseSpecificOffsets(
                                            specificOffsets, SCAN_STARTUP_SPECIFIC_OFFSETS.key());

                                    break;
                            }
                        });
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    public static KafkaDynamicTableSource.TopicPartitionSubscription getTopicPartitionSubscription(
            ReadableConfig tableOptions) {
        if (tableOptions.getOptional(TOPIC).isPresent()) {
            return KafkaDynamicTableSource.TopicPartitionSubscription.topics(
                    tableOptions.get(TOPIC));
        }
        if (tableOptions.getOptional(TOPIC_PATTERN).isPresent()) {
            return KafkaDynamicTableSource.TopicPartitionSubscription.topicPattern(
                    Pattern.compile(tableOptions.get(TOPIC_PATTERN)));
        }
        if (tableOptions.getOptional(PARTITIONS).isPresent()) {
            return KafkaDynamicTableSource.TopicPartitionSubscription.partitions(
                    parsePartitionSet(tableOptions));
        }
        throw new TableException(
                "At lease one kind of topic partition subscription should be specified. "
                        + "Validator should have checked that.");
    }

    private static Set<TopicPartition> parsePartitionSet(ReadableConfig tableOptions) {
        return tableOptions
                .getOptional(PARTITIONS)
                .map(
                        partitionList -> {
                            final Set<TopicPartition> partitionSet = new HashSet<>();
                            partitionList.forEach(
                                    topicPartitionString -> {
                                        final String[] split = topicPartitionString.split(":");
                                        if (split.length != 2) {
                                            throw new TableException(
                                                    String.format(
                                                            "Unrecognized topic partition string \"%s\"",
                                                            topicPartitionString));
                                        }
                                        partitionSet.add(
                                                new TopicPartition(
                                                        split[0], Integer.parseInt(split[1])));
                                    });
                            return partitionSet;
                        })
                .orElseThrow(
                        () ->
                                new TableException(
                                        String.format(
                                                "Option \"%s\" does not exist in table options",
                                                PARTITIONS.key())));
    }

    public static List<String> getSourceTopics(ReadableConfig tableOptions) {
        return tableOptions.getOptional(TOPIC).orElse(null);
    }

    public static Pattern getSourceTopicPattern(ReadableConfig tableOptions) {
        return tableOptions.getOptional(TOPIC_PATTERN).map(Pattern::compile).orElse(null);
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private static boolean isSingleTopic(ReadableConfig tableOptions) {
        // Option 'topic-pattern' is regarded as multi-topics.
        return tableOptions.getOptional(TOPIC).map(t -> t.size() == 1).orElse(false);
    }

    public static OffsetsInitializer getOffsetsInitializer(
            ReadableConfig tableOptions, StartOrStop startOrStop) {
        ConfigOption<OffsetMode> option =
                startOrStop.equals(StartOrStop.START) ? SCAN_STARTUP_MODE : SCAN_STOP_MODE;
        return tableOptions
                .getOptional(option)
                .map(
                        mode -> {
                            switch (mode) {
                                case EARLIEST_OFFSET:
                                    return OffsetsInitializer.earliest();
                                case LATEST_OFFSET:
                                    return OffsetsInitializer.latest();
                                case GROUP_OFFSETS:
                                    return OffsetsInitializer.committedOffsets();
                                case SPECIFIC_OFFSETS:
                                    return OffsetsInitializer.offsets(
                                            buildSpecificOffsets(
                                                    tableOptions,
                                                    tableOptions.get(TOPIC).get(0),
                                                    startOrStop));
                                case TIMESTAMP:
                                    final ConfigOption<Long> timestampOption =
                                            startOrStop.equals(StartOrStop.START)
                                                    ? SCAN_STARTUP_TIMESTAMP_MILLIS
                                                    : SCAN_STOP_TIMESTAMP_MILLIS;
                                    return OffsetsInitializer.timestamp(
                                            tableOptions.get(timestampOption));
                                default:
                                    throw new TableException(
                                            "Unsupported startup mode. Validator should have checked that.");
                            }
                        })
                .orElseGet(
                        () -> {
                            switch (startOrStop) {
                                case START:
                                    return OffsetsInitializer.committedOffsets();
                                case STOP:
                                    return null;
                                default:
                                    throw new TableException(
                                            "OffsetsInitializer is only available for starting and stopping offsets");
                            }
                        });
    }

    public static Boundedness getBoundedness(ReadableConfig tableOptions) {
        return tableOptions
                .getOptional(BOUNDEDNESS)
                .orElse(
                        tableOptions.getOptional(SCAN_STOP_MODE).isPresent()
                                ? Boundedness.BOUNDED
                                : Boundedness.CONTINUOUS_UNBOUNDED);
    }

    public static StartupOptions getStartupOptions(ReadableConfig tableOptions) {
        final Map<KafkaTopicPartition, Long> specificOffsets = new HashMap<>();
        final StartupMode startupMode =
                tableOptions
                        .getOptional(SCAN_STARTUP_MODE)
                        .map(StartupMode::fromOption)
                        .orElse(StartupMode.GROUP_OFFSETS);
        if (startupMode == StartupMode.SPECIFIC_OFFSETS) {
            // It will be refactored after support specific offset for multiple topics in
            // FLINK-18602. We have already checked tableOptions.get(TOPIC) contains one topic in
            // validateScanStartupMode().
            buildSpecificOffsets(tableOptions, tableOptions.get(TOPIC).get(0), specificOffsets);
        }

        final StartupOptions options = new StartupOptions();
        options.startupMode = startupMode;
        options.specificOffsets = specificOffsets;
        if (startupMode == StartupMode.TIMESTAMP) {
            options.startupTimestampMillis = tableOptions.get(SCAN_STARTUP_TIMESTAMP_MILLIS);
        }
        return options;
    }

    private static Map<TopicPartition, Long> buildSpecificOffsets(
            ReadableConfig tableOptions, String topic, StartOrStop startOrStop) {
        final Map<TopicPartition, Long> topicPartitionOffsets = new HashMap<>();
        final ConfigOption<String> offsetOption =
                startOrStop.equals(StartOrStop.START)
                        ? SCAN_STARTUP_SPECIFIC_OFFSETS
                        : SCAN_STOP_SPECIFIC_OFFSETS;
        String specificOffsetsStrOpt = tableOptions.get(offsetOption);
        final Map<Integer, Long> offsetMap =
                parseSpecificOffsets(specificOffsetsStrOpt, offsetOption.key());
        offsetMap.forEach(
                (partition, offset) ->
                        topicPartitionOffsets.put(new TopicPartition(topic, partition), offset));
        return topicPartitionOffsets;
    }

    private static void buildSpecificOffsets(
            ReadableConfig tableOptions,
            String topic,
            Map<KafkaTopicPartition, Long> specificOffsets) {
        String specificOffsetsStrOpt = tableOptions.get(SCAN_STARTUP_SPECIFIC_OFFSETS);
        final Map<Integer, Long> offsetMap =
                parseSpecificOffsets(specificOffsetsStrOpt, SCAN_STARTUP_SPECIFIC_OFFSETS.key());
        offsetMap.forEach(
                (partition, offset) -> {
                    final KafkaTopicPartition topicPartition =
                            new KafkaTopicPartition(topic, partition);
                    specificOffsets.put(topicPartition, offset);
                });
    }

    public static Properties getKafkaProperties(Map<String, String> tableOptions) {
        final Properties kafkaProperties = new Properties();

        if (hasKafkaClientProperties(tableOptions)) {
            tableOptions.keySet().stream()
                    .filter(key -> key.startsWith(PROPERTIES_PREFIX))
                    .forEach(
                            key -> {
                                final String value = tableOptions.get(key);
                                final String subKey = key.substring((PROPERTIES_PREFIX).length());
                                kafkaProperties.put(subKey, value);
                            });
        }
        return kafkaProperties;
    }

    /**
     * The partitioner can be either "fixed", "round-robin" or a customized partitioner full class
     * name.
     */
    public static Optional<FlinkKafkaPartitioner<RowData>> getFlinkKafkaPartitioner(
            ReadableConfig tableOptions, ClassLoader classLoader) {
        return tableOptions
                .getOptional(SINK_PARTITIONER)
                .flatMap(
                        (String partitioner) -> {
                            switch (partitioner) {
                                case SINK_PARTITIONER_VALUE_FIXED:
                                    return Optional.of(new FlinkFixedPartitioner<>());
                                case SINK_PARTITIONER_VALUE_DEFAULT:
                                case SINK_PARTITIONER_VALUE_ROUND_ROBIN:
                                    return Optional.empty();
                                    // Default fallback to full class name of the partitioner.
                                default:
                                    return Optional.of(
                                            initializePartitioner(partitioner, classLoader));
                            }
                        });
    }

    /**
     * Parses SpecificOffsets String to Map.
     *
     * <p>SpecificOffsets String format was given as following:
     *
     * <pre>
     *     scan.startup.specific-offsets = partition:0,offset:42;partition:1,offset:300
     * </pre>
     *
     * @return SpecificOffsets with Map format, key is partition, and value is offset
     */
    public static Map<Integer, Long> parseSpecificOffsets(
            String specificOffsetsStr, String optionKey) {
        final Map<Integer, Long> offsetMap = new HashMap<>();
        final String[] pairs = specificOffsetsStr.split(";");
        final String validationExceptionMessage =
                String.format(
                        "Invalid properties '%s' should follow the format "
                                + "'partition:0,offset:42;partition:1,offset:300', but is '%s'.",
                        optionKey, specificOffsetsStr);

        if (pairs.length == 0) {
            throw new ValidationException(validationExceptionMessage);
        }

        for (String pair : pairs) {
            if (null == pair || pair.length() == 0 || !pair.contains(",")) {
                throw new ValidationException(validationExceptionMessage);
            }

            final String[] kv = pair.split(",");
            if (kv.length != 2
                    || !kv[0].startsWith(PARTITION + ':')
                    || !kv[1].startsWith(OFFSET + ':')) {
                throw new ValidationException(validationExceptionMessage);
            }

            String partitionValue = kv[0].substring(kv[0].indexOf(":") + 1);
            String offsetValue = kv[1].substring(kv[1].indexOf(":") + 1);
            try {
                final Integer partition = Integer.valueOf(partitionValue);
                final Long offset = Long.valueOf(offsetValue);
                offsetMap.put(partition, offset);
            } catch (NumberFormatException e) {
                throw new ValidationException(validationExceptionMessage, e);
            }
        }
        return offsetMap;
    }

    /**
     * Decides if the table options contains Kafka client properties that start with prefix
     * 'properties'.
     */
    private static boolean hasKafkaClientProperties(Map<String, String> tableOptions) {
        return tableOptions.keySet().stream().anyMatch(k -> k.startsWith(PROPERTIES_PREFIX));
    }

    /** Returns a class value with the given class name. */
    private static <T> FlinkKafkaPartitioner<T> initializePartitioner(
            String name, ClassLoader classLoader) {
        try {
            Class<?> clazz = Class.forName(name, true, classLoader);
            if (!FlinkKafkaPartitioner.class.isAssignableFrom(clazz)) {
                throw new ValidationException(
                        String.format(
                                "Sink partitioner class '%s' should extend from the required class %s",
                                name, FlinkKafkaPartitioner.class.getName()));
            }
            @SuppressWarnings("unchecked")
            final FlinkKafkaPartitioner<T> kafkaPartitioner =
                    InstantiationUtil.instantiate(name, FlinkKafkaPartitioner.class, classLoader);

            return kafkaPartitioner;
        } catch (ClassNotFoundException | FlinkException e) {
            throw new ValidationException(
                    String.format("Could not find and instantiate partitioner class '%s'", name),
                    e);
        }
    }

    /**
     * Creates an array of indices that determine which physical fields of the table schema to
     * include in the key format and the order that those fields have in the key format.
     *
     * <p>See {@link KafkaConnectorOptions#KEY_FORMAT}, {@link KafkaConnectorOptions#KEY_FIELDS},
     * and {@link KafkaConnectorOptions#KEY_FIELDS_PREFIX} for more information.
     */
    public static int[] createKeyFormatProjection(
            ReadableConfig options, DataType physicalDataType) {
        final LogicalType physicalType = physicalDataType.getLogicalType();
        Preconditions.checkArgument(
                hasRoot(physicalType, LogicalTypeRoot.ROW), "Row data type expected.");
        final Optional<String> optionalKeyFormat = options.getOptional(KEY_FORMAT);
        final Optional<List<String>> optionalKeyFields = options.getOptional(KEY_FIELDS);

        if (!optionalKeyFormat.isPresent() && optionalKeyFields.isPresent()) {
            throw new ValidationException(
                    String.format(
                            "The option '%s' can only be declared if a key format is defined using '%s'.",
                            KEY_FIELDS.key(), KEY_FORMAT.key()));
        } else if (optionalKeyFormat.isPresent()
                && (!optionalKeyFields.isPresent() || optionalKeyFields.get().size() == 0)) {
            throw new ValidationException(
                    String.format(
                            "A key format '%s' requires the declaration of one or more of key fields using '%s'.",
                            KEY_FORMAT.key(), KEY_FIELDS.key()));
        }

        if (!optionalKeyFormat.isPresent()) {
            return new int[0];
        }

        final String keyPrefix = options.getOptional(KEY_FIELDS_PREFIX).orElse("");

        final List<String> keyFields = optionalKeyFields.get();
        final List<String> physicalFields = LogicalTypeChecks.getFieldNames(physicalType);
        return keyFields.stream()
                .mapToInt(
                        keyField -> {
                            final int pos = physicalFields.indexOf(keyField);
                            // check that field name exists
                            if (pos < 0) {
                                throw new ValidationException(
                                        String.format(
                                                "Could not find the field '%s' in the table schema for usage in the key format. "
                                                        + "A key field must be a regular, physical column. "
                                                        + "The following columns can be selected in the '%s' option:\n"
                                                        + "%s",
                                                keyField, KEY_FIELDS.key(), physicalFields));
                            }
                            // check that field name is prefixed correctly
                            if (!keyField.startsWith(keyPrefix)) {
                                throw new ValidationException(
                                        String.format(
                                                "All fields in '%s' must be prefixed with '%s' when option '%s' "
                                                        + "is set but field '%s' is not prefixed.",
                                                KEY_FIELDS.key(),
                                                keyPrefix,
                                                KEY_FIELDS_PREFIX.key(),
                                                keyField));
                            }
                            return pos;
                        })
                .toArray();
    }

    /**
     * Creates an array of indices that determine which physical fields of the table schema to
     * include in the value format.
     *
     * <p>See {@link KafkaConnectorOptions#VALUE_FORMAT}, {@link
     * KafkaConnectorOptions#VALUE_FIELDS_INCLUDE}, and {@link
     * KafkaConnectorOptions#KEY_FIELDS_PREFIX} for more information.
     */
    public static int[] createValueFormatProjection(
            ReadableConfig options, DataType physicalDataType) {
        final LogicalType physicalType = physicalDataType.getLogicalType();
        Preconditions.checkArgument(
                hasRoot(physicalType, LogicalTypeRoot.ROW), "Row data type expected.");
        final int physicalFieldCount = LogicalTypeChecks.getFieldCount(physicalType);
        final IntStream physicalFields = IntStream.range(0, physicalFieldCount);

        final String keyPrefix = options.getOptional(KEY_FIELDS_PREFIX).orElse("");

        final ValueFieldsStrategy strategy = options.get(VALUE_FIELDS_INCLUDE);
        if (strategy == ValueFieldsStrategy.ALL) {
            if (keyPrefix.length() > 0) {
                throw new ValidationException(
                        String.format(
                                "A key prefix is not allowed when option '%s' is set to '%s'. "
                                        + "Set it to '%s' instead to avoid field overlaps.",
                                VALUE_FIELDS_INCLUDE.key(),
                                ValueFieldsStrategy.ALL,
                                ValueFieldsStrategy.EXCEPT_KEY));
            }
            return physicalFields.toArray();
        } else if (strategy == ValueFieldsStrategy.EXCEPT_KEY) {
            final int[] keyProjection = createKeyFormatProjection(options, physicalDataType);
            return physicalFields
                    .filter(pos -> IntStream.of(keyProjection).noneMatch(k -> k == pos))
                    .toArray();
        }
        throw new TableException("Unknown value fields strategy:" + strategy);
    }

    /**
     * Returns a new table context with a default schema registry subject value in the options if
     * the format is a schema registry format (e.g. 'avro-confluent') and the subject is not
     * defined.
     */
    public static DynamicTableFactory.Context autoCompleteSchemaRegistrySubject(
            DynamicTableFactory.Context context) {
        Map<String, String> tableOptions = context.getCatalogTable().getOptions();
        Map<String, String> newOptions = autoCompleteSchemaRegistrySubject(tableOptions);
        if (newOptions.size() > tableOptions.size()) {
            // build a new context
            return new FactoryUtil.DefaultDynamicTableContext(
                    context.getObjectIdentifier(),
                    context.getCatalogTable().copy(newOptions),
                    context.getConfiguration(),
                    context.getClassLoader(),
                    context.isTemporary());
        } else {
            return context;
        }
    }

    private static Map<String, String> autoCompleteSchemaRegistrySubject(
            Map<String, String> options) {
        Configuration configuration = Configuration.fromMap(options);
        // the subject autoComplete should only be used in sink, check the topic first
        validateSinkTopic(configuration);
        final Optional<String> valueFormat = configuration.getOptional(VALUE_FORMAT);
        final Optional<String> keyFormat = configuration.getOptional(KEY_FORMAT);
        final Optional<String> format = configuration.getOptional(FORMAT);
        final String topic = configuration.get(TOPIC).get(0);

        if (format.isPresent() && SCHEMA_REGISTRY_FORMATS.contains(format.get())) {
            autoCompleteSubject(configuration, format.get(), topic + "-value");
        } else if (valueFormat.isPresent() && SCHEMA_REGISTRY_FORMATS.contains(valueFormat.get())) {
            autoCompleteSubject(configuration, "value." + valueFormat.get(), topic + "-value");
        }

        if (keyFormat.isPresent() && SCHEMA_REGISTRY_FORMATS.contains(keyFormat.get())) {
            autoCompleteSubject(configuration, "key." + keyFormat.get(), topic + "-key");
        }
        return configuration.toMap();
    }

    private static void autoCompleteSubject(
            Configuration configuration, String format, String subject) {
        ConfigOption<String> subjectOption =
                ConfigOptions.key(format + "." + SCHEMA_REGISTRY_SUBJECT.key())
                        .stringType()
                        .noDefaultValue();
        if (!configuration.getOptional(subjectOption).isPresent()) {
            configuration.setString(subjectOption, subject);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Inner classes
    // --------------------------------------------------------------------------------------------

    /** Kafka startup options. * */
    public static class StartupOptions {
        public StartupMode startupMode;
        public Map<KafkaTopicPartition, Long> specificOffsets;
        public long startupTimestampMillis;
    }

    /** Starting or stopping offsets. */
    public enum StartOrStop {
        START,
        STOP
    }

    private KafkaConnectorOptionsUtil() {}
}
