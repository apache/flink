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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaSinkSemantic.AT_LEAST_ONCE;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaSinkSemantic.EXACTLY_ONCE;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaSinkSemantic.NONE;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT_SUFFIX;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;

/** Option utils for Kafka table source sink. */
public class KafkaOptions {
    private KafkaOptions() {}

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
                            "Defines a strategy how to deal with key columns in the data type of "
                                    + "the value format. By default, '"
                                    + ValueFieldsStrategy.ALL
                                    + "' physical "
                                    + "columns of the table schema will be included in the value format which "
                                    + "means that key columns appear in the data type for both the key and value "
                                    + "format.");

    public static final ConfigOption<String> KEY_FIELDS_PREFIX =
            ConfigOptions.key("key.fields-prefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines a custom prefix for all fields of the key format to avoid "
                                    + "name clashes with fields of the value format. By default, the prefix is empty. "
                                    + "If a custom prefix is defined, both the table schema and "
                                    + "'"
                                    + KEY_FIELDS.key()
                                    + "' will work with prefixed names. When constructing "
                                    + "the data type of the key format, the prefix will be removed and the "
                                    + "non-prefixed names will be used within the key format. Please note that this "
                                    + "option requires that '"
                                    + VALUE_FIELDS_INCLUDE.key()
                                    + "' must be '"
                                    + ValueFieldsStrategy.EXCEPT_KEY
                                    + "'.");

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

    public static final ConfigOption<String> SCAN_STARTUP_MODE =
            ConfigOptions.key("scan.startup.mode")
                    .stringType()
                    .defaultValue("group-offsets")
                    .withDescription(
                            "Optional startup mode for Kafka consumer, valid enumerations are "
                                    + "\"earliest-offset\", \"latest-offset\", \"group-offsets\", \"timestamp\"\n"
                                    + "or \"specific-offsets\"");

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
                            "Optional output partitioning from Flink's partitions\n"
                                    + "into Kafka's partitions valid enumerations are\n"
                                    + "\"default\": (use kafka default partitioner to partition records),\n"
                                    + "\"fixed\": (each Flink partition ends up in at most one Kafka partition),\n"
                                    + "\"round-robin\": (a Flink partition is distributed to Kafka partitions round-robin when 'key.fields' is not specified.)\n"
                                    + "\"custom class name\": (use a custom FlinkKafkaPartitioner subclass)");

    public static final ConfigOption<String> SINK_SEMANTIC =
            ConfigOptions.key("sink.semantic")
                    .stringType()
                    .defaultValue("at-least-once")
                    .withDescription(
                            "Optional semantic when commit. Valid enumerationns are [\"at-least-once\", \"exactly-once\", \"none\"]");

    // --------------------------------------------------------------------------------------------
    // Option enumerations
    // --------------------------------------------------------------------------------------------

    // Start up offset.
    public static final String SCAN_STARTUP_MODE_VALUE_EARLIEST = "earliest-offset";
    public static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";
    public static final String SCAN_STARTUP_MODE_VALUE_GROUP_OFFSETS = "group-offsets";
    public static final String SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS = "specific-offsets";
    public static final String SCAN_STARTUP_MODE_VALUE_TIMESTAMP = "timestamp";

    private static final Set<String> SCAN_STARTUP_MODE_ENUMS =
            new HashSet<>(
                    Arrays.asList(
                            SCAN_STARTUP_MODE_VALUE_EARLIEST,
                            SCAN_STARTUP_MODE_VALUE_LATEST,
                            SCAN_STARTUP_MODE_VALUE_GROUP_OFFSETS,
                            SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS,
                            SCAN_STARTUP_MODE_VALUE_TIMESTAMP));

    // Sink partitioner.
    public static final String SINK_PARTITIONER_VALUE_DEFAULT = "default";
    public static final String SINK_PARTITIONER_VALUE_FIXED = "fixed";
    public static final String SINK_PARTITIONER_VALUE_ROUND_ROBIN = "round-robin";

    // Sink semantic
    public static final String SINK_SEMANTIC_VALUE_EXACTLY_ONCE = "exactly-once";
    public static final String SINK_SEMANTIC_VALUE_AT_LEAST_ONCE = "at-least-once";
    public static final String SINK_SEMANTIC_VALUE_NONE = "none";

    private static final Set<String> SINK_SEMANTIC_ENUMS =
            new HashSet<>(
                    Arrays.asList(
                            SINK_SEMANTIC_VALUE_AT_LEAST_ONCE,
                            SINK_SEMANTIC_VALUE_EXACTLY_ONCE,
                            SINK_SEMANTIC_VALUE_NONE));

    // Prefix for Kafka specific properties.
    public static final String PROPERTIES_PREFIX = "properties.";

    // Other keywords.
    private static final String PARTITION = "partition";
    private static final String OFFSET = "offset";

    // --------------------------------------------------------------------------------------------
    // Validation
    // --------------------------------------------------------------------------------------------

    public static void validateTableSourceOptions(ReadableConfig tableOptions) {
        validateSourceTopic(tableOptions);
        validateScanStartupMode(tableOptions);
    }

    public static void validateTableSinkOptions(ReadableConfig tableOptions) {
        validateSinkTopic(tableOptions);
        validateSinkPartitioner(tableOptions);
        validateSinkSemantic(tableOptions);
    }

    public static void validateSourceTopic(ReadableConfig tableOptions) {
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
            } else {
                throw new ValidationException(
                        String.format(errorMessageTemp, "'topic'", tableOptions.get(TOPIC)));
            }
        }
    }

    private static void validateScanStartupMode(ReadableConfig tableOptions) {
        tableOptions
                .getOptional(SCAN_STARTUP_MODE)
                .map(String::toLowerCase)
                .ifPresent(
                        mode -> {
                            if (!SCAN_STARTUP_MODE_ENUMS.contains(mode)) {
                                throw new ValidationException(
                                        String.format(
                                                "Invalid value for option '%s'. Supported values are %s, but was: %s",
                                                SCAN_STARTUP_MODE.key(),
                                                "[earliest-offset, latest-offset, group-offsets, specific-offsets, timestamp]",
                                                mode));
                            }

                            if (mode.equals(SCAN_STARTUP_MODE_VALUE_TIMESTAMP)) {
                                if (!tableOptions
                                        .getOptional(SCAN_STARTUP_TIMESTAMP_MILLIS)
                                        .isPresent()) {
                                    throw new ValidationException(
                                            String.format(
                                                    "'%s' is required in '%s' startup mode"
                                                            + " but missing.",
                                                    SCAN_STARTUP_TIMESTAMP_MILLIS.key(),
                                                    SCAN_STARTUP_MODE_VALUE_TIMESTAMP));
                                }
                            }
                            if (mode.equals(SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS)) {
                                if (!tableOptions
                                        .getOptional(SCAN_STARTUP_SPECIFIC_OFFSETS)
                                        .isPresent()) {
                                    throw new ValidationException(
                                            String.format(
                                                    "'%s' is required in '%s' startup mode"
                                                            + " but missing.",
                                                    SCAN_STARTUP_SPECIFIC_OFFSETS.key(),
                                                    SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS));
                                }
                                if (!isSingleTopic(tableOptions)) {
                                    throw new ValidationException(
                                            "Currently Kafka source only supports specific offset for single topic.");
                                }
                                String specificOffsets =
                                        tableOptions.get(SCAN_STARTUP_SPECIFIC_OFFSETS);
                                parseSpecificOffsets(
                                        specificOffsets, SCAN_STARTUP_SPECIFIC_OFFSETS.key());
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

    private static void validateSinkSemantic(ReadableConfig tableOptions) {
        tableOptions
                .getOptional(SINK_SEMANTIC)
                .ifPresent(
                        semantic -> {
                            if (!SINK_SEMANTIC_ENUMS.contains(semantic)) {
                                throw new ValidationException(
                                        String.format(
                                                "Unsupported value '%s' for '%s'. Supported values are ['at-least-once', 'exactly-once', 'none'].",
                                                semantic, SINK_SEMANTIC.key()));
                            }
                        });
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    public static KafkaSinkSemantic getSinkSemantic(ReadableConfig tableOptions) {
        switch (tableOptions.get(SINK_SEMANTIC)) {
            case SINK_SEMANTIC_VALUE_EXACTLY_ONCE:
                return EXACTLY_ONCE;
            case SINK_SEMANTIC_VALUE_AT_LEAST_ONCE:
                return AT_LEAST_ONCE;
            case SINK_SEMANTIC_VALUE_NONE:
                return NONE;
            default:
                throw new TableException("Validator should have checked that");
        }
    }

    public static List<String> getSourceTopics(ReadableConfig tableOptions) {
        return tableOptions.getOptional(TOPIC).orElse(null);
    }

    public static Pattern getSourceTopicPattern(ReadableConfig tableOptions) {
        return tableOptions.getOptional(TOPIC_PATTERN).map(Pattern::compile).orElse(null);
    }

    private static boolean isSingleTopic(ReadableConfig tableOptions) {
        // Option 'topic-pattern' is regarded as multi-topics.
        return tableOptions.getOptional(TOPIC).map(t -> t.size() == 1).orElse(false);
    }

    public static StartupOptions getStartupOptions(ReadableConfig tableOptions) {
        final Map<KafkaTopicPartition, Long> specificOffsets = new HashMap<>();
        final StartupMode startupMode =
                tableOptions
                        .getOptional(SCAN_STARTUP_MODE)
                        .map(
                                modeString -> {
                                    switch (modeString) {
                                        case SCAN_STARTUP_MODE_VALUE_EARLIEST:
                                            return StartupMode.EARLIEST;

                                        case SCAN_STARTUP_MODE_VALUE_LATEST:
                                            return StartupMode.LATEST;

                                        case SCAN_STARTUP_MODE_VALUE_GROUP_OFFSETS:
                                            return StartupMode.GROUP_OFFSETS;

                                        case SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS:
                                            // It will be refactored after support specific offset
                                            // for multiple topics in FLINK-18602.
                                            // We have already checked tableOptions.get(TOPIC)
                                            // contains one topic in validateScanStartupMode().
                                            buildSpecificOffsets(
                                                    tableOptions,
                                                    tableOptions.get(TOPIC).get(0),
                                                    specificOffsets);
                                            return StartupMode.SPECIFIC_OFFSETS;

                                        case SCAN_STARTUP_MODE_VALUE_TIMESTAMP:
                                            return StartupMode.TIMESTAMP;

                                        default:
                                            throw new TableException(
                                                    "Unsupported startup mode. Validator should have checked that.");
                                    }
                                })
                        .orElse(StartupMode.GROUP_OFFSETS);
        final StartupOptions options = new StartupOptions();
        options.startupMode = startupMode;
        options.specificOffsets = specificOffsets;
        if (startupMode == StartupMode.TIMESTAMP) {
            options.startupTimestampMillis = tableOptions.get(SCAN_STARTUP_TIMESTAMP_MILLIS);
        }
        return options;
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
     * <p>See {@link #KEY_FORMAT}, {@link #KEY_FIELDS}, and {@link #KEY_FIELDS_PREFIX} for more
     * information.
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
     * <p>See {@link #VALUE_FORMAT}, {@link #VALUE_FIELDS_INCLUDE}, and {@link #KEY_FIELDS_PREFIX}
     * for more information.
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

    // --------------------------------------------------------------------------------------------
    // Inner classes
    // --------------------------------------------------------------------------------------------

    /** Kafka startup options. * */
    public static class StartupOptions {
        public StartupMode startupMode;
        public Map<KafkaTopicPartition, Long> specificOffsets;
        public long startupTimestampMillis;
    }

    /** Strategies to derive the data type of a value format by considering a key format. */
    public enum ValueFieldsStrategy {
        ALL,
        EXCEPT_KEY
    }
}
