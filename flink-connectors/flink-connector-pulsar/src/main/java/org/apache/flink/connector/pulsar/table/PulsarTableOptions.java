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

package org.apache.flink.connector.pulsar.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.pulsar.config.StartupMode;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions;
import org.apache.flink.streaming.connectors.pulsar.table.descriptors.PulsarValidator;
import org.apache.flink.streaming.connectors.pulsar.util.KeyHashMessageRouterImpl;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.base.CaseFormat;

import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.impl.MessageIdImpl;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.IntStream;

import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.ADMIN_URL_OPTION_KEY;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.PULSAR_OPTION_KEY_PREFIX;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.SERVICE_URL_OPTION_KEY;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarSinkSemantic.AT_LEAST_ONCE;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarSinkSemantic.EXACTLY_ONCE;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarSinkSemantic.NONE;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;

/** Option utils for pulsar table source sink. */
@Slf4j
public class PulsarTableOptions {

    // --------------------------------------------------------------------------------------------
    // Format options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<String> KEY_FORMAT =
            ConfigOptions.key("key." + FORMAT.key())
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines the format identifier for encoding key data. "
                                    + "The identifier is used to discover a suitable format factory.");

    public static final ConfigOption<String> VALUE_FORMAT =
            ConfigOptions.key("value." + FORMAT.key())
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys(FORMAT.key())
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
    // Pulsar specific options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<String> SERVICE_URL =
            ConfigOptions.key(SERVICE_URL_OPTION_KEY)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Required pulsar server connection string");

    public static final ConfigOption<String> ADMIN_URL =
            ConfigOptions.key(ADMIN_URL_OPTION_KEY)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Required pulsar admin connection string");

    public static final ConfigOption<Boolean> GENERIC =
            ConfigOptions.key(PulsarOptions.GENERIC)
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Indicate if the table is a generic flink table");

    // --------------------------------------------------------------------------------------------
    // Scan specific options
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

    public static final ConfigOption<String> SCAN_STARTUP_MODE =
            ConfigOptions.key("scan.startup.mode")
                    .stringType()
                    .defaultValue("latest")
                    .withDescription(
                            "Optional startup mode for Pulsar consumer, valid enumerations are "
                                    + "\"earliest\", \"latest\", \"external-subscription\",\n"
                                    + "or \"specific-offsets\"");

    public static final ConfigOption<String> SCAN_STARTUP_SPECIFIC_OFFSETS =
            ConfigOptions.key("scan.startup.specific-offsets")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional offsets used in case of \"specific-offsets\" startup mode");

    public static final ConfigOption<String> SCAN_STARTUP_SUB_NAME =
            ConfigOptions.key("scan.startup.sub-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional sub-name used in case of \"external-subscription\" startup mode");

    public static final ConfigOption<String> SCAN_STARTUP_SUB_START_OFFSET =
            ConfigOptions.key("scan.startup.sub-start-offset")
                    .stringType()
                    .defaultValue("latest")
                    .withDescription(
                            "Optional sub-start-offset used in case of \"external-subscription\" startup mode");

    public static final ConfigOption<Long> SCAN_STARTUP_TIMESTAMP_MILLIS =
            ConfigOptions.key("scan.startup.timestamp-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional timestamp used in case of \"timestamp\" startup mode");

    public static final ConfigOption<Long> PARTITION_DISCOVERY_INTERVAL_MILLIS =
            ConfigOptions.key("partition.discovery.interval-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional discovery topic interval of \"interval-millis\" millis");

    // --------------------------------------------------------------------------------------------
    // Sink specific options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<String> SINK_MESSAGE_ROUTER =
            ConfigOptions.key("sink.message-router")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional output MessageRouter \n"
                                    + "into pulsar's partitions valid enumerations are\n"
                                    + "\"key-hash\": (each Flink partition ends up in at most one pulsar partition by key'hash, must set key for message),\n"
                                    + "\"round-robin\": (a Flink partition is distributed to pulsar partitions round-robin, it's default messageRouter in pulsar)\n"
                                    + "\"custom class name\": (use a custom MessageRouter subclass)");

    public static final ConfigOption<String> SINK_SEMANTIC =
            ConfigOptions.key("sink.semantic")
                    .stringType()
                    .defaultValue("at-least-once")
                    .withDescription(
                            "Optional semantic when commit. Valid enumerationns are [\"at-least-once\", \"exactly-once\", \"none\"]");

    public static final ConfigOption<Map<String, String>> PROPERTIES =
            ConfigOptions.key("properties")
                    .mapType()
                    .defaultValue(Collections.emptyMap())
                    .withDescription("Optional pulsar config.");

    // --------------------------------------------------------------------------------------------
    // Option enumerations
    // --------------------------------------------------------------------------------------------

    // Start up offset.
    public static final String SCAN_STARTUP_MODE_VALUE_EARLIEST = "earliest";
    public static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest";
    public static final String SCAN_STARTUP_MODE_VALUE_EXTERNAL_SUBSCRIPTION =
            "external-subscription";
    public static final String SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS = "specific-offsets";
    public static final String SCAN_STARTUP_MODE_TIMESTAMP = "timestamp";

    private static final Set<String> SCAN_STARTUP_MODE_ENUMS =
            new HashSet<>(
                    Arrays.asList(
                            SCAN_STARTUP_MODE_VALUE_EARLIEST,
                            SCAN_STARTUP_MODE_VALUE_LATEST,
                            SCAN_STARTUP_MODE_VALUE_EXTERNAL_SUBSCRIPTION,
                            SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS,
                            SCAN_STARTUP_MODE_TIMESTAMP));

    // Sink partitioner.
    public static final String SINK_MESSAGE_ROUTER_VALUE_KEY_HASH = "key-hash";
    public static final String SINK_MESSAGE_ROUTER_VALUE_ROUND_ROBIN = "round-robin";

    private static final Set<String> SINK_MESSAGE_ROUTER_ENUMS =
            new HashSet<>(
                    Arrays.asList(
                            SINK_MESSAGE_ROUTER_VALUE_KEY_HASH,
                            SINK_MESSAGE_ROUTER_VALUE_ROUND_ROBIN));

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

    // Prefix for pulsar specific properties.
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
                                                "[earliest, latest, specific-offsets, external-subscription]",
                                                mode));
                            }

                            if (mode.equals(SCAN_STARTUP_MODE_VALUE_EXTERNAL_SUBSCRIPTION)) {
                                if (!tableOptions.getOptional(SCAN_STARTUP_SUB_NAME).isPresent()) {
                                    throw new ValidationException(
                                            String.format(
                                                    "'%s' is required in '%s' startup mode"
                                                            + " but missing.",
                                                    SCAN_STARTUP_SUB_NAME.key(),
                                                    SCAN_STARTUP_SUB_NAME));
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
                                String specificOffsets =
                                        tableOptions.get(SCAN_STARTUP_SPECIFIC_OFFSETS);
                                parseSpecificOffsets(
                                        specificOffsets, SCAN_STARTUP_SPECIFIC_OFFSETS.key());
                            }
                        });
    }

    public static void validateTableSinkOptions(ReadableConfig tableOptions) {
        validateSinkTopic(tableOptions);
        validateSinkSemantic(tableOptions);
    }

    public static void validateSinkMessageRouter(ReadableConfig tableOptions) {
        tableOptions
                .getOptional(SINK_MESSAGE_ROUTER)
                .ifPresent(
                        router -> {
                            if (!SINK_MESSAGE_ROUTER_ENUMS.contains(router.toLowerCase())) {
                                if (router.isEmpty()) {
                                    throw new ValidationException(
                                            String.format(
                                                    "Option '%s' should be a non-empty string.",
                                                    SINK_MESSAGE_ROUTER.key()));
                                }
                            }
                        });
    }

    public static void validateSinkTopic(ReadableConfig tableOptions) {
        String errorMessageTemp =
                "Flink Pulsar sink currently only supports single topic, but got %s: %s.";
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

    private static boolean isSingleTopic(ReadableConfig tableOptions) {
        // Option 'topic-pattern' is regarded as multi-topics.
        return tableOptions.getOptional(TOPIC).map(t -> t.size() == 1).orElse(false);
    }

    public static PulsarSinkSemantic getSinkSemantic(ReadableConfig tableOptions) {
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

    public static Properties getPulsarProperties(Map<String, String> tableOptions) {
        final Properties pulsarProperties = new Properties();

        if (hasPulsarClientProperties(tableOptions)) {
            tableOptions.keySet().stream()
                    .filter(key -> key.startsWith(PROPERTIES_PREFIX))
                    .forEach(
                            key -> {
                                final String value = tableOptions.get(key);
                                String subKey = key.substring((PROPERTIES_PREFIX).length());
                                if (subKey.startsWith(PULSAR_OPTION_KEY_PREFIX)) {
                                    subKey =
                                            CaseFormat.LOWER_HYPHEN.to(
                                                    CaseFormat.LOWER_CAMEL, subKey);
                                }
                                pulsarProperties.put(subKey, value);
                            });
        }
        pulsarProperties.computeIfAbsent(
                PARTITION_DISCOVERY_INTERVAL_MILLIS.key(), tableOptions::get);
        return pulsarProperties;
    }

    /**
     * The messageRouter can be either "key-hash", "round-robin" or a customized messageRouter
     * subClass full class name.
     */
    public static Optional<MessageRouter> getMessageRouter(
            ReadableConfig tableOptions, ClassLoader classLoader) {
        return tableOptions
                .getOptional(SINK_MESSAGE_ROUTER)
                .flatMap(
                        (String partitioner) -> {
                            switch (partitioner) {
                                case SINK_MESSAGE_ROUTER_VALUE_KEY_HASH:
                                    return Optional.of(KeyHashMessageRouterImpl.INSTANCE);
                                case SINK_MESSAGE_ROUTER_VALUE_ROUND_ROBIN:
                                    return Optional.empty();
                                    // Default fallback to full class name of the messageRouter.
                                default:
                                    return Optional.of(
                                            initializeMessageRouter(partitioner, classLoader));
                            }
                        });
    }

    /** Returns a class value with the given class name. */
    private static MessageRouter initializeMessageRouter(String name, ClassLoader classLoader) {
        try {
            Class<?> clazz = Class.forName(name, true, classLoader);
            if (!MessageRouter.class.isAssignableFrom(clazz)) {
                throw new ValidationException(
                        String.format(
                                "Sink messageRouter class '%s' should extend from the required class %s",
                                name, MessageRouter.class.getName()));
            }
            @SuppressWarnings("unchecked")
            final MessageRouter messageRouter =
                    InstantiationUtil.instantiate(name, MessageRouter.class, classLoader);

            return messageRouter;
        } catch (ClassNotFoundException | FlinkException e) {
            throw new ValidationException(
                    String.format("Could not find and instantiate messageRouter class '%s'", name),
                    e);
        }
    }

    /**
     * Decides if the table options contains Pulsar client properties that start with prefix
     * 'properties'.
     */
    private static boolean hasPulsarClientProperties(Map<String, String> tableOptions) {
        return tableOptions.keySet().stream().anyMatch(k -> k.startsWith(PROPERTIES_PREFIX));
    }

    public static StartupOptions getStartupOptions(ReadableConfig tableOptions) {
        final StartupOptions options = new StartupOptions();
        final Map<String, MessageId> specificOffsets = new HashMap<>();
        Optional<String> modeString = tableOptions.getOptional(SCAN_STARTUP_MODE);
        if (!modeString.isPresent()) {
            options.startupMode = StartupMode.LATEST;
        } else {
            switch (modeString.get()) {
                case PulsarValidator.CONNECTOR_STARTUP_MODE_VALUE_EARLIEST:
                    options.startupMode = StartupMode.EARLIEST;
                    break;

                case PulsarValidator.CONNECTOR_STARTUP_MODE_VALUE_LATEST:
                    options.startupMode = StartupMode.LATEST;
                    break;

                case PulsarValidator.CONNECTOR_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS:
                    String specificOffsetsStrOpt = tableOptions.get(SCAN_STARTUP_SPECIFIC_OFFSETS);

                    final Map<Integer, String> offsetList =
                            parseSpecificOffsets(
                                    specificOffsetsStrOpt, SCAN_STARTUP_SPECIFIC_OFFSETS.key());
                    offsetList.forEach(
                            (partition, offset) -> {
                                try {
                                    final MessageIdImpl messageId = parseMessageId(offset);
                                    specificOffsets.put(partition.toString(), messageId);
                                } catch (Exception e) {
                                    log.error(
                                            "Failed to decode message id from properties {}",
                                            ExceptionUtils.stringifyException(e));
                                    throw new IllegalStateException(e);
                                }
                            });
                    options.startupMode = StartupMode.SPECIFIC_OFFSETS;
                    options.specificOffsets = specificOffsets;
                    break;

                case PulsarValidator.CONNECTOR_STARTUP_MODE_VALUE_EXTERNAL_SUB:
                    options.externalSubscriptionName = tableOptions.get(SCAN_STARTUP_SUB_NAME);
                    options.externalSubStartOffset =
                            tableOptions.get(SCAN_STARTUP_SUB_START_OFFSET);
                    options.startupMode = StartupMode.EXTERNAL_SUBSCRIPTION;
                    break;

                case PulsarValidator.CONNECTOR_STARTUP_MODE_VALUE_TIMESTAMP:
                    options.startupMode = StartupMode.TIMESTAMP;
                    options.startupTimestampMills = tableOptions.get(SCAN_STARTUP_TIMESTAMP_MILLIS);
                    break;

                default:
                    throw new TableException(
                            "Unsupported startup mode. Validator should have checked that.");
            }
        }
        return options;
    }

    private static MessageIdImpl parseMessageId(String offset) {
        final String[] split = offset.split(":");
        return new MessageIdImpl(
                Long.parseLong(split[0]), Long.parseLong(split[1]), Integer.parseInt(split[2]));
    }

    /**
     * Parses SpecificOffsets String to Map.
     *
     * <p>SpecificOffsets String format was given as following:
     *
     * <pre>
     *     scan.startup.specific-offsets = 42:1012:0;44:1011:1
     * </pre>
     *
     * @return SpecificOffsets with Map format, key is partition, and value is offset
     */
    public static Map<Integer, String> parseSpecificOffsets(
            String specificOffsetsStr, String optionKey) {
        final Map<Integer, String> offsetMap = new HashMap<>();
        final String[] pairs = specificOffsetsStr.split(";");
        final String validationExceptionMessage =
                String.format(
                        "Invalid properties '%s' should follow the format "
                                + "messageId with partition'42:1012:0;44:1011:1', but is '%s'.",
                        optionKey, specificOffsetsStr);

        if (pairs.length == 0) {
            throw new ValidationException(validationExceptionMessage);
        }

        for (String pair : pairs) {
            if (null == pair || pair.length() == 0) {
                break;
            }
            if (pair.contains(",")) {
                throw new ValidationException(validationExceptionMessage);
            }

            final String[] kv = pair.split(":");
            if (kv.length != 3) {
                throw new ValidationException(validationExceptionMessage);
            }

            String partitionValue = kv[2];
            String offsetValue = pair;
            try {
                final Integer partition = Integer.valueOf(partitionValue);
                offsetMap.put(partition, offsetValue);
            } catch (NumberFormatException e) {
                throw new ValidationException(validationExceptionMessage, e);
            }
        }
        return offsetMap;
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

    /** pulsar startup options. */
    @EqualsAndHashCode
    public static class StartupOptions {
        public StartupMode startupMode;
        public Map<String, MessageId> specificOffsets = new HashMap<>();
        public String externalSubscriptionName;
        public String externalSubStartOffset;
        public long startupTimestampMills;
    }

    /** Strategies to derive the data type of a value format by considering a key format. */
    public enum ValueFieldsStrategy {
        ALL,
        EXCEPT_KEY
    }

    private PulsarTableOptions() {}
}
