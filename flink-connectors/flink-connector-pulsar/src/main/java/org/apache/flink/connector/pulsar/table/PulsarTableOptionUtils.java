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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.pulsar.sink.writer.router.TopicRouter;
import org.apache.flink.connector.pulsar.sink.writer.router.TopicRoutingMode;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;

import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageIdImpl;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.IntStream;

import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.KEY_FIELDS;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.KEY_FORMAT;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SINK_CUSTOM_TOPIC_ROUTER;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SINK_MESSAGE_DELAY_INTERVAL;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SINK_TOPIC_ROUTING_MODE;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_START_FROM_MESSAGE_ID;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_START_FROM_PUBLISH_TIME;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_STOP_AFTER_MESSAGE_ID;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_STOP_AT_MESSAGE_ID;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_STOP_AT_PUBLISH_TIME;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_SUBSCRIPTION_TYPE;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.TOPICS;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.VALUE_FORMAT;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A util class for getting fields from config options, getting formats and other useful
 * information.
 *
 * <p>It contains the following functionalities.
 *
 * <ul>
 *   <li>Get Topics from configurations.
 *   <li>Get source StartCursor from configurations.
 *   <li>Get source SubscriptionType from configurations.
 *   <li>Get sink messageDelayMillis from configurations.
 *   <li>Get sink TopicRouter/TopicRoutingMode from configurations.
 *   <li>Create key and value encoding/decoding format.
 *   <li>Create key and value projection.
 * </ul>
 */
public class PulsarTableOptionUtils {

    private PulsarTableOptionUtils() {}

    public static final String TOPIC_LIST_DELIMITER = ";";

    // --------------------------------------------------------------------------------------------
    // Decoding / Encoding and Projection
    // --------------------------------------------------------------------------------------------

    @Nullable
    public static DecodingFormat<DeserializationSchema<RowData>> getKeyDecodingFormat(
            FactoryUtil.TableFactoryHelper helper) {
        return helper.discoverOptionalDecodingFormat(DeserializationFormatFactory.class, KEY_FORMAT)
                .orElse(null);
    }

    @Nullable
    public static EncodingFormat<SerializationSchema<RowData>> getKeyEncodingFormat(
            FactoryUtil.TableFactoryHelper helper) {
        return helper.discoverOptionalEncodingFormat(SerializationFormatFactory.class, KEY_FORMAT)
                .orElse(null);
    }

    public static DecodingFormat<DeserializationSchema<RowData>> getValueDecodingFormat(
            FactoryUtil.TableFactoryHelper helper) {
        return helper.discoverOptionalDecodingFormat(
                        DeserializationFormatFactory.class, FactoryUtil.FORMAT)
                .orElseGet(
                        () ->
                                helper.discoverDecodingFormat(
                                        DeserializationFormatFactory.class, VALUE_FORMAT));
    }

    public static EncodingFormat<SerializationSchema<RowData>> getValueEncodingFormat(
            FactoryUtil.TableFactoryHelper helper) {
        return helper.discoverOptionalEncodingFormat(
                        SerializationFormatFactory.class, FactoryUtil.FORMAT)
                .orElseGet(
                        () ->
                                helper.discoverEncodingFormat(
                                        SerializationFormatFactory.class, VALUE_FORMAT));
    }

    /**
     * Creates an array of indices that determine which physical fields of the table schema to
     * include in the key format and the order that those fields have in the key format.
     */
    public static int[] createKeyFormatProjection(
            ReadableConfig options, DataType physicalDataType) {
        final LogicalType physicalType = physicalDataType.getLogicalType();
        checkArgument(physicalType.is(LogicalTypeRoot.ROW), "Row data type expected.");
        final Optional<String> optionalKeyFormat = options.getOptional(KEY_FORMAT);
        final Optional<List<String>> optionalKeyFields = options.getOptional(KEY_FIELDS);

        if (!optionalKeyFormat.isPresent()) {
            return new int[0];
        }

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
                                                        + "The following columns can be selected in the '%s' option: %s",
                                                keyField, KEY_FIELDS.key(), physicalFields));
                            }
                            // check that field name is prefixed correctly
                            return pos;
                        })
                .toArray();
    }

    public static int[] createValueFormatProjection(
            ReadableConfig options, DataType physicalDataType) {
        final LogicalType physicalType = physicalDataType.getLogicalType();
        checkArgument(physicalType.is(LogicalTypeRoot.ROW), "Row data type expected.");

        final int physicalFieldCount = LogicalTypeChecks.getFieldCount(physicalType);
        final IntStream physicalFields = IntStream.range(0, physicalFieldCount);
        final int[] keyProjection = createKeyFormatProjection(options, physicalDataType);
        return physicalFields
                .filter(pos -> IntStream.of(keyProjection).noneMatch(k -> k == pos))
                .toArray();
    }

    // --------------------------------------------------------------------------------------------
    // Table Source Option Utils
    // --------------------------------------------------------------------------------------------

    public static List<String> getTopicListFromOptions(ReadableConfig tableOptions) {
        List<String> topics = tableOptions.get(TOPICS);
        return topics;
    }

    public static Properties getPulsarProperties(ReadableConfig tableOptions) {
        final Map<String, String> configs = ((Configuration) tableOptions).toMap();
        return getPulsarProperties(configs);
    }

    public static Properties getPulsarProperties(Map<String, String> configs) {
        return getPulsarPropertiesWithPrefix(configs, "pulsar");
    }

    public static Properties getPulsarPropertiesWithPrefix(
            ReadableConfig tableOptions, String prefix) {
        final Map<String, String> configs = ((Configuration) tableOptions).toMap();
        return getPulsarPropertiesWithPrefix(configs, prefix);
    }

    public static Properties getPulsarPropertiesWithPrefix(
            Map<String, String> configs, String prefix) {
        final Properties pulsarProperties = new Properties();
        configs.keySet().stream()
                .filter(key -> key.startsWith(prefix))
                .forEach(key -> pulsarProperties.put(key, configs.get(key)));
        return pulsarProperties;
    }

    public static StartCursor getStartCursor(ReadableConfig tableOptions) {
        if (tableOptions.getOptional(SOURCE_START_FROM_MESSAGE_ID).isPresent()) {
            return parseMessageIdStartCursor(tableOptions.get(SOURCE_START_FROM_MESSAGE_ID));
        } else if (tableOptions.getOptional(SOURCE_START_FROM_PUBLISH_TIME).isPresent()) {
            return parsePublishTimeStartCursor(tableOptions.get(SOURCE_START_FROM_PUBLISH_TIME));
        } else {
            return StartCursor.earliest();
        }
    }

    public static StopCursor getStopCursor(ReadableConfig tableOptions) {
        if (tableOptions.getOptional(SOURCE_STOP_AT_MESSAGE_ID).isPresent()) {
            return parseAtMessageIdStopCursor(tableOptions.get(SOURCE_STOP_AT_MESSAGE_ID));
        } else if (tableOptions.getOptional(SOURCE_STOP_AFTER_MESSAGE_ID).isPresent()) {
            return parseAfterMessageIdStopCursor(tableOptions.get(SOURCE_STOP_AFTER_MESSAGE_ID));
        } else if (tableOptions.getOptional(SOURCE_STOP_AT_PUBLISH_TIME).isPresent()) {
            return parseAtPublishTimeStopCursor(tableOptions.get(SOURCE_STOP_AT_PUBLISH_TIME));
        } else {
            return StopCursor.never();
        }
    }

    public static SubscriptionType getSubscriptionType(ReadableConfig tableOptions) {
        return tableOptions.get(SOURCE_SUBSCRIPTION_TYPE);
    }

    protected static StartCursor parseMessageIdStartCursor(String config) {
        if (Objects.equals(config, "earliest")) {
            return StartCursor.earliest();
        } else if (Objects.equals(config, "latest")) {
            return StartCursor.latest();
        } else {
            return StartCursor.fromMessageId(parseMessageIdString(config));
        }
    }

    protected static StartCursor parsePublishTimeStartCursor(Long config) {
        return StartCursor.fromPublishTime(config);
    }

    protected static StopCursor parseAtMessageIdStopCursor(String config) {
        if (Objects.equals(config, "never")) {
            return StopCursor.never();
        } else if (Objects.equals(config, "latest")) {
            return StopCursor.latest();
        } else {
            return StopCursor.atMessageId(parseMessageIdString(config));
        }
    }

    protected static StopCursor parseAfterMessageIdStopCursor(String config) {
        return StopCursor.afterMessageId(parseMessageIdString(config));
    }

    protected static StopCursor parseAtPublishTimeStopCursor(Long config) {
        return StopCursor.atPublishTime(config);
    }

    protected static MessageIdImpl parseMessageIdString(String config) {
        String[] tokens = config.split(":", 3);
        checkArgument(tokens.length == 3, "MessageId format must be ledgerId:entryId:partitionId.");

        try {
            long ledgerId = Long.parseLong(tokens[0]);
            long entryId = Long.parseLong(tokens[1]);
            int partitionId = Integer.parseInt(tokens[2]);
            MessageIdImpl messageId = new MessageIdImpl(ledgerId, entryId, partitionId);
            return messageId;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "MessageId format must be ledgerId:entryId:partitionId. "
                            + "Each id should be able to parsed to long type.");
        }
    }

    // --------------------------------------------------------------------------------------------
    // Table Sink Option Utils
    // --------------------------------------------------------------------------------------------

    public static TopicRouter<RowData> getTopicRouter(
            ReadableConfig readableConfig, ClassLoader classLoader) {
        if (!readableConfig.getOptional(SINK_CUSTOM_TOPIC_ROUTER).isPresent()) {
            return null;
        }

        String className = readableConfig.get(SINK_CUSTOM_TOPIC_ROUTER);
        try {
            Class<?> clazz = Class.forName(className, true, classLoader);
            if (!TopicRouter.class.isAssignableFrom(clazz)) {
                throw new ValidationException(
                        String.format(
                                "Sink TopicRouter class '%s' should extend from the required class %s",
                                className, TopicRouter.class.getName()));
            }
            @SuppressWarnings("unchecked")
            final TopicRouter<RowData> topicRouter =
                    InstantiationUtil.instantiate(className, TopicRouter.class, classLoader);

            return topicRouter;
        } catch (ClassNotFoundException | FlinkException e) {
            throw new ValidationException(
                    String.format(
                            "Could not find and instantiate TopicRouter class '%s'", className),
                    e);
        }
    }

    public static TopicRoutingMode getTopicRoutingMode(ReadableConfig readableConfig) {
        return readableConfig.get(SINK_TOPIC_ROUTING_MODE);
    }

    public static long getMessageDelayMillis(ReadableConfig readableConfig) {
        return readableConfig.get(SINK_MESSAGE_DELAY_INTERVAL).toMillis();
    }

    // --------------------------------------------------------------------------------------------
    // Table Topic Name Utils
    // --------------------------------------------------------------------------------------------

    public static boolean hasMultipleTopics(String topicsConfigString) {
        checkNotNull(topicsConfigString);
        String[] topics = topicsConfigString.split(TOPIC_LIST_DELIMITER);
        return topics.length > 1;
    }

    public static String getFirstTopic(String topicsConfigString) {
        checkNotNull(topicsConfigString);
        String[] topics = topicsConfigString.split(TOPIC_LIST_DELIMITER);
        checkArgument(topics.length > 0);
        return topics[0];
    }
}
