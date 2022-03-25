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
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
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
import org.apache.flink.util.Preconditions;

import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageIdImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.IntStream;

import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.KEY_FIELDS;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.KEY_FORMAT;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_SOURCE_SUBSCRIPTION_TYPE;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_START_FROM_MESSAGE_ID;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_START_FROM_PUBLISH_TIME;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.TOPIC;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT;

/**
 * A util class for getting fields from config options, getting formats and other useful
 * information.
 *
 * <p>TODO add more description
 */
public class PulsarTableOptionUtils {

    public static List<String> getTopicListFromOptions(ReadableConfig tableOptions) {
        List<String> topics = tableOptions.getOptional(TOPIC).orElse(new ArrayList<>());
        return topics;
    }

    // --------------------------------------------------------------------------------------------
    // Validation
    // --------------------------------------------------------------------------------------------

    // TODO will the cast lose context ? Enum and Map type ?
    // TODO by default it only supports string ConfigOptions
    public static Properties getPulsarProperties(ReadableConfig tableOptions) {
        final Properties pulsarProperties = new Properties();
        final Map<String, String> configs = ((Configuration) tableOptions).toMap();
        configs.keySet().stream()
                .filter(key -> key.startsWith("pulsar"))
                .forEach(key -> pulsarProperties.put(key, configs.get(key)));
        return pulsarProperties;
    }

    public static Optional<StartCursor> getStartCursor(ReadableConfig tableOptions) {
        if (tableOptions.getOptional(SOURCE_START_FROM_MESSAGE_ID).isPresent()) {
            return Optional.of(
                    parseMessageIdStartCursor(tableOptions.get(SOURCE_START_FROM_MESSAGE_ID)));
        } else if (tableOptions.getOptional(SOURCE_START_FROM_PUBLISH_TIME).isPresent()) {
            return Optional.of(
                    parsePublishTimeStartCursor(tableOptions.get(SOURCE_START_FROM_PUBLISH_TIME)));
        } else {
            return Optional.empty();
        }
    }

    // TODO this can be simplified, and only 2 subcsription type is allowed
    public static Optional<SubscriptionType> getSubscriptionType(ReadableConfig tableOptions) {
        if (tableOptions.getOptional(SOURCE_SOURCE_SUBSCRIPTION_TYPE).isPresent()) {
            return Optional.of(tableOptions.get(SOURCE_SOURCE_SUBSCRIPTION_TYPE));
        } else {
            return Optional.empty();
        }
    }

    public static DecodingFormat<DeserializationSchema<RowData>> getKeyDecodingFormat(
            FactoryUtil.TableFactoryHelper helper) {
        return helper.discoverOptionalDecodingFormat(DeserializationFormatFactory.class, KEY_FORMAT)
                .orElse(null);
    }

    public static EncodingFormat<SerializationSchema<RowData>> getKeyEncodingFormat(
            FactoryUtil.TableFactoryHelper helper) {
        return helper.discoverOptionalEncodingFormat(SerializationFormatFactory.class, KEY_FORMAT)
                .orElse(null);
    }

    public static DecodingFormat<DeserializationSchema<RowData>> getValueDecodingFormat(
            FactoryUtil.TableFactoryHelper helper) {
        return helper.discoverOptionalDecodingFormat(DeserializationFormatFactory.class, FORMAT)
                .get();
    }

    public static EncodingFormat<SerializationSchema<RowData>> getValueEncodingFormat(
            FactoryUtil.TableFactoryHelper helper) {
        return helper.discoverOptionalEncodingFormat(SerializationFormatFactory.class, FORMAT)
                .get();
    }

    /**
     * Creates an array of indices that determine which physical fields of the table schema to
     * include in the key format and the order that those fields have in the key format.
     */
    public static int[] createKeyFormatProjection(
            ReadableConfig options, DataType physicalDataType) {
        final LogicalType physicalType = physicalDataType.getLogicalType();
        Preconditions.checkArgument(
                physicalType.is(LogicalTypeRoot.ROW), "Row data type expected.");
        final Optional<String> optionalKeyFormat = options.getOptional(KEY_FORMAT);
        final Optional<List<String>> optionalKeyFields = options.getOptional(KEY_FIELDS);

        // TODO validations here

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
                                // TODO find a pattern to create Validation Exceptions
                                throw new ValidationException(
                                        String.format(
                                                "Could not find the field '%s' in the table schema for usage in the key format. "
                                                        + "A key field must be a regular, physical column. "
                                                        + "The following columns can be selected in the '%s' option:\n"
                                                        + "%s",
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
        //        Preconditions.checkArgument(
        //                physicalType.is(LogicalTypeRoot.ROW), "Row data type expected.");
        final int physicalFieldCount = LogicalTypeChecks.getFieldCount(physicalType);
        final IntStream physicalFields = IntStream.range(0, physicalFieldCount);
        final int[] keyProjection = createKeyFormatProjection(options, physicalDataType);
        return physicalFields
                .filter(pos -> IntStream.of(keyProjection).noneMatch(k -> k == pos))
                .toArray();
    }

    private static StartCursor parseMessageIdStartCursor(String config) {
        if (Objects.equals(config, "earliest")) {
            return StartCursor.earliest();
        } else if (Objects.equals(config, "latest")) {
            return StartCursor.latest();
        } else {
            return parseMessageIdString(config);
        }
    }

    // TODO how to parse the messageId?
    private static StartCursor parseMessageIdString(String config) {
        String[] tokens = config.split(":", 3);
        if (tokens.length != 3) {
            throw new IllegalArgumentException(
                    "MessageId format must be ledgerId:entryId:partitionId .");
        }
        Long ledgerId = Long.parseLong(tokens[0]);
        Long entryId = Long.parseLong(tokens[1]);
        Integer partitionId = Integer.parseInt(tokens[2]);

        MessageIdImpl messageId = new MessageIdImpl(ledgerId, entryId, partitionId);
        return StartCursor.fromMessageId(messageId);
    }

    private static StartCursor parsePublishTimeStartCursor(Long config) {
        return StartCursor.fromMessageTime(config);
    }
}
