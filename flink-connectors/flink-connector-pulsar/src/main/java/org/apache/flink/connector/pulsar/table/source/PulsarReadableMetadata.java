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

package org.apache.flink.connector.pulsar.table.source;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;

import org.apache.pulsar.client.api.Message;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Class for reading metadata fields from a Pulsar message and put in corresponding Flink row
 * fields.
 *
 * <p>Contains list of readable metadata and provide util methods for metadata manipulation.
 */
public class PulsarReadableMetadata implements Serializable {

    private static final long serialVersionUID = -4409932324481235973L;

    private final List<String> connectorMetadataKeys;

    private final List<MetadataConverter> metadataConverters;

    public PulsarReadableMetadata(List<String> connectorMetadataKeys) {
        this.connectorMetadataKeys = connectorMetadataKeys;
        this.metadataConverters = initializeMetadataConverters();
    }

    private List<MetadataConverter> initializeMetadataConverters() {
        return connectorMetadataKeys.stream()
                .map(
                        k ->
                                Stream.of(ReadableMetadata.values())
                                        .filter(rm -> rm.key.equals(k))
                                        .findFirst()
                                        .orElseThrow(IllegalStateException::new))
                .map(m -> m.converter)
                .collect(Collectors.toList());
    }

    public void appendProducedRowWithMetadata(
            GenericRowData producedRowData, int physicalArity, Message<?> message) {
        for (int metadataPos = 0; metadataPos < metadataConverters.size(); metadataPos++) {
            producedRowData.setField(
                    physicalArity + metadataPos, metadataConverters.get(metadataPos).read(message));
        }
    }

    public int getConnectorMetadataArity() {
        return metadataConverters.size();
    }

    interface MetadataConverter extends Serializable {
        Object read(Message<?> message);
    }

    /** Lists the metadata that is readable from a Pulsar message. Used in SQL source connector. */
    public enum ReadableMetadata {
        TOPIC(
                "topic",
                DataTypes.STRING().notNull(),
                message -> StringData.fromString(message.getTopicName())),

        MESSAGE_SIZE("message_size", DataTypes.INT().notNull(), message -> message.size()),

        PRODUCER_NAME(
                "producer_name",
                DataTypes.STRING().notNull(),
                message -> StringData.fromString(message.getProducerName())),

        MESSAGE_ID(
                "message_id",
                DataTypes.BYTES().notNull(),
                message -> message.getMessageId().toByteArray()),

        SEQUENCE_ID("sequenceId", DataTypes.BIGINT().notNull(), Message::getSequenceId),

        PUBLISH_TIME(
                "publish_time",
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull(),
                message -> TimestampData.fromEpochMillis(message.getPublishTime())),

        EVENT_TIME(
                "event_time",
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull(),
                message -> TimestampData.fromEpochMillis(message.getEventTime())),

        PROPERTIES(
                "properties",
                // key and value of the map are nullable to make handling easier in queries
                DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.STRING().nullable())
                        .notNull(),
                message -> {
                    final Map<StringData, StringData> map = new HashMap<>();
                    for (Map.Entry<String, String> e : message.getProperties().entrySet()) {
                        map.put(
                                StringData.fromString(e.getKey()),
                                StringData.fromString(e.getValue()));
                    }
                    return new GenericMapData(map);
                });

        public final String key;

        public final DataType dataType;

        public final MetadataConverter converter;

        ReadableMetadata(String key, DataType dataType, MetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.converter = converter;
        }
    }
}
