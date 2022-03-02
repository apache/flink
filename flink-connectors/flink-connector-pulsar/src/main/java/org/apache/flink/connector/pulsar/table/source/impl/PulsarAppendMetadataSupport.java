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

package org.apache.flink.connector.pulsar.table.source.impl;

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

// TODO this class can be renamed to PulsarConnectorMetadataSupport
public class PulsarAppendMetadataSupport implements Serializable {

    private static final long serialVersionUID = -4409932324481235973L;
    private final List<String> metadataKeys;
    private final List<MetadataConverter> metadataConverters;

    public PulsarAppendMetadataSupport(List<String> metadataKeys) {
        this.metadataKeys = metadataKeys;
        this.metadataConverters = initializeMetadataConverters();
    }

    // TODO need to take another look at the exceptions
    private List<MetadataConverter> initializeMetadataConverters() {
        return metadataKeys.stream()
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

    public int getMetadataArity() {
        return metadataConverters.size();
    }

    // --------------------------------------------------------------------------------------------
    // Metadata handling
    // --------------------------------------------------------------------------------------------
    interface MetadataConverter extends Serializable {
        Object read(Message<?> message);
    }
    // TODO need to take another look at these fields
    public enum ReadableMetadata {
        TOPIC(
                "topic",
                DataTypes.STRING().notNull(),
                message -> StringData.fromString(message.getTopicName())),

        MESSAGE_ID(
                "messageId",
                DataTypes.BYTES().notNull(),
                message -> message.getMessageId().toByteArray()),

        SEQUENCE_ID("sequenceId", DataTypes.BIGINT().notNull(), Message::getSequenceId),

        PUBLISH_TIME(
                "publishTime",
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull(),
                message -> TimestampData.fromEpochMillis(message.getPublishTime())),

        EVENT_TIME(
                "eventTime",
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
