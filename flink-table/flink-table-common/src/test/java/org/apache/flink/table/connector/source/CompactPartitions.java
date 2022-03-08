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

package org.apache.flink.table.connector.source;

import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.factories.DynamicTableFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Test POJO to represent the compaction info for the partition lists to be injected when {@link
 * org.apache.flink.table.factories.TestManagedTableFactory#onCompactTable(DynamicTableFactory.Context,
 * CatalogPartitionSpec)} is called.
 */
@JsonSerialize(using = CompactPartitions.CompactPartitionsSerializer.class)
@JsonDeserialize(using = CompactPartitions.CompactPartitionsDeserializer.class)
public class CompactPartitions implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final String FIELD_NAME_COMPACT_PARTITIONS = "compact-partitions";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final List<CompactPartition> compactPartitions;

    private CompactPartitions(List<CompactPartition> compactPartitions) {
        this.compactPartitions = compactPartitions;
    }

    public static CompactPartitions from(List<CompactPartition> compactPartitions) {
        return new CompactPartitions(compactPartitions);
    }

    public static Optional<CompactPartitions> deserializeCompactPartitions(String json) {
        try {
            return Optional.of(MAPPER.readValue(json, CompactPartitions.class));
        } catch (JsonProcessingException ignored) {
        }
        return Optional.empty();
    }

    public static Optional<String> serializeCompactPartitions(CompactPartitions partitions) {
        try {
            return Optional.of(MAPPER.writeValueAsString(partitions));
        } catch (JsonProcessingException ignored) {
        }
        return Optional.empty();
    }

    public List<CompactPartition> getCompactPartitions() {
        return compactPartitions;
    }

    @Override
    public String toString() {
        return "CompactPartitions{" + "compactPartitions=" + compactPartitions + '}';
    }

    // ~ Inner Classes ------------------------------------------------------------------

    /** The serializer for {@link CompactPartitions}. */
    public static class CompactPartitionsSerializer extends StdSerializer<CompactPartitions> {

        private static final long serialVersionUID = 1L;

        private final CompactPartition.CompactPartitionSerializer serializer =
                new CompactPartition.CompactPartitionSerializer();

        public CompactPartitionsSerializer() {
            super(CompactPartitions.class);
        }

        @Override
        public void serialize(
                CompactPartitions compactPartitions,
                JsonGenerator jsonGenerator,
                SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeFieldName(FIELD_NAME_COMPACT_PARTITIONS);
            jsonGenerator.writeStartArray();
            for (CompactPartition partition : compactPartitions.compactPartitions) {
                serializer.serialize(partition, jsonGenerator, serializerProvider);
            }
            jsonGenerator.writeEndArray();
            jsonGenerator.writeEndObject();
        }
    }

    /** The deserializer for {@link CompactPartitions}. */
    public static class CompactPartitionsDeserializer extends StdDeserializer<CompactPartitions> {

        private static final long serialVersionUID = 6089784742093294800L;

        private final CompactPartition.CompactPartitionDeserializer deserializer =
                new CompactPartition.CompactPartitionDeserializer();

        public CompactPartitionsDeserializer() {
            super(CompactPartitions.class);
        }

        @Override
        public CompactPartitions deserialize(
                JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException {
            List<CompactPartition> partitions = new ArrayList<>();
            JsonNode rootNode = jsonParser.readValueAsTree();
            JsonNode partitionNodes = rootNode.get(FIELD_NAME_COMPACT_PARTITIONS);

            for (JsonNode partitionEntry : partitionNodes) {
                deserializer.setRoot(partitionEntry);
                partitions.add(deserializer.deserialize(jsonParser, deserializationContext));
            }

            return CompactPartitions.from(partitions);
        }
    }
}
