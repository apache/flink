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

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.factories.DynamicTableFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Test POJO to represent the compaction info for a single partition to be injected when {@link
 * org.apache.flink.table.factories.TestManagedTableFactory#onCompactTable(DynamicTableFactory.Context,
 * CatalogPartitionSpec)} is called.
 */
@JsonSerialize(using = CompactPartition.CompactPartitionSerializer.class)
@JsonDeserialize(using = CompactPartition.CompactPartitionDeserializer.class)
public class CompactPartition implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final String FIELD_NAME_RESOLVED_PARTITION_SPEC = "resolved-partition-spec";
    private static final String FIELD_NAME_FILE_ENTRIES = "file-entries";

    private final LinkedHashMap<String, String> resolvedPartitionSpec;
    private final List<String> fileEntries;

    private CompactPartition(
            LinkedHashMap<String, String> partitionSpec, List<String> fileEntries) {
        this.resolvedPartitionSpec = partitionSpec;
        this.fileEntries = fileEntries;
    }

    public LinkedHashMap<String, String> getResolvedPartitionSpec() {
        return resolvedPartitionSpec;
    }

    public List<String> getFileEntries() {
        return fileEntries;
    }

    public static CompactPartition of(CatalogPartitionSpec partitionSpec, List<Path> paths) {
        return new CompactPartition(
                new LinkedHashMap<>(partitionSpec.getPartitionSpec()),
                paths.stream().map(Path::getPath).collect(Collectors.toList()));
    }

    public static CompactPartition of(
            LinkedHashMap<String, String> partitionSpec, List<String> paths) {
        return new CompactPartition(partitionSpec, paths);
    }

    @Override
    public String toString() {
        return "CompactPartition{"
                + "resolvedPartitionSpec="
                + resolvedPartitionSpec
                + ", fileEntries="
                + fileEntries
                + '}';
    }

    // ~ Inner Classes ------------------------------------------------------------------

    /** The serializer for {@link CompactPartition}. */
    public static class CompactPartitionSerializer extends StdSerializer<CompactPartition> {

        private static final long serialVersionUID = 1L;

        public CompactPartitionSerializer() {
            super(CompactPartition.class);
        }

        @Override
        public void serialize(
                CompactPartition compactPartition,
                JsonGenerator jsonGenerator,
                SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeObjectFieldStart(FIELD_NAME_RESOLVED_PARTITION_SPEC);
            for (Map.Entry<String, String> entry :
                    compactPartition.getResolvedPartitionSpec().entrySet()) {
                jsonGenerator.writeStringField(entry.getKey(), entry.getValue());
            }
            jsonGenerator.writeEndObject();
            jsonGenerator.writeFieldName(FIELD_NAME_FILE_ENTRIES);
            jsonGenerator.writeStartArray();
            for (String fileEntry : compactPartition.getFileEntries()) {
                jsonGenerator.writeString(fileEntry);
            }
            jsonGenerator.writeEndArray();
            jsonGenerator.writeEndObject();
        }
    }

    /** The deserializer for {@link CompactPartition}. */
    public static class CompactPartitionDeserializer extends StdDeserializer<CompactPartition> {

        private static final long serialVersionUID = 1L;

        private JsonNode root;

        public CompactPartitionDeserializer() {
            super(CompactPartition.class);
        }

        @Override
        public CompactPartition deserialize(
                JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException {
            JsonNode rootNode =
                    root == null ? (JsonNode) jsonParser.readValueAsTree().get(0) : root;

            JsonNode partitionSpecNode = rootNode.get(FIELD_NAME_RESOLVED_PARTITION_SPEC);
            LinkedHashMap<String, String> resolvedPartitionSpec = new LinkedHashMap<>();
            List<String> fileEntries = new ArrayList<>();
            Iterator<Map.Entry<String, JsonNode>> mapIterator = partitionSpecNode.fields();
            while (mapIterator.hasNext()) {
                Map.Entry<String, JsonNode> entry = mapIterator.next();
                resolvedPartitionSpec.put(entry.getKey(), entry.getValue().textValue());
            }
            JsonNode fileEntriesNode = rootNode.get(FIELD_NAME_FILE_ENTRIES);
            for (JsonNode fileEntry : fileEntriesNode) {
                fileEntries.add(fileEntry.asText());
            }
            return CompactPartition.of(resolvedPartitionSpec, fileEntries);
        }

        public void setRoot(final JsonNode root) {
            this.root = root;
        }
    }
}
