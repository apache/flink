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

package org.apache.flink.formats.parquet.utils;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/** Utilities for emitting and validating GeoParquet metadata. */
public final class GeoParquetMetadataUtil {

    public static final String GEO_METADATA_KEY = "geo";
    public static final String GEOPARQUET_VERSION = "1.1.0";

    private static final String GEOGRAPHY_ENCODING = "WKB";
    private static final String GEOGRAPHY_EDGES = "spherical";
    private static final ObjectMapper OBJECT_MAPPER = JacksonMapperFactory.createObjectMapper();

    private GeoParquetMetadataUtil() {}

    public static Map<String, String> createGeoParquetKeyValueMetaData(RowType rowType) {
        final ObjectNode columnsNode = OBJECT_MAPPER.createObjectNode();
        String primaryColumn = null;

        for (int i = 0; i < rowType.getFieldCount(); i++) {
            final LogicalType fieldType = rowType.getTypeAt(i);
            if (fieldType.getTypeRoot() != LogicalTypeRoot.GEOGRAPHY) {
                continue;
            }

            final String fieldName = rowType.getFieldNames().get(i);
            if (primaryColumn == null) {
                primaryColumn = fieldName;
            }

            final ObjectNode columnNode = columnsNode.putObject(fieldName);
            columnNode.put("encoding", GEOGRAPHY_ENCODING);
            columnNode.putArray("geometry_types");
            columnNode.put("edges", GEOGRAPHY_EDGES);
        }

        if (primaryColumn == null) {
            return Collections.emptyMap();
        }

        final ObjectNode rootNode = OBJECT_MAPPER.createObjectNode();
        rootNode.put("version", GEOPARQUET_VERSION);
        rootNode.put("primary_column", primaryColumn);
        rootNode.set("columns", columnsNode);

        try {
            return Collections.singletonMap(
                    GEO_METADATA_KEY, OBJECT_MAPPER.writeValueAsString(rootNode));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize GeoParquet metadata.", e);
        }
    }

    public static void validateGeoParquetMetadata(
            Map<String, String> keyValueMetaData,
            MessageType requestedSchema,
            LogicalType[] selectedTypes)
            throws IOException {
        if (keyValueMetaData == null || !keyValueMetaData.containsKey(GEO_METADATA_KEY)) {
            return;
        }

        final JsonNode geoNode = OBJECT_MAPPER.readTree(keyValueMetaData.get(GEO_METADATA_KEY));
        final JsonNode columnsNode = geoNode.get("columns");
        if (columnsNode == null || !columnsNode.isObject()) {
            throw new IOException("Invalid GeoParquet metadata: missing 'columns' object.");
        }

        for (int i = 0; i < selectedTypes.length; i++) {
            if (selectedTypes[i].getTypeRoot() != LogicalTypeRoot.GEOGRAPHY) {
                continue;
            }

            final String columnName = requestedSchema.getFields().get(i).getName();
            final JsonNode columnNode = columnsNode.get(columnName);
            if (columnNode == null || !columnNode.isObject()) {
                throw new IOException(
                        String.format(
                                "Invalid GeoParquet metadata: missing GEOGRAPHY column '%s'.",
                                columnName));
            }

            final JsonNode encodingNode = columnNode.get("encoding");
            if (encodingNode == null || !encodingNode.isTextual()) {
                throw new IOException(
                        String.format(
                                "Invalid GeoParquet metadata: missing encoding for GEOGRAPHY column '%s'.",
                                columnName));
            }

            if (!GEOGRAPHY_ENCODING.equals(encodingNode.asText())) {
                throw new IOException(
                        String.format(
                                "Unsupported GeoParquet encoding '%s' for GEOGRAPHY column '%s'.",
                                encodingNode.asText(), columnName));
            }
        }
    }
}
