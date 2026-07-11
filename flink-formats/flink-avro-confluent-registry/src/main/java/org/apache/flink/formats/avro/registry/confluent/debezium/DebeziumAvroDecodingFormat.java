/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro.registry.confluent.debezium;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.registry.confluent.debezium.DebeziumAvroDeserializationSchema.MetadataConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.ProjectableDecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.RowKind;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** {@link DecodingFormat} for Debezium using Avro encoding. */
public class DebeziumAvroDecodingFormat
        implements ProjectableDecodingFormat<DeserializationSchema<RowData>> {

    // ----------------------------------------------------------------------------------------
    // Mutable attributes
    // ----------------------------------------------------------------------------------------

    private List<String> metadataKeys;

    // ----------------------------------------------------------------------------------------
    // Debezium-specific attributes
    // ----------------------------------------------------------------------------------------

    private final String schemaRegistryURL;
    private final String schema;
    private final Map<String, ?> optionalPropertiesMap;

    public DebeziumAvroDecodingFormat(
            String schemaRegistryURL, String schema, Map<String, ?> optionalPropertiesMap) {
        this.schemaRegistryURL = schemaRegistryURL;
        this.schema = schema;
        this.optionalPropertiesMap = optionalPropertiesMap;
        this.metadataKeys = Collections.emptyList();
    }

    @Override
    public DeserializationSchema<RowData> createRuntimeDecoder(
            DynamicTableSource.Context context, DataType physicalDataType, int[][] projections) {
        physicalDataType = Projection.of(projections).project(physicalDataType);

        final List<ReadableMetadata> readableMetadata =
                metadataKeys.stream()
                        .map(
                                k ->
                                        Stream.of(ReadableMetadata.values())
                                                .filter(rm -> rm.key.equals(k))
                                                .findFirst()
                                                .orElseThrow(IllegalStateException::new))
                        .collect(Collectors.toList());
        final List<DataTypes.Field> metadataFields =
                readableMetadata.stream()
                        .map(m -> DataTypes.FIELD(m.key, m.dataType))
                        .collect(Collectors.toList());

        final DataType producedDataType =
                DataTypeUtils.appendRowFields(physicalDataType, metadataFields);
        final TypeInformation<RowData> producedTypeInfo =
                context.createTypeInformation(producedDataType);

        return new DebeziumAvroDeserializationSchema(
                physicalDataType,
                readableMetadata,
                producedTypeInfo,
                schemaRegistryURL,
                schema,
                optionalPropertiesMap);
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        final Map<String, DataType> metadataMap = new LinkedHashMap<>();
        Stream.of(ReadableMetadata.values())
                .forEachOrdered(m -> metadataMap.put(m.key, m.dataType));
        return metadataMap;
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys) {
        this.metadataKeys = metadataKeys;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    // ----------------------------------------------------------------------------------------
    // Metadata handling
    // ----------------------------------------------------------------------------------------

    // StringData keys for reading source metadata from MAP runtime payload.
    private static final StringData KEY_SOURCE_TIMESTAMP = StringData.fromString("ts_ms");
    private static final StringData KEY_SOURCE_DATABASE = StringData.fromString("db");
    private static final StringData KEY_SOURCE_SCHEMA = StringData.fromString("schema");
    private static final StringData KEY_SOURCE_TABLE = StringData.fromString("table");

    // Keep Avro schema generation stable while source values are read from runtime MapData.
    private static final DataTypes.Field REQUIRED_SOURCE_AVRO_FIELD =
            DataTypes.FIELD("source", DataTypes.ROW());

    /** List of metadata that can be read with this format. */
    enum ReadableMetadata {
        INGESTION_TIMESTAMP(
                "ingestion-timestamp",
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).nullable(),
                DataTypes.FIELD("ts_ms", DataTypes.BIGINT()),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(GenericRowData row, int pos) {
                        if (row.isNullAt(pos)) {
                            return null;
                        }
                        return TimestampData.fromEpochMillis(row.getLong(pos));
                    }
                }),

        SOURCE_TIMESTAMP(
                "source.timestamp",
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).nullable(),
                REQUIRED_SOURCE_AVRO_FIELD,
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(GenericRowData row, int pos) {
                        final StringData timestamp =
                                (StringData) readProperty(row, pos, KEY_SOURCE_TIMESTAMP);
                        if (timestamp == null) {
                            return null;
                        }
                        return TimestampData.fromEpochMillis(Long.parseLong(timestamp.toString()));
                    }
                }),

        SOURCE_DATABASE(
                "source.database",
                DataTypes.STRING().nullable(),
                REQUIRED_SOURCE_AVRO_FIELD,
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(GenericRowData row, int pos) {
                        return readProperty(row, pos, KEY_SOURCE_DATABASE);
                    }
                }),

        SOURCE_SCHEMA(
                "source.schema",
                DataTypes.STRING().nullable(),
                REQUIRED_SOURCE_AVRO_FIELD,
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(GenericRowData row, int pos) {
                        return readProperty(row, pos, KEY_SOURCE_SCHEMA);
                    }
                }),

        SOURCE_TABLE(
                "source.table",
                DataTypes.STRING().nullable(),
                REQUIRED_SOURCE_AVRO_FIELD,
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(GenericRowData row, int pos) {
                        return readProperty(row, pos, KEY_SOURCE_TABLE);
                    }
                }),

        SOURCE_PROPERTIES(
                "source.properties",
                // key and value of the map are nullable to make handling easier in queries
                DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.STRING().nullable())
                        .nullable(),
                REQUIRED_SOURCE_AVRO_FIELD,
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(GenericRowData row, int pos) {
                        return row.getMap(pos);
                    }
                });

        final String key;
        final DataType dataType;
        final DataTypes.Field requiredAvroField;
        final MetadataConverter converter;

        ReadableMetadata(
                String key,
                DataType dataType,
                DataTypes.Field requiredAvroField,
                MetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.requiredAvroField = requiredAvroField;
            this.converter = converter;
        }
    }

    /**
     * Reads a property from the source MAP by key.
     *
     * <p>The source field contains GenericMapData injected at runtime. Safe to cast because the
     * runtime type is guaranteed.
     *
     * @param row the rootRow containing source MapData at pos
     * @param pos the configured source field position
     * @param key the StringData key to lookup
     * @return the field value, or null if not found
     */
    private static Object readProperty(GenericRowData row, int pos, StringData key) {
        final GenericMapData map = (GenericMapData) row.getMap(pos);
        if (map == null) {
            return null;
        }
        return map.get(key);
    }
}
