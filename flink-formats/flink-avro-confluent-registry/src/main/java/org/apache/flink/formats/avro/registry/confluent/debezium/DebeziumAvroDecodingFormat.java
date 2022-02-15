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
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.RowKind;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** {@link DecodingFormat} for Debezium using Avro encoding. */
public class DebeziumAvroDecodingFormat
        implements ProjectableDecodingFormat<DeserializationSchema<RowData>> {

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    private List<String> metadataKeys;

    // --------------------------------------------------------------------------------------------
    // Debezium-specific attributes
    // --------------------------------------------------------------------------------------------

    private final String schemaRegistryURL;
    private final Map<String, ?> optionalPropertiesMap;

    public DebeziumAvroDecodingFormat(
            String schemaRegistryURL, Map<String, ?> optionalPropertiesMap) {
        this.schemaRegistryURL = schemaRegistryURL;
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

    // --------------------------------------------------------------------------------------------
    // Metadata handling
    // --------------------------------------------------------------------------------------------

    /** List of metadata that can be read with this format. */
    enum ReadableMetadata {
        INGESTION_TIMESTAMP(
                "ingestion-timestamp",
                DataTypes.TIMESTAMP(3).nullable(),
                DataTypes.FIELD("ts_ms", DataTypes.TIMESTAMP(3)),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(GenericRowData row, int unused) {
                        return row;
                    }
                }),

        SOURCE_TIMESTAMP(
                "source.timestamp",
                DataTypes.TIMESTAMP(3).nullable(),
                SOURCE_FIELD,
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(GenericRowData row, int unused) {
                        int pos = SOURCE_PROPERTY_POSITION.get("ts_ms");
                        return row.getField(pos);
                    }
                }),

        SOURCE_DATABASE(
                "source.database",
                DataTypes.STRING().nullable(),
                SOURCE_FIELD,
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(GenericRowData row, int unused) {
                        int pos = SOURCE_PROPERTY_POSITION.get("db");
                        return row.getField(pos);
                    }
                }),

        SOURCE_SCHEMA(
                "source.schema",
                DataTypes.STRING().nullable(),
                SOURCE_FIELD,
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(GenericRowData row, int unused) {
                        int pos = SOURCE_PROPERTY_POSITION.get("schema");
                        return row.getField(pos);
                    }
                }),

        SOURCE_TABLE(
                "source.table",
                DataTypes.STRING().nullable(),
                SOURCE_FIELD,
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(GenericRowData row, int unused) {
                        int pos = SOURCE_PROPERTY_POSITION.get("table");
                        return row.getField(pos);
                    }
                }),

        SOURCE_PROPERTIES(
                "source.properties",
                // key and value of the map are nullable to make handling easier in queries
                DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.STRING().nullable())
                        .nullable(),
                SOURCE_FIELD,
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(GenericRowData row, int unused) {
                        Map<StringData, StringData> result = new HashMap<>();
                        for (int i = 0; i < SOURCE_PROPERTY_FIELDS.length; i++) {
                            Object value = row.getField(i);
                            result.put(
                                    StringData.fromString(SOURCE_PROPERTY_FIELDS[i].getName()),
                                    value == null ? null : StringData.fromString(value.toString()));
                        }
                        return new GenericMapData(result);
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

    private static final DataTypes.Field[] SOURCE_PROPERTY_FIELDS = {
        DataTypes.FIELD("version", DataTypes.STRING()),
        DataTypes.FIELD("connector", DataTypes.STRING()),
        DataTypes.FIELD("name", DataTypes.STRING()),
        DataTypes.FIELD("ts_ms", DataTypes.TIMESTAMP(3)),
        DataTypes.FIELD("snapshot", DataTypes.STRING().nullable()),
        DataTypes.FIELD("db", DataTypes.STRING()),
        DataTypes.FIELD("sequence", DataTypes.STRING().nullable()),
        DataTypes.FIELD("schema", DataTypes.STRING()),
        DataTypes.FIELD("table", DataTypes.STRING()),
        DataTypes.FIELD("txId", DataTypes.STRING().nullable()),
        DataTypes.FIELD("scn", DataTypes.STRING().nullable()),
        DataTypes.FIELD("commit_scn", DataTypes.STRING().nullable()),
        DataTypes.FIELD("lcr_position", DataTypes.STRING().nullable())
    };
    private static final Map<String, Integer> SOURCE_PROPERTY_POSITION =
            IntStream.range(0, SOURCE_PROPERTY_FIELDS.length)
                    .boxed()
                    .collect(Collectors.toMap(i -> SOURCE_PROPERTY_FIELDS[i].getName(), i -> i));
    private static final DataTypes.Field SOURCE_FIELD =
            DataTypes.FIELD("source", DataTypes.ROW(SOURCE_PROPERTY_FIELDS));
}
