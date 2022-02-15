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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.AvroRowDataDeserializationSchema;
import org.apache.flink.formats.avro.AvroToRowDataConverters;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.debezium.DebeziumAvroDecodingFormat.ReadableMetadata;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Deserialization schema from Debezium Avro to Flink Table/SQL internal data structure {@link
 * RowData}. The deserialization schema knows Debezium's schema definition and can extract the
 * database data and convert into {@link RowData} with {@link RowKind}. Deserializes a <code>byte[]
 * </code> message as a JSON object and reads the specified fields. Failures during deserialization
 * are forwarded as wrapped IOExceptions.
 *
 * @see <a href="https://debezium.io/">Debezium</a>
 */
@Internal
public final class DebeziumAvroDeserializationSchema implements DeserializationSchema<RowData> {
    private static final long serialVersionUID = 1L;

    /** snapshot read. */
    private static final String OP_READ = "r";
    /** insert operation. */
    private static final String OP_CREATE = "c";
    /** update operation. */
    private static final String OP_UPDATE = "u";
    /** delete operation. */
    private static final String OP_DELETE = "d";

    private static final String REPLICA_IDENTITY_EXCEPTION =
            "The \"before\" field of %s message is null, "
                    + "if you are using Debezium Postgres Connector, "
                    + "please check the Postgres table has been set REPLICA IDENTITY to FULL level.";

    /** The deserializer to deserialize Debezium Avro data. */
    private final AvroRowDataDeserializationSchema avroDeserializer;

    /** TypeInformation of the produced {@link RowData}. */
    private final TypeInformation<RowData> producedTypeInfo;

    /** Flag that indicates that an additional projection is required for metadata. */
    private final boolean hasMetadata;

    /** Metadata to be extracted for every record. */
    private final MetadataConverter[] metadataConverters;

    public DebeziumAvroDeserializationSchema(
            DataType physicalDataType,
            List<ReadableMetadata> requestedMetadata,
            TypeInformation<RowData> producedTypeInfo,
            String schemaRegistryUrl,
            @Nullable Map<String, ?> registryConfigs) {
        this.producedTypeInfo = producedTypeInfo;
        RowType debeziumAvroRowType =
                createDebeziumAvroRowType(physicalDataType, requestedMetadata);

        this.avroDeserializer =
                new AvroRowDataDeserializationSchema(
                        ConfluentRegistryAvroDeserializationSchema.forGeneric(
                                AvroSchemaConverter.convertToSchema(debeziumAvroRowType),
                                schemaRegistryUrl,
                                registryConfigs),
                        AvroToRowDataConverters.createRowConverter(debeziumAvroRowType),
                        producedTypeInfo);

        this.hasMetadata = requestedMetadata.size() > 0;
        this.metadataConverters =
                requestedMetadata.stream()
                        .map(
                                m -> {
                                    final int rootPosition =
                                            debeziumAvroRowType
                                                    .getFieldNames()
                                                    .indexOf(m.requiredAvroField.getName());
                                    return (MetadataConverter)
                                            (row, pos) -> {
                                                Object result = row.getField(rootPosition);
                                                if (result instanceof GenericRowData) {
                                                    result =
                                                            m.converter.convert(
                                                                    (GenericRowData) result, pos);
                                                }
                                                return result;
                                            };
                                })
                        .toArray(MetadataConverter[]::new);
    }

    @VisibleForTesting
    DebeziumAvroDeserializationSchema(
            TypeInformation<RowData> producedTypeInfo,
            AvroRowDataDeserializationSchema avroDeserializer) {
        this.producedTypeInfo = producedTypeInfo;
        this.avroDeserializer = avroDeserializer;
        this.hasMetadata = false;
        this.metadataConverters = new MetadataConverter[0];
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        avroDeserializer.open(context);
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        throw new RuntimeException(
                "Please invoke DeserializationSchema#deserialize(byte[], Collector<RowData>) instead.");
    }

    @Override
    public void deserialize(byte[] message, Collector<RowData> out) throws IOException {

        if (message == null || message.length == 0) {
            // skip tombstone messages
            return;
        }
        try {
            GenericRowData row = (GenericRowData) avroDeserializer.deserialize(message);

            GenericRowData before = (GenericRowData) row.getField(0);
            GenericRowData after = (GenericRowData) row.getField(1);
            String op = row.getField(2).toString();
            if (OP_CREATE.equals(op) || OP_READ.equals(op)) {
                after.setRowKind(RowKind.INSERT);
                emitRow(row, after, out);
            } else if (OP_UPDATE.equals(op)) {
                if (before == null) {
                    throw new IllegalStateException(
                            String.format(REPLICA_IDENTITY_EXCEPTION, "UPDATE"));
                }
                before.setRowKind(RowKind.UPDATE_BEFORE);
                after.setRowKind(RowKind.UPDATE_AFTER);
                emitRow(row, before, out);
                emitRow(row, after, out);
            } else if (OP_DELETE.equals(op)) {
                if (before == null) {
                    throw new IllegalStateException(
                            String.format(REPLICA_IDENTITY_EXCEPTION, "DELETE"));
                }
                before.setRowKind(RowKind.DELETE);
                emitRow(row, before, out);
            } else {
                throw new IOException(
                        format(
                                "Unknown \"op\" value \"%s\". The Debezium Avro message is '%s'",
                                op, new String(message)));
            }
        } catch (Throwable t) {
            // a big try catch to protect the processing.
            throw new IOException("Can't deserialize Debezium Avro message.", t);
        }
    }

    private void emitRow(
            GenericRowData rootRow, GenericRowData physicalRow, Collector<RowData> out) {
        // shortcut in case no output projection is required
        if (!hasMetadata) {
            out.collect(physicalRow);
            return;
        }

        final int physicalArity = physicalRow.getArity();
        final int metadataArity = metadataConverters.length;

        final GenericRowData producedRow =
                new GenericRowData(physicalRow.getRowKind(), physicalArity + metadataArity);

        for (int physicalPos = 0; physicalPos < physicalArity; physicalPos++) {
            producedRow.setField(physicalPos, physicalRow.getField(physicalPos));
        }

        for (int metadataPos = 0; metadataPos < metadataArity; metadataPos++) {
            producedRow.setField(
                    physicalArity + metadataPos,
                    metadataConverters[metadataPos].convert(rootRow, metadataPos));
        }

        out.collect(producedRow);
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DebeziumAvroDeserializationSchema that = (DebeziumAvroDeserializationSchema) o;
        return Objects.equals(avroDeserializer, that.avroDeserializer)
                && Objects.equals(producedTypeInfo, that.producedTypeInfo)
                && hasMetadata == that.hasMetadata;
    }

    @Override
    public int hashCode() {
        return Objects.hash(avroDeserializer, producedTypeInfo, hasMetadata);
    }

    public static RowType createDebeziumAvroRowType(
            DataType databaseSchema, List<ReadableMetadata> readableMetadata) {
        // Debezium Avro contains other information, e.g. "source", "ts_ms"
        DataType payload =
                DataTypes.ROW(
                        DataTypes.FIELD("before", databaseSchema.nullable()),
                        DataTypes.FIELD("after", databaseSchema.nullable()),
                        DataTypes.FIELD("op", DataTypes.STRING()));

        // append fields that are required for reading metadata in the payload
        final List<DataTypes.Field> payloadMetadataFields =
                readableMetadata.stream()
                        .map(m -> m.requiredAvroField)
                        .distinct()
                        .collect(Collectors.toList());
        payload = DataTypeUtils.appendRowFields(payload, payloadMetadataFields);

        return (RowType) payload.getLogicalType();
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Converter that extracts a metadata field from the row payload that comes out of the Avro
     * schema and converts it to the desired data type.
     */
    interface MetadataConverter extends Serializable {
        Object convert(GenericRowData row, int pos);
    }
}
