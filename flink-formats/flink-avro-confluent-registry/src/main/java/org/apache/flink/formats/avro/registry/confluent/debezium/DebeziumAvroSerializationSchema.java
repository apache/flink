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
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.avro.AvroRowDataSerializationSchema;
import org.apache.flink.formats.avro.RowDataToAvroConverters;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Objects;

import static java.lang.String.format;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/**
 * Serialization schema from Flink Table/SQL internal data structure {@link RowData} to Debezium
 * Avro.
 */
@Internal
public class DebeziumAvroSerializationSchema implements SerializationSchema<RowData> {
    private static final long serialVersionUID = 1L;

    /** insert operation. */
    private static final StringData OP_INSERT = StringData.fromString("c");
    /** delete operation. */
    private static final StringData OP_DELETE = StringData.fromString("d");

    /** The deserializer to deserialize Debezium Avro data. */
    private final AvroRowDataSerializationSchema avroSerializer;

    private transient GenericRowData outputReuse;

    public DebeziumAvroSerializationSchema(
            RowType rowType,
            String schemaRegistryUrl,
            String schemaRegistrySubject,
            @Nullable Map<String, ?> registryConfigs) {
        RowType debeziumAvroRowType = createDebeziumAvroRowType(fromLogicalToDataType(rowType));

        this.avroSerializer =
                new AvroRowDataSerializationSchema(
                        debeziumAvroRowType,
                        ConfluentRegistryAvroSerializationSchema.forGeneric(
                                schemaRegistrySubject,
                                AvroSchemaConverter.convertToSchema(debeziumAvroRowType),
                                schemaRegistryUrl,
                                registryConfigs),
                        RowDataToAvroConverters.createConverter(debeziumAvroRowType));
    }

    @VisibleForTesting
    DebeziumAvroSerializationSchema(AvroRowDataSerializationSchema avroSerializer) {
        this.avroSerializer = avroSerializer;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        avroSerializer.open(context);
        outputReuse = new GenericRowData(3);
    }

    @Override
    public byte[] serialize(RowData rowData) {
        try {
            switch (rowData.getRowKind()) {
                case INSERT:
                case UPDATE_AFTER:
                    outputReuse.setField(0, null);
                    outputReuse.setField(1, rowData);
                    outputReuse.setField(2, OP_INSERT);
                    return avroSerializer.serialize(outputReuse);
                case UPDATE_BEFORE:
                case DELETE:
                    outputReuse.setField(0, rowData);
                    outputReuse.setField(1, null);
                    outputReuse.setField(2, OP_DELETE);
                    return avroSerializer.serialize(outputReuse);
                default:
                    throw new UnsupportedOperationException(
                            format(
                                    "Unsupported operation '%s' for row kind.",
                                    rowData.getRowKind()));
            }
        } catch (Throwable t) {
            throw new RuntimeException(format("Could not serialize row '%s'.", rowData), t);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DebeziumAvroSerializationSchema that = (DebeziumAvroSerializationSchema) o;
        return Objects.equals(avroSerializer, that.avroSerializer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(avroSerializer);
    }

    public static RowType createDebeziumAvroRowType(DataType dataType) {
        // Debezium Avro contains other information, e.g. "source", "ts_ms"
        // but we don't need them
        return (RowType)
                DataTypes.ROW(
                                DataTypes.FIELD("before", dataType.nullable()),
                                DataTypes.FIELD("after", dataType.nullable()),
                                DataTypes.FIELD("op", DataTypes.STRING()))
                        .getLogicalType();
    }
}
