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

package org.apache.flink.formats.protobuf.registry.confluent.debezium;


import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.protobuf.registry.confluent.ProtoRegistrySerializationSchema;
import org.apache.flink.formats.protobuf.registry.confluent.SchemaRegistryCoder;
import org.apache.flink.formats.protobuf.registry.confluent.SchemaRegistryConfig;
import org.apache.flink.formats.protobuf.registry.confluent.utils.ProtoToFlinkSchemaConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.apache.kafka.common.utils.ByteUtils;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;

import static java.lang.String.format;
import static org.apache.flink.formats.protobuf.registry.confluent.debezium.DebeziumProtoRegistryFormatFactory.IDENTIFIER;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

// TODO - checks for type of message
// TODO - add proto in resources and use those
public class DebeziumProtoRegistrySerializationSchema implements SerializationSchema<RowData> {
    private static final long serialVersionUID = 1L;

    /** RowType to generate the runtime converter. */
    private final RowType rowType;
    private final SchemaRegistryConfig schemaRegistryConfig;

    /** The converter that converts internal data formats to JsonNode. */
    private final ProtoRegistrySerializationSchema protoSerializer;
    private final transient RowType debeziumProtoRowType ;


    private transient SchemaRegistryCoder schemaCoder;
    /** Output stream to write message to. */
    private transient ByteArrayOutputStream arrayOutputStream;

    public DebeziumProtoRegistrySerializationSchema(
            SchemaRegistryConfig registryConfig, RowType rowType) {

        this.rowType = Preconditions.checkNotNull(rowType);
        this.schemaRegistryConfig = Preconditions.checkNotNull(registryConfig);
        this.debeziumProtoRowType =  createDebeziumProtoRowType(fromLogicalToDataType(rowType));

        this.protoSerializer = new ProtoRegistrySerializationSchema(registryConfig,debeziumProtoRowType);
    }


    static void validateSchemaWithFlinkRowType(ProtobufSchema schema, RowType rowType) {
        if (schema.toDescriptor() != null) {
            LogicalType convertedDataType =
                    ProtoToFlinkSchemaConverter.toFlinkSchema(schema.toDescriptor());

            if (!convertedDataType.equals(rowType)) {
                throw new IllegalArgumentException(
                        format(
                                "Schema provided for '%s' format must be a nullable record type with fields 'before', 'after', 'op'"
                                        + " and schema of fields 'before' and 'after' must match the table schema: %s",
                                IDENTIFIER, schema));
            }
        }
    }

    private RowType createDebeziumProtoRowType(DataType dataType) {
        // but we don't need them
        return (RowType)
                DataTypes.ROW(
                                DataTypes.FIELD("before", dataType.nullable()),
                                DataTypes.FIELD("after", dataType.nullable()),
                                DataTypes.FIELD("op", DataTypes.STRING()))
                        .getLogicalType();
    }

    private static ByteBuffer writeMessageIndexes() {
        // write empty message indices for now
        ByteBuffer buffer = ByteBuffer.allocate(ByteUtils.sizeOfVarint(0));
        ByteUtils.writeVarint(0, buffer);
        return buffer;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        protoSerializer.open(context);
        Optional<ProtobufSchema> userSchema = protoSerializer.schema();
        ProtobufSchema schema = userSchema.orElseThrow(()->
                new IllegalStateException("Null/Incorrect schema registered"));
          validateSchemaWithFlinkRowType(schema ,rowType);

        //get the registered schema from the underlying protoSerializer

        final SchemaRegistryClient schemaRegistryClient = schemaRegistryConfig.createClient();
        this.schemaCoder =
                new SchemaRegistryCoder(schemaRegistryConfig.getSchemaId(), schemaRegistryClient);
        this.arrayOutputStream = new ByteArrayOutputStream();

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DebeziumProtoRegistrySerializationSchema that =
                (DebeziumProtoRegistrySerializationSchema) o;
        return Objects.equals(rowType, that.rowType)
                && Objects.equals(schemaRegistryConfig, that.schemaRegistryConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowType, schemaRegistryConfig);
    }

    @Override
    public byte[] serialize(RowData row) {
        return protoSerializer.serialize(row);
    }
}
