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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.protobuf.registry.confluent.ProtoRegistryDeserializationSchema;
import org.apache.flink.formats.protobuf.registry.confluent.SchemaCoder;
import org.apache.flink.formats.protobuf.registry.confluent.SchemaRegistryClientFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Objects;

import static java.lang.String.format;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/**
 * Deserialization schema from Debezium protobuf encoded message to Flink Table/SQL internal {@link
 * RowData}. The deserialization schema knows Debezium's schema definition and can extract the
 * database data within the debezium envelop into a {@link RowData} with a {@link RowKind}.
 *
 * <p>Deserializes a <code>byte[]</code> message. Delegates the actual deserialization to {@link
 * org.apache.flink.formats.protobuf.registry.confluent.ProtoRegistryDeserializationSchema } after
 * setting up the appropriate {@link org.apache.flink.table.types.logical.RowType} corresponding to
 * a debezium envelop. Failures during deserialization are forwarded as wrapped IOExceptions.
 *
 * @see <a href="https://debezium.io/">Debezium</a>
 */
public class DebeziumProtoRegistryDeserializationSchema implements DeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;
    private static final String OP_READ = "r";
    // ** insert operation.
    private static final String OP_CREATE = "c";
    // ** update operation.
    private static final String OP_UPDATE = "u";
    // ** delete operation.
    private static final String OP_DELETE = "d";

    private final ProtoRegistryDeserializationSchema protoDeserializer;
    private final TypeInformation<RowData> producedTypeInfo;

    public DebeziumProtoRegistryDeserializationSchema(
            ReadableConfig formatOptions,
            RowType rowType,
            TypeInformation<RowData> producedTypeInfo) {
        this.producedTypeInfo = producedTypeInfo;
        RowType debeziumProtoRowType = createDebeziumProtoRowType(fromLogicalToDataType(rowType));
        final SchemaCoder schemaCoder =
                SchemaRegistryClientFactory.getCoder(debeziumProtoRowType, formatOptions);

        protoDeserializer =
                new ProtoRegistryDeserializationSchema(
                        schemaCoder, debeziumProtoRowType, producedTypeInfo);
    }

    @VisibleForTesting
    DebeziumProtoRegistryDeserializationSchema(
            SchemaCoder coder, RowType rowType, TypeInformation<RowData> producedTypeInfo) {
        this.producedTypeInfo = producedTypeInfo;
        RowType debeziumProtoRowType = createDebeziumProtoRowType(fromLogicalToDataType(rowType));
        protoDeserializer =
                new ProtoRegistryDeserializationSchema(
                        coder, debeziumProtoRowType, producedTypeInfo);
    }

    private RowType createDebeziumProtoRowType(DataType databaseSchema) {
        // Debezium proto might contain other information, e.g. "source", "ts_ms"
        // but we don't need them
        return (RowType)
                DataTypes.ROW(
                                DataTypes.FIELD("before", databaseSchema.nullable()),
                                DataTypes.FIELD("after", databaseSchema.nullable()),
                                DataTypes.FIELD("op", DataTypes.STRING()))
                        .getLogicalType();
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        protoDeserializer.open(context);
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        throw new RuntimeException(
                "Please invoke DeserializationSchema#deserialize(byte[], Collector<RowData>) instead.");
    }

    @Override
    public void deserialize(byte[] message, Collector<RowData> out) throws IOException {

        GenericRowData debeziumEnvelop = (GenericRowData) protoDeserializer.deserialize(message);
        RowData before = (RowData) debeziumEnvelop.getField(0);
        RowData after = (RowData) debeziumEnvelop.getField(1);
        String op = debeziumEnvelop.getField(2).toString();

        if (OP_CREATE.equals(op) || OP_READ.equals(op)) {
            after.setRowKind(RowKind.INSERT);
            out.collect(after);
        } else if (OP_UPDATE.equals(op)) {
            before.setRowKind(RowKind.UPDATE_BEFORE);
            after.setRowKind(RowKind.UPDATE_AFTER);
            out.collect(before);
            out.collect(after);
        } else if (OP_DELETE.equals(op)) {
            before.setRowKind(RowKind.DELETE);
            out.collect(before);
        } else {
            throw new IOException(
                    format(
                            "Unknown \"op\" value \"%s\". The Debezium proto message is '%s'",
                            op, new String(message)));
        }
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
        if (!(o instanceof DebeziumProtoRegistryDeserializationSchema)) {
            return false;
        }
        DebeziumProtoRegistryDeserializationSchema that =
                (DebeziumProtoRegistryDeserializationSchema) o;
        return protoDeserializer.equals(that.protoDeserializer)
                && producedTypeInfo.equals(that.producedTypeInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(protoDeserializer, producedTypeInfo);
    }
}
