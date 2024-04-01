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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.protobuf.registry.confluent.ProtoRegistrySerializationSchema;
import org.apache.flink.formats.protobuf.registry.confluent.SchemaCoder;
import org.apache.flink.formats.protobuf.registry.confluent.SchemaRegistryClientFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.util.Objects;

import static java.lang.String.format;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

// TODO - checks for type of message
// TODO - add proto in resources and use those
public class DebeziumProtoRegistrySerializationSchema implements SerializationSchema<RowData> {
    private static final long serialVersionUID = 1L;

    /** insert operation. */
    private static final StringData OP_INSERT = StringData.fromString("c");
    /** delete operation. */
    private static final StringData OP_DELETE = StringData.fromString("d");

    /** RowType to generate the runtime converter. */
    private final RowType rowType;

    /** The converter that converts internal data formats to JsonNode. */
    private ProtoRegistrySerializationSchema protoSerializer;

    private transient GenericRowData outputReuse;

    public DebeziumProtoRegistrySerializationSchema(ReadableConfig formatOptions, RowType rowType) {

        this.rowType = Preconditions.checkNotNull(rowType);
        RowType debeziumProtoRowType = createDebeziumProtoRowType(fromLogicalToDataType(rowType));
        // call validate to check if schema is same
        final SchemaCoder schemaCoder =
                SchemaRegistryClientFactory.getCoder(debeziumProtoRowType, formatOptions);
        this.protoSerializer =
                new ProtoRegistrySerializationSchema(schemaCoder, debeziumProtoRowType);
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

    @Override
    public void open(InitializationContext context) throws Exception {
        protoSerializer.open(context);
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
                    return protoSerializer.serialize(outputReuse);
                case UPDATE_BEFORE:
                case DELETE:
                    outputReuse.setField(0, rowData);
                    outputReuse.setField(1, null);
                    outputReuse.setField(2, OP_DELETE);
                    return protoSerializer.serialize(outputReuse);
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
        if (this == o) return true;
        if (!(o instanceof DebeziumProtoRegistrySerializationSchema)) return false;
        DebeziumProtoRegistrySerializationSchema that =
                (DebeziumProtoRegistrySerializationSchema) o;
        return rowType.equals(that.rowType) && protoSerializer.equals(that.protoSerializer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowType, protoSerializer);
    }
}
