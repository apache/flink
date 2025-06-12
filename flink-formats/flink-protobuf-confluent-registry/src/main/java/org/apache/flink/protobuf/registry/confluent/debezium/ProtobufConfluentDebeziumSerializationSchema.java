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

package org.apache.flink.protobuf.registry.confluent.debezium;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.protobuf.registry.confluent.ProtobufConfluentSerializationSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import static java.lang.String.format;

/**
 * Serialization schema from Flink Table/SQL internal data structure {@link RowData} to Debezium
 * Protobuf.
 */
public class ProtobufConfluentDebeziumSerializationSchema implements SerializationSchema<RowData> {
    private static final long serialVersionUID = 1L;

    /** insert operation. */
    private static final StringData OP_INSERT = StringData.fromString("c");
    /** delete operation. */
    private static final StringData OP_DELETE = StringData.fromString("d");

    private final ProtobufConfluentSerializationSchema protobufConfluentSerializationSchema;

    private transient GenericRowData outputReuse;

    public ProtobufConfluentDebeziumSerializationSchema(
            ProtobufConfluentSerializationSchema protobufConfluentSerializationSchema) {
        this.protobufConfluentSerializationSchema = protobufConfluentSerializationSchema;
        ProtobufConfluentDebeziumDeserializationSchema.updateRowType(
                protobufConfluentSerializationSchema);
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        protobufConfluentSerializationSchema.open(context);
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
                    return protobufConfluentSerializationSchema.serialize(outputReuse);
                case UPDATE_BEFORE:
                case DELETE:
                    outputReuse.setField(0, rowData);
                    outputReuse.setField(1, null);
                    outputReuse.setField(2, OP_DELETE);
                    return protobufConfluentSerializationSchema.serialize(outputReuse);
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
}
