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

package org.apache.flink.formats.protobuf.registry.confluent;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;

import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;

import java.io.ByteArrayOutputStream;
import java.util.Objects;

/**
 * Serialization schema that serializes an object of Flink internal data structure with a Protobuf
 * format.
 *
 * <p>Result <code>byte[]</code> messages can be deserialized using {@link
 * ProtoRegistryDeserializationSchema}.
 */
public class ProtoRegistrySerializationSchema implements SerializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    /** RowType to generate the runtime converter. */
    private final RowType rowType;

    private final SchemaCoder schemaCoder;

    /** The converter that converts internal data formats to JsonNode. */
    private transient RowDataToProtoConverters.RowDataToProtoConverter runtimeConverter;

    /** Output stream to write message to. */
    private transient ByteArrayOutputStream arrayOutputStream;

    public ProtoRegistrySerializationSchema(SchemaCoder schemaCoder, RowType rowType) {
        this.schemaCoder = schemaCoder;
        this.rowType = rowType;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        schemaCoder.initialize();
        ProtobufSchema writerSchema = schemaCoder.writerSchema();
        this.runtimeConverter =
                RowDataToProtoConverters.createConverter(rowType, writerSchema.toDescriptor());
        this.arrayOutputStream = new ByteArrayOutputStream();
    }

    @Override
    public byte[] serialize(RowData row) {
        try {
            final DynamicMessage converted = (DynamicMessage) runtimeConverter.convert(row);
            arrayOutputStream.reset();
            schemaCoder.writeSchema(arrayOutputStream);
            converted.writeTo(arrayOutputStream);
            return arrayOutputStream.toByteArray();
        } catch (Throwable t) {
            throw new FlinkRuntimeException(String.format("Could not serialize row '%s'.", row), t);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ProtoRegistrySerializationSchema)) {
            return false;
        }
        ProtoRegistrySerializationSchema that = (ProtoRegistrySerializationSchema) o;
        return rowType.equals(that.rowType) && schemaCoder.equals(that.schemaCoder);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowType, schemaCoder);
    }
}
