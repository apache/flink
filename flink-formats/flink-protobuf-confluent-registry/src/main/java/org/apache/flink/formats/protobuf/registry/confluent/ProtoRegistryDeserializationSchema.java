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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.protobuf.registry.confluent.utils.MutableByteArrayInputStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.apache.kafka.common.utils.ByteUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Objects;

/**
 * A {@link DeserializationSchema} that deserializes {@link RowData} from Protobuf messages using
 * Schema Registry protocol.
 */
public class ProtoRegistryDeserializationSchema implements DeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final SchemaRegistryConfig schemaRegistryConfig;
    private final RowType rowType;
    private final TypeInformation<RowData> producedType;

    /** Input stream to read message from. */
    private transient MutableByteArrayInputStream inputStream;

    private transient SchemaRegistryCoder schemaCoder;

    private transient ProtoToRowDataConverters.ProtoToRowDataConverter runtimeConverter;

    public Descriptor getMessageDescriptor() {
        return descriptor;
    }

    private transient Descriptor descriptor;

    public ProtoRegistryDeserializationSchema(
            SchemaRegistryConfig schemaRegistryConfig,
            RowType rowType,
            TypeInformation<RowData> producedType) {
        this.schemaRegistryConfig = Preconditions.checkNotNull(schemaRegistryConfig);
        this.rowType = Preconditions.checkNotNull(rowType);
        this.producedType = Preconditions.checkNotNull(producedType);
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        final SchemaRegistryClient schemaRegistryClient = schemaRegistryConfig.createClient();
        this.schemaCoder =
                new SchemaRegistryCoder(schemaRegistryConfig.getSchemaId(), schemaRegistryClient);
        final ProtobufSchema schema =
                (ProtobufSchema)
                        schemaRegistryClient.getSchemaById(schemaRegistryConfig.getSchemaId());
        this.descriptor = schema.toDescriptor();
        this.runtimeConverter = ProtoToRowDataConverters.createConverter(descriptor, rowType);
        this.inputStream = new MutableByteArrayInputStream();
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        if (message == null) {
            return null;
        }
        try {
            inputStream.setBuffer(message);
            schemaCoder.readSchema(inputStream);
            // Not sure what the message indexes are, it is some Confluent Schema Registry Protobuf
            // magic. Until we figure out what that is, let's skip it
            skipMessageIndexes(inputStream);

            final DynamicMessage dynamicMessage = DynamicMessage.parseFrom(descriptor, inputStream);
            return (RowData) runtimeConverter.convert(dynamicMessage);
        } catch (Exception e) {
            throw new IOException("Failed to deserialize P protobuf message.", e);
        }
    }

    public DynamicMessage parseFrom(byte[] message) throws IOException {
        if (message == null) {
            return null;
        }
        try {
            inputStream.setBuffer(message);
            schemaCoder.readSchema(inputStream);
            // Not sure what the message indexes are, it is some Confluent Schema Registry Protobuf
            // magic. Until we figure out what that is, let's skip it
            skipMessageIndexes(inputStream);

            return DynamicMessage.parseFrom(descriptor, inputStream);
        } catch (Exception e) {
            throw new IOException("Failed to deserialize P protobuf message.", e);
        }
    }

    private void skipMessageIndexes(MutableByteArrayInputStream inputStream) throws IOException {
        final DataInputStream dataInputStream = new DataInputStream(inputStream);
        int size = ByteUtils.readVarint(dataInputStream);
        if (size == 0) {
            // optimization
            return;
        }
        for (int i = 0; i < size; i++) {
            ByteUtils.readVarint(dataInputStream);
        }
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ProtoRegistryDeserializationSchema that = (ProtoRegistryDeserializationSchema) o;
        return Objects.equals(schemaRegistryConfig, that.schemaRegistryConfig)
                && Objects.equals(rowType, that.rowType)
                && Objects.equals(producedType, that.producedType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaRegistryConfig, rowType, producedType);
    }
}
