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
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * A {@link DeserializationSchema} that deserializes {@link RowData} from Protobuf messages using
 * Schema Registry protocol.
 */
public class ProtoRegistryDeserializationSchema implements DeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final RowType rowType;
    private final TypeInformation<RowData> producedType;

    /** Input stream to read message from. */
    private transient MutableByteArrayInputStream inputStream;

    private transient SchemaCoder schemaCoder;

    private transient Map<String, ProtoToRowDataConverters.ProtoToRowDataConverter>
            runtimeConverterMap;

    public ProtoRegistryDeserializationSchema(
            SchemaCoder schemaCoder, RowType rowType, TypeInformation<RowData> producedType) {
        this.schemaCoder = schemaCoder;
        this.rowType = Preconditions.checkNotNull(rowType);
        this.producedType = Preconditions.checkNotNull(producedType);
        runtimeConverterMap = new ConcurrentHashMap<>();
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        schemaCoder.initialize();
        this.inputStream = new MutableByteArrayInputStream();
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        if (message == null) {
            return null;
        }
        try {
            inputStream.setBuffer(message);
            ProtobufSchema schema = schemaCoder.readSchema(inputStream);
            Descriptor descriptor = schema.toDescriptor();
            final DynamicMessage dynamicMessage = DynamicMessage.parseFrom(descriptor, inputStream);
            ProtoToRowDataConverters.ProtoToRowDataConverter converter =
                    runtimeConverterMap.computeIfAbsent(
                            descriptor.getName(), initializeConverterFn(descriptor));
            return (RowData) converter.convert(dynamicMessage);

        } catch (Exception e) {
            throw new IOException("Failed to deserialize protobuf message.", e);
        }
    }

    private Function<String, ProtoToRowDataConverters.ProtoToRowDataConverter>
            initializeConverterFn(Descriptor descriptor) {
        return str -> initializeConverter(descriptor);
    }

    private ProtoToRowDataConverters.ProtoToRowDataConverter initializeConverter(
            Descriptor descriptor) {
        return ProtoToRowDataConverters.createConverter(descriptor, rowType);
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
        if (!(o instanceof ProtoRegistryDeserializationSchema)) {
            return false;
        }
        ProtoRegistryDeserializationSchema that = (ProtoRegistryDeserializationSchema) o;
        return rowType.equals(that.rowType)
                && producedType.equals(that.producedType)
                && schemaCoder.equals(that.schemaCoder)
                && Objects.equals(runtimeConverterMap, that.runtimeConverterMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowType, producedType, schemaCoder, runtimeConverterMap);
    }
}
