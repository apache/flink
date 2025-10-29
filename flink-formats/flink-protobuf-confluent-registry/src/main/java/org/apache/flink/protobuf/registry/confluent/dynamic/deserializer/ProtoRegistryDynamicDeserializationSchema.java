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

package org.apache.flink.protobuf.registry.confluent.dynamic.deserializer;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.protobuf.PbFormatConfig;
import org.apache.flink.formats.protobuf.deserialize.ProtoToRowConverter;
import org.apache.flink.protobuf.registry.confluent.ProtobufConfluentDeserializationSchema;
import org.apache.flink.protobuf.registry.confluent.SchemaRegistryClientProvider;
import org.apache.flink.protobuf.registry.confluent.dynamic.ProtoCompiler;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Deserialization schema that dynamically deserializes Confluent Protobuf messages using schemas
 * fetched from the schema registry.
 *
 * <p>Use TODO ProtoRegistryStaticDeserializationSchema to deserialize messages with a fixed schema.
 * While we expect that all messages in the input will be compatible with the row schema, it is
 * possible that we will encounter more than one version of the schema in the input. Therefore, for
 * a number of services used by ProtoRegistryDeserializationSchema, we need to maintain a map of
 * schema ID -> service.
 */
public class ProtoRegistryDynamicDeserializationSchema
        implements ProtobufConfluentDeserializationSchema {
    private static final long serialVersionUID = 1L;

    private RowType rowType;
    private final TypeInformation<RowData> resultTypeInfo;

    private final Map<Integer, KafkaProtobufDeserializer> kafkaProtobufDeserializers;
    private final boolean ignoreParseErrors;
    private final boolean readDefaultValues;
    private final String schemaRegistryUrl;
    private final SchemaRegistryClientProvider schemaRegistryClientProvider;
    private final boolean useDefaultProtoIncludes;
    private final List<String> customProtoIncludes;

    private transient SchemaRegistryClient schemaRegistryClient;
    // Since these services operate on dynamically compiled and loaded classes, we need to
    // assume that the new worker don't have the classes loaded yet.
    private transient Map<Integer, ProtoToRowConverter> protoToRowConverters;
    private transient Map<Integer, Class> generatedMessageClasses;
    private transient ProtoCompiler protoCompiler;

    private static final String FAKE_TOPIC = "fake_topic";

    public ProtoRegistryDynamicDeserializationSchema(
            SchemaRegistryClientProvider schemaRegistryClientProvider,
            String schemaRegistryUrl,
            RowType rowType,
            TypeInformation<RowData> resultTypeInfo,
            boolean ignoreParseErrors,
            boolean readDefaultValues,
            boolean useDefaultProtoIncludes,
            List<String> customProtoIncludes) {
        this.rowType = rowType;
        this.resultTypeInfo = resultTypeInfo;
        this.ignoreParseErrors = ignoreParseErrors;
        this.readDefaultValues = readDefaultValues;
        this.schemaRegistryClientProvider = schemaRegistryClientProvider;
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.kafkaProtobufDeserializers = new HashMap<>();
        this.useDefaultProtoIncludes = useDefaultProtoIncludes;
        this.customProtoIncludes = customProtoIncludes;
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) {
            return null;
        }

        int schemaId = getSchemaIdFromMessage(message);
        ProtoToRowConverter protoToRowConverter = getOrCreateProtoConverter(schemaId);
        KafkaProtobufDeserializer kafkaProtobufDeserializer =
                getOrCreateKafkaProtobufDeserializer(schemaId);
        Message protoMessage = kafkaProtobufDeserializer.deserialize(FAKE_TOPIC, message);

        try {
            return protoToRowConverter.convertProtoObjectToRow(protoMessage);
        } catch (Exception e) {
            if (ignoreParseErrors) {
                return null;
            }
            throw new IOException("Failed to deserialize Protobuf message", e);
        }
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return this.resultTypeInfo;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        schemaRegistryClient = schemaRegistryClientProvider.createSchemaRegistryClient();
        protoToRowConverters = new HashMap<>();
        protoCompiler =
                new ProtoCompiler(
                        this.useDefaultProtoIncludes,
                        this.customProtoIncludes.toArray(new String[0]));
        generatedMessageClasses = new HashMap<>();
    }

    @Override
    public void setRowType(RowType rowType) {
        this.rowType = rowType;
    }

    @Override
    public RowType getRowType() {
        return this.rowType;
    }

    private int getSchemaIdFromMessage(byte[] message) {
        InputStream inputStream = new ByteArrayInputStream(message);
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        try {
            if (dataInputStream.readByte() != 0) {
                throw new RuntimeException("Unknown data format. Magic number does not match");
            }
        } catch (IOException e) {
            throw new RuntimeException("Could not read magic number from message", e);
        }

        try {
            return dataInputStream.readInt();
        } catch (IOException e) {
            throw new RuntimeException("Could not read schema ID from message", e);
        }
    }

    private ProtobufSchema getProtobufSchema(int schemaId) {
        try {
            return (ProtobufSchema) schemaRegistryClient.getSchemaById(schemaId);
        } catch (Exception e) {
            throw new RuntimeException("Could not retrieve schema from schema registry", e);
        }
    }

    private KafkaProtobufDeserializer getOrCreateKafkaProtobufDeserializer(int schemaId) {
        if (!kafkaProtobufDeserializers.containsKey(schemaId)) {
            Map<String, String> config = new HashMap<>();
            config.put("schema.registry.url", schemaRegistryUrl);
            Class messageClass = generatedMessageClasses.get(schemaId);
            KafkaProtobufDeserializer deserializer =
                    new KafkaProtobufDeserializer(schemaRegistryClient, config, messageClass);
            kafkaProtobufDeserializers.put(schemaId, deserializer);
        }
        return kafkaProtobufDeserializers.get(schemaId);
    }

    private ProtoToRowConverter getOrCreateProtoConverter(int schemaId) {
        if (protoToRowConverters.containsKey(schemaId)) {
            return protoToRowConverters.get(schemaId);
        }

        ProtobufSchema protobufSchema = getProtobufSchema(schemaId);
        Class messageClass = protoCompiler.generateMessageClass(protobufSchema, schemaId);
        generatedMessageClasses.put(schemaId, messageClass);
        PbFormatConfig pbFormatConfig =
                new PbFormatConfig(
                        messageClass.getName(), ignoreParseErrors, readDefaultValues, null);
        try {
            ProtoToRowConverter protoToRowConverter =
                    new ProtoToRowConverter(rowType, pbFormatConfig);
            protoToRowConverters.put(schemaId, protoToRowConverter);
            return protoToRowConverter;
        } catch (Exception e) {
            throw new RuntimeException("Could not create ProtoToRowConverter", e);
        }
    }
}
