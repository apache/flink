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

package org.apache.flink.protobuf.registry.confluent.dynamic;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;

import org.apache.flink.formats.protobuf.proto.FlatProto3OuterClass;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class ProtoRegistryDynamicDeserializationSchemaTest {

    private MockSchemaRegistryClient mockSchemaRegistryClient;
    private KafkaProtobufSerializer kafkaProtobufSerializer;
    private ProtoRegistryDynamicDeserializerFormatConfig formatConfig;
    private static final String DUMMY_SCHEMA_REGISTRY_URL = "http://registry:8081";

    @BeforeEach
    public void setup() {
        mockSchemaRegistryClient = new MockSchemaRegistryClient();
        Map<String, String> opts = new HashMap<>();
        opts.put("schema.registry.url", DUMMY_SCHEMA_REGISTRY_URL);
        kafkaProtobufSerializer = new KafkaProtobufSerializer(mockSchemaRegistryClient, opts);
        formatConfig = new ProtoRegistryDynamicDeserializerFormatConfig(DUMMY_SCHEMA_REGISTRY_URL, false, false);
    }

    @AfterEach
    public void teardown() {
        mockSchemaRegistryClient.reset();
        kafkaProtobufSerializer.close();
    }

    @Test
    public void deserializerTest() throws Exception {
        FlatProto3OuterClass.FlatProto3 in = FlatProto3OuterClass.FlatProto3.newBuilder()
                .setString(ProtoCompilerTest.TEST_STRING)
                .setInt(ProtoCompilerTest.TEST_INT)
                .setLong(ProtoCompilerTest.TEST_LONG)
                .setFloat(ProtoCompilerTest.TEST_FLOAT)
                .setDouble(ProtoCompilerTest.TEST_DOUBLE)
                .addInts(ProtoCompilerTest.TEST_INT)
                .setBytes(ProtoCompilerTest.TEST_BYTES)
                .setBool(ProtoCompilerTest.TEST_BOOL)
                .build();

        byte[] inBytes = kafkaProtobufSerializer.serialize("topic", in);

        List<RowType.RowField> fields = new ArrayList<>();
        fields.add(new RowType.RowField("string", new VarCharType()));
        fields.add(new RowType.RowField("int", new IntType()));
        fields.add(new RowType.RowField("long", new BigIntType()));

        RowType rowType = new RowType(fields);

        ProtoRegistryDynamicDeserializationSchema deser = new ProtoRegistryDynamicDeserializationSchema(
                mockSchemaRegistryClient, rowType, null, formatConfig
        );
        deser.open(null);

        RowData actual = deser.deserialize(inBytes);
        Assertions.assertEquals(3, actual.getArity());
        Assertions.assertEquals(ProtoCompilerTest.TEST_STRING, actual.getString(0).toString());
        Assertions.assertEquals(ProtoCompilerTest.TEST_INT, actual.getInt(1));
        Assertions.assertEquals(ProtoCompilerTest.TEST_LONG, actual.getLong(2));
    }

}
