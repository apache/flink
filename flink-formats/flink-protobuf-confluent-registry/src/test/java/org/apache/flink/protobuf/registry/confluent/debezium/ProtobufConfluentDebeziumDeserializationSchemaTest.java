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

import org.apache.flink.protobuf.registry.confluent.SchemaRegistryClientProviders;
import org.apache.flink.protobuf.registry.confluent.TestUtils;
import org.apache.flink.protobuf.registry.confluent.dynamic.deserializer.ProtoRegistryDynamicDeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import my_table.products.DebeziumProto3;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.protobuf.registry.confluent.TestUtils.BOOL_FIELD;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.CUSTOM_PROTO_INCLUDES;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.DUMMY_SCHEMA_REGISTRY_URL;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.FAKE_TOPIC;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.IGNORE_PARSE_ERRORS;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.LONG_FIELD;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.READ_DEFAULT_VALUES;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.STRING_FIELD;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.TEST_BOOL;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.TEST_LONG;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.TEST_STRING;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.USE_DEFAULT_PROTO_INCLUDES;

public class ProtobufConfluentDebeziumDeserializationSchemaTest {

    private MockSchemaRegistryClient mockSchemaRegistryClient;
    private KafkaProtobufSerializer kafkaProtobufSerializer;
    private SimpleCollector collector;
    private ProtobufConfluentDebeziumDeserializationSchema deser;

    @BeforeEach
    public void setup() throws Exception {
        mockSchemaRegistryClient = new MockSchemaRegistryClient();
        Map<String, String> opts = new HashMap<>();
        opts.put("schema.registry.url", DUMMY_SCHEMA_REGISTRY_URL);
        kafkaProtobufSerializer = new KafkaProtobufSerializer(mockSchemaRegistryClient, opts);
        collector = new SimpleCollector();
        RowType rowType =
                TestUtils.createRowType(
                        new RowType.RowField(STRING_FIELD, new VarCharType()),
                        new RowType.RowField(LONG_FIELD, new BigIntType()),
                        new RowType.RowField(BOOL_FIELD, new BooleanType()));
        ProtoRegistryDynamicDeserializationSchema wrappedDeser =
                new ProtoRegistryDynamicDeserializationSchema(
                        new SchemaRegistryClientProviders.MockSchemaRegistryClientProvider(
                                mockSchemaRegistryClient),
                        DUMMY_SCHEMA_REGISTRY_URL,
                        rowType,
                        null,
                        IGNORE_PARSE_ERRORS,
                        READ_DEFAULT_VALUES,
                        USE_DEFAULT_PROTO_INCLUDES,
                        CUSTOM_PROTO_INCLUDES);
        deser = new ProtobufConfluentDebeziumDeserializationSchema(wrappedDeser);
        deser.open(null);
    }

    @Test
    public void deserializeDelete() throws Exception {
        DebeziumProto3.Envelope.Value before =
                DebeziumProto3.Envelope.Value.newBuilder()
                        .setString(TEST_STRING)
                        .setLong(TEST_LONG)
                        .setBool(TEST_BOOL)
                        .build();
        DebeziumProto3.Envelope.Source source =
                DebeziumProto3.Envelope.Source.newBuilder()
                        .setVersion(TEST_STRING)
                        .setConnector(TEST_STRING)
                        .setDb(TEST_STRING)
                        .setTable(TEST_STRING)
                        .setTsMs(TEST_LONG)
                        .setSnapshot(TEST_STRING)
                        .build();
        DebeziumProto3.Envelope envelope =
                DebeziumProto3.Envelope.newBuilder()
                        .setOp(ProtobufConfluentDebeziumDeserializationSchema.OP_DELETE)
                        .setBefore(before)
                        .setSource(source)
                        .setTsMs(TEST_LONG)
                        .build();
        byte[] inBytes = kafkaProtobufSerializer.serialize(FAKE_TOPIC, envelope);

        deser.deserialize(inBytes, collector);
        Assertions.assertEquals(1, collector.list.size());

        RowData actual = collector.list.get(0);

        Assertions.assertEquals(3, actual.getArity());
        Assertions.assertEquals(TEST_STRING, actual.getString(0).toString());
        Assertions.assertEquals(TEST_LONG, actual.getLong(1));
        Assertions.assertEquals(TEST_BOOL, actual.getBoolean(2));
        Assertions.assertEquals(RowKind.DELETE, actual.getRowKind());
    }

    @Test
    public void deserializeUpdate() throws Exception {
        DebeziumProto3.Envelope.Value before =
                DebeziumProto3.Envelope.Value.newBuilder()
                        .setString(TEST_STRING)
                        .setLong(TEST_LONG)
                        .setBool(TEST_BOOL)
                        .build();
        DebeziumProto3.Envelope.Value after =
                DebeziumProto3.Envelope.Value.newBuilder()
                        .setString(TEST_STRING)
                        .setLong((TEST_LONG * 2))
                        .setBool(false)
                        .build();
        DebeziumProto3.Envelope.Source source =
                DebeziumProto3.Envelope.Source.newBuilder()
                        .setVersion(TEST_STRING)
                        .setConnector(TEST_STRING)
                        .setDb(TEST_STRING)
                        .setTable(TEST_STRING)
                        .setTsMs(TEST_LONG)
                        .setSnapshot(TEST_STRING)
                        .build();
        DebeziumProto3.Envelope envelope =
                DebeziumProto3.Envelope.newBuilder()
                        .setOp(ProtobufConfluentDebeziumDeserializationSchema.OP_UPDATE)
                        .setBefore(before)
                        .setAfter(after)
                        .setSource(source)
                        .setTsMs(TEST_LONG)
                        .build();
        byte[] inBytes = kafkaProtobufSerializer.serialize(FAKE_TOPIC, envelope);

        deser.deserialize(inBytes, collector);
        Assertions.assertEquals(2, collector.list.size());

        RowData deleteActual = collector.list.get(0);

        Assertions.assertEquals(3, deleteActual.getArity());
        Assertions.assertEquals(TEST_STRING, deleteActual.getString(0).toString());
        Assertions.assertEquals(TEST_LONG, deleteActual.getLong(1));
        Assertions.assertEquals(TEST_BOOL, deleteActual.getBoolean(2));
        Assertions.assertEquals(RowKind.UPDATE_BEFORE, deleteActual.getRowKind());

        RowData insertActual = collector.list.get(1);

        Assertions.assertEquals(3, insertActual.getArity());
        Assertions.assertEquals(TEST_STRING, insertActual.getString(0).toString());
        Assertions.assertEquals(TEST_LONG * 2, insertActual.getLong(1));
        Assertions.assertFalse(insertActual.getBoolean(2));
        Assertions.assertEquals(RowKind.UPDATE_AFTER, insertActual.getRowKind());
    }

    @Test
    public void deserializeCreate() throws Exception {
        DebeziumProto3.Envelope.Value after =
                DebeziumProto3.Envelope.Value.newBuilder()
                        .setString(TEST_STRING)
                        .setLong(TEST_LONG)
                        .setBool(TEST_BOOL)
                        .build();
        DebeziumProto3.Envelope.Source source =
                DebeziumProto3.Envelope.Source.newBuilder()
                        .setVersion(TEST_STRING)
                        .setConnector(TEST_STRING)
                        .setDb(TEST_STRING)
                        .setTable(TEST_STRING)
                        .setTsMs(TEST_LONG)
                        .setSnapshot(TEST_STRING)
                        .build();
        DebeziumProto3.Envelope envelope =
                DebeziumProto3.Envelope.newBuilder()
                        .setOp(ProtobufConfluentDebeziumDeserializationSchema.OP_CREATE)
                        .setAfter(after)
                        .setSource(source)
                        .setTsMs(TEST_LONG)
                        .build();
        byte[] inBytes = kafkaProtobufSerializer.serialize(FAKE_TOPIC, envelope);

        deser.deserialize(inBytes, collector);
        Assertions.assertEquals(1, collector.list.size());

        RowData actual = collector.list.get(0);

        Assertions.assertEquals(3, actual.getArity());
        Assertions.assertEquals(TEST_STRING, actual.getString(0).toString());
        Assertions.assertEquals(TEST_LONG, actual.getLong(1));
        Assertions.assertEquals(TEST_BOOL, actual.getBoolean(2));
        Assertions.assertEquals(RowKind.INSERT, actual.getRowKind());
    }

    private static class SimpleCollector implements Collector<RowData> {

        public final List<RowData> list = new ArrayList<>();

        @Override
        public void collect(RowData record) {
            list.add(record);
        }

        @Override
        public void close() {}
    }
}
