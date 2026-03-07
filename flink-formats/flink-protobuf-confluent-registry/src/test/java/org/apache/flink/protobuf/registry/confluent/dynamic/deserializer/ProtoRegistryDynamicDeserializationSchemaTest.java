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

import org.apache.flink.formats.protobuf.proto.AddressAndUser;
import org.apache.flink.formats.protobuf.proto.ConfluentDebeziumProto3;
import org.apache.flink.formats.protobuf.proto.FlatProto3OuterClass;
import org.apache.flink.formats.protobuf.proto.MapProto3;
import org.apache.flink.formats.protobuf.proto.NestedProto3OuterClass;
import org.apache.flink.formats.protobuf.proto.TimestampProto3OuterClass;
import org.apache.flink.protobuf.registry.confluent.SchemaRegistryClientProviders;
import org.apache.flink.protobuf.registry.confluent.TestUtils;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.protobuf.type.Decimal;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.protobuf.registry.confluent.TestUtils.CUSTOM_PROTO_INCLUDES;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.DUMMY_SCHEMA_REGISTRY_URL;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.FAKE_TOPIC;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.IGNORE_PARSE_ERRORS;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.READ_DEFAULT_VALUES;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.TEST_BYTES;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.TEST_INT;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.USE_DEFAULT_PROTO_INCLUDES;

class ProtoRegistryDynamicDeserializationSchemaTest {

    private MockSchemaRegistryClient mockSchemaRegistryClient;
    private KafkaProtobufSerializer kafkaProtobufSerializer;
    private SchemaRegistryClientProviders.MockSchemaRegistryClientProvider
            mockSchemaRegistryClientProvider;

    @BeforeEach
    public void setup() {
        mockSchemaRegistryClient = new MockSchemaRegistryClient();
        Map<String, String> opts = new HashMap<>();
        opts.put("schema.registry.url", DUMMY_SCHEMA_REGISTRY_URL);
        kafkaProtobufSerializer = new KafkaProtobufSerializer(mockSchemaRegistryClient, opts);
        mockSchemaRegistryClientProvider =
                new SchemaRegistryClientProviders.MockSchemaRegistryClientProvider(
                        mockSchemaRegistryClient);
    }

    @Test
    public void deserializePrimitives() throws Exception {
        FlatProto3OuterClass.FlatProto3 in =
                FlatProto3OuterClass.FlatProto3.newBuilder()
                        .setString(TestUtils.TEST_STRING)
                        .setInt(TestUtils.TEST_INT)
                        .setLong(TestUtils.TEST_LONG)
                        .setFloat(TestUtils.TEST_FLOAT)
                        .setDouble(TestUtils.TEST_DOUBLE)
                        .addInts(TestUtils.TEST_INT)
                        .setBytes(TestUtils.TEST_BYTES)
                        .setBool(TestUtils.TEST_BOOL)
                        .build();

        byte[] inBytes = kafkaProtobufSerializer.serialize(FAKE_TOPIC, in);

        RowType rowType =
                TestUtils.createRowType(
                        new RowType.RowField(TestUtils.STRING_FIELD, new VarCharType()),
                        new RowType.RowField(TestUtils.INT_FIELD, new IntType()),
                        new RowType.RowField(TestUtils.LONG_FIELD, new BigIntType()));

        ProtoRegistryDynamicDeserializationSchema deser =
                new ProtoRegistryDynamicDeserializationSchema(
                        mockSchemaRegistryClientProvider,
                        DUMMY_SCHEMA_REGISTRY_URL,
                        rowType,
                        null,
                        IGNORE_PARSE_ERRORS,
                        READ_DEFAULT_VALUES,
                        USE_DEFAULT_PROTO_INCLUDES,
                        CUSTOM_PROTO_INCLUDES);
        deser.open(null);

        RowData actual = deser.deserialize(inBytes);
        Assertions.assertEquals(3, actual.getArity());
        Assertions.assertEquals(TestUtils.TEST_STRING, actual.getString(0).toString());
        Assertions.assertEquals(TestUtils.TEST_INT, actual.getInt(1));
        Assertions.assertEquals(TestUtils.TEST_LONG, actual.getLong(2));
    }

    @Test
    public void deserializeNestedRow() throws Exception {
        NestedProto3OuterClass.NestedProto3 in =
                NestedProto3OuterClass.NestedProto3.newBuilder()
                        .setString(TestUtils.TEST_STRING)
                        .setInt(TestUtils.TEST_INT)
                        .setLong(TestUtils.TEST_LONG)
                        .setNested(
                                NestedProto3OuterClass.NestedProto3.Nested.newBuilder()
                                        .setDouble(TestUtils.TEST_DOUBLE)
                                        .setFloat(TestUtils.TEST_FLOAT)
                                        .build())
                        .build();

        byte[] inBytes = kafkaProtobufSerializer.serialize(FAKE_TOPIC, in);

        RowType rowType =
                TestUtils.createRowType(
                        new RowType.RowField(
                                TestUtils.NESTED_FIELD,
                                TestUtils.createRowType(
                                        new RowType.RowField(
                                                TestUtils.FLOAT_FIELD, new FloatType()),
                                        new RowType.RowField(
                                                TestUtils.DOUBLE_FIELD, new DoubleType()))),
                        new RowType.RowField(TestUtils.STRING_FIELD, new VarCharType()),
                        new RowType.RowField(TestUtils.INT_FIELD, new IntType()));

        ProtoRegistryDynamicDeserializationSchema deser =
                new ProtoRegistryDynamicDeserializationSchema(
                        mockSchemaRegistryClientProvider,
                        DUMMY_SCHEMA_REGISTRY_URL,
                        rowType,
                        null,
                        IGNORE_PARSE_ERRORS,
                        READ_DEFAULT_VALUES,
                        USE_DEFAULT_PROTO_INCLUDES,
                        CUSTOM_PROTO_INCLUDES);
        deser.open(null);

        RowData actual = deser.deserialize(inBytes);
        Assertions.assertEquals(3, actual.getArity());
        Assertions.assertEquals(TestUtils.TEST_STRING, actual.getString(1).toString());
        Assertions.assertEquals(TestUtils.TEST_INT, actual.getInt(2));

        GenericRowData nestedValue = new GenericRowData(2);
        nestedValue.setField(0, TestUtils.TEST_FLOAT);
        nestedValue.setField(1, TestUtils.TEST_DOUBLE);
        Assertions.assertEquals(nestedValue, actual.getRow(0, 2));
    }

    @Test
    public void deserializeArray() throws Exception {
        FlatProto3OuterClass.FlatProto3 in =
                FlatProto3OuterClass.FlatProto3.newBuilder()
                        .addInts(TestUtils.TEST_INT)
                        .addInts(TestUtils.TEST_INT * 2)
                        .build();

        byte[] inBytes = kafkaProtobufSerializer.serialize(FAKE_TOPIC, in);

        RowType rowType =
                TestUtils.createRowType(new RowType.RowField("ints", new ArrayType(new IntType())));

        ProtoRegistryDynamicDeserializationSchema deser =
                new ProtoRegistryDynamicDeserializationSchema(
                        mockSchemaRegistryClientProvider,
                        DUMMY_SCHEMA_REGISTRY_URL,
                        rowType,
                        null,
                        IGNORE_PARSE_ERRORS,
                        READ_DEFAULT_VALUES,
                        USE_DEFAULT_PROTO_INCLUDES,
                        CUSTOM_PROTO_INCLUDES);
        deser.open(null);

        RowData actual = deser.deserialize(inBytes);
        Assertions.assertEquals(1, actual.getArity());
        Assertions.assertEquals(TestUtils.TEST_INT, actual.getArray(0).getInt(0));
        Assertions.assertEquals(TestUtils.TEST_INT * 2, actual.getArray(0).getInt(1));
    }

    @Test
    public void deserializeMap() throws Exception {
        MapProto3.Proto3Map in =
                MapProto3.Proto3Map.newBuilder()
                        .putMap(TestUtils.TEST_STRING, TestUtils.TEST_STRING)
                        .putNested(
                                TestUtils.TEST_STRING,
                                MapProto3.Proto3Map.Nested.newBuilder()
                                        .setDouble(TestUtils.TEST_DOUBLE)
                                        .setFloat(TestUtils.TEST_FLOAT)
                                        .build())
                        .build();

        byte[] inBytes = kafkaProtobufSerializer.serialize(FAKE_TOPIC, in);

        RowType rowType =
                TestUtils.createRowType(
                        new RowType.RowField(
                                TestUtils.MAP_FIELD,
                                new MapType(new VarCharType(), new VarCharType())),
                        new RowType.RowField(
                                TestUtils.NESTED_FIELD,
                                new MapType(
                                        new VarCharType(),
                                        TestUtils.createRowType(
                                                new RowType.RowField(
                                                        TestUtils.DOUBLE_FIELD, new DoubleType()),
                                                new RowType.RowField(
                                                        TestUtils.FLOAT_FIELD,
                                                        new DoubleType())))));

        ProtoRegistryDynamicDeserializationSchema deser =
                new ProtoRegistryDynamicDeserializationSchema(
                        mockSchemaRegistryClientProvider,
                        DUMMY_SCHEMA_REGISTRY_URL,
                        rowType,
                        null,
                        IGNORE_PARSE_ERRORS,
                        READ_DEFAULT_VALUES,
                        USE_DEFAULT_PROTO_INCLUDES,
                        CUSTOM_PROTO_INCLUDES);
        deser.open(null);

        RowData actual = deser.deserialize(inBytes);
        Assertions.assertEquals(2, actual.getArity());

        Map<BinaryStringData, BinaryStringData> expectedMap = new HashMap<>();
        BinaryStringData binaryString = BinaryStringData.fromString(TestUtils.TEST_STRING);
        expectedMap.put(binaryString, binaryString);
        Assertions.assertEquals(new GenericMapData(expectedMap), actual.getMap(0));

        GenericRowData nestedMapValue = new GenericRowData(2);
        nestedMapValue.setField(0, TestUtils.TEST_DOUBLE);
        nestedMapValue.setField(1, TestUtils.TEST_FLOAT);
        Assertions.assertEquals(nestedMapValue, actual.getMap(1).valueArray().getRow(0, 2));
    }

    @Test
    public void deserializeTimestamp() throws Exception {
        TimestampProto3OuterClass.TimestampProto3 in =
                TimestampProto3OuterClass.TimestampProto3.newBuilder()
                        .setTs(
                                com.google.protobuf.Timestamp.newBuilder()
                                        .setSeconds(TestUtils.TEST_LONG)
                                        .setNanos(TestUtils.TEST_INT)
                                        .build())
                        .build();
        byte[] inBytes = kafkaProtobufSerializer.serialize(FAKE_TOPIC, in);

        RowType rowType =
                TestUtils.createRowType(
                        new RowType.RowField(
                                TestUtils.TIMESTAMP_FIELD,
                                TestUtils.createRowType(
                                        new RowType.RowField(
                                                TestUtils.SECONDS_FIELD, new BigIntType()),
                                        new RowType.RowField(
                                                TestUtils.NANOS_FIELD, new IntType()))));

        ProtoRegistryDynamicDeserializationSchema deser =
                new ProtoRegistryDynamicDeserializationSchema(
                        mockSchemaRegistryClientProvider,
                        DUMMY_SCHEMA_REGISTRY_URL,
                        rowType,
                        null,
                        IGNORE_PARSE_ERRORS,
                        READ_DEFAULT_VALUES,
                        USE_DEFAULT_PROTO_INCLUDES,
                        CUSTOM_PROTO_INCLUDES);
        deser.open(null);

        RowData actual = deser.deserialize(inBytes);
        Assertions.assertEquals(1, actual.getArity());

        GenericRowData timestampValue = new GenericRowData(2);
        timestampValue.setField(0, TestUtils.TEST_LONG);
        timestampValue.setField(1, TestUtils.TEST_INT);
        Assertions.assertEquals(timestampValue, actual.getRow(0, 2));
    }

    @Test
    public void deserializeConfluentDecimal() throws Exception {
        ConfluentDebeziumProto3.DecimalProto3 in =
                ConfluentDebeziumProto3.DecimalProto3.newBuilder()
                        .setDecimal(
                                Decimal.newBuilder()
                                        .setValue(TEST_BYTES)
                                        .setPrecision(TEST_INT * 2)
                                        .setScale(TEST_INT)
                                        .build())
                        .build();
        byte[] inBytes = kafkaProtobufSerializer.serialize(FAKE_TOPIC, in);

        RowType rowType =
                TestUtils.createRowType(
                        new RowType.RowField(
                                "decimal",
                                TestUtils.createRowType(
                                        new RowType.RowField("value", new BinaryType()),
                                        new RowType.RowField("precision", new IntType()),
                                        new RowType.RowField("scale", new IntType()))));

        ProtoRegistryDynamicDeserializationSchema deser =
                new ProtoRegistryDynamicDeserializationSchema(
                        mockSchemaRegistryClientProvider,
                        DUMMY_SCHEMA_REGISTRY_URL,
                        rowType,
                        null,
                        IGNORE_PARSE_ERRORS,
                        READ_DEFAULT_VALUES,
                        USE_DEFAULT_PROTO_INCLUDES,
                        CUSTOM_PROTO_INCLUDES);
        deser.open(null);

        RowData actual = deser.deserialize(inBytes);
        Assertions.assertEquals(1, actual.getArity());

        GenericRowData decimalValue = new GenericRowData(3);
        decimalValue.setField(0, TestUtils.TEST_BYTES.toByteArray());
        decimalValue.setField(1, TestUtils.TEST_INT * 2);
        decimalValue.setField(2, TestUtils.TEST_INT);
        Assertions.assertEquals(decimalValue, actual.getRow(0, 3));
    }

    @Test
    public void deserializeUsingSchemaWithReferences() throws Exception {

        ProtobufSchema addressSchema =
                new ProtobufSchema(AddressAndUser.AddressProto.getDescriptor());
        mockSchemaRegistryClient.register("Address", addressSchema);

        // Register the User schema
        SchemaReference schemaReference = new SchemaReference("Address", "Address", null);
        ProtobufSchema userSchema =
                new ProtobufSchema(
                        AddressAndUser.UserProto.getDescriptor(), Arrays.asList(schemaReference));
        mockSchemaRegistryClient.register("User", userSchema);

        Assertions.assertFalse(
                mockSchemaRegistryClient.getLatestSchemaMetadata("User").getReferences().isEmpty(),
                "User schema should have references");

        AddressAndUser.UserProto in =
                AddressAndUser.UserProto.newBuilder()
                        .setName(TestUtils.TEST_STRING)
                        .setAddress(
                                AddressAndUser.AddressProto.newBuilder()
                                        .setStreet(TestUtils.TEST_STRING)
                                        .setCity(TestUtils.TEST_STRING)
                                        .build())
                        .build();

        byte[] inBytes = kafkaProtobufSerializer.serialize(FAKE_TOPIC, in);

        RowType rowType =
                TestUtils.createRowType(
                        new RowType.RowField("name", new VarCharType()),
                        new RowType.RowField(
                                "address",
                                TestUtils.createRowType(
                                        new RowType.RowField("street", new VarCharType()),
                                        new RowType.RowField("city", new VarCharType()))));
        ProtoRegistryDynamicDeserializationSchema deser =
                new ProtoRegistryDynamicDeserializationSchema(
                        mockSchemaRegistryClientProvider,
                        DUMMY_SCHEMA_REGISTRY_URL,
                        rowType,
                        null,
                        IGNORE_PARSE_ERRORS,
                        READ_DEFAULT_VALUES,
                        USE_DEFAULT_PROTO_INCLUDES,
                        CUSTOM_PROTO_INCLUDES);
        deser.open(null);

        RowData actual = deser.deserialize(inBytes);
        Assertions.assertEquals(2, actual.getArity());
        Assertions.assertEquals(TestUtils.TEST_STRING, actual.getString(0).toString());
        Assertions.assertEquals(TestUtils.TEST_STRING, actual.getRow(1, 2).getString(0).toString());
        Assertions.assertEquals(TestUtils.TEST_STRING, actual.getRow(1, 2).getString(1).toString());
    }

    @Test
    public void deserializeTombstone() throws Exception {

        byte[] inBytes = null;

        RowType rowType =
                TestUtils.createRowType(
                        new RowType.RowField(TestUtils.STRING_FIELD, new VarCharType()));
        ProtoRegistryDynamicDeserializationSchema deser =
                new ProtoRegistryDynamicDeserializationSchema(
                        mockSchemaRegistryClientProvider,
                        DUMMY_SCHEMA_REGISTRY_URL,
                        rowType,
                        null,
                        IGNORE_PARSE_ERRORS,
                        READ_DEFAULT_VALUES,
                        USE_DEFAULT_PROTO_INCLUDES,
                        CUSTOM_PROTO_INCLUDES);
        deser.open(null);

        Assertions.assertNull(deser.deserialize(inBytes));
    }
}
