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

package org.apache.flink.protobuf.registry.confluent.dynamic.serializer;

import org.apache.flink.protobuf.registry.confluent.SchemaRegistryClientProviders;
import org.apache.flink.protobuf.registry.confluent.TestUtils;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.protobuf.registry.confluent.TestUtils.CUSTOM_PROTO_INCLUDES;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.DUMMY_SCHEMA_REGISTRY_URL;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.FAKE_SUBJECT;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.USE_DEFAULT_PROTO_INCLUDES;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.parseBytesToMessage;

public class ProtoRegistryDynamicSerializationSchemaTest {

    private MockSchemaRegistryClient mockSchemaRegistryClient;
    private String className;
    private SchemaRegistryClientProviders.MockSchemaRegistryClientProvider
            mockSchemaRegistryClientProvider;

    @BeforeEach
    public void setup() {
        mockSchemaRegistryClient = new MockSchemaRegistryClient();
        className = TestUtils.DEFAULT_CLASS_NAME + UUID.randomUUID().toString().replace("-", "");
        mockSchemaRegistryClientProvider =
                new SchemaRegistryClientProviders.MockSchemaRegistryClientProvider(
                        mockSchemaRegistryClient);
    }

    @Test
    public void serializePrimitiveTypes() throws Exception {
        RowType rowType =
                TestUtils.createRowType(
                        new RowType.RowField(TestUtils.STRING_FIELD, new VarCharType()),
                        new RowType.RowField(TestUtils.INT_FIELD, new IntType()),
                        new RowType.RowField(TestUtils.LONG_FIELD, new BigIntType()),
                        new RowType.RowField(TestUtils.FLOAT_FIELD, new FloatType()),
                        new RowType.RowField(TestUtils.DOUBLE_FIELD, new DoubleType()),
                        new RowType.RowField(TestUtils.BOOL_FIELD, new BooleanType()),
                        new RowType.RowField(TestUtils.BYTES_FIELD, new BinaryType()));
        ProtoRegistryDynamicSerializationSchema protoRegistryDynamicSerializationSchema =
                new ProtoRegistryDynamicSerializationSchema(
                        TestUtils.DEFAULT_PACKAGE,
                        className,
                        rowType,
                        FAKE_SUBJECT,
                        mockSchemaRegistryClientProvider,
                        DUMMY_SCHEMA_REGISTRY_URL,
                        USE_DEFAULT_PROTO_INCLUDES,
                        CUSTOM_PROTO_INCLUDES);
        protoRegistryDynamicSerializationSchema.open(null);

        GenericRowData rowData = new GenericRowData(7);
        rowData.setField(0, StringData.fromString(TestUtils.TEST_STRING));
        rowData.setField(1, TestUtils.TEST_INT);
        rowData.setField(2, TestUtils.TEST_LONG);
        rowData.setField(3, TestUtils.TEST_FLOAT);
        rowData.setField(4, TestUtils.TEST_DOUBLE);
        rowData.setField(5, true);
        rowData.setField(6, TestUtils.TEST_BYTES.toByteArray());

        byte[] actualBytes = protoRegistryDynamicSerializationSchema.serialize(rowData);

        Message message = parseBytesToMessage(actualBytes, mockSchemaRegistryClient);
        Descriptors.FieldDescriptor stringField =
                message.getDescriptorForType().findFieldByName(TestUtils.STRING_FIELD);
        Descriptors.FieldDescriptor intField =
                message.getDescriptorForType().findFieldByName(TestUtils.INT_FIELD);
        Descriptors.FieldDescriptor longField =
                message.getDescriptorForType().findFieldByName(TestUtils.LONG_FIELD);
        Descriptors.FieldDescriptor floatField =
                message.getDescriptorForType().findFieldByName(TestUtils.FLOAT_FIELD);
        Descriptors.FieldDescriptor doubleField =
                message.getDescriptorForType().findFieldByName(TestUtils.DOUBLE_FIELD);
        Descriptors.FieldDescriptor boolField =
                message.getDescriptorForType().findFieldByName(TestUtils.BOOL_FIELD);
        Descriptors.FieldDescriptor bytesField =
                message.getDescriptorForType().findFieldByName(TestUtils.BYTES_FIELD);

        Assertions.assertEquals(TestUtils.TEST_STRING, message.getField(stringField));
        Assertions.assertEquals(TestUtils.TEST_INT, message.getField(intField));
        Assertions.assertEquals(TestUtils.TEST_LONG, message.getField(longField));
        Assertions.assertEquals(TestUtils.TEST_FLOAT, message.getField(floatField));
        Assertions.assertEquals(TestUtils.TEST_DOUBLE, message.getField(doubleField));
        Assertions.assertEquals(true, message.getField(boolField));
        Assertions.assertEquals(TestUtils.TEST_BYTES, message.getField(bytesField));
    }

    @Test
    public void serializeNestedRow() throws Exception {
        RowType rowType =
                TestUtils.createRowType(
                        new RowType.RowField(
                                TestUtils.NESTED_FIELD,
                                TestUtils.createRowType(
                                        new RowType.RowField(
                                                TestUtils.STRING_FIELD, new VarCharType()),
                                        new RowType.RowField(TestUtils.INT_FIELD, new IntType()))));

        ProtoRegistryDynamicSerializationSchema protoRegistryDynamicSerializationSchema =
                new ProtoRegistryDynamicSerializationSchema(
                        TestUtils.DEFAULT_PACKAGE,
                        className,
                        rowType,
                        FAKE_SUBJECT,
                        mockSchemaRegistryClientProvider,
                        DUMMY_SCHEMA_REGISTRY_URL,
                        USE_DEFAULT_PROTO_INCLUDES,
                        CUSTOM_PROTO_INCLUDES);
        protoRegistryDynamicSerializationSchema.open(null);

        GenericRowData nestedRow = new GenericRowData(2);
        nestedRow.setField(0, StringData.fromString(TestUtils.TEST_STRING));
        nestedRow.setField(1, TestUtils.TEST_INT);

        GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, nestedRow);

        byte[] actualBytes = protoRegistryDynamicSerializationSchema.serialize(rowData);

        Message message = parseBytesToMessage(actualBytes, mockSchemaRegistryClient);
        Descriptors.FieldDescriptor nestedField =
                message.getDescriptorForType().findFieldByName(TestUtils.NESTED_FIELD);
        DynamicMessage nestedMessage = (DynamicMessage) message.getField(nestedField);
        Descriptors.FieldDescriptor stringField =
                nestedMessage.getDescriptorForType().findFieldByName(TestUtils.STRING_FIELD);
        Descriptors.FieldDescriptor intField =
                nestedMessage.getDescriptorForType().findFieldByName(TestUtils.INT_FIELD);

        Assertions.assertEquals(TestUtils.TEST_STRING, nestedMessage.getField(stringField));
        Assertions.assertEquals(TestUtils.TEST_INT, nestedMessage.getField(intField));
    }

    @Test
    public void serializeArray() throws Exception {
        RowType rowType =
                TestUtils.createRowType(
                        new RowType.RowField(
                                TestUtils.ARRAY_FIELD, new ArrayType(new VarCharType())));

        ProtoRegistryDynamicSerializationSchema protoRegistryDynamicSerializationSchema =
                new ProtoRegistryDynamicSerializationSchema(
                        TestUtils.DEFAULT_PACKAGE,
                        className,
                        rowType,
                        FAKE_SUBJECT,
                        mockSchemaRegistryClientProvider,
                        DUMMY_SCHEMA_REGISTRY_URL,
                        USE_DEFAULT_PROTO_INCLUDES,
                        CUSTOM_PROTO_INCLUDES);
        protoRegistryDynamicSerializationSchema.open(null);

        GenericRowData rowData = new GenericRowData(1);
        StringData[] arrayData =
                new StringData[] {StringData.fromString("a"), StringData.fromString("b")};
        rowData.setField(0, new GenericArrayData(arrayData));

        byte[] actualBytes = protoRegistryDynamicSerializationSchema.serialize(rowData);

        Message message = parseBytesToMessage(actualBytes, mockSchemaRegistryClient);
        Descriptors.FieldDescriptor arrayField =
                message.getDescriptorForType().findFieldByName(TestUtils.ARRAY_FIELD);

        Assertions.assertArrayEquals(
                new String[] {"a", "b"}, ((List) message.getField(arrayField)).toArray());
    }

    @Test
    public void serializeMap() throws Exception {
        String otherMapField = "other_m_a_p"; // Exercises the camel case logic
        String nestedMapField = "nested_map";

        RowType nestedMapValueType =
                TestUtils.createRowType(
                        new RowType.RowField(
                                TestUtils.ARRAY_FIELD, new ArrayType(new VarCharType())),
                        new RowType.RowField(TestUtils.INT_FIELD, new IntType()));

        RowType rowType =
                TestUtils.createRowType(
                        new RowType.RowField(
                                TestUtils.MAP_FIELD, new MapType(new VarCharType(), new IntType())),
                        new RowType.RowField(
                                otherMapField, new MapType(new VarCharType(), new IntType())),
                        new RowType.RowField(
                                nestedMapField,
                                new MapType(new VarCharType(), nestedMapValueType)));
        ProtoRegistryDynamicSerializationSchema protoRegistryDynamicSerializationSchema =
                new ProtoRegistryDynamicSerializationSchema(
                        TestUtils.DEFAULT_PACKAGE,
                        className,
                        rowType,
                        FAKE_SUBJECT,
                        mockSchemaRegistryClientProvider,
                        DUMMY_SCHEMA_REGISTRY_URL,
                        USE_DEFAULT_PROTO_INCLUDES,
                        CUSTOM_PROTO_INCLUDES);
        protoRegistryDynamicSerializationSchema.open(null);

        GenericRowData rowData = new GenericRowData(3);
        Map<StringData, Integer> mapContent = new HashMap<>();
        mapContent.put(StringData.fromString(TestUtils.TEST_STRING), TestUtils.TEST_INT);
        rowData.setField(0, new GenericMapData(mapContent));
        rowData.setField(1, new GenericMapData(mapContent));

        Map<StringData, RowData> nestedMapContent = new HashMap<>();
        GenericRowData nestedMapValue = new GenericRowData(2);
        StringData[] arrayData =
                new StringData[] {StringData.fromString("a"), StringData.fromString("b")};
        nestedMapValue.setField(0, new GenericArrayData(arrayData));
        nestedMapValue.setField(1, TestUtils.TEST_INT);
        nestedMapContent.put(StringData.fromString(TestUtils.TEST_STRING), nestedMapValue);
        rowData.setField(2, new GenericMapData(nestedMapContent));

        byte[] actualBytes = protoRegistryDynamicSerializationSchema.serialize(rowData);

        Message message = parseBytesToMessage(actualBytes, mockSchemaRegistryClient);

        Descriptors.FieldDescriptor mapMessageField =
                message.getDescriptorForType().findFieldByName(TestUtils.MAP_FIELD);
        DynamicMessage mapMessage =
                (DynamicMessage) ((List) message.getField(mapMessageField)).get(0);
        Descriptors.FieldDescriptor mapKeyField =
                mapMessage.getDescriptorForType().findFieldByName("key");
        Descriptors.FieldDescriptor mapValueField =
                mapMessage.getDescriptorForType().findFieldByName("value");

        Assertions.assertEquals(TestUtils.TEST_STRING, mapMessage.getField(mapKeyField));
        Assertions.assertEquals(TestUtils.TEST_INT, mapMessage.getField(mapValueField));

        Descriptors.FieldDescriptor otherMapMessageField =
                message.getDescriptorForType().findFieldByName(otherMapField);
        DynamicMessage otherMapMessage =
                (DynamicMessage) ((List) message.getField(otherMapMessageField)).get(0);
        Descriptors.FieldDescriptor otherMapKeyField =
                otherMapMessage.getDescriptorForType().findFieldByName("key");
        Descriptors.FieldDescriptor otherMapValueField =
                otherMapMessage.getDescriptorForType().findFieldByName("value");

        Assertions.assertEquals(TestUtils.TEST_STRING, otherMapMessage.getField(otherMapKeyField));
        Assertions.assertEquals(TestUtils.TEST_INT, otherMapMessage.getField(otherMapValueField));

        Descriptors.FieldDescriptor nestedMapMessageField =
                message.getDescriptorForType().findFieldByName(nestedMapField);
        DynamicMessage nestedMapMessage =
                (DynamicMessage) ((List) message.getField(nestedMapMessageField)).get(0);
        Descriptors.FieldDescriptor nestedMapValueField =
                nestedMapMessage.getDescriptorForType().findFieldByName("value");
        DynamicMessage nestedMapValueMessage =
                (DynamicMessage) nestedMapMessage.getField(nestedMapValueField);
        Descriptors.FieldDescriptor nestedMapValueArrayField =
                nestedMapValueField.getMessageType().findFieldByName(TestUtils.ARRAY_FIELD);
        Descriptors.FieldDescriptor nestedMapValueIntField =
                nestedMapValueField.getMessageType().findFieldByName(TestUtils.INT_FIELD);

        Assertions.assertArrayEquals(
                new String[] {"a", "b"},
                ((List) nestedMapValueMessage.getField(nestedMapValueArrayField)).toArray());
        Assertions.assertEquals(
                TestUtils.TEST_INT, nestedMapValueMessage.getField(nestedMapValueIntField));
    }

    @Test
    public void serializeTimestamp() throws Exception {
        RowType rowType =
                TestUtils.createRowType(
                        new RowType.RowField(
                                TestUtils.TIMESTAMP_FIELD,
                                TestUtils.createRowType(
                                        new RowType.RowField(
                                                TestUtils.SECONDS_FIELD, new BigIntType()),
                                        new RowType.RowField(
                                                TestUtils.NANOS_FIELD, new IntType()))));
        ProtoRegistryDynamicSerializationSchema protoRegistryDynamicSerializationSchema =
                new ProtoRegistryDynamicSerializationSchema(
                        TestUtils.DEFAULT_PACKAGE,
                        className,
                        rowType,
                        FAKE_SUBJECT,
                        mockSchemaRegistryClientProvider,
                        DUMMY_SCHEMA_REGISTRY_URL,
                        USE_DEFAULT_PROTO_INCLUDES,
                        CUSTOM_PROTO_INCLUDES);
        protoRegistryDynamicSerializationSchema.open(null);

        GenericRowData nestedRow = new GenericRowData(2);
        nestedRow.setField(0, TestUtils.TEST_LONG);
        nestedRow.setField(1, TestUtils.TEST_INT);

        GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, nestedRow);

        byte[] actualBytes = protoRegistryDynamicSerializationSchema.serialize(rowData);

        Message message = parseBytesToMessage(actualBytes, mockSchemaRegistryClient);
        Descriptors.FieldDescriptor tsFieldDescriptor =
                message.getDescriptorForType().findFieldByName(TestUtils.TIMESTAMP_FIELD);
        DynamicMessage tsMessage = (DynamicMessage) message.getField(tsFieldDescriptor);
        Descriptors.FieldDescriptor secondsField =
                tsMessage.getDescriptorForType().findFieldByName(TestUtils.SECONDS_FIELD);
        Descriptors.FieldDescriptor nanosField =
                tsMessage.getDescriptorForType().findFieldByName(TestUtils.NANOS_FIELD);

        Assertions.assertEquals(TestUtils.TEST_LONG, tsMessage.getField(secondsField));
        Assertions.assertEquals(TestUtils.TEST_INT, tsMessage.getField(nanosField));
    }
}
