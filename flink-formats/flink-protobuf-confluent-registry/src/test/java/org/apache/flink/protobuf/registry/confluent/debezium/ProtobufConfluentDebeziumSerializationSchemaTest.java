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
import org.apache.flink.protobuf.registry.confluent.dynamic.serializer.ProtoRegistryDynamicSerializationSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.apache.flink.protobuf.registry.confluent.TestUtils.CUSTOM_PROTO_INCLUDES;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.DUMMY_SCHEMA_REGISTRY_URL;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.FAKE_SUBJECT;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.USE_DEFAULT_PROTO_INCLUDES;
import static org.apache.flink.protobuf.registry.confluent.TestUtils.parseBytesToMessage;

public class ProtobufConfluentDebeziumSerializationSchemaTest {

    private MockSchemaRegistryClient mockSchemaRegistryClient;
    private String className;
    private ProtobufConfluentDebeziumSerializationSchema ser;
    private GenericRowData rowData;
    private static final String AFTER_FIELD = "after";
    private static final String BEFORE_FIELD = "before";
    private static final String OP_FIELD = "op";

    @BeforeEach
    public void setup() throws Exception {
        mockSchemaRegistryClient = new MockSchemaRegistryClient();
        className = TestUtils.DEFAULT_CLASS_NAME + UUID.randomUUID().toString().replace("-", "");
        RowType rowType =
                TestUtils.createRowType(
                        new RowType.RowField(TestUtils.STRING_FIELD, new VarCharType()));
        ProtoRegistryDynamicSerializationSchema wrappedSer =
                new ProtoRegistryDynamicSerializationSchema(
                        TestUtils.DEFAULT_PACKAGE,
                        className,
                        rowType,
                        FAKE_SUBJECT,
                        new SchemaRegistryClientProviders.MockSchemaRegistryClientProvider(
                                mockSchemaRegistryClient),
                        DUMMY_SCHEMA_REGISTRY_URL,
                        USE_DEFAULT_PROTO_INCLUDES,
                        CUSTOM_PROTO_INCLUDES);
        ser = new ProtobufConfluentDebeziumSerializationSchema(wrappedSer);
        ser.open(null);
        rowData = new GenericRowData(1);
        rowData.setField(0, StringData.fromString(TestUtils.TEST_STRING));
    }

    @Test
    public void serializeInsert() throws Exception {

        rowData.setRowKind(RowKind.INSERT);
        runCreateTest(rowData);
    }

    @Test
    public void serializeDelete() throws Exception {

        rowData.setRowKind(RowKind.DELETE);
        runDeleteTest(rowData);
    }

    @Test
    public void serializeUpdateAfter() throws Exception {

        rowData.setRowKind(RowKind.UPDATE_AFTER);
        runCreateTest(rowData);
    }

    @Test
    public void serializeUpdateBefore() throws Exception {

        rowData.setRowKind(RowKind.UPDATE_BEFORE);
        runDeleteTest(rowData);
    }

    private void runCreateTest(RowData rowData) {
        byte[] actualBytes = ser.serialize(rowData);

        Message message = parseBytesToMessage(actualBytes, mockSchemaRegistryClient);
        Descriptors.FieldDescriptor opField =
                message.getDescriptorForType().findFieldByName(OP_FIELD);
        Descriptors.FieldDescriptor afterField =
                message.getDescriptorForType().findFieldByName(AFTER_FIELD);
        Descriptors.FieldDescriptor beforeField =
                message.getDescriptorForType().findFieldByName(BEFORE_FIELD);
        DynamicMessage afterMessage = (DynamicMessage) message.getField(afterField);
        Descriptors.FieldDescriptor stringField =
                afterMessage.getDescriptorForType().findFieldByName(TestUtils.STRING_FIELD);

        Assertions.assertFalse(message.hasField(beforeField));
        Assertions.assertEquals(TestUtils.TEST_STRING, afterMessage.getField(stringField));
        Assertions.assertEquals(
                ProtobufConfluentDebeziumDeserializationSchema.OP_CREATE,
                message.getField(opField));
    }

    private void runDeleteTest(RowData rowData) {
        byte[] actualBytes = ser.serialize(rowData);

        Message message = parseBytesToMessage(actualBytes, mockSchemaRegistryClient);
        Descriptors.FieldDescriptor opField =
                message.getDescriptorForType().findFieldByName(OP_FIELD);
        Descriptors.FieldDescriptor afterField =
                message.getDescriptorForType().findFieldByName(AFTER_FIELD);
        Descriptors.FieldDescriptor beforeField =
                message.getDescriptorForType().findFieldByName(BEFORE_FIELD);
        DynamicMessage beforeMessage = (DynamicMessage) message.getField(beforeField);
        Descriptors.FieldDescriptor stringField =
                beforeMessage.getDescriptorForType().findFieldByName(TestUtils.STRING_FIELD);

        Assertions.assertFalse(message.hasField(afterField));
        Assertions.assertEquals(TestUtils.TEST_STRING, beforeMessage.getField(stringField));
        Assertions.assertEquals(
                ProtobufConfluentDebeziumDeserializationSchema.OP_DELETE,
                message.getField(opField));
    }
}
