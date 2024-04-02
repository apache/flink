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

package org.apache.flink.formats.protobuf.registry.confluent.debezium;

import org.apache.flink.formats.protobuf.registry.confluent.SchemaCoder;
import org.apache.flink.formats.protobuf.registry.confluent.SchemaCoderProviders;
import org.apache.flink.formats.protobuf.registry.confluent.utils.MockInitializationContext;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.confluent.connect.protobuf.ProtobufConverter;
import io.confluent.connect.protobuf.ProtobufData;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;
import static org.junit.Assert.assertEquals;

/** Tests for deserializing Schema Registry Proto Debezium encoded messages. */
@ExtendWith(TestLoggerExtension.class)
public class DebeziumProtoRegistrySerializationDeserializationTest {

    private static final String TEST_TOPIC = "test-topic";

    /** Kafka Connect's Converters use topic-name-value as default subject. */
    private static final String SUBJECT = TEST_TOPIC + "-value";

    private static final Map<String, ?> SR_CONFIG =
            Collections.singletonMap(
                    AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "localhost");

    private static final String DEBEZIUM_PROTO_SCHEMA =
            "syntax = \"proto3\";\n"
                    + "package proto_full_postgres.public.employee;\n"
                    + "\n"
                    + "message Envelope {\n"
                    + "  Value before = 1;\n"
                    + "  Value after = 2;\n"
                    + "  Source source = 3;\n"
                    + "  string op = 4;\n"
                    + "  int64 ts_ms = 5;\n"
                    + "  block transaction = 6;\n"
                    + "\n"
                    + "  message Value {\n"
                    + "    int64 id = 1;\n"
                    + "    string name = 2;\n"
                    + "    int32 salary = 3;\n"
                    + "  }\n"
                    + "  message Source {\n"
                    + "    string version = 1;\n"
                    + "    string connector = 2;\n"
                    + "    string name = 3;\n"
                    + "    int64 ts_ms = 4;\n"
                    + "    string snapshot = 5;\n"
                    + "    string db = 6;\n"
                    + "    string sequence = 7;\n"
                    + "    string schema = 8;\n"
                    + "    string table = 9;\n"
                    + "    int64 txId = 10;\n"
                    + "    int64 lsn = 11;\n"
                    + "    int64 xmin = 12;\n"
                    + "  }\n"
                    + "  message block {\n"
                    + "    string id = 1;\n"
                    + "    int64 total_order = 2;\n"
                    + "    int64 data_collection_order = 3;\n"
                    + "  }\n"
                    + "}\n";
    private static final RowType rowType =
            (RowType)
                    ROW(FIELD("id", BIGINT()), FIELD("name", STRING()), FIELD("salary", INT()))
                            .getLogicalType();

    private static SchemaRegistryClient client;

    @BeforeAll
    static void beforeClass() {
        client = new MockSchemaRegistryClient();
    }

    @AfterEach
    void after() throws IOException, RestClientException {
        client.deleteSubject(SUBJECT);
    }

    /**
     * Populates a debezium message with valid payload corresponding to before key.
     *
     * @return
     */
    private DynamicMessage populateDebeziumMessageWithBeforeKey() {
        return createDebeziumMessageWithKeyAndOperation("before", "d");
    }

    /**
     * Populates a debezium message with valid payload corresponding to after key.
     *
     * @return
     */
    private DynamicMessage populateDebeziumMessageWithAfterKey() {
        return createDebeziumMessageWithKeyAndOperation("after", "c");
    }

    private Descriptors.Descriptor envelopDescriptor() {
        ProtobufSchema protoSchema = new ProtobufSchema(DEBEZIUM_PROTO_SCHEMA);
        Descriptors.Descriptor protoDescriptor = protoSchema.toDescriptor();
        Descriptors.FileDescriptor fileDescriptor = protoDescriptor.getFile();
        return fileDescriptor.findMessageTypeByName("Envelope");
    }

    private DynamicMessage createDebeziumMessageWithKeyAndOperation(String key, String op) {
        Descriptors.Descriptor envelopDescriptor = envelopDescriptor();
        DynamicMessage.Builder envelopBuilder = DynamicMessage.newBuilder(envelopDescriptor);

        Descriptors.Descriptor valueDescriptor = envelopDescriptor.findNestedTypeByName("Value");
        DynamicMessage.Builder valueBuilder = DynamicMessage.newBuilder(valueDescriptor);

        valueBuilder.setField(valueDescriptor.findFieldByName("id"), 10L);
        valueBuilder.setField(valueDescriptor.findFieldByName("name"), "testName");
        valueBuilder.setField(valueDescriptor.findFieldByName("salary"), 10);
        DynamicMessage value = valueBuilder.build();

        envelopBuilder.setField(envelopDescriptor.findFieldByName("op"), op);
        envelopBuilder.setField(envelopDescriptor.findFieldByName(key), value);
        DynamicMessage outerEnvelop = envelopBuilder.build();
        return outerEnvelop;
    }

    /**
     * Populates a debezium message with before and after fields.
     *
     * @return Dynamic message in debezium format with before and after sections populated.
     */
    private DynamicMessage createDebeziumMessageForUpdate() {
        Descriptors.Descriptor envelopDescriptor = envelopDescriptor();
        DynamicMessage.Builder envelopBuilder = DynamicMessage.newBuilder(envelopDescriptor);

        Descriptors.Descriptor valueDescriptor = envelopDescriptor.findNestedTypeByName("Value");
        DynamicMessage.Builder valueBuilderBefore = DynamicMessage.newBuilder(valueDescriptor);
        valueBuilderBefore.setField(valueDescriptor.findFieldByName("id"), 10L);
        valueBuilderBefore.setField(valueDescriptor.findFieldByName("name"), "testName");
        valueBuilderBefore.setField(valueDescriptor.findFieldByName("salary"), 10);
        DynamicMessage valueBefore = valueBuilderBefore.build();

        DynamicMessage.Builder valueBuilderAfter = DynamicMessage.newBuilder(valueDescriptor);
        valueBuilderAfter.setField(valueDescriptor.findFieldByName("id"), 10L);
        valueBuilderAfter.setField(valueDescriptor.findFieldByName("name"), "testNameUpdated");
        valueBuilderAfter.setField(valueDescriptor.findFieldByName("salary"), 10);
        DynamicMessage valueAfter = valueBuilderAfter.build();

        envelopBuilder.setField(envelopDescriptor.findFieldByName("op"), "u");
        envelopBuilder.setField(envelopDescriptor.findFieldByName("before"), valueBefore);
        envelopBuilder.setField(envelopDescriptor.findFieldByName("after"), valueAfter);
        DynamicMessage outerEnvelop = envelopBuilder.build();
        return outerEnvelop;
    }

    /**
     * Tests to validate that we can read a byte[] written by connect from Flink's Deserializer.
     *
     * @throws Exception
     */
    @Test
    void testDeserializationForConnectEncodedMessage() throws Exception {
        DynamicMessage debeziumMessage = populateDebeziumMessageWithBeforeKey();
        ProtobufSchema schema = new ProtobufSchema(DEBEZIUM_PROTO_SCHEMA);
        ProtobufData protoToSchemaAndValueConverter = new ProtobufData();
        SchemaAndValue schemaAndValue =
                protoToSchemaAndValueConverter.toConnectData(schema, debeziumMessage);
        ProtobufConverter protoConverter = new ProtobufConverter(client);
        // need to call configure to properly setup the converter
        protoConverter.configure(SR_CONFIG, false);
        byte[] payload =
                protoConverter.fromConnectData(
                        TEST_TOPIC, schemaAndValue.schema(), schemaAndValue.value());
        SchemaCoder coder = getDefaultCoder(rowType);

        // should be able to read this now from flink deserialization machinery
        DebeziumProtoRegistryDeserializationSchema protoDeserializer =
                new DebeziumProtoRegistryDeserializationSchema(
                        coder, rowType, InternalTypeInfo.of(rowType));
        protoDeserializer.open(new MockInitializationContext());
        SimpleCollector collector = new SimpleCollector();
        protoDeserializer.deserialize(payload, collector);
        List<RowData> rows = collector.list;
        assertEquals(rows.size(), 1);
        RowData row = rows.get(0);
        String name = row.getString(1).toString();
        assertEquals("testName", name);
        assertEquals(row.getRowKind(), RowKind.DELETE);
    }

    /**
     * Validate that type of rows obtained post deserialization matches supplied kinds.
     *
     * <p>Post deserialization we might get multiple {@link org.apache.flink.table.data.RowData}
     * rows. This method validates that type of rows match supplied types for a specified index.
     *
     * @param debeziumMessageGenerator supplier to generate debezium encoded protobuf message.
     * @param kinds list of {@link org.apache.flink.types.RowKind} to match corresponding rows
     *     against.
     * @throws Exception
     */
    private void testDataDeserialization(
            Supplier<DynamicMessage> debeziumMessageGenerator, List<RowKind> kinds)
            throws Exception {
        DynamicMessage debeziumMessage = debeziumMessageGenerator.get();
        ProtobufSchema schema = new ProtobufSchema(DEBEZIUM_PROTO_SCHEMA);
        // get an outputstream and populate with debezium data
        ByteArrayOutputStream byteOutStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteOutStream);
        int schemaId = client.register(SUBJECT, schema);
        dataOutputStream.writeByte(0);
        dataOutputStream.writeInt(schemaId);
        dataOutputStream.write(SchemaCoder.emptyMessageIndexes().array());
        dataOutputStream.write(debeziumMessage.toByteArray());
        // we don't care about data yet
        dataOutputStream.flush();

        byte[] payload = byteOutStream.toByteArray();
        SchemaCoder coder = getDefaultCoder(rowType);
        // should be able to read this now from flink deserialization machinery
        DebeziumProtoRegistryDeserializationSchema protoDeserializer =
                new DebeziumProtoRegistryDeserializationSchema(
                        coder, rowType, InternalTypeInfo.of(rowType));
        protoDeserializer.open(new MockInitializationContext());
        SimpleCollector collector = new SimpleCollector();
        protoDeserializer.deserialize(payload, collector);
        List<RowData> rows = collector.list;
        assertEquals(rows.size(), kinds.size());
        for (int i = 0; i < kinds.size(); i++) {
            assertEquals(rows.get(i).getRowKind(), kinds.get(i));
        }
    }

    @Test
    public void testDeleteDataDeserialization() throws Exception {
        testDataDeserialization(
                this::populateDebeziumMessageWithBeforeKey, ImmutableList.of(RowKind.DELETE));
    }

    @Test
    public void testInsertDataDeserialization() throws Exception {
        testDataDeserialization(
                this::populateDebeziumMessageWithAfterKey, ImmutableList.of(RowKind.INSERT));
    }

    @Test
    public void testUpdateDataDeserialization() throws Exception {
        testDataDeserialization(
                this::createDebeziumMessageForUpdate,
                ImmutableList.of(RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER));
    }

    @Test
    void testSerializationForConnectDecodedMessage() throws Exception {

        RowType debeziumRowType =
                DebeziumProtoRegistrySerializationSchema.createDebeziumProtoRowType(
                        fromLogicalToDataType(rowType));
        SchemaCoder coder = SchemaCoderProviders.createDefault(SUBJECT, debeziumRowType, client);
        DebeziumProtoRegistrySerializationSchema protoSerializer =
                new DebeziumProtoRegistrySerializationSchema(coder, rowType);
        protoSerializer.open(new MockInitializationContext());
        GenericRowData row = GenericRowData.of(1L, StringData.fromString("testValue"), 10);
        row.setRowKind(RowKind.INSERT);
        byte[] payload = protoSerializer.serialize(row);

        ProtobufConverter protoConverter = new ProtobufConverter(client);
        protoConverter.configure(SR_CONFIG, false); // out schema registry version
        SchemaAndValue connectData = protoConverter.toConnectData(TEST_TOPIC, payload);
        Struct value = (Struct) connectData.value();
        Struct after = (Struct) value.get("after");
        assertEquals("testValue", after.get("name"));
    }

    private SchemaCoder getDefaultCoder(RowType rowType) {
        return SchemaCoderProviders.createDefault(SUBJECT, rowType, client);
    }

    private static class SimpleCollector implements Collector<RowData> {

        private final List<RowData> list = new ArrayList<>();

        @Override
        public void collect(RowData record) {
            list.add(record);
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
