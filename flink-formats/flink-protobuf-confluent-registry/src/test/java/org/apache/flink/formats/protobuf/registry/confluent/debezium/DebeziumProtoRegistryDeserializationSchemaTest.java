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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.formats.protobuf.registry.confluent.utils.MockInitializationContext;
import org.apache.flink.formats.protobuf.registry.confluent.utils.TestSchemaRegistryConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLoggerExtension;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;

@ExtendWith(TestLoggerExtension.class)
public class DebeziumProtoRegistryDeserializationSchemaTest {

    private static final String SUBJECT = "test-subject";
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

    @Test
    void testDeserialization() throws Exception {
        final String schemaStr =
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

        ProtobufSchema protoSchema = new ProtobufSchema(schemaStr);
        Descriptors.Descriptor protoDescriptor = protoSchema.toDescriptor();

        Descriptors.FileDescriptor fileDescriptor = protoDescriptor.getFile();
        Descriptors.Descriptor envelopDescriptor = fileDescriptor.findMessageTypeByName("Envelope");
        DynamicMessage.Builder envelopBuilder = DynamicMessage.newBuilder(envelopDescriptor);

        Descriptors.Descriptor valueDescriptor = envelopDescriptor.findNestedTypeByName("Value");
        DynamicMessage.Builder valueBuilder = DynamicMessage.newBuilder(valueDescriptor);

        valueBuilder.setField(valueDescriptor.findFieldByName("id"), 10l);
        valueBuilder.setField(valueDescriptor.findFieldByName("name"), "Foobar");
        valueBuilder.setField(valueDescriptor.findFieldByName("salary"), 10);
        DynamicMessage value = valueBuilder.build();
        envelopBuilder.setField(envelopDescriptor.findFieldByName("op"), "d");
        envelopBuilder.setField(envelopDescriptor.findFieldByName("before"), value);
        DynamicMessage outerEnvelop = envelopBuilder.build();
        System.out.println(outerEnvelop);

        byte[] protoRepr = outerEnvelop.toByteArray();

        final int schemaId = client.register(SUBJECT, protoSchema, 0, 100002);

        final DeserializationSchema<RowData> deserializationSchema =
                new DebeziumProtoRegistryDeserializationSchema(
                        rowType,
                        InternalTypeInfo.of(rowType),
                        new TestSchemaRegistryConfig(schemaId, client));
        deserializationSchema.open(new MockInitializationContext());
        SimpleCollector collector = new SimpleCollector();

        deserializationSchema.deserialize(protoRepr, collector);
        System.out.println(collector.list.size());
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

    private RowType defineRowTypesForDebeziumEnvelop() {

        //
        final RowType nestedRow =
                new RowType(
                        false,
                        Arrays.asList(
                                new RowType.RowField("id", new IntType(false)),
                                new RowType.RowField(
                                        "name", new VarCharType(false, VarCharType.MAX_LENGTH)),
                                new RowType.RowField("salary", new IntType(false))));

        final RowType rowType =
                new RowType(
                        false,
                        Arrays.asList(
                                new RowType.RowField("before", nestedRow),
                                new RowType.RowField("after", nestedRow)));
        return rowType;
    }
}
