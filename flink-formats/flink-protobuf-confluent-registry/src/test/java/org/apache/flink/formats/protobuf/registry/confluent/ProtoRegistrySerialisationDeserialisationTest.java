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
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.protobuf.registry.confluent.utils.FlinkToProtoSchemaConverter;
import org.apache.flink.formats.protobuf.registry.confluent.utils.MockInitializationContext;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.DateTimeUtils;
import org.apache.flink.util.TestLoggerExtension;

import com.google.protobuf.Descriptors.Descriptor;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Smoke tests for checking {@link ProtoRegistrySerializationSchema} and {@link
 * ProtoRegistryDeserializationSchema}.
 *
 * <p>For more thorough tests on converting different types see {@link RowDataToProtoConvertersTest}
 * and/or {@link ProtoToRowDataConvertersTest}.
 */
@ExtendWith(TestLoggerExtension.class)
public class ProtoRegistrySerialisationDeserialisationTest {

    private static final String SUBJECT = "test-subject";

    private static SchemaRegistryClient client;

    @BeforeAll
    static void beforeClass() {
        client = new MockSchemaRegistryClient();
    }

    private static byte[] serialize(int schemaId, GenericRowData rowData, RowType rowType)
            throws Exception {

        SchemaCoder coder =
                new SchemaCoderProviders.PreRegisteredSchemaCoder(schemaId, null, client);

        final SerializationSchema<RowData> serializationSchema =
                new ProtoRegistrySerializationSchema(coder, rowType);
        serializationSchema.open(new MockInitializationContext());
        return serializationSchema.serialize(rowData);
    }

    private static RowData deserialize(byte[] data, int schemaId, RowType rowType)
            throws Exception {

        SchemaCoder coder =
                new SchemaCoderProviders.PreRegisteredSchemaCoder(schemaId, null, client);

        final DeserializationSchema<RowData> deserializationSchema =
                new ProtoRegistryDeserializationSchema(
                        coder, rowType, InternalTypeInfo.of(rowType));
        deserializationSchema.open(new MockInitializationContext());
        return deserializationSchema.deserialize(data);
    }

    @AfterEach
    void after() throws IOException, RestClientException {
        client.deleteSubject(SUBJECT);
    }

    @Test
    void testSerDe() throws Exception {
        final RowType rowType =
                new RowType(
                        false,
                        Arrays.asList(
                                new RowField("booleanNotNull", new BooleanType(false)),
                                new RowField("tinyIntNotNull", new TinyIntType(false)),
                                new RowField("smallIntNotNull", new SmallIntType(false)),
                                new RowField("intNotNull", new IntType(false)),
                                new RowField("bigintNotNull", new BigIntType(false)),
                                new RowField("doubleNotNull", new DoubleType(false)),
                                new RowField("floatNotNull", new FloatType(false)),
                                new RowField("date", new DateType(true)),
                                new RowField("decimal", new DecimalType(true, 5, 1)),
                                new RowField("timestamp", new LocalZonedTimestampType(true, 9)),
                                new RowField("time", new TimeType(true, 3)),
                                new RowField(
                                        "string", new VarCharType(true, VarCharType.MAX_LENGTH)),
                                new RowField(
                                        "bytes",
                                        new VarBinaryType(true, VarBinaryType.MAX_LENGTH))));

        final int timestampSeconds = 960000000;
        final int timestampNanos = 34567890;
        final GenericRowData row = new GenericRowData(13);
        row.setField(0, true);
        row.setField(1, (byte) 42);
        row.setField(2, (short) 42);
        row.setField(3, 42);
        row.setField(4, 42L);
        row.setField(5, 42D);
        row.setField(6, 42F);
        row.setField(7, DateTimeUtils.toInternal(LocalDate.of(2023, 9, 4)));
        row.setField(8, DecimalData.fromBigDecimal(BigDecimal.valueOf(12345L, 1), 5, 1));
        row.setField(
                9,
                TimestampData.fromEpochMillis(
                        timestampSeconds * 1000L + timestampNanos / 1000_000,
                        timestampNanos % 1000_000));
        row.setField(10, DateTimeUtils.toInternal(LocalTime.of(16, 45, 1, 999_000_000)));
        row.setField(11, StringData.fromString("Random string"));
        row.setField(12, new byte[] {1, 2, 3});

        final Descriptor descriptor =
                FlinkToProtoSchemaConverter.fromFlinkSchema(
                        rowType, "Row", "io.confluent.generated");
        final int schemaId = client.register(SUBJECT, new ProtobufSchema(descriptor));
        final byte[] serialized = serialize(schemaId, row, rowType);
        final RowData deserialized = deserialize(serialized, schemaId, rowType);
        assertThat(deserialized).isEqualTo(row);
    }

    /*@Test
    void test() throws Exception {
        final byte[] data = {
            0, 0, 1, -122, -94, 0, 10, 4, 83, 69, 76, 76, 16, -120, 6, 26, 4, 90, 66, 90, 88, 32,
            107, 42, 6, 88, 89, 90, 55, 56, 57, 50, 6, 85, 115, 101, 114, 95, 53
        };
        final String schemaStr =
                "syntax = \"proto3\";\n"
                        + "package ksql;\n"
                        + "\n"
                        + "message StockTrade {\n"
                        + "  string side = 1;\n"
                        + "  int32 quantity = 2;\n"
                        + "  string symbol = 3;\n"
                        + "  int32 price = 4;\n"
                        + "  string account = 5;\n"
                        + "  string userid = 6;\n"
                        + "}";

        final ProtobufSchema protobufSchema = new ProtobufSchema(schemaStr);
        final int schemaId = client.register(SUBJECT, protobufSchema, 0, 100002);
        final LogicalType flinkSchema =
                ProtoToFlinkSchemaConverter.toFlinkSchema(protobufSchema.toDescriptor());
        final DeserializationSchema<RowData> serializationSchema =
                new ProtoRegistryDeserializationSchema(
                        new TestSchemaRegistryConfig(schemaId, client),
                        (RowType) flinkSchema,
                        InternalTypeInfo.of(flinkSchema));
        serializationSchema.open(new MockInitializationContext());
        final RowData deserialized = serializationSchema.deserialize(data);
        final GenericRowData expected = new GenericRowData(6);
        expected.setField(0, StringData.fromString("SELL"));
        expected.setField(1, 776);
        expected.setField(2, StringData.fromString("ZBZX"));
        expected.setField(3, 107);
        expected.setField(4, StringData.fromString("XYZ789"));
        expected.setField(5, StringData.fromString("User_5"));
        assertThat(deserialized).isEqualTo(expected);
    }*/
}
