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

import org.apache.flink.formats.protobuf.registry.confluent.ProtoToRowDataConverters.ProtoToRowDataConverter;
import org.apache.flink.formats.protobuf.registry.confluent.utils.ProtoToFlinkSchemaConverter;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.DateTimeUtils;
import org.apache.flink.util.TestLoggerExtension;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import com.google.type.Date;
import com.google.type.TimeOfDay;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.protobuf.type.utils.DecimalUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ProtoToRowDataConverters}. */
@ExtendWith(TestLoggerExtension.class)
class ProtoToRowDataConvertersTest {

    @Test
    void testPlainRow() throws IOException {
        final String schemaStr =
                "syntax = \"proto3\";\n"
                        + "package io.confluent.protobuf.generated;\n"
                        + "\n"
                        + "import \"confluent/type/decimal.proto\";\n"
                        + "import \"google/protobuf/timestamp.proto\";\n"
                        + "import \"google/type/date.proto\";\n"
                        + "import \"google/type/timeofday.proto\";\n"
                        + "\n"
                        + "message Row {\n"
                        + "  bool booleanNotNull = 1;\n"
                        + "  int32 tinyIntNotNull = 3 [(confluent.field_meta) = {\n"
                        + "    params: [\n"
                        + "      {\n"
                        + "        key: \"connect.type\",\n"
                        + "        value: \"int8\"\n"
                        + "      }\n"
                        + "    ]\n"
                        + "  }];\n"
                        + "  int32 smallIntNotNull = 5 [(confluent.field_meta) = {\n"
                        + "    params: [\n"
                        + "      {\n"
                        + "        key: \"connect.type\",\n"
                        + "        value: \"int16\"\n"
                        + "      }\n"
                        + "    ]\n"
                        + "  }];\n"
                        + "  int32 intNotNull = 7;\n"
                        + "  int64 bigintNotNull = 9;\n"
                        + "  double doubleNotNull = 11;\n"
                        + "  float floatNotNull = 13;\n"
                        + "  optional .google.type.Date date = 15;\n"
                        + "  optional .confluent.type.Decimal decimal = 16 [(confluent.field_meta) = {\n"
                        + "    params: [\n"
                        + "      {\n"
                        + "        value: \"5\",\n"
                        + "        key: \"precision\"\n"
                        + "      },\n"
                        + "      {\n"
                        + "        value: \"1\",\n"
                        + "        key: \"scale\"\n"
                        + "      }\n"
                        + "    ]\n"
                        + "  }];\n"
                        + "  optional .google.protobuf.Timestamp timestamp = 17;"
                        + "  optional .google.type.TimeOfDay time = 18;\n"
                        + "  optional string string = 19;\n"
                        + "  optional bytes bytes = 20;\n"
                        + "}";

        ProtobufSchema protoSchema = new ProtobufSchema(schemaStr);
        Descriptor schema = protoSchema.toDescriptor();

        final RowType flinkSchema = (RowType) ProtoToFlinkSchemaConverter.toFlinkSchema(schema);

        final ProtoToRowDataConverters.ProtoToRowDataConverter converter =
                ProtoToRowDataConverters.createConverter(schema, flinkSchema);

        final FieldDescriptor decimalDescriptor = schema.findFieldByName("decimal");
        final int timestampSeconds = 960000000;
        final int timestampNanos = 34567890;
        final DynamicMessage message =
                DynamicMessage.newBuilder(schema)
                        .setField(schema.findFieldByName("booleanNotNull"), true)
                        .setField(schema.findFieldByName("tinyIntNotNull"), 42)
                        .setField(schema.findFieldByName("smallIntNotNull"), 42)
                        .setField(schema.findFieldByName("intNotNull"), 42)
                        .setField(schema.findFieldByName("bigintNotNull"), 42L)
                        .setField(schema.findFieldByName("doubleNotNull"), 42D)
                        .setField(schema.findFieldByName("floatNotNull"), 42F)
                        .setField(
                                schema.findFieldByName("date"),
                                Date.newBuilder().setYear(2023).setMonth(9).setDay(4).build())
                        .setField(
                                decimalDescriptor,
                                DecimalUtils.fromBigDecimal(BigDecimal.valueOf(12345L, 1)))
                        .setField(
                                schema.findFieldByName("timestamp"),
                                Timestamp.newBuilder()
                                        .setSeconds(timestampSeconds)
                                        .setNanos(timestampNanos)
                                        .build())
                        .setField(
                                schema.findFieldByName("time"),
                                TimeOfDay.newBuilder()
                                        .setHours(16)
                                        .setMinutes(45)
                                        .setSeconds(1)
                                        .setNanos(9000)
                                        .build())
                        .setField(schema.findFieldByName("string"), "Random string")
                        .setField(
                                schema.findFieldByName("bytes"),
                                ByteString.copyFrom(new byte[] {1, 2, 3}))
                        .build();
        final RowData row = (RowData) converter.convert(message);

        final GenericRowData expected = new GenericRowData(13);
        expected.setField(0, true);
        expected.setField(1, (byte) 42);
        expected.setField(2, (short) 42);
        expected.setField(3, 42);
        expected.setField(4, 42L);
        expected.setField(5, 42D);
        expected.setField(6, 42F);
        expected.setField(7, DateTimeUtils.toInternal(LocalDate.of(2023, 9, 4)));
        expected.setField(8, DecimalData.fromBigDecimal(BigDecimal.valueOf(12345L, 1), 5, 1));
        expected.setField(
                9,
                TimestampData.fromEpochMillis(
                        timestampSeconds * 1000L + timestampNanos / 1000_000,
                        timestampNanos % 1000_000));
        expected.setField(10, DateTimeUtils.toInternal(LocalTime.of(16, 45, 1, 999)));
        expected.setField(11, StringData.fromString("Random string"));
        expected.setField(12, new byte[] {1, 2, 3});
        assertThat(row).isEqualTo(expected);
    }

    @Test
    void testOptionalPrimitiveType() throws IOException {
        final String schemaStr =
                "syntax = \"proto3\";\n"
                        + "package io.confluent.protobuf.generated;\n"
                        + "\n"
                        + "message Row {\n"
                        + "  int32 intNotNull = 1;\n"
                        + "  optional int32 int = 2;\n"
                        + "}";

        ProtobufSchema protoSchema = new ProtobufSchema(schemaStr);
        Descriptor schema = protoSchema.toDescriptor();

        final RowType flinkSchema = (RowType) ProtoToFlinkSchemaConverter.toFlinkSchema(schema);

        final ProtoToRowDataConverter converter =
                ProtoToRowDataConverters.createConverter(schema, flinkSchema);

        final DynamicMessage message = DynamicMessage.newBuilder(schema).build();
        final RowData row = (RowData) converter.convert(message);

        final GenericRowData expected = new GenericRowData(2);
        expected.setField(0, 0);
        expected.setField(1, null);
        assertThat(row).isEqualTo(expected);
    }

    @Test
    void testCollections() throws Exception {
        final String schemaStr =
                "syntax = \"proto3\";\n"
                        + "package io.confluent.protobuf.generated;\n"
                        + "\n"
                        + "message Row {\n"
                        + "  repeated int64 array = 1;\n"
                        + "  repeated MapEntry map = 2;\n"
                        + "\n"
                        + "message MapEntry {\n"
                        + "    optional string key = 1;\n"
                        + "    optional int64 value = 2;\n"
                        + "  }\n"
                        + "}";

        ProtobufSchema protoSchema = new ProtobufSchema(schemaStr);
        Descriptor schema = protoSchema.toDescriptor();
        final Descriptor mapSchema =
                schema.getNestedTypes().stream()
                        .filter(descriptor -> descriptor.getName().equals("MapEntry"))
                        .findFirst()
                        .get();

        final RowType flinkSchema = (RowType) ProtoToFlinkSchemaConverter.toFlinkSchema(schema);

        final ProtoToRowDataConverter converter =
                ProtoToRowDataConverters.createConverter(schema, flinkSchema);
        final DynamicMessage message =
                DynamicMessage.newBuilder(schema)
                        .addRepeatedField(schema.findFieldByName("array"), 42L)
                        .addRepeatedField(schema.findFieldByName("array"), 422L)
                        .addRepeatedField(schema.findFieldByName("array"), 4422L)
                        .addRepeatedField(
                                schema.findFieldByName("map"),
                                DynamicMessage.newBuilder(mapSchema)
                                        .setField(mapSchema.findFieldByName("key"), "ABC")
                                        .setField(mapSchema.findFieldByName("value"), 123L)
                                        .build())
                        .addRepeatedField(
                                schema.findFieldByName("map"),
                                DynamicMessage.newBuilder(mapSchema)
                                        .setField(mapSchema.findFieldByName("key"), "DEF")
                                        .setField(mapSchema.findFieldByName("value"), 420L)
                                        .build())
                        .build();
        RowData row = (RowData) converter.convert(message);

        final GenericRowData expected = new GenericRowData(2);
        expected.setField(0, new GenericArrayData(new Long[] {42L, 422L, 4422L}));
        final Map<BinaryStringData, Long> expectedMap = new HashMap<>();
        expectedMap.put(BinaryStringData.fromString("ABC"), 123L);
        expectedMap.put(BinaryStringData.fromString("DEF"), 420L);
        expected.setField(1, new GenericMapData(expectedMap));
        assertThat(row).isEqualTo(expected);
    }

    @Test
    void testMultiset() throws Exception {
        final String schemaStr =
                "syntax = \"proto3\";\n"
                        + "package io.confluent.protobuf.generated;\n"
                        + "\n"
                        + "message Row {\n"
                        + "  repeated MapEntry multiset = 1;\n"
                        + "\n"
                        + "message MapEntry {\n"
                        + "    optional string key = 1;\n"
                        + "    int32 value = 2;\n"
                        + "  }\n"
                        + "}";

        ProtobufSchema protoSchema = new ProtobufSchema(schemaStr);
        Descriptor schema = protoSchema.toDescriptor();
        final Descriptor mapSchema =
                schema.getNestedTypes().stream()
                        .filter(descriptor -> descriptor.getName().equals("MapEntry"))
                        .findFirst()
                        .get();

        final RowType flinkSchema =
                new RowType(
                        false,
                        Collections.singletonList(
                                new RowField(
                                        "multiset",
                                        new MultisetType(
                                                false,
                                                new VarCharType(true, VarCharType.MAX_LENGTH)))));

        final ProtoToRowDataConverter converter =
                ProtoToRowDataConverters.createConverter(schema, flinkSchema);
        final DynamicMessage message =
                DynamicMessage.newBuilder(schema)
                        .addRepeatedField(
                                schema.findFieldByName("multiset"),
                                DynamicMessage.newBuilder(mapSchema)
                                        .setField(mapSchema.findFieldByName("key"), "ABC")
                                        .setField(mapSchema.findFieldByName("value"), 123)
                                        .build())
                        .addRepeatedField(
                                schema.findFieldByName("multiset"),
                                DynamicMessage.newBuilder(mapSchema)
                                        .setField(mapSchema.findFieldByName("key"), "DEF")
                                        .setField(mapSchema.findFieldByName("value"), 420)
                                        .build())
                        .build();
        RowData row = (RowData) converter.convert(message);

        final GenericRowData expected = new GenericRowData(1);
        final Map<BinaryStringData, Integer> expectedMap = new HashMap<>();
        expectedMap.put(BinaryStringData.fromString("ABC"), 123);
        expectedMap.put(BinaryStringData.fromString("DEF"), 420);
        expected.setField(0, new GenericMapData(expectedMap));
        assertThat(row).isEqualTo(expected);
    }

    @Test
    void testNestedRow() throws IOException {
        final String schemaStr =
                "syntax = \"proto3\";\n"
                        + "package io.confluent.protobuf.generated;\n"
                        + "\n"
                        + "message Row {\n"
                        + "  optional meta_Row meta = 1;\n"
                        + "\n"
                        + "  message meta_Row {\n"
                        + "    optional tags_Row tags = 1;\n"
                        + "  \n"
                        + "    message tags_Row {\n"
                        + "      float a = 1;\n"
                        + "      float b = 2;\n"
                        + "    }\n"
                        + "  }\n"
                        + "}\n";

        ProtobufSchema protoSchema = new ProtobufSchema(schemaStr);
        Descriptor schema = protoSchema.toDescriptor();
        final Descriptor metaSchema =
                schema.getNestedTypes().stream()
                        .filter(descriptor -> descriptor.getName().equals("meta_Row"))
                        .findFirst()
                        .get();

        final Descriptor tagsSchema =
                metaSchema.getNestedTypes().stream()
                        .filter(descriptor -> descriptor.getName().equals("tags_Row"))
                        .findFirst()
                        .get();

        final RowType flinkSchema = (RowType) ProtoToFlinkSchemaConverter.toFlinkSchema(schema);

        final ProtoToRowDataConverter converter =
                ProtoToRowDataConverters.createConverter(schema, flinkSchema);

        final DynamicMessage message =
                DynamicMessage.newBuilder(schema)
                        .setField(
                                schema.findFieldByName("meta"),
                                DynamicMessage.newBuilder(metaSchema)
                                        .setField(
                                                metaSchema.findFieldByName("tags"),
                                                DynamicMessage.newBuilder(tagsSchema)
                                                        .setField(
                                                                tagsSchema.findFieldByName("a"),
                                                                42.0F)
                                                        .setField(
                                                                tagsSchema.findFieldByName("b"),
                                                                123.0F)
                                                        .build())
                                        .build())
                        .build();
        RowData row = (RowData) converter.convert(message);
        final GenericRowData expected = new GenericRowData(1);
        final GenericRowData tags = new GenericRowData(2);
        tags.setField(0, 42.0F);
        tags.setField(1, 123.0F);
        final GenericRowData meta = new GenericRowData(1);
        meta.setField(0, tags);
        expected.setField(0, meta);

        assertThat(row).isEqualTo(expected);
    }

    @Test
    void testOneOfAndEnum() throws IOException {
        final String schemaStr =
                "syntax = \"proto3\";\n"
                        + "\n"
                        + "package foo;\n"
                        + "\n"
                        + "message Event {\n"
                        + "  Action action = 1;\n"
                        + "  Target target = 2;\n"
                        + "\n"
                        + "  message Target {\n"
                        + "    oneof payload {\n"
                        + "      string payload_id = 1;\n"
                        + "      Action action = 2;\n"
                        + "    }\n"
                        + "    enum Action {\n"
                        + "      ON  = 0;\n"
                        + "      OFF = 1;\n"
                        + "    }\n"
                        + "  }\n"
                        + "\n"
                        + "  enum Action {\n"
                        + "    ON  = 0;\n"
                        + "    OFF = 1;\n"
                        + "  }\n"
                        + "}";

        ProtobufSchema protoSchema = new ProtobufSchema(schemaStr);
        Descriptor schema = protoSchema.toDescriptor();
        final Descriptor targetSchema =
                schema.getNestedTypes().stream()
                        .filter(descriptor -> descriptor.getName().equals("Target"))
                        .findFirst()
                        .get();

        final RowType flinkSchema = (RowType) ProtoToFlinkSchemaConverter.toFlinkSchema(schema);

        final ProtoToRowDataConverter converter =
                ProtoToRowDataConverters.createConverter(schema, flinkSchema);

        final FieldDescriptor actionFieldDescriptor = schema.findFieldByName("action");
        final EnumDescriptor enumType = actionFieldDescriptor.getEnumType();
        final DynamicMessage message =
                DynamicMessage.newBuilder(schema)
                        .setField(actionFieldDescriptor, enumType.findValueByName("ON"))
                        .setField(
                                schema.findFieldByName("target"),
                                DynamicMessage.newBuilder(targetSchema)
                                        .setField(
                                                targetSchema.findFieldByName("action"),
                                                enumType.findValueByName("OFF"))
                                        .build())
                        .build();
        RowData row = (RowData) converter.convert(message);
        final GenericRowData expected = new GenericRowData(2);
        expected.setField(0, BinaryStringData.fromString("ON"));
        final GenericRowData target = new GenericRowData(1);
        final GenericRowData payload = new GenericRowData(2);
        payload.setField(1, BinaryStringData.fromString("OFF"));
        target.setField(0, payload);
        expected.setField(1, target);

        assertThat(row).isEqualTo(expected);
    }

    @Test
    void testWrappers() throws IOException {
        final String schemaStr =
                "syntax = \"proto3\";\n"
                        + "package io.confluent.protobuf.generated;\n"
                        + "\n"
                        + "import \"google/protobuf/wrappers.proto\";"
                        + "\n"
                        + "message Row {\n"
                        + "  google.protobuf.StringValue string = 1;\n"
                        + "  google.protobuf.Int32Value int = 3;\n"
                        + "  google.protobuf.Int64Value bigint = 4;\n"
                        + "  google.protobuf.FloatValue float = 5;\n"
                        + "  google.protobuf.DoubleValue double = 6;\n"
                        + "  google.protobuf.BoolValue boolean = 7;\n"
                        + "}\n";

        ProtobufSchema protoSchema = new ProtobufSchema(schemaStr);
        Descriptor schema = protoSchema.toDescriptor();

        final RowType flinkSchema = (RowType) ProtoToFlinkSchemaConverter.toFlinkSchema(schema);

        final ProtoToRowDataConverter converter =
                ProtoToRowDataConverters.createConverter(schema, flinkSchema);

        final DynamicMessage message = DynamicMessage.newBuilder(schema).build();
        RowData row = (RowData) converter.convert(message);
        final GenericRowData expected = new GenericRowData(6);

        assertThat(row).isEqualTo(expected);
    }
}
