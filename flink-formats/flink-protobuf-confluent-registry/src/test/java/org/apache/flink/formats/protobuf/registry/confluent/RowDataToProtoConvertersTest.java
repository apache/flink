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

import org.apache.flink.formats.protobuf.registry.confluent.RowDataToProtoConverters.RowDataToProtoConverter;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.DateTimeUtils;

import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.FloatValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.google.type.Date;
import com.google.type.TimeOfDay;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.protobuf.type.utils.DecimalUtils;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RowDataToProtoConverters}. */
public class RowDataToProtoConvertersTest {

    @Test
    void testPlainRow() {
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

        ProtobufSchema protoSchema = new ProtobufSchema(schemaStr);
        Descriptor schema = protoSchema.toDescriptor();

        final RowDataToProtoConverters.RowDataToProtoConverter converter =
                RowDataToProtoConverters.createConverter(rowType, schema);

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

        final DynamicMessage converted = (DynamicMessage) converter.convert(row);

        final FieldDescriptor decimalDescriptor = schema.findFieldByName("decimal");
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
                                        .setNanos(999_000_000)
                                        .build())
                        .setField(schema.findFieldByName("string"), "Random string")
                        .setField(
                                schema.findFieldByName("bytes"),
                                ByteString.copyFrom(new byte[] {1, 2, 3}))
                        .build();

        assertThat(converted).isEqualTo(message);
    }

    @Test
    void testWrappers() {
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
                        + "  google.protobuf.BytesValue bytes = 8;\n"
                        + "}";
        final RowType rowType =
                new RowType(
                        false,
                        Arrays.asList(
                                new RowField(
                                        "string", new VarCharType(true, VarCharType.MAX_LENGTH)),
                                new RowField("int", new IntType(true)),
                                new RowField("bigint", new BigIntType(true)),
                                new RowField("float", new FloatType(true)),
                                new RowField("double", new DoubleType(true)),
                                new RowField("boolean", new BooleanType(true)),
                                new RowField(
                                        "bytes",
                                        new VarBinaryType(true, VarBinaryType.MAX_LENGTH))));

        ProtobufSchema protoSchema = new ProtobufSchema(schemaStr);
        Descriptor schema = protoSchema.toDescriptor();

        final RowDataToProtoConverter converter =
                RowDataToProtoConverters.createConverter(rowType, schema);

        final GenericRowData row = new GenericRowData(7);
        row.setField(0, StringData.fromString("Random string"));
        row.setField(1, 42);
        row.setField(2, 42L);
        row.setField(3, 42F);
        row.setField(4, 42D);
        row.setField(5, true);
        row.setField(6, new byte[] {1, 2, 3});

        final DynamicMessage converted = (DynamicMessage) converter.convert(row);

        final DynamicMessage message =
                DynamicMessage.newBuilder(schema)
                        .setField(schema.findFieldByName("boolean"), BoolValue.of(true))
                        .setField(schema.findFieldByName("int"), Int32Value.of(42))
                        .setField(schema.findFieldByName("bigint"), Int64Value.of(42L))
                        .setField(schema.findFieldByName("double"), DoubleValue.of(42D))
                        .setField(schema.findFieldByName("float"), FloatValue.of(42F))
                        .setField(schema.findFieldByName("string"), StringValue.of("Random string"))
                        .setField(
                                schema.findFieldByName("bytes"),
                                BytesValue.of(ByteString.copyFrom(new byte[] {1, 2, 3})))
                        .build();

        assertThat(converted).isEqualTo(message);
    }

    @Test
    void testNullWrappers() {
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
                        + "  google.protobuf.BytesValue bytes = 8;\n"
                        + "}";
        final RowType rowType =
                new RowType(
                        false,
                        Arrays.asList(
                                new RowField(
                                        "string", new VarCharType(true, VarCharType.MAX_LENGTH)),
                                new RowField("int", new IntType(true)),
                                new RowField("bigint", new BigIntType(true)),
                                new RowField("float", new FloatType(true)),
                                new RowField("double", new DoubleType(true)),
                                new RowField("boolean", new BooleanType(true)),
                                new RowField(
                                        "bytes",
                                        new VarBinaryType(true, VarBinaryType.MAX_LENGTH))));

        ProtobufSchema protoSchema = new ProtobufSchema(schemaStr);
        Descriptor schema = protoSchema.toDescriptor();

        final RowDataToProtoConverter converter =
                RowDataToProtoConverters.createConverter(rowType, schema);

        final GenericRowData row = new GenericRowData(7);

        final DynamicMessage converted = (DynamicMessage) converter.convert(row);

        final DynamicMessage message = DynamicMessage.newBuilder(schema).build();

        assertThat(converted).isEqualTo(message);
    }

    @Test
    void testCollections() {
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

        final RowType rowType =
                new RowType(
                        false,
                        Arrays.asList(
                                new RowField("array", new ArrayType(false, new BigIntType(false))),
                                new RowField(
                                        "map",
                                        new MapType(
                                                false,
                                                new VarCharType(true, VarCharType.MAX_LENGTH),
                                                new BigIntType(true)))));

        ProtobufSchema protoSchema = new ProtobufSchema(schemaStr);
        Descriptor schema = protoSchema.toDescriptor();

        final RowDataToProtoConverter converter =
                RowDataToProtoConverters.createConverter(rowType, schema);

        final GenericRowData row = new GenericRowData(2);
        row.setField(0, new GenericArrayData(new Long[] {42L, 422L, 4422L}));
        final Map<BinaryStringData, Long> expectedMap = new HashMap<>();
        expectedMap.put(BinaryStringData.fromString("ABC"), 123L);
        expectedMap.put(BinaryStringData.fromString("DEF"), 420L);
        row.setField(1, new GenericMapData(expectedMap));

        final DynamicMessage converted = (DynamicMessage) converter.convert(row);

        final Descriptor mapSchema =
                schema.getNestedTypes().stream()
                        .filter(descriptor -> descriptor.getName().equals("MapEntry"))
                        .findFirst()
                        .get();
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

        assertThat(converted).isEqualTo(message);
    }

    @Test
    void testNestedRow() {
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
        final RowType rowType =
                new RowType(
                        false,
                        Collections.singletonList(
                                new RowField(
                                        "meta",
                                        new RowType(
                                                Collections.singletonList(
                                                        new RowField(
                                                                "tags",
                                                                new RowType(
                                                                        asList(
                                                                                new RowField(
                                                                                        "a",
                                                                                        new FloatType(
                                                                                                false)),
                                                                                new RowField(
                                                                                        "b",
                                                                                        new FloatType(
                                                                                                false))))))))));

        ProtobufSchema protoSchema = new ProtobufSchema(schemaStr);
        Descriptor schema = protoSchema.toDescriptor();

        final RowDataToProtoConverter converter =
                RowDataToProtoConverters.createConverter(rowType, schema);

        final GenericRowData row = new GenericRowData(1);
        final GenericRowData tags = new GenericRowData(2);
        tags.setField(0, 42.0F);
        tags.setField(1, 123.0F);
        final GenericRowData meta = new GenericRowData(1);
        meta.setField(0, tags);
        row.setField(0, meta);

        final DynamicMessage converted = (DynamicMessage) converter.convert(row);

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

        assertThat(converted).isEqualTo(message);
    }

    @Test
    void testOneOfAndEnum() {
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
        final VarCharType stringType = new VarCharType(false, VarCharType.MAX_LENGTH);
        final RowType rowType =
                new RowType(
                        false,
                        Arrays.asList(
                                new RowField("action", stringType),
                                new RowField(
                                        "target",
                                        new RowType(
                                                Collections.singletonList(
                                                        new RowField(
                                                                "payload",
                                                                new RowType(
                                                                        asList(
                                                                                new RowField(
                                                                                        "payload_id",
                                                                                        stringType),
                                                                                new RowField(
                                                                                        "action",
                                                                                        stringType)))))))));

        ProtobufSchema protoSchema = new ProtobufSchema(schemaStr);
        Descriptor schema = protoSchema.toDescriptor();

        final RowDataToProtoConverter converter =
                RowDataToProtoConverters.createConverter(rowType, schema);

        final GenericRowData row = new GenericRowData(2);
        row.setField(0, BinaryStringData.fromString("ON"));
        final GenericRowData target = new GenericRowData(1);
        final GenericRowData payload = new GenericRowData(2);
        payload.setField(1, BinaryStringData.fromString("OFF"));
        target.setField(0, payload);
        row.setField(1, target);

        final DynamicMessage converted = (DynamicMessage) converter.convert(row);

        final Descriptor targetSchema =
                schema.getNestedTypes().stream()
                        .filter(descriptor -> descriptor.getName().equals("Target"))
                        .findFirst()
                        .get();

        final FieldDescriptor actionFieldDescriptor = schema.findFieldByName("action");
        final EnumDescriptor actionEnumType = actionFieldDescriptor.getEnumType();
        final FieldDescriptor targetActionFieldDescriptor = targetSchema.findFieldByName("action");
        final EnumDescriptor targetActionEnumType = targetActionFieldDescriptor.getEnumType();
        final DynamicMessage message =
                DynamicMessage.newBuilder(schema)
                        .setField(actionFieldDescriptor, actionEnumType.findValueByName("ON"))
                        .setField(
                                schema.findFieldByName("target"),
                                DynamicMessage.newBuilder(targetSchema)
                                        .setField(
                                                targetActionFieldDescriptor,
                                                targetActionEnumType.findValueByName("OFF"))
                                        .build())
                        .build();

        assertThat(converted).isEqualTo(message);
    }
}
