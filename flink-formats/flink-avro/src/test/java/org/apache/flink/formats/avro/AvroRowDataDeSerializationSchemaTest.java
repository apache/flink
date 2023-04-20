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

package org.apache.flink.formats.avro;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.generated.LogicalTimeRecord;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.formats.avro.utils.AvroTestUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.InstantiationUtil;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.BYTES;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.SMALLINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TINYINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for the Avro serialization and deserialization schema. */
class AvroRowDataDeSerializationSchemaTest {

    @Test
    void testDeserializeNullRow() throws Exception {
        final DataType dataType = ROW(FIELD("bool", BOOLEAN())).nullable();
        AvroRowDataDeserializationSchema deserializationSchema =
                createDeserializationSchema(dataType);

        assertThat(deserializationSchema.deserialize(null)).isNull();
    }

    @Test
    void testSerializeDeserialize() throws Exception {
        final DataType dataType =
                ROW(
                                FIELD("bool", BOOLEAN()),
                                FIELD("tinyint", TINYINT()),
                                FIELD("smallint", SMALLINT()),
                                FIELD("int", INT()),
                                FIELD("bigint", BIGINT()),
                                FIELD("float", FLOAT()),
                                FIELD("double", DOUBLE()),
                                FIELD("name", STRING()),
                                FIELD("bytes", BYTES()),
                                FIELD("decimal", DECIMAL(19, 6)),
                                FIELD("doubles", ARRAY(DOUBLE())),
                                FIELD("time", TIME(0)),
                                FIELD("date", DATE()),
                                FIELD("timestamp3", TIMESTAMP(3)),
                                FIELD("timestamp3_2", TIMESTAMP(3)),
                                FIELD("map", MAP(STRING(), BIGINT())),
                                FIELD("map2map", MAP(STRING(), MAP(STRING(), INT()))),
                                FIELD("map2array", MAP(STRING(), ARRAY(INT()))),
                                FIELD("nullEntryMap", MAP(STRING(), STRING())))
                        .notNull();
        final RowType rowType = (RowType) dataType.getLogicalType();

        final Schema schema = AvroSchemaConverter.convertToSchema(rowType);
        final GenericRecord record = new GenericData.Record(schema);
        record.put(0, true);
        record.put(1, (int) Byte.MAX_VALUE);
        record.put(2, (int) Short.MAX_VALUE);
        record.put(3, 33);
        record.put(4, 44L);
        record.put(5, 12.34F);
        record.put(6, 23.45);
        record.put(7, "hello avro");
        record.put(8, ByteBuffer.wrap(new byte[] {1, 2, 4, 5, 6, 7, 8, 12}));

        record.put(
                9, ByteBuffer.wrap(BigDecimal.valueOf(123456789, 6).unscaledValue().toByteArray()));

        List<Double> doubles = new ArrayList<>();
        doubles.add(1.2);
        doubles.add(3.4);
        doubles.add(567.8901);
        record.put(10, doubles);

        record.put(11, 18397);
        record.put(12, 10087);
        record.put(13, 1589530213123L);
        record.put(14, 1589530213122L);

        Map<String, Long> map = new HashMap<>();
        map.put("flink", 12L);
        map.put("avro", 23L);
        record.put(15, map);

        Map<String, Map<String, Integer>> map2map = new HashMap<>();
        Map<String, Integer> innerMap = new HashMap<>();
        innerMap.put("inner_key1", 123);
        innerMap.put("inner_key2", 234);
        map2map.put("outer_key", innerMap);
        record.put(16, map2map);

        List<Integer> list1 = Arrays.asList(1, 2, 3, 4, 5, 6);
        List<Integer> list2 = Arrays.asList(11, 22, 33, 44, 55);
        Map<String, List<Integer>> map2list = new HashMap<>();
        map2list.put("list1", list1);
        map2list.put("list2", list2);
        record.put(17, map2list);

        Map<String, String> map2 = new HashMap<>();
        map2.put("key1", null);
        record.put(18, map2);

        AvroRowDataSerializationSchema serializationSchema = createSerializationSchema(dataType);
        AvroRowDataDeserializationSchema deserializationSchema =
                createDeserializationSchema(dataType);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        GenericDatumWriter<IndexedRecord> datumWriter = new GenericDatumWriter<>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
        datumWriter.write(record, encoder);
        encoder.flush();
        byte[] input = byteArrayOutputStream.toByteArray();

        RowData rowData = deserializationSchema.deserialize(input);
        byte[] output = serializationSchema.serialize(rowData);

        assertThat(output).isEqualTo(input);
    }

    @Test
    void testSerializeDeserializeBasedOnNestedSchema() throws Exception {
        final Schema innerSchema = AvroTestUtils.getSmallSchema();
        final DataType dataType = AvroSchemaConverter.convertToDataType(innerSchema.toString());

        SchemaBuilder.FieldAssembler<Schema> outerSchemaBuilder =
                SchemaBuilder.builder().record("outerSchemaName").fields();
        outerSchemaBuilder
                .name("before")
                .type(Schema.createUnion(SchemaBuilder.builder().nullType(), innerSchema))
                .withDefault(null);
        outerSchemaBuilder
                .name("after")
                .type(Schema.createUnion(SchemaBuilder.builder().nullType(), innerSchema))
                .withDefault(null);
        outerSchemaBuilder
                .name("op")
                .type(
                        Schema.createUnion(
                                SchemaBuilder.builder().nullType(),
                                SchemaBuilder.builder().stringType()))
                .withDefault(null);

        Schema outerSchema = outerSchemaBuilder.endRecord();
        final Schema nullableOuterSchema =
                Schema.createUnion(SchemaBuilder.builder().nullType(), outerSchema);

        final GenericRecord innerRecord = new GenericData.Record(innerSchema);
        innerRecord.put(0, "test");
        final GenericRecord outerRecord = new GenericData.Record(outerSchema);
        outerRecord.put(0, null);
        outerRecord.put(1, innerRecord);
        outerRecord.put(2, "c");

        RowType rowType =
                (RowType)
                        ROW(
                                        FIELD("before", dataType.nullable()),
                                        FIELD("after", dataType.nullable()),
                                        FIELD("op", STRING()))
                                .getLogicalType();

        AvroRowDataSerializationSchema serializationSchema =
                new AvroRowDataSerializationSchema(
                        rowType,
                        AvroSerializationSchema.forGeneric(nullableOuterSchema),
                        RowDataToAvroConverters.createConverter(rowType));

        AvroRowDataDeserializationSchema deserializationSchema =
                new AvroRowDataDeserializationSchema(
                        AvroDeserializationSchema.forGeneric(nullableOuterSchema),
                        AvroToRowDataConverters.createRowConverter(rowType),
                        InternalTypeInfo.of(rowType));

        final byte[] serBytes = InstantiationUtil.serializeObject(serializationSchema);
        final byte[] deserBytes = InstantiationUtil.serializeObject(deserializationSchema);

        final AvroRowDataSerializationSchema serCopy =
                InstantiationUtil.deserializeObject(
                        serBytes, Thread.currentThread().getContextClassLoader());
        final AvroRowDataDeserializationSchema deserCopy =
                InstantiationUtil.deserializeObject(
                        deserBytes, Thread.currentThread().getContextClassLoader());

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        GenericDatumWriter<IndexedRecord> datumWriter =
                new GenericDatumWriter<>(nullableOuterSchema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
        datumWriter.write(outerRecord, encoder);
        encoder.flush();
        byte[] input = byteArrayOutputStream.toByteArray();

        RowData rowData = deserCopy.deserialize(input);

        serCopy.open(null);
        byte[] output = serCopy.serialize(rowData);

        assertThat(output).isEqualTo(input);
    }

    @Test
    void testSpecificType() throws Exception {
        LogicalTimeRecord record = new LogicalTimeRecord();
        Instant timestamp = Instant.parse("2010-06-30T01:20:20Z");
        record.setTypeTimestampMillis(timestamp);
        record.setTypeDate(LocalDate.parse("2014-03-01"));
        record.setTypeTimeMillis(LocalTime.parse("12:12:12"));
        SpecificDatumWriter<LogicalTimeRecord> datumWriter =
                new SpecificDatumWriter<>(LogicalTimeRecord.class);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
        datumWriter.write(record, encoder);
        encoder.flush();
        byte[] input = byteArrayOutputStream.toByteArray();

        DataType dataType =
                ROW(
                                FIELD("type_timestamp_millis", TIMESTAMP(3).notNull()),
                                FIELD("type_date", DATE().notNull()),
                                FIELD("type_time_millis", TIME(3).notNull()))
                        .notNull();
        AvroRowDataSerializationSchema serializationSchema = createSerializationSchema(dataType);
        AvroRowDataDeserializationSchema deserializationSchema =
                createDeserializationSchema(dataType);

        RowData rowData = deserializationSchema.deserialize(input);
        byte[] output = serializationSchema.serialize(rowData);
        RowData rowData2 = deserializationSchema.deserialize(output);
        assertThat(rowData2).isEqualTo(rowData);
        assertThat(rowData.getTimestamp(0, 3).toInstant()).isEqualTo(timestamp);

        assertThat(
                        DataFormatConverters.LocalDateConverter.INSTANCE
                                .toExternal(rowData.getInt(1))
                                .toString())
                .isEqualTo("2014-03-01");
        assertThat(
                        DataFormatConverters.LocalTimeConverter.INSTANCE
                                .toExternal(rowData.getInt(2))
                                .toString())
                .isEqualTo("12:12:12");
    }

    @Test
    void testSerializationWithTypesMismatch() throws Exception {
        AvroRowDataSerializationSchema serializationSchema =
                createSerializationSchema(ROW(FIELD("f0", INT()), FIELD("f1", STRING())).notNull());
        GenericRowData rowData = new GenericRowData(2);
        rowData.setField(0, 1);
        rowData.setField(1, 2); // This should be a STRING

        assertThatThrownBy(() -> serializationSchema.serialize(rowData))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to serialize row.")
                .hasStackTraceContaining("Fail to serialize at field: f1");
    }

    private AvroRowDataSerializationSchema createSerializationSchema(DataType dataType)
            throws Exception {
        final RowType rowType = (RowType) dataType.getLogicalType();

        AvroRowDataSerializationSchema serializationSchema =
                new AvroRowDataSerializationSchema(rowType);
        serializationSchema.open(null);
        return serializationSchema;
    }

    private AvroRowDataDeserializationSchema createDeserializationSchema(DataType dataType)
            throws Exception {
        final RowType rowType = (RowType) dataType.getLogicalType();
        final TypeInformation<RowData> typeInfo = InternalTypeInfo.of(rowType);

        AvroRowDataDeserializationSchema deserializationSchema =
                new AvroRowDataDeserializationSchema(rowType, typeInfo);
        deserializationSchema.open(null);
        return deserializationSchema;
    }
}
