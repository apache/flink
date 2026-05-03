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

import org.apache.flink.formats.avro.AvroFormatOptions.AvroEncoding;
import org.apache.flink.formats.avro.generated.Colors;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.ByteArrayOutputStream;

import static org.apache.flink.formats.avro.utils.AvroTestUtils.createEncoder;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link AvroRowDataSerializationSchema} and {@link AvroRowDataDeserializationSchema}
 * when the Avro schema is provided upfront rather than derived from the Flink RowType.
 *
 * <p>This is the counterpart of {@link AvroRowDataDeSerializationSchemaTest} which tests the flow
 * where the Avro schema is derived from the RowType. Having a predefined Avro schema is the typical
 * case for schema-registry based formats (e.g. avro-confluent) where the Avro schema may contain
 * types (such as ENUM) that have no direct equivalent in Flink's type system.
 */
class AvroRowDataSchemaProvidedSerDeSchemaTest {

    private static final Schema COLORS_SCHEMA = Colors.getClassSchema();

    @ParameterizedTest
    @EnumSource(AvroEncoding.class)
    void testSerializeDeserializeWithPredefinedEnumSchema(AvroEncoding encoding) throws Exception {
        final Schema avroSchema =
                SchemaBuilder.record("TestRecord")
                        .namespace("org.apache.flink.formats.avro.generated")
                        .fields()
                        .requiredString("name")
                        .name("type_enum")
                        .type(COLORS_SCHEMA)
                        .noDefault()
                        .requiredInt("id")
                        .endRecord();

        final RowType rowType =
                (RowType)
                        ROW(
                                        FIELD("name", STRING()),
                                        FIELD("type_enum", STRING()),
                                        FIELD("id", INT()))
                                .notNull()
                                .getLogicalType();

        AvroRowDataSerializationSchema serializationSchema =
                createSerializationSchema(rowType, avroSchema, encoding);
        AvroRowDataDeserializationSchema deserializationSchema =
                createDeserializationSchema(rowType, avroSchema, encoding);

        GenericRowData rowData = new GenericRowData(3);
        rowData.setField(0, StringData.fromString("Alice"));
        rowData.setField(1, StringData.fromString("RED"));
        rowData.setField(2, 42);

        byte[] serialized = serializationSchema.serialize(rowData);
        RowData deserialized = deserializationSchema.deserialize(serialized);

        assertThat(deserialized.getString(0).toString()).isEqualTo("Alice");
        assertThat(deserialized.getString(1).toString()).isEqualTo("RED");
        assertThat(deserialized.getInt(2)).isEqualTo(42);
    }

    @ParameterizedTest
    @EnumSource(AvroEncoding.class)
    void testSerializeDeserializeWithNullableEnumSchema(AvroEncoding encoding) throws Exception {
        final Schema nullableColorsSchema =
                Schema.createUnion(SchemaBuilder.builder().nullType(), COLORS_SCHEMA);
        final Schema avroSchema =
                SchemaBuilder.record("TestNullableEnum")
                        .namespace("org.apache.flink.formats.avro.generated")
                        .fields()
                        .requiredString("name")
                        .name("type_enum")
                        .type(nullableColorsSchema)
                        .withDefault(null)
                        .endRecord();

        final RowType rowType =
                (RowType)
                        ROW(FIELD("name", STRING()), FIELD("type_enum", STRING()))
                                .notNull()
                                .getLogicalType();

        AvroRowDataSerializationSchema serializationSchema =
                createSerializationSchema(rowType, avroSchema, encoding);
        AvroRowDataDeserializationSchema deserializationSchema =
                createDeserializationSchema(rowType, avroSchema, encoding);

        // Test with a non-null enum value
        GenericRowData rowData = new GenericRowData(2);
        rowData.setField(0, StringData.fromString("Bob"));
        rowData.setField(1, StringData.fromString("GREEN"));

        byte[] serialized = serializationSchema.serialize(rowData);
        RowData deserialized = deserializationSchema.deserialize(serialized);

        assertThat(deserialized.getString(0).toString()).isEqualTo("Bob");
        assertThat(deserialized.getString(1).toString()).isEqualTo("GREEN");

        // Test with a null enum value
        GenericRowData rowDataWithNull = new GenericRowData(2);
        rowDataWithNull.setField(0, StringData.fromString("Charlie"));
        rowDataWithNull.setField(1, null);

        byte[] serializedNull = serializationSchema.serialize(rowDataWithNull);
        RowData deserializedNull = deserializationSchema.deserialize(serializedNull);

        assertThat(deserializedNull.getString(0).toString()).isEqualTo("Charlie");
        assertThat(deserializedNull.isNullAt(1)).isTrue();
    }

    @ParameterizedTest
    @EnumSource(AvroEncoding.class)
    void testSerializeWithInvalidEnumSymbol(AvroEncoding encoding) throws Exception {
        final Schema avroSchema =
                SchemaBuilder.record("TestRecord")
                        .namespace("org.apache.flink.formats.avro.generated")
                        .fields()
                        .name("type_enum")
                        .type(COLORS_SCHEMA)
                        .noDefault()
                        .endRecord();

        final RowType rowType =
                (RowType) ROW(FIELD("type_enum", STRING())).notNull().getLogicalType();

        AvroRowDataSerializationSchema serializationSchema =
                createSerializationSchema(rowType, avroSchema, encoding);

        GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, StringData.fromString("YELLOW"));

        assertThatThrownBy(() -> serializationSchema.serialize(rowData))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to serialize row.");
    }

    @ParameterizedTest
    @EnumSource(AvroEncoding.class)
    void testDeserializeEnumFromPredefinedSchema(AvroEncoding encoding) throws Exception {
        final Schema avroSchema =
                SchemaBuilder.record("TestRecord")
                        .namespace("org.apache.flink.formats.avro.generated")
                        .fields()
                        .name("type_enum")
                        .type(COLORS_SCHEMA)
                        .noDefault()
                        .requiredString("label")
                        .endRecord();

        final GenericRecord record = new GenericData.Record(avroSchema);
        record.put("type_enum", new GenericData.EnumSymbol(COLORS_SCHEMA, "BLUE"));
        record.put("label", "urgent");

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        GenericDatumWriter<IndexedRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
        Encoder encoder = createEncoder(encoding, avroSchema, byteArrayOutputStream);
        datumWriter.write(record, encoder);
        encoder.flush();
        byte[] input = byteArrayOutputStream.toByteArray();

        final RowType rowType =
                (RowType)
                        ROW(FIELD("type_enum", STRING()), FIELD("label", STRING()))
                                .notNull()
                                .getLogicalType();

        AvroRowDataDeserializationSchema deserializationSchema =
                createDeserializationSchema(rowType, avroSchema, encoding);

        RowData deserialized = deserializationSchema.deserialize(input);

        assertThat(deserialized.getString(0).toString()).isEqualTo("BLUE");
        assertThat(deserialized.getString(1).toString()).isEqualTo("urgent");
    }

    @ParameterizedTest
    @EnumSource(AvroEncoding.class)
    void testSerializeDeserializeRoundTripWithEnumSchema(AvroEncoding encoding) throws Exception {
        final Schema avroSchema =
                SchemaBuilder.record("TestRecord")
                        .namespace("org.apache.flink.formats.avro.generated")
                        .fields()
                        .requiredInt("id")
                        .name("type_enum")
                        .type(COLORS_SCHEMA)
                        .noDefault()
                        .endRecord();

        final RowType rowType =
                (RowType)
                        ROW(FIELD("id", INT()), FIELD("type_enum", STRING()))
                                .notNull()
                                .getLogicalType();

        AvroRowDataSerializationSchema serializationSchema =
                createSerializationSchema(rowType, avroSchema, encoding);
        AvroRowDataDeserializationSchema deserializationSchema =
                createDeserializationSchema(rowType, avroSchema, encoding);

        // Serialize from Avro GenericRecord to get reference bytes
        final GenericRecord record = new GenericData.Record(avroSchema);
        record.put("id", 7);
        record.put("type_enum", new GenericData.EnumSymbol(COLORS_SCHEMA, "GREEN"));

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        GenericDatumWriter<IndexedRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
        Encoder encoder = createEncoder(encoding, avroSchema, byteArrayOutputStream);
        datumWriter.write(record, encoder);
        encoder.flush();
        byte[] referenceBytes = byteArrayOutputStream.toByteArray();

        // Deserialize reference bytes and re-serialize
        RowData rowData = deserializationSchema.deserialize(referenceBytes);
        byte[] reserializedBytes = serializationSchema.serialize(rowData);

        assertThat(reserializedBytes).isEqualTo(referenceBytes);
    }

    private static AvroRowDataSerializationSchema createSerializationSchema(
            RowType rowType, Schema avroSchema, AvroEncoding encoding) throws Exception {
        AvroRowDataSerializationSchema serializationSchema =
                new AvroRowDataSerializationSchema(
                        rowType,
                        AvroSerializationSchema.forGeneric(avroSchema, encoding),
                        RowDataToAvroConverters.createConverter(rowType));
        serializationSchema.open(null);
        return serializationSchema;
    }

    private static AvroRowDataDeserializationSchema createDeserializationSchema(
            RowType rowType, Schema avroSchema, AvroEncoding encoding) throws Exception {
        AvroRowDataDeserializationSchema deserializationSchema =
                new AvroRowDataDeserializationSchema(
                        AvroDeserializationSchema.forGeneric(avroSchema, encoding),
                        AvroToRowDataConverters.createRowConverter(rowType),
                        InternalTypeInfo.of(rowType));
        deserializationSchema.open(null);
        return deserializationSchema;
    }
}
