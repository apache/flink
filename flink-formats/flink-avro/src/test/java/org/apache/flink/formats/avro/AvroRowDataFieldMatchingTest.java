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

package org.apache.flink.formats.avro;

import org.apache.flink.formats.avro.AvroFormatOptions.AvroEncoding;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.InstantiationUtil;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.formats.avro.utils.AvroTestUtils.createEncoder;
import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link FieldMatching#NAME}, which pairs the fields of a {@link RowType} with the fields
 * of an Avro record schema by name rather than by position.
 *
 * <p>All of these need an Avro schema that is supplied independently of the row type: a schema
 * derived from the row type by {@link org.apache.flink.formats.avro.typeutils.AvroSchemaConverter}
 * always agrees on field order, so there is nothing for name matching to fix.
 */
class AvroRowDataFieldMatchingTest {

    @ParameterizedTest
    @EnumSource(AvroEncoding.class)
    void testFieldOrderMismatch(AvroEncoding encoding) throws Exception {
        final Schema avroSchema =
                record(
                        "{'name':'c','type':'boolean'}",
                        "{'name':'a','type':'string'}",
                        "{'name':'b','type':'int'}");
        final RowType rowType =
                rowType(
                        FIELD("a", STRING().notNull()),
                        FIELD("b", INT().notNull()),
                        FIELD("c", BOOLEAN().notNull()));

        final byte[] serialized =
                serializer(rowType, avroSchema, encoding, FieldMatching.NAME)
                        .serialize(GenericRowData.of(StringData.fromString("hello"), 42, true));

        // Byte-for-byte identical to what an Avro writer produces for that schema.
        final GenericRecord expected = new GenericData.Record(avroSchema);
        expected.put("a", "hello");
        expected.put("b", 42);
        expected.put("c", true);
        assertThat(serialized).isEqualTo(encode(expected, avroSchema, encoding));

        final RowData roundTripped =
                deserializer(rowType, avroSchema, encoding, FieldMatching.NAME)
                        .deserialize(serialized);
        assertThat(roundTripped.getString(0).toString()).isEqualTo("hello");
        assertThat(roundTripped.getInt(1)).isEqualTo(42);
        assertThat(roundTripped.getBoolean(2)).isTrue();
    }

    @ParameterizedTest
    @EnumSource(AvroEncoding.class)
    void testIndexMatchingIgnoresFieldNames(AvroEncoding encoding) throws Exception {
        // Spells out the behaviour FieldMatching.NAME exists to avoid: with index matching, a
        // reordered schema of same-typed fields silently swaps the values.
        final Schema avroSchema =
                record("{'name':'b','type':'string'}", "{'name':'a','type':'string'}");
        final RowType rowType =
                rowType(FIELD("a", STRING().notNull()), FIELD("b", STRING().notNull()));
        final GenericRowData row =
                GenericRowData.of(
                        StringData.fromString("valueOfA"), StringData.fromString("valueOfB"));

        final GenericRecord viaIndex =
                decode(
                        serializer(rowType, avroSchema, encoding, FieldMatching.INDEX)
                                .serialize(row),
                        avroSchema,
                        encoding);
        assertThat(viaIndex.get("b").toString()).isEqualTo("valueOfA");
        assertThat(viaIndex.get("a").toString()).isEqualTo("valueOfB");

        final GenericRecord viaName =
                decode(
                        serializer(rowType, avroSchema, encoding, FieldMatching.NAME)
                                .serialize(row),
                        avroSchema,
                        encoding);
        assertThat(viaName.get("a").toString()).isEqualTo("valueOfA");
        assertThat(viaName.get("b").toString()).isEqualTo("valueOfB");
    }

    @ParameterizedTest
    @EnumSource(AvroEncoding.class)
    void testCaseInsensitiveFieldMatching(AvroEncoding encoding) throws Exception {
        final Schema avroSchema =
                record(
                        "{'name':'LASTNAME','type':'string'}",
                        "{'name':'firstname','type':'string'}");
        final RowType rowType =
                rowType(
                        FIELD("firstName", STRING().notNull()),
                        FIELD("lastName", STRING().notNull()));

        final GenericRecord written =
                decode(
                        serializer(rowType, avroSchema, encoding, FieldMatching.NAME)
                                .serialize(
                                        GenericRowData.of(
                                                StringData.fromString("Ada"),
                                                StringData.fromString("Lovelace"))),
                        avroSchema,
                        encoding);

        assertThat(written.get("firstname").toString()).isEqualTo("Ada");
        assertThat(written.get("LASTNAME").toString()).isEqualTo("Lovelace");
    }

    @ParameterizedTest
    @EnumSource(AvroEncoding.class)
    void testAvroFieldAliasMatching(AvroEncoding encoding) throws Exception {
        final Schema avroSchema = record("{'name':'full_name','aliases':['name'],'type':'string'}");
        final RowType rowType = rowType(FIELD("name", STRING().notNull()));

        final GenericRecord written =
                decode(
                        serializer(rowType, avroSchema, encoding, FieldMatching.NAME)
                                .serialize(GenericRowData.of(StringData.fromString("Grace"))),
                        avroSchema,
                        encoding);

        assertThat(written.get("full_name").toString()).isEqualTo("Grace");
    }

    @ParameterizedTest
    @EnumSource(AvroEncoding.class)
    void testFieldOrderMismatchInNestedRow(AvroEncoding encoding) throws Exception {
        final Schema avroSchema =
                record(
                        "{'name':'nested','type':{'type':'record','name':'Nested','fields':["
                                + "{'name':'y','type':'int'},{'name':'x','type':'string'}]}}",
                        "{'name':'id','type':'int'}");
        final RowType rowType =
                rowType(
                        FIELD("id", INT().notNull()),
                        FIELD(
                                "nested",
                                ROW(FIELD("x", STRING().notNull()), FIELD("y", INT().notNull()))
                                        .notNull()));

        final GenericRowData row =
                GenericRowData.of(1, GenericRowData.of(StringData.fromString("deep"), 9));
        final byte[] serialized =
                serializer(rowType, avroSchema, encoding, FieldMatching.NAME).serialize(row);

        final GenericRecord written = decode(serialized, avroSchema, encoding);
        assertThat(written.get("id")).isEqualTo(1);
        final GenericRecord writtenNested = (GenericRecord) written.get("nested");
        assertThat(writtenNested.get("x").toString()).isEqualTo("deep");
        assertThat(writtenNested.get("y")).isEqualTo(9);

        final RowData roundTripped =
                deserializer(rowType, avroSchema, encoding, FieldMatching.NAME)
                        .deserialize(serialized);
        assertThat(roundTripped.getInt(0)).isEqualTo(1);
        assertThat(roundTripped.getRow(1, 2).getString(0).toString()).isEqualTo("deep");
        assertThat(roundTripped.getRow(1, 2).getInt(1)).isEqualTo(9);
    }

    @ParameterizedTest
    @EnumSource(AvroEncoding.class)
    void testFieldOrderMismatchInArrayElement(AvroEncoding encoding) throws Exception {
        final Schema avroSchema =
                record(
                        "{'name':'items','type':{'type':'array','items':"
                                + "{'type':'record','name':'Item','fields':["
                                + "{'name':'quantity','type':'int'},{'name':'sku','type':'string'}]}}}");
        final RowType rowType =
                rowType(
                        FIELD(
                                "items",
                                ARRAY(
                                                ROW(
                                                                FIELD("sku", STRING().notNull()),
                                                                FIELD("quantity", INT().notNull()))
                                                        .notNull())
                                        .notNull()));

        final GenericRowData row =
                GenericRowData.of(
                        new GenericArrayData(
                                new Object[] {
                                    GenericRowData.of(StringData.fromString("A-1"), 3),
                                    GenericRowData.of(StringData.fromString("B-2"), 5)
                                }));
        final byte[] serialized =
                serializer(rowType, avroSchema, encoding, FieldMatching.NAME).serialize(row);

        @SuppressWarnings("unchecked")
        final List<GenericRecord> items =
                (List<GenericRecord>) decode(serialized, avroSchema, encoding).get("items");
        assertThat(items).hasSize(2);
        assertThat(items.get(0).get("sku").toString()).isEqualTo("A-1");
        assertThat(items.get(0).get("quantity")).isEqualTo(3);
        assertThat(items.get(1).get("sku").toString()).isEqualTo("B-2");

        final RowData roundTripped =
                deserializer(rowType, avroSchema, encoding, FieldMatching.NAME)
                        .deserialize(serialized);
        assertThat(roundTripped.getArray(0).getRow(0, 2).getString(0).toString()).isEqualTo("A-1");
        assertThat(roundTripped.getArray(0).getRow(1, 2).getInt(1)).isEqualTo(5);
    }

    @ParameterizedTest
    @EnumSource(AvroEncoding.class)
    void testFieldOrderMismatchInMapValue(AvroEncoding encoding) throws Exception {
        final Schema avroSchema =
                record(
                        "{'name':'byKey','type':{'type':'map','values':"
                                + "{'type':'record','name':'Value','fields':["
                                + "{'name':'count','type':'int'},{'name':'label','type':'string'}]}}}");
        final RowType rowType =
                rowType(
                        FIELD(
                                "byKey",
                                MAP(
                                                STRING().notNull(),
                                                ROW(
                                                                FIELD("label", STRING().notNull()),
                                                                FIELD("count", INT().notNull()))
                                                        .notNull())
                                        .notNull()));

        final GenericRowData row =
                GenericRowData.of(
                        new GenericMapData(
                                Collections.singletonMap(
                                        StringData.fromString("k"),
                                        GenericRowData.of(StringData.fromString("hits"), 11))));
        final byte[] serialized =
                serializer(rowType, avroSchema, encoding, FieldMatching.NAME).serialize(row);

        @SuppressWarnings("unchecked")
        final Map<?, GenericRecord> byKey =
                (Map<?, GenericRecord>) decode(serialized, avroSchema, encoding).get("byKey");
        assertThat(byKey).hasSize(1);
        final GenericRecord value = byKey.values().iterator().next();
        assertThat(value.get("label").toString()).isEqualTo("hits");
        assertThat(value.get("count")).isEqualTo(11);
    }

    @ParameterizedTest
    @EnumSource(AvroEncoding.class)
    void testEnumWithReorderedFields(AvroEncoding encoding) throws Exception {
        final Schema avroSchema =
                record(
                        "{'name':'color','type':{'type':'enum','name':'Color','symbols':['RED','GREEN']}}",
                        "{'name':'name','type':'string'}");
        final RowType rowType =
                rowType(FIELD("name", STRING().notNull()), FIELD("color", STRING().notNull()));

        final byte[] serialized =
                serializer(rowType, avroSchema, encoding, FieldMatching.NAME)
                        .serialize(
                                GenericRowData.of(
                                        StringData.fromString("Alice"),
                                        StringData.fromString("GREEN")));

        final GenericRecord written = decode(serialized, avroSchema, encoding);
        assertThat(written.get("name").toString()).isEqualTo("Alice");
        assertThat(written.get("color").toString()).isEqualTo("GREEN");

        final RowData roundTripped =
                deserializer(rowType, avroSchema, encoding, FieldMatching.NAME)
                        .deserialize(serialized);
        assertThat(roundTripped.getString(0).toString()).isEqualTo("Alice");
        assertThat(roundTripped.getString(1).toString()).isEqualTo("GREEN");
    }

    @ParameterizedTest
    @EnumSource(AvroEncoding.class)
    void testUnmatchedAvroFieldFallsBackToItsDefault(AvroEncoding encoding) throws Exception {
        final Schema avroSchema =
                record(
                        "{'name':'a','type':'string'}",
                        "{'name':'version','type':'int','default':7}");
        final RowType rowType = rowType(FIELD("a", STRING().notNull()));

        final GenericRecord written =
                decode(
                        serializer(rowType, avroSchema, encoding, FieldMatching.NAME)
                                .serialize(GenericRowData.of(StringData.fromString("only"))),
                        avroSchema,
                        encoding);

        assertThat(written.get("a").toString()).isEqualTo("only");
        assertThat(written.get("version")).isEqualTo(7);
    }

    @ParameterizedTest
    @EnumSource(AvroEncoding.class)
    void testColumnAbsentFromTheAvroSchemaIsReadAsNull(AvroEncoding encoding) throws Exception {
        final Schema avroSchema = record("{'name':'a','type':'string'}");
        final RowType rowType = rowType(FIELD("a", STRING()), FIELD("b", INT()));

        final GenericRecord record = new GenericData.Record(avroSchema);
        record.put("a", "present");

        final RowData roundTripped =
                deserializer(rowType, avroSchema, encoding, FieldMatching.NAME)
                        .deserialize(encode(record, avroSchema, encoding));

        assertThat(roundTripped.getString(0).toString()).isEqualTo("present");
        assertThat(roundTripped.isNullAt(1)).isTrue();
    }

    @ParameterizedTest
    @EnumSource(AvroEncoding.class)
    void testColumnAbsentFromTheAvroSchemaCannotBeWritten(AvroEncoding encoding) throws Exception {
        final Schema avroSchema = record("{'name':'a','type':'string'}");
        final RowType rowType =
                rowType(FIELD("a", STRING().notNull()), FIELD("ghost", INT().notNull()));

        final AvroRowDataSerializationSchema serializer =
                serializer(rowType, avroSchema, encoding, FieldMatching.NAME);

        assertThatThrownBy(
                        () ->
                                serializer.serialize(
                                        GenericRowData.of(StringData.fromString("a"), 1)))
                .isInstanceOf(RuntimeException.class)
                .hasStackTraceContaining("Column 'ghost' cannot be written");
    }

    @ParameterizedTest
    @EnumSource(AvroEncoding.class)
    void testConvertersSurviveJavaSerialization(AvroEncoding encoding) throws Exception {
        // The resolved pairing is transient, so a converter has to be able to rebuild it after
        // being shipped to a task.
        final Schema avroSchema =
                record("{'name':'b','type':'int'}", "{'name':'a','type':'string'}");
        final RowType rowType =
                rowType(FIELD("a", STRING().notNull()), FIELD("b", INT().notNull()));

        final AvroRowDataSerializationSchema serializer =
                roundTripThroughJavaSerialization(
                        serializer(rowType, avroSchema, encoding, FieldMatching.NAME));
        final AvroRowDataDeserializationSchema deserializer =
                roundTripThroughJavaSerialization(
                        deserializer(rowType, avroSchema, encoding, FieldMatching.NAME));
        serializer.open(null);
        deserializer.open(null);

        final RowData roundTripped =
                deserializer.deserialize(
                        serializer.serialize(GenericRowData.of(StringData.fromString("x"), 1)));

        assertThat(roundTripped.getString(0).toString()).isEqualTo("x");
        assertThat(roundTripped.getInt(1)).isEqualTo(1);
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    private static AvroRowDataSerializationSchema serializer(
            RowType rowType, Schema avroSchema, AvroEncoding encoding, FieldMatching fieldMatching)
            throws Exception {
        final AvroRowDataSerializationSchema serializationSchema =
                new AvroRowDataSerializationSchema(
                        rowType,
                        AvroSerializationSchema.forGeneric(avroSchema, encoding),
                        RowDataToAvroConverters.createConverter(rowType, true, fieldMatching));
        serializationSchema.open(null);
        return serializationSchema;
    }

    private static AvroRowDataDeserializationSchema deserializer(
            RowType rowType, Schema avroSchema, AvroEncoding encoding, FieldMatching fieldMatching)
            throws Exception {
        final AvroRowDataDeserializationSchema deserializationSchema =
                new AvroRowDataDeserializationSchema(
                        AvroDeserializationSchema.forGeneric(avroSchema, encoding),
                        AvroToRowDataConverters.createRowConverter(
                                avroSchema, rowType, fieldMatching),
                        InternalTypeInfo.of(rowType));
        deserializationSchema.open(null);
        return deserializationSchema;
    }

    private static <T> T roundTripThroughJavaSerialization(T object) throws Exception {
        return InstantiationUtil.deserializeObject(
                InstantiationUtil.serializeObject(object),
                Thread.currentThread().getContextClassLoader());
    }

    private static byte[] encode(GenericRecord record, Schema schema, AvroEncoding encoding)
            throws Exception {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final Encoder encoder = createEncoder(encoding, schema, out);
        new GenericDatumWriter<IndexedRecord>(schema).write(record, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    private static GenericRecord decode(byte[] bytes, Schema schema, AvroEncoding encoding)
            throws Exception {
        final AvroDeserializationSchema<GenericRecord> deserializationSchema =
                AvroDeserializationSchema.forGeneric(schema, encoding);
        deserializationSchema.open(null);
        return deserializationSchema.deserialize(bytes);
    }

    private static RowType rowType(org.apache.flink.table.api.DataTypes.Field... fields) {
        return (RowType) ROW(fields).notNull().getLogicalType();
    }

    /** Builds a record schema from the given field declarations, using {@code '} for {@code "}. */
    private static Schema record(String... fields) {
        return new Schema.Parser()
                .parse(
                        ("{'type':'record','name':'TestRecord','fields':["
                                        + String.join(",", fields)
                                        + "]}")
                                .replace('\'', '"'));
    }
}
