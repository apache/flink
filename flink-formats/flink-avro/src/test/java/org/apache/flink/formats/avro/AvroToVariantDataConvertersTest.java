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

import org.apache.flink.types.variant.Variant;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AvroToVariantDataConvertersTest {

    private static final Schema TEST_SCHEMA_1 =
            new Schema.Parser()
                    .parse(
                            ""
                                    + "{\"namespace\": \"org.apache.flink.formats.avro.generated\",\n"
                                    + " \"type\": \"record\",\n"
                                    + " \"name\": \"Data\",\n"
                                    + " \"fields\": [\n"
                                    + "     {\"name\": \"a\", \"type\": \"string\"},\n"
                                    + "     {\"name\": \"b\", \"type\": \"int\"}\n"
                                    + "  ]\n"
                                    + "}");

    private static final Schema TEST_SCHEMA_2 =
            new Schema.Parser()
                    .parse(
                            ""
                                    + "{\"namespace\": \"org.apache.flink.formats.avro.generated\",\n"
                                    + " \"type\": \"record\",\n"
                                    + " \"name\": \"Timestamps\",\n"
                                    + " \"fields\": [\n"
                                    + "     {\"name\": \"tsMillis\", \"type\": {\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}},\n"
                                    + "     {\"name\": \"tsMicros\", \"type\": {\"type\": \"long\", \"logicalType\": \"timestamp-micros\"}},\n"
                                    + "     {\"name\": \"tsMicros_union\",\n"
                                    + "      \"type\" : [ \"null\", {\n"
                                    + "        \"type\" : \"long\",\n"
                                    + "        \"logicalType\" : \"timestamp-micros\"\n"
                                    + "      } ],\n"
                                    + "      \"default\": null},\n"
                                    + "     {\"name\": \"timeMillis\", \"type\": {\"type\": \"int\", \"logicalType\": \"time-millis\"}},\n"
                                    + "     {\"name\": \"date\", \"type\": {\"type\": \"int\", \"logicalType\": \"date\"}}\n"
                                    + "  ]\n"
                                    + "}");

    private static final Schema INNER_SCHEMA =
            new Schema.Parser()
                    .parse(
                            ""
                                    + "{\"namespace\": \"org.apache.flink.formats.avro.generated\",\n"
                                    + " \"type\": \"record\",\n"
                                    + " \"name\": \"InnerType\",\n"
                                    + " \"fields\": [\n"
                                    + "     {\"name\": \"id\", \"type\": \"int\"},\n"
                                    + "     {\"name\": \"key\", \"type\": \"string\"}\n"
                                    + "  ]\n"
                                    + "}");

    private static final Schema COMPLEX_UNION_SCHEMA =
            new Schema.Parser()
                    .parse(
                            ""
                                    + "{\"namespace\": \"org.apache.flink.formats.avro.generated\",\n"
                                    + " \"type\": \"record\",\n"
                                    + " \"name\": \"ComplexWithUnionType\",\n"
                                    + " \"fields\": [\n"
                                    + "     {\"name\": \"id\", \"type\": \"int\"},\n"
                                    + "     {\"name\": \"value\", \"type\": \"string\"},\n"
                                    + "     { \"name\" : \"nullableCol\", \"type\" : [ \"null\", \"string\" ], \"default\" : null },"
                                    + "     {\"name\": \"inner\", \"type\": [ \"null\", "
                                    + INNER_SCHEMA
                                    + " ], \"default\" : null }"
                                    + "  ]\n"
                                    + "}");

    private static final Schema ARRAY_SCHEMA =
            new Schema.Parser()
                    .parse(
                            "{\n"
                                    + "  \"type\": \"record\",\n"
                                    + "  \"name\": \"ArrayRecord\",\n"
                                    + "  \"namespace\": \"org.apache.flink\",\n"
                                    + "  \"fields\": [\n"
                                    + "    {\n"
                                    + "      \"name\": \"values\",\n"
                                    + "      \"type\": {\n"
                                    + "        \"type\": \"array\",\n"
                                    + "        \"items\": \"string\"\n"
                                    + "      }\n"
                                    + "    }\n"
                                    + "  ]\n"
                                    + "}");

    private static final Schema SIMPLE_TYPES_SCHEMA =
            new Schema.Parser()
                    .parse(
                            ""
                                    + "{\"namespace\": \"org.apache.flink.formats.avro.generated\",\n"
                                    + " \"type\": \"record\",\n"
                                    + " \"name\": \"SimpleTypes\",\n"
                                    + " \"fields\": [\n"
                                    + "     {\"name\": \"boolField\", \"type\": \"boolean\"},\n"
                                    + "     {\"name\": \"intField\", \"type\": \"int\"},\n"
                                    + "     {\"name\": \"longField\", \"type\": \"long\"},\n"
                                    + "     {\"name\": \"floatField\", \"type\": \"float\"},\n"
                                    + "     {\"name\": \"doubleField\", \"type\": \"double\"},\n"
                                    + "     {\"name\": \"stringField\", \"type\": \"string\"}\n"
                                    + "  ]\n"
                                    + "}");

    @Test
    public void testBasicAvroToVariantConversion() {
        var converter = AvroToVariantDataConverters.createVariantConverter(TEST_SCHEMA_1);

        GenericRecord record =
                new GenericRecordBuilder(TEST_SCHEMA_1).set("a", "hello").set("b", 42).build();

        Variant variant = converter.convert(record);

        assertTrue(variant.isObject());
        assertEquals("hello", variant.getField("a").getString());
        assertEquals(42, variant.getField("b").getInt());
    }

    @Test
    public void testSimpleTypesConversion() {
        var converter = AvroToVariantDataConverters.createVariantConverter(SIMPLE_TYPES_SCHEMA);

        GenericRecord record =
                new GenericRecordBuilder(SIMPLE_TYPES_SCHEMA)
                        .set("boolField", true)
                        .set("intField", 123)
                        .set("longField", 456L)
                        .set("floatField", 78.9f)
                        .set("doubleField", 12.34)
                        .set("stringField", "test")
                        .build();

        Variant variant = converter.convert(record);

        assertTrue(variant.isObject());
        assertTrue(variant.getField("boolField").getBoolean());
        assertEquals(123, variant.getField("intField").getInt());
        assertEquals(456L, variant.getField("longField").getLong());
        assertEquals(78.9f, variant.getField("floatField").getFloat(), 0.001f);
        assertEquals(12.34, variant.getField("doubleField").getDouble(), 0.001);
        assertEquals("test", variant.getField("stringField").getString());
    }

    @Test
    public void testTimestampAndDateLogicalTypes() {
        var converter = AvroToVariantDataConverters.createVariantConverter(TEST_SCHEMA_2);

        long currentTimeMillis = System.currentTimeMillis();
        long currentTimeMicros = currentTimeMillis * 1000 + 123;
        int currentDateDays = (int) (currentTimeMillis / (24 * 60 * 60 * 1000L));
        int timeMillis8am = (int) TimeUnit.HOURS.toMillis(8);

        long currentTimeSeconds = currentTimeMillis / 1000;
        int nanosTsMicros = (int) (currentTimeMicros - currentTimeSeconds * 1000_000) * 1000;
        int nanosTsMillis = (int) (currentTimeMillis - currentTimeSeconds * 1000) * 1000_000;

        GenericRecord record =
                new GenericRecordBuilder(TEST_SCHEMA_2)
                        .set("tsMillis", currentTimeMillis)
                        .set("tsMicros", currentTimeMicros)
                        .set("tsMicros_union", currentTimeMicros)
                        .set("timeMillis", timeMillis8am)
                        .set("date", currentDateDays)
                        .build();

        Variant variant = converter.convert(record);

        assertTrue(variant.isObject());

        // Timestamps should be converted to LocalDateTime
        assertEquals(variant.getField("tsMillis").getType(), Variant.Type.TIMESTAMP);
        assertEquals(variant.getField("tsMicros").getType(), Variant.Type.TIMESTAMP);
        assertEquals(variant.getField("tsMicros_union").getType(), Variant.Type.TIMESTAMP);
        assertEquals(
                variant.getField("tsMillis").getDateTime(),
                LocalDateTime.ofEpochSecond(currentTimeSeconds, nanosTsMillis, ZoneOffset.UTC));
        assertEquals(
                variant.getField("tsMicros").getDateTime(),
                LocalDateTime.ofEpochSecond(currentTimeSeconds, nanosTsMicros, ZoneOffset.UTC));
        assertEquals(
                variant.getField("tsMicros_union").getDateTime(),
                LocalDateTime.ofEpochSecond(currentTimeSeconds, nanosTsMicros, ZoneOffset.UTC));

        // Time-millis should be converted to long (micros)
        assertEquals(timeMillis8am * 1000L, variant.getField("timeMillis").getLong());

        // Date should be converted to LocalDate
        assertEquals(variant.getField("date").getType(), Variant.Type.DATE);
        LocalDate expectedDate = LocalDate.ofEpochDay(currentDateDays);
        assertEquals(expectedDate, variant.getField("date").getDate());
    }

    @Test
    public void testComplexRecordWithUnion() {
        var converter = AvroToVariantDataConverters.createVariantConverter(COMPLEX_UNION_SCHEMA);

        GenericData.Record inner =
                new GenericRecordBuilder(INNER_SCHEMA).set("id", 10).set("key", "test_key").build();
        GenericData.Record record =
                new GenericRecordBuilder(COMPLEX_UNION_SCHEMA)
                        .set("id", 1)
                        .set("value", "main_value")
                        .set("nullableCol", "not_null")
                        .set("inner", inner)
                        .build();

        Variant variant = converter.convert(record);

        assertTrue(variant.isObject());
        assertEquals(1, variant.getField("id").getInt());
        assertEquals("main_value", variant.getField("value").getString());
        assertEquals("not_null", variant.getField("nullableCol").getString());

        var innerVariant = variant.getField("inner");
        assertTrue(innerVariant.isObject());
        assertEquals(10, innerVariant.getField("id").getInt());
        assertEquals("test_key", innerVariant.getField("key").getString());
    }

    @Test
    public void testComplexRecordWithNullValues() {
        var converter = AvroToVariantDataConverters.createVariantConverter(COMPLEX_UNION_SCHEMA);

        GenericData.Record record =
                new GenericRecordBuilder(COMPLEX_UNION_SCHEMA)
                        .set("id", 1)
                        .set("value", "main_value")
                        .set("nullableCol", null)
                        .set("inner", null)
                        .build();

        Variant variant = converter.convert(record);

        assertTrue(variant.isObject());
        assertEquals(1, variant.getField("id").getInt());
        assertEquals("main_value", variant.getField("value").getString());
        assertTrue(variant.getField("nullableCol").isNull());
        assertTrue(variant.getField("inner").isNull());
    }

    @Test
    public void testArrayConversion() {
        var converter = AvroToVariantDataConverters.createVariantConverter(ARRAY_SCHEMA);

        List<String> stringList = new ArrayList<>();
        stringList.add("item1");
        stringList.add("item2");
        stringList.add("item3");

        GenericData.Record record =
                new GenericRecordBuilder(ARRAY_SCHEMA).set("values", stringList).build();

        Variant variant = converter.convert(record);

        assertTrue(variant.isObject());
        var arrayVariant = variant.getField("values");
        assertTrue(arrayVariant.isArray());

        assertEquals("item1", arrayVariant.getElement(0).getString());
        assertEquals("item2", arrayVariant.getElement(1).getString());
        assertEquals("item3", arrayVariant.getElement(2).getString());
    }

    @Test
    public void testNullRecord() {
        var converter = AvroToVariantDataConverters.createVariantConverter(TEST_SCHEMA_1);

        Variant variant = converter.convert(null);
        assertTrue(variant.isNull());
    }
}
