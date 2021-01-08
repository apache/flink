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

package org.apache.flink.formats.csv;

import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;

import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalTime;
import java.util.function.Consumer;

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
import static org.apache.flink.table.api.DataTypes.RAW;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.SMALLINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TINYINT;
import static org.apache.flink.table.data.StringData.fromString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/** Tests for {@link CsvRowDataDeserializationSchema} and {@link CsvRowDataSerializationSchema}. */
public class CsvRowDataSerDeSchemaTest {

    @Test
    public void testSerializeDeserialize() throws Exception {
        testNullableField(BIGINT(), "null", null);
        testNullableField(STRING(), "null", null);
        testNullableField(STRING(), "\"This is a test.\"", "This is a test.");
        testNullableField(STRING(), "\"This is a test\n\r.\"", "This is a test\n\r.");
        testNullableField(BOOLEAN(), "true", true);
        testNullableField(BOOLEAN(), "null", null);
        testNullableField(TINYINT(), "124", (byte) 124);
        testNullableField(SMALLINT(), "10000", (short) 10000);
        testNullableField(INT(), "1234567", 1234567);
        testNullableField(BIGINT(), "12345678910", 12345678910L);
        testNullableField(FLOAT(), "0.33333334", 0.33333334f);
        testNullableField(DOUBLE(), "0.33333333332", 0.33333333332d);
        testNullableField(
                DECIMAL(38, 25),
                "\"1234.0000000000000000000000001\"",
                new BigDecimal("1234.0000000000000000000000001"));
        testNullableField(
                DECIMAL(38, 0),
                "\"123400000000000000000000000001\"",
                new BigDecimal("123400000000000000000000000001"));
        testNullableField(DATE(), "2018-10-12", Date.valueOf("2018-10-12"));
        testNullableField(TIME(0), "12:12:12", Time.valueOf("12:12:12"));
        testNullableField(
                TIMESTAMP(0), "\"2018-10-12 12:12:12\"", Timestamp.valueOf("2018-10-12 12:12:12"));
        testNullableField(
                ROW(FIELD("f0", STRING()), FIELD("f1", INT()), FIELD("f2", BOOLEAN())),
                "Hello;42;false",
                Row.of("Hello", 42, false));
        testNullableField(ARRAY(STRING()), "a;b;c", new String[] {"a", "b", "c"});
        testNullableField(ARRAY(TINYINT()), "12;4;null", new Byte[] {12, 4, null});
        testNullableField(BYTES(), "awML", new byte[] {107, 3, 11});
        testNullableField(TIME(3), "12:12:12.232", LocalTime.parse("12:12:12.232"));
        testNullableField(TIME(2), "12:12:12.23", LocalTime.parse("12:12:12.23"));
        testNullableField(TIME(1), "12:12:12.2", LocalTime.parse("12:12:12.2"));
        testNullableField(TIME(0), "12:12:12", LocalTime.parse("12:12:12"));
    }

    @Test
    public void testSerializeDeserializeCustomizedProperties() throws Exception {

        Consumer<CsvRowDataSerializationSchema.Builder> serConfig =
                (serSchemaBuilder) ->
                        serSchemaBuilder
                                .setEscapeCharacter('*')
                                .setQuoteCharacter('\'')
                                .setArrayElementDelimiter(":")
                                .setFieldDelimiter(';');

        Consumer<CsvRowDataDeserializationSchema.Builder> deserConfig =
                (deserSchemaBuilder) ->
                        deserSchemaBuilder
                                .setEscapeCharacter('*')
                                .setQuoteCharacter('\'')
                                .setArrayElementDelimiter(":")
                                .setFieldDelimiter(';');

        testFieldDeserialization(STRING(), "123*'4**", "123'4*", deserConfig, ";");
        testField(STRING(), "'123''4**'", "'123''4**'", serConfig, deserConfig, ";");
        testFieldDeserialization(STRING(), "'a;b*'c'", "a;b'c", deserConfig, ";");
        testField(STRING(), "'a;b''c'", "a;b'c", serConfig, deserConfig, ";");
        testFieldDeserialization(INT(), "       12          ", 12, deserConfig, ";");
        testField(INT(), "12", 12, serConfig, deserConfig, ";");
        testFieldDeserialization(
                ROW(FIELD("f0", STRING()), FIELD("f1", STRING())),
                "1:hello",
                Row.of("1", "hello"),
                deserConfig,
                ";");
        testField(
                ROW(FIELD("f0", STRING()), FIELD("f1", STRING())),
                "'1:hello'",
                Row.of("1", "hello"),
                serConfig,
                deserConfig,
                ";");
        testField(
                ROW(FIELD("f0", STRING()), FIELD("f1", STRING())),
                "'1:hello world'",
                Row.of("1", "hello world"),
                serConfig,
                deserConfig,
                ";");
        testField(
                STRING(),
                "null",
                "null",
                serConfig,
                deserConfig,
                ";"); // string because null literal has not been set
        testFieldDeserialization(
                TIME(3), "12:12:12.232", LocalTime.parse("12:12:12.232"), deserConfig, ";");
        testFieldDeserialization(
                TIME(3), "12:12:12.232342", LocalTime.parse("12:12:12.232"), deserConfig, ";");
        testFieldDeserialization(
                TIME(3), "12:12:12.23", LocalTime.parse("12:12:12.23"), deserConfig, ";");
        testFieldDeserialization(
                TIME(2), "12:12:12.23", LocalTime.parse("12:12:12.23"), deserConfig, ";");
        testFieldDeserialization(
                TIME(2), "12:12:12.232312", LocalTime.parse("12:12:12.23"), deserConfig, ";");
        testFieldDeserialization(
                TIME(2), "12:12:12.2", LocalTime.parse("12:12:12.2"), deserConfig, ";");
        testFieldDeserialization(
                TIME(1), "12:12:12.2", LocalTime.parse("12:12:12.2"), deserConfig, ";");
        testFieldDeserialization(
                TIME(1), "12:12:12.2235", LocalTime.parse("12:12:12.2"), deserConfig, ";");
        testFieldDeserialization(
                TIME(1), "12:12:12", LocalTime.parse("12:12:12"), deserConfig, ";");
        testFieldDeserialization(
                TIME(0), "12:12:12", LocalTime.parse("12:12:12"), deserConfig, ";");
        testFieldDeserialization(
                TIME(0), "12:12:12.45", LocalTime.parse("12:12:12"), deserConfig, ";");
        int precision = 5;
        try {
            testFieldDeserialization(
                    TIME(5), "12:12:12.45", LocalTime.parse("12:12:12"), deserConfig, ";");
            fail();
        } catch (Exception e) {
            assertEquals(
                    "Csv does not support TIME type with precision: 5, it only supports precision 0 ~ 3.",
                    e.getMessage());
        }
    }

    @Test
    public void testDeserializeParseError() throws Exception {
        try {
            testDeserialization(false, false, "Test,null,Test"); // null not supported
            fail("Missing field should cause failure.");
        } catch (IOException e) {
            // valid exception
        }
    }

    @Test
    public void testDeserializeUnsupportedNull() throws Exception {
        // unsupported null for integer
        assertEquals(
                Row.of("Test", null, "Test"), testDeserialization(true, false, "Test,null,Test"));
    }

    @Test
    public void testDeserializeNullRow() throws Exception {
        // return null for null input
        assertNull(testDeserialization(false, false, null));
    }

    @Test
    public void testDeserializeIncompleteRow() throws Exception {
        // last two columns are missing
        assertEquals(Row.of("Test", null, null), testDeserialization(true, false, "Test"));
    }

    @Test
    public void testDeserializeMoreColumnsThanExpected() throws Exception {
        // one additional string column
        assertNull(testDeserialization(true, false, "Test,12,Test,Test"));
    }

    @Test
    public void testDeserializeIgnoreComment() throws Exception {
        // # is part of the string
        assertEquals(
                Row.of("#Test", 12, "Test"), testDeserialization(false, false, "#Test,12,Test"));
    }

    @Test
    public void testDeserializeAllowComment() throws Exception {
        // entire row is ignored
        assertNull(testDeserialization(true, true, "#Test,12,Test"));
    }

    @Test
    public void testSerializationProperties() throws Exception {
        DataType dataType = ROW(FIELD("f0", STRING()), FIELD("f1", INT()), FIELD("f2", STRING()));
        RowType rowType = (RowType) dataType.getLogicalType();
        CsvRowDataSerializationSchema.Builder serSchemaBuilder =
                new CsvRowDataSerializationSchema.Builder(rowType);

        assertArrayEquals(
                "Test,12,Hello".getBytes(),
                serialize(serSchemaBuilder, rowData("Test", 12, "Hello")));

        serSchemaBuilder.setQuoteCharacter('#');

        assertArrayEquals(
                "Test,12,#2019-12-26 12:12:12#".getBytes(),
                serialize(serSchemaBuilder, rowData("Test", 12, "2019-12-26 12:12:12")));

        serSchemaBuilder.disableQuoteCharacter();

        assertArrayEquals(
                "Test,12,2019-12-26 12:12:12".getBytes(),
                serialize(serSchemaBuilder, rowData("Test", 12, "2019-12-26 12:12:12")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidNesting() throws Exception {
        testNullableField(
                ROW(FIELD("f0", ROW(FIELD("f0", STRING())))), "FAIL", Row.of(Row.of("FAIL")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidType() throws Exception {
        testNullableField(
                RAW(TypeExtractor.getForClass(java.util.Date.class)), "FAIL", new java.util.Date());
    }

    @Test
    public void testSerializeDeserializeNestedTypes() throws Exception {
        DataType subDataType0 =
                ROW(FIELD("f0c0", STRING()), FIELD("f0c1", INT()), FIELD("f0c2", STRING()));
        DataType subDataType1 =
                ROW(FIELD("f1c0", STRING()), FIELD("f1c1", INT()), FIELD("f1c2", STRING()));
        DataType dataType = ROW(FIELD("f0", subDataType0), FIELD("f1", subDataType1));
        RowType rowType = (RowType) dataType.getLogicalType();

        // serialization
        CsvRowDataSerializationSchema.Builder serSchemaBuilder =
                new CsvRowDataSerializationSchema.Builder(rowType);
        // deserialization
        CsvRowDataDeserializationSchema.Builder deserSchemaBuilder =
                new CsvRowDataDeserializationSchema.Builder(rowType, InternalTypeInfo.of(rowType));

        RowData normalRow =
                GenericRowData.of(
                        rowData("hello", 1, "This is 1st top column"),
                        rowData("world", 2, "This is 2nd top column"));
        testSerDeConsistency(normalRow, serSchemaBuilder, deserSchemaBuilder);

        RowData nullRow =
                GenericRowData.of(null, rowData("world", 2, "This is 2nd top column after null"));
        testSerDeConsistency(nullRow, serSchemaBuilder, deserSchemaBuilder);
    }

    private void testNullableField(DataType fieldType, String string, Object value)
            throws Exception {
        testField(
                fieldType,
                string,
                value,
                (deserSchema) -> deserSchema.setNullLiteral("null"),
                (serSchema) -> serSchema.setNullLiteral("null"),
                ",");
    }

    private void testField(
            DataType fieldType,
            String csvValue,
            Object value,
            Consumer<CsvRowDataSerializationSchema.Builder> serializationConfig,
            Consumer<CsvRowDataDeserializationSchema.Builder> deserializationConfig,
            String fieldDelimiter)
            throws Exception {
        RowType rowType =
                (RowType)
                        ROW(FIELD("f0", STRING()), FIELD("f1", fieldType), FIELD("f2", STRING()))
                                .getLogicalType();
        String expectedCsv = "BEGIN" + fieldDelimiter + csvValue + fieldDelimiter + "END";

        // deserialization
        CsvRowDataDeserializationSchema.Builder deserSchemaBuilder =
                new CsvRowDataDeserializationSchema.Builder(rowType, InternalTypeInfo.of(rowType));
        deserializationConfig.accept(deserSchemaBuilder);
        RowData deserializedRow = deserialize(deserSchemaBuilder, expectedCsv);

        // serialization
        CsvRowDataSerializationSchema.Builder serSchemaBuilder =
                new CsvRowDataSerializationSchema.Builder(rowType);
        serializationConfig.accept(serSchemaBuilder);
        byte[] serializedRow = serialize(serSchemaBuilder, deserializedRow);
        assertEquals(expectedCsv, new String(serializedRow));
    }

    @SuppressWarnings("unchecked")
    private void testFieldDeserialization(
            DataType fieldType,
            String csvValue,
            Object value,
            Consumer<CsvRowDataDeserializationSchema.Builder> deserializationConfig,
            String fieldDelimiter)
            throws Exception {
        DataType dataType =
                ROW(FIELD("f0", STRING()), FIELD("f1", fieldType), FIELD("f2", STRING()));
        RowType rowType = (RowType) dataType.getLogicalType();
        String csv = "BEGIN" + fieldDelimiter + csvValue + fieldDelimiter + "END";
        Row expectedRow = Row.of("BEGIN", value, "END");

        // deserialization
        CsvRowDataDeserializationSchema.Builder deserSchemaBuilder =
                new CsvRowDataDeserializationSchema.Builder(rowType, InternalTypeInfo.of(rowType));
        deserializationConfig.accept(deserSchemaBuilder);
        RowData deserializedRow = deserialize(deserSchemaBuilder, csv);
        Row actualRow =
                (Row)
                        DataFormatConverters.getConverterForDataType(dataType)
                                .toExternal(deserializedRow);
        assertEquals(expectedRow, actualRow);
    }

    @SuppressWarnings("unchecked")
    private Row testDeserialization(
            boolean allowParsingErrors, boolean allowComments, String string) throws Exception {
        DataType dataType = ROW(FIELD("f0", STRING()), FIELD("f1", INT()), FIELD("f2", STRING()));
        RowType rowType = (RowType) dataType.getLogicalType();
        CsvRowDataDeserializationSchema.Builder deserSchemaBuilder =
                new CsvRowDataDeserializationSchema.Builder(rowType, InternalTypeInfo.of(rowType))
                        .setIgnoreParseErrors(allowParsingErrors)
                        .setAllowComments(allowComments);
        RowData deserializedRow = deserialize(deserSchemaBuilder, string);
        return (Row)
                DataFormatConverters.getConverterForDataType(dataType).toExternal(deserializedRow);
    }

    private void testSerDeConsistency(
            RowData originalRow,
            CsvRowDataSerializationSchema.Builder serSchemaBuilder,
            CsvRowDataDeserializationSchema.Builder deserSchemaBuilder)
            throws Exception {
        RowData deserializedRow =
                deserialize(
                        deserSchemaBuilder, new String(serialize(serSchemaBuilder, originalRow)));
        assertEquals(deserializedRow, originalRow);
    }

    private static byte[] serialize(
            CsvRowDataSerializationSchema.Builder serSchemaBuilder, RowData row) throws Exception {
        // we serialize and deserialize the schema to test runtime behavior
        // when the schema is shipped to the cluster
        CsvRowDataSerializationSchema schema =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(serSchemaBuilder.build()),
                        CsvRowDeSerializationSchemaTest.class.getClassLoader());
        return schema.serialize(row);
    }

    private static RowData deserialize(
            CsvRowDataDeserializationSchema.Builder deserSchemaBuilder, String csv)
            throws Exception {
        // we serialize and deserialize the schema to test runtime behavior
        // when the schema is shipped to the cluster
        CsvRowDataDeserializationSchema schema =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(deserSchemaBuilder.build()),
                        CsvRowDeSerializationSchemaTest.class.getClassLoader());
        return schema.deserialize(csv != null ? csv.getBytes() : null);
    }

    private static RowData rowData(String str1, int integer, String str2) {
        return GenericRowData.of(fromString(str1), integer, fromString(str2));
    }
}
