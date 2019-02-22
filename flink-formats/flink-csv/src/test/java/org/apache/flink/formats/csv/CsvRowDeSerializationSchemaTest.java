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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;

import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.function.Consumer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Tests for {@link CsvRowSerializationSchema} and {@link CsvRowDeserializationSchema}.
 */
public class CsvRowDeSerializationSchemaTest {

	@Test
	@SuppressWarnings("unchecked")
	public void testSerializeDeserialize() throws Exception {

		testNullableField(Types.LONG, "null", null);
		testNullableField(Types.STRING, "null", null);
		testNullableField(Types.VOID, "null", null);
		testNullableField(Types.STRING, "\"This is a test.\"", "This is a test.");
		testNullableField(Types.STRING, "\"This is a test\n\r.\"", "This is a test\n\r.");
		testNullableField(Types.BOOLEAN, "true", true);
		testNullableField(Types.BOOLEAN, "null", null);
		testNullableField(Types.BYTE, "124", (byte) 124);
		testNullableField(Types.SHORT, "10000", (short) 10000);
		testNullableField(Types.INT, "1234567", 1234567);
		testNullableField(Types.LONG, "12345678910", 12345678910L);
		testNullableField(Types.FLOAT, "0.33333334", 0.33333334f);
		testNullableField(Types.DOUBLE, "0.33333333332", 0.33333333332d);
		testNullableField(Types.BIG_DEC,
			"\"1234.0000000000000000000000001\"",
			new BigDecimal("1234.0000000000000000000000001"));
		testNullableField(Types.BIG_INT,
			"\"123400000000000000000000000000\"",
			new BigInteger("123400000000000000000000000000"));
		testNullableField(Types.SQL_DATE, "2018-10-12", Date.valueOf("2018-10-12"));
		testNullableField(Types.SQL_TIME, "12:12:12", Time.valueOf("12:12:12"));
		testNullableField(
			Types.SQL_TIMESTAMP,
			"\"2018-10-12 12:12:12.0\"",
			Timestamp.valueOf("2018-10-12 12:12:12"));
		testNullableField(
			Types.ROW(Types.STRING, Types.INT, Types.BOOLEAN),
			"Hello;42;false",
			Row.of("Hello", 42, false));
		testNullableField(
			Types.OBJECT_ARRAY(Types.STRING),
			"a;b;c",
			new String[] {"a", "b", "c"});
		testNullableField(
			Types.OBJECT_ARRAY(Types.BYTE),
			"12;4;null",
			new Byte[] {12, 4, null});
		testNullableField(
			(TypeInformation<byte[]>) Types.PRIMITIVE_ARRAY(Types.BYTE),
			"awML",
			new byte[] {107, 3, 11});
	}

	@Test
	public void testSerializeDeserializeCustomizedProperties() throws Exception {

		final Consumer<CsvRowSerializationSchema.Builder> serConfig = (serSchemaBuilder) -> serSchemaBuilder
			.setEscapeCharacter('*')
			.setQuoteCharacter('\'')
			.setArrayElementDelimiter(":")
			.setFieldDelimiter(';');

		final Consumer<CsvRowDeserializationSchema.Builder> deserConfig = (deserSchemaBuilder) -> deserSchemaBuilder
			.setEscapeCharacter('*')
			.setQuoteCharacter('\'')
			.setArrayElementDelimiter(":")
			.setFieldDelimiter(';');

		testField(Types.STRING, "123*'4**", "123'4*", deserConfig, ";");
		testField(Types.STRING, "'123''4**'", "123'4*", serConfig, deserConfig, ";");
		testField(Types.STRING, "'a;b*'c'", "a;b'c", deserConfig, ";");
		testField(Types.STRING, "'a;b''c'", "a;b'c", serConfig, deserConfig, ";");
		testField(Types.INT, "       12          ", 12, deserConfig, ";");
		testField(Types.INT, "12", 12, serConfig, deserConfig, ";");
		testField(Types.ROW(Types.STRING, Types.STRING), "1:hello", Row.of("1", "hello"), deserConfig, ";");
		testField(Types.ROW(Types.STRING, Types.STRING), "'1:hello'", Row.of("1", "hello"), serConfig, deserConfig, ";");
		testField(Types.ROW(Types.STRING, Types.STRING), "'1:hello world'", Row.of("1", "hello world"), serConfig, deserConfig, ";");
		testField(Types.STRING, "null", "null", serConfig, deserConfig, ";"); // string because null literal has not been set
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
		assertEquals(Row.of("Test", null, "Test"), testDeserialization(true, false, "Test,null,Test"));
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
		assertEquals(Row.of("#Test", 12, "Test"), testDeserialization(false, false, "#Test,12,Test"));
	}

	@Test
	public void testDeserializeAllowComment() throws Exception {
		// entire row is ignored
		assertNull(testDeserialization(true, true, "#Test,12,Test"));
	}

	@Test
	public void testSerializationProperties() throws Exception {
		final TypeInformation<Row> rowInfo = Types.ROW(Types.STRING, Types.INT, Types.STRING);
		final CsvRowSerializationSchema.Builder serSchemaBuilder = new CsvRowSerializationSchema.Builder(rowInfo)
			.setLineDelimiter("\r");

		assertArrayEquals(
			"Test,12,Hello\r".getBytes(),
			serialize(serSchemaBuilder, Row.of("Test", 12, "Hello")));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidNesting() throws Exception {
		testNullableField(Types.ROW(Types.ROW(Types.STRING)), "FAIL", Row.of(Row.of("FAIL")));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidType() throws Exception {
		testNullableField(Types.GENERIC(java.util.Date.class), "FAIL", new java.util.Date());
	}

	private <T> void testNullableField(TypeInformation<T> fieldInfo, String string, T value) throws Exception {
		testField(
			fieldInfo,
			string,
			value,
			(deserSchema) -> deserSchema.setNullLiteral("null"),
			(serSchema) -> serSchema.setNullLiteral("null"),
			",");
	}

	private <T> void testField(
			TypeInformation<T> fieldInfo,
			String csvValue,
			T value,
			Consumer<CsvRowSerializationSchema.Builder> serializationConfig,
			Consumer<CsvRowDeserializationSchema.Builder> deserializationConfig,
			String fieldDelimiter) throws Exception {
		final TypeInformation<Row> rowInfo = Types.ROW(Types.STRING, fieldInfo, Types.STRING);
		final String expectedCsv = "BEGIN" + fieldDelimiter + csvValue + fieldDelimiter + "END\n";
		final Row expectedRow = Row.of("BEGIN", value, "END");

		// serialization
		final CsvRowSerializationSchema.Builder serSchemaBuilder = new CsvRowSerializationSchema.Builder(rowInfo);
		serializationConfig.accept(serSchemaBuilder);
		final byte[] serializedRow = serialize(serSchemaBuilder, expectedRow);
		assertEquals(expectedCsv, new String(serializedRow));

		// deserialization
		final CsvRowDeserializationSchema.Builder deserSchemaBuilder = new CsvRowDeserializationSchema.Builder(rowInfo);
		deserializationConfig.accept(deserSchemaBuilder);
		final Row deserializedRow = deserialize(deserSchemaBuilder, expectedCsv);
		assertEquals(expectedRow, deserializedRow);
	}

	private <T> void testField(
			TypeInformation<T> fieldInfo,
			String csvValue,
			T value,
			Consumer<CsvRowDeserializationSchema.Builder> deserializationConfig,
			String fieldDelimiter) throws Exception {
		final TypeInformation<Row> rowInfo = Types.ROW(Types.STRING, fieldInfo, Types.STRING);
		final String csv = "BEGIN" + fieldDelimiter + csvValue + fieldDelimiter + "END\n";
		final Row expectedRow = Row.of("BEGIN", value, "END");

		// deserialization
		final CsvRowDeserializationSchema.Builder deserSchemaBuilder = new CsvRowDeserializationSchema.Builder(rowInfo);
		deserializationConfig.accept(deserSchemaBuilder);
		final Row deserializedRow = deserialize(deserSchemaBuilder, csv);
		assertEquals(expectedRow, deserializedRow);
	}

	private Row testDeserialization(
			boolean allowParsingErrors,
			boolean allowComments,
			String string) throws Exception {
		final TypeInformation<Row> rowInfo = Types.ROW(Types.STRING, Types.INT, Types.STRING);
		final CsvRowDeserializationSchema.Builder deserSchemaBuilder = new CsvRowDeserializationSchema.Builder(rowInfo)
			.setIgnoreParseErrors(allowParsingErrors)
			.setAllowComments(allowComments);
		return deserialize(deserSchemaBuilder, string);
	}

	private static byte[] serialize(CsvRowSerializationSchema.Builder serSchemaBuilder, Row row) throws Exception {
		// we serialize and deserialize the schema to test runtime behavior
		// when the schema is shipped to the cluster
		final CsvRowSerializationSchema schema = InstantiationUtil.deserializeObject(
			InstantiationUtil.serializeObject(serSchemaBuilder.build()),
			CsvRowDeSerializationSchemaTest.class.getClassLoader());
		return schema.serialize(row);
	}

	private static Row deserialize(CsvRowDeserializationSchema.Builder deserSchemaBuilder, String csv) throws Exception {
		// we serialize and deserialize the schema to test runtime behavior
		// when the schema is shipped to the cluster
		final CsvRowDeserializationSchema schema = InstantiationUtil.deserializeObject(
			InstantiationUtil.serializeObject(deserSchemaBuilder.build()),
			CsvRowDeSerializationSchemaTest.class.getClassLoader());
		return schema.deserialize(csv.getBytes());
	}
}
