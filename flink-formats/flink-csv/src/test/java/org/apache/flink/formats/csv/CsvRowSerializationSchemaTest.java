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

import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Testing for {@link CsvRowSerializationSchema}.
 */
public class CsvRowSerializationSchemaTest extends TestLogger {

	@Test
	public void testSerializeAndDeserialize() throws IOException {
		final String[] fields = new String[]{"a", "b", "c", "d", "e", "f", "g"};
		final TypeInformation[] types = new TypeInformation[]{
			Types.BOOLEAN(), Types.STRING(), Types.INT(), Types.DECIMAL(),
			Types.SQL_TIMESTAMP(), PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
			Types.ROW(
				new String[]{"g1", "g2"},
				new TypeInformation[]{Types.STRING(), Types.LONG()})
		};
		final TypeInformation<Row> rowSchema = Types.ROW(fields, types);
		Row row = new Row(7);
		Row nestedRow = new Row(2);
		nestedRow.setField(0, "z\"xcv");
		nestedRow.setField(1, 123L);
		row.setField(0, true);
		row.setField(1, "abcd");
		row.setField(2, 1);
		row.setField(3, BigDecimal.valueOf(1.2334));
		row.setField(4, new Timestamp(System.currentTimeMillis()));
		row.setField(5, "qwecxcr".getBytes());
		row.setField(6, nestedRow);

		final Row resultRow = serializeAndDeserialize(rowSchema, row);
		assertEquals(row, resultRow);
	}

	@Test
	public void testSerialize() {
		long currentMillis = System.currentTimeMillis();
		Row row = new Row(4);
		Row nestedRow = new Row(2);
		row.setField(0, "abc");
		row.setField(1, 34);
		row.setField(2, new Timestamp(currentMillis));
		nestedRow.setField(0, "bc");
		nestedRow.setField(1, "qwertyu".getBytes());
		row.setField(3, nestedRow);

		final TypeInformation<Row> typeInfo = Types.ROW(
			new String[]{"a", "b", "c", "d"},
			new TypeInformation[]{Types.STRING(), Types.INT(), Types.SQL_TIMESTAMP(),
				Types.ROW(
					new String[]{"d1", "d2"},
					new TypeInformation[]{Types.STRING(), PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO}
			)}
		);

		final CsvRowSerializationSchema schema = new CsvRowSerializationSchema(typeInfo);
		byte[] result = schema.serialize(row);
		String c1 = "abc";
		String c2 = String.valueOf(34);
		String c3 = "\"" + new Timestamp(currentMillis).toString() + "\"";
		String c4 = "bc;" + new String("qwertyu".getBytes());
		byte[] expect = (c1 + "," + c2 + "," + c3 + "," + c4 + "\n").getBytes();
		assertArrayEquals(expect, result);
	}

	@Test
	public void testCustomizedProperties() throws IOException {
		final TypeInformation<Row> rowTypeInfo = Types.ROW(
			new String[]{"a", "b", "c"},
			new TypeInformation[]{Types.STRING(), Types.STRING(),
				Types.ROW(
					new String[]{"c1", "c2"},
					new TypeInformation[]{Types.INT(), Types.STRING()}
				)}
		);

		final Row row = new Row(3);
		final Row nestedRow = new Row(2);
		nestedRow.setField(0, 1);
		nestedRow.setField(1, "zxv");
		row.setField(0, "12*3'4");
		row.setField(1, "a,bc");
		row.setField(2, nestedRow);

		final CsvRowSerializationSchema serializationSchema = new CsvRowSerializationSchema(rowTypeInfo);
		serializationSchema.setEscapeCharacter('*');
		serializationSchema.setQuoteCharacter('\'');
		serializationSchema.setArrayElementDelimiter(":");
		byte[] result = serializationSchema.serialize(row);

		final String c1 = "'12**3''4'";
		final String c2 = "'a,bc'";
		final String c3 = "1:zxv";
		byte[] expect = (c1 + "," + c2 + "," + c3 + "\n").getBytes();
		assertArrayEquals(expect, result);
	}

	@Test
	public void testCharset() throws UnsupportedEncodingException {
		final TypeInformation<Row> rowTypeInfo = Types.ROW(
			new String[]{"a"},
			new TypeInformation[]{PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO}
		);
		final CsvRowSerializationSchema serializationSchema = new CsvRowSerializationSchema(rowTypeInfo);
		serializationSchema.setCharset("UTF-16");

		final Row row = new Row(1);
		row.setField(0, "123".getBytes(StandardCharsets.UTF_16));
		byte[] result = serializationSchema.serialize(row);
		byte[] expect = "123\n".getBytes();

		assertArrayEquals(expect, result);
	}

	@Test
	public void testSerializationOfTwoRows() throws IOException {
		final TypeInformation<Row> rowSchema = Types.ROW(
			new String[] {"f1", "f2", "f3"},
			new TypeInformation[]{Types.INT(), Types.BOOLEAN(), Types.STRING()});

		final Row row1 = new Row(3);
		row1.setField(0, 1);
		row1.setField(1, true);
		row1.setField(2, "str");

		final CsvRowSerializationSchema serializationSchema = new CsvRowSerializationSchema(rowSchema);
		final CsvRowDeserializationSchema deserializationSchema = new CsvRowDeserializationSchema(rowSchema);

		byte[] bytes = serializationSchema.serialize(row1);
		assertEquals(row1, deserializationSchema.deserialize(bytes));

		final Row row2 = new Row(3);
		row2.setField(0, 10);
		row2.setField(1, false);
		row2.setField(2, "newStr");

		bytes = serializationSchema.serialize(row2);
		assertEquals(row2, deserializationSchema.deserialize(bytes));
	}

	@Test(expected = RuntimeException.class)
	public void testSerializeRowWithInvalidNumberOfFields() {
		final TypeInformation<Row> rowSchema = Types.ROW(
			new String[] {"f1", "f2", "f3"},
			new TypeInformation[]{Types.INT(), Types.BOOLEAN(), Types.STRING()});
		final Row row = new Row(1);
		row.setField(0, 1);

		final CsvRowSerializationSchema serializationSchema = new CsvRowSerializationSchema(rowSchema);
		serializationSchema.serialize(row);
	}

	@Test(expected = RuntimeException.class)
	public void testSerializeNestedRowInNestedRow() {
		final TypeInformation<Row> rowSchema = Types.ROW(
			new String[]{"a"},
			new TypeInformation[]{Types.ROW(
				new String[]{"a1"},
				new TypeInformation[]{Types.ROW(
					new String[]{"a11"},
					new TypeInformation[]{Types.STRING()}
				)}
			)}
		);
		final Row row = new Row(1);
		final Row nestedRow = new Row(1);
		final Row doubleNestedRow = new Row(1);
		doubleNestedRow.setField(0, "123");
		nestedRow.setField(0, doubleNestedRow);
		row.setField(0, nestedRow);
		final CsvRowSerializationSchema serializationSchema = new CsvRowSerializationSchema(rowSchema);
		serializationSchema.serialize(row);
	}

	private Row serializeAndDeserialize(TypeInformation<Row> rowSchema, Row row) throws IOException {
		final CsvRowSerializationSchema serializationSchema = new CsvRowSerializationSchema(rowSchema);
		final CsvRowDeserializationSchema deserializationSchema = new CsvRowDeserializationSchema(rowSchema);

		final byte[] bytes = serializationSchema.serialize(row);
		return deserializationSchema.deserialize(bytes);
	}
}
