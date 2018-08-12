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
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Testing for {@link CsvRowDeserializationSchema}.
 */
public class CsvRowDeserializationSchemaTest extends TestLogger {

	@Test
	public void testDeserialize() throws IOException {
		final long currentMills = System.currentTimeMillis();
		final TypeInformation<Row> rowTypeInfo = Types.ROW(
			new String[]{"a", "b", "c", "d", "e", "f", "g"},
			new TypeInformation[]{
				Types.STRING(), Types.LONG(), Types.DECIMAL(),
				Types.ROW(
					new String[]{"c1", "c2", "c3"},
					new TypeInformation[]{
						Types.INT(),
						PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
						Types.STRING()
					}
				), Types.SQL_TIMESTAMP(), Types.BOOLEAN(),
				PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO
			}
		);

		String c1 = "123";
		String c2 = String.valueOf(34L);
		String c3 = String.valueOf(1233.2);
		String c4 = "1" + ";" + new String("abc".getBytes()) + ";" + "cba";
		String c5 = new Timestamp(currentMills).toString();
		String c6 = "true";
		String c7 =  new String("12345".getBytes());
		byte[] bytes = (c1 + "," + c2 + "," + c3 + "," + c4 + ","
			+ c5 + "," + c6 + "," + c7).getBytes();
		CsvRowDeserializationSchema deserializationSchema = new CsvRowDeserializationSchema(rowTypeInfo);
		Row deserializedRow = deserializationSchema.deserialize(bytes);

		assertEquals(7, deserializedRow.getArity());
		assertEquals("123", deserializedRow.getField(0));
		assertEquals(34L, deserializedRow.getField(1));
		assertEquals(BigDecimal.valueOf(1233.2), deserializedRow.getField(2));
		assertArrayEquals("abc".getBytes(), (byte[]) ((Row) deserializedRow.getField(3)).getField(1));
		assertEquals(new Timestamp(currentMills), deserializedRow.getField(4));
		assertEquals(true, deserializedRow.getField(5));
		assertArrayEquals("12345".getBytes(), (byte[]) deserializedRow.getField(6));
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

		String c1 = "123*\"4";
		String c2 = "'a,bc'";
		String c3 = "1:zxv";

		byte[] bytes = (c1 + "," + c2 + "," + c3).getBytes();
		CsvRowDeserializationSchema deserializationSchema = new CsvRowDeserializationSchema(rowTypeInfo);
		deserializationSchema.setEscapeCharacter('*');
		deserializationSchema.setQuoteCharacter('\'');
		deserializationSchema.setArrayElementDelimiter(":");
		Row deserializedRow = deserializationSchema.deserialize(bytes);

		assertEquals("123\"4", deserializedRow.getField(0));
		assertEquals("a,bc", deserializedRow.getField(1));
		assertEquals("zxv", ((Row) deserializedRow.getField(2)).getField(1));
	}

	@Test
	public void testCharset() throws IOException {
		final TypeInformation<Row> rowTypeInfo = Types.ROW(
			new String[]{"a"},
			new TypeInformation[]{PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO}
		);
		final CsvRowDeserializationSchema schema = new CsvRowDeserializationSchema(rowTypeInfo);
		schema.setCharset("UTF-16");

		byte[] bytes = "abc".getBytes(StandardCharsets.UTF_16);
		Row result = schema.deserialize(bytes);

		assertEquals("abc", new String((byte[]) result.getField(0), StandardCharsets.UTF_16));
	}

	@Test
	public void testNull() throws IOException {
		final TypeInformation<Row> rowTypeInfo = Types.ROW(
			new String[]{"a"},
			new TypeInformation[]{Types.STRING()}
		);

		final byte[] bytes = "123".getBytes();

		final CsvRowDeserializationSchema deserializationSchema = new CsvRowDeserializationSchema(rowTypeInfo);
		deserializationSchema.setNullValue("123");
		final Row row = deserializationSchema.deserialize(bytes);
		assertNull(row.getField(0));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNumberOfFieldNamesAndTypesMismatch() {
		TypeInformation<Row> rowTypeInfo = Types.ROW(
			new String[]{"a", "b"},
			new TypeInformation[]{Types.STRING()}
		);
		new CsvRowDeserializationSchema(rowTypeInfo);
	}

}
