/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.json;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link JsonRowSerializationSchema}.
 */
public class JsonRowSerializationSchemaTest {

	@Test
	public void testRowSerialization() throws IOException {
		final TypeInformation<Row> rowSchema = Types.ROW_NAMED(
			new String[] {"f1", "f2", "f3"},
			Types.INT, Types.BOOLEAN, Types.STRING);

		final Row row = new Row(3);
		row.setField(0, 1);
		row.setField(1, true);
		row.setField(2, "str");

		final Row resultRow = serializeAndDeserialize(rowSchema, row);
		assertEquals(row, resultRow);
	}

	@Test
	public void testSerializationOfTwoRows() throws IOException {
		final TypeInformation<Row> rowSchema = Types.ROW_NAMED(
			new String[] {"f1", "f2", "f3"},
			Types.INT, Types.BOOLEAN, Types.STRING);

		final Row row1 = new Row(3);
		row1.setField(0, 1);
		row1.setField(1, true);
		row1.setField(2, "str");

		final JsonRowSerializationSchema serializationSchema = new JsonRowSerializationSchema(rowSchema);
		final JsonRowDeserializationSchema deserializationSchema = new JsonRowDeserializationSchema(rowSchema);

		byte[] bytes = serializationSchema.serialize(row1);
		assertEquals(row1, deserializationSchema.deserialize(bytes));

		final Row row2 = new Row(3);
		row2.setField(0, 10);
		row2.setField(1, false);
		row2.setField(2, "newStr");

		bytes = serializationSchema.serialize(row2);
		assertEquals(row2, deserializationSchema.deserialize(bytes));
	}

	@Test
	public void testNestedSchema() throws IOException {
		final TypeInformation<Row> rowSchema = Types.ROW_NAMED(
			new String[] {"f1", "f2", "f3"},
			Types.INT, Types.BOOLEAN, Types.ROW(Types.INT, Types.DOUBLE));

		final Row row = new Row(3);
		row.setField(0, 42);
		row.setField(1, false);
		final Row nested = new Row(2);
		nested.setField(0, 22);
		nested.setField(1, 2.3);
		row.setField(2, nested);

		final Row resultRow = serializeAndDeserialize(rowSchema, row);
		assertEquals(row, resultRow);
	}

	@Test(expected = RuntimeException.class)
	public void testSerializeRowWithInvalidNumberOfFields() {
		final TypeInformation<Row> rowSchema = Types.ROW_NAMED(
			new String[] {"f1", "f2", "f3"},
			Types.INT, Types.BOOLEAN, Types.STRING);

		final Row row = new Row(1);
		row.setField(0, 1);

		final JsonRowSerializationSchema serializationSchema = new JsonRowSerializationSchema(rowSchema);
		serializationSchema.serialize(row);
	}

	@Test
	public void testSchema() throws IOException {
		final TypeInformation<Row> rowSchema = JsonRowSchemaConverter.convert(
			"{" +
			"    type: 'object'," +
			"    properties: {" +
			"         id: { type: 'integer' }," +
			"         idNumber: { type: 'number' }," +
			"         idOrNull: { type: ['integer', 'null'] }," +
			"         name: { type: 'string' }," +
			"         date: { type: 'string', format: 'date' }," +
			"         time: { type: 'string', format: 'time' }," +
			"         timestamp: { type: 'string', format: 'date-time' }," +
			"         bytes: { type: 'string', contentEncoding: 'base64' }," +
			"         numbers: { type: 'array', items: { type: 'integer' } }," +
			"         strings: { type: 'array', items: { type: 'string' } }," +
			"         nested: { " +
			"             type: 'object'," +
			"             properties: { " +
			"                 booleanField: { type: 'boolean' }," +
			"                 decimalField: { type: 'number' }" +
			"             }" +
			"         }" +
			"    }" +
			"}");

		final Row row = new Row(11);
		row.setField(0, BigDecimal.valueOf(-333));
		row.setField(1, BigDecimal.valueOf(12.2222));
		row.setField(2, null);
		row.setField(3, "");
		row.setField(4, Date.valueOf("1990-10-14"));
		row.setField(5, Time.valueOf("12:12:43"));
		row.setField(6, Timestamp.valueOf("1990-10-14 12:12:43"));

		final byte[] bytes = new byte[1024];
		ThreadLocalRandom.current().nextBytes(bytes);
		row.setField(7, bytes);
		final BigDecimal[] numbers = new BigDecimal[] {
			BigDecimal.valueOf(1), BigDecimal.valueOf(2), BigDecimal.valueOf(3)};
		row.setField(8, numbers);
		final String[] strings = new String[] {"one", "two", "three"};
		row.setField(9, strings);
		final Row nestedRow = new Row(2);
		nestedRow.setField(0, true);
		nestedRow.setField(1, BigDecimal.valueOf(12));
		row.setField(10, nestedRow);

		final Row resultRow = serializeAndDeserialize(rowSchema, row);
		assertEquals(row, resultRow);
	}

	// --------------------------------------------------------------------------------------------

	private Row serializeAndDeserialize(TypeInformation<Row> rowSchema, Row row) throws IOException {
		final JsonRowSerializationSchema serializationSchema = new JsonRowSerializationSchema(rowSchema);
		final JsonRowDeserializationSchema deserializationSchema = new JsonRowDeserializationSchema(rowSchema);

		final byte[] bytes = serializationSchema.serialize(row);
		return deserializationSchema.deserialize(bytes);
	}
}
