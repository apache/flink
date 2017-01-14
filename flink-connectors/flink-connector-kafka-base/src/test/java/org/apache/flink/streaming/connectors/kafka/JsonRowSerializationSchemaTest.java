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
package org.apache.flink.streaming.connectors.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.connectors.kafka.internals.TypeUtil;
import org.apache.flink.streaming.util.serialization.JsonRowDeserializationSchema;
import org.apache.flink.streaming.util.serialization.JsonRowSerializationSchema;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class JsonRowSerializationSchemaTest {

	private final ObjectMapper objectMapper = new ObjectMapper();
	private static final String[] FLAT_ROW_FIELDS_NAMES = new String[]{"f1", "f2", "f3"};
	private static final Class[] FLAT_FOW_FIELD_TYPES = new Class[]{Integer.class, Boolean.class, String.class};
	private static final String[] NESTED_ROW_FIELD_NAMES = new String[]{"f1", "f2"};
	private static final TypeInformation<?>[] NESTED_ROW_FIELD_TYPES = new TypeInformation<?>[]{
		TypeInformation.of(Integer.class),
		createFlatRowTypeInfo()};

	@Test
	public void testRowSerializationFormat() throws IOException {
		Row row = createFlatRow();

		JsonNode actualObject = serializeToJsonObject(FLAT_ROW_FIELDS_NAMES, FLAT_FOW_FIELD_TYPES, row);
		ObjectNode expectedObject = createFlatSerializedRow();

		assertEquals(expectedObject, actualObject);
	}

	@Test
	public void testRowSerialization() throws IOException {
		Row row = createFlatRow();

		Row resultRow = serializeAndDeserialize(FLAT_ROW_FIELDS_NAMES, FLAT_FOW_FIELD_TYPES, createFlatRow());
		assertEqualRows(row, resultRow);
	}

	@Test
	public void testNestedRowSerialization() throws IOException {
		Row row = createNestedRow();

		Row resultRow = serializeAndDeserialize(NESTED_ROW_FIELD_NAMES, NESTED_ROW_FIELD_TYPES, row);
		assertEqualRows(row, resultRow);
	}

	@Test
	public void testNestedRowSerializationFormat() throws IOException {
		Row row = createNestedRow();
		byte[] bytes = serializeRow(row, NESTED_ROW_FIELD_NAMES, NESTED_ROW_FIELD_TYPES);

		JsonNode actualObject = objectMapper.readTree(bytes);
		ObjectNode nestedRowNode = createFlatSerializedRow();
		JsonNode expectedObject = objectMapper.createObjectNode()
			.put("f1", 1)
			.set("f2", nestedRowNode);

		assertEquals(expectedObject, actualObject);
	}

	@Test(expected = IllegalStateException.class)
	public void testNestedRowSerializationWithIncorrectTypeInformation() throws IOException {
		Row row = new Row(2);
		row.setField(0, 1);
		// Row instance expected at this position
		row.setField(1, true);

		serializeAndDeserialize(NESTED_ROW_FIELD_NAMES, NESTED_ROW_FIELD_TYPES, row);
	}

	@Test(expected = IllegalStateException.class)
	public void testNestedRowSerializationWithNestedRowWithIncorrectNumberOfField() throws IOException {
		Row nestedRow = new Row(1);
		nestedRow.setField(0, 42);

		Row row = new Row(2);
		row.setField(0, 1);
		// Row instance expected at this position
		row.setField(1, nestedRow);

		serializeAndDeserialize(NESTED_ROW_FIELD_NAMES, NESTED_ROW_FIELD_TYPES, row);
	}

	@Test
	public void testSerializationOfTwoRows() throws IOException {
		Row row1 = createFlatRow();

		JsonRowSerializationSchema serializationSchema = new JsonRowSerializationSchema(FLAT_ROW_FIELDS_NAMES, FLAT_FOW_FIELD_TYPES);
		JsonRowDeserializationSchema deserializationSchema = new JsonRowDeserializationSchema(FLAT_ROW_FIELDS_NAMES, FLAT_FOW_FIELD_TYPES);

		byte[] bytes = serializationSchema.serialize(row1);
		assertEqualRows(row1, deserializationSchema.deserialize(bytes));

		Row row2 = new Row(3);
		row2.setField(0, 10);
		row2.setField(1, false);
		row2.setField(2, "newStr");

		bytes = serializationSchema.serialize(row2);
		assertEqualRows(row2, deserializationSchema.deserialize(bytes));
	}

	@Test(expected = NullPointerException.class)
	public void testInputValidation() {
		new JsonRowSerializationSchema(null, new Class[] {Integer.class});
	}

	@Test(expected = IllegalStateException.class)
	public void testSerializeRowWithInvalidNumberOfFields() {
		String[] fieldNames = new String[] {"f1", "f2", "f3"};
		Row row = new Row(1);
		row.setField(0, 1);

		JsonRowSerializationSchema serializationSchema = new JsonRowSerializationSchema(fieldNames, new Class[] {Integer.class, Boolean.class, String.class});
		serializationSchema.serialize(row);
	}

	private JsonNode serializeToJsonObject(String[] fieldNames, Class[] fieldTypes, Row row) throws IOException {
		return objectMapper.readTree(serializeRow(row, fieldNames, TypeUtil.toTypeInfo(fieldTypes)));
	}

	private byte[] serializeRow(Row row, String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		JsonRowSerializationSchema serializationSchema = new JsonRowSerializationSchema(fieldNames, fieldTypes);
		return serializationSchema.serialize(row);
	}

	private ObjectNode createFlatSerializedRow() {
		return objectMapper.createObjectNode()
			.put("f1", 1)
			.put("f2", true)
			.put("f3", "str");
	}

	private Row createFlatRow() {
		Row row = new Row(3);
		row.setField(0, 1);
		row.setField(1, true);
		row.setField(2, "str");
		return row;
	}

	private Row serializeAndDeserialize(String[] fieldNames, Class[] fieldTypes, Row row) throws IOException {
		JsonRowSerializationSchema serializationSchema = new JsonRowSerializationSchema(fieldNames, fieldTypes);
		JsonRowDeserializationSchema deserializationSchema = new JsonRowDeserializationSchema(fieldNames, fieldTypes);

		byte[] bytes = serializationSchema.serialize(row);
		return deserializationSchema.deserialize(bytes);
	}

	private Row serializeAndDeserialize(String[] fieldNames, TypeInformation<?>[] fieldTypes, Row row) throws IOException {
		JsonRowSerializationSchema serializationSchema = new JsonRowSerializationSchema(fieldNames, fieldTypes);
		JsonRowDeserializationSchema deserializationSchema = new JsonRowDeserializationSchema(fieldNames, fieldTypes);

		byte[] bytes = serializationSchema.serialize(row);
		return deserializationSchema.deserialize(bytes);
	}

	private Row createNestedRow() {
		Row row = new Row(2);
		row.setField(0, 1);
		row.setField(1, createFlatRow());
		return row;
	}

	static private RowTypeInfo createFlatRowTypeInfo() {
		return new RowTypeInfo(
			TypeUtil.toTypeInfo(new Class[] {Integer.class, Boolean.class, String.class}),
			new String[] {"f1", "f2", "f3"}
		);
	}
	private void assertEqualRows(Row expectedRow, Row resultRow) {
		assertEquals("Deserialized row should have expected number of fields",
			expectedRow.getArity(), resultRow.getArity());
		for (int i = 0; i < expectedRow.getArity(); i++) {
			assertEquals(String.format("Field number %d should be as in the original row", i),
				expectedRow.getField(i), resultRow.getField(i));
		}
	}

}
