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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.apache.flink.streaming.util.serialization.JsonRowDeserializationSchema;
import org.apache.flink.streaming.util.serialization.JsonRowSerializationSchema;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class JsonRowSerializationSchemaTest {

	@Test
	public void testRowSerialization() throws IOException {
		String[] fieldNames = new String[] {"f1", "f2", "f3"};
		TypeInformation<?>[] fieldTypes = new TypeInformation<?>[] { Types.INT(), Types.BOOLEAN(), Types.STRING() };
		Row row = new Row(3);
		row.setField(0, 1);
		row.setField(1, true);
		row.setField(2, "str");

		Row resultRow = serializeAndDeserialize(fieldNames, fieldTypes, row);
		assertEqualRows(row, resultRow);
	}

	@Test
	public void testSerializationOfTwoRows() throws IOException {
		String[] fieldNames = new String[] {"f1", "f2", "f3"};
		TypeInformation<Row> row = Types.ROW(
			fieldNames,
			new TypeInformation<?>[] { Types.INT(), Types.BOOLEAN(), Types.STRING() }
		);
		Row row1 = new Row(3);
		row1.setField(0, 1);
		row1.setField(1, true);
		row1.setField(2, "str");

		JsonRowSerializationSchema serializationSchema = new JsonRowSerializationSchema(fieldNames);
		JsonRowDeserializationSchema deserializationSchema = new JsonRowDeserializationSchema(row);

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
		new JsonRowSerializationSchema(null);
	}

	@Test(expected = IllegalStateException.class)
	public void testSerializeRowWithInvalidNumberOfFields() {
		String[] fieldNames = new String[] {"f1", "f2", "f3"};
		Row row = new Row(1);
		row.setField(0, 1);

		JsonRowSerializationSchema serializationSchema = new JsonRowSerializationSchema(fieldNames);
		serializationSchema.serialize(row);
	}

	private Row serializeAndDeserialize(String[] fieldNames, TypeInformation<?>[] fieldTypes, Row row) throws IOException {
		JsonRowSerializationSchema serializationSchema = new JsonRowSerializationSchema(fieldNames);
		JsonRowDeserializationSchema deserializationSchema = new JsonRowDeserializationSchema(Types.ROW(fieldNames, fieldTypes));

		byte[] bytes = serializationSchema.serialize(row);
		return deserializationSchema.deserialize(bytes);
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
