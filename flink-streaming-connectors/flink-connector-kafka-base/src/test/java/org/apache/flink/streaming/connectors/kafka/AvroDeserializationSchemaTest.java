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
package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.table.Row;
import org.apache.flink.streaming.util.serialization.AvroRowDeserializationSchema;
import org.apache.flink.streaming.util.serialization.AvroRowSerializationSchema;
import org.junit.Test;

import java.io.IOException;

import static org.apache.flink.streaming.connectors.kafka.internals.TypeUtil.toTypeInfo;
import static org.junit.Assert.assertEquals;

public class AvroDeserializationSchemaTest {

	private static final String[] FIELD_NAMES = new String[]{"f1", "f2", "f3"};
	private static final TypeInformation[] FIELD_TYPES = toTypeInfo(new Class[]{Integer.class, Boolean.class, String.class});

	private AvroRowSerializationSchema serializationSchema =  new AvroRowSerializationSchema(
		FIELD_NAMES, FIELD_TYPES
	);
	private AvroRowDeserializationSchema deserializationSchema = new AvroRowDeserializationSchema(
		FIELD_NAMES, FIELD_TYPES
	);

	@Test
	public void serializeAndDeserializeRow() throws IOException {
		Row row = createRow();

		byte[] bytes = serializationSchema.serialize(row);
		Row resultRow = deserializationSchema.deserialize(bytes);

		assertEqualsRows(row, resultRow);
	}

	@Test
	public void serializeRowSeveralTimes() throws IOException {
		Row row = createRow();

		serializationSchema.serialize(row);
		serializationSchema.serialize(row);
		byte[] bytes = serializationSchema.serialize(row);
		Row resultRow = deserializationSchema.deserialize(bytes);

		assertEqualsRows(row, resultRow);
	}

	@Test
	public void deserializeRowSeveralTimes() throws IOException {
		Row row = createRow();

		byte[] bytes = serializationSchema.serialize(row);
		deserializationSchema.deserialize(bytes);
		deserializationSchema.deserialize(bytes);
		Row resultRow = deserializationSchema.deserialize(bytes);

		assertEqualsRows(row, resultRow);
	}

	private Row createRow() {
		Row row = new Row(3);
		row.setField(0, 5);
		row.setField(1, true);
		row.setField(2, "str");
		return row;
	}

	private void assertEqualsRows(Row row, Row resultRow) {
		assertEquals(
			String.format("Rows %s and %s have different number of fields", row, resultRow),
			row.productArity(),
			resultRow.productArity());

		for (int i = 0; i < row.productArity(); i++) {
			assertEquals(String.format("Rows %s and %s have different fields", row, resultRow),
				row.productElement(i),
				resultRow.productElement(i));
		}
	}
}
