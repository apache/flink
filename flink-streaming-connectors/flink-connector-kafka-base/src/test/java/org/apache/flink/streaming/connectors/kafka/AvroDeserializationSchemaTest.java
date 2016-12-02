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

import org.apache.avro.Schema;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.table.Row;
import org.apache.flink.streaming.util.serialization.AvroRowDeserializationSchema;
import org.apache.flink.streaming.util.serialization.AvroRowSerializationSchema;
import org.apache.flink.streaming.util.serialization.DefaultGenericRecordToRowConverter;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Objects;

import static org.apache.flink.streaming.connectors.kafka.internals.TypeUtil.createRowAvroSchema;
import static org.apache.flink.streaming.connectors.kafka.internals.TypeUtil.toTypeInfo;
import static org.junit.Assert.assertEquals;

public class AvroDeserializationSchemaTest {

	private static final String[] FIELD_NAMES = new String[]{"f1", "f2", "f3"};
	private static final TypeInformation[] FIELD_TYPES = toTypeInfo(new Class[]{Integer.class, Boolean.class, String.class});

	private static final Schema SCHEMA = createRowAvroSchema(FIELD_NAMES, FIELD_TYPES);

	private AvroRowSerializationSchema serializationSchema =  new AvroRowSerializationSchema(
		FIELD_NAMES, SCHEMA
	);
	private AvroRowDeserializationSchema deserializationSchema = new AvroRowDeserializationSchema(
		new DefaultGenericRecordToRowConverter(FIELD_NAMES, FIELD_TYPES), SCHEMA
	);

	@Test
	public void testSerializeAndDeserializeRow() throws IOException {
		Row row = createRow();

		byte[] bytes = serializationSchema.serialize(row);
		Row resultRow = deserializationSchema.deserialize(bytes);

		assertEqualsRows(row, resultRow);
	}

	@Test
	public void testSerializeRowSeveralTimes() throws IOException {
		Row row = createRow();

		serializationSchema.serialize(row);
		serializationSchema.serialize(row);
		byte[] bytes = serializationSchema.serialize(row);
		Row resultRow = deserializationSchema.deserialize(bytes);

		assertEqualsRows(row, resultRow);
	}

	@Test
	public void testDeserializeRowSeveralTimes() throws IOException {
		Row row = createRow();

		byte[] bytes = serializationSchema.serialize(row);
		deserializationSchema.deserialize(bytes);
		deserializationSchema.deserialize(bytes);
		Row resultRow = deserializationSchema.deserialize(bytes);

		assertEqualsRows(row, resultRow);
	}

	@Test
	public void testDeserializeRowWithNullField() throws IOException {
		Row row = createNullRow();

		byte[] bytes = serializationSchema.serialize(row);
		deserializationSchema.deserialize(bytes);
		deserializationSchema.deserialize(bytes);
		Row resultRow = deserializationSchema.deserialize(bytes);

		assertEqualsRows(row, resultRow);
	}

	@Test
	public void deserializeRowWithComplexTypes() throws IOException {
		Row row = new Row(2);
		row.setField(0, new BigInteger("10"));
		row.setField(1, new BigDecimal("10"));

		String[] fieldNames = new String[]{"f1", "f2"};
		TypeInformation[] fieldTypes = toTypeInfo(new Class[]{BigInteger.class, BigDecimal.class});
		Schema schema = createRowAvroSchema(fieldNames, fieldTypes);

		AvroRowSerializationSchema serializationSchema =  new AvroRowSerializationSchema(
			fieldNames, schema
		);
		AvroRowDeserializationSchema deserializationSchema = new AvroRowDeserializationSchema(
			new DefaultGenericRecordToRowConverter(fieldNames, fieldTypes), schema
		);

		byte[] bytes = serializationSchema.serialize(row);
		deserializationSchema.deserialize(bytes);
		deserializationSchema.deserialize(bytes);
		Row resultRow = deserializationSchema.deserialize(bytes);

		assertEqualsRows(row, resultRow);
	}

	@Test
	public void testDeserializeCustomPOJO() throws IOException {
		Row row = new Row(1);
		row.setField(0, new CustomPojo("str", 10, true));

		TypeInformation[] fieldTypes = toTypeInfo(new Class[]{CustomPojo.class});
		String[] fieldNames = new String[]{"f1"};
		Schema schema = createRowAvroSchema(fieldNames, fieldTypes);

		AvroRowSerializationSchema serializationSchema =  new AvroRowSerializationSchema(
			fieldNames, schema
		);
		AvroRowDeserializationSchema deserializationSchema = new AvroRowDeserializationSchema(
			new DefaultGenericRecordToRowConverter(fieldNames, fieldTypes), schema
		);

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

	private Row createNullRow() {
		Row row = new Row(3);
		row.setField(0, null);
		row.setField(1, null);
		row.setField(2, null);
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

	public static final class CustomPojo {
		private String str;
		private int i;
		private boolean b;

		public CustomPojo() {
		}

		public CustomPojo(String str, int i, boolean b) {
			this.str = str;
			this.i = i;
			this.b = b;
		}

		public String getStr() {
			return str;
		}

		public void setStr(String str) {
			this.str = str;
		}

		public int getI() {
			return i;
		}

		public void setI(int i) {
			this.i = i;
		}

		public boolean isB() {
			return b;
		}

		public void setB(boolean b) {
			this.b = b;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			CustomPojo that = (CustomPojo) o;
			return i == that.i &&
				b == that.b &&
				Objects.equals(str, that.str);
		}

		@Override
		public int hashCode() {
			return Objects.hash(str, i, b);
		}

		@Override
		public String toString() {
			return new ToStringBuilder(this)
				.append("str", str)
				.append("i", i)
				.append("b", b)
				.toString();
		}
	}
}
