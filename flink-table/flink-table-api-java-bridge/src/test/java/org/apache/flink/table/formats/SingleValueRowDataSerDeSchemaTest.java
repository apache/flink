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

package org.apache.flink.table.formats;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.CHAR;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.VARBINARY;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link SingleValueRowDataDeserialization} {@link SingleValueRowDataSerialization}.
 */
public class SingleValueRowDataSerDeSchemaTest {

	@Test
	public void testSerDe() throws IOException {
		for (TestSpec testSpec : testData) {
			testParse(testSpec);
		}
	}

	private void testParse(TestSpec testSpec) throws IOException {
		SingleValueRowDataDeserialization deserializationSchema = new SingleValueRowDataDeserialization(
			testSpec.rowType, new MockRowDataTypeInfo(testSpec.rowType));
		SingleValueRowDataSerialization serializationSchema = new SingleValueRowDataSerialization(
			testSpec.rowType);

		FieldGetter fieldGetter = RowData.createFieldGetter(testSpec.rowType.getTypeAt(0), 0);
		GenericRowData originRowData = new GenericRowData(1);
		originRowData.setField(0, testSpec.singleValue);

		byte[] serializedBytes = serializationSchema.serialize(originRowData);
		RowData deserializeRowData = deserializationSchema.deserialize(serializedBytes);

		Object expectedValue = testSpec.singleValue;
		Object deserializeValue = fieldGetter.getFieldOrNull(deserializeRowData);

		assertEquals(expectedValue, deserializeValue);
	}

	private static List<TestSpec> testData = Arrays.asList(
		TestSpec
			.singleValue(1024)
			.rowType(ROW(FIELD("single", INT())))
			.expect(Row.of(1024)),

		TestSpec
			.singleValue(1024.0f)
			.rowType(ROW(FIELD("single", FLOAT())))
			.expect(Row.of(1024.0f)),

		TestSpec
			.singleValue(1024.0d)
			.rowType(ROW(FIELD("single", DOUBLE())))
			.expect(Row.of(1024.0d)),

		TestSpec
			.singleValue(1024L)
			.rowType(ROW(FIELD("single", BIGINT())))
			.expect(Row.of(1024L)),

		TestSpec
			.singleValue(StringData.fromString("hello,world"))
			.rowType(ROW(FIELD("single", STRING())))
			.expect(Row.of("hello,world")),

		TestSpec
			.singleValue(StringData.fromString(String.valueOf('H')))
			.rowType(ROW(FIELD("single", CHAR(1))))
			.expect(Row.of(String.valueOf('H'))),

		TestSpec
			.singleValue(false)
			.rowType(ROW(FIELD("single", BOOLEAN())))
			.expect(Row.of(false)),

		TestSpec
			.singleValue(new byte[1024])
			.rowType(ROW(FIELD("single", VARBINARY(1024))))
			.expect(Row.of(new byte[1024]))
	);

	private static class TestSpec {

		private Object singleValue;
		private DataType dataType;
		private RowType rowType;
		private Row expected;

		private TestSpec(Object value) {
			this.singleValue = value;
		}

		public static TestSpec singleValue(Object value) {
			return new TestSpec(value);
		}

		public TestSpec rowType(DataType rowType) {
			this.dataType = rowType;
			this.rowType = (RowType) rowType.getLogicalType();
			return this;
		}

		public TestSpec expect(Row row) {
			this.expected = row;
			return this;
		}
	}
}
