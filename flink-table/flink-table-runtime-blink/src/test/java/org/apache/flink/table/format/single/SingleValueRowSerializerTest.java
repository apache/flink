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

package org.apache.flink.table.format.single;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link SingleValueRowDataDeserialization,SingleValueRowDataSerialization}.
 */
public class SingleValueRowSerializerTest {

	@Test
	public void testDeSeSingleValue() throws IOException {
		String testString = "hello,world";
		byte[] testBytes = testString.getBytes();
		int testInt = 1024;
		short testShort = 1024;
		byte testByte = (byte) 1;
		float testFloat = 1024.0f;
		double testDouble = 1024.0d;
		long testLong = 1024L;
		boolean testBoolean = false;
		char testChar = 'T';

		testSeDeSingleValue(getValueBytes(testString), StringData.fromString(testString), new VarCharType());
		testSeDeSingleValue(getValueBytes(testBytes), testBytes, new VarBinaryType());
		testSeDeSingleValue(getValueBytes(testInt), testInt, new IntType());
		testSeDeSingleValue(getValueBytes(testShort), testShort, new SmallIntType());
		testSeDeSingleValue(getValueBytes(testByte), testByte, new TinyIntType());
		testSeDeSingleValue(getValueBytes(testFloat), testFloat, new FloatType());
		testSeDeSingleValue(getValueBytes(testDouble), testDouble, new DoubleType());
		testSeDeSingleValue(getValueBytes(testLong), testLong, new BigIntType());
		testSeDeSingleValue(getValueBytes(testBoolean), testBoolean, new BooleanType());
		testSeDeSingleValue(getValueBytes(testChar), StringData.fromBytes(getValueBytes(testChar)),
			new CharType());
	}

	public void testSeDeSingleValue(byte[] bytes, Object value, LogicalType logicalType)
		throws IOException {
		RowType rowType = RowType.of(logicalType);
		GenericRowData rowData = new GenericRowData(1);
		FieldGetter fieldGetter = RowData.createFieldGetter(logicalType, 0);
		rowData.setField(0, value);

		SingleValueRowDataDeserialization deser = new SingleValueRowDataDeserialization(rowType,
			new RowDataTypeInfo(rowType));
		SingleValueRowDataSerialization ser = new SingleValueRowDataSerialization(rowType);

		RowData expectRowData = deser.deserialize(bytes);
		Object expectValue = fieldGetter.getFieldOrNull(expectRowData);

		byte[] expects = ser.serialize(rowData);

		assertEquals(value, expectValue);
		assertTrue(Arrays.equals(bytes, expects));
	}

	// convert value to byte[]
	private static byte[] getValueBytes(Object val) {
		ByteBuffer buffer;
		if (val instanceof Integer) {
			buffer = ByteBuffer.allocate(4);
			buffer.putInt((int) val);
		} else if (val instanceof Long) {
			buffer = ByteBuffer.allocate(8);
			buffer.putLong((long) val);
		} else if (val instanceof Float) {
			buffer = ByteBuffer.allocate(4);
			buffer.putFloat((float) val);
		} else if (val instanceof Double) {
			buffer = ByteBuffer.allocate(8);
			buffer.putDouble((double) val);
		} else if (val instanceof Character) {
			buffer = ByteBuffer.allocate(2);
			buffer.putChar((Character) val);
		} else if (val instanceof Short) {
			buffer = ByteBuffer.allocate(2);
			buffer.putShort((Short) val);
		} else if (val instanceof Boolean) {
			buffer = ByteBuffer.allocate(1);
			buffer.put((Boolean) val ? (byte) 1 : (byte) 0);
		} else if (val instanceof Byte) {
			return new byte[]{(Byte) val};
		} else if (val instanceof byte[]) {
			return (byte[]) val;
		} else if (val instanceof String) {
			return ((String) val).getBytes();
		} else {
			throw new RuntimeException("Unsupported class:" + val.getClass());
		}

		return buffer.array();
	}
}
