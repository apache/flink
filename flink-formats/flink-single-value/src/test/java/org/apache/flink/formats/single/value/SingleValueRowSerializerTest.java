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

package org.apache.flink.formats.single.value;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static junit.framework.TestCase.assertEquals;

/**
 * Single value serializer test.
 */
public class SingleValueRowSerializerTest {

	@Test
	public void testStringSerializer() {
		String testString = "This is a test string";
		String testFieldName = "test";
		DataType testFieldType = DataTypes.VARCHAR(20);
		TableSchema tableSchema = TableSchema.builder()
			.field(testFieldName, testFieldType)
			.build();

		SingleValueRowSerializer singleValueRowSerializer = new SingleValueRowSerializer(tableSchema);

		try {
			Row row = singleValueRowSerializer.deserialize(testString.getBytes());
			assertEquals(testString, row.getField(0));
		} catch (IOException e) {
			fail();
		}

	}

	@Test
	public void testBytesSerializer() {
		byte[] testBytes = "This is a test string".getBytes();
		String testFieldName = "test";
		DataType testFiledType = DataTypes.BYTES();

		TableSchema bytesTableSchema = TableSchema.builder()
			.field(testFieldName, testFiledType)
			.build();

		SingleValueRowSerializer singleValueRowSerializer = new SingleValueRowSerializer(bytesTableSchema);

		try {
			Row row = singleValueRowSerializer.deserialize(testBytes);
			assertEquals(testBytes, row.getField(0));
			assertTrue(Arrays.equals(testBytes, (byte[]) row.getField(0)));
		} catch (IOException e) {
			fail();
		}
	}

	@Test
	public void testBasicSerializer() {
		int testIntVar = 1024;
		long testLongVar = 1024L;
		float testFloatVar = 1024.0f;
		double testDoubleVar = 1024.0d;

		String testFieldName = "test";
		DataType testIntType = DataTypes.INT();
		DataType testLongType = DataTypes.BIGINT();
		DataType testFloatType = DataTypes.FLOAT();
		DataType testDoubleType = DataTypes.DOUBLE();
		TableSchema intTableSchema = TableSchema.builder()
			.field(testFieldName, testIntType)
			.build();
		TableSchema longTableSchema = TableSchema.builder()
			.field(testFieldName, testLongType)
			.build();
		TableSchema floatTableSchema = TableSchema.builder()
			.field(testFieldName, testFloatType)
			.build();
		TableSchema doubleTableSchema = TableSchema.builder()
			.field(testFieldName, testDoubleType)
			.build();

		SingleValueRowSerializer intSerializer = new SingleValueRowSerializer(intTableSchema);
		SingleValueRowSerializer longSerializer = new SingleValueRowSerializer(longTableSchema);
		SingleValueRowSerializer floatSerializer = new SingleValueRowSerializer(floatTableSchema);
		SingleValueRowSerializer doubleSerializer = new SingleValueRowSerializer(doubleTableSchema);

		Row testIntRow = Row.of(testIntVar);
		Row testLongRow = Row.of(testLongVar);
		Row testFloatRow = Row.of(testFloatVar);
		Row testDoubleRow = Row.of(testDoubleVar);

		assertTrue(Arrays.equals(getBasicValueBytes(testIntVar), intSerializer.serialize(testIntRow)));
		assertTrue(Arrays.equals(getBasicValueBytes(testLongVar), longSerializer.serialize(testLongRow)));
		assertTrue(Arrays.equals(getBasicValueBytes(testFloatVar), floatSerializer.serialize(testFloatRow)));
		assertTrue(Arrays.equals(getBasicValueBytes(testDoubleVar), doubleSerializer.serialize(testDoubleRow)));

		try {
			assertEquals(testIntVar, intSerializer.deserialize(getBasicValueBytes(testIntVar)).getField(0));
			assertEquals(testLongVar, longSerializer.deserialize(getBasicValueBytes(testLongVar)).getField(0));
			assertEquals(testFloatVar, floatSerializer.deserialize(getBasicValueBytes(testFloatVar)).getField(0));
			assertEquals(testDoubleVar, doubleSerializer.deserialize(getBasicValueBytes(testDoubleVar)).getField(0));
		} catch (IOException e) {
			fail();
		}
	}

	private static byte[] getBasicValueBytes(Object val) {
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
			buffer = ByteBuffer.allocate(1);
			buffer.putChar((Character) val);
		} else if (val instanceof Short) {
			buffer = ByteBuffer.allocate(2);
			buffer.putShort((Short) val);
		} else {
			throw new RuntimeException("Unsupported class:" + val.getClass());
		}

		return buffer.array();
	}

	@Test(expected = RuntimeException.class)
	public void testInvalidDataType() {
		TableSchema tableSchema = TableSchema.builder()
			.build();

		new SingleValueRowSerializer(tableSchema);
	}
}
