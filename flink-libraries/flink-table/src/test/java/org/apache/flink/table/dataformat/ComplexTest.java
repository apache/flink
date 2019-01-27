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

package org.apache.flink.table.dataformat;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.TypeConverters;
import org.apache.flink.table.dataformat.BinaryRowTest.MyObj;
import org.apache.flink.table.dataformat.util.BaseRowUtil;
import org.apache.flink.table.typeutils.BaseRowComparator;
import org.apache.flink.table.typeutils.BaseRowSerializer;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.dataformat.BinaryString.fromString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test for nestedRow, array, map.
 */
public class ComplexTest {

	@Test
	public void testNestedRow() {
		BinaryRow row = new BinaryRow(1);
		BinaryRowWriter writer = new BinaryRowWriter(row);

		GenericTypeInfo info = new GenericTypeInfo(MyObj.class);
		TypeSerializer genericSerializer = info.createSerializer(new ExecutionConfig());
		GenericRow gRow = new GenericRow(5);
		gRow.update(0, 1);
		gRow.update(1, 5L);
		gRow.update(2, fromString("12345678"));
		gRow.update(3, null);
		gRow.update(4, new MyObj(15, 5));

		BaseRowSerializer<GenericRow> serializer = new BaseRowSerializer<>(
				Types.INT(), Types.LONG(), Types.STRING(), Types.STRING(), info);
		BaseRowUtil.writeBaseRow(writer, 0, gRow, serializer);
		writer.complete();

		{
			BaseRow nestedRow = row.getBaseRow(0, 5);
			assertEquals(nestedRow.getInt(0), 1);
			assertEquals(nestedRow.getLong(1), 5L);
			assertEquals(nestedRow.getBinaryString(2), fromString("12345678"));
			assertTrue(nestedRow.isNullAt(3));
			assertEquals(new MyObj(15, 5), nestedRow.getGeneric(4, genericSerializer));
		}

		MemorySegment[] segments = splitBytes(row.getMemorySegment().getHeapMemory(), 3);
		row.pointTo(segments, 3, row.getSizeInBytes());
		{
			BaseRow nestedRow = row.getBaseRow(0, 5);
			assertEquals(nestedRow.getInt(0), 1);
			assertEquals(nestedRow.getLong(1), 5L);
			assertEquals(nestedRow.getBinaryString(2), fromString("12345678"));
			assertTrue(nestedRow.isNullAt(3));
			assertEquals(new MyObj(15, 5), nestedRow.getGeneric(4, genericSerializer));
		}
	}

	@Test
	public void testNestInNestedRow() {
		//1.layer1
		GenericRow gRow = new GenericRow(4);
		gRow.update(0, 1);
		gRow.update(1, 5L);
		gRow.update(2, fromString("12345678"));
		gRow.update(3, null);

		//2.layer2
		BaseRowSerializer<GenericRow> serializer = new BaseRowSerializer<>(
				Types.INT(), Types.LONG(), Types.STRING(), Types.STRING());
		BinaryRow row = new BinaryRow(2);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		writer.writeString(0, "hahahahafff");
		BaseRowUtil.writeBaseRow(writer, 1, gRow, serializer);
		writer.complete();

		//3.layer3
		BinaryRow row2 = new BinaryRow(1);
		BinaryRowWriter writer2 = new BinaryRowWriter(row2);
		BaseRowUtil.writeBaseRow(writer2, 0, row, null);
		writer2.complete();

		// verify
		{
			NestedRow nestedRow = (NestedRow) row2.getBaseRow(0, 2);
			BinaryRow binaryRow = new BinaryRow(2);
			binaryRow.pointTo(nestedRow.getSegments(), nestedRow.getBaseOffset(),
					nestedRow.getSizeInBytes());
			assertEquals(binaryRow, row);
		}

		assertEquals(row2.getBaseRow(0, 2).getString(0), "hahahahafff");
		BaseRow nestedRow = row2.getBaseRow(0, 2).getBaseRow(1, 4);
		assertEquals(nestedRow.getInt(0), 1);
		assertEquals(nestedRow.getLong(1), 5L);
		assertEquals(nestedRow.getBinaryString(2), fromString("12345678"));
		assertTrue(nestedRow.isNullAt(3));
	}

	@Test
	public void testGenericArray() {
		// 1. array test
		Integer[] javaArray = {6, null, 666};
		GenericArray array = new GenericArray(javaArray, 3, false);

		assertEquals(array.getInt(0), 6);
		assertTrue(array.isNullAt(1));
		assertEquals(array.getInt(2), 666);

		// 2. test write array to binary row.
		BinaryRow row2 = new BinaryRow(1);
		BinaryRowWriter writer2 = new BinaryRowWriter(row2);
		InternalType at = DataTypes.createArrayType(DataTypes.INT);
		BaseRowUtil.write(
			writer2, 0, array, at,
			TypeConverters.createInternalTypeInfoFromDataType(at)
				.createSerializer(new ExecutionConfig()));
		writer2.complete();

		BaseArray array2 = row2.getBaseArray(0);
		assertEquals(6, array2.getInt(0));
		assertTrue(array2.isNullAt(1));
		assertEquals(666, array2.getInt(2));
	}

	@Test
	public void testBinaryArray() {
		// 1. array test
		BinaryArray array = new BinaryArray();
		BinaryArrayWriter writer = new BinaryArrayWriter(
				array, 3, BinaryArray.calculateElementSize(DataTypes.INT));

		writer.writeInt(0, 6);
		writer.setNullInt(1);
		writer.writeInt(2, 666);
		writer.complete();

		assertEquals(array.getInt(0), 6);
		assertTrue(array.isNullAt(1));
		assertEquals(array.getInt(2), 666);

		// 2. test write array to binary row.
		BinaryRow row2 = new BinaryRow(1);
		BinaryRowWriter writer2 = new BinaryRowWriter(row2);
		writer2.writeBinaryArray(0, array);
		writer2.complete();

		BinaryArray array2 = (BinaryArray) row2.getBaseArray(0);
		assertEquals(array, array2);
		assertEquals(6, array2.getInt(0));
		assertTrue(array2.isNullAt(1));
		assertEquals(666, array2.getInt(2));
	}

	@Test
	public void testGenericMap() {
		Map<Integer, BinaryString> javaMap = new HashMap<>();
		javaMap.put(6, fromString("6"));
		javaMap.put(5, fromString("5"));
		javaMap.put(666, fromString("666"));
		javaMap.put(0, null);

		GenericMap genericMap = new GenericMap(javaMap);

		BinaryRow row = new BinaryRow(1);
		BinaryRowWriter rowWriter = new BinaryRowWriter(row);
		InternalType mt = DataTypes.createMapType(DataTypes.INT, DataTypes.STRING);
		BaseRowUtil.write(
			rowWriter, 0, genericMap, mt,
			TypeConverters.createInternalTypeInfoFromDataType(mt)
				.createSerializer(new ExecutionConfig()));
		rowWriter.complete();

		Map map = row.getBaseMap(0).toJavaMap(DataTypes.INT, DataTypes.STRING);
		assertEquals(fromString("6"), map.get(6));
		assertEquals(fromString("5"), map.get(5));
		assertEquals(fromString("666"), map.get(666));
		assertTrue(map.containsKey(0));
		assertNull(map.get(0));
	}

	@Test
	public void testBinaryMap() {
		BinaryArray array1 = new BinaryArray();
		BinaryArrayWriter writer1 = new BinaryArrayWriter(
				array1, 4, BinaryArray.calculateElementSize(DataTypes.INT));
		writer1.writeInt(0, 6);
		writer1.writeInt(1, 5);
		writer1.writeInt(2, 666);
		writer1.writeInt(3, 0);
		writer1.complete();

		BinaryArray array2 = new BinaryArray();
		BinaryArrayWriter writer2 = new BinaryArrayWriter(
				array2, 4, BinaryArray.calculateElementSize(DataTypes.STRING));
		writer2.writeBinaryString(0, fromString("6"));
		writer2.writeBinaryString(1, fromString("5"));
		writer2.writeBinaryString(2, fromString("666"));
		writer2.setNullAt(3, DataTypes.STRING);
		writer2.complete();

		BinaryMap binaryMap = BinaryMap.valueOf(array1, array2);

		BinaryRow row = new BinaryRow(1);
		BinaryRowWriter rowWriter = new BinaryRowWriter(row);
		rowWriter.writeBinaryMap(0, binaryMap);
		rowWriter.complete();

		BinaryMap map = (BinaryMap) row.getBaseMap(0);
		BinaryArray key = map.keyArray();
		BinaryArray value = map.valueArray();

		assertEquals(binaryMap, map);
		assertEquals(array1, key);
		assertEquals(array2, value);

		assertEquals(5, key.getInt(1));
		assertEquals(fromString("5"), value.getBinaryString(1));
		assertEquals(0, key.getInt(3));
		assertTrue(value.isNullAt(3));
	}

	@Test
	public void testComparator() {
		GenericRow row1 = new GenericRow(3);
		row1.update(0, 5);
		row1.update(1, 5);
		row1.update(2, 20);

		GenericRow row2 = new GenericRow(3);
		row2.update(0, 5);
		row2.update(1, 5);
		row2.update(2, 30);

		BaseRowComparator comparator = new BaseRowComparator(
				new TypeInformation[]{Types.INT(), Types.INT(), Types.INT()}, true);
		int cmp = comparator.compare(row1, row2);
		assertTrue(cmp < 0);
	}

	public static MemorySegment[] splitBytes(byte[] bytes, int baseOffset) {
		int newSize = (bytes.length + 1) / 2 + baseOffset;
		MemorySegment[] ret = new MemorySegment[2];
		ret[0] = MemorySegmentFactory.wrap(new byte[newSize]);
		ret[1] = MemorySegmentFactory.wrap(new byte[newSize]);

		ret[0].put(baseOffset, bytes, 0, newSize - baseOffset);
		ret[1].put(0, bytes, newSize - baseOffset, bytes.length - (newSize - baseOffset));
		return ret;
	}

	@Test
	public void testToArray() {
		BinaryArray array = new BinaryArray();
		BinaryArrayWriter writer = new BinaryArrayWriter(
				array, 3, BinaryArray.calculateElementSize(DataTypes.SHORT));
		writer.writeShort(0, (short) 5);
		writer.writeShort(1, (short) 10);
		writer.writeShort(2, (short) 15);
		writer.complete();

		short[] shorts = array.toShortArray();
		assertEquals(5, shorts[0]);
		assertEquals(10, shorts[1]);
		assertEquals(15, shorts[2]);

		MemorySegment[] segments = splitBytes(writer.segment.getHeapMemory(), 3);
		array.pointTo(segments, 3, array.getSizeInBytes());
		short[] shorts2 = array.toShortArray();
		assertEquals(5, shorts2[0]);
		assertEquals(10, shorts2[1]);
		assertEquals(15, shorts2[2]);
	}
}
