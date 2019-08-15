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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LocalDateSerializer;
import org.apache.flink.api.common.typeutils.base.LocalDateTimeSerializer;
import org.apache.flink.api.common.typeutils.base.LocalTimeSerializer;
import org.apache.flink.api.common.typeutils.base.SqlDateSerializer;
import org.apache.flink.api.common.typeutils.base.SqlTimeSerializer;
import org.apache.flink.api.common.typeutils.base.SqlTimestampSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.disk.RandomAccessInputView;
import org.apache.flink.runtime.io.disk.RandomAccessOutputView;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.runtime.typeutils.BaseArraySerializer;
import org.apache.flink.table.runtime.typeutils.BaseMapSerializer;
import org.apache.flink.table.runtime.typeutils.BaseRowSerializer;
import org.apache.flink.table.runtime.typeutils.BinaryRowSerializer;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.Assert;
import org.junit.Test;

import java.io.EOFException;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.apache.flink.table.dataformat.BinaryString.fromBytes;
import static org.apache.flink.table.dataformat.BinaryString.fromString;
import static org.apache.flink.table.dataformat.DataFormatTestUtil.MyObj;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Test of {@link BinaryRow} and {@link BinaryRowWriter}.
 */
public class BinaryRowTest {

	@Test
	public void testBasic() {
		// consider header 1 byte.
		assertEquals(8, new BinaryRow(0).getFixedLengthPartSize());
		assertEquals(16, new BinaryRow(1).getFixedLengthPartSize());
		assertEquals(536, new BinaryRow(65).getFixedLengthPartSize());
		assertEquals(1048, new BinaryRow(128).getFixedLengthPartSize());

		MemorySegment segment = MemorySegmentFactory.wrap(new byte[100]);
		BinaryRow row = new BinaryRow(2);
		row.pointTo(segment, 10, 48);
		assertSame(row.getSegments()[0], segment);
		row.setInt(0, 5);
		row.setDouble(1, 5.8D);
	}

	@Test
	public void testSetAndGet() {
		MemorySegment segment = MemorySegmentFactory.wrap(new byte[80]);
		BinaryRow row = new BinaryRow(9);
		row.pointTo(segment, 0, 80);
		row.setNullAt(0);
		row.setInt(1, 11);
		row.setLong(2, 22);
		row.setDouble(3, 33);
		row.setBoolean(4, true);
		row.setShort(5, (short) 55);
		row.setByte(6, (byte) 66);
		row.setFloat(7, 77f);

		assertEquals(33d, (long) row.getDouble(3), 0);
		assertEquals(11, row.getInt(1));
		assertTrue(row.isNullAt(0));
		assertEquals(55, row.getShort(5));
		assertEquals(22, row.getLong(2));
		assertTrue(row.getBoolean(4));
		assertEquals((byte) 66, row.getByte(6));
		assertEquals(77f, row.getFloat(7), 0);
	}

	@Test
	public void testWriter() {

		int arity = 13;
		BinaryRow row = new BinaryRow(arity);
		BinaryRowWriter writer = new BinaryRowWriter(row, 20);

		writer.writeString(0, fromString("1"));
		writer.writeString(3, fromString("1234567"));
		writer.writeString(5, fromString("12345678"));
		writer.writeString(9, fromString("啦啦啦啦啦我是快乐的粉刷匠"));

		writer.writeBoolean(1, true);
		writer.writeByte(2, (byte) 99);
		writer.writeDouble(6, 87.1d);
		writer.writeFloat(7, 26.1f);
		writer.writeInt(8, 88);
		writer.writeLong(10, 284);
		writer.writeShort(11, (short) 292);
		writer.setNullAt(12);

		writer.complete();

		assertTestWriterRow(row);
		assertTestWriterRow(row.copy());

		// test copy from var segments.
		int subSize = row.getFixedLengthPartSize() + 10;
		MemorySegment subMs1 = MemorySegmentFactory.wrap(new byte[subSize]);
		MemorySegment subMs2 = MemorySegmentFactory.wrap(new byte[subSize]);
		row.getSegments()[0].copyTo(0, subMs1, 0, subSize);
		row.getSegments()[0].copyTo(subSize, subMs2, 0, row.getSizeInBytes() - subSize);

		BinaryRow toCopy = new BinaryRow(arity);
		toCopy.pointTo(new MemorySegment[]{subMs1, subMs2}, 0, row.getSizeInBytes());
		assertEquals(row, toCopy);
		assertTestWriterRow(toCopy);
		assertTestWriterRow(toCopy.copy(new BinaryRow(arity)));
	}

	@Test
	public void testWriteString() {
		{
			// litter byte[]
			BinaryRow row = new BinaryRow(1);
			BinaryRowWriter writer = new BinaryRowWriter(row);
			char[] chars = new char[2];
			chars[0] = 0xFFFF;
			chars[1] = 0;
			writer.writeString(0, fromString(new String(chars)));
			writer.complete();

			String str = row.getString(0).toString();
			assertEquals(chars[0], str.charAt(0));
			assertEquals(chars[1], str.charAt(1));
		}

		{
			// big byte[]
			String str = "啦啦啦啦啦我是快乐的粉刷匠";
			BinaryRow row = new BinaryRow(2);
			BinaryRowWriter writer = new BinaryRowWriter(row);
			writer.writeString(0, fromString(str));
			writer.writeString(1, fromBytes(str.getBytes(StandardCharsets.UTF_8)));
			writer.complete();

			assertEquals(str, row.getString(0).toString());
			assertEquals(str, row.getString(1).toString());
		}
	}

	@Test
	public void testPagesSer() throws IOException {
		MemorySegment[] memorySegments = new MemorySegment[5];
		ArrayList<MemorySegment> memorySegmentList = new ArrayList<>();
		for (int i = 0; i < 5; i++) {
			memorySegments[i] = MemorySegmentFactory.wrap(new byte[64]);
			memorySegmentList.add(memorySegments[i]);
		}

		{
			// multi memorySegments
			String str = "啦啦啦啦啦我是快乐的粉刷匠，啦啦啦啦啦我是快乐的粉刷匠，" +
					"啦啦啦啦啦我是快乐的粉刷匠。";
			BinaryRow row = new BinaryRow(1);
			BinaryRowWriter writer = new BinaryRowWriter(row);
			writer.writeString(0, BinaryString.fromString(str));
			writer.complete();

			RandomAccessOutputView out = new RandomAccessOutputView(memorySegments, 64);
			BinaryRowSerializer serializer = new BinaryRowSerializer(1);
			serializer.serializeToPages(row, out);

			BinaryRow mapRow = serializer.mapFromPages(new RandomAccessInputView(memorySegmentList, 64));
			writer.reset();
			writer.writeString(0, mapRow.getString(0));
			writer.complete();
			assertEquals(str, row.getString(0).toString());

			BinaryRow deserRow = serializer.deserializeFromPages(new RandomAccessInputView(memorySegmentList, 64));
			writer.reset();
			writer.writeString(0, deserRow.getString(0));
			writer.complete();
			assertEquals(str, row.getString(0).toString());
		}

		{
			// multi memorySegments
			String str1 = "啦啦啦啦啦我是快乐的粉刷匠，啦啦啦啦啦我是快乐的粉刷匠，" +
					"啦啦啦啦啦我是快乐的粉刷匠。";
			String str2 = "啦啦啦啦啦我是快乐的粉刷匠。";
			BinaryRow row = new BinaryRow(2);
			BinaryRowWriter writer = new BinaryRowWriter(row);
			writer.writeString(0, BinaryString.fromString(str1));
			writer.writeString(1, BinaryString.fromString(str2));
			writer.complete();

			RandomAccessOutputView out = new RandomAccessOutputView(memorySegments, 64);
			out.skipBytesToWrite(40);
			BinaryRowSerializer serializer = new BinaryRowSerializer(2);
			serializer.serializeToPages(row, out);

			RandomAccessInputView in = new RandomAccessInputView(memorySegmentList, 64);
			in.skipBytesToRead(40);
			BinaryRow mapRow = serializer.mapFromPages(in);
			writer.reset();
			writer.writeString(0, mapRow.getString(0));
			writer.writeString(1, mapRow.getString(1));
			writer.complete();
			assertEquals(str1, row.getString(0).toString());
			assertEquals(str2, row.getString(1).toString());

			in = new RandomAccessInputView(memorySegmentList, 64);
			in.skipBytesToRead(40);
			BinaryRow deserRow = serializer.deserializeFromPages(in);
			writer.reset();
			writer.writeString(0, deserRow.getString(0));
			writer.writeString(1, deserRow.getString(1));
			writer.complete();
			assertEquals(str1, row.getString(0).toString());
			assertEquals(str2, row.getString(1).toString());
		}
	}

	private void assertTestWriterRow(BinaryRow row) {
		assertEquals("1", row.getString(0).toString());
		assertEquals(88, row.getInt(8));
		assertEquals((short) 292, row.getShort(11));
		assertEquals(284, row.getLong(10));
		assertEquals((byte) 99, row.getByte(2));
		assertEquals(87.1d, row.getDouble(6), 0);
		assertEquals(26.1f, row.getFloat(7), 0);
		assertTrue(row.getBoolean(1));
		assertEquals("1234567", row.getString(3).toString());
		assertEquals("12345678", row.getString(5).toString());
		assertEquals("啦啦啦啦啦我是快乐的粉刷匠", row.getString(9).toString());
		assertEquals(fromString("啦啦啦啦啦我是快乐的粉刷匠").hashCode(), row.getString(9).hashCode());
		assertTrue(row.isNullAt(12));
	}

	@Test
	public void testReuseWriter() {
		BinaryRow row = new BinaryRow(2);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		writer.writeString(0, fromString("01234567"));
		writer.writeString(1, fromString("012345678"));
		writer.complete();
		assertEquals("01234567", row.getString(0).toString());
		assertEquals("012345678", row.getString(1).toString());

		writer.reset();
		writer.writeString(0, fromString("1"));
		writer.writeString(1, fromString("0123456789"));
		writer.complete();
		assertEquals("1", row.getString(0).toString());
		assertEquals("0123456789", row.getString(1).toString());
	}

	@Test
	public void anyNullTest() {
		{
			BinaryRow row = new BinaryRow(3);
			BinaryRowWriter writer = new BinaryRowWriter(row);
			assertFalse(row.anyNull());

			// test header should not compute by anyNull
			row.setHeader((byte) 1);
			assertFalse(row.anyNull());

			writer.setNullAt(2);
			assertTrue(row.anyNull());

			writer.setNullAt(0);
			assertTrue(row.anyNull(new int[]{0, 1, 2}));
			assertFalse(row.anyNull(new int[]{1}));

			writer.setNullAt(1);
			assertTrue(row.anyNull());
		}

		int numFields = 80;
		for (int i = 0; i < numFields; i++) {
			BinaryRow row = new BinaryRow(numFields);
			BinaryRowWriter writer = new BinaryRowWriter(row);
			row.setHeader((byte) 17);
			assertFalse(row.anyNull());
			writer.setNullAt(i);
			assertTrue(row.anyNull());
		}
	}

	@Test
	public void testSingleSegmentBinaryRowHashCode() {
		final Random rnd = new Random(System.currentTimeMillis());
		// test hash stabilization
		BinaryRow row = new BinaryRow(13);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		for (int i = 0; i < 99; i++) {
			writer.reset();
			writer.writeString(0, fromString("" + rnd.nextInt()));
			writer.writeString(3, fromString("01234567"));
			writer.writeString(5, fromString("012345678"));
			writer.writeString(9, fromString("啦啦啦啦啦我是快乐的粉刷匠"));
			writer.writeBoolean(1, true);
			writer.writeByte(2, (byte) 99);
			writer.writeDouble(6, 87.1d);
			writer.writeFloat(7, 26.1f);
			writer.writeInt(8, 88);
			writer.writeLong(10, 284);
			writer.writeShort(11, (short) 292);
			writer.setNullAt(12);
			writer.complete();
			BinaryRow copy = row.copy();
			assertEquals(row.hashCode(), copy.hashCode());
		}

		// test hash distribution
		int count = 999999;
		Set<Integer> hashCodes = new HashSet<>(count);
		for (int i = 0; i < count; i++) {
			row.setInt(8, i);
			hashCodes.add(row.hashCode());
		}
		assertEquals(count, hashCodes.size());
		hashCodes.clear();
		row = new BinaryRow(1);
		writer = new BinaryRowWriter(row);
		for (int i = 0; i < count; i++) {
			writer.reset();
			writer.writeString(0, fromString("啦啦啦啦啦我是快乐的粉刷匠" + i));
			writer.complete();
			hashCodes.add(row.hashCode());
		}
		Assert.assertTrue(hashCodes.size() > count *  0.997);
	}

	@Test
	public void testHeaderSize() {
		assertEquals(8, BinaryRow.calculateBitSetWidthInBytes(56));
		assertEquals(16, BinaryRow.calculateBitSetWidthInBytes(57));
		assertEquals(16, BinaryRow.calculateBitSetWidthInBytes(120));
		assertEquals(24, BinaryRow.calculateBitSetWidthInBytes(121));
	}

	@Test
	public void testHeader() {
		BinaryRow row = new BinaryRow(2);
		BinaryRowWriter writer = new BinaryRowWriter(row);

		writer.writeInt(0, 10);
		writer.setNullAt(1);
		writer.writeHeader((byte) 29);
		writer.complete();

		BinaryRow newRow = row.copy();
		assertEquals(row, newRow);
		assertEquals((byte) 29, newRow.getHeader());

		newRow.setHeader((byte) 19);
		assertEquals((byte) 19, newRow.getHeader());
	}

	@Test
	public void testDecimal() {
		// 1.compact
		{
			int precision = 4;
			int scale = 2;
			BinaryRow row = new BinaryRow(2);
			BinaryRowWriter writer = new BinaryRowWriter(row);
			writer.writeDecimal(0, Decimal.fromLong(5, precision, scale), precision);
			writer.setNullAt(1);
			writer.complete();

			assertEquals("0.05", row.getDecimal(0, precision, scale).toString());
			assertTrue(row.isNullAt(1));
			row.setDecimal(0, Decimal.fromLong(6, precision, scale), precision);
			assertEquals("0.06", row.getDecimal(0, precision, scale).toString());
		}

		// 2.not compact
		{
			int precision = 25;
			int scale = 5;
			Decimal decimal1 = Decimal.fromBigDecimal(BigDecimal.valueOf(5.55), precision, scale);
			Decimal decimal2 = Decimal.fromBigDecimal(BigDecimal.valueOf(6.55), precision, scale);

			BinaryRow row = new BinaryRow(2);
			BinaryRowWriter writer = new BinaryRowWriter(row);
			writer.writeDecimal(0, decimal1, precision);
			writer.writeDecimal(1, null, precision);
			writer.complete();

			assertEquals("5.55000", row.getDecimal(0, precision, scale).toString());
			assertTrue(row.isNullAt(1));
			row.setDecimal(0, decimal2, precision);
			assertEquals("6.55000", row.getDecimal(0, precision, scale).toString());
		}
	}

	@Test
	public void testGeneric() {
		BinaryRow row = new BinaryRow(3);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		BinaryGeneric<String> hahah = new BinaryGeneric<>("hahah", StringSerializer.INSTANCE);
		writer.writeGeneric(0, hahah);
		writer.setNullAt(1);
		hahah.ensureMaterialized();
		writer.writeGeneric(2, hahah);
		writer.complete();

		BinaryGeneric<String> generic0 = row.getGeneric(0);
		assertEquals(hahah, generic0);
		assertTrue(row.isNullAt(1));
		BinaryGeneric<String> generic2 = row.getGeneric(2);
		assertEquals(hahah, generic2);
	}

	@Test
	public void testNested() {
		BinaryRow row = new BinaryRow(2);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		writer.writeRow(0, GenericRow.of(fromString("1"), 1),
				new BaseRowSerializer(null, RowType.of(new VarCharType(VarCharType.MAX_LENGTH), new IntType())));
		writer.setNullAt(1);
		writer.complete();

		BaseRow nestedRow = row.getRow(0, 2);
		assertEquals("1", nestedRow.getString(0).toString());
		assertEquals(1, nestedRow.getInt(1));
		assertTrue(row.isNullAt(1));
	}

	@Test
	public void testBinary() {
		BinaryRow row = new BinaryRow(2);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		byte[] bytes1 = new byte[] {1, -1, 5};
		byte[] bytes2 = new byte[] {1, -1, 5, 5, 1, 5, 1, 5};
		writer.writeBinary(0, bytes1);
		writer.writeBinary(1, bytes2);
		writer.complete();

		Assert.assertArrayEquals(bytes1, row.getBinary(0));
		Assert.assertArrayEquals(bytes2, row.getBinary(1));
	}

	@Test
	public void testBinaryArray() {
		// 1. array test
		BinaryArray array = new BinaryArray();
		BinaryArrayWriter arrayWriter = new BinaryArrayWriter(
			array, 3, BinaryArray.calculateFixLengthPartSize(DataTypes.INT().getLogicalType()));

		arrayWriter.writeInt(0, 6);
		arrayWriter.setNullInt(1);
		arrayWriter.writeInt(2, 666);
		arrayWriter.complete();

		assertEquals(array.getInt(0), 6);
		assertTrue(array.isNullAt(1));
		assertEquals(array.getInt(2), 666);

		// 2. test write array to binary row
		BinaryRow row = new BinaryRow(1);
		BinaryRowWriter rowWriter = new BinaryRowWriter(row);
		BaseArraySerializer serializer = new BaseArraySerializer(
			DataTypes.INT().getLogicalType(), new ExecutionConfig());
		rowWriter.writeArray(0, array, serializer);
		rowWriter.complete();

		BinaryArray array2 = (BinaryArray) row.getArray(0);
		assertEquals(array, array2);
		assertEquals(6, array2.getInt(0));
		assertTrue(array2.isNullAt(1));
		assertEquals(666, array2.getInt(2));
	}

	@Test
	public void testGenericArray() {
		// 1. array test
		Integer[] javaArray = {6, null, 666};
		GenericArray array = new GenericArray(javaArray, 3, false);

		assertEquals(array.getInt(0), 6);
		assertTrue(array.isNullAt(1));
		assertEquals(array.getInt(2), 666);

		// 2. test write array to binary row
		BinaryRow row2 = new BinaryRow(1);
		BinaryRowWriter writer2 = new BinaryRowWriter(row2);
		BaseArraySerializer serializer = new BaseArraySerializer(
			DataTypes.INT().getLogicalType(), new ExecutionConfig());
		writer2.writeArray(0, array, serializer);
		writer2.complete();

		BaseArray array2 = row2.getArray(0);
		assertEquals(6, array2.getInt(0));
		assertTrue(array2.isNullAt(1));
		assertEquals(666, array2.getInt(2));
	}

	@Test
	public void testBinaryMap() {
		BinaryArray array1 = new BinaryArray();
		BinaryArrayWriter writer1 = new BinaryArrayWriter(
			array1, 4, BinaryArray.calculateFixLengthPartSize(DataTypes.INT().getLogicalType()));
		writer1.writeInt(0, 6);
		writer1.writeInt(1, 5);
		writer1.writeInt(2, 666);
		writer1.writeInt(3, 0);
		writer1.complete();

		BinaryArray array2 = new BinaryArray();
		BinaryArrayWriter writer2 = new BinaryArrayWriter(
			array2, 4, BinaryArray.calculateFixLengthPartSize(DataTypes.STRING().getLogicalType()));
		writer2.writeString(0, BinaryString.fromString("6"));
		writer2.writeString(1, BinaryString.fromString("5"));
		writer2.writeString(2, BinaryString.fromString("666"));
		writer2.setNullAt(3, DataTypes.STRING().getLogicalType());
		writer2.complete();

		BinaryMap binaryMap = BinaryMap.valueOf(array1, array2);

		BinaryRow row = new BinaryRow(1);
		BinaryRowWriter rowWriter = new BinaryRowWriter(row);
		BaseMapSerializer serializer = new BaseMapSerializer(
			DataTypes.STRING().getLogicalType(),
			DataTypes.INT().getLogicalType(),
			new ExecutionConfig());
		rowWriter.writeMap(0, binaryMap, serializer);
		rowWriter.complete();

		BinaryMap map = (BinaryMap) row.getMap(0);
		BinaryArray key = map.keyArray();
		BinaryArray value = map.valueArray();

		assertEquals(binaryMap, map);
		assertEquals(array1, key);
		assertEquals(array2, value);

		assertEquals(5, key.getInt(1));
		assertEquals(BinaryString.fromString("5"), value.getString(1));
		assertEquals(0, key.getInt(3));
		assertTrue(value.isNullAt(3));
	}

	@Test
	public void testGenericMap() {
		Map javaMap = new HashMap();
		javaMap.put(6, BinaryString.fromString("6"));
		javaMap.put(5, BinaryString.fromString("5"));
		javaMap.put(666, BinaryString.fromString("666"));
		javaMap.put(0, null);

		GenericMap genericMap = new GenericMap(javaMap);

		BinaryRow row = new BinaryRow(1);
		BinaryRowWriter rowWriter = new BinaryRowWriter(row);
		BaseMapSerializer serializer = new BaseMapSerializer(
			DataTypes.INT().getLogicalType(),
			DataTypes.STRING().getLogicalType(),
			new ExecutionConfig());
		rowWriter.writeMap(0, genericMap, serializer);
		rowWriter.complete();

		Map map = row.getMap(0).toJavaMap(DataTypes.INT().getLogicalType(), DataTypes.STRING().getLogicalType());
		assertEquals(BinaryString.fromString("6"), map.get(6));
		assertEquals(BinaryString.fromString("5"), map.get(5));
		assertEquals(BinaryString.fromString("666"), map.get(666));
		assertTrue(map.containsKey(0));
		assertNull(map.get(0));
	}

	@Test
	public void testGenericObject() throws Exception {

		GenericTypeInfo<MyObj> info = new GenericTypeInfo<>(MyObj.class);
		TypeSerializer<MyObj> genericSerializer = info.createSerializer(new ExecutionConfig());

		BinaryRow row = new BinaryRow(4);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		writer.writeInt(0, 0);

		BinaryGeneric<MyObj> myObj1 = new BinaryGeneric<>(new MyObj(0, 1), genericSerializer);
		writer.writeGeneric(1, myObj1);
		BinaryGeneric<MyObj> myObj2 = new BinaryGeneric<>(new MyObj(123, 5.0), genericSerializer);
		myObj2.ensureMaterialized();
		writer.writeGeneric(2, myObj2);
		BinaryGeneric<MyObj> myObj3 = new BinaryGeneric<>(new MyObj(1, 1), genericSerializer);
		writer.writeGeneric(3, myObj3);
		writer.complete();

		assertTestGenericObjectRow(row, genericSerializer);

		// getBytes from var-length memorySegments.
		BinaryRowSerializer serializer = new BinaryRowSerializer(4);
		MemorySegment[] memorySegments = new MemorySegment[3];
		ArrayList<MemorySegment> memorySegmentList = new ArrayList<>();
		for (int i = 0; i < 3; i++) {
			memorySegments[i] = MemorySegmentFactory.wrap(new byte[64]);
			memorySegmentList.add(memorySegments[i]);
		}
		RandomAccessOutputView out = new RandomAccessOutputView(memorySegments, 64);
		serializer.serializeToPages(row, out);

		BinaryRow mapRow = serializer.mapFromPages(new RandomAccessInputView(memorySegmentList, 64));
		assertTestGenericObjectRow(mapRow, genericSerializer);
	}

	private void assertTestGenericObjectRow(BinaryRow row, TypeSerializer<MyObj> serializer) {
		assertEquals(0, row.getInt(0));
		BinaryGeneric<MyObj> binaryGeneric1 = row.getGeneric(1);
		BinaryGeneric<MyObj> binaryGeneric2 = row.getGeneric(2);
		BinaryGeneric<MyObj> binaryGeneric3 = row.getGeneric(3);
		assertEquals(new MyObj(0, 1), BinaryGeneric.getJavaObjectFromBinaryGeneric(binaryGeneric1, serializer));
		assertEquals(new MyObj(123, 5.0), BinaryGeneric.getJavaObjectFromBinaryGeneric(binaryGeneric2, serializer));
		assertEquals(new MyObj(1, 1), BinaryGeneric.getJavaObjectFromBinaryGeneric(binaryGeneric3, serializer));
	}

	@Test
	public void testDateAndTimeAsGenericObject() {
		BinaryRow row = new BinaryRow(7);
		BinaryRowWriter writer = new BinaryRowWriter(row);

		LocalDate localDate = LocalDate.of(2019, 7, 16);
		LocalTime localTime = LocalTime.of(17, 31);
		LocalDateTime localDateTime = LocalDateTime.of(localDate, localTime);

		writer.writeInt(0, 0);
		writer.writeGeneric(1, new BinaryGeneric<>(new Date(123), SqlDateSerializer.INSTANCE));
		writer.writeGeneric(2, new BinaryGeneric<>(new Time(456), SqlTimeSerializer.INSTANCE));
		writer.writeGeneric(3, new BinaryGeneric<>(new Timestamp(789), SqlTimestampSerializer.INSTANCE));
		writer.writeGeneric(4, new BinaryGeneric<>(localDate, LocalDateSerializer.INSTANCE));
		writer.writeGeneric(5, new BinaryGeneric<>(localTime, LocalTimeSerializer.INSTANCE));
		writer.writeGeneric(6, new BinaryGeneric<>(localDateTime, LocalDateTimeSerializer.INSTANCE));
		writer.complete();

		assertEquals(new Date(123), BinaryGeneric.getJavaObjectFromBinaryGeneric(
			row.getGeneric(1), SqlDateSerializer.INSTANCE));
		assertEquals(new Time(456), BinaryGeneric.getJavaObjectFromBinaryGeneric(
			row.getGeneric(2), SqlTimeSerializer.INSTANCE));
		assertEquals(new Timestamp(789), BinaryGeneric.getJavaObjectFromBinaryGeneric(
			row.getGeneric(3), SqlTimestampSerializer.INSTANCE));
		assertEquals(localDate, BinaryGeneric.getJavaObjectFromBinaryGeneric(
			row.getGeneric(4), LocalDateSerializer.INSTANCE));
		assertEquals(localTime, BinaryGeneric.getJavaObjectFromBinaryGeneric(
			row.getGeneric(5), LocalTimeSerializer.INSTANCE));
		assertEquals(localDateTime, BinaryGeneric.getJavaObjectFromBinaryGeneric(
			row.getGeneric(6), LocalDateTimeSerializer.INSTANCE));
	}

	@Test
	public void testSerializeVariousSize() throws IOException {
		// in this test, we are going to start serializing from the i-th byte (i in 0...`segSize`)
		// and the size of the row we're going to serialize is j bytes
		// (j in `rowFixLength` to the maximum length we can write)

		int segSize = 64;
		int segTotalNumber = 3;

		BinaryRow row = new BinaryRow(1);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		Random random = new Random();
		byte[] bytes = new byte[1024];
		random.nextBytes(bytes);
		writer.writeBinary(0, bytes);
		writer.complete();

		MemorySegment[] memorySegments = new MemorySegment[segTotalNumber];
		Map<MemorySegment, Integer> msIndex = new HashMap<>();
		for (int i = 0; i < segTotalNumber; i++) {
			memorySegments[i] = MemorySegmentFactory.wrap(new byte[segSize]);
			msIndex.put(memorySegments[i], i);
		}

		BinaryRowSerializer serializer = new BinaryRowSerializer(1);

		int rowSizeInt = 4;
		// note that as there is only one field in the row, the fixed-length part is 16 bytes (header + 1 field)
		int rowFixLength = 16;
		for (int i = 0; i < segSize; i++) {
			// this is the maximum row size we can serialize
			// if we are going to serialize from the i-th byte of the input view
			int maxRowSize = (segSize * segTotalNumber) - i - rowSizeInt;
			if (segSize - i < rowFixLength + rowSizeInt) {
				// oops, we can't write the whole fixed-length part in the first segment
				// because the remaining space is too small, so we have to start serializing from the second segment.
				// when serializing, we need to first write the length of the row,
				// then write the fixed-length part of the row.
				maxRowSize -= segSize - i;
			}
			for (int j = rowFixLength; j < maxRowSize; j++) {
				// ok, now we're going to serialize a row of j bytes
				testSerialize(row, memorySegments, msIndex, serializer, i, j);
			}
		}
	}

	private void testSerialize(
		BinaryRow row, MemorySegment[] memorySegments,
		Map<MemorySegment, Integer> msIndex, BinaryRowSerializer serializer, int position,
		int rowSize) throws IOException {
		RandomAccessOutputView out = new RandomAccessOutputView(memorySegments, 64);
		out.skipBytesToWrite(position);
		row.setTotalSize(rowSize);

		// this `row` contains random bytes, and now we're going to serialize `rowSize` bytes
		// (not including the row header) of the contents
		serializer.serializeToPages(row, out);

		// let's see how many segments we have written
		int segNumber = msIndex.get(out.getCurrentSegment()) + 1;
		int lastSegSize = out.getCurrentPositionInSegment();

		// now deserialize from the written segments
		ArrayList<MemorySegment> segments = new ArrayList<>(Arrays.asList(memorySegments).subList(0, segNumber));
		RandomAccessInputView input = new RandomAccessInputView(segments, 64, lastSegSize);
		input.skipBytesToRead(position);
		BinaryRow mapRow = serializer.mapFromPages(input);

		assertEquals(row, mapRow);
	}

	@Test
	public void testZeroOutPaddingGeneric() {

		GenericTypeInfo<MyObj> info = new GenericTypeInfo<>(MyObj.class);
		TypeSerializer<MyObj> genericSerializer = info.createSerializer(new ExecutionConfig());

		Random random = new Random();
		byte[] bytes = new byte[1024];

		BinaryRow row = new BinaryRow(1);
		BinaryRowWriter writer = new BinaryRowWriter(row);

		// let's random the bytes
		writer.reset();
		random.nextBytes(bytes);
		writer.writeBinary(0, bytes);
		writer.reset();
		writer.writeGeneric(0, new BinaryGeneric<>(new MyObj(0, 1), genericSerializer));
		writer.complete();
		int hash1 = row.hashCode();

		writer.reset();
		random.nextBytes(bytes);
		writer.writeBinary(0, bytes);
		writer.reset();
		writer.writeGeneric(0, new BinaryGeneric<>(new MyObj(0, 1), genericSerializer));
		writer.complete();
		int hash2 = row.hashCode();

		assertEquals(hash1, hash2);
	}

	@Test
	public void testZeroOutPaddingString() {

		Random random = new Random();
		byte[] bytes = new byte[1024];

		BinaryRow row = new BinaryRow(1);
		BinaryRowWriter writer = new BinaryRowWriter(row);

		writer.reset();
		random.nextBytes(bytes);
		writer.writeBinary(0, bytes);
		writer.reset();
		writer.writeString(0, BinaryString.fromString("wahahah"));
		writer.complete();
		int hash1 = row.hashCode();

		writer.reset();
		random.nextBytes(bytes);
		writer.writeBinary(0, bytes);
		writer.reset();
		writer.writeString(0, BinaryString.fromString("wahahah"));
		writer.complete();
		int hash2 = row.hashCode();

		assertEquals(hash1, hash2);
	}

	@Test
	public void testHashAndCopy() throws IOException {
		MemorySegment[] segments = new MemorySegment[3];
		for (int i = 0; i < 3; i++) {
			segments[i] = MemorySegmentFactory.wrap(new byte[64]);
		}
		RandomAccessOutputView out = new RandomAccessOutputView(segments, 64);
		BinaryRowSerializer serializer = new BinaryRowSerializer(2);

		BinaryRow row = new BinaryRow(2);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		writer.writeString(0, BinaryString.fromString("hahahahahahahahahahahahahahahahahahahhahahahahahahahahah"));
		writer.writeString(1, BinaryString.fromString("hahahahahahahahahahahahahahahahahahahhahahahahahahahahaa"));
		writer.complete();
		serializer.serializeToPages(row, out);

		ArrayList<MemorySegment> segmentList = new ArrayList<>(Arrays.asList(segments));
		RandomAccessInputView input = new RandomAccessInputView(segmentList, 64, 64);

		BinaryRow mapRow = serializer.mapFromPages(input);
		assertEquals(row, mapRow);
		assertEquals(row.getString(0), mapRow.getString(0));
		assertEquals(row.getString(1), mapRow.getString(1));
		assertNotEquals(row.getString(0), mapRow.getString(1));

		// test if the hash code before and after serialization are the same
		assertEquals(row.hashCode(), mapRow.hashCode());
		assertEquals(row.getString(0).hashCode(), mapRow.getString(0).hashCode());
		assertEquals(row.getString(1).hashCode(), mapRow.getString(1).hashCode());

		// test if the copy method produce a row with the same contents
		assertEquals(row.copy(), mapRow.copy());
		assertEquals(row.getString(0).copy(), mapRow.getString(0).copy());
		assertEquals(row.getString(1).copy(), mapRow.getString(1).copy());
	}

	@Test
	public void testSerStringToKryo() throws IOException {
		KryoSerializer<BinaryString> serializer = new KryoSerializer<>(
			BinaryString.class, new ExecutionConfig());

		BinaryString string = BinaryString.fromString("hahahahaha");
		RandomAccessOutputView out = new RandomAccessOutputView(
			new MemorySegment[]{MemorySegmentFactory.wrap(new byte[1024])}, 64);
		serializer.serialize(string, out);

		RandomAccessInputView input = new RandomAccessInputView(
			new ArrayList<>(Collections.singletonList(out.getCurrentSegment())), 64, 64);
		BinaryString newStr = serializer.deserialize(input);

		assertEquals(string, newStr);
	}

	@Test
	public void testSerializerPages() throws IOException {
		// Boundary tests
		BinaryRow row24 = DataFormatTestUtil.get24BytesBinaryRow();
		BinaryRow row160 = DataFormatTestUtil.get160BytesBinaryRow();
		testSerializerPagesInternal(row24, row160);
		testSerializerPagesInternal(row24, DataFormatTestUtil.getMultiSeg160BytesBinaryRow(row160));
	}

	private void testSerializerPagesInternal(BinaryRow row24, BinaryRow row160) throws IOException {
		BinaryRowSerializer serializer = new BinaryRowSerializer(2);

		// 1. test middle row with just on the edge1
		{
			MemorySegment[] segments = new MemorySegment[4];
			for (int i = 0; i < segments.length; i++) {
				segments[i] = MemorySegmentFactory.wrap(new byte[64]);
			}
			RandomAccessOutputView out = new RandomAccessOutputView(segments, segments[0].size());
			serializer.serializeToPages(row24, out);
			serializer.serializeToPages(row160, out);
			serializer.serializeToPages(row24, out);

			RandomAccessInputView in = new RandomAccessInputView(
				new ArrayList<>(Arrays.asList(segments)),
				segments[0].size(),
				out.getCurrentPositionInSegment());

			BinaryRow retRow = new BinaryRow(2);
			List<BinaryRow> rets = new ArrayList<>();
			while (true) {
				try {
					retRow = serializer.mapFromPages(retRow, in);
				} catch (EOFException e) {
					break;
				}
				rets.add(retRow.copy());
			}
			assertEquals(row24, rets.get(0));
			assertEquals(row160, rets.get(1));
			assertEquals(row24, rets.get(2));
		}

		// 2. test middle row with just on the edge2
		{
			MemorySegment[] segments = new MemorySegment[7];
			for (int i = 0; i < segments.length; i++) {
				segments[i] = MemorySegmentFactory.wrap(new byte[64]);
			}
			RandomAccessOutputView out = new RandomAccessOutputView(segments, segments[0].size());
			serializer.serializeToPages(row24, out);
			serializer.serializeToPages(row160, out);
			serializer.serializeToPages(row160, out);

			RandomAccessInputView in = new RandomAccessInputView(
				new ArrayList<>(Arrays.asList(segments)),
				segments[0].size(),
				out.getCurrentPositionInSegment());

			BinaryRow retRow = new BinaryRow(2);
			List<BinaryRow> rets = new ArrayList<>();
			while (true) {
				try {
					retRow = serializer.mapFromPages(retRow, in);
				} catch (EOFException e) {
					break;
				}
				rets.add(retRow.copy());
			}
			assertEquals(row24, rets.get(0));
			assertEquals(row160, rets.get(1));
			assertEquals(row160, rets.get(2));
		}

		// 3. test last row with just on the edge
		{
			MemorySegment[] segments = new MemorySegment[3];
			for (int i = 0; i < segments.length; i++) {
				segments[i] = MemorySegmentFactory.wrap(new byte[64]);
			}
			RandomAccessOutputView out = new RandomAccessOutputView(segments, segments[0].size());
			serializer.serializeToPages(row24, out);
			serializer.serializeToPages(row160, out);

			RandomAccessInputView in = new RandomAccessInputView(
				new ArrayList<>(Arrays.asList(segments)),
				segments[0].size(),
				out.getCurrentPositionInSegment());

			BinaryRow retRow = new BinaryRow(2);
			List<BinaryRow> rets = new ArrayList<>();
			while (true) {
				try {
					retRow = serializer.mapFromPages(retRow, in);
				} catch (EOFException e) {
					break;
				}
				rets.add(retRow.copy());
			}
			assertEquals(row24, rets.get(0));
			assertEquals(row160, rets.get(1));
		}
	}
}
