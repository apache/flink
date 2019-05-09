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
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.disk.RandomAccessInputView;
import org.apache.flink.runtime.io.disk.RandomAccessOutputView;
import org.apache.flink.table.type.InternalTypes;
import org.apache.flink.table.typeutils.BaseRowSerializer;
import org.apache.flink.table.typeutils.BinaryRowSerializer;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.apache.flink.table.dataformat.BinaryString.fromBytes;
import static org.apache.flink.table.dataformat.BinaryString.fromString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
		assertTrue(row.getSegments()[0] == segment);
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
		assertEquals(true, row.getBoolean(4));
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
	public void testWriteString() throws IOException {
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
			writer.writeString(1, fromBytes(str.getBytes()));
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
		assertEquals(true, row.getBoolean(1));
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
	public void anyNullTest() throws IOException {
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

		{
			BinaryRow row = new BinaryRow(80);
			BinaryRowWriter writer = new BinaryRowWriter(row);
			assertFalse(row.anyNull());

			writer.setNullAt(3);
			assertTrue(row.anyNull());

			writer = new BinaryRowWriter(row);
			writer.setNullAt(65);
			assertTrue(row.anyNull());
		}
	}

	@Test
	public void testSingleSegmentBinaryRowHashCode() throws IOException {
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
	public void testHeaderSize() throws IOException {
		assertEquals(8, BinaryRow.calculateBitSetWidthInBytes(56));
		assertEquals(16, BinaryRow.calculateBitSetWidthInBytes(57));
		assertEquals(16, BinaryRow.calculateBitSetWidthInBytes(120));
		assertEquals(24, BinaryRow.calculateBitSetWidthInBytes(121));
	}

	@Test
	public void testHeader() throws IOException {
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
		BaseRowSerializer nestedSer = new BaseRowSerializer(
				new ExecutionConfig(), InternalTypes.STRING, InternalTypes.INT);
		writer.writeRow(0, GenericRow.of(fromString("1"), 1), nestedSer);
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
}
