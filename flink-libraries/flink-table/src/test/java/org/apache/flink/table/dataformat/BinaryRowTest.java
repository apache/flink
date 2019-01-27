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
import org.apache.flink.api.common.typeutils.base.SqlDateSerializer;
import org.apache.flink.api.common.typeutils.base.SqlTimeSerializer;
import org.apache.flink.api.common.typeutils.base.SqlTimestampSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.TestDataOutputSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.disk.RandomAccessInputView;
import org.apache.flink.runtime.io.disk.RandomAccessOutputView;
import org.apache.flink.table.api.types.Types;
import org.apache.flink.table.typeutils.BaseRowSerializer;
import org.apache.flink.table.typeutils.BinaryRowSerializer;
import org.apache.flink.table.util.hash.Murmur32;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
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
		assertTrue(row.getAllSegments()[0] == segment);
		row.setInt(0, 5);
		row.setDouble(1, 5.8D);
		row.toString(); //check no exception.
		assertEquals("[0,5,5.8]", row.toOriginString(Types.INT, Types.DOUBLE));
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
		row.setChar(8, 'a');

		assertEquals(33d, (long) row.getDouble(3), 0);
		assertEquals(11, row.getInt(1));
		assertTrue(row.isNullAt(0));
		assertEquals(55, row.getShort(5));
		assertEquals(22, row.getLong(2));
		assertEquals(true, row.getBoolean(4));
		assertEquals((byte) 66, row.getByte(6));
		assertEquals(77f, row.getFloat(7), 0);
		assertEquals('a', row.getChar(8));
	}

	@Test
	public void testWriter() {

		int arity = 13;
		BinaryRow row = new BinaryRow(arity);
		BinaryRowWriter writer = new BinaryRowWriter(row, 20);

		writer.writeString(0, "1");
		writer.writeString(3, "1234567");
		writer.writeString(5, "12345678");
		writer.writeBinaryString(9, BinaryString.fromString("啦啦啦啦啦我是快乐的粉刷匠"));

		writer.writeBoolean(1, true);
		writer.writeByte(2, (byte) 99);
		writer.writeChar(4, 'x');
		writer.writeDouble(6, 87.1d);
		writer.writeFloat(7, 26.1f);
		writer.writeInt(8, 88);
		writer.writeLong(10, 284);
		writer.writeShort(11, (short) 292);
		writer.setNullAt(12);

		writer.complete();

		assertTestWriterRow(row);
		assertTestWriterRow(row.copy(new BinaryRow(arity)));

		// test copy from var segments.
		int subSize = row.getFixedLengthPartSize() + 10;
		MemorySegment subMs1 = MemorySegmentFactory.wrap(new byte[subSize]);
		MemorySegment subMs2 = MemorySegmentFactory.wrap(new byte[subSize]);
		row.getMemorySegment().copyTo(0, subMs1, 0, subSize);
		row.getMemorySegment().copyTo(subSize, subMs2, 0, row.getSizeInBytes() - subSize);

		BinaryRow toCopy = new BinaryRow(arity);
		toCopy.pointTo(new MemorySegment[]{subMs1, subMs2}, 0, row.getSizeInBytes());
		assertTestWriterRow(toCopy);
		assertTestWriterRow(toCopy.copy(new BinaryRow(arity)));
	}

	@Test
	public void testWriteLittleString() {
		BinaryRow row = new BinaryRow(1);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		char[] chars = new char[2];
		chars[0] = 0xFFFF;
		chars[1] = 0;
		writer.writeString(0, new String(chars));
		writer.complete();

		String str = row.getString(0);
		assertEquals(chars[0], str.charAt(0));
		assertEquals(chars[1], str.charAt(1));
	}

	@Test
	public void testWriteBinaryString() throws IOException {
		{
			// litter string
			BinaryRow row = new BinaryRow(1);
			BinaryRowWriter writer = new BinaryRowWriter(row);
			char[] chars = new char[2];
			chars[0] = 0xFFFF;
			chars[1] = 0;
			writer.writeBinaryString(0, BinaryString.fromString(new String(chars)));
			writer.complete();

			String str = row.getString(0);
			assertEquals(chars[0], str.charAt(0));
			assertEquals(chars[1], str.charAt(1));
		}

		{
			// big string
			String str = "啦啦啦啦啦我是快乐的粉刷匠";
			BinaryRow row = new BinaryRow(1);
			BinaryRowWriter writer = new BinaryRowWriter(row);
			writer.writeBinaryString(0, BinaryString.fromString(str));
			writer.complete();

			assertEquals(str, row.getString(0));
		}

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
			writer.writeBinaryString(0, BinaryString.fromString(str));
			writer.complete();

			RandomAccessOutputView out = new RandomAccessOutputView(memorySegments, 64);
			BinaryRowSerializer serializer = new BinaryRowSerializer(Types.STRING);
			serializer.serializeToPages(row, out);
			BinaryRow mapRow = serializer.mapFromPages(new RandomAccessInputView(memorySegmentList, 64));

			writer.reset();
			writer.writeBinaryString(0, mapRow.getBinaryString(0));

			assertEquals(str, row.getString(0));
		}

		{
			// multi memorySegments
			String str1 = "啦啦啦啦啦我是快乐的粉刷匠，啦啦啦啦啦我是快乐的粉刷匠，" +
					"啦啦啦啦啦我是快乐的粉刷匠。";
			String str2 = "啦啦啦啦啦我是快乐的粉刷匠。";
			BinaryRow row = new BinaryRow(2);
			BinaryRowWriter writer = new BinaryRowWriter(row);
			writer.writeBinaryString(0, BinaryString.fromString(str1));
			writer.writeBinaryString(1, BinaryString.fromString(str2));
			writer.complete();

			RandomAccessOutputView out = new RandomAccessOutputView(memorySegments, 64);
			out.skipBytesToWrite(40);
			BinaryRowSerializer serializer = new BinaryRowSerializer(Types.STRING, Types.STRING);
			serializer.serializeToPages(row, out);

			RandomAccessInputView in = new RandomAccessInputView(memorySegmentList, 64);
			in.skipBytesToRead(40);
			BinaryRow mapRow = serializer.mapFromPages(in);

			writer.reset();
			writer.writeBinaryString(0, mapRow.getBinaryString(0));
			writer.writeBinaryString(1, mapRow.getBinaryString(1));

			assertEquals(str1, row.getString(0));
			assertEquals(str2, row.getString(1));
		}
	}

	private void assertTestWriterRow(BinaryRow row) {
		assertEquals("1", row.getBinaryString(0).toString());
		assertEquals(88, row.getInt(8));
		assertEquals((short) 292, row.getShort(11));
		assertEquals(284, row.getLong(10));
		assertEquals((byte) 99, row.getByte(2));
		assertEquals('x', row.getChar(4));
		assertEquals(87.1d, row.getDouble(6), 0);
		assertEquals(26.1f, row.getFloat(7), 0);
		assertEquals(true, row.getBoolean(1));
		assertEquals("1234567", row.getBinaryString(3).toString());
		assertEquals("12345678", row.getBinaryString(5).toString());
		assertEquals("啦啦啦啦啦我是快乐的粉刷匠", row.getBinaryString(9).toString());
		assertTrue(row.isNullAt(12));
	}

	@Test
	public void testReuseWriter() {
		BinaryRow row = new BinaryRow(2);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		writer.writeString(0, "01234567");
		writer.writeString(1, "012345678");
		writer.complete();
		assertEquals("01234567", row.getBinaryString(0).toString());
		assertEquals("012345678", row.getBinaryString(1).toString());

		writer.reset();
		writer.writeString(0, "1");
		writer.writeString(1, "0123456789");
		writer.complete();
		assertEquals("1", row.getBinaryString(0).toString());
		assertEquals("0123456789", row.getBinaryString(1).toString());
	}

	@Test
	public void testGeneric() throws IOException {

		GenericTypeInfo info = new GenericTypeInfo(MyObj.class);
		TypeSerializer genericSerializer = info.createSerializer(new ExecutionConfig());

		BinaryRow row = new BinaryRow(4);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		writer.writeInt(0, 0);
		writer.writeGeneric(1, new MyObj(0, 1), genericSerializer);
		writer.writeGeneric(2, new MyObj(123, 5.0), genericSerializer);
		writer.writeGeneric(3, new MyObj(1, 1), genericSerializer);
		writer.complete();

		assertTestGenericRow(row, genericSerializer);

		// getBytes from var-length memorySegments.
		BinaryRowSerializer serializer = new BinaryRowSerializer(
			org.apache.flink.api.common.typeinfo.Types.INT, info, info, info);
		MemorySegment[] memorySegments = new MemorySegment[3];
		ArrayList<MemorySegment> memorySegmentList = new ArrayList<>();
		for (int i = 0; i < 3; i++) {
			memorySegments[i] = MemorySegmentFactory.wrap(new byte[64]);
			memorySegmentList.add(memorySegments[i]);
		}
		RandomAccessOutputView out = new RandomAccessOutputView(memorySegments, 64);
		serializer.serializeToPages(row, out);

		BinaryRow mapRow = serializer.mapFromPages(new RandomAccessInputView(memorySegmentList, 64));
		assertTestGenericRow(mapRow, genericSerializer);
	}

	@Test
	public void testSqlDateAndTime() throws IOException {
		BinaryRow row = new BinaryRow(4);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		writer.writeInt(0, 0);
		writer.writeGeneric(1, new Date(123), SqlDateSerializer.INSTANCE);
		writer.writeGeneric(2, new Timestamp(456), SqlTimestampSerializer.INSTANCE);
		writer.writeGeneric(3, new Time(789), SqlTimeSerializer.INSTANCE);
		writer.complete();

		assertEquals(new Date(123), row.getGeneric(1, SqlDateSerializer.INSTANCE));
		assertEquals(new Timestamp(456), row.getGeneric(2, SqlTimestampSerializer.INSTANCE));
		assertEquals(new Time(789), row.getGeneric(3, SqlTimeSerializer.INSTANCE));
	}

	@Test
	public void allSizeSerializeTest() throws IOException {

		int segSize = 64;
		int segTotalNumber = 3;

		BinaryRow row = new BinaryRow(1);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		Random random = new Random();
		byte[] bytes = new byte[1024];
		random.nextBytes(bytes);
		writer.writeBigBytes(0, bytes);
		writer.complete();

		MemorySegment[] memorySegments = new MemorySegment[segTotalNumber];
		Map<MemorySegment, Integer> msIndex = new HashMap<>();
		for (int i = 0; i < segTotalNumber; i++) {
			memorySegments[i] = MemorySegmentFactory.wrap(new byte[segSize]);
			msIndex.put(memorySegments[i], i);
		}

		BinaryRowSerializer serializer = new BinaryRowSerializer(Types.STRING);

		int rowSizeInt = 4;
		int rowFixLength = 16;
		for (int i = 0; i < segSize; i++) {
			int maxRowSize = (segSize * segTotalNumber) - i - rowSizeInt;
			if (segSize - i < rowFixLength + rowSizeInt) {
				maxRowSize -= segSize - i;
			}
			for (int j = rowFixLength; j < maxRowSize; j++) {
				testSerialize(row, memorySegments, msIndex, serializer, i, j);
			}
		}
	}

	@Test
	public void anyNullTest() throws IOException {
		{
			BinaryRow row = new BinaryRow(3);
			BinaryRowWriter writer = new BinaryRowWriter(row);
			assertFalse(row.anyNull());

			writer.setNullAt(2);
			assertTrue(row.anyNull());

			writer.setNullAt(0);
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
			writer.writeString(0, "" + rnd.nextInt());
			writer.writeString(3, "01234567");
			writer.writeString(5, "012345678");
			writer.writeBinaryString(9, BinaryString.fromString("啦啦啦啦啦我是快乐的粉刷匠"));
			writer.writeBoolean(1, true);
			writer.writeByte(2, (byte) 99);
			writer.writeChar(4, 'x');
			writer.writeDouble(6, 87.1d);
			writer.writeFloat(7, 26.1f);
			writer.writeInt(8, 88);
			writer.writeLong(10, 284);
			writer.writeShort(11, (short) 292);
			writer.setNullAt(12);
			writer.complete();
			BinaryRow copy = row.copy();
			assertEquals(
					Murmur32.hashBytesByWords(row.getMemorySegment(), 0, row.getSizeInBytes(), 42),
					Murmur32.hashBytesByWords(copy.getMemorySegment(), 0, copy.getSizeInBytes(), 42));
		}

		// test hash distribution
		int count = 9999999;
		Set<Integer> hashCodes = new HashSet<>(count);
		for (int i = 0; i < count; i++) {
			row.setInt(8, i);
			hashCodes.add(
					Murmur32.hashBytesByWords(row.getMemorySegment(), 0, row.getSizeInBytes(), 42));
		}
		assertEquals(count, hashCodes.size());
		hashCodes.clear();
		row = new BinaryRow(1);
		writer = new BinaryRowWriter(row);
		for (int i = 0; i < count; i++) {
			writer.reset();
			writer.writeString(0, "啦啦啦啦啦我是快乐的粉刷匠" + i);
			writer.complete();
			hashCodes.add(
					Murmur32.hashBytesByWords(row.getMemorySegment(), 0, row.getSizeInBytes(), 42));
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
		BaseRowSerializer serializer = new BaseRowSerializer(
			BaseRow.class,
			org.apache.flink.api.common.typeinfo.Types.INT,
			org.apache.flink.api.common.typeinfo.Types.INT);
		TestDataOutputSerializer output = new TestDataOutputSerializer(1024);
		serializer.serialize(row, output);

		MemorySegment segment = MemorySegmentFactory.wrap(output.copyByteBuffer());
		BinaryRow newRow = new BinaryRow(2);
		newRow.pointTo(segment, 4, segment.size() - 4);
		assertEquals(row, newRow);
		assertEquals((byte) 29, newRow.getHeader());
	}

	@Test
	public void testZeroOutPaddingGeneric() {

		GenericTypeInfo info = new GenericTypeInfo(MyObj.class);
		TypeSerializer genericSerializer = info.createSerializer(new ExecutionConfig());

		Random random = new Random();
		byte[] bytes = new byte[1024];

		BinaryRow row = new BinaryRow(1);
		BinaryRowWriter writer = new BinaryRowWriter(row);

		// let's random the bytes
		writer.reset();
		random.nextBytes(bytes);
		writer.writeBigBytes(0, bytes);
		writer.reset();
		writer.writeGeneric(0, new MyObj(0, 1), genericSerializer);
		writer.complete();
		int hash1 = row.hashCode();

		writer.reset();
		random.nextBytes(bytes);
		writer.writeBigBytes(0, bytes);
		writer.reset();
		writer.writeGeneric(0, new MyObj(0, 1), genericSerializer);
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

		{
			writer.reset();
			random.nextBytes(bytes);
			writer.writeBigBytes(0, bytes);
			writer.reset();
			writer.writeString(0, "wahahah");
			writer.complete();
			int hash1 = row.hashCode();

			writer.reset();
			random.nextBytes(bytes);
			writer.writeBigBytes(0, bytes);
			writer.reset();
			writer.writeString(0, "wahahah");
			writer.complete();
			int hash2 = row.hashCode();

			assertEquals(hash1, hash2);
		}

		{
			writer.reset();
			random.nextBytes(bytes);
			writer.writeBigBytes(0, bytes);
			writer.reset();
			writer.writeBinaryString(0, BinaryString.fromString("wahahah"));
			writer.complete();
			int hash1 = row.hashCode();

			writer.reset();
			random.nextBytes(bytes);
			writer.writeBigBytes(0, bytes);
			writer.reset();
			writer.writeBinaryString(0, BinaryString.fromString("wahahah"));
			writer.complete();
			int hash2 = row.hashCode();

			assertEquals(hash1, hash2);
		}
	}

	@Test
	public void testHashAndCopy() throws IOException {
		MemorySegment[] segments = new MemorySegment[3];
		for (int i = 0; i < 3; i++) {
			segments[i] = MemorySegmentFactory.wrap(new byte[64]);
		}
		RandomAccessOutputView out = new RandomAccessOutputView(segments, 64);
		BinaryRowSerializer serializer = new BinaryRowSerializer(Types.STRING, Types.STRING);

		BinaryRow row = new BinaryRow(2);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		writer.writeString(0, "hahahahahahahahahahahahahahahahahahahhahahahahahahahahah");
		writer.writeString(1, "hahahahahahahahahahahahahahahahahahahhahahahahahahahahaa");
		writer.complete();
		serializer.serializeToPages(row, out);

		ArrayList<MemorySegment> segmentList = new ArrayList<>();
		segmentList.addAll(Arrays.asList(segments));
		RandomAccessInputView input = new RandomAccessInputView(segmentList, 64, 64);

		BinaryRow mapRow = serializer.mapFromPages(input);
		assertEquals(row, mapRow);
		assertEquals(row.getBinaryString(0), mapRow.getBinaryString(0));
		assertEquals(row.getBinaryString(1), mapRow.getBinaryString(1));
		assertNotEquals(row.getBinaryString(0), mapRow.getBinaryString(1));

		assertEquals(row.hashCode(), mapRow.hashCode());
		assertEquals(row.getBinaryString(0).hashCode(), mapRow.getBinaryString(0).hashCode());
		assertEquals(row.getBinaryString(1).hashCode(), mapRow.getBinaryString(1).hashCode());

		assertEquals(row.copy(), mapRow.copy());
		assertEquals(row.getBinaryString(0).copy(), mapRow.getBinaryString(0).copy());
		assertEquals(row.getBinaryString(1).copy(), mapRow.getBinaryString(1).copy());
	}

	@Test
	public void testByteArray() throws IOException {
		BinaryRow row = new BinaryRow(1);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		byte[] bytes = new byte[1024];
		new Random().nextBytes(bytes);
		writer.writeByteArray(0, bytes);
		writer.complete();

		byte[] getFromByteArray = row.getByteArray(0);
		assertTrue(Arrays.equals(bytes, getFromByteArray));
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

	@Test(expected = RuntimeException.class)
	public void testWrongBinaryRowSerializer() throws IOException {
		BinaryRowSerializer serializer = new BinaryRowSerializer(Types.STRING);
		BinaryRow row = new BinaryRow(1);
		row.pointTo(MemorySegmentFactory.wrap(new byte[15]), 5, 30);
		serializer.serialize(row, new DataOutputViewStreamWrapper(new ByteArrayOutputStream()));
	}

	private void testSerialize(BinaryRow row, MemorySegment[] memorySegments,
			Map<MemorySegment, Integer> msIndex, BinaryRowSerializer serializer, int position,
			int rowSize) throws IOException {
		RandomAccessOutputView out = new RandomAccessOutputView(memorySegments, 64);
		out.skipBytesToWrite(position);
		row.setTotalSize(rowSize);

		serializer.serializeToPages(row, out);

		int segNumber = msIndex.get(out.getCurrentSegment()) + 1;
		int lastSegSize = out.getCurrentPositionInSegment();

		ArrayList<MemorySegment> segments = new ArrayList<>();
		segments.addAll(Arrays.asList(memorySegments).subList(0, segNumber));
		RandomAccessInputView input = new RandomAccessInputView(segments, 64, lastSegSize);
		input.skipBytesToRead(position);
		BinaryRow mapRow = serializer.mapFromPages(input);

		assertEquals(row, mapRow);
	}

	private void assertTestGenericRow(BinaryRow row, TypeSerializer genericSerializer) {
		assertEquals(0, row.getInt(0));
		assertEquals(new MyObj(0, 1), row.getGeneric(1, genericSerializer));
		assertEquals(new MyObj(123, 5.0), row.getGeneric(2, genericSerializer));
		assertEquals(new MyObj(1, 1), row.getGeneric(3, genericSerializer));
	}

	/**
	 * Test class.
	 */
	public static class MyObj {
		public int i;
		public double j;

		public MyObj(int i, double j) {
			this.i = i;
			this.j = j;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			MyObj myObj = (MyObj) o;

			return i == myObj.i && Double.compare(myObj.j, j) == 0;
		}

		@Override
		public String toString() {
			return "MyObj{" +
					"i=" + i +
					", j=" + j +
					'}';
		}
	}
}
