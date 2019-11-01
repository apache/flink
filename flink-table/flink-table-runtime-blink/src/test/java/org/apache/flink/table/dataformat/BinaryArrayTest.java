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

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.runtime.typeutils.BaseArraySerializer;
import org.apache.flink.table.runtime.typeutils.BaseMapSerializer;
import org.apache.flink.table.runtime.typeutils.BaseRowSerializer;
import org.apache.flink.table.runtime.typeutils.BinaryGenericSerializer;
import org.apache.flink.table.runtime.util.SegmentsUtil;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;

import static org.apache.flink.table.dataformat.BinaryString.fromString;
import static org.apache.flink.table.utils.BinaryGenericAsserter.equivalent;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Test of {@link BinaryArray} and {@link BinaryArrayWriter}.
 */
public class BinaryArrayTest {

	@Test
	public void testArray() {
		// 1.array test
		BinaryArray array = new BinaryArray();
		BinaryArrayWriter writer = new BinaryArrayWriter(array, 3, 4);

		writer.writeInt(0, 6);
		writer.setNullInt(1);
		writer.writeInt(2, 666);
		writer.complete();

		assertEquals(array.getInt(0), 6);
		assertTrue(array.isNullAt(1));
		assertEquals(array.getInt(2), 666);

		//2.test write to binary row.
		{
			BinaryRow row2 = new BinaryRow(1);
			BinaryRowWriter writer2 = new BinaryRowWriter(row2);
			writer2.writeArray(0, array, new BaseArraySerializer(DataTypes.INT().getLogicalType(), null));
			writer2.complete();

			BinaryArray array2 = (BinaryArray) row2.getArray(0);
			assertEquals(array2, array);
			assertEquals(array2.getInt(0), 6);
			assertTrue(array2.isNullAt(1));
			assertEquals(array2.getInt(2), 666);
		}

		//3.test write var seg array to binary row.
		{
			BinaryArray array3 = splitArray(array);

			BinaryRow row2 = new BinaryRow(1);
			BinaryRowWriter writer2 = new BinaryRowWriter(row2);
			writer2.writeArray(0, array3, new BaseArraySerializer(DataTypes.INT().getLogicalType(), null));
			writer2.complete();

			BinaryArray array2 = (BinaryArray) row2.getArray(0);
			assertEquals(array2, array);
			assertEquals(array2.getInt(0), 6);
			assertTrue(array2.isNullAt(1));
			assertEquals(array2.getInt(2), 666);
		}
	}

	@Test
	public void testArrayTypes() {
		{
			// test bool
			BinaryArray array = new BinaryArray();
			BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 1);
			writer.setNullBoolean(0);
			writer.writeBoolean(1, true);
			writer.complete();

			assertTrue(array.isNullAt(0));
			assertEquals(true, array.getBoolean(1));
			array.setBoolean(0, true);
			assertEquals(true, array.getBoolean(0));
			array.setNullBoolean(0);
			assertTrue(array.isNullAt(0));

			BinaryArray newArray = splitArray(array);
			assertTrue(newArray.isNullAt(0));
			assertEquals(true, newArray.getBoolean(1));
			newArray.setBoolean(0, true);
			assertEquals(true, newArray.getBoolean(0));
			newArray.setNullBoolean(0);
			assertTrue(newArray.isNullAt(0));

			newArray.setBoolean(0, true);
			assertEquals(newArray, BinaryArray.fromPrimitiveArray(newArray.toBooleanArray()));
		}

		{
			// test byte
			BinaryArray array = new BinaryArray();
			BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 1);
			writer.setNullByte(0);
			writer.writeByte(1, (byte) 25);
			writer.complete();

			assertTrue(array.isNullAt(0));
			assertEquals(25, array.getByte(1));
			array.setByte(0, (byte) 5);
			assertEquals(5, array.getByte(0));
			array.setNullByte(0);
			assertTrue(array.isNullAt(0));

			BinaryArray newArray = splitArray(array);
			assertTrue(newArray.isNullAt(0));
			assertEquals(25, newArray.getByte(1));
			newArray.setByte(0, (byte) 5);
			assertEquals(5, newArray.getByte(0));
			newArray.setNullByte(0);
			assertTrue(newArray.isNullAt(0));

			newArray.setByte(0, (byte) 3);
			assertEquals(newArray, BinaryArray.fromPrimitiveArray(newArray.toByteArray()));
		}

		{
			// test short
			BinaryArray array = new BinaryArray();
			BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 2);
			writer.setNullShort(0);
			writer.writeShort(1, (short) 25);
			writer.complete();

			assertTrue(array.isNullAt(0));
			assertEquals(25, array.getShort(1));
			array.setShort(0, (short) 5);
			assertEquals(5, array.getShort(0));
			array.setNullShort(0);
			assertTrue(array.isNullAt(0));

			BinaryArray newArray = splitArray(array);
			assertTrue(newArray.isNullAt(0));
			assertEquals(25, newArray.getShort(1));
			newArray.setShort(0, (short) 5);
			assertEquals(5, newArray.getShort(0));
			newArray.setNullShort(0);
			assertTrue(newArray.isNullAt(0));

			newArray.setShort(0, (short) 3);
			assertEquals(newArray, BinaryArray.fromPrimitiveArray(newArray.toShortArray()));
		}

		{
			// test int
			BinaryArray array = new BinaryArray();
			BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 4);
			writer.setNullInt(0);
			writer.writeInt(1, 25);
			writer.complete();

			assertTrue(array.isNullAt(0));
			assertEquals(25, array.getInt(1));
			array.setInt(0, 5);
			assertEquals(5, array.getInt(0));
			array.setNullInt(0);
			assertTrue(array.isNullAt(0));

			BinaryArray newArray = splitArray(array);
			assertTrue(newArray.isNullAt(0));
			assertEquals(25, newArray.getInt(1));
			newArray.setInt(0, 5);
			assertEquals(5, newArray.getInt(0));
			newArray.setNullInt(0);
			assertTrue(newArray.isNullAt(0));

			newArray.setInt(0, 3);
			assertEquals(newArray, BinaryArray.fromPrimitiveArray(newArray.toIntArray()));
		}

		{
			// test long
			BinaryArray array = new BinaryArray();
			BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);
			writer.setNullLong(0);
			writer.writeLong(1, 25);
			writer.complete();

			assertTrue(array.isNullAt(0));
			assertEquals(25, array.getLong(1));
			array.setLong(0, 5);
			assertEquals(5, array.getLong(0));
			array.setNullLong(0);
			assertTrue(array.isNullAt(0));

			BinaryArray newArray = splitArray(array);
			assertTrue(newArray.isNullAt(0));
			assertEquals(25, newArray.getLong(1));
			newArray.setLong(0, 5);
			assertEquals(5, newArray.getLong(0));
			newArray.setNullLong(0);
			assertTrue(newArray.isNullAt(0));

			newArray.setLong(0, 3);
			assertEquals(newArray, BinaryArray.fromPrimitiveArray(newArray.toLongArray()));
		}

		{
			// test float
			BinaryArray array = new BinaryArray();
			BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 4);
			writer.setNullFloat(0);
			writer.writeFloat(1, 25);
			writer.complete();

			assertTrue(array.isNullAt(0));
			assertTrue(25 == array.getFloat(1));
			array.setFloat(0, 5);
			assertTrue(5 == array.getFloat(0));
			array.setNullFloat(0);
			assertTrue(array.isNullAt(0));

			BinaryArray newArray = splitArray(array);
			assertTrue(newArray.isNullAt(0));
			assertTrue(25 == newArray.getFloat(1));
			newArray.setFloat(0, 5);
			assertTrue(5 == newArray.getFloat(0));
			newArray.setNullFloat(0);
			assertTrue(newArray.isNullAt(0));

			newArray.setFloat(0, 3);
			assertEquals(newArray, BinaryArray.fromPrimitiveArray(newArray.toFloatArray()));
		}

		{
			// test double
			BinaryArray array = new BinaryArray();
			BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);
			writer.setNullDouble(0);
			writer.writeDouble(1, 25);
			writer.complete();

			assertTrue(array.isNullAt(0));
			assertTrue(25 == array.getDouble(1));
			array.setDouble(0, 5);
			assertTrue(5 == array.getDouble(0));
			array.setNullDouble(0);
			assertTrue(array.isNullAt(0));

			BinaryArray newArray = splitArray(array);
			assertTrue(newArray.isNullAt(0));
			assertTrue(25 == newArray.getDouble(1));
			newArray.setDouble(0, 5);
			assertTrue(5 == newArray.getDouble(0));
			newArray.setNullDouble(0);
			assertTrue(newArray.isNullAt(0));

			newArray.setDouble(0, 3);
			assertEquals(newArray, BinaryArray.fromPrimitiveArray(newArray.toDoubleArray()));
		}

		{
			// test string
			BinaryArray array = new BinaryArray();
			BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);
			writer.setNullAt(0);
			writer.writeString(1, fromString("jaja"));
			writer.complete();

			assertTrue(array.isNullAt(0));
			assertEquals(fromString("jaja"), array.getString(1));

			BinaryArray newArray = splitArray(array);
			assertTrue(newArray.isNullAt(0));
			assertEquals(fromString("jaja"), newArray.getString(1));
		}

		BinaryArray subArray = new BinaryArray();
		BinaryArrayWriter subWriter = new BinaryArrayWriter(subArray, 2, 8);
		subWriter.setNullAt(0);
		subWriter.writeString(1, fromString("hehehe"));
		subWriter.complete();

		{
			// test array
			BinaryArray array = new BinaryArray();
			BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);
			writer.setNullAt(0);
			writer.writeArray(1, subArray, new BaseArraySerializer(DataTypes.INT().getLogicalType(), null));
			writer.complete();

			assertTrue(array.isNullAt(0));
			assertEquals(subArray, array.getArray(1));

			BinaryArray newArray = splitArray(array);
			assertTrue(newArray.isNullAt(0));
			assertEquals(subArray, newArray.getArray(1));
		}

		{
			// test map
			BinaryArray array = new BinaryArray();
			BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);
			writer.setNullAt(0);
			writer.writeMap(1, BinaryMap.valueOf(subArray, subArray),
					new BaseMapSerializer(DataTypes.INT().getLogicalType(), DataTypes.INT().getLogicalType(), null));
			writer.complete();

			assertTrue(array.isNullAt(0));
			assertEquals(BinaryMap.valueOf(subArray, subArray), array.getMap(1));

			BinaryArray newArray = splitArray(array);
			assertTrue(newArray.isNullAt(0));
			assertEquals(BinaryMap.valueOf(subArray, subArray), newArray.getMap(1));
		}
	}

	@Test
	public void testMap() {
		BinaryArray array1 = new BinaryArray();
		BinaryArrayWriter writer1 = new BinaryArrayWriter(array1, 3, 4);
		writer1.writeInt(0, 6);
		writer1.writeInt(1, 5);
		writer1.writeInt(2, 666);
		writer1.complete();

		BinaryArray array2 = new BinaryArray();
		BinaryArrayWriter writer2 = new BinaryArrayWriter(array2, 3, 8);
		writer2.writeString(0, fromString("6"));
		writer2.writeString(1, fromString("5"));
		writer2.writeString(2, fromString("666"));
		writer2.complete();

		BinaryMap binaryMap = BinaryMap.valueOf(array1, array2);

		BinaryRow row = new BinaryRow(1);
		BinaryRowWriter rowWriter = new BinaryRowWriter(row);
		rowWriter.writeMap(0, binaryMap,
				new BaseMapSerializer(DataTypes.INT().getLogicalType(), DataTypes.INT().getLogicalType(), null));
		rowWriter.complete();

		BinaryMap map = (BinaryMap) row.getMap(0);
		BinaryArray key = map.keyArray();
		BinaryArray value = map.valueArray();

		assertEquals(binaryMap, map);
		assertEquals(array1, key);
		assertEquals(array2, value);

		assertEquals(key.getInt(1), 5);
		assertEquals(value.getString(1), fromString("5"));
	}

	private static BinaryArray splitArray(BinaryArray array) {
		BinaryArray ret = new BinaryArray();
		MemorySegment[] segments = splitBytes(SegmentsUtil.copyToBytes(array.segments, 0, array.sizeInBytes), 0);
		ret.pointTo(segments, 0, array.sizeInBytes);
		return ret;
	}

	private static MemorySegment[] splitBytes(byte[] bytes, int baseOffset) {
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
		BinaryArrayWriter writer = new BinaryArrayWriter(array, 3, 2);
		writer.writeShort(0, (short) 5);
		writer.writeShort(1, (short) 10);
		writer.writeShort(2, (short) 15);
		writer.complete();

		short[] shorts = array.toShortArray();
		assertEquals(5, shorts[0]);
		assertEquals(10, shorts[1]);
		assertEquals(15, shorts[2]);

		MemorySegment[] segments = splitBytes(writer.segment.getArray(), 3);
		array.pointTo(segments, 3, array.getSizeInBytes());
		assertEquals(5, array.getShort(0));
		assertEquals(10, array.getShort(1));
		assertEquals(15, array.getShort(2));
		short[] shorts2 = array.toShortArray();
		assertEquals(5, shorts2[0]);
		assertEquals(10, shorts2[1]);
		assertEquals(15, shorts2[2]);
	}

	@Test
	public void testDecimal() {

		BinaryArray array = new BinaryArray();
		BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

		// 1.compact
		{
			int precision = 4;
			int scale = 2;
			writer.reset();
			writer.writeDecimal(0, Decimal.fromLong(5, precision, scale), precision);
			writer.setNullAt(1);
			writer.complete();

			assertEquals("0.05", array.getDecimal(0, precision, scale).toString());
			assertTrue(array.isNullAt(1));
			array.setDecimal(0, Decimal.fromLong(6, precision, scale), precision);
			assertEquals("0.06", array.getDecimal(0, precision, scale).toString());
		}

		// 2.not compact
		{
			int precision = 25;
			int scale = 5;
			Decimal decimal1 = Decimal.fromBigDecimal(BigDecimal.valueOf(5.55), precision, scale);
			Decimal decimal2 = Decimal.fromBigDecimal(BigDecimal.valueOf(6.55), precision, scale);

			writer.reset();
			writer.writeDecimal(0, decimal1, precision);
			writer.writeDecimal(1, null, precision);
			writer.complete();

			assertEquals("5.55000", array.getDecimal(0, precision, scale).toString());
			assertTrue(array.isNullAt(1));
			array.setDecimal(0, decimal2, precision);
			assertEquals("6.55000", array.getDecimal(0, precision, scale).toString());
		}
	}

	@Test
	public void testGeneric() {
		BinaryArray array = new BinaryArray();
		BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);
		BinaryGeneric<String> generic = new BinaryGeneric<>("hahah");
		BinaryGenericSerializer<String> serializer = new BinaryGenericSerializer<>(StringSerializer.INSTANCE);
		writer.writeGeneric(0, generic, serializer);
		writer.setNullAt(1);
		writer.complete();

		BinaryGeneric newGeneric = array.getGeneric(0);
		assertThat(newGeneric, equivalent(generic, serializer));
		assertTrue(array.isNullAt(1));
	}

	@Test
	public void testNested() {
		BinaryArray array = new BinaryArray();
		BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);
		writer.writeRow(0, GenericRow.of(fromString("1"), 1),
				new BaseRowSerializer(null, RowType.of(new VarCharType(VarCharType.MAX_LENGTH), new IntType())));
		writer.setNullAt(1);
		writer.complete();

		BaseRow nestedRow = array.getRow(0, 2);
		assertEquals("1", nestedRow.getString(0).toString());
		assertEquals(1, nestedRow.getInt(1));
		assertTrue(array.isNullAt(1));
	}

	@Test
	public void testBinary() {
		BinaryArray array = new BinaryArray();
		BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);
		byte[] bytes1 = new byte[] {1, -1, 5};
		byte[] bytes2 = new byte[] {1, -1, 5, 5, 1, 5, 1, 5};
		writer.writeBinary(0, bytes1);
		writer.writeBinary(1, bytes2);
		writer.complete();

		Assert.assertArrayEquals(bytes1, array.getBinary(0));
		Assert.assertArrayEquals(bytes2, array.getBinary(1));
	}
}
