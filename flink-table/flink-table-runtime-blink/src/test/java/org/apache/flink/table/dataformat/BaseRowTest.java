/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.dataformat;

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.runtime.typeutils.BaseArraySerializer;
import org.apache.flink.table.runtime.typeutils.BaseMapSerializer;
import org.apache.flink.table.runtime.typeutils.BaseRowSerializer;
import org.apache.flink.table.runtime.typeutils.BinaryGenericSerializer;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import static org.apache.flink.table.utils.BinaryGenericAsserter.equivalent;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link BaseRow}s.
 */
public class BaseRowTest {

	private BinaryString str;
	private BinaryGeneric generic;
	private Decimal decimal1;
	private Decimal decimal2;
	private BinaryArray array;
	private BinaryMap map;
	private BinaryRow underRow;
	private byte[] bytes;
	private BinaryGenericSerializer<String> genericSerializer;
	private SqlTimestamp sqlTimestamp1;
	private SqlTimestamp sqlTimestamp2;

	@Before
	public void before() {
		str = BinaryString.fromString("haha");
		generic = new BinaryGeneric<>("haha");
		genericSerializer = new BinaryGenericSerializer<>(StringSerializer.INSTANCE);
		decimal1 = Decimal.fromLong(10, 5, 0);
		decimal2 = Decimal.fromBigDecimal(new BigDecimal(11), 20, 0);
		array = new BinaryArray();
		{
			BinaryArrayWriter arrayWriter = new BinaryArrayWriter(array, 2, 4);
			arrayWriter.writeInt(0, 15);
			arrayWriter.writeInt(1, 16);
			arrayWriter.complete();
		}
		map = BinaryMap.valueOf(array, array);
		underRow = new BinaryRow(2);
		{
			BinaryRowWriter writer = new BinaryRowWriter(underRow);
			writer.writeInt(0, 15);
			writer.writeInt(1, 16);
			writer.complete();
		}
		bytes = new byte[] {1, 5, 6};
		sqlTimestamp1 = SqlTimestamp.fromEpochMillis(123L);
		sqlTimestamp2 = SqlTimestamp.fromLocalDateTime(LocalDateTime.of(1969, 1, 1, 0, 0, 0, 123456789));
	}

	@Test
	public void testBinaryRow() {
		testAll(getBinaryRow());
	}

	@Test
	public void testNestedRow() {
		BinaryRow row = new BinaryRow(1);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		writer.writeRow(0, getBinaryRow(), null);
		writer.complete();
		testAll(row.getRow(0, 18));
	}

	private BinaryRow getBinaryRow() {
		BinaryRow row = new BinaryRow(18);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		writer.writeBoolean(0, true);
		writer.writeByte(1, (byte) 1);
		writer.writeShort(2, (short) 2);
		writer.writeInt(3, 3);
		writer.writeLong(4, 4);
		writer.writeFloat(5, 5);
		writer.writeDouble(6, 6);
		writer.writeString(8, str);
		writer.writeGeneric(9, generic, genericSerializer);
		writer.writeDecimal(10, decimal1, 5);
		writer.writeDecimal(11, decimal2, 20);
		writer.writeArray(12, array, new BaseArraySerializer(DataTypes.INT().getLogicalType(), null));
		writer.writeMap(13, map, new BaseMapSerializer(
			DataTypes.INT().getLogicalType(), DataTypes.INT().getLogicalType(), null));
		writer.writeRow(14, underRow, new BaseRowSerializer(null, RowType.of(new IntType(), new IntType())));
		writer.writeBinary(15, bytes);
		writer.writeTimestamp(16, sqlTimestamp1, 3);
		writer.writeTimestamp(17, sqlTimestamp2, 9);
		return row;
	}

	@Test
	public void testGenericRow() {
		GenericRow row = new GenericRow(18);
		row.setField(0, true);
		row.setField(1, (byte) 1);
		row.setField(2, (short) 2);
		row.setField(3, 3);
		row.setField(4, (long) 4);
		row.setField(5, (float) 5);
		row.setField(6, (double) 6);
		row.setField(7, (char) 7);
		row.setField(8, str);
		row.setField(9, generic);
		row.setField(10, decimal1);
		row.setField(11, decimal2);
		row.setField(12, array);
		row.setField(13, map);
		row.setField(14, underRow);
		row.setField(15, bytes);
		row.setField(16, sqlTimestamp1);
		row.setField(17, sqlTimestamp2);
		testAll(row);
	}

	@Test
	public void testBoxedWrapperRow() {
		BoxedWrapperRow row = new BoxedWrapperRow(18);
		row.setBoolean(0, true);
		row.setByte(1, (byte) 1);
		row.setShort(2, (short) 2);
		row.setInt(3, 3);
		row.setLong(4, (long) 4);
		row.setFloat(5, (float) 5);
		row.setDouble(6, (double) 6);
		row.setNonPrimitiveValue(8, str);
		row.setNonPrimitiveValue(9, generic);
		row.setNonPrimitiveValue(10, decimal1);
		row.setNonPrimitiveValue(11, decimal2);
		row.setNonPrimitiveValue(12, array);
		row.setNonPrimitiveValue(13, map);
		row.setNonPrimitiveValue(14, underRow);
		row.setNonPrimitiveValue(15, bytes);
		row.setNonPrimitiveValue(16, sqlTimestamp1);
		row.setNonPrimitiveValue(17, sqlTimestamp2);
		testAll(row);
	}

	@Test
	public void testJoinedRow() {
		GenericRow row1 = new GenericRow(5);
		row1.setField(0, true);
		row1.setField(1, (byte) 1);
		row1.setField(2, (short) 2);
		row1.setField(3, 3);
		row1.setField(4, (long) 4);

		GenericRow row2 = new GenericRow(13);
		row2.setField(0, (float) 5);
		row2.setField(1, (double) 6);
		row2.setField(2, (char) 7);
		row2.setField(3, str);
		row2.setField(4, generic);
		row2.setField(5, decimal1);
		row2.setField(6, decimal2);
		row2.setField(7, array);
		row2.setField(8, map);
		row2.setField(9, underRow);
		row2.setField(10, bytes);
		row2.setField(11, sqlTimestamp1);
		row2.setField(12, sqlTimestamp2);
		testAll(new JoinedRow(row1, row2));
	}

	private void testAll(BaseRow row) {
		assertEquals(18, row.getArity());

		// test header
		assertEquals(0, row.getHeader());
		row.setHeader((byte) 1);
		assertEquals(1, row.getHeader());

		// test get
		assertTrue(row.getBoolean(0));
		assertEquals(1, row.getByte(1));
		assertEquals(2, row.getShort(2));
		assertEquals(3, row.getInt(3));
		assertEquals(4, row.getLong(4));
		assertEquals(5, (int) row.getFloat(5));
		assertEquals(6, (int) row.getDouble(6));
		assertEquals(str, row.getString(8));
		assertThat(row.getGeneric(9), equivalent(generic, genericSerializer));
		assertEquals(decimal1, row.getDecimal(10, 5, 0));
		assertEquals(decimal2, row.getDecimal(11, 20, 0));
		assertEquals(array, row.getArray(12));
		assertEquals(map, row.getMap(13));
		assertEquals(15, row.getRow(14, 2).getInt(0));
		assertEquals(16, row.getRow(14, 2).getInt(1));
		assertArrayEquals(bytes, row.getBinary(15));
		assertEquals(sqlTimestamp1, row.getTimestamp(16, 3));
		assertEquals(sqlTimestamp2, row.getTimestamp(17, 9));

		// test set
		row.setBoolean(0, false);
		assertFalse(row.getBoolean(0));
		row.setByte(1, (byte) 2);
		assertEquals(2, row.getByte(1));
		row.setShort(2, (short) 3);
		assertEquals(3, row.getShort(2));
		row.setInt(3, 4);
		assertEquals(4, row.getInt(3));
		row.setLong(4, 5);
		assertEquals(5, row.getLong(4));
		row.setFloat(5, 6);
		assertEquals(6, (int) row.getFloat(5));
		row.setDouble(6, 7);
		assertEquals(7, (int) row.getDouble(6));
		row.setDecimal(10, Decimal.fromLong(11, 5, 0), 5);
		assertEquals(Decimal.fromLong(11, 5, 0), row.getDecimal(10, 5, 0));
		row.setDecimal(11, Decimal.fromBigDecimal(new BigDecimal(12), 20, 0), 20);
		assertEquals(Decimal.fromBigDecimal(new BigDecimal(12), 20, 0), row.getDecimal(11, 20, 0));

		row.setTimestamp(16, SqlTimestamp.fromEpochMillis(456L), 3);
		assertEquals(SqlTimestamp.fromEpochMillis(456L), row.getTimestamp(16, 3));
		row.setTimestamp(17, SqlTimestamp.fromTimestamp(Timestamp.valueOf("1970-01-01 00:00:00.123456789")), 9);
		assertEquals(SqlTimestamp.fromTimestamp(Timestamp.valueOf("1970-01-01 00:00:00.123456789")), row.getTimestamp(17, 9));

		// test null
		assertFalse(row.isNullAt(0));
		row.setNullAt(0);
		assertTrue(row.isNullAt(0));
	}

}
