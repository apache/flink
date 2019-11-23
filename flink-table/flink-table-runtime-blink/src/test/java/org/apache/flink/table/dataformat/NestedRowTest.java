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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.runtime.typeutils.BaseRowSerializer;
import org.apache.flink.table.runtime.typeutils.BinaryGenericSerializer;
import org.apache.flink.table.runtime.typeutils.BinaryStringSerializer;
import org.apache.flink.table.types.logical.LogicalType;

import org.junit.Test;

import static org.apache.flink.table.dataformat.DataFormatTestUtil.MyObj;
import static org.apache.flink.table.dataformat.DataFormatTestUtil.splitBytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link NestedRow}s.
 */
public class NestedRowTest {

	@Test
	public void testNestedRowWithOneSegment() {
		BinaryRow row = getBinaryRow();
		GenericTypeInfo<MyObj> info = new GenericTypeInfo<>(MyObj.class);
		TypeSerializer<MyObj> genericSerializer = info.createSerializer(new ExecutionConfig());

		BaseRow nestedRow = row.getRow(0, 5);
		assertEquals(nestedRow.getInt(0), 1);
		assertEquals(nestedRow.getLong(1), 5L);
		assertEquals(nestedRow.getString(2), BinaryString.fromString("12345678"));
		assertTrue(nestedRow.isNullAt(3));
		assertEquals(new MyObj(15, 5),
			BinaryGeneric.getJavaObjectFromBinaryGeneric(nestedRow.getGeneric(4), genericSerializer));
	}

	@Test
	public void testNestedRowWithMultipleSegments() {
		BinaryRow row = getBinaryRow();
		GenericTypeInfo<MyObj> info = new GenericTypeInfo<>(MyObj.class);
		TypeSerializer<MyObj> genericSerializer = info.createSerializer(new ExecutionConfig());

		MemorySegment[] segments = splitBytes(row.getSegments()[0].getHeapMemory(), 3);
		row.pointTo(segments, 3, row.getSizeInBytes());
		{
			BaseRow nestedRow = row.getRow(0, 5);
			assertEquals(nestedRow.getInt(0), 1);
			assertEquals(nestedRow.getLong(1), 5L);
			assertEquals(nestedRow.getString(2), BinaryString.fromString("12345678"));
			assertTrue(nestedRow.isNullAt(3));
			assertEquals(new MyObj(15, 5),
				BinaryGeneric.getJavaObjectFromBinaryGeneric(nestedRow.getGeneric(4), genericSerializer));
		}
	}

	@Test
	public void testNestInNestedRow() {
		// layer1
		GenericRow gRow = new GenericRow(4);
		gRow.setField(0, 1);
		gRow.setField(1, 5L);
		gRow.setField(2, BinaryString.fromString("12345678"));
		gRow.setField(3, null);

		// layer2
		BaseRowSerializer serializer = new BaseRowSerializer(
			new LogicalType[]{
				DataTypes.INT().getLogicalType(),
				DataTypes.BIGINT().getLogicalType(),
				DataTypes.STRING().getLogicalType(),
				DataTypes.STRING().getLogicalType()
			},
			new TypeSerializer[]{
				IntSerializer.INSTANCE,
				LongSerializer.INSTANCE,
				StringSerializer.INSTANCE,
				StringSerializer.INSTANCE
			});
		BinaryRow row = new BinaryRow(2);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		writer.writeString(0, BinaryString.fromString("hahahahafff"));
		writer.writeRow(1, gRow, serializer);
		writer.complete();

		// layer3
		BinaryRow row2 = new BinaryRow(1);
		BinaryRowWriter writer2 = new BinaryRowWriter(row2);
		writer2.writeRow(0, row, null);
		writer2.complete();

		// verify
		{
			NestedRow nestedRow = (NestedRow) row2.getRow(0, 2);
			BinaryRow binaryRow = new BinaryRow(2);
			binaryRow.pointTo(nestedRow.getSegments(), nestedRow.getOffset(),
				nestedRow.getSizeInBytes());
			assertEquals(binaryRow, row);
		}

		assertEquals(row2.getRow(0, 2).getString(0), BinaryString.fromString("hahahahafff"));
		BaseRow nestedRow = row2.getRow(0, 2).getRow(1, 4);
		assertEquals(nestedRow.getInt(0), 1);
		assertEquals(nestedRow.getLong(1), 5L);
		assertEquals(nestedRow.getString(2), BinaryString.fromString("12345678"));
		assertTrue(nestedRow.isNullAt(3));
	}

	private BinaryRow getBinaryRow() {
		BinaryRow row = new BinaryRow(1);
		BinaryRowWriter writer = new BinaryRowWriter(row);

		GenericTypeInfo<MyObj> info = new GenericTypeInfo<>(MyObj.class);
		TypeSerializer<MyObj> genericSerializer = info.createSerializer(new ExecutionConfig());
		GenericRow gRow = new GenericRow(5);
		gRow.setField(0, 1);
		gRow.setField(1, 5L);
		gRow.setField(2, BinaryString.fromString("12345678"));
		gRow.setField(3, null);
		gRow.setField(4, new BinaryGeneric<>(new MyObj(15, 5)));

		BaseRowSerializer serializer = new BaseRowSerializer(
			new LogicalType[]{
				DataTypes.INT().getLogicalType(),
				DataTypes.BIGINT().getLogicalType(),
				DataTypes.STRING().getLogicalType(),
				DataTypes.STRING().getLogicalType(),
				DataTypes.RAW(info).getLogicalType()
			},
			new TypeSerializer[]{
				IntSerializer.INSTANCE,
				LongSerializer.INSTANCE,
				BinaryStringSerializer.INSTANCE,
				BinaryStringSerializer.INSTANCE,
				new BinaryGenericSerializer<>(genericSerializer)
			});
		writer.writeRow(0, gRow, serializer);
		writer.complete();

		return row;
	}
}
