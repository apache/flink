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

package org.apache.flink.table.typeutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.SerializerTestInstance;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryArray;
import org.apache.flink.table.dataformat.BinaryArrayWriter;
import org.apache.flink.table.dataformat.BinaryMap;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.type.InternalTypes;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.apache.flink.table.dataformat.BinaryString.fromString;
import static org.junit.Assert.assertEquals;

/**
 * Test for {@link BaseRowSerializer}.
 */
@RunWith(Parameterized.class)
public class BaseRowSerializerTest extends SerializerTestInstance<BaseRow> {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private final BaseRowSerializer serializer;
	private final BaseRow[] testData;

	public BaseRowSerializerTest(BaseRowSerializer serializer, BaseRow[] testData) {
		super(serializer, BaseRow.class, -1, testData);
		this.serializer = serializer;
		this.testData = testData;
	}

	@Parameterized.Parameters
	public static Collection<Object[]> parameters() {
		return Arrays.asList(
				testBaseRowSerializer(),
				testLargeBaseRowSerializer(),
				testBaseRowSerializerWithComplexTypes());
	}

	private static Object[] testBaseRowSerializer() {
		BaseRowTypeInfo typeInfo = new BaseRowTypeInfo(InternalTypes.INT, InternalTypes.STRING);
		GenericRow row1 = new GenericRow(2);
		row1.setField(0, 1);
		row1.setField(1, fromString("a"));

		GenericRow row2 = new GenericRow(2);
		row2.setField(0, 2);
		row2.setField(1, null);

		BaseRowSerializer serializer = typeInfo.createSerializer(new ExecutionConfig());
		return new Object[] {serializer, new BaseRow[]{row1, row2}};
	}

	private static Object[] testLargeBaseRowSerializer() {
		BaseRowTypeInfo typeInfo = new BaseRowTypeInfo(
			InternalTypes.INT,
			InternalTypes.INT,
			InternalTypes.INT,
			InternalTypes.INT,
			InternalTypes.INT,
			InternalTypes.INT,
			InternalTypes.INT,
			InternalTypes.INT,
			InternalTypes.INT,
			InternalTypes.INT,
			InternalTypes.INT,
			InternalTypes.INT,
			InternalTypes.STRING);

		GenericRow row = new GenericRow(13);
		row.setField(0, 2);
		row.setField(1, null);
		row.setField(3, null);
		row.setField(4, null);
		row.setField(5, null);
		row.setField(6, null);
		row.setField(7, null);
		row.setField(8, null);
		row.setField(9, null);
		row.setField(10, null);
		row.setField(11, null);
		row.setField(12, fromString("Test"));

		BaseRowSerializer serializer = typeInfo.createSerializer(new ExecutionConfig());
		return new Object[] {serializer, new BaseRow[]{row}};
	}

	private static Object[] testBaseRowSerializerWithComplexTypes() {
		BaseRowTypeInfo typeInfo = new BaseRowTypeInfo(
			InternalTypes.INT,
			InternalTypes.DOUBLE,
			InternalTypes.STRING,
			InternalTypes.createArrayType(InternalTypes.INT),
			InternalTypes.createMapType(InternalTypes.INT, InternalTypes.INT));

		GenericRow[] data = new GenericRow[]{
			createRow(null, null, null, null, null),
			createRow(0, null, null, null, null),
			createRow(0, 0.0, null, null, null),
			createRow(0, 0.0, fromString("a"), null, null),
			createRow(1, 0.0, fromString("a"), null, null),
			createRow(1, 1.0, fromString("a"), null, null),
			createRow(1, 1.0, fromString("b"), null, null),
			createRow(1, 1.0, fromString("b"), createArray(1), createMap(new int[]{1}, new int[]{1})),
			createRow(1, 1.0, fromString("b"), createArray(1, 2), createMap(new int[]{1, 4}, new int[]{1, 2})),
			createRow(1, 1.0, fromString("b"), createArray(1, 2, 3), createMap(new int[]{1, 5}, new int[]{1, 3})),
			createRow(1, 1.0, fromString("b"), createArray(1, 2, 3, 4), createMap(new int[]{1, 6}, new int[]{1, 4})),
			createRow(1, 1.0, fromString("b"), createArray(1, 2, 3, 4, 5), createMap(new int[]{1, 7}, new int[]{1, 5})),
			createRow(1, 1.0, fromString("b"), createArray(1, 2, 3, 4, 5, 6), createMap(new int[]{1, 8}, new int[]{1, 6}))
		};

		BaseRowSerializer serializer = typeInfo.createSerializer(new ExecutionConfig());
		return new Object[] {serializer, data};
	}

	// ----------------------------------------------------------------------------------------------

	private static BinaryArray createArray(int... ints) {
		BinaryArray array = new BinaryArray();
		BinaryArrayWriter writer = new BinaryArrayWriter(array, ints.length, 4);
		for (int i = 0; i < ints.length; i++) {
			writer.writeInt(i, ints[i]);
		}
		writer.complete();
		return array;
	}

	private static BinaryMap createMap(int[] keys, int[] values) {
		return BinaryMap.valueOf(createArray(keys), createArray(values));
	}

	private static GenericRow createRow(Object f0, Object f1, Object f2, Object f3, Object f4) {
		GenericRow row = new GenericRow(5);
		row.setField(0, f0);
		row.setField(1, f1);
		row.setField(2, f2);
		row.setField(3, f3);
		row.setField(4, f4);
		return row;
	}

	@Override
	protected void deepEquals(String message, BaseRow should, BaseRow is) {
		int arity = should.getArity();
		assertEquals(message, arity, is.getArity());
		assertEquals(serializer.baseRowToBinary(should), serializer.baseRowToBinary(is));
	}

	private void deepEquals(BaseRow should, BaseRow is) {
		deepEquals("", should, is);
	}

	/**
	 * Override testDuplicate, Because it uses Object equals, deserialize BaseRow to BinaryRow,
	 * which cannot be directly equals.
	 * See {@link BaseRowSerializer#deserialize}.
	 */
	@Test
	@Override
	public void testDuplicate() throws Exception {}

	@Test
	public void testCopy() {
		for (BaseRow row : testData) {
			deepEquals(row, serializer.copy(row));
		}

		for (BaseRow row : testData) {
			deepEquals(row, serializer.copy(row, new GenericRow(row.getArity())));
		}

		for (BaseRow row : testData) {
			deepEquals(row, serializer.copy(serializer.baseRowToBinary(row),
					new GenericRow(row.getArity())));
		}

		for (BaseRow row : testData) {
			deepEquals(row, serializer.copy(serializer.baseRowToBinary(row)));
		}

		for (BaseRow row : testData) {
			deepEquals(row, serializer.copy(serializer.baseRowToBinary(row),
					new BinaryRow(row.getArity())));
		}
	}

	@Test
	public void testWrongCopy() {
		thrown.expect(IllegalArgumentException.class);
		serializer.copy(new GenericRow(serializer.getArity() + 1));
	}

	@Test
	public void testWrongCopyReuse() {
		thrown.expect(IllegalArgumentException.class);
		for (BaseRow row : testData) {
			deepEquals(row, serializer.copy(row, new GenericRow(row.getArity() + 1)));
		}
	}
}
