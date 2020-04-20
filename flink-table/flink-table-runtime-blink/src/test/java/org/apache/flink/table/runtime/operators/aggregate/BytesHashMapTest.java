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

package org.apache.flink.table.runtime.operators.aggregate;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.disk.RandomAccessInputView;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.BinaryRowWriter;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.runtime.typeutils.BinaryRowSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 */
public class BytesHashMapTest {

	private static final long RANDOM_SEED = 76518743207143L;
	private static final int PAGE_SIZE = 32 * 1024;
	private static final int NUM_ENTRIES = 10000;
	private static final int NUM_REWRITES = 10;

	private final LogicalType[] keyTypes;
	private final LogicalType[] valueTypes;
	private final BinaryRow defaultValue;
	private final BinaryRowSerializer keySerializer;
	private final BinaryRowSerializer valueSerializer;

	public BytesHashMapTest() {

		this.keyTypes = new LogicalType[] {
				new IntType(),
				new VarCharType(VarCharType.MAX_LENGTH),
				new DoubleType(),
				new BigIntType(),
				new BooleanType(),
				new FloatType(),
				new SmallIntType()
		};
		this.valueTypes = new LogicalType[] {
				new DoubleType(),
				new BigIntType(),
				new BooleanType(),
				new FloatType(),
				new SmallIntType()
		};

		this.keySerializer = new BinaryRowSerializer(keyTypes.length);
		this.valueSerializer = new BinaryRowSerializer(valueTypes.length);
		this.defaultValue = valueSerializer.createInstance();
		int valueSize = defaultValue.getFixedLengthPartSize();
		this.defaultValue.pointTo(MemorySegmentFactory.wrap(new byte[valueSize]), 0, valueSize);
	}

	@Test
	public void testHashSetMode() throws IOException {
		final int numMemSegments = needNumMemSegments(
				NUM_ENTRIES,
				rowLength(RowType.of(valueTypes)),
				rowLength(RowType.of(keyTypes)),
				PAGE_SIZE);
		int memorySize = numMemSegments * PAGE_SIZE;
		MemoryManager memoryManager = MemoryManagerBuilder
			.newBuilder()
			.setMemorySize(numMemSegments * PAGE_SIZE)
			.build();

		BytesHashMap table = new BytesHashMap(this, memoryManager,
				memorySize, keyTypes, new LogicalType[]{});
		Assert.assertTrue(table.isHashSetMode());

		final Random rnd = new Random(RANDOM_SEED);
		BinaryRow[] keys = getRandomizedInput(NUM_ENTRIES, rnd, true);
		verifyKeyInsert(keys, table);
		verifyKeyPresent(keys, table);
		table.free();
	}

	@Test
	public void testBuildAndRetrieve() throws Exception {

		final int numMemSegments = needNumMemSegments(
				NUM_ENTRIES,
				rowLength(RowType.of(valueTypes)),
				rowLength(RowType.of(keyTypes)),
				PAGE_SIZE);
		int memorySize = numMemSegments * PAGE_SIZE;
		MemoryManager memoryManager = MemoryManagerBuilder
			.newBuilder()
			.setMemorySize(memorySize)
			.build();

		BytesHashMap table = new BytesHashMap(this, memoryManager,
				memorySize, keyTypes, valueTypes);

		final Random rnd = new Random(RANDOM_SEED);
		BinaryRow[] rows = getRandomizedInput(NUM_ENTRIES, rnd, true);
		List<BinaryRow> expected = new ArrayList<>(NUM_ENTRIES);
		verifyInsert(rows, expected, table);
		verifyRetrieve(table, rows, expected);
		table.free();
	}

	@Test
	public void testBuildAndUpdate() throws Exception {
		final Random rnd = new Random(RANDOM_SEED);
		final BinaryRow[] rows = getRandomizedInput(NUM_ENTRIES, rnd, true);
		final int numMemSegments = needNumMemSegments(
				NUM_ENTRIES,
				rowLength(RowType.of(valueTypes)),
				rowLength(RowType.of(keyTypes)),
				PAGE_SIZE);
		int memorySize = numMemSegments * PAGE_SIZE;

		MemoryManager memoryManager = MemoryManagerBuilder
			.newBuilder()
			.setMemorySize(memorySize)
			.build();

		BytesHashMap table = new BytesHashMap(this, memoryManager,
				memorySize, keyTypes, valueTypes);

		List<BinaryRow> expected = new ArrayList<>(NUM_ENTRIES);
		verifyInsertAndUpdate(rnd, rows, expected, table);
		verifyRetrieve(table, rows, expected);
		table.free();
	}

	@Test
	public void testRest() throws Exception {
		final Random rnd = new Random(RANDOM_SEED);
		final BinaryRow[] rows = getRandomizedInput(NUM_ENTRIES, rnd, true);

		final int numMemSegments = needNumMemSegments(
				NUM_ENTRIES,
				rowLength(RowType.of(valueTypes)),
				rowLength(RowType.of(keyTypes)),
				PAGE_SIZE);

		int memorySize = numMemSegments * PAGE_SIZE;

		MemoryManager memoryManager = MemoryManagerBuilder
			.newBuilder()
			.setMemorySize(memorySize)
			.build();

		BytesHashMap table = new BytesHashMap(this, memoryManager,
				memorySize, keyTypes, valueTypes);

		List<BinaryRow> expected = new ArrayList<>(NUM_ENTRIES);
		verifyInsertAndUpdate(rnd, rows, expected, table);
		verifyRetrieve(table, rows, expected);

		table.reset();
		Assert.assertEquals(0, table.getNumElements());
		Assert.assertTrue(table.getRecordAreaMemorySegments().size() == 1);

		expected.clear();
		verifyInsertAndUpdate(rnd, rows, expected, table);
		verifyRetrieve(table, rows, expected);
		table.free();
	}

	@Test
	public void testResetAndOutput() throws Exception {
		final Random rnd = new Random(RANDOM_SEED);
		final int reservedMemSegments = 32;

		int minMemorySize = reservedMemSegments * PAGE_SIZE;

		MemoryManager memoryManager = MemoryManagerBuilder
			.newBuilder()
			.setMemorySize(minMemorySize)
			.build();
		BytesHashMap table = new BytesHashMap(this, memoryManager,
				minMemorySize, keyTypes, valueTypes, true);

		BinaryRow[] rows = getRandomizedInput(NUM_ENTRIES, rnd, true);

		List<BinaryRow> expected = new ArrayList<>(NUM_ENTRIES);
		List<BinaryRow> actualValues = new ArrayList<>(NUM_ENTRIES);
		List<BinaryRow> actualKeys = new ArrayList<>(NUM_ENTRIES);
		for (int i = 0; i < NUM_ENTRIES; i++) {
			BinaryRow groupKey = rows[i];
			// look up and insert
			BytesHashMap.LookupInfo info = table.lookup(groupKey);
			Assert.assertFalse(info.isFound());
			try {
				BinaryRow entry =
						table.append(info, defaultValue);
				Assert.assertNotNull(entry);
				// mock multiple updates
				for (int j = 0; j < NUM_REWRITES; j++) {
					updateOutputBuffer(entry, rnd);
				}
				expected.add(entry.copy());
			} catch (Exception e) {
				ArrayList<MemorySegment> segments = table.getRecordAreaMemorySegments();
				RandomAccessInputView inView = new RandomAccessInputView(segments, segments.get(0).size());
				BinaryRow reuseKey = this.keySerializer.createInstance();
				BinaryRow reuseValue = this.valueSerializer.createInstance();
				BytesHashMap.Entry reuse = new BytesHashMap.Entry(reuseKey, reuseValue);
				for (int index = 0; index < table.getNumElements(); index++) {
					reuseKey = keySerializer.mapFromPages(reuseKey, inView);
					reuseValue = valueSerializer.mapFromPages(reuseValue, inView);
					actualKeys.add(reuse.getKey().copy());
					actualValues.add(reuse.getValue().copy());
				}
				table.reset();
				// retry
				info = table.lookup(groupKey);
				BinaryRow entry = table.append(info, defaultValue);
				Assert.assertNotNull(entry);
				// mock multiple updates
				for (int j = 0; j < NUM_REWRITES; j++) {
					updateOutputBuffer(entry, rnd);
				}
				expected.add(entry.copy());
			}
		}
		MutableObjectIterator<BytesHashMap.Entry> iter = table.getEntryIterator();
		BinaryRow reuseKey = this.keySerializer.createInstance();
		BinaryRow reuseValue = this.valueSerializer.createInstance();
		BytesHashMap.Entry reuse = new BytesHashMap.Entry(reuseKey, reuseValue);

		while ((reuse = iter.next(reuse)) != null) {
			actualKeys.add(reuse.getKey().copy());
			actualValues.add(reuse.getValue().copy());
		}
		Assert.assertEquals(NUM_ENTRIES, expected.size());
		Assert.assertEquals(NUM_ENTRIES, actualKeys.size());
		Assert.assertEquals(NUM_ENTRIES, actualValues.size());
		Assert.assertEquals(expected, actualValues);
		table.free();
	}

	@Test
	public void testSingleKeyMultipleOps() throws Exception {
		final int numMemSegments = needNumMemSegments(
				NUM_ENTRIES,
				rowLength(RowType.of(valueTypes)),
				rowLength(RowType.of(keyTypes)),
				PAGE_SIZE);

		int memorySize = numMemSegments * PAGE_SIZE;
		MemoryManager memoryManager = MemoryManagerBuilder
			.newBuilder()
			.setMemorySize(memorySize)
			.build();
		BytesHashMap table = new BytesHashMap(this, memoryManager,
				memorySize, keyTypes, valueTypes);
		final Random rnd = new Random(RANDOM_SEED);
		BinaryRow row = getNullableGroupkeyInput(rnd);
		for (int i = 0; i < 3; i++) {
			BytesHashMap.LookupInfo info = table.lookup(row);
			Assert.assertFalse(info.isFound());
		}

		for (int i = 0; i < 3; i++) {
			BytesHashMap.LookupInfo info = table.lookup(row);
			BinaryRow entry = info.getValue();
			if (i == 0) {
				Assert.assertFalse(info.isFound());
				entry = table.append(info, defaultValue);
			} else {
				Assert.assertTrue(info.isFound());
			}
			Assert.assertNotNull(entry);
		}
		table.free();
	}

	// ----------------------------------------------
	/**
	 * It will be codegened when in HashAggExec
	 * using rnd to mock update/initExprs resultTerm.
	 */
	private void updateOutputBuffer(BinaryRow reuse, Random rnd) {
		long longVal = rnd.nextLong();
		double doubleVal = rnd.nextDouble();
		boolean boolVal = longVal % 2 == 0;
		reuse.setDouble(2, doubleVal);
		reuse.setLong(3, longVal);
		reuse.setBoolean(4, boolVal);
	}

	// ----------------------- Utilities  -----------------------

	private void verifyRetrieve(
			BytesHashMap table,
			BinaryRow[] keys,
			List<BinaryRow> expected) throws IOException {
		Assert.assertEquals(NUM_ENTRIES, table.getNumElements());
		for (int i = 0; i < NUM_ENTRIES; i++) {
			BinaryRow groupKey = keys[i];
			// look up and retrieve
			BytesHashMap.LookupInfo info = table.lookup(groupKey);
			Assert.assertTrue(info.isFound());
			Assert.assertNotNull(info.getValue());
			Assert.assertEquals(expected.get(i), info.getValue());
		}
	}

	private void verifyInsert(
			BinaryRow[] keys,
			List<BinaryRow> inserted,
			BytesHashMap table) throws IOException {
		for (int i = 0; i < NUM_ENTRIES; i++) {
			BinaryRow groupKey = keys[i];
			// look up and insert
			BytesHashMap.LookupInfo info = table.lookup(groupKey);
			Assert.assertFalse(info.isFound());
			BinaryRow entry  = table.append(info, defaultValue);
			Assert.assertNotNull(entry);
			Assert.assertEquals(entry, defaultValue);
			inserted.add(entry.copy());
		}
		Assert.assertEquals(NUM_ENTRIES, table.getNumElements());
	}

	private void verifyInsertAndUpdate(
			Random rnd,
			BinaryRow[] keys,
			List<BinaryRow> inserted,
			BytesHashMap table) throws IOException {
		for (int i = 0; i < NUM_ENTRIES; i++) {
			BinaryRow groupKey = keys[i];
			// look up and insert
			BytesHashMap.LookupInfo info = table.lookup(groupKey);
			Assert.assertFalse(info.isFound());
			BinaryRow entry  = table.append(info, defaultValue);
			Assert.assertNotNull(entry);
			// mock multiple updates
			for (int j = 0; j < NUM_REWRITES; j++) {
				updateOutputBuffer(entry, rnd);
			}
			inserted.add(entry.copy());
		}
		Assert.assertEquals(NUM_ENTRIES, table.getNumElements());
	}

	private void verifyKeyPresent(BinaryRow[] keys, BytesHashMap table) throws IOException {
		Assert.assertEquals(NUM_ENTRIES, table.getNumElements());
		BinaryRow present = new BinaryRow(0);
		present.pointTo(MemorySegmentFactory.wrap(new byte[8]), 0, 8);
		for (int i = 0; i < NUM_ENTRIES; i++) {
			BinaryRow groupKey = keys[i];
			// look up and retrieve
			BytesHashMap.LookupInfo info = table.lookup(groupKey);
			Assert.assertTrue(info.isFound());
			Assert.assertNotNull(info.getValue());
			Assert.assertEquals(present, info.getValue());
		}
	}

	private void verifyKeyInsert(BinaryRow[] keys, BytesHashMap table) throws IOException {
		BinaryRow present = new BinaryRow(0);
		present.pointTo(MemorySegmentFactory.wrap(new byte[8]), 0, 8);
		for (int i = 0; i < NUM_ENTRIES; i++) {
			BinaryRow groupKey = keys[i];
			// look up and insert
			BytesHashMap.LookupInfo info = table.lookup(groupKey);
			Assert.assertFalse(info.isFound());
			BinaryRow entry  = table.append(info, defaultValue);
			Assert.assertNotNull(entry);
			Assert.assertEquals(entry, present);
		}
		Assert.assertEquals(NUM_ENTRIES, table.getNumElements());
	}

	// ----------------------------------------------

	private List<MemorySegment> getMemory(int numSegments, int segmentSize) {
		ArrayList<MemorySegment> list = new ArrayList<>(numSegments);
		for (int i = 0; i < numSegments; i++) {
			list.add(MemorySegmentFactory.allocateUnpooledSegment(segmentSize));
		}
		return list;
	}

	private int needNumMemSegments(int numEntries, int valLen, int keyLen, int pageSize) {
		return 2 * (valLen + keyLen + 1024 * 3 + 4 + 8 + 8) * numEntries / pageSize;
	}

	// ----------------------------------------------

	private BinaryRow[] getRandomizedInput(int num, Random rnd, boolean nullable) {
		BinaryRow[] lists = new BinaryRow[num];
		for (int i = 0; i < num; i++) {
			Integer intVal = rnd.nextInt(Integer.MAX_VALUE);
			Long longVal = -rnd.nextLong();
			Boolean boolVal = longVal % 2 == 0;
			String strVal = nullable && boolVal ? null : getString(intVal, intVal % 1024) + i;
			Double doubleVal = rnd.nextDouble();
			Short shotVal = intVal.shortValue();
			Float floatVal = nullable && boolVal ? null : rnd.nextFloat();
			lists[i] = createRow(intVal, strVal, doubleVal, longVal, boolVal, floatVal, shotVal);
		}
		return lists;
	}

	private BinaryRow getNullableGroupkeyInput(Random rnd) {
		Integer intVal = -rnd.nextInt(Integer.MAX_VALUE);
		Long longVal = rnd.nextLong();
		Boolean boolVal = intVal % 2 == 0;
		Double doubleVal = rnd.nextDouble();
		Short shotVal = intVal.shortValue();
		Float floatVal = rnd.nextFloat();
		return createRow(intVal, null, doubleVal, longVal, boolVal, floatVal, shotVal);
	}

	private BinaryRow createRow(
			Integer f0, String f1,
			Double f2, Long f3, Boolean f4, Float f5, Short f6) {

		BinaryRow row = new BinaryRow(7);
		BinaryRowWriter writer = new BinaryRowWriter(row);

		// int, string, double, long, boolean
		if (f0 == null) {
			writer.setNullAt(0);
		} else {
			writer.writeInt(0, f0);
		}
		if (f1 == null) {
			writer.setNullAt(1);
		} else {
			writer.writeString(1, BinaryString.fromString(f1));
		}
		if (f2 == null) {
			writer.setNullAt(2);
		} else {
			writer.writeDouble(2, f2);
		}
		if (f3 == null) {
			writer.setNullAt(3);
		} else {
			writer.writeLong(3, f3);
		}
		if (f4 == null) {
			writer.setNullAt(4);
		} else {
			writer.writeBoolean(4, f4);
		}
		if (f5 == null) {
			writer.setNullAt(5);
		} else {
			writer.writeFloat(5, f5);
		}
		if (f6 == null) {
			writer.setNullAt(6);
		} else {
			writer.writeShort(6, f6);
		}
		writer.complete();
		return row;
	}

	private String getString(int count, int length) {
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < length; i++) {
			builder.append(count);
		}
		return builder.toString();
	}

	private int rowLength(RowType tpe) {
		return BinaryRow.calculateFixPartSizeInBytes(tpe.getFieldCount())
				+ BytesHashMap.getVariableLength(tpe.getChildren().toArray(new LogicalType[0]));
	}

}
