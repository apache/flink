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

package org.apache.flink.table.runtime.util.collections.binary;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.disk.RandomAccessInputView;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.util.KeyValueIterator;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/** Test case for {@link BytesHashMap}. */
public class BytesHashMapTest extends BytesMapTestBase {

    private static final int NUM_REWRITES = 10;

    private final LogicalType[] keyTypes;
    private final LogicalType[] valueTypes;
    private final BinaryRowData defaultValue;
    private final BinaryRowDataSerializer keySerializer;
    private final BinaryRowDataSerializer valueSerializer;

    public BytesHashMapTest() {

        this.keyTypes =
                new LogicalType[] {
                    new IntType(),
                    new VarCharType(VarCharType.MAX_LENGTH),
                    new DoubleType(),
                    new BigIntType(),
                    new BooleanType(),
                    new FloatType(),
                    new SmallIntType()
                };
        this.valueTypes =
                new LogicalType[] {
                    new DoubleType(),
                    new BigIntType(),
                    new BooleanType(),
                    new FloatType(),
                    new SmallIntType()
                };

        this.keySerializer = new BinaryRowDataSerializer(keyTypes.length);
        this.valueSerializer = new BinaryRowDataSerializer(valueTypes.length);
        this.defaultValue = valueSerializer.createInstance();
        int valueSize = defaultValue.getFixedLengthPartSize();
        this.defaultValue.pointTo(MemorySegmentFactory.wrap(new byte[valueSize]), 0, valueSize);
    }

    @Test
    public void testHashSetMode() throws IOException {
        final int numMemSegments =
                needNumMemSegments(
                        NUM_ENTRIES,
                        rowLength(RowType.of(valueTypes)),
                        rowLength(RowType.of(keyTypes)),
                        PAGE_SIZE);
        int memorySize = numMemSegments * PAGE_SIZE;
        MemoryManager memoryManager =
                MemoryManagerBuilder.newBuilder().setMemorySize(numMemSegments * PAGE_SIZE).build();

        BytesHashMap table =
                new BytesHashMap(this, memoryManager, memorySize, keyTypes, new LogicalType[] {});
        Assert.assertTrue(table.isHashSetMode());

        final Random rnd = new Random(RANDOM_SEED);
        BinaryRowData[] keys = getRandomizedInput(NUM_ENTRIES, rnd, true);
        verifyKeyInsert(keys, table);
        verifyKeyPresent(keys, table);
        table.free();
    }

    @Test
    public void testBuildAndRetrieve() throws Exception {

        final int numMemSegments =
                needNumMemSegments(
                        NUM_ENTRIES,
                        rowLength(RowType.of(valueTypes)),
                        rowLength(RowType.of(keyTypes)),
                        PAGE_SIZE);
        int memorySize = numMemSegments * PAGE_SIZE;
        MemoryManager memoryManager =
                MemoryManagerBuilder.newBuilder().setMemorySize(memorySize).build();

        BytesHashMap table =
                new BytesHashMap(this, memoryManager, memorySize, keyTypes, valueTypes);

        final Random rnd = new Random(RANDOM_SEED);
        BinaryRowData[] rows = getRandomizedInput(NUM_ENTRIES, rnd, true);
        List<BinaryRowData> expected = new ArrayList<>(NUM_ENTRIES);
        verifyInsert(rows, expected, table);
        verifyRetrieve(table, rows, expected);
        table.free();
    }

    @Test
    public void testBuildAndUpdate() throws Exception {
        final Random rnd = new Random(RANDOM_SEED);
        final BinaryRowData[] rows = getRandomizedInput(NUM_ENTRIES, rnd, true);
        final int numMemSegments =
                needNumMemSegments(
                        NUM_ENTRIES,
                        rowLength(RowType.of(valueTypes)),
                        rowLength(RowType.of(keyTypes)),
                        PAGE_SIZE);
        int memorySize = numMemSegments * PAGE_SIZE;

        MemoryManager memoryManager =
                MemoryManagerBuilder.newBuilder().setMemorySize(memorySize).build();

        BytesHashMap table =
                new BytesHashMap(this, memoryManager, memorySize, keyTypes, valueTypes);

        List<BinaryRowData> expected = new ArrayList<>(NUM_ENTRIES);
        verifyInsertAndUpdate(rnd, rows, expected, table);
        verifyRetrieve(table, rows, expected);
        table.free();
    }

    @Test
    public void testRest() throws Exception {
        final Random rnd = new Random(RANDOM_SEED);
        final BinaryRowData[] rows = getRandomizedInput(NUM_ENTRIES, rnd, true);

        final int numMemSegments =
                needNumMemSegments(
                        NUM_ENTRIES,
                        rowLength(RowType.of(valueTypes)),
                        rowLength(RowType.of(keyTypes)),
                        PAGE_SIZE);

        int memorySize = numMemSegments * PAGE_SIZE;

        MemoryManager memoryManager =
                MemoryManagerBuilder.newBuilder().setMemorySize(memorySize).build();

        BytesHashMap table =
                new BytesHashMap(this, memoryManager, memorySize, keyTypes, valueTypes);

        List<BinaryRowData> expected = new ArrayList<>(NUM_ENTRIES);
        verifyInsertAndUpdate(rnd, rows, expected, table);
        verifyRetrieve(table, rows, expected);

        table.reset();
        Assert.assertEquals(0, table.getNumElements());
        Assert.assertEquals(1, table.getRecordAreaMemorySegments().size());

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

        MemoryManager memoryManager =
                MemoryManagerBuilder.newBuilder().setMemorySize(minMemorySize).build();
        BytesHashMap table =
                new BytesHashMap(this, memoryManager, minMemorySize, keyTypes, valueTypes, true);

        BinaryRowData[] rows = getRandomizedInput(NUM_ENTRIES, rnd, true);

        List<BinaryRowData> expected = new ArrayList<>(NUM_ENTRIES);
        List<BinaryRowData> actualValues = new ArrayList<>(NUM_ENTRIES);
        List<BinaryRowData> actualKeys = new ArrayList<>(NUM_ENTRIES);
        for (int i = 0; i < NUM_ENTRIES; i++) {
            BinaryRowData groupKey = rows[i];
            // look up and insert
            BytesHashMap.LookupInfo<BinaryRowData> info = table.lookup(groupKey);
            Assert.assertFalse(info.isFound());
            try {
                BinaryRowData entry = table.append(info, defaultValue);
                Assert.assertNotNull(entry);
                // mock multiple updates
                for (int j = 0; j < NUM_REWRITES; j++) {
                    updateOutputBuffer(entry, rnd);
                }
                expected.add(entry.copy());
            } catch (Exception e) {
                ArrayList<MemorySegment> segments = table.getRecordAreaMemorySegments();
                RandomAccessInputView inView =
                        new RandomAccessInputView(segments, segments.get(0).size());
                BinaryRowData reuseKey = this.keySerializer.createInstance();
                BinaryRowData reuseValue = this.valueSerializer.createInstance();
                for (int index = 0; index < table.getNumElements(); index++) {
                    reuseKey = keySerializer.mapFromPages(reuseKey, inView);
                    reuseValue = valueSerializer.mapFromPages(reuseValue, inView);
                    actualKeys.add(reuseKey.copy());
                    actualValues.add(reuseValue.copy());
                }
                table.reset();
                // retry
                info = table.lookup(groupKey);
                BinaryRowData entry = table.append(info, defaultValue);
                Assert.assertNotNull(entry);
                // mock multiple updates
                for (int j = 0; j < NUM_REWRITES; j++) {
                    updateOutputBuffer(entry, rnd);
                }
                expected.add(entry.copy());
            }
        }
        KeyValueIterator<RowData, RowData> iter = table.getEntryIterator();
        while (iter.advanceNext()) {
            actualKeys.add(((BinaryRowData) iter.getKey()).copy());
            actualValues.add(((BinaryRowData) iter.getValue()).copy());
        }
        Assert.assertEquals(NUM_ENTRIES, expected.size());
        Assert.assertEquals(NUM_ENTRIES, actualKeys.size());
        Assert.assertEquals(NUM_ENTRIES, actualValues.size());
        Assert.assertEquals(expected, actualValues);
        table.free();
    }

    @Test
    public void testSingleKeyMultipleOps() throws Exception {
        final int numMemSegments =
                needNumMemSegments(
                        NUM_ENTRIES,
                        rowLength(RowType.of(valueTypes)),
                        rowLength(RowType.of(keyTypes)),
                        PAGE_SIZE);

        int memorySize = numMemSegments * PAGE_SIZE;
        MemoryManager memoryManager =
                MemoryManagerBuilder.newBuilder().setMemorySize(memorySize).build();
        BytesHashMap table =
                new BytesHashMap(this, memoryManager, memorySize, keyTypes, valueTypes);
        final Random rnd = new Random(RANDOM_SEED);
        BinaryRowData row = getNullableGroupkeyInput(rnd);
        for (int i = 0; i < 3; i++) {
            BytesHashMap.LookupInfo<BinaryRowData> info = table.lookup(row);
            Assert.assertFalse(info.isFound());
        }

        for (int i = 0; i < 3; i++) {
            BytesHashMap.LookupInfo<BinaryRowData> info = table.lookup(row);
            BinaryRowData entry = info.getValue();
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
    /** It will be codegened when in HashAggExec using rnd to mock update/initExprs resultTerm. */
    private void updateOutputBuffer(BinaryRowData reuse, Random rnd) {
        long longVal = rnd.nextLong();
        double doubleVal = rnd.nextDouble();
        boolean boolVal = longVal % 2 == 0;
        reuse.setDouble(2, doubleVal);
        reuse.setLong(3, longVal);
        reuse.setBoolean(4, boolVal);
    }

    // ----------------------- Utilities  -----------------------

    private void verifyRetrieve(
            BytesHashMap table, BinaryRowData[] keys, List<BinaryRowData> expected) {
        Assert.assertEquals(NUM_ENTRIES, table.getNumElements());
        for (int i = 0; i < NUM_ENTRIES; i++) {
            BinaryRowData groupKey = keys[i];
            // look up and retrieve
            BytesHashMap.LookupInfo<BinaryRowData> info = table.lookup(groupKey);
            Assert.assertTrue(info.isFound());
            Assert.assertNotNull(info.getValue());
            Assert.assertEquals(expected.get(i), info.getValue());
        }
    }

    private void verifyInsert(
            BinaryRowData[] keys, List<BinaryRowData> inserted, BytesHashMap table)
            throws IOException {
        for (int i = 0; i < NUM_ENTRIES; i++) {
            BinaryRowData groupKey = keys[i];
            // look up and insert
            BytesHashMap.LookupInfo<BinaryRowData> info = table.lookup(groupKey);
            Assert.assertFalse(info.isFound());
            BinaryRowData entry = table.append(info, defaultValue);
            Assert.assertNotNull(entry);
            Assert.assertEquals(entry, defaultValue);
            inserted.add(entry.copy());
        }
        Assert.assertEquals(NUM_ENTRIES, table.getNumElements());
    }

    private void verifyInsertAndUpdate(
            Random rnd, BinaryRowData[] keys, List<BinaryRowData> inserted, BytesHashMap table)
            throws IOException {
        for (int i = 0; i < NUM_ENTRIES; i++) {
            BinaryRowData groupKey = keys[i];
            // look up and insert
            BytesHashMap.LookupInfo<BinaryRowData> info = table.lookup(groupKey);
            Assert.assertFalse(info.isFound());
            BinaryRowData entry = table.append(info, defaultValue);
            Assert.assertNotNull(entry);
            // mock multiple updates
            for (int j = 0; j < NUM_REWRITES; j++) {
                updateOutputBuffer(entry, rnd);
            }
            inserted.add(entry.copy());
        }
        Assert.assertEquals(NUM_ENTRIES, table.getNumElements());
    }

    private void verifyKeyPresent(BinaryRowData[] keys, BytesHashMap table) {
        Assert.assertEquals(NUM_ENTRIES, table.getNumElements());
        BinaryRowData present = new BinaryRowData(0);
        present.pointTo(MemorySegmentFactory.wrap(new byte[8]), 0, 8);
        for (int i = 0; i < NUM_ENTRIES; i++) {
            BinaryRowData groupKey = keys[i];
            // look up and retrieve
            BytesHashMap.LookupInfo<BinaryRowData> info = table.lookup(groupKey);
            Assert.assertTrue(info.isFound());
            Assert.assertNotNull(info.getValue());
            Assert.assertEquals(present, info.getValue());
        }
    }

    private void verifyKeyInsert(BinaryRowData[] keys, BytesHashMap table) throws IOException {
        BinaryRowData present = new BinaryRowData(0);
        present.pointTo(MemorySegmentFactory.wrap(new byte[8]), 0, 8);
        for (int i = 0; i < NUM_ENTRIES; i++) {
            BinaryRowData groupKey = keys[i];
            // look up and insert
            BytesHashMap.LookupInfo<BinaryRowData> info = table.lookup(groupKey);
            Assert.assertFalse(info.isFound());
            BinaryRowData entry = table.append(info, defaultValue);
            Assert.assertNotNull(entry);
            Assert.assertEquals(entry, present);
        }
        Assert.assertEquals(NUM_ENTRIES, table.getNumElements());
    }

    // ----------------------------------------------

    private BinaryRowData getNullableGroupkeyInput(Random rnd) {
        int intVal = -rnd.nextInt(Integer.MAX_VALUE);
        Long longVal = rnd.nextLong();
        Boolean boolVal = intVal % 2 == 0;
        Double doubleVal = rnd.nextDouble();
        Short shotVal = (short) intVal;
        Float floatVal = rnd.nextFloat();
        return createRow(intVal, null, doubleVal, longVal, boolVal, floatVal, shotVal);
    }
}
