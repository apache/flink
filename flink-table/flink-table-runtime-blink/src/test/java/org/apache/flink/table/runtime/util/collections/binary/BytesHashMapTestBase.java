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
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.PagedTypeSerializer;
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

/** Base test class for both {@link BytesHashMap} and {@link WindowBytesHashMap}. */
public abstract class BytesHashMapTestBase<K> extends BytesMapTestBase {

    private static final int NUM_REWRITES = 10;

    static final LogicalType[] KEY_TYPES =
            new LogicalType[] {
                new IntType(),
                new VarCharType(VarCharType.MAX_LENGTH),
                new DoubleType(),
                new BigIntType(),
                new BooleanType(),
                new FloatType(),
                new SmallIntType()
            };

    static final LogicalType[] VALUE_TYPES =
            new LogicalType[] {
                new DoubleType(),
                new BigIntType(),
                new BooleanType(),
                new FloatType(),
                new SmallIntType()
            };

    protected final BinaryRowData defaultValue;
    protected final PagedTypeSerializer<K> keySerializer;
    protected final BinaryRowDataSerializer valueSerializer;

    public BytesHashMapTestBase(PagedTypeSerializer<K> keySerializer) {
        this.keySerializer = keySerializer;
        this.valueSerializer = new BinaryRowDataSerializer(VALUE_TYPES.length);
        this.defaultValue = valueSerializer.createInstance();
        int valueSize = defaultValue.getFixedLengthPartSize();
        this.defaultValue.pointTo(MemorySegmentFactory.wrap(new byte[valueSize]), 0, valueSize);
    }

    /**
     * Creates the specific BytesHashMap, either {@link BytesHashMap} or {@link WindowBytesHashMap}.
     */
    public abstract AbstractBytesHashMap<K> createBytesHashMap(
            MemoryManager memoryManager,
            int memorySize,
            LogicalType[] keyTypes,
            LogicalType[] valueTypes);

    /**
     * Generates {@code num} random keys, the types of key fields are defined in {@link #KEY_TYPES}.
     */
    public abstract K[] generateRandomKeys(int num);

    // ------------------------------------------------------------------------------------------
    // Tests
    // ------------------------------------------------------------------------------------------

    @Test
    public void testHashSetMode() throws IOException {
        final int numMemSegments =
                needNumMemSegments(
                        NUM_ENTRIES,
                        rowLength(RowType.of(VALUE_TYPES)),
                        rowLength(RowType.of(KEY_TYPES)),
                        PAGE_SIZE);
        int memorySize = numMemSegments * PAGE_SIZE;
        MemoryManager memoryManager =
                MemoryManagerBuilder.newBuilder().setMemorySize(numMemSegments * PAGE_SIZE).build();

        AbstractBytesHashMap<K> table =
                createBytesHashMap(memoryManager, memorySize, KEY_TYPES, new LogicalType[] {});
        Assert.assertTrue(table.isHashSetMode());

        K[] keys = generateRandomKeys(NUM_ENTRIES);
        verifyKeyInsert(keys, table);
        verifyKeyPresent(keys, table);
        table.free();
    }

    @Test
    public void testBuildAndRetrieve() throws Exception {

        final int numMemSegments =
                needNumMemSegments(
                        NUM_ENTRIES,
                        rowLength(RowType.of(VALUE_TYPES)),
                        rowLength(RowType.of(KEY_TYPES)),
                        PAGE_SIZE);
        int memorySize = numMemSegments * PAGE_SIZE;
        MemoryManager memoryManager =
                MemoryManagerBuilder.newBuilder().setMemorySize(memorySize).build();

        AbstractBytesHashMap<K> table =
                createBytesHashMap(memoryManager, memorySize, KEY_TYPES, VALUE_TYPES);

        K[] keys = generateRandomKeys(NUM_ENTRIES);
        List<BinaryRowData> expected = new ArrayList<>(NUM_ENTRIES);
        verifyInsert(keys, expected, table);
        verifyRetrieve(table, keys, expected);
        table.free();
    }

    @Test
    public void testBuildAndUpdate() throws Exception {
        final int numMemSegments =
                needNumMemSegments(
                        NUM_ENTRIES,
                        rowLength(RowType.of(VALUE_TYPES)),
                        rowLength(RowType.of(KEY_TYPES)),
                        PAGE_SIZE);
        int memorySize = numMemSegments * PAGE_SIZE;

        MemoryManager memoryManager =
                MemoryManagerBuilder.newBuilder().setMemorySize(memorySize).build();

        AbstractBytesHashMap<K> table =
                createBytesHashMap(memoryManager, memorySize, KEY_TYPES, VALUE_TYPES);

        K[] keys = generateRandomKeys(NUM_ENTRIES);
        List<BinaryRowData> expected = new ArrayList<>(NUM_ENTRIES);
        verifyInsertAndUpdate(keys, expected, table);
        verifyRetrieve(table, keys, expected);
        table.free();
    }

    @Test
    public void testRest() throws Exception {
        final int numMemSegments =
                needNumMemSegments(
                        NUM_ENTRIES,
                        rowLength(RowType.of(VALUE_TYPES)),
                        rowLength(RowType.of(KEY_TYPES)),
                        PAGE_SIZE);

        int memorySize = numMemSegments * PAGE_SIZE;

        MemoryManager memoryManager =
                MemoryManagerBuilder.newBuilder().setMemorySize(memorySize).build();

        AbstractBytesHashMap<K> table =
                createBytesHashMap(memoryManager, memorySize, KEY_TYPES, VALUE_TYPES);

        final K[] keys = generateRandomKeys(NUM_ENTRIES);
        List<BinaryRowData> expected = new ArrayList<>(NUM_ENTRIES);
        verifyInsertAndUpdate(keys, expected, table);
        verifyRetrieve(table, keys, expected);

        table.reset();
        Assert.assertEquals(0, table.getNumElements());
        Assert.assertEquals(1, table.getRecordAreaMemorySegments().size());

        expected.clear();
        verifyInsertAndUpdate(keys, expected, table);
        verifyRetrieve(table, keys, expected);
        table.free();
    }

    @Test
    public void testResetAndOutput() throws Exception {
        final Random rnd = new Random(RANDOM_SEED);
        final int reservedMemSegments = 64;

        int minMemorySize = reservedMemSegments * PAGE_SIZE;

        MemoryManager memoryManager =
                MemoryManagerBuilder.newBuilder().setMemorySize(minMemorySize).build();
        AbstractBytesHashMap<K> table =
                createBytesHashMap(memoryManager, minMemorySize, KEY_TYPES, VALUE_TYPES);

        K[] keys = generateRandomKeys(NUM_ENTRIES);
        List<BinaryRowData> expected = new ArrayList<>(NUM_ENTRIES);
        List<BinaryRowData> actualValues = new ArrayList<>(NUM_ENTRIES);
        List<K> actualKeys = new ArrayList<>(NUM_ENTRIES);
        for (int i = 0; i < NUM_ENTRIES; i++) {
            K groupKey = keys[i];
            // look up and insert
            BytesMap.LookupInfo<K, BinaryRowData> lookupInfo = table.lookup(groupKey);
            Assert.assertFalse(lookupInfo.isFound());
            try {
                BinaryRowData entry = table.append(lookupInfo, defaultValue);
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
                K reuseKey = keySerializer.createInstance();
                BinaryRowData reuseValue = valueSerializer.createInstance();
                for (int index = 0; index < table.getNumElements(); index++) {
                    reuseKey = keySerializer.mapFromPages(reuseKey, inView);
                    reuseValue = valueSerializer.mapFromPages(reuseValue, inView);
                    actualKeys.add(keySerializer.copy(reuseKey));
                    actualValues.add(reuseValue.copy());
                }
                table.reset();
                // retry
                lookupInfo = table.lookup(groupKey);
                BinaryRowData entry = table.append(lookupInfo, defaultValue);
                Assert.assertNotNull(entry);
                // mock multiple updates
                for (int j = 0; j < NUM_REWRITES; j++) {
                    updateOutputBuffer(entry, rnd);
                }
                expected.add(entry.copy());
            }
        }
        KeyValueIterator<K, BinaryRowData> iter = table.getEntryIterator();
        while (iter.advanceNext()) {
            actualKeys.add(keySerializer.copy(iter.getKey()));
            actualValues.add(iter.getValue().copy());
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
                        rowLength(RowType.of(VALUE_TYPES)),
                        rowLength(RowType.of(KEY_TYPES)),
                        PAGE_SIZE);

        int memorySize = numMemSegments * PAGE_SIZE;
        MemoryManager memoryManager =
                MemoryManagerBuilder.newBuilder().setMemorySize(memorySize).build();
        AbstractBytesHashMap<K> table =
                createBytesHashMap(memoryManager, memorySize, KEY_TYPES, VALUE_TYPES);
        final K key = generateRandomKeys(1)[0];
        for (int i = 0; i < 3; i++) {
            BytesMap.LookupInfo<K, BinaryRowData> lookupInfo = table.lookup(key);
            Assert.assertFalse(lookupInfo.isFound());
        }

        for (int i = 0; i < 3; i++) {
            BytesMap.LookupInfo<K, BinaryRowData> lookupInfo = table.lookup(key);
            BinaryRowData entry = lookupInfo.getValue();
            if (i == 0) {
                Assert.assertFalse(lookupInfo.isFound());
                entry = table.append(lookupInfo, defaultValue);
            } else {
                Assert.assertTrue(lookupInfo.isFound());
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
            AbstractBytesHashMap<K> table, K[] keys, List<BinaryRowData> expected) {
        Assert.assertEquals(NUM_ENTRIES, table.getNumElements());
        for (int i = 0; i < NUM_ENTRIES; i++) {
            K groupKey = keys[i];
            // look up and retrieve
            BytesMap.LookupInfo<K, BinaryRowData> lookupInfo = table.lookup(groupKey);
            Assert.assertTrue(lookupInfo.isFound());
            Assert.assertNotNull(lookupInfo.getValue());
            Assert.assertEquals(expected.get(i), lookupInfo.getValue());
        }
    }

    private void verifyInsert(K[] keys, List<BinaryRowData> inserted, AbstractBytesHashMap<K> table)
            throws IOException {
        for (int i = 0; i < NUM_ENTRIES; i++) {
            K groupKey = keys[i];
            // look up and insert
            BytesMap.LookupInfo<K, BinaryRowData> lookupInfo = table.lookup(groupKey);
            Assert.assertFalse(lookupInfo.isFound());
            BinaryRowData entry = table.append(lookupInfo, defaultValue);
            Assert.assertNotNull(entry);
            Assert.assertEquals(entry, defaultValue);
            inserted.add(entry.copy());
        }
        Assert.assertEquals(NUM_ENTRIES, table.getNumElements());
    }

    private void verifyInsertAndUpdate(
            K[] keys, List<BinaryRowData> inserted, AbstractBytesHashMap<K> table)
            throws IOException {
        final Random rnd = new Random(RANDOM_SEED);
        for (int i = 0; i < NUM_ENTRIES; i++) {
            K groupKey = keys[i];
            // look up and insert
            BytesMap.LookupInfo<K, BinaryRowData> lookupInfo = table.lookup(groupKey);
            Assert.assertFalse(lookupInfo.isFound());
            BinaryRowData entry = table.append(lookupInfo, defaultValue);
            Assert.assertNotNull(entry);
            // mock multiple updates
            for (int j = 0; j < NUM_REWRITES; j++) {
                updateOutputBuffer(entry, rnd);
            }
            inserted.add(entry.copy());
        }
        Assert.assertEquals(NUM_ENTRIES, table.getNumElements());
    }

    private void verifyKeyPresent(K[] keys, AbstractBytesHashMap<K> table) {
        Assert.assertEquals(NUM_ENTRIES, table.getNumElements());
        BinaryRowData present = new BinaryRowData(0);
        present.pointTo(MemorySegmentFactory.wrap(new byte[8]), 0, 8);
        for (int i = 0; i < NUM_ENTRIES; i++) {
            K groupKey = keys[i];
            // look up and retrieve
            BytesMap.LookupInfo<K, BinaryRowData> lookupInfo = table.lookup(groupKey);
            Assert.assertTrue(lookupInfo.isFound());
            Assert.assertNotNull(lookupInfo.getValue());
            Assert.assertEquals(present, lookupInfo.getValue());
        }
    }

    private void verifyKeyInsert(K[] keys, AbstractBytesHashMap<K> table) throws IOException {
        BinaryRowData present = new BinaryRowData(0);
        present.pointTo(MemorySegmentFactory.wrap(new byte[8]), 0, 8);
        for (int i = 0; i < NUM_ENTRIES; i++) {
            K groupKey = keys[i];
            // look up and insert
            BytesMap.LookupInfo<K, BinaryRowData> lookupInfo = table.lookup(groupKey);
            Assert.assertFalse(lookupInfo.isFound());
            BinaryRowData entry = table.append(lookupInfo, defaultValue);
            Assert.assertNotNull(entry);
            Assert.assertEquals(entry, present);
        }
        Assert.assertEquals(NUM_ENTRIES, table.getNumElements());
    }
}
