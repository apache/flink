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

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
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

import java.util.Iterator;
import java.util.Random;

/** Base test class for {@link BytesMultiMap} and {@link WindowBytesMultiMap}. */
public abstract class BytesMultiMapTestBase<K> extends BytesMapTestBase {
    protected static final int NUM_VALUE_PER_KEY = 50;

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
                new VarCharType(VarCharType.MAX_LENGTH), new IntType(),
            };

    protected final PagedTypeSerializer<K> keySerializer;
    protected final BinaryRowDataSerializer valueSerializer;

    public BytesMultiMapTestBase(PagedTypeSerializer<K> keySerializer) {
        this.keySerializer = keySerializer;
        this.valueSerializer = new BinaryRowDataSerializer(VALUE_TYPES.length);
    }

    /**
     * Creates the specific BytesHashMap, either {@link BytesMultiMap} or {@link
     * WindowBytesMultiMap}.
     */
    public abstract AbstractBytesMultiMap<K> createBytesMultiMap(
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
    public void testBuildAndRetrieve() throws Exception {
        final int numMemSegments =
                needNumMemSegments(
                        NUM_ENTRIES,
                        rowLength(RowType.of(VALUE_TYPES)),
                        rowLength(RowType.of(KEY_TYPES)),
                        PAGE_SIZE);
        int memorySize = numMemSegments * PAGE_SIZE;
        MemoryManager memoryManager =
                MemoryManagerBuilder.newBuilder().setMemorySize(numMemSegments * PAGE_SIZE).build();

        AbstractBytesMultiMap<K> table =
                createBytesMultiMap(memoryManager, memorySize, KEY_TYPES, VALUE_TYPES);

        K[] keys = generateRandomKeys(NUM_ENTRIES / 10);
        BinaryRowData[] values = genValues(NUM_VALUE_PER_KEY);

        for (K key : keys) {
            BytesMap.LookupInfo<K, Iterator<RowData>> lookupInfo;
            for (BinaryRowData value : values) {
                lookupInfo = table.lookup(key);
                table.append(lookupInfo, value);
            }
        }

        KeyValueIterator<K, Iterator<RowData>> iter = table.getEntryIterator();
        while (iter.advanceNext()) {
            int i = 0;
            Iterator<RowData> valueIter = iter.getValue();
            while (valueIter.hasNext()) {
                Assert.assertEquals(valueIter.next(), values[i++]);
            }
        }
    }

    private BinaryRowData[] genValues(int num) {
        BinaryRowData[] values = new BinaryRowData[num];
        final Random rnd = new Random(RANDOM_SEED);
        for (int i = 0; i < num; i++) {
            values[i] = new BinaryRowData(2);
            BinaryRowWriter writer = new BinaryRowWriter(values[i]);
            writer.writeString(0, StringData.fromString("string" + rnd.nextInt()));
            writer.writeInt(1, rnd.nextInt());
            writer.complete();
        }
        return values;
    }
}
