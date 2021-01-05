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

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
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

import java.util.Iterator;
import java.util.Random;

/** Verify the correctness of {@link BytesMultiMap}. */
public class BytesMultiMapTest extends BytesMapTestBase {
    protected static final int NUM_VALUE_PER_KEY = 50;

    protected final LogicalType[] keyTypes;
    protected final LogicalType[] valueTypes;
    protected final BinaryRowDataSerializer keySerializer;
    protected final BinaryRowDataSerializer valueSerializer;

    public BytesMultiMapTest() {
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
                    new VarCharType(VarCharType.MAX_LENGTH), new IntType(),
                };

        this.keySerializer = new BinaryRowDataSerializer(keyTypes.length);
        this.valueSerializer = new BinaryRowDataSerializer(valueTypes.length);
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
                MemoryManagerBuilder.newBuilder().setMemorySize(numMemSegments * PAGE_SIZE).build();

        BytesMultiMap table =
                new BytesMultiMap(this, memoryManager, memorySize, keyTypes, valueTypes);

        final Random rnd = new Random(RANDOM_SEED);
        BinaryRowData[] keys = getRandomizedInput(NUM_ENTRIES / 10, rnd, true);
        BinaryRowData[] values = genValues(NUM_VALUE_PER_KEY);

        for (BinaryRowData key : keys) {
            BytesMultiMap.LookupInfo<Iterator<RowData>> info;
            for (BinaryRowData value : values) {
                info = table.lookup(key);
                table.append(info, value);
            }
        }

        KeyValueIterator<RowData, Iterator<RowData>> iter = table.getEntryIterator();
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
