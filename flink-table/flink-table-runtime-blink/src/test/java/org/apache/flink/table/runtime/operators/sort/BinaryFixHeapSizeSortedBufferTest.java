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

package org.apache.flink.table.runtime.operators.sort;

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;
import org.apache.flink.table.runtime.typeutils.AbstractRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.util.LazyMemorySegmentPool;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.runtime.operators.sort.BinarySorterUtils.MockBinaryRowReader;
import static org.apache.flink.table.runtime.operators.sort.BinarySorterUtils.getString;

/** Test for {@link BinaryFixHeapSizeSortedBuffer}. */
public class BinaryFixHeapSizeSortedBufferTest {

    private static final int MEMORY_SIZE = 1024 * 1024 * 32;
    private MemoryManager memoryManager;
    private LazyMemorySegmentPool pool;
    private BinaryRowDataSerializer serializer;
    private BinaryRowData next;

    private final int size = 1_000;
    private final int capacity = 200;

    @Before
    public void beforeTest() {
        this.memoryManager = MemoryManagerBuilder.newBuilder().setMemorySize(MEMORY_SIZE).build();
        int bufferNum = MEMORY_SIZE / memoryManager.getPageSize();
        this.pool =
                new LazyMemorySegmentPool(
                        BinaryFixHeapSizeSortedBuffer.class, memoryManager, bufferNum);
        this.serializer = new BinaryRowDataSerializer(2);
        this.next = serializer.createInstance();
    }

    @After
    public void afterTest() {
        pool.close();
        if (this.memoryManager != null) {
            Assert.assertTrue(
                    "Memory leak: not all segments have been returned to the memory manager.",
                    this.memoryManager.verifyEmpty());
            this.memoryManager.shutdown();
            this.memoryManager = null;
        }
    }

    @Test
    public void testSortInAscendingOrder() throws Exception {
        innerSortTest(
                Collections.singletonList(new MockBinaryRowReader(size)),
                false,
                IntNormalizedKeyComputerFactory.createComputerInAscendingOrder());
    }

    @Test
    public void testSortInDescendingOrder() throws Exception {
        innerSortTest(
                Collections.singletonList(new MockBinaryRowReader(size)),
                true,
                IntNormalizedKeyComputerFactory.createComputerInDescendingOrder());
    }

    @Test
    public void testSortWithRepeatInAscendOrder() throws Exception {
        innerSortTest(
                Arrays.asList(
                        new MockBinaryRowReader(size),
                        new MockBinaryRowReader(size),
                        new MockBinaryRowReader(size)),
                false,
                IntNormalizedKeyComputerFactory.createComputerInAscendingOrder());
    }

    @Test
    public void testSortWithRepeatInDescendingOrder() throws Exception {
        innerSortTest(
                Arrays.asList(
                        new MockBinaryRowReader(size),
                        new MockBinaryRowReader(size),
                        new MockBinaryRowReader(size)),
                true,
                IntNormalizedKeyComputerFactory.createComputerInDescendingOrder());
    }

    @Test
    public void testSortWithoutNormalizedKeyInAscendingOrder() throws Exception {
        innerSortTest(
                Collections.singletonList(new MockBinaryRowReader(size)),
                false,
                IntNormalizedKeyComputerFactory.createComputerWithoutNormalizedKey());
    }

    @Test
    public void testSortWithoutNormalizedKeyInDescendingOrder() throws Exception {
        innerSortTest(
                Collections.singletonList(new MockBinaryRowReader(size)),
                true,
                IntNormalizedKeyComputerFactory.createComputerWithoutNormalizedKey());
    }

    @SuppressWarnings("unchecked")
    private void innerSortTest(
            List<MockBinaryRowReader> readers,
            boolean isDescendingOrder,
            NormalizedKeyComputer computer)
            throws Exception {
        int repeat = readers.size();
        BinaryFixHeapSizeSortedBuffer sortedBuffer =
                BinaryFixHeapSizeSortedBuffer.createExtendedBinaryInMemoryFixedHeapSorter(
                        computer,
                        (AbstractRowDataSerializer) serializer,
                        serializer,
                        new IntRecordComparator(isDescendingOrder),
                        pool,
                        capacity);
        for (MockBinaryRowReader reader : readers) {
            writeIntoBuffer(reader, sortedBuffer);
        }

        MutableObjectIterator<BinaryRowData> iterator = sortedBuffer.getIterator();

        if (isDescendingOrder) {
            for (int i = size - 1; (size - 1 - i) * repeat < capacity; i--) {
                for (int j = 0; j < repeat && (size - 1 - i) * repeat + j < capacity; j++) {
                    next = iterator.next(next);
                    Assert.assertEquals(i, next.getInt(0));
                    Assert.assertEquals(getString(i), next.getString(1).toString());
                }
            }
        } else {
            for (int i = 0; i < capacity / repeat; i++) {
                for (int j = 0; j < repeat && i * repeat + j < capacity; j++) {
                    next = iterator.next(next);
                    Assert.assertEquals(i, next.getInt(0));
                    Assert.assertEquals(getString(i), next.getString(1).toString());
                }
            }
        }
        sortedBuffer.dispose();
    }

    private void writeIntoBuffer(MockBinaryRowReader reader, BinaryFixHeapSizeSortedBuffer buffer)
            throws IOException {
        BinaryRowData record;
        while ((record = reader.next()) != null) {
            buffer.write(record);
        }
    }
}
