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
package org.apache.flink.runtime.operators.util;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class BitSetTest {

    private BitSet bitSet;
    int byteSize;
    MemorySegment memorySegment;

    public BitSetTest(int byteSize) {
        this.byteSize = byteSize;
        memorySegment = MemorySegmentFactory.allocateUnpooledSegment(byteSize);
    }

    @Before
    public void init() {
        bitSet = new BitSet(byteSize);
        bitSet.setMemorySegment(memorySegment, 0);
        bitSet.clear();
    }

    @Test(expected = IllegalArgumentException.class)
    public void verifyBitSetSize1() {
        bitSet.setMemorySegment(memorySegment, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void verifyBitSetSize2() {
        bitSet.setMemorySegment(null, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void verifyBitSetSize3() {
        bitSet.setMemorySegment(memorySegment, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void verifyInputIndex1() {
        bitSet.set(8 * byteSize + 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void verifyInputIndex2() {
        bitSet.set(-1);
    }

    @Test
    public void testSetValues() {
        int bitSize = bitSet.bitSize();
        assertEquals(bitSize, 8 * byteSize);
        for (int i = 0; i < bitSize; i++) {
            assertFalse(bitSet.get(i));
            if (i % 2 == 0) {
                bitSet.set(i);
            }
        }

        for (int i = 0; i < bitSize; i++) {
            if (i % 2 == 0) {
                assertTrue(bitSet.get(i));
            } else {
                assertFalse(bitSet.get(i));
            }
        }
    }

    @Parameterized.Parameters(name = "byte size = {0}")
    public static Object[] getByteSize() {
        return new Integer[] {1000, 1024, 2019};
    }
}
