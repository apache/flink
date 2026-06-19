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
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(ParameterizedTestExtension.class)
class BitSetTest {

    private BitSet bitSet;
    int byteSize;
    MemorySegment memorySegment;

    BitSetTest(int byteSize) {
        this.byteSize = byteSize;
        memorySegment = MemorySegmentFactory.allocateUnpooledSegment(byteSize);
    }

    @BeforeEach
    void init() {
        bitSet = new BitSet(byteSize);
        bitSet.setMemorySegment(memorySegment, 0);
        bitSet.clear();
    }

    @TestTemplate
    void verifyBitSetSize1() {
        assertThatThrownBy(() -> bitSet.setMemorySegment(memorySegment, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @TestTemplate
    void verifyBitSetSize2() {
        assertThatThrownBy(() -> bitSet.setMemorySegment(null, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @TestTemplate
    void verifyBitSetSize3() {
        assertThatThrownBy(() -> bitSet.setMemorySegment(memorySegment, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @TestTemplate
    void verifyInputIndex1() {
        assertThatThrownBy(() -> bitSet.set(8 * byteSize))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @TestTemplate
    void verifyInputIndex2() {
        assertThatThrownBy(() -> bitSet.set(-1)).isInstanceOf(IllegalArgumentException.class);
    }

    @TestTemplate
    void testSetValues() {
        int bitSize = bitSet.bitSize();
        assertThat(bitSize).isEqualTo(8 * byteSize);
        for (int i = 0; i < bitSize; i++) {
            assertThat(bitSet.get(i)).isFalse();
            if (i % 2 == 0) {
                bitSet.set(i);
            }
        }

        for (int i = 0; i < bitSize; i++) {
            if (i % 2 == 0) {
                assertThat(bitSet.get(i)).isTrue();
            } else {
                assertThat(bitSet.get(i)).isFalse();
            }
        }
    }

    @Parameters(name = "byte size = {0}")
    private static List<Integer> getByteSize() {
        return Arrays.asList(1000, 1024, 2019);
    }
}
