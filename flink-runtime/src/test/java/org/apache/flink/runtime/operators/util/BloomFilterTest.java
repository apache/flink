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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BloomFilterTest {

    private static BloomFilter bloomFilter;
    private static BloomFilter bloomFilter2;
    private static final int INPUT_SIZE = 1024;
    private static final double FALSE_POSITIVE_PROBABILITY = 0.05;

    @BeforeAll
    static void init() {
        int bitsSize = BloomFilter.optimalNumOfBits(INPUT_SIZE, FALSE_POSITIVE_PROBABILITY);
        bitsSize = bitsSize + (Long.SIZE - (bitsSize % Long.SIZE));
        int byteSize = bitsSize >>> 3;
        MemorySegment memorySegment = MemorySegmentFactory.allocateUnpooledSegment(byteSize);
        bloomFilter = new BloomFilter(INPUT_SIZE, byteSize);
        bloomFilter.setBitsLocation(memorySegment, 0);

        MemorySegment memorySegment2 = MemorySegmentFactory.allocateUnpooledSegment(byteSize);
        bloomFilter2 = new BloomFilter(INPUT_SIZE, byteSize);
        bloomFilter2.setBitsLocation(memorySegment2, 0);
    }

    @Test
    void testBloomFilterArguments1() {
        assertThatThrownBy(() -> new BloomFilter(-1, 128))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testBloomFilterArguments2() {
        assertThatThrownBy(() -> new BloomFilter(0, 128))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testBloomFilterArguments3() {
        assertThatThrownBy(() -> new BloomFilter(1024, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testBloomFilterArguments4() {
        assertThatThrownBy(() -> new BloomFilter(1024, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testBloomNumBits() {
        assertThat(BloomFilter.optimalNumOfBits(0, 0)).isZero();
        assertThat(BloomFilter.optimalNumOfBits(0, 0)).isZero();
        assertThat(BloomFilter.optimalNumOfBits(0, 1)).isZero();
        assertThat(BloomFilter.optimalNumOfBits(1, 1)).isZero();
        assertThat(BloomFilter.optimalNumOfBits(1, 0.03)).isEqualTo(7);
        assertThat(BloomFilter.optimalNumOfBits(10, 0.03)).isEqualTo(72);
        assertThat(BloomFilter.optimalNumOfBits(100, 0.03)).isEqualTo(729);
        assertThat(BloomFilter.optimalNumOfBits(1000, 0.03)).isEqualTo(7298);
        assertThat(BloomFilter.optimalNumOfBits(10000, 0.03)).isEqualTo(72984);
        assertThat(BloomFilter.optimalNumOfBits(100000, 0.03)).isEqualTo(729844);
        assertThat(BloomFilter.optimalNumOfBits(1000000, 0.03)).isEqualTo(7298440);
        assertThat(BloomFilter.optimalNumOfBits(1000000, 0.05)).isEqualTo(6235224);
    }

    @Test
    void testBloomFilterNumHashFunctions() {
        assertThat(BloomFilter.optimalNumOfHashFunctions(-1, -1)).isOne();
        assertThat(BloomFilter.optimalNumOfHashFunctions(0, 0)).isOne();
        assertThat(BloomFilter.optimalNumOfHashFunctions(10, 0)).isOne();
        assertThat(BloomFilter.optimalNumOfHashFunctions(10, 10)).isOne();
        assertThat(BloomFilter.optimalNumOfHashFunctions(10, 100)).isEqualTo(7);
        assertThat(BloomFilter.optimalNumOfHashFunctions(100, 100)).isOne();
        assertThat(BloomFilter.optimalNumOfHashFunctions(1000, 100)).isOne();
        assertThat(BloomFilter.optimalNumOfHashFunctions(10000, 100)).isOne();
        assertThat(BloomFilter.optimalNumOfHashFunctions(100000, 100)).isOne();
        assertThat(BloomFilter.optimalNumOfHashFunctions(1000000, 100)).isOne();
    }

    @Test
    void testBloomFilterFalsePositiveProbability() {
        assertThat(BloomFilter.optimalNumOfBits(1000000, 0.03)).isEqualTo(7298440);
        assertThat(BloomFilter.optimalNumOfBits(1000000, 0.05)).isEqualTo(6235224);
        assertThat(BloomFilter.optimalNumOfBits(1000000, 0.1)).isEqualTo(4792529);
        assertThat(BloomFilter.optimalNumOfBits(1000000, 0.2)).isEqualTo(3349834);
        assertThat(BloomFilter.optimalNumOfBits(1000000, 0.3)).isEqualTo(2505911);
        assertThat(BloomFilter.optimalNumOfBits(1000000, 0.4)).isEqualTo(1907139);

        // Make sure the estimated fpp error is less than 1%.
        assertThat(
                        Math.abs(
                                        BloomFilter.estimateFalsePositiveProbability(
                                                        1000000, 7298440)
                                                - 0.03)
                                < 0.01)
                .isTrue();
        assertThat(
                        Math.abs(
                                        BloomFilter.estimateFalsePositiveProbability(
                                                        1000000, 6235224)
                                                - 0.05)
                                < 0.01)
                .isTrue();
        assertThat(
                        Math.abs(
                                        BloomFilter.estimateFalsePositiveProbability(
                                                        1000000, 4792529)
                                                - 0.1)
                                < 0.01)
                .isTrue();
        assertThat(
                        Math.abs(
                                        BloomFilter.estimateFalsePositiveProbability(
                                                        1000000, 3349834)
                                                - 0.2)
                                < 0.01)
                .isTrue();
        assertThat(
                        Math.abs(
                                        BloomFilter.estimateFalsePositiveProbability(
                                                        1000000, 2505911)
                                                - 0.3)
                                < 0.01)
                .isTrue();
        assertThat(
                        Math.abs(
                                        BloomFilter.estimateFalsePositiveProbability(
                                                        1000000, 1907139)
                                                - 0.4)
                                < 0.01)
                .isTrue();
    }

    @Test
    void testHashcodeInput() {
        bloomFilter.reset();
        int val1 = "val1".hashCode();
        int val2 = "val2".hashCode();
        int val3 = "val3".hashCode();
        int val4 = "val4".hashCode();
        int val5 = "val5".hashCode();

        assertThat(bloomFilter.testHash(val1)).isFalse();
        assertThat(bloomFilter.testHash(val2)).isFalse();
        assertThat(bloomFilter.testHash(val3)).isFalse();
        assertThat(bloomFilter.testHash(val4)).isFalse();
        assertThat(bloomFilter.testHash(val5)).isFalse();
        bloomFilter.addHash(val1);
        assertThat(bloomFilter.testHash(val1)).isTrue();
        assertThat(bloomFilter.testHash(val2)).isFalse();
        assertThat(bloomFilter.testHash(val3)).isFalse();
        assertThat(bloomFilter.testHash(val4)).isFalse();
        assertThat(bloomFilter.testHash(val5)).isFalse();
        bloomFilter.addHash(val2);
        assertThat(bloomFilter.testHash(val1)).isTrue();
        assertThat(bloomFilter.testHash(val2)).isTrue();
        assertThat(bloomFilter.testHash(val3)).isFalse();
        assertThat(bloomFilter.testHash(val4)).isFalse();
        assertThat(bloomFilter.testHash(val5)).isFalse();
        bloomFilter.addHash(val3);
        assertThat(bloomFilter.testHash(val1)).isTrue();
        assertThat(bloomFilter.testHash(val2)).isTrue();
        assertThat(bloomFilter.testHash(val3)).isTrue();
        assertThat(bloomFilter.testHash(val4)).isFalse();
        assertThat(bloomFilter.testHash(val5)).isFalse();
        bloomFilter.addHash(val4);
        assertThat(bloomFilter.testHash(val1)).isTrue();
        assertThat(bloomFilter.testHash(val2)).isTrue();
        assertThat(bloomFilter.testHash(val3)).isTrue();
        assertThat(bloomFilter.testHash(val4)).isTrue();
        assertThat(bloomFilter.testHash(val5)).isFalse();
        bloomFilter.addHash(val5);
        assertThat(bloomFilter.testHash(val1)).isTrue();
        assertThat(bloomFilter.testHash(val2)).isTrue();
        assertThat(bloomFilter.testHash(val3)).isTrue();
        assertThat(bloomFilter.testHash(val4)).isTrue();
        assertThat(bloomFilter.testHash(val5)).isTrue();
    }

    @Test
    void testBloomFilterSerDe() {
        bloomFilter.reset();
        int val1 = "val1".hashCode();
        int val2 = "val2".hashCode();
        int val3 = "val3".hashCode();
        int val4 = "val4".hashCode();
        int val5 = "val5".hashCode();

        assertThat(bloomFilter.testHash(val1)).isFalse();
        assertThat(bloomFilter.testHash(val2)).isFalse();
        assertThat(bloomFilter.testHash(val3)).isFalse();
        assertThat(bloomFilter.testHash(val4)).isFalse();
        assertThat(bloomFilter.testHash(val5)).isFalse();

        // add element
        bloomFilter.addHash(val1);
        bloomFilter.addHash(val2);
        bloomFilter.addHash(val3);
        bloomFilter.addHash(val4);
        bloomFilter.addHash(val5);

        // test exists before merge
        assertThat(bloomFilter.testHash(val1)).isTrue();
        assertThat(bloomFilter.testHash(val2)).isTrue();
        assertThat(bloomFilter.testHash(val3)).isTrue();
        assertThat(bloomFilter.testHash(val4)).isTrue();
        assertThat(bloomFilter.testHash(val5)).isTrue();

        // Serialize bloomFilter
        byte[] serBytes = BloomFilter.toBytes(bloomFilter);
        // Deserialize bloomFilter
        BloomFilter deBloomFilter = BloomFilter.fromBytes(serBytes);

        // test exists after serialization and deserialization
        assertThat(deBloomFilter.testHash(val1)).isTrue();
        assertThat(deBloomFilter.testHash(val2)).isTrue();
        assertThat(deBloomFilter.testHash(val3)).isTrue();
        assertThat(deBloomFilter.testHash(val4)).isTrue();
        assertThat(deBloomFilter.testHash(val5)).isTrue();
    }

    @Test
    void testSerializedBloomFilterMerge() {
        bloomFilter.reset();
        bloomFilter2.reset();

        int val1 = "val1".hashCode();
        int val2 = "val2".hashCode();
        int val3 = "val3".hashCode();
        int val4 = "val4".hashCode();
        int val5 = "val5".hashCode();

        assertThat(bloomFilter.testHash(val1)).isFalse();
        assertThat(bloomFilter.testHash(val2)).isFalse();
        assertThat(bloomFilter.testHash(val3)).isFalse();
        assertThat(bloomFilter.testHash(val4)).isFalse();
        assertThat(bloomFilter.testHash(val5)).isFalse();

        assertThat(bloomFilter2.testHash(val1)).isFalse();
        assertThat(bloomFilter2.testHash(val2)).isFalse();
        assertThat(bloomFilter2.testHash(val3)).isFalse();
        assertThat(bloomFilter2.testHash(val4)).isFalse();
        assertThat(bloomFilter2.testHash(val5)).isFalse();

        // add element
        bloomFilter.addHash(val1);
        bloomFilter.addHash(val2);

        bloomFilter2.addHash(val3);
        bloomFilter2.addHash(val4);
        bloomFilter2.addHash(val5);

        // test exists and not-exists before serialization and merge
        assertThat(bloomFilter.testHash(val1)).isTrue();
        assertThat(bloomFilter.testHash(val2)).isTrue();
        assertThat(bloomFilter.testHash(val3)).isFalse();
        assertThat(bloomFilter.testHash(val4)).isFalse();
        assertThat(bloomFilter.testHash(val5)).isFalse();

        assertThat(bloomFilter2.testHash(val1)).isFalse();
        assertThat(bloomFilter2.testHash(val2)).isFalse();
        assertThat(bloomFilter2.testHash(val3)).isTrue();
        assertThat(bloomFilter2.testHash(val4)).isTrue();
        assertThat(bloomFilter2.testHash(val5)).isTrue();

        // serialize bloomFilter and bloomFilter2
        byte[] bytes = BloomFilter.toBytes(bloomFilter);
        byte[] bytes2 = BloomFilter.toBytes(bloomFilter2);

        // merge serialized bloomFilter2 to bloomFilter directly
        byte[] mergedBytes = BloomFilter.mergeSerializedBloomFilters(bytes, bytes2);
        BloomFilter mergedBloomFilter = BloomFilter.fromBytes(mergedBytes);

        // test all exists in merged bloomFilter
        assertThat(mergedBloomFilter.testHash(val1)).isTrue();
        assertThat(mergedBloomFilter.testHash(val2)).isTrue();
        assertThat(mergedBloomFilter.testHash(val3)).isTrue();
        assertThat(mergedBloomFilter.testHash(val4)).isTrue();
        assertThat(mergedBloomFilter.testHash(val5)).isTrue();
    }
}
