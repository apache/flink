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

package org.apache.flink.streaming.runtime.operators.windowing;

import org.junit.jupiter.api.Test;

import java.util.BitSet;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KeyMap}. */
class KeyMapPutTest {

    @Test
    void testPutUniqueKeysAndGrowth() {
        KeyMap<Integer, Integer> map = new KeyMap<>();

        final int numElements = 1000000;

        for (int i = 0; i < numElements; i++) {
            map.put(i, 2 * i + 1);

            assertThat(map.size()).isEqualTo(i + 1).isLessThanOrEqualTo(map.getRehashThreshold());
            assertThat(map.getCurrentTableCapacity())
                    .isGreaterThan(map.size())
                    .isGreaterThan(map.getRehashThreshold());
        }

        assertThat(map).hasSize(numElements);
        assertThat(map.traverseAndCountElements()).isEqualTo(numElements);
        assertThat(map.getCurrentTableCapacity()).isEqualTo(1 << 21);

        for (int i = 0; i < numElements; i++) {
            assertThat(map.get(i)).isEqualTo(2 * i + 1);
        }

        for (int i = numElements - 1; i >= 0; i--) {
            assertThat(map.get(i)).isEqualTo(2 * i + 1);
        }

        BitSet bitset = new BitSet();
        int numContained = 0;
        for (KeyMap.Entry<Integer, Integer> entry : map) {
            numContained++;

            assertThat(entry.getValue()).isEqualTo(entry.getKey() * 2 + 1);
            assertThat(bitset.get(entry.getKey())).isFalse();
            bitset.set(entry.getKey());
        }

        assertThat(numContained).isEqualTo(numElements);
        assertThat(bitset.cardinality()).isEqualTo(numElements);

        assertThat(map).hasSize(numElements);
        assertThat(map.traverseAndCountElements()).isEqualTo(numElements);
        assertThat(map.getCurrentTableCapacity()).isEqualTo(1 << 21);
        assertThat(map.getLongestChainLength()).isLessThanOrEqualTo(7);
    }

    @Test
    void testPutDuplicateKeysAndGrowth() {
        final KeyMap<Integer, Integer> map = new KeyMap<>();
        final int numElements = 1000000;

        for (int i = 0; i < numElements; i++) {
            Integer put = map.put(i, 2 * i + 1);
            assertThat(put).isNull();
        }

        for (int i = 0; i < numElements; i += 3) {
            Integer put = map.put(i, 2 * i);
            assertThat(put).isNotNull().isEqualTo(2 * i + 1);
        }

        for (int i = 0; i < numElements; i++) {
            int expected = (i % 3 == 0) ? (2 * i) : (2 * i + 1);
            assertThat(map.get(i)).isEqualTo(expected);
        }

        assertThat(map).hasSize(numElements);
        assertThat(map.traverseAndCountElements()).isEqualTo(numElements);
        assertThat(map.getCurrentTableCapacity()).isEqualTo(1 << 21);
        assertThat(map.getLongestChainLength()).isLessThanOrEqualTo(7);

        BitSet bitset = new BitSet();
        int numContained = 0;
        for (KeyMap.Entry<Integer, Integer> entry : map) {
            numContained++;

            int key = entry.getKey();
            int expected = key % 3 == 0 ? (2 * key) : (2 * key + 1);

            assertThat(entry.getValue()).isEqualTo(expected);
            assertThat(bitset.get(key)).isFalse();
            bitset.set(key);
        }

        assertThat(numContained).isEqualTo(numElements);
        assertThat(bitset.cardinality()).isEqualTo(numElements);
    }
}
