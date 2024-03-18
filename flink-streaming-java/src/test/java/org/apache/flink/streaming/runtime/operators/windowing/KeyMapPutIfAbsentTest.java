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

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KeyMap}. */
class KeyMapPutIfAbsentTest {

    @Test
    void testPutIfAbsentUniqueKeysAndGrowth() {
        KeyMap<Integer, Integer> map = new KeyMap<>();
        IntegerFactory factory = new IntegerFactory();

        final int numElements = 1000000;

        for (int i = 0; i < numElements; i++) {
            factory.set(2 * i + 1);
            map.putIfAbsent(i, factory);

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

        assertThat(map).hasSize(numElements);
        assertThat(map.traverseAndCountElements()).isEqualTo(numElements);
        assertThat(map.getCurrentTableCapacity()).isEqualTo(1 << 21);
        assertThat(map.getLongestChainLength()).isLessThanOrEqualTo(7);
    }

    @Test
    void testPutIfAbsentDuplicateKeysAndGrowth() {
        KeyMap<Integer, Integer> map = new KeyMap<>();
        IntegerFactory factory = new IntegerFactory();

        final int numElements = 1000000;

        for (int i = 0; i < numElements; i++) {
            int val = 2 * i + 1;
            factory.set(val);
            Integer put = map.putIfAbsent(i, factory);
            assertThat(put).isEqualTo(val);
        }

        for (int i = 0; i < numElements; i += 3) {
            factory.set(2 * i);
            Integer put = map.putIfAbsent(i, factory);
            assertThat(put).isEqualTo(2 * i + 1);
        }

        for (int i = 0; i < numElements; i++) {
            assertThat(map.get(i)).isEqualTo(2 * i + 1);
        }

        assertThat(map).hasSize(numElements);
        assertThat(map.traverseAndCountElements()).isEqualTo(numElements);
        assertThat(map.getCurrentTableCapacity()).isEqualTo(1 << 21);
        assertThat(map.getLongestChainLength()).isLessThanOrEqualTo(7);
    }

    // ------------------------------------------------------------------------

    private static class IntegerFactory implements KeyMap.LazyFactory<Integer> {

        private Integer toCreate;

        public void set(Integer toCreate) {
            this.toCreate = toCreate;
        }

        @Override
        public Integer create() {
            return toCreate;
        }
    }
}
