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

package org.apache.flink.runtime.util;

import org.apache.flink.api.java.tuple.Tuple2;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link DualKeyLinkedMap}. */
class DualKeyLinkedMapTest {

    @Test
    void testKeySets() {
        final Random random = new Random();
        final Set<Tuple2<Integer, Integer>> keys = new HashSet<>();

        for (int i = 0; i < 10; i++) {
            int keyA = random.nextInt();
            int keyB = random.nextInt();
            keys.add(Tuple2.of(keyA, keyB));
        }

        final DualKeyLinkedMap<Integer, Integer, String> dualKeyMap = new DualKeyLinkedMap<>();

        for (Tuple2<Integer, Integer> key : keys) {
            dualKeyMap.put(key.f0, key.f1, "foobar");
        }

        assertThat(dualKeyMap.keySetA())
                .isEqualTo(keys.stream().map(t -> t.f0).collect(Collectors.toSet()));
        assertThat(dualKeyMap.keySetB())
                .isEqualTo(keys.stream().map(t -> t.f1).collect(Collectors.toSet()));
    }

    @Test
    void ensuresOneToOneMappingBetweenKeysSamePrimaryKey() {
        final DualKeyLinkedMap<Integer, Integer, String> map = new DualKeyLinkedMap<>();

        final String secondValue = "barfoo";
        map.put(1, 1, "foobar");
        map.put(1, 2, secondValue);

        assertThat(map.getValueByKeyB(1)).isNull();
        assertThat(map.getValueByKeyA(1)).isEqualTo(secondValue);
        assertThat(map.getValueByKeyB(2)).isEqualTo(secondValue);
    }

    @Test
    void ensuresOneToOneMappingBetweenKeysSameSecondaryKey() {
        final DualKeyLinkedMap<Integer, Integer, String> map = new DualKeyLinkedMap<>();

        final String secondValue = "barfoo";
        map.put(1, 1, "foobar");
        map.put(2, 1, secondValue);

        assertThat(map.getValueByKeyA(1)).isNull();
        assertThat(map.getValueByKeyB(1)).isEqualTo(secondValue);
        assertThat(map.getValueByKeyA(2)).isEqualTo(secondValue);
    }

    @Test
    void testPrimaryKeyOrderIsNotAffectedIfReInsertedWithSameSecondaryKey() {
        final DualKeyLinkedMap<Integer, Integer, String> map = new DualKeyLinkedMap<>();

        final String value1 = "1";
        map.put(1, 1, value1);
        final String value2 = "2";
        map.put(2, 2, value2);

        final String value3 = "3";
        map.put(1, 1, value3);
        assertThat(map.keySetA().iterator().next()).isOne();
        assertThat(map.values().iterator().next()).isEqualTo(value3);
    }

    @Test
    void testPrimaryKeyOrderIsNotAffectedIfReInsertedWithDifferentSecondaryKey() {
        final DualKeyLinkedMap<Integer, Integer, String> map = new DualKeyLinkedMap<>();

        final String value1 = "1";
        map.put(1, 1, value1);
        final String value2 = "2";
        map.put(2, 2, value2);

        final String value3 = "3";
        map.put(1, 3, value3);
        assertThat(map.keySetA().iterator().next()).isOne();
        assertThat(map.values().iterator().next()).isEqualTo(value3);
    }
}
