/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.java.tuple.Tuple2;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KeyGroupRangeOffsetTest {

    @Test
    void testKeyGroupIntersection() {
        long[] offsets = new long[9];
        for (int i = 0; i < offsets.length; ++i) {
            offsets[i] = i;
        }

        int startKeyGroup = 2;

        KeyGroupRangeOffsets keyGroupRangeOffsets =
                new KeyGroupRangeOffsets(KeyGroupRange.of(startKeyGroup, 10), offsets);
        KeyGroupRangeOffsets intersection =
                keyGroupRangeOffsets.getIntersection(KeyGroupRange.of(3, 7));
        KeyGroupRangeOffsets expected =
                new KeyGroupRangeOffsets(
                        KeyGroupRange.of(3, 7),
                        Arrays.copyOfRange(offsets, 3 - startKeyGroup, 8 - startKeyGroup));
        assertThat(intersection).isEqualTo(expected);

        assertThat(keyGroupRangeOffsets.getIntersection(keyGroupRangeOffsets.getKeyGroupRange()))
                .isEqualTo(keyGroupRangeOffsets);

        intersection = keyGroupRangeOffsets.getIntersection(KeyGroupRange.of(11, 13));
        assertThat(intersection.getKeyGroupRange()).isEqualTo(KeyGroupRange.EMPTY_KEY_GROUP_RANGE);
        assertThat(intersection.iterator()).isExhausted();

        intersection = keyGroupRangeOffsets.getIntersection(KeyGroupRange.of(5, 13));
        expected =
                new KeyGroupRangeOffsets(
                        KeyGroupRange.of(5, 10),
                        Arrays.copyOfRange(offsets, 5 - startKeyGroup, 11 - startKeyGroup));
        assertThat(intersection).isEqualTo(expected);

        intersection = keyGroupRangeOffsets.getIntersection(KeyGroupRange.of(0, 2));
        expected =
                new KeyGroupRangeOffsets(
                        KeyGroupRange.of(2, 2),
                        Arrays.copyOfRange(offsets, 2 - startKeyGroup, 3 - startKeyGroup));
        assertThat(intersection).isEqualTo(expected);
    }

    @Test
    void testKeyGroupRangeOffsetsBasics() {
        testKeyGroupRangeOffsetsBasicsInternal(0, 0);
        testKeyGroupRangeOffsetsBasicsInternal(0, 1);
        testKeyGroupRangeOffsetsBasicsInternal(1, 2);
        testKeyGroupRangeOffsetsBasicsInternal(42, 42);
        testKeyGroupRangeOffsetsBasicsInternal(3, 7);
        testKeyGroupRangeOffsetsBasicsInternal(0, Short.MAX_VALUE);
        testKeyGroupRangeOffsetsBasicsInternal(Short.MAX_VALUE - 1, Short.MAX_VALUE);

        assertThatThrownBy(() -> testKeyGroupRangeOffsetsBasicsInternal(-3, 2))
                .isInstanceOf(IllegalArgumentException.class);

        KeyGroupRangeOffsets testNoGivenOffsets = new KeyGroupRangeOffsets(3, 7);
        for (int i = 3; i <= 7; ++i) {
            testNoGivenOffsets.setKeyGroupOffset(i, i + 1);
        }
        for (int i = 3; i <= 7; ++i) {
            assertThat(testNoGivenOffsets.getKeyGroupOffset(i)).isEqualTo(i + 1);
        }
    }

    private void testKeyGroupRangeOffsetsBasicsInternal(int startKeyGroup, int endKeyGroup) {

        long[] offsets = new long[endKeyGroup - startKeyGroup + 1];
        for (int i = 0; i < offsets.length; ++i) {
            offsets[i] = i;
        }

        KeyGroupRangeOffsets keyGroupRange =
                new KeyGroupRangeOffsets(startKeyGroup, endKeyGroup, offsets);
        KeyGroupRangeOffsets sameButDifferentConstr =
                new KeyGroupRangeOffsets(KeyGroupRange.of(startKeyGroup, endKeyGroup), offsets);
        assertThat(sameButDifferentConstr).isEqualTo(keyGroupRange);

        int numberOfKeyGroup = keyGroupRange.getKeyGroupRange().getNumberOfKeyGroups();
        assertThat(numberOfKeyGroup).isEqualTo(Math.max(0, endKeyGroup - startKeyGroup + 1));
        if (numberOfKeyGroup > 0) {
            assertThat(keyGroupRange.getKeyGroupRange().getStartKeyGroup())
                    .isEqualTo(startKeyGroup);
            assertThat(keyGroupRange.getKeyGroupRange().getEndKeyGroup()).isEqualTo(endKeyGroup);
            int c = startKeyGroup;
            for (Tuple2<Integer, Long> tuple : keyGroupRange) {
                assertThat(tuple.f0).isEqualTo(c);
                assertThat(keyGroupRange.getKeyGroupRange()).contains(tuple.f0);
                assertThat(tuple.f1).isEqualTo((long) c - startKeyGroup);
                ++c;
            }

            for (int i = startKeyGroup; i <= endKeyGroup; ++i) {
                assertThat(keyGroupRange.getKeyGroupOffset(i)).isEqualTo(i - startKeyGroup);
            }

            int newOffset = 42;
            for (int i = startKeyGroup; i <= endKeyGroup; ++i) {
                keyGroupRange.setKeyGroupOffset(i, newOffset);
                ++newOffset;
            }

            for (int i = startKeyGroup; i <= endKeyGroup; ++i) {
                assertThat(keyGroupRange.getKeyGroupOffset(i)).isEqualTo(42 + i - startKeyGroup);
            }

            assertThat(c).isEqualTo(endKeyGroup + 1);
            assertThat(keyGroupRange.getKeyGroupRange()).doesNotContain(startKeyGroup - 1);
            assertThat(keyGroupRange.getKeyGroupRange()).doesNotContain(endKeyGroup + 1);
        } else {
            assertThat(keyGroupRange).isEqualTo(KeyGroupRange.EMPTY_KEY_GROUP_RANGE);
        }
    }
}
