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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KeyGroupRangeTest {

    @Test
    void testKeyGroupIntersection() {
        KeyGroupRange keyGroupRange1 = KeyGroupRange.of(0, 10);
        KeyGroupRange keyGroupRange2 = KeyGroupRange.of(3, 7);
        KeyGroupRange intersection = keyGroupRange1.getIntersection(keyGroupRange2);
        assertThat(intersection.getStartKeyGroup()).isEqualTo(3);
        assertThat(intersection.getEndKeyGroup()).isEqualTo(7);
        assertThat(keyGroupRange2.getIntersection(keyGroupRange1)).isEqualTo(intersection);

        assertThat(keyGroupRange1.getIntersection(keyGroupRange1)).isEqualTo(keyGroupRange1);

        keyGroupRange1 = KeyGroupRange.of(0, 5);
        keyGroupRange2 = KeyGroupRange.of(6, 10);
        intersection = keyGroupRange1.getIntersection(keyGroupRange2);
        assertThat(intersection).isEqualTo(KeyGroupRange.EMPTY_KEY_GROUP_RANGE);
        assertThat(intersection).isEqualTo(keyGroupRange2.getIntersection(keyGroupRange1));

        keyGroupRange1 = KeyGroupRange.of(0, 10);
        keyGroupRange2 = KeyGroupRange.of(5, 20);
        intersection = keyGroupRange1.getIntersection(keyGroupRange2);
        assertThat(intersection.getStartKeyGroup()).isEqualTo(5);
        assertThat(intersection.getEndKeyGroup()).isEqualTo(10);
        assertThat(keyGroupRange2.getIntersection(keyGroupRange1)).isEqualTo(intersection);

        keyGroupRange1 = KeyGroupRange.of(3, 12);
        keyGroupRange2 = KeyGroupRange.of(0, 10);
        intersection = keyGroupRange1.getIntersection(keyGroupRange2);
        assertThat(intersection.getStartKeyGroup()).isEqualTo(3);
        assertThat(intersection.getEndKeyGroup()).isEqualTo(10);
        assertThat(keyGroupRange2.getIntersection(keyGroupRange1)).isEqualTo(intersection);
    }

    @Test
    void testKeyGroupRangeBasics() {
        testKeyGroupRangeBasicsInternal(0, 0);
        testKeyGroupRangeBasicsInternal(0, 1);
        testKeyGroupRangeBasicsInternal(1, 2);
        testKeyGroupRangeBasicsInternal(42, 42);
        testKeyGroupRangeBasicsInternal(3, 7);
        testKeyGroupRangeBasicsInternal(0, Short.MAX_VALUE);
        testKeyGroupRangeBasicsInternal(Short.MAX_VALUE - 1, Short.MAX_VALUE);

        assertThatThrownBy(() -> testKeyGroupRangeBasicsInternal(-3, 2))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private void testKeyGroupRangeBasicsInternal(int startKeyGroup, int endKeyGroup) {
        KeyGroupRange keyGroupRange = KeyGroupRange.of(startKeyGroup, endKeyGroup);
        int numberOfKeyGroup = keyGroupRange.getNumberOfKeyGroups();
        assertThat(numberOfKeyGroup).isEqualTo(Math.max(0, endKeyGroup - startKeyGroup + 1));
        if (keyGroupRange.getNumberOfKeyGroups() > 0) {
            assertThat(keyGroupRange.getStartKeyGroup()).isEqualTo(startKeyGroup);
            assertThat(keyGroupRange.getEndKeyGroup()).isEqualTo(endKeyGroup);
            int c = startKeyGroup;
            for (int i : keyGroupRange) {
                assertThat(i).isEqualTo(c);
                assertThat(keyGroupRange.contains(i)).isTrue();
                ++c;
            }

            assertThat(c).isEqualTo(endKeyGroup + 1);
            assertThat(keyGroupRange.contains(startKeyGroup - 1)).isFalse();
            assertThat(keyGroupRange.contains(endKeyGroup + 1)).isFalse();
        } else {
            assertThat(keyGroupRange).isEqualTo(KeyGroupRange.EMPTY_KEY_GROUP_RANGE);
        }
    }
}
