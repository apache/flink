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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link LongArrayList}. */
class LongArrayListTest {

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void testAddGrowsAndPreservesOrder(int initialCapacity) {
        final LongArrayList list = new LongArrayList(initialCapacity);
        for (long i = 0; i < 10; i++) {
            list.add(i);
        }

        assertThat(list.size()).isEqualTo(10);
        assertThat(list.toArray()).containsExactly(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
    }

    @Test
    void testToArrayReturnsCopyOfSizeElements() {
        final LongArrayList list = new LongArrayList(10);
        list.add(1L);
        list.add(2L);

        final long[] array = list.toArray();
        assertThat(array).containsExactly(1L, 2L);

        array[0] = 42L;
        assertThat(list.toArray()).containsExactly(1L, 2L);
    }

    @Test
    void testRemoveLongFromMiddleShiftsFollowingElements() {
        final LongArrayList list = listOf(10L, 20L, 30L, 40L);

        assertThat(list.removeLong(1)).isEqualTo(20L);
        assertThat(list.size()).isEqualTo(3);
        assertThat(list.toArray()).containsExactly(10L, 30L, 40L);

        assertThat(list.removeLong(0)).isEqualTo(10L);
        assertThat(list.toArray()).containsExactly(30L, 40L);
    }

    @Test
    void testRemoveLongLastElement() {
        final LongArrayList list = listOf(10L, 20L, 30L);

        assertThat(list.removeLong(2)).isEqualTo(30L);
        assertThat(list.toArray()).containsExactly(10L, 20L);
    }

    @Test
    void testRemoveLongAtSizeThrows() {
        final LongArrayList list = listOf(10L, 20L);

        assertThatThrownBy(() -> list.removeLong(2)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThat(list.toArray()).containsExactly(10L, 20L);
    }

    @Test
    void testClearAllowsReuse() {
        final LongArrayList list = listOf(1L, 2L, 3L);

        list.clear();
        assertThat(list.isEmpty()).isTrue();
        assertThat(list.size()).isZero();
        assertThat(list.toArray()).isEmpty();
        assertThatThrownBy(() -> list.removeLong(0)).isInstanceOf(IndexOutOfBoundsException.class);

        list.add(4L);
        assertThat(list.isEmpty()).isFalse();
        assertThat(list.toArray()).containsExactly(4L);
    }

    @Test
    void testGetInternalArrayIsBackingArray() {
        final LongArrayList list = new LongArrayList(4);
        list.add(1L);
        list.add(2L);

        final long[] internal = list.getInternalArray();
        assertThat(internal).hasSize(4).startsWith(1L, 2L);

        internal[0] = 42L;
        assertThat(list.toArray()).containsExactly(42L, 2L);

        list.removeLong(0);
        assertThat(list.getInternalArray()).isSameAs(internal).startsWith(2L);
    }

    private static LongArrayList listOf(long... values) {
        final LongArrayList list = new LongArrayList(values.length);
        for (long value : values) {
            list.add(value);
        }
        return list;
    }
}
