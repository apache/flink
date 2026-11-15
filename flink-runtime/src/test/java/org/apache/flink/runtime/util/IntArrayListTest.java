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

import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link IntArrayList}. */
class IntArrayListTest {

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void testAddGrowsAndPreservesOrder(int initialCapacity) {
        final IntArrayList list = new IntArrayList(initialCapacity);
        for (int i = 0; i < 10; i++) {
            list.add(i);
        }

        assertThat(list.size()).isEqualTo(10);
        assertThat(list.toArray()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @Test
    void testToArrayReturnsCopyOfSizeElements() {
        final IntArrayList list = new IntArrayList(10);
        list.add(1);
        list.add(2);

        final int[] array = list.toArray();
        assertThat(array).containsExactly(1, 2);

        array[0] = 42;
        assertThat(list.toArray()).containsExactly(1, 2);
    }

    @Test
    void testRemoveLastReturnsElementsInReverseOrder() {
        final IntArrayList list = new IntArrayList(1);
        list.add(1);
        list.add(2);
        list.add(3);

        assertThat(list.removeLast()).isEqualTo(3);
        assertThat(list.removeLast()).isEqualTo(2);
        assertThat(list.toArray()).containsExactly(1);
        assertThat(list.removeLast()).isEqualTo(1);
        assertThat(list.isEmpty()).isTrue();
    }

    @Test
    void testRemoveLastOnEmptyListThrows() {
        final IntArrayList list = new IntArrayList(4);

        assertThatThrownBy(list::removeLast).isInstanceOf(NoSuchElementException.class);

        list.add(1);
        list.removeLast();
        assertThatThrownBy(list::removeLast).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void testClearAllowsReuse() {
        final IntArrayList list = new IntArrayList(2);
        list.add(1);
        list.add(2);
        list.add(3);

        list.clear();
        assertThat(list.isEmpty()).isTrue();
        assertThat(list.size()).isZero();
        assertThat(list.toArray()).isEmpty();

        list.add(4);
        assertThat(list.isEmpty()).isFalse();
        assertThat(list.toArray()).containsExactly(4);
    }

    @Test
    void testEmptyListIsImmutable() {
        assertThat(IntArrayList.EMPTY.isEmpty()).isTrue();
        assertThat(IntArrayList.EMPTY.toArray()).isEmpty();

        assertThatThrownBy(() -> IntArrayList.EMPTY.add(1))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(IntArrayList.EMPTY::removeLast)
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(IntArrayList.EMPTY.size()).isZero();
    }
}
