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

/** Tests for {@link BooleanArrayList}. */
class BooleanArrayListTest {

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void testAddGrowsAndPreservesOrder(int initialCapacity) {
        final BooleanArrayList list = new BooleanArrayList(initialCapacity);
        list.add(true);
        list.add(false);
        list.add(false);
        list.add(true);
        list.add(true);

        assertThat(list.size()).isEqualTo(5);
        assertThat(list.toArray()).containsExactly(true, false, false, true, true);
    }

    @Test
    void testToArrayReturnsCopyOfSizeElements() {
        final BooleanArrayList list = new BooleanArrayList(10);
        list.add(true);
        list.add(false);

        final boolean[] array = list.toArray();
        assertThat(array).containsExactly(true, false);

        array[0] = false;
        assertThat(list.toArray()).containsExactly(true, false);
    }

    @Test
    void testClearAllowsReuse() {
        final BooleanArrayList list = new BooleanArrayList(2);
        assertThat(list.isEmpty()).isTrue();

        list.add(true);
        list.add(true);
        list.add(true);
        assertThat(list.isEmpty()).isFalse();

        list.clear();
        assertThat(list.isEmpty()).isTrue();
        assertThat(list.size()).isZero();
        assertThat(list.toArray()).isEmpty();

        list.add(false);
        assertThat(list.toArray()).containsExactly(false);
    }
}
