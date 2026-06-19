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

package org.apache.flink.util;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link UnionIterator}. */
class UnionIteratorTest {

    @Test
    void testUnion() {
        UnionIterator<Integer> iter = new UnionIterator<>();

        // should succeed and be empty
        assertThat(iter.iterator()).isExhausted();

        iter.clear();

        assertThatThrownBy(() -> iter.iterator().next()).isInstanceOf(NoSuchElementException.class);

        iter.clear();
        iter.addList(Arrays.asList(1, 2, 3, 4, 5, 6, 7));
        iter.addList(Collections.emptyList());
        iter.addList(Arrays.asList(8, 9, 10, 11));

        int val = 1;
        for (int i : iter) {
            assertThat(i).isEqualTo(val++);
        }
    }

    @Test
    void testTraversableOnce() {
        UnionIterator<Integer> iter = new UnionIterator<>();

        // should succeed
        iter.iterator();

        // should fail
        assertThatThrownBy(iter::iterator).isInstanceOf(TraversableOnceException.class);

        // should fail again
        assertThatThrownBy(iter::iterator).isInstanceOf(TraversableOnceException.class);

        // reset the thing, keep it empty
        iter.clear();

        // should succeed
        iter.iterator();

        // should fail
        assertThatThrownBy(iter::iterator).isInstanceOf(TraversableOnceException.class);

        // should fail again
        assertThatThrownBy(iter::iterator).isInstanceOf(TraversableOnceException.class);

        // reset the thing, add some data
        iter.clear();
        iter.addList(Arrays.asList(1, 2, 3, 4, 5, 6, 7));

        // should succeed
        Iterator<Integer> ints = iter.iterator();
        assertThat(ints.next()).isNotNull();
        assertThat(ints.next()).isNotNull();
        assertThat(ints.next()).isNotNull();

        // should fail if called in the middle of operations
        assertThatThrownBy(iter::iterator).isInstanceOf(TraversableOnceException.class);

        // reset the thing, keep it empty
        iter.clear();

        // should succeed again
        assertThat(iter.iterator()).isExhausted();
    }
}
