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

package org.apache.flink.runtime.operators.sort;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.base.IntComparator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.operators.testutils.TestData;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MergeIteratorTest {

    private TypeComparator<Tuple2<Integer, String>> comparator;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setup() {
        this.comparator = TestData.getIntStringTupleComparator();
    }

    private MutableObjectIterator<Tuple2<Integer, String>> newIterator(
            final int[] keys, final String[] values) {

        return new MutableObjectIterator<Tuple2<Integer, String>>() {

            private int key = 0;
            private String value = new String();

            private int current = 0;

            @Override
            public Tuple2<Integer, String> next(Tuple2<Integer, String> reuse) {
                if (current < keys.length) {
                    key = keys[current];
                    value = values[current];
                    current++;
                    reuse.setField(key, 0);
                    reuse.setField(value, 1);
                    return reuse;
                } else {
                    return null;
                }
            }

            @Override
            public Tuple2<Integer, String> next() {
                if (current < keys.length) {
                    Tuple2<Integer, String> result = new Tuple2<>(keys[current], values[current]);
                    current++;
                    return result;
                } else {
                    return null;
                }
            }
        };
    }

    @Test
    void testMergeOfTwoStreams() throws Exception {
        // iterators
        List<MutableObjectIterator<Tuple2<Integer, String>>> iterators = new ArrayList<>();
        iterators.add(
                newIterator(new int[] {1, 2, 4, 5, 10}, new String[] {"1", "2", "4", "5", "10"}));
        iterators.add(
                newIterator(new int[] {3, 6, 7, 10, 12}, new String[] {"3", "6", "7", "10", "12"}));

        final int[] expected = new int[] {1, 2, 3, 4, 5, 6, 7, 10, 10, 12};

        // comparator
        TypeComparator<Integer> comparator = new IntComparator(true);

        // merge iterator
        MutableObjectIterator<Tuple2<Integer, String>> iterator =
                new MergeIterator<>(iterators, this.comparator);

        // check expected order
        Tuple2<Integer, String> rec1 = new Tuple2<>();
        Tuple2<Integer, String> rec2 = new Tuple2<>();
        int k1 = 0;
        int k2 = 0;

        int pos = 1;

        assertThat(rec1 = iterator.next(rec1)).isNotNull();
        assertThat(rec1.f0).isEqualTo(expected[0]);

        while ((rec2 = iterator.next(rec2)) != null) {
            k1 = rec1.f0;
            k2 = rec2.f0;

            assertThat(comparator.compare(k1, k2)).isLessThanOrEqualTo(0);
            assertThat(k2).isEqualTo(expected[pos++]);

            Tuple2<Integer, String> tmp = rec1;
            rec1 = rec2;
            rec2 = tmp;
        }
    }

    @Test
    void testMergeOfTenStreams() throws Exception {
        // iterators
        List<MutableObjectIterator<Tuple2<Integer, String>>> iterators = new ArrayList<>();
        iterators.add(
                newIterator(new int[] {1, 2, 17, 23, 23}, new String[] {"A", "B", "C", "D", "E"}));
        iterators.add(
                newIterator(new int[] {2, 6, 7, 8, 9}, new String[] {"A", "B", "C", "D", "E"}));
        iterators.add(
                newIterator(new int[] {4, 10, 11, 11, 12}, new String[] {"A", "B", "C", "D", "E"}));
        iterators.add(
                newIterator(new int[] {3, 6, 7, 10, 12}, new String[] {"A", "B", "C", "D", "E"}));
        iterators.add(
                newIterator(new int[] {7, 10, 15, 19, 44}, new String[] {"A", "B", "C", "D", "E"}));
        iterators.add(
                newIterator(new int[] {6, 6, 11, 17, 18}, new String[] {"A", "B", "C", "D", "E"}));
        iterators.add(
                newIterator(new int[] {1, 2, 4, 5, 10}, new String[] {"A", "B", "C", "D", "E"}));
        iterators.add(
                newIterator(new int[] {5, 10, 19, 23, 29}, new String[] {"A", "B", "C", "D", "E"}));
        iterators.add(
                newIterator(new int[] {9, 9, 9, 9, 9}, new String[] {"A", "B", "C", "D", "E"}));
        iterators.add(
                newIterator(new int[] {8, 8, 14, 14, 15}, new String[] {"A", "B", "C", "D", "E"}));

        // comparator
        TypeComparator<Integer> comparator = new IntComparator(true);

        // merge iterator
        MutableObjectIterator<Tuple2<Integer, String>> iterator =
                new MergeIterator<>(iterators, this.comparator);

        int elementsFound = 1;
        // check expected order
        Tuple2<Integer, String> rec1 = new Tuple2<>();
        Tuple2<Integer, String> rec2 = new Tuple2<>();

        assertThat(rec1 = iterator.next(rec1)).isNotNull();
        while ((rec2 = iterator.next(rec2)) != null) {
            elementsFound++;

            assertThat(comparator.compare(rec1.f0, rec2.f0)).isLessThanOrEqualTo(0);

            Tuple2<Integer, String> tmp = rec1;
            rec1 = rec2;
            rec2 = tmp;
        }

        assertThat(elementsFound)
                .withFailMessage("Too few elements returned from stream.")
                .isEqualTo(50);
    }

    @Test
    void testInvalidMerge() throws Exception {
        // iterators
        List<MutableObjectIterator<Tuple2<Integer, String>>> iterators = new ArrayList<>();
        iterators.add(
                newIterator(new int[] {1, 2, 17, 23, 23}, new String[] {"A", "B", "C", "D", "E"}));
        iterators.add(
                newIterator(new int[] {2, 6, 7, 8, 9}, new String[] {"A", "B", "C", "D", "E"}));
        iterators.add(
                newIterator(new int[] {4, 10, 11, 11, 12}, new String[] {"A", "B", "C", "D", "E"}));
        iterators.add(
                newIterator(new int[] {3, 6, 10, 7, 12}, new String[] {"A", "B", "C", "D", "E"}));
        iterators.add(
                newIterator(new int[] {7, 10, 15, 19, 44}, new String[] {"A", "B", "C", "D", "E"}));
        iterators.add(
                newIterator(new int[] {6, 6, 11, 17, 18}, new String[] {"A", "B", "C", "D", "E"}));
        iterators.add(
                newIterator(new int[] {1, 2, 4, 5, 10}, new String[] {"A", "B", "C", "D", "E"}));
        iterators.add(
                newIterator(new int[] {5, 10, 19, 23, 29}, new String[] {"A", "B", "C", "D", "E"}));
        iterators.add(
                newIterator(new int[] {9, 9, 9, 9, 9}, new String[] {"A", "B", "C", "D", "E"}));
        iterators.add(
                newIterator(new int[] {8, 8, 14, 14, 15}, new String[] {"A", "B", "C", "D", "E"}));

        // comparator
        TypeComparator<Integer> comparator = new IntComparator(true);

        // merge iterator
        MutableObjectIterator<Tuple2<Integer, String>> iterator =
                new MergeIterator<>(iterators, this.comparator);

        boolean violationFound = false;

        // check expected order
        Tuple2<Integer, String> rec1 = new Tuple2<>();
        Tuple2<Integer, String> rec2 = new Tuple2<>();

        assertThat(rec1 = iterator.next(rec1)).isNotNull();
        while ((rec2 = iterator.next(rec2)) != null) {
            if (comparator.compare(rec1.f0, rec2.f0) > 0) {
                violationFound = true;
                break;
            }

            Tuple2<Integer, String> tmp = rec1;
            rec1 = rec2;
            rec2 = tmp;
        }

        assertThat(violationFound)
                .withFailMessage("Merge must have returned a wrong result")
                .isTrue();
    }
}
