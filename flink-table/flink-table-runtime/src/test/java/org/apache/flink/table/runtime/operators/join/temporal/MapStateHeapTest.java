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

package org.apache.flink.table.runtime.operators.join.temporal;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link MapStateHeap}. */
class MapStateHeapTest {

    @Test
    void topAndPopOnEmptyHeap() throws Exception {
        MapStateHeap<Integer> heap = newHeap();

        assertThat(heap.isEmpty()).isTrue();
        assertThat(heap.size()).isZero();
        assertThat(heap.top()).isNull();
        assertThatThrownBy(heap::pop).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void singleElementRoundTrip() throws Exception {
        MapStateHeap<Integer> heap = newHeap();

        heap.add(42);

        assertThat(heap.size()).isEqualTo(1);
        assertThat(heap.top()).isEqualTo(42);
        assertThat(heap.pop()).isEqualTo(42);
        assertThat(heap.isEmpty()).isTrue();
    }

    @Test
    void popReturnsElementsInAscendingOrder() throws Exception {
        MapStateHeap<Integer> heap = newHeap();
        List<Integer> input = new ArrayList<>();
        Random random = new Random(42);
        for (int i = 0; i < 500; i++) {
            input.add(random.nextInt(10_000));
        }

        for (int value : input) {
            heap.add(value);
        }
        assertThat(heap.size()).isEqualTo(input.size());

        List<Integer> popped = new ArrayList<>();
        while (!heap.isEmpty()) {
            Integer top = heap.top();
            Integer popResult = heap.pop();
            assertThat(popResult).isEqualTo(top);
            popped.add(popResult);
        }

        List<Integer> expected = new ArrayList<>(input);
        expected.sort(Comparator.naturalOrder());
        assertThat(popped).isEqualTo(expected);
    }

    @Test
    void popReturnsElementsInAscendingOrderWithManyDuplicates() throws Exception {
        MapStateHeap<Integer> heap = newHeap();
        List<Integer> input = new ArrayList<>();
        Random random = new Random(7);
        // Small value range forces many duplicate keys, stressing the tie-breaking paths in
        // add() (equal to the parent) and pop() (equal to a child).
        for (int i = 0; i < 300; i++) {
            input.add(random.nextInt(5));
        }

        for (int value : input) {
            heap.add(value);
        }

        List<Integer> popped = new ArrayList<>();
        while (!heap.isEmpty()) {
            popped.add(heap.pop());
        }

        List<Integer> expected = new ArrayList<>(input);
        expected.sort(Comparator.naturalOrder());
        assertThat(popped).isEqualTo(expected);
    }

    @Test
    void interleavedAddAndPopMatchesPriorityQueue() throws Exception {
        MapStateHeap<Integer> heap = newHeap();
        PriorityQueue<Integer> reference = new PriorityQueue<>();
        Random random = new Random(123);

        for (int i = 0; i < 2000; i++) {
            if (reference.isEmpty() || random.nextBoolean()) {
                int value = random.nextInt(10_000);
                heap.add(value);
                reference.add(value);
            } else {
                assertThat(heap.pop()).isEqualTo(reference.poll());
            }
            assertThat(heap.size()).isEqualTo(reference.size());
        }

        while (!reference.isEmpty()) {
            assertThat(heap.pop()).isEqualTo(reference.poll());
        }
        assertThat(heap.isEmpty()).isTrue();
    }

    @Test
    void heapIsStatelessAndBackedEntirelyByTheGivenMapAndSize() throws Exception {
        InMemoryMapState<Long, Integer> map = new InMemoryMapState<>();
        InMemoryValueState<Long> sizeState = new InMemoryValueState<>();

        new MapStateHeap<>(map, sizeState, Comparator.<Integer>naturalOrder()).add(5);
        // A brand new instance, wrapping the same backing map/size state, must see the element
        // added by a previous, already-discarded instance.
        MapStateHeap<Integer> second =
                new MapStateHeap<>(map, sizeState, Comparator.naturalOrder());
        assertThat(second.size()).isEqualTo(1);
        assertThat(second.top()).isEqualTo(5);

        second.add(3);
        MapStateHeap<Integer> third = new MapStateHeap<>(map, sizeState, Comparator.naturalOrder());
        assertThat(third.pop()).isEqualTo(3);
        assertThat(third.pop()).isEqualTo(5);
        assertThat(third.isEmpty()).isTrue();
    }

    private static MapStateHeap<Integer> newHeap() {
        return new MapStateHeap<Integer>(
                new InMemoryMapState<Long, Integer>(),
                new InMemoryValueState<Long>(),
                Comparator.naturalOrder());
    }

    private static class InMemoryMapState<K, V> implements MapState<K, V> {

        private final Map<K, V> map = new HashMap<>();

        @Override
        public V get(K k) {
            return map.get(k);
        }

        @Override
        public void put(K k, V v) {
            map.put(k, v);
        }

        @Override
        public void putAll(Map<K, V> map) {
            this.map.putAll(map);
        }

        @Override
        public void remove(K k) {
            map.remove(k);
        }

        @Override
        public boolean contains(K k) {
            return map.containsKey(k);
        }

        @Override
        public Iterable<Map.Entry<K, V>> entries() {
            return map.entrySet();
        }

        @Override
        public Iterable<K> keys() {
            return map.keySet();
        }

        @Override
        public Iterable<V> values() {
            return map.values();
        }

        @Override
        public Iterator<Map.Entry<K, V>> iterator() {
            return map.entrySet().iterator();
        }

        @Override
        public boolean isEmpty() {
            return map.isEmpty();
        }

        @Override
        public void clear() {
            map.clear();
        }
    }

    private static class InMemoryValueState<V> implements ValueState<V> {

        private V value;

        @Override
        public V value() {
            return value;
        }

        @Override
        public void update(V value) {
            this.value = value;
        }

        @Override
        public void clear() {
            value = null;
        }
    }
}
