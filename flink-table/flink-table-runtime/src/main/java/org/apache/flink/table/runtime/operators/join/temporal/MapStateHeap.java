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

import java.util.Comparator;
import java.util.NoSuchElementException;

/**
 * A binary min-heap whose backing array is a {@link MapState}: the map's keys are always exactly
 * the heap indices {@code 0 .. size() - 1}, and its values are the heap elements. Because {@link
 * MapState} has no constant-time {@code size()}, the element count is tracked in a separate {@link
 * ValueState}.
 *
 * <p>This class holds no state of its own - everything lives in the {@code heap} map and the {@code
 * heapSize} value state handed to the constructor - so a fresh instance can (and is meant to) be
 * created for every single heap operation. Each operation only reads/writes the entries on the path
 * between the root and the affected leaf, so {@link #add} and {@link #pop} never need to touch
 * elements that are not involved in the current operation.
 */
final class MapStateHeap<T> {

    private final MapState<Long, T> heap;
    private final ValueState<Long> heapSize;
    private final Comparator<T> comparator;

    MapStateHeap(MapState<Long, T> heap, ValueState<Long> heapSize, Comparator<T> comparator) {
        this.heap = heap;
        this.heapSize = heapSize;
        this.comparator = comparator;
    }

    long size() throws Exception {
        Long size = heapSize.value();
        return size == null ? 0L : size;
    }

    boolean isEmpty() throws Exception {
        return size() == 0L;
    }

    /**
     * @return the smallest element, or {@code null} if the heap is empty.
     */
    T top() throws Exception {
        if (isEmpty()) {
            return null;
        }
        return heap.get(0L);
    }

    void add(T element) throws Exception {
        long index = size();
        heap.put(index, element);
        heapSize.update(index + 1);

        while (index != 0) {
            long parentIndex = (index - 1) / 2;
            T parent = heap.get(parentIndex);
            if (comparator.compare(parent, element) <= 0) {
                break;
            }
            heap.put(index, parent);
            heap.put(parentIndex, element);
            index = parentIndex;
        }
    }

    /** Removes and returns the smallest element. */
    T pop() throws Exception {
        long currentSize = size();
        if (currentSize == 0) {
            throw new NoSuchElementException("Cannot pop from an empty heap.");
        }

        T min = heap.get(0L);
        if (currentSize == 1) {
            heap.remove(0L);
            heapSize.update(0L);
            return min;
        }

        // The element sinking down is always this one value - it never needs to be re-read from
        // the map, since the map may still physically hold its old, already-relocated contents at
        // `index` until it is written back below.
        long lastIndex = currentSize - 1;
        T sinkingValue = heap.get(lastIndex);
        heap.remove(lastIndex);
        long newSize = lastIndex;
        heapSize.update(newSize);

        long index = 0;
        while (true) {
            long leftChildIndex = 2 * index + 1;
            long rightChildIndex = leftChildIndex + 1;
            long smallestIndex = index;
            T smallestValue = sinkingValue;

            if (leftChildIndex < newSize) {
                T leftChildValue = heap.get(leftChildIndex);
                if (comparator.compare(leftChildValue, smallestValue) < 0) {
                    smallestIndex = leftChildIndex;
                    smallestValue = leftChildValue;
                }
            }
            if (rightChildIndex < newSize) {
                T rightChildValue = heap.get(rightChildIndex);
                if (comparator.compare(rightChildValue, smallestValue) < 0) {
                    smallestIndex = rightChildIndex;
                    smallestValue = rightChildValue;
                }
            }
            if (smallestIndex == index) {
                break;
            }

            heap.put(index, smallestValue);
            index = smallestIndex;
        }
        heap.put(index, sinkingValue);

        return min;
    }
}
