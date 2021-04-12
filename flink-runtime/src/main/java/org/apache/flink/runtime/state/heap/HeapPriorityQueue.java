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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.PriorityComparator;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

/**
 * Basic heap-based priority queue for {@link HeapPriorityQueueElement} objects. This heap supports
 * fast deletes because it manages position indexes of the contained {@link
 * HeapPriorityQueueElement}s. The heap implementation is a simple binary tree stored inside an
 * array. Element indexes in the heap array start at 1 instead of 0 to make array index computations
 * a bit simpler in the hot methods. Object identification of remove is based on object identity and
 * not on equals. We use the managed index from {@link HeapPriorityQueueElement} to find an element
 * in the queue array to support fast deletes.
 *
 * <p>Possible future improvements:
 *
 * <ul>
 *   <li>We could also implement shrinking for the heap.
 * </ul>
 *
 * @param <T> type of the contained elements.
 */
public class HeapPriorityQueue<T extends HeapPriorityQueueElement>
        extends AbstractHeapPriorityQueue<T> {

    /** The index of the head element in the array that represents the heap. */
    private static final int QUEUE_HEAD_INDEX = 1;

    /** Comparator for the priority of contained elements. */
    @Nonnull protected final PriorityComparator<T> elementPriorityComparator;

    /**
     * Creates an empty {@link HeapPriorityQueue} with the requested initial capacity.
     *
     * @param elementPriorityComparator comparator for the priority of contained elements.
     * @param minimumCapacity the minimum and initial capacity of this priority queue.
     */
    @SuppressWarnings("unchecked")
    public HeapPriorityQueue(
            @Nonnull PriorityComparator<T> elementPriorityComparator,
            @Nonnegative int minimumCapacity) {
        super(minimumCapacity);
        this.elementPriorityComparator = elementPriorityComparator;
    }

    public void adjustModifiedElement(@Nonnull T element) {
        final int elementIndex = element.getInternalIndex();
        if (element == queue[elementIndex]) {
            adjustElementAtIndex(element, elementIndex);
        }
    }

    @Override
    protected int getHeadElementIndex() {
        return QUEUE_HEAD_INDEX;
    }

    @Override
    protected void addInternal(@Nonnull T element) {
        final int newSize = increaseSizeByOne();
        moveElementToIdx(element, newSize);
        siftUp(newSize);
    }

    @Override
    protected T removeInternal(int removeIdx) {
        T[] heap = this.queue;
        T removedValue = heap[removeIdx];

        assert removedValue.getInternalIndex() == removeIdx;

        final int oldSize = size;

        if (removeIdx != oldSize) {
            T element = heap[oldSize];
            moveElementToIdx(element, removeIdx);
            adjustElementAtIndex(element, removeIdx);
        }

        heap[oldSize] = null;

        --size;
        return removedValue;
    }

    private void adjustElementAtIndex(T element, int index) {
        siftDown(index);
        if (queue[index] == element) {
            siftUp(index);
        }
    }

    private void siftUp(int idx) {
        final T[] heap = this.queue;
        final T currentElement = heap[idx];
        int parentIdx = idx >>> 1;

        while (parentIdx > 0 && isElementPriorityLessThen(currentElement, heap[parentIdx])) {
            moveElementToIdx(heap[parentIdx], idx);
            idx = parentIdx;
            parentIdx >>>= 1;
        }

        moveElementToIdx(currentElement, idx);
    }

    private void siftDown(int idx) {
        final T[] heap = this.queue;
        final int heapSize = this.size;

        final T currentElement = heap[idx];
        int firstChildIdx = idx << 1;
        int secondChildIdx = firstChildIdx + 1;

        if (isElementIndexValid(secondChildIdx, heapSize)
                && isElementPriorityLessThen(heap[secondChildIdx], heap[firstChildIdx])) {
            firstChildIdx = secondChildIdx;
        }

        while (isElementIndexValid(firstChildIdx, heapSize)
                && isElementPriorityLessThen(heap[firstChildIdx], currentElement)) {
            moveElementToIdx(heap[firstChildIdx], idx);
            idx = firstChildIdx;
            firstChildIdx = idx << 1;
            secondChildIdx = firstChildIdx + 1;

            if (isElementIndexValid(secondChildIdx, heapSize)
                    && isElementPriorityLessThen(heap[secondChildIdx], heap[firstChildIdx])) {
                firstChildIdx = secondChildIdx;
            }
        }

        moveElementToIdx(currentElement, idx);
    }

    private boolean isElementIndexValid(int elementIndex, int heapSize) {
        return elementIndex <= heapSize;
    }

    private boolean isElementPriorityLessThen(T a, T b) {
        return elementPriorityComparator.comparePriority(a, b) < 0;
    }

    private int increaseSizeByOne() {
        final int oldArraySize = queue.length;
        final int minRequiredNewSize = ++size;
        if (minRequiredNewSize >= oldArraySize) {
            final int grow = (oldArraySize < 64) ? oldArraySize + 2 : oldArraySize >> 1;
            resizeQueueArray(oldArraySize + grow, minRequiredNewSize);
        }
        // TODO implement shrinking as well?
        return minRequiredNewSize;
    }
}
