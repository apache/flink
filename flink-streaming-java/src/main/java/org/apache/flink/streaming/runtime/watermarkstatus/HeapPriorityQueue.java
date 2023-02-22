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

package org.apache.flink.streaming.runtime.watermarkstatus;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Arrays;

import static org.apache.flink.util.CollectionUtil.MAX_ARRAY_SIZE;

/**
 * This class has similar functions with {@link
 * org.apache.flink.runtime.state.heap.HeapPriorityQueue}. It is introduced as the replacement of
 * {@link org.apache.flink.runtime.state.heap.HeapPriorityQueue} to be used in {@link
 * StatusWatermarkValve}, to avoid affecting the performance of memory state backend.
 *
 * <p>The reason why the performance of memory state backend will be affected if we reuse the {@link
 * org.apache.flink.runtime.state.heap.HeapPriorityQueue}: In some scenarios, the {@link
 * org.apache.flink.runtime.state.heap.HeapPriorityQueueElement} will only have one
 * implementation(used by memory state backend), which allows the jvm to inline its
 * methods(getInternalIndex, setInternalIndex). If we reuse it in {@link StatusWatermarkValve}, it
 * will cause it to have multiple implementations. Once there are multiple implementations, its
 * methods will be difficult to be inlined by jvm, which will result in poor performance of memory
 * state backend.
 *
 * @param <T> type of the contained elements.
 */
public class HeapPriorityQueue<T extends HeapPriorityQueue.HeapPriorityQueueElement> {

    /** The index of the head element in the array that represents the heap. */
    private static final int QUEUE_HEAD_INDEX = 1;

    /** Comparator for the priority of contained elements. */
    @Nonnull private final PriorityComparator<T> elementPriorityComparator;

    /** The array that represents the heap-organized priority queue. */
    @Nonnull private T[] queue;

    /** The current size of the priority queue. */
    @Nonnegative private int size;

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
        this.queue = (T[]) new HeapPriorityQueueElement[getHeadElementIndex() + minimumCapacity];
        this.size = 0;
        this.elementPriorityComparator = elementPriorityComparator;
    }

    public void adjustModifiedElement(@Nonnull T element) {
        final int elementIndex = element.getInternalIndex();
        if (element == queue[elementIndex]) {
            adjustElementAtIndex(element, elementIndex);
        }
    }

    @Nullable
    public T poll() {
        return size() > 0 ? removeInternal(getHeadElementIndex()) : null;
    }

    @Nullable
    public T peek() {
        // References to removed elements are expected to become set to null.
        return queue[getHeadElementIndex()];
    }

    public boolean add(@Nonnull T toAdd) {
        addInternal(toAdd);
        return toAdd.getInternalIndex() == getHeadElementIndex();
    }

    public boolean remove(@Nonnull T toRemove) {
        final int elementIndex = toRemove.getInternalIndex();
        removeInternal(elementIndex);
        return elementIndex == getHeadElementIndex();
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public int size() {
        return size;
    }

    /** Clears the queue. */
    public void clear() {
        final int arrayOffset = getHeadElementIndex();
        Arrays.fill(queue, arrayOffset, arrayOffset + size, null);
        size = 0;
    }

    private void resizeQueueArray(int desiredSize, int minRequiredSize) {
        if (isValidArraySize(desiredSize)) {
            queue = Arrays.copyOf(queue, desiredSize);
        } else if (isValidArraySize(minRequiredSize)) {
            queue = Arrays.copyOf(queue, MAX_ARRAY_SIZE);
        } else {
            throw new OutOfMemoryError(
                    "Required minimum heap size "
                            + minRequiredSize
                            + " exceeds maximum size of "
                            + MAX_ARRAY_SIZE
                            + ".");
        }
    }

    private void moveElementToIdx(T element, int idx) {
        queue[idx] = element;
        element.setInternalIndex(idx);
    }

    private static boolean isValidArraySize(int size) {
        return size >= 0 && size <= MAX_ARRAY_SIZE;
    }

    private int getHeadElementIndex() {
        return QUEUE_HEAD_INDEX;
    }

    private void addInternal(@Nonnull T element) {
        final int newSize = increaseSizeByOne();
        moveElementToIdx(element, newSize);
        siftUp(newSize);
    }

    private T removeInternal(int removeIdx) {
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

    /**
     * This interface works similar to {@link Comparable} and is used to prioritize between two
     * objects. The main difference between this interface and {@link Comparable} is it is not
     * require to follow the usual contract between that {@link Comparable#compareTo(Object)} and
     * {@link Object#equals(Object)}. The contract of this interface is: When two objects are equal,
     * they indicate the same priority, but indicating the same priority does not require that both
     * objects are equal.
     *
     * @param <T> type of the compared objects.
     */
    interface PriorityComparator<T> {

        /**
         * Compares two objects for priority. Returns a negative integer, zero, or a positive
         * integer as the first argument has lower, equal to, or higher priority than the second.
         *
         * @param left left operand in the comparison by priority.
         * @param right left operand in the comparison by priority.
         * @return a negative integer, zero, or a positive integer as the first argument has lower,
         *     equal to, or higher priority than the second.
         */
        int comparePriority(T left, T right);
    }

    /**
     * Interface for objects that can be managed by a {@link HeapPriorityQueue}. Such an object can
     * only be contained in at most one {@link HeapPriorityQueue} at a time.
     */
    interface HeapPriorityQueueElement {

        /**
         * The index that indicates that a {@link HeapPriorityQueueElement} object is not contained
         * in and managed by any {@link HeapPriorityQueue}. We do not strictly enforce that internal
         * indexes must be reset to this value when elements are removed from a {@link
         * HeapPriorityQueue}.
         */
        int NOT_CONTAINED = Integer.MIN_VALUE;

        /**
         * Returns the current index of this object in the internal array of {@link
         * HeapPriorityQueue}.
         */
        int getInternalIndex();

        /**
         * Sets the current index of this object in the {@link HeapPriorityQueue} and should only be
         * called by the owning {@link HeapPriorityQueue}.
         *
         * @param newIndex the new index in the timer heap.
         */
        void setInternalIndex(int newIndex);
    }
}
