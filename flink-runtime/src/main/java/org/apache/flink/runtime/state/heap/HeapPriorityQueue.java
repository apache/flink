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

import org.apache.flink.runtime.state.InternalPriorityQueue;
import org.apache.flink.util.CloseableIterator;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.apache.flink.util.CollectionUtil.MAX_ARRAY_SIZE;

/**
 * Basic heap-based priority queue for {@link HeapPriorityQueueElement} objects. This heap supports fast deletes
 * because it manages position indexes of the contained {@link HeapPriorityQueueElement}s. The heap implementation is
 * a simple binary tree stored inside an array. Element indexes in the heap array start at 1 instead of 0 to make array
 * index computations a bit simpler in the hot methods. Object identification of remove is based on object identity and
 * not on equals. We use the managed index from {@link HeapPriorityQueueElement} to find an element in the queue
 * array to support fast deletes.
 *
 * <p>Possible future improvements:
 * <ul>
 *  <li>We could also implement shrinking for the heap.</li>
 * </ul>
 *
 * @param <T> type of the contained elements.
 */
public class HeapPriorityQueue<T extends HeapPriorityQueueElement> implements InternalPriorityQueue<T> {

	/**
	 * The index of the head element in the array that represents the heap.
	 */
	private static final int QUEUE_HEAD_INDEX = 1;

	/**
	 * Comparator for the contained elements.
	 */
	private final Comparator<T> elementComparator;

	/**
	 * The array that represents the heap-organized priority queue.
	 */
	private T[] queue;

	/**
	 * The current size of the priority queue.
	 */
	private int size;

	/**
	 * Creates an empty {@link HeapPriorityQueue} with the requested initial capacity.
	 *
	 * @param elementComparator comparator for the contained elements.
	 * @param minimumCapacity the minimum and initial capacity of this priority queue.
	 */
	@SuppressWarnings("unchecked")
	public HeapPriorityQueue(
		@Nonnull Comparator<T> elementComparator,
		@Nonnegative int minimumCapacity) {

		this.elementComparator = elementComparator;
		this.queue = (T[]) new HeapPriorityQueueElement[QUEUE_HEAD_INDEX + minimumCapacity];
	}

	@Override
	@Nullable
	public T poll() {
		return size() > 0 ? removeElementAtIndex(QUEUE_HEAD_INDEX) : null;
	}

	@Override
	@Nullable
	public T peek() {
		return size() > 0 ? queue[QUEUE_HEAD_INDEX] : null;
	}

	/**
	 * Adds the element to add to the heap. This element should not be managed by any other {@link HeapPriorityQueue}.
	 *
	 * @return <code>true</code> if the operation changed the head element or if is it unclear if the head element changed.
	 * Only returns <code>false</code> iff the head element was not changed by this operation.
	 */
	@Override
	public boolean add(@Nonnull T toAdd) {
		return addInternal(toAdd);
	}

	/**
	 * This remove is based on object identity, not the result of equals. We use the objects managed index to find
	 * the instance in the queue array.
	 *
	 * @return <code>true</code> if the operation changed the head element or if is it unclear if the head element changed.
	 * Only returns <code>false</code> iff the head element was not changed by this operation.
	 */
	@Override
	public boolean remove(@Nonnull T toRemove) {
		return removeInternal(toRemove);
	}

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	@Override
	@Nonnegative
	public int size() {
		return size;
	}

	public void clear() {
		size = 0;
		Arrays.fill(queue, null);
	}

	@SuppressWarnings({"unchecked"})
	@Nonnull
	public <O> O[] toArray(O[] out) {
		if (out.length < size) {
			return (O[]) Arrays.copyOfRange(queue, QUEUE_HEAD_INDEX, QUEUE_HEAD_INDEX + size, out.getClass());
		} else {
			System.arraycopy(queue, QUEUE_HEAD_INDEX, out, 0, size);
			if (out.length > size) {
				out[size] = null;
			}
			return out;
		}
	}

	/**
	 * Returns an iterator over the elements in this queue. The iterator
	 * does not return the elements in any particular order.
	 *
	 * @return an iterator over the elements in this queue.
	 */
	@Nonnull
	public CloseableIterator<T> iterator() {
		return new HeapIterator();
	}

	@Override
	public void addAll(@Nullable Collection<? extends T> restoredElements) {

		if (restoredElements == null) {
			return;
		}

		resizeForBulkLoad(restoredElements.size());

		for (T element : restoredElements) {
			add(element);
		}
	}

	private boolean addInternal(@Nonnull T element) {
		final int newSize = increaseSizeByOne();
		moveElementToIdx(element, newSize);
		siftUp(newSize);
		return element.getInternalIndex() == QUEUE_HEAD_INDEX;
	}

	private boolean removeInternal(@Nonnull T elementToRemove) {
		final int elementIndex = elementToRemove.getInternalIndex();
		removeElementAtIndex(elementIndex);
		return elementIndex == QUEUE_HEAD_INDEX;
	}

	private T removeElementAtIndex(int removeIdx) {
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

	public void adjustModifiedElement(@Nonnull T element) {
		final int elementIndex = element.getInternalIndex();
		if (element == queue[elementIndex]) {
			adjustElementAtIndex(element, elementIndex);
		}
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

		while (parentIdx > 0 && isElementLessThen(currentElement, heap[parentIdx])) {
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

		if (isElementIndexValid(secondChildIdx, heapSize) &&
			isElementLessThen(heap[secondChildIdx], heap[firstChildIdx])) {
			firstChildIdx = secondChildIdx;
		}

		while (isElementIndexValid(firstChildIdx, heapSize) &&
			isElementLessThen(heap[firstChildIdx], currentElement)) {
			moveElementToIdx(heap[firstChildIdx], idx);
			idx = firstChildIdx;
			firstChildIdx = idx << 1;
			secondChildIdx = firstChildIdx + 1;

			if (isElementIndexValid(secondChildIdx, heapSize) &&
				isElementLessThen(heap[secondChildIdx], heap[firstChildIdx])) {
				firstChildIdx = secondChildIdx;
			}
		}

		moveElementToIdx(currentElement, idx);
	}

	private boolean isElementIndexValid(int elementIndex, int heapSize) {
		return elementIndex <= heapSize;
	}

	private boolean isElementLessThen(T a, T b) {
		return elementComparator.compare(a, b) < 0;
	}

	private void moveElementToIdx(T element, int idx) {
		queue[idx] = element;
		element.setInternalIndex(idx);
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

	private void resizeForBulkLoad(int totalSize) {
		if (totalSize > queue.length) {
			int desiredSize = totalSize + (totalSize >>> 3);
			resizeQueueArray(desiredSize, totalSize);
		}
	}

	private void resizeQueueArray(int desiredSize, int minRequiredSize) {
		if (isValidArraySize(desiredSize)) {
			queue = Arrays.copyOf(queue, desiredSize);
		} else if (isValidArraySize(minRequiredSize)) {
			queue = Arrays.copyOf(queue, MAX_ARRAY_SIZE);
		} else {
			throw new OutOfMemoryError("Required minimum heap size " + minRequiredSize +
				" exceeds maximum size of " + MAX_ARRAY_SIZE + ".");
		}
	}

	private static boolean isValidArraySize(int size) {
		return size >= 0 && size <= MAX_ARRAY_SIZE;
	}

	/**
	 * {@link Iterator} implementation for {@link HeapPriorityQueue}.
	 * {@link Iterator#remove()} is not supported.
	 */
	private class HeapIterator implements CloseableIterator<T> {

		private int iterationIdx;

		HeapIterator() {
			this.iterationIdx = QUEUE_HEAD_INDEX - 1;
		}

		@Override
		public boolean hasNext() {
			return iterationIdx < size;
		}

		@Override
		public T next() {
			if (iterationIdx >= size) {
				throw new NoSuchElementException("Iterator has no next element.");
			}
			return queue[++iterationIdx];
		}

		@Override
		public void close() {
		}
	}
}
