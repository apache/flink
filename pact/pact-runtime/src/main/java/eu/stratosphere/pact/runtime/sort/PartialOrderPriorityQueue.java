/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.sort;

import java.util.AbstractQueue;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Queue;

/**
 * This class implements a priority-queue, which maintains a partial
 * ordering of its elements such that the+ least element can always be found
 * in constant time. Put()'s and pop()'s require log(size) time.
 * 
 * @author Erik Nijkamp
 */
public class PartialOrderPriorityQueue<T> extends AbstractQueue<T> implements Queue<T> {
	/**
	 * The heap, organized as an array.
	 */
	private final T[] heap;

	/**
	 * The comparator used to establish the order between the streams.
	 */
	private final Comparator<T> comparator;

	/**
	 * The maximum size of the heap.
	 */
	private final int capacity;

	/**
	 * The current number of elements in the queue.
	 */
	private int size;

	@SuppressWarnings("unchecked")
	public PartialOrderPriorityQueue(Comparator<T> comparator, int capacity) {
		this.comparator = comparator;
		this.capacity = capacity + 1;
		this.size = 0;
		this.heap = (T[]) new Object[this.capacity];
	}

	/**
	 * Determines the ordering of objects in this priority queue.
	 * 
	 * @param a
	 *        The first element.
	 * @param b
	 *        The second element.
	 * @return True, if a &lt; b, false otherwise.
	 */
	private final boolean lessThan(T a, T b) {
		return comparator.compare(a, b) < 0;
	}

	/**
	 * Returns the remaining capacity of the backing array.
	 * 
	 * @return The remaining capacity of the backing array.
	 */
	public int remainingCapacity() {
		return capacity - size;
	}

	/**
	 * Adds a buffer to a PriorityQueue in log(size) time. If one tries to
	 * add more objects than maxSize from initialize a RuntimeException
	 * (ArrayIndexOutOfBound) is thrown.
	 */
	public final void put(T element) {
		size++;
		heap[size] = element;
		upHeap();
	}

	/**
	 * Adds element to the PriorityQueue in log(size) time if either the
	 * PriorityQueue is not full, or not lessThan(element, top()).
	 * 
	 * @param element
	 *        The element to insert,
	 * @return True, if element is added, false otherwise.
	 */
	public boolean offer(T element) {
		if (size < capacity) {
			put(element);
			return true;
		} else if (size > 0 && !lessThan(element, peek())) {
			heap[1] = element;
			adjustTop();
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Returns the least element of the PriorityQueue in constant time, but
	 * does not remove it from the priority queue.
	 * 
	 * @return The least element.
	 */
	public final T peek() {
		if (size > 0) {
			return heap[1];
		} else {
			return null;
		}
	}

	/**
	 * Removes and returns the least element of the PriorityQueue in
	 * log(size) time.
	 * 
	 * @return The least element.
	 */
	public final T poll() {
		if (size > 0) {
			T result = heap[1]; // save first value
			heap[1] = heap[size]; // move last to first
			heap[size] = null; // permit GC of objects
			size--;
			downHeap(); // adjust heap
			return result;
		} else {
			return null;
		}
	}

	/**
	 * Should be called when the Object at top changes values. Still log(n)
	 * worst case, but it's at least twice as fast to
	 * 
	 * <pre>
	 * {
	 * 	pq.top().change();
	 * 	pq.adjustTop();
	 * }
	 * </pre>
	 * 
	 * instead of
	 * 
	 * <pre>
	 * {
	 * 	o = pq.pop();
	 * 	o.change();
	 * 	pq.push(o);
	 * }
	 * </pre>
	 */
	public final void adjustTop() {
		downHeap();
	}

	/**
	 * Returns the number of elements currently stored in the PriorityQueue.
	 * 
	 * @return The number of elements in the queue.
	 */
	public final int size() {
		return size;
	}

	/**
	 * Removes all entries from the PriorityQueue.
	 */
	public final void clear() {
		for (int i = 0; i <= size; i++) {
			heap[i] = null;
		}
		size = 0;
	}

	private final void upHeap() {
		int i = size;
		T node = heap[i]; // save bottom node
		int j = i >>> 1;
		while (j > 0 && lessThan(node, heap[j])) {
			heap[i] = heap[j]; // shift parents down
			i = j;
			j = j >>> 1;
		}
		heap[i] = node; // install saved node
	}

	private final void downHeap() {
		int i = 1;
		T node = heap[i]; // save top node
		int j = i << 1; // find smaller child
		int k = j + 1;
		if (k <= size && lessThan(heap[k], heap[j])) {
			j = k;
		}

		while (j <= size && lessThan(heap[j], node)) {
			heap[i] = heap[j]; // shift up child
			i = j;
			j = i << 1;
			k = j + 1;
			if (k <= size && lessThan(heap[k], heap[j])) {
				j = k;
			}
		}

		heap[i] = node; // install saved node
	}

	@Override
	public Iterator<T> iterator() {
		return Arrays.asList(heap).iterator();
	}
}
