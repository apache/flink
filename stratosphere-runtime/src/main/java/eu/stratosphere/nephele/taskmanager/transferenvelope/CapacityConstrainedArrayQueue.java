/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.taskmanager.transferenvelope;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;

/**
 * This class implements a simple, capacity-constrained queue. The implementation is highlighted by its small
 * overhead in terms of memory consumption and management.
 * <p>
 * This class is not thread-safe.
 * 
 * @param <E>
 *        the type of the elements stored in this queue
 */
public final class CapacityConstrainedArrayQueue<E> implements Queue<E> {

	/**
	 * The maximum capacity supported by this type of queue.
	 */
	private static final int MAX_CAPACITY = 128;

	/**
	 * The array storing the actual elements of the queue.
	 */
	private final E[] elements;

	/**
	 * Index to the current head of the queue.
	 */
	private byte head = 0;

	/**
	 * Index to the current tail of the queue.
	 */
	private byte tail = 0;

	/**
	 * The current size of the queue.
	 */
	private byte size = 0;

	/**
	 * Constructs a new capacity-constrained array queue.
	 * 
	 * @param capacity
	 *        the capacity limit of the queue
	 */
	@SuppressWarnings("unchecked")
	public CapacityConstrainedArrayQueue(final int capacity) {

		if (capacity > MAX_CAPACITY) {
			throw new IllegalArgumentException("This queue does only support capacities up to " + MAX_CAPACITY);
		}

		this.elements = (E[]) new Object[capacity];
	}


	@Override
	public boolean addAll(final Collection<? extends E> c) {

		throw new UnsupportedOperationException("addAll is not supported on this type of queue");
	}

	/**
	 * Checks if there is capacity left in the queue.
	 * 
	 * @return <code>true</code> if there is capacity left in the queue, <code>false</code> otherwise
	 */
	private boolean capacityLeft() {

		return ((this.elements.length - this.size) > 0);
	}

	/**
	 * Increments the head of the queue.
	 */
	private void incrementHead() {

		if (++this.head == this.elements.length) {
			this.head = 0;
		}
	}

	/**
	 * Increments the tail of the queue.
	 */
	private void incrementTail() {

		if (++this.tail == this.elements.length) {
			this.tail = 0;
		}
	}


	@Override
	public void clear() {

		this.head = 0;
		this.tail = 0;
		this.size = 0;
	}


	@Override
	public boolean containsAll(final Collection<?> c) {

		throw new UnsupportedOperationException("containsAll is not supported on this type of queue");
	}


	@Override
	public boolean isEmpty() {

		return (this.size == 0);
	}


	@Override
	public boolean removeAll(final Collection<?> c) {

		throw new UnsupportedOperationException("removeAll is not supported on this type of queue");
	}


	@Override
	public boolean retainAll(final Collection<?> c) {

		throw new UnsupportedOperationException("retainAll is not supported on this type of queue");
	}


	@Override
	public Object[] toArray() {

		throw new UnsupportedOperationException("toArray is not supported on this type of queue");
	}


	@Override
	public <T> T[] toArray(T[] a) {

		throw new UnsupportedOperationException("toArray is not supported on this type of queue");
	}


	@Override
	public boolean add(final E arg0) {

		throw new UnsupportedOperationException("add is not supported on this type of queue");
	}


	@Override
	public boolean contains(final Object arg0) {

		throw new UnsupportedOperationException("contains is not supported on this type of queue");
	}


	@Override
	public E element() {

		throw new UnsupportedOperationException("element is not supported on this type of queue");
	}


	@Override
	public Iterator<E> iterator() {

		return new CapacityConstrainedArrayQueueIterator(this.head);
	}


	@Override
	public boolean offer(final E arg0) {

		if (!capacityLeft()) {
			return false;
		}

		this.elements[this.tail] = arg0;
		incrementTail();
		++this.size;

		return true;
	}


	@Override
	public E peek() {

		if (isEmpty()) {
			return null;
		}

		return this.elements[this.head];
	}


	@Override
	public E poll() {

		if (isEmpty()) {
			return null;
		}

		final E retVal = this.elements[this.head];
		incrementHead();
		--this.size;

		return retVal;
	}


	@Override
	public E remove() {

		final E retVal = poll();
		if (retVal == null) {
			throw new NoSuchElementException();
		}

		return retVal;
	}


	@Override
	public boolean remove(final Object arg0) {

		throw new UnsupportedOperationException("remove is not supported on this type of queue");
	}


	@Override
	public int size() {

		return this.size;
	}

	/**
	 * This class implements an iterator for the capacity-constrained array queue.
	 * 
	 */
	private final class CapacityConstrainedArrayQueueIterator implements Iterator<E> {

		/**
		 * The current position of the iterator.
		 */
		private byte pos;

		/**
		 * Counter how many this the position index has been modified.
		 */
		private byte count = 0;

		/**
		 * Constructs a new capacity-constrained array queue iterator.
		 * 
		 * @param startPos
		 *        the start position of the iterator
		 */
		private CapacityConstrainedArrayQueueIterator(final byte startPos) {
			this.pos = startPos;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean hasNext() {

			if (this.count < size) {
				return true;
			}

			return false;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public E next() {

			final E retVal = elements[this.pos];

			if (++this.pos == elements.length) {
				this.pos = 0;
			}

			++this.count;

			return retVal;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void remove() {

			throw new UnsupportedOperationException("remove is not supported by this iterator");
		}

	}
}
