/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.state;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.commons.collections.BoundedCollection;
import org.apache.commons.collections.Buffer;
import org.apache.commons.collections.BufferUnderflowException;

@SuppressWarnings("rawtypes")
public class NullableCircularBuffer extends AbstractCollection implements Buffer,
		BoundedCollection, Serializable {

	/** Serialization version */
	private static final long serialVersionUID = 5603722811189451017L;

	/** Underlying storage array */
	private transient Object[] elements;

	/** Array index of first (oldest) buffer element */
	private transient int start = 0;

	/**
	 * Index mod maxElements of the array position following the last buffer
	 * element. Buffer elements start at elements[start] and "wrap around"
	 * elements[maxElements-1], ending at elements[decrement(end)]. For example,
	 * elements = {c,a,b}, start=1, end=1 corresponds to the buffer [a,b,c].
	 */
	private transient int end = 0;

	/** Flag to indicate if the buffer is currently full. */
	private transient boolean full = false;

	/** Capacity of the buffer */
	private final int maxElements;

	/**
	 * Constructs a new <code>BoundedFifoBuffer</code> big enough to hold 32
	 * elements.
	 */
	public NullableCircularBuffer() {
		this(32);
	}

	/**
	 * Constructs a new <code>BoundedFifoBuffer</code> big enough to hold the
	 * specified number of elements.
	 *
	 * @param size
	 *            the maximum number of elements for this fifo
	 * @throws IllegalArgumentException
	 *             if the size is less than 1
	 */
	public NullableCircularBuffer(int size) {
		if (size <= 0) {
			throw new IllegalArgumentException("The size must be greater than 0");
		}
		elements = new Object[size];
		maxElements = elements.length;
	}

	/**
	 * Constructs a new <code>BoundedFifoBuffer</code> big enough to hold all of
	 * the elements in the specified collection. That collection's elements will
	 * also be added to the buffer.
	 *
	 * @param coll
	 *            the collection whose elements to add, may not be null
	 * @throws NullPointerException
	 *             if the collection is null
	 */
	@SuppressWarnings("unchecked")
	public NullableCircularBuffer(Collection coll) {
		this(coll.size());
		addAll(coll);
	}

	// -----------------------------------------------------------------------
	/**
	 * Write the buffer out using a custom routine.
	 * 
	 * @param out
	 *            the output stream
	 * @throws IOException
	 */
	private void writeObject(ObjectOutputStream out) throws IOException {
		out.defaultWriteObject();
		out.writeInt(size());
		for (Iterator it = iterator(); it.hasNext();) {
			out.writeObject(it.next());
		}
	}

	/**
	 * Read the buffer in using a custom routine.
	 * 
	 * @param in
	 *            the input stream
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		elements = new Object[maxElements];
		int size = in.readInt();
		for (int i = 0; i < size; i++) {
			elements[i] = in.readObject();
		}
		start = 0;
		full = (size == maxElements);
		if (full) {
			end = 0;
		} else {
			end = size;
		}
	}

	// -----------------------------------------------------------------------
	/**
	 * Returns the number of elements stored in the buffer.
	 *
	 * @return this buffer's size
	 */
	public int size() {
		int size = 0;

		if (end < start) {
			size = maxElements - start + end;
		} else if (end == start) {
			size = (full ? maxElements : 0);
		} else {
			size = end - start;
		}

		return size;
	}

	/**
	 * Returns true if this buffer is empty; false otherwise.
	 *
	 * @return true if this buffer is empty
	 */
	public boolean isEmpty() {
		return size() == 0;
	}

	/**
	 * Returns true if this collection is full and no new elements can be added.
	 *
	 * @return <code>true</code> if the collection is full
	 */
	public boolean isFull() {
		return size() == maxElements;
	}

	/**
	 * Gets the maximum size of the collection (the bound).
	 *
	 * @return the maximum number of elements the collection can hold
	 */
	public int maxSize() {
		return maxElements;
	}

	/**
	 * Clears this buffer.
	 */
	public void clear() {
		full = false;
		start = 0;
		end = 0;
		Arrays.fill(elements, null);
	}

	/**
	 * Adds the given element to this buffer.
	 *
	 * @param element
	 *            the element to add
	 * @return true, always
	 */
	public boolean add(Object element) {

		if (isFull()) {
			remove();
		}

		elements[end++] = element;

		if (end >= maxElements) {
			end = 0;
		}

		if (end == start) {
			full = true;
		}

		return true;
	}

	/**
	 * Returns the least recently inserted element in this buffer.
	 *
	 * @return the least recently inserted element
	 * @throws BufferUnderflowException
	 *             if the buffer is empty
	 */
	public Object get() {
		if (isEmpty()) {
			throw new BufferUnderflowException("The buffer is already empty");
		}

		return elements[start];
	}

	/**
	 * Removes the least recently inserted element from this buffer.
	 *
	 * @return the least recently inserted element
	 * @throws BufferUnderflowException
	 *             if the buffer is empty
	 */
	public Object remove() {
		if (isEmpty()) {
			throw new BufferUnderflowException("The buffer is already empty");
		}

		Object element = elements[start];

		elements[start++] = null;

		if (start >= maxElements) {
			start = 0;
		}

		full = false;

		return element;
	}

	/**
	 * Increments the internal index.
	 * 
	 * @param index
	 *            the index to increment
	 * @return the updated index
	 */
	private int increment(int index) {
		index++;
		if (index >= maxElements) {
			index = 0;
		}
		return index;
	}

	/**
	 * Decrements the internal index.
	 * 
	 * @param index
	 *            the index to decrement
	 * @return the updated index
	 */
	private int decrement(int index) {
		index--;
		if (index < 0) {
			index = maxElements - 1;
		}
		return index;
	}

	/**
	 * Returns an iterator over this buffer's elements.
	 *
	 * @return an iterator over this buffer's elements
	 */
	public Iterator iterator() {
		return new Iterator() {

			private int index = start;
			private int lastReturnedIndex = -1;
			private boolean isFirst = full;

			public boolean hasNext() {
				return isFirst || (index != end);

			}

			public Object next() {
				if (!hasNext()) {
					throw new NoSuchElementException();
				}
				isFirst = false;
				lastReturnedIndex = index;
				index = increment(index);
				return elements[lastReturnedIndex];
			}

			public void remove() {
				if (lastReturnedIndex == -1) {
					throw new IllegalStateException();
				}

				// First element can be removed quickly
				if (lastReturnedIndex == start) {
					NullableCircularBuffer.this.remove();
					lastReturnedIndex = -1;
					return;
				}

				int pos = lastReturnedIndex + 1;
				if (start < lastReturnedIndex && pos < end) {
					// shift in one part
					System.arraycopy(elements, pos, elements, lastReturnedIndex, end - pos);
				} else {
					// Other elements require us to shift the subsequent
					// elements
					while (pos != end) {
						if (pos >= maxElements) {
							elements[pos - 1] = elements[0];
							pos = 0;
						} else {
							elements[decrement(pos)] = elements[pos];
							pos = increment(pos);
						}
					}
				}

				lastReturnedIndex = -1;
				end = decrement(end);
				elements[end] = null;
				full = false;
				index = decrement(index);
			}

		};
	}

}
