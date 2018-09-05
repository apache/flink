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

package org.apache.flink.runtime.util;

import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * This class implements a list (array based) that is physically bounded in maximum size, but can virtually grow beyond
 * the bounded size. When the list grows beyond the size bound, elements are dropped from the head of the list (FIFO
 * order). If dropped elements are accessed, a default element is returned instead.
 * 
 * <p>The list by itself is serializable, but a full list can only be serialized if the values
 * are also serializable.
 *
 * @param <T> type of the list elements
 */
public class EvictingBoundedList<T> implements Iterable<T>, Serializable {

	private static final long serialVersionUID = -1863961980953613146L;

	@SuppressWarnings("NonSerializableFieldInSerializableClass")
	/** the default element returned for positions that were evicted */
	private final T defaultElement;

	@SuppressWarnings("NonSerializableFieldInSerializableClass")
	/** the array (viewed as a circular buffer) that holds the latest (= non-evicted) elements */
	private final Object[] elements;

	/** The next index to put an element in the array */
	private int idx;

	/** The current number of (virtual) elements in the list */
	private int count;

	/** Modification count for fail-fast iterators */
	private long modCount;

	// ------------------------------------------------------------------------

	public EvictingBoundedList(int sizeLimit) {
		this(sizeLimit, null);
	}

	public EvictingBoundedList(EvictingBoundedList<T> other) {
		Preconditions.checkNotNull(other);
		this.defaultElement = other.defaultElement;
		this.elements = other.elements.clone();
		this.idx = other.idx;
		this.count = other.count;
		this.modCount = 0L;
	}

	public EvictingBoundedList(int sizeLimit, T defaultElement) {
		this.elements = new Object[sizeLimit];
		this.defaultElement = defaultElement;
		this.idx = 0;
		this.count = 0;
		this.modCount = 0L;
	}

	// ------------------------------------------------------------------------

	public int size() {
		return count;
	}

	public boolean isEmpty() {
		return 0 == count;
	}

	public boolean add(T t) {
		elements[idx] = t;
		idx = (idx + 1) % elements.length;
		++count;
		++modCount;
		return true;
	}

	public void clear() {
		if (!isEmpty()) {
			for (int i = 0; i < elements.length; ++i) {
				elements[i] = null;
			}
			count = 0;
			idx = 0;
			++modCount;
		}
	}

	public T get(int index) {
		if (index >= 0 && index < count) {
			return isDroppedIndex(index) ? getDefaultElement() : accessInternal(index % elements.length);
		} else {
			throw new IndexOutOfBoundsException(String.valueOf(index));
		}
	}

	public int getSizeLimit() {
		return elements.length;
	}

	public T set(int index, T element) {
		Preconditions.checkArgument(index >= 0 && index < count);
		++modCount;
		if (isDroppedIndex(index)) {
			return getDefaultElement();
		} else {
			int idx = index % elements.length;
			T old = accessInternal(idx);
			elements[idx] = element;
			return old;
		}
	}

	public T getDefaultElement() {
		return defaultElement;
	}

	private boolean isDroppedIndex(int idx) {
		return idx < count - elements.length;
	}

	@SuppressWarnings("unchecked")
	private T accessInternal(int arrayIndex) {
		return (T) elements[arrayIndex];
	}

	@Nonnull
	@Override
	public Iterator<T> iterator() {
		return new Iterator<T>() {

			int pos = 0;
			final long oldModCount = modCount;

			@Override
			public boolean hasNext() {
				return pos < count;
			}

			@Override
			public T next() {
				if (oldModCount != modCount) {
					throw new ConcurrentModificationException();
				}
				if (pos < count) {
					return get(pos++);
				} else {
					throw new NoSuchElementException("Iterator exhausted.");
				}
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException("Read-only iterator");
			}
		};
	}
}
