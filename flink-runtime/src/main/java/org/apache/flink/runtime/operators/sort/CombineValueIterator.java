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

import org.apache.flink.util.TraversableOnceException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * This class implements an iterator over values from a sort buffer. The iterator returns the values of a given
 * interval.
 */
final class CombineValueIterator<E> implements Iterator<E>, Iterable<E> {

	private static final Logger LOG = LoggerFactory.getLogger(CombineValueIterator.class);

	private final InMemorySorter<E> buffer; // the buffer from which values are returned

	private final E recordReuse;

	private final boolean objectReuseEnabled;

	private int last; // the position of the last value to be returned

	private int position; // the position of the next value to be returned

	private boolean iteratorAvailable;

	/**
	 * Creates an iterator over the values in a <tt>BufferSortable</tt>.
	 *
	 * @param buffer The buffer to get the values from.
	 */
	public CombineValueIterator(InMemorySorter<E> buffer, E instance, boolean objectReuseEnabled) {
		this.buffer = buffer;
		this.recordReuse = instance;
		this.objectReuseEnabled = objectReuseEnabled;
	}

	/**
	 * Sets the interval for the values that are to be returned by this iterator.
	 *
	 * @param first The position of the first value to be returned.
	 * @param last The position of the last value to be returned.
	 */
	public void set(int first, int last) {
		this.last = last;
		this.position = first;
		this.iteratorAvailable = true;
	}

	@Override
	public boolean hasNext() {
		return this.position <= this.last;
	}

	@Override
	public E next() {
		if (this.position <= this.last) {
			try {
				E record;
				if (objectReuseEnabled) {
					record = this.buffer.getRecord(this.recordReuse, this.position);
				} else {
					record = this.buffer.getRecord(this.position);
				}
				this.position++;
				return record;
			} catch (IOException ioex) {
				LOG.error("Error retrieving a value from a buffer.", ioex);
				throw new RuntimeException("Could not load the next value: " + ioex.getMessage(), ioex);
			}
		} else {
			throw new NoSuchElementException();
		}
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Nonnull
	@Override
	public Iterator<E> iterator() {
		if (iteratorAvailable) {
			iteratorAvailable = false;
			return this;
		} else {
			throw new TraversableOnceException();
		}
	}
}
