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

package org.apache.flink.runtime.util;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.flink.util.TraversableOnceException;

/**
 * The key grouped iterator returns a key and all values that share the same key.
 */
public final class NonReusingKeyGroupedIterator<E> implements KeyGroupedIterator<E> {
	
	private final MutableObjectIterator<E> iterator;
	
	private final TypeComparator<E> comparator;
	
	private ValuesIterator valuesIterator;
	
	private E lastKeyRecord;
	
	private E lookahead;
	
	private boolean done;

	/**
	 * Initializes the KeyGroupedIterator. It requires an iterator which returns its result
	 * sorted by the key fields.
	 * 
	 * @param iterator An iterator over records, which are sorted by the key fields, in any order.
	 * @param comparator The comparator for the data type iterated over.
	 */
	public NonReusingKeyGroupedIterator(MutableObjectIterator<E> iterator, TypeComparator<E> comparator) {
		if (iterator == null || comparator == null) {
			throw new NullPointerException();
		}
		
		this.iterator = iterator;
		this.comparator = comparator;
	}

	/**
	 * Moves the iterator to the next key. This method may skip any values that have not yet been returned by the
	 * iterator created by the {@link #getValues()} method. Hence, if called multiple times it "removes" key groups.
	 * 
	 * @return true, if the input iterator has an other group of records with the same key.
	 */
	public boolean nextKey() throws IOException {
		
		if (lookahead != null) {
			// common case: whole value-iterator was consumed and a new key group is available.
			this.comparator.setReference(this.lookahead);
			this.valuesIterator.next = this.lookahead;
			this.lastKeyRecord = this.lookahead;
			this.lookahead = null;
			this.valuesIterator.iteratorAvailable = true;
			return true;
		}
		
		// first element, empty/done, or the values iterator was not entirely consumed
		if (this.done) {
			return false;
		}
			
		if (this.valuesIterator != null) {
			// values was not entirely consumed. move to the next key
			// Required if user code / reduce() method did not read the whole value iterator.
			E next;
			while (true) {
				if ((next = this.iterator.next()) != null) {
					if (!this.comparator.equalToReference(next)) {
						// the keys do not match, so we have a new group. store the current key
						this.comparator.setReference(next);
						this.valuesIterator.next = next;
						this.lastKeyRecord = next;
						this.valuesIterator.iteratorAvailable = true;
						return true;
					}
				}
				else {
					// input exhausted
					this.valuesIterator.next = null;
					this.valuesIterator = null;
					this.lastKeyRecord = null;
					this.done = true;
					return false;
				}
			}
		}
		else {
			// first element
			// get the next element
			E first = this.iterator.next();
			if (first != null) {
				this.comparator.setReference(first);
				this.valuesIterator = new ValuesIterator(first);
				this.lastKeyRecord = first;
				return true;
			}
			else {
				// empty input, set everything null
				this.done = true;
				return false;
			}
		}
	}
	
	private E advanceToNext() {
		try {
			E next = this.iterator.next();
			if (next != null) {
				if (comparator.equalToReference(next)) {
					// same key
					return next;
				} else {
					// moved to the next key, no more values here
					this.lookahead = next;
					return null;
				}
			}
			else {
				// backing iterator is consumed
				this.done = true;
				return null;
			}
		}
		catch (IOException e) {
			throw new RuntimeException("An error occurred while reading the next record.", e);
		}
	}
	
	public E getCurrent() {
		return lastKeyRecord;
	}
	
	public TypeComparator<E> getComparatorWithCurrentReference() {
		return this.comparator;
	}

	/**
	 * Returns an iterator over all values that belong to the current key. The iterator is initially <code>null</code>
	 * (before the first call to {@link #nextKey()} and after all keys are consumed. In general, this method returns
	 * always a non-null value, if a previous call to {@link #nextKey()} return <code>true</code>.
	 * 
	 * @return Iterator over all values that belong to the current key.
	 */
	@Override
	public ValuesIterator getValues() {
		return this.valuesIterator;
	}

	// --------------------------------------------------------------------------------------------
	
	public final class ValuesIterator implements Iterator<E>, Iterable<E> {
		
		private E next;
		
		private boolean iteratorAvailable = true;
		
		private ValuesIterator(E first) {
			this.next = first;
		}

		@Override
		public boolean hasNext() {
			return next != null;
		}

		@Override
		public E next() {
			if (this.next != null) {
				E current = this.next;
				this.next = NonReusingKeyGroupedIterator.this.advanceToNext();
				return current;
			} else {
				throw new NoSuchElementException();
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
		
		@Override
		public Iterator<E> iterator() {
			if (iteratorAvailable) {
				iteratorAvailable = false;
				return this;
			}
			else {
				throw new TraversableOnceException();
			}
		}
	}
}
