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

package eu.stratosphere.pact.runtime.task.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Pair;
import eu.stratosphere.pact.common.type.Value;

/**
 * The KeyValueIterator returns a key and all values that belong to the key (share the same key).
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class KeyGroupedIterator<K extends Key, V extends Value> {
	
	private final Iterator<KeyValuePair<K, V>> iterator;

	private KeyValuePair<K, V> next;

	private K currentKey;

	private ValuesIterator valuesIterator;

	private boolean nextIsFresh;

	/**
	 * Initializes the KeyValueIterator. It requires a KeyValuePair iterator which returns its result
	 * sorted by the key.
	 * 
	 * @param iterator
	 *        An iterator over key value pairs, which are sorted by the key.
	 */
	public KeyGroupedIterator(Iterator<KeyValuePair<K, V>> iterator) {
		this.iterator = iterator;
	}

	/**
	 * Moves the iterator to the next key. This method may skip any values that have not yet been returned by the
	 * iterator created by the {@link #getValues()} method. Hence, if called multiple times it "removes" pairs.
	 * 
	 * @return true if the input iterator has an other group of key-value pairs that share the same key.
	 */
	public boolean nextKey() {
		// first element
		if (this.next == null) {
			if (this.iterator.hasNext()) {
				this.next = this.iterator.next();
				this.currentKey = this.next.getKey();
				this.nextIsFresh = false;
				this.valuesIterator = new ValuesIterator();
				this.valuesIterator.nextIsUnconsumed = true;
				return true;
			} else {
				this.currentKey = null;
				this.valuesIterator = null;
				return false;
			}
		}

		// Whole value-iterator was read and a new key is available.
		if (this.nextIsFresh) {
			this.nextIsFresh = false;
			this.currentKey = this.next.getKey();
			this.valuesIterator.nextIsUnconsumed = true;
			return true;
		}

		// try to move to next key.
		// Required if user code / reduce() method did not read the whole value iterator.
		while (true) {
			Pair<K, V> prev = this.next;
			if (this.iterator.hasNext()) {
				this.next = this.iterator.next();
				if (next.getKey().compareTo(prev.getKey()) != 0) {
					this.nextIsFresh = false;
					this.currentKey = this.next.getKey();
					this.valuesIterator.nextIsUnconsumed = true;
					return true;
				}
			} else {
				this.currentKey = null;
				this.valuesIterator = null;
				return false;
			}
		}
	}

	/**
	 * Returns the current key. The current key is initially <code>null</code> (before the first call to
	 * {@link #nextKey()} and after all distinct keys are consumed. In general, this method returns
	 * always a non-null value, if a previous call to {@link #nextKey()} return <code>true</code>.
	 * 
	 * @return The current key.
	 */
	public K getKey() {
		return this.currentKey;
	}

	/**
	 * Returns an iterator over all values that belong to the current key. The iterator is initially <code>null</code>
	 * (before the first call to {@link #nextKey()} and after all keys are consumed. In general, this method returns
	 * always a non-null value, if a previous call to {@link #nextKey()} return <code>true</code>.
	 * 
	 * @return Iterator over all values that belong to the current key.
	 */
	public Iterator<V> getValues() {
		return valuesIterator;
	}

	private final class ValuesIterator implements Iterator<V> {
		private boolean nextIsUnconsumed = false;

		@Override
		public boolean hasNext() {
			if (KeyGroupedIterator.this.next == null || KeyGroupedIterator.this.nextIsFresh) {
				return false;
			}

			if (this.nextIsUnconsumed) {
				return true;
			}

			if (KeyGroupedIterator.this.iterator.hasNext()) {
				KeyGroupedIterator.this.next = KeyGroupedIterator.this.iterator.next();
				if (KeyGroupedIterator.this.next.getKey().compareTo(KeyGroupedIterator.this.currentKey) == 0) {
					// same key, next value is in "next"
					this.nextIsUnconsumed = true;
					return true;
				} else {
					// moved to the next key, no more values here
					KeyGroupedIterator.this.nextIsFresh = true;
					return false;
				}
			} else {
				// backing iterator is consumed
				KeyGroupedIterator.this.next = null;
				return false;
			}
		}

		/**
		 * Prior to call this method, call hasNext() once!
		 */
		@Override
		public V next() {
			if (hasNext()) {
				this.nextIsUnconsumed = false;
				return KeyGroupedIterator.this.next.getValue();
			} else {
				throw new NoSuchElementException();
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

}
