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

package eu.stratosphere.pact.runtime.util;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.plugable.TypeComparator;
import eu.stratosphere.pact.runtime.plugable.TypeSerializer;

/**
 * The KeyValueIterator returns a key and all values that belong to the key (share the same key).
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class KeyGroupedIterator<E>
{
	private final MutableObjectIterator<E> iterator;

	private final TypeSerializer<E> serializer;
	
	private final TypeComparator<E> comparator;
	
	private E next;

	private ValuesIterator valuesIterator;

	private boolean nextIsFresh;

	/**
	 * Initializes the KeyGroupedIterator. It requires an iterator which returns its result
	 * sorted by the key fields.
	 * 
	 * @param iterator An iterator over records, which are sorted by the key fields, in any order.
	 * @param keyPositions The positions of the keys in the records.
	 * @param keyClasses The types of the key fields.
	 */
	public KeyGroupedIterator(MutableObjectIterator<E> iterator,
			TypeSerializer<E> serializer, TypeComparator<E> comparator)
	{
		if (iterator == null || serializer == null || comparator == null) {
			throw new NullPointerException();
		}
		
		this.iterator = iterator;
		this.serializer = serializer;
		this.comparator = comparator;
	}

	/**
	 * Moves the iterator to the next key. This method may skip any values that have not yet been returned by the
	 * iterator created by the {@link #getValues()} method. Hence, if called multiple times it "removes" pairs.
	 * 
	 * @return true if the input iterator has an other group of key-value pairs that share the same key.
	 */
	public boolean nextKey() throws IOException
	{
		// first element
		if (this.next == null) {
			this.next = this.serializer.createInstance();
			if (this.iterator.next(this.next)) {
				this.comparator.setReference(this.next);
				this.nextIsFresh = false;
				this.valuesIterator = new ValuesIterator();
				this.valuesIterator.nextIsUnconsumed = true;
				return true;
			} else {
				// empty input, set everything null
				this.valuesIterator = null;
				return false;
			}
		}

		// Whole value-iterator was read and a new key is available.
		if (this.nextIsFresh) {
			this.nextIsFresh = false;
			this.comparator.setReference(this.next);
			this.valuesIterator.nextIsUnconsumed = true;
			return true;
		}

		// try to move to next key.
		// Required if user code / reduce() method did not read the whole value iterator.
		while (true) {
			if (this.iterator.next(this.next)) {
				if (!this.comparator.equalToReference(this.next)) {
					// the keys do not match, so we have a new group. store the current keys
					this.comparator.setReference(this.next);						
					this.nextIsFresh = false;
					this.valuesIterator.nextIsUnconsumed = true;
					return true;
				}
			}
			else {
				this.valuesIterator = null;
				return false;
			}
		}
	}
	
	public E getCurrent() {
		return this.next;
	}

	/**
	 * Returns an iterator over all values that belong to the current key. The iterator is initially <code>null</code>
	 * (before the first call to {@link #nextKey()} and after all keys are consumed. In general, this method returns
	 * always a non-null value, if a previous call to {@link #nextKey()} return <code>true</code>.
	 * 
	 * @return Iterator over all values that belong to the current key.
	 */
	public ValuesIterator getValues() {
		return valuesIterator;
	}

	// --------------------------------------------------------------------------------------------
	
	public final class ValuesIterator implements Iterator<E>
	{
		private final TypeSerializer<E> serializer = KeyGroupedIterator.this.serializer;
		private final TypeComparator<E> comparator = KeyGroupedIterator.this.comparator; 
		
		private E bufferRec = this.serializer.createInstance();
		private boolean nextIsUnconsumed = false;
		
		private ValuesIterator() {}

		@Override
		public boolean hasNext()
		{
			if (KeyGroupedIterator.this.next == null || KeyGroupedIterator.this.nextIsFresh) {
				return false;
			}
			if (this.nextIsUnconsumed) {
				return true;
			}
			
			try {
				if (KeyGroupedIterator.this.iterator.next(this.bufferRec)) {
					// exchange the buffer record and the next record
					E tmp = this.bufferRec;
					this.bufferRec = KeyGroupedIterator.this.next;
					KeyGroupedIterator.this.next = tmp;
					
					if (!this.comparator.equalToReference(tmp)) {
						// moved to the next key, no more values here
						KeyGroupedIterator.this.nextIsFresh = true;
						return false;						
					}
					
					// same key, next value is in "next"
					this.nextIsUnconsumed = true;
					return true;
				}
				else {
					// backing iterator is consumed
					KeyGroupedIterator.this.next = null;
					return false;
				}
			}
			catch (IOException ioex) {
				throw new RuntimeException("An error occurred while reading the next record: " + 
					ioex.getMessage(), ioex);
			}
		}

		/**
		 * Prior to call this method, call hasNext() once!
		 */
		@Override
		public E next() {
			if (this.nextIsUnconsumed || hasNext()) {
				this.nextIsUnconsumed = false;
				return KeyGroupedIterator.this.next;
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
