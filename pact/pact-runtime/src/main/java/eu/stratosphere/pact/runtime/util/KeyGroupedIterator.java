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
import eu.stratosphere.pact.generic.types.TypeComparator;
import eu.stratosphere.pact.generic.types.TypeSerializer;

/**
 * The KeyValueIterator returns a key and all values that belong to the key (share the same key).
 * 
 * @author Stephan Ewen
 */
public final class KeyGroupedIterator<E>
{
	private final MutableObjectIterator<E> iterator;

	private final TypeSerializer<E> serializer;
	
	private final TypeComparator<E> comparator;
	
	private E current;
	
	private E lookahead;

	private ValuesIterator valuesIterator;

	private boolean lookAheadHasNext;
	
	private boolean done;

	/**
	 * Initializes the KeyGroupedIterator. It requires an iterator which returns its result
	 * sorted by the key fields.
	 * 
	 * @param iterator An iterator over records, which are sorted by the key fields, in any order.
	 * @param serializer The serializer for the data type iterated over.
	 * @param comparator The comparator for the data type iterated over.
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
		// first element (or empty)
		if (this.current == null) {
			if (this.done) {
				this.valuesIterator = null;
				return false;
			}
			this.current = this.serializer.createInstance();
			if (this.iterator.next(this.current)) {
				this.comparator.setReference(this.current);
				this.lookAheadHasNext = false;
				this.valuesIterator = new ValuesIterator();
				this.valuesIterator.currentIsUnconsumed = true;
				return true;
			} else {
				// empty input, set everything null
				this.valuesIterator = null;
				this.current = null;
				this.done = true;
				return false;
			}
		}

		// Whole value-iterator was read and a new key is available.
		if (this.lookAheadHasNext) {
			this.lookAheadHasNext = false;
			this.current = this.lookahead;
			this.lookahead = null;
			this.comparator.setReference(this.current);
			this.valuesIterator.currentIsUnconsumed = true;
			return true;
		}

		// try to move to next key.
		// Required if user code / reduce() method did not read the whole value iterator.
		while (true) {
			if (this.iterator.next(this.current)) {
				if (!this.comparator.equalToReference(this.current)) {
					// the keys do not match, so we have a new group. store the current keys
					this.comparator.setReference(this.current);						
					this.lookAheadHasNext = false;
					this.valuesIterator.currentIsUnconsumed = true;
					return true;
				}
			}
			else {
				this.valuesIterator = null;
				this.current = null;
				this.done = true;
				return false;
			}
		}
	}
	
	public TypeComparator<E> getComparatorWithCurrentReference() {
		return this.comparator;
	}
	
	public E getCurrent() {
		return this.current;
	}

	/**
	 * Returns an iterator over all values that belong to the current key. The iterator is initially <code>null</code>
	 * (before the first call to {@link #nextKey()} and after all keys are consumed. In general, this method returns
	 * always a non-null value, if a previous call to {@link #nextKey()} return <code>true</code>.
	 * 
	 * @return Iterator over all values that belong to the current key.
	 */
	public ValuesIterator getValues() {
		return this.valuesIterator;
	}

	// --------------------------------------------------------------------------------------------
	
	public final class ValuesIterator implements Iterator<E>
	{
		private final TypeSerializer<E> serializer = KeyGroupedIterator.this.serializer;
		private final TypeComparator<E> comparator = KeyGroupedIterator.this.comparator; 
		
		private E staging = this.serializer.createInstance();
		private boolean currentIsUnconsumed = false;
		
		private ValuesIterator() {}

		@Override
		public boolean hasNext()
		{
			if (KeyGroupedIterator.this.current == null || KeyGroupedIterator.this.lookAheadHasNext) {
				return false;
			}
			if (this.currentIsUnconsumed) {
				return true;
			}
			
			try {
				// read the next value into the staging record to make sure we keep the
				// current as it is in case the key changed
				if (KeyGroupedIterator.this.iterator.next(this.staging)) {
					if (this.comparator.equalToReference(this.staging)) {
						// same key, next value is in staging, so exchange staging with current
						final E tmp = this.staging;
						this.staging = KeyGroupedIterator.this.current;
						KeyGroupedIterator.this.current = tmp;
						this.currentIsUnconsumed = true;
						return true;
					} else {
						// moved to the next key, no more values here
						KeyGroupedIterator.this.lookAheadHasNext = true;
						KeyGroupedIterator.this.lookahead = this.staging;
						this.staging = KeyGroupedIterator.this.current;
						return false;						
					}
				}
				else {
					// backing iterator is consumed
					KeyGroupedIterator.this.done = true;
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
			if (this.currentIsUnconsumed || hasNext()) {
				this.currentIsUnconsumed = false;
				return KeyGroupedIterator.this.current;
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
