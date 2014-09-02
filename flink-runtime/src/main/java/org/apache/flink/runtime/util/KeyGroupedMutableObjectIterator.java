/**
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

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.MutableObjectIterator;

/**
 * The KeyValueIterator returns a key and all values that belong to the key (share the same key).
 * A sub-iterator over all values with the same key is provided.
 */
public final class KeyGroupedMutableObjectIterator<E> {
	
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
	 * @param serializer The serializer for the data type iterated over.
	 * @param comparator The comparator for the data type iterated over.
	 */
	public KeyGroupedMutableObjectIterator(MutableObjectIterator<E> iterator,
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
			if ((this.next = this.iterator.next(this.next)) != null) {
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
			if ((this.next = this.iterator.next(this.next)) != null) {
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

	/**
	 * Returns an iterator over all values that belong to the current key. The iterator is initially <code>null</code>
	 * (before the first call to {@link #nextKey()} and after all keys are consumed. In general, this method returns
	 * always a non-null value, if a previous call to {@link #nextKey()} return <code>true</code>.
	 * 
	 * @return Iterator over all values that belong to the current key.
	 */
	public MutableObjectIterator<E> getValues() {
		return this.valuesIterator;
	}

	// --------------------------------------------------------------------------------------------
	
	private final class ValuesIterator implements MutableObjectIterator<E>
	{
		private final TypeSerializer<E> serializer = KeyGroupedMutableObjectIterator.this.serializer;
		private final TypeComparator<E> comparator = KeyGroupedMutableObjectIterator.this.comparator; 
		
		private boolean nextIsUnconsumed = false;

		@Override
		public E next(E target)
		{
			if (KeyGroupedMutableObjectIterator.this.next == null || KeyGroupedMutableObjectIterator.this.nextIsFresh) {
				return null;
			}
			if (this.nextIsUnconsumed) {
				return this.serializer.copy(KeyGroupedMutableObjectIterator.this.next, target);
			}
			
			try {
				if ((target = KeyGroupedMutableObjectIterator.this.iterator.next(target)) != null) {
					// check whether the keys are equal
					if (!this.comparator.equalToReference(target)) {
						// moved to the next key, no more values here
						KeyGroupedMutableObjectIterator.this.next =
								this.serializer.copy(target, KeyGroupedMutableObjectIterator.this.next);
						KeyGroupedMutableObjectIterator.this.nextIsFresh = true;
						return null;
					}
					// same key, next value is in "next"
					return target;
				}
				else {
					// backing iterator is consumed
					KeyGroupedMutableObjectIterator.this.next = null;
					return null;
				}
			}
			catch (IOException ioex) {
				throw new RuntimeException("An error occurred while reading the next record: " + 
					ioex.getMessage(), ioex);
			}
		}
	}
}
