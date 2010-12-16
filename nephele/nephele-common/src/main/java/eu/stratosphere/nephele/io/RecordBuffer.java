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

package eu.stratosphere.nephele.io;

import java.lang.reflect.Array;

/**
 * Buffer to temporarily store records.
 * 
 * @author warneke
 * @param <T>
 *        the type of records that can be stored in this buffer
 */
public class RecordBuffer<T> {

	/**
	 * Index to the current head element of the record buffer.
	 */
	private int currentHead = 0;

	/**
	 * Index to the current tail element of the record buffer.
	 */
	private int currentTail = 0;

	/**
	 * The current size of the record buffer.
	 */
	private int currentSize = 0;

	/**
	 * The record buffer itself as an array of records.
	 */
	private final T[] buffer;

	/**
	 * Constructs a new record buffer for the given record type.
	 * 
	 * @param type
	 *        the type of records to be stored inside the buffer
	 * @param maxSize
	 *        the maximum size of the buffer
	 */
	@SuppressWarnings("unchecked")
	public RecordBuffer(Class<T> type, int maxSize) {

		this.buffer = (T[]) Array.newInstance(type, maxSize);
	}

	/**
	 * Adds a new record to the tail of the buffer.
	 * 
	 * @param record
	 *        the record to be added to the buffer.
	 */
	public void put(T record) {

		if (record == null) {
			return;
		}

		if (currentSize < buffer.length) {

			// Append to tail of queue
			this.buffer[currentTail] = record;
			decrementTail();
			this.currentSize++;
		} else {
			throw new ArrayIndexOutOfBoundsException("Trying to add element to full queue");
		}
	}

	/**
	 * Returns a record from the head of the buffer.
	 * 
	 * @return a record from the head of the buffer
	 */
	public T get() {

		if (currentSize == 0) {
			throw new ArrayIndexOutOfBoundsException("Trying to get element from empty queue");
		}

		// Now there is at least one item in the queue
		final T record = this.buffer[currentHead];
		decrementHead();
		this.currentSize--;

		return record;
	}

	/**
	 * Decrements the current head position of the buffer.
	 */
	private void decrementHead() {
		this.currentHead--;
		if (this.currentHead < 0) {
			this.currentHead = this.buffer.length - 1;
		}
	}

	/**
	 * Decrements the current tail position of the buffer.
	 */
	private void decrementTail() {
		this.currentTail--;
		if (this.currentTail < 0) {
			this.currentTail = this.buffer.length - 1;
		}
	}

	/**
	 * Returns the current size of the buffer.
	 * 
	 * @return the current size of the buffer
	 */
	public final int getSize() {
		return this.currentSize;
	}

	/**
	 * Returns the capacity of the buffer.
	 * 
	 * @return the capacity of the buffer
	 */
	public final int capacity() {
		return this.buffer.length;
	}

	public void clear() {
		this.currentHead = 0;
		this.currentTail = 0;
		this.currentSize = 0;
	}
}
