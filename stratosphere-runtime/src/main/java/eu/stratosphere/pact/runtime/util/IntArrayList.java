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

package eu.stratosphere.pact.runtime.util;

/**
 * Minimal implementation of an array-backed list of ints
 */
public class IntArrayList {
	
	private int size;
	
	private int[] array;

	public IntArrayList(final int capacity) {
		this.size = 0;
		this.array = new int[capacity];
	}
	
	public int size() {
		return size;
	}
	
	public boolean add(final int number) {
		grow(size+1);
		array[size++] = number;
		return true;
	}

	public int removeInt(int index) {
		if(index >= size) {
			throw new IndexOutOfBoundsException("Index (" + index + ") is greater than or equal to list size (" + size + ")");
		}
		final int old = array[ index ];
		size--;
		if(index != size) {
			System.arraycopy(array, index+1, array, index, size-index );
		}
		return old;
	}
	
	public void clear() {
		size = 0;
	}
	
	public boolean isEmpty() {
		return (size==0);
	}
	
	private void grow(final int length) {
		if(length > array.length) {
			final int newLength = (int)Math.max(Math.min(2L * array.length, Integer.MAX_VALUE-8), length);
			final int[] t = new int[newLength];
			System.arraycopy(array, 0, t, 0, size);
			array = t;
		}
	}

}
