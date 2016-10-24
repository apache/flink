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

import java.util.Arrays;

/**
 * Minimal implementation of an array-backed list of longs
 */
public class LongArrayList {
		
	private int size;
	
	private long[] array;
	
	public LongArrayList(int capacity) {
		this.size = 0;
		this.array = new long[capacity];
	}
	
	public int size() {
		return size;
	}
	
	public boolean add(long number) {
		grow(size+1);
		array[size++] = number;
		return true;
	}
	
	public long removeLong(int index) {
		if(index >= size) {
			throw new IndexOutOfBoundsException( "Index (" + index + ") is greater than or equal to list size (" + size + ")" );
		}
		final long old = array[index];
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

	public long[] toArray() {
		return Arrays.copyOf(array, size);
	}
	
	private void grow(int length) {
		if(length > array.length) {
			final int newLength = (int)Math.max(Math.min(2L * array.length, Integer.MAX_VALUE-8), length);
			final long[] t = new long[newLength];
			System.arraycopy(array, 0, t, 0, size);
			array = t;
		}
	}

}
