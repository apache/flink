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

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An empty iterator that never returns anything.
 */
public final class EmptyIterator<E> implements Iterator<E>, Iterable<E> {

	/**
	 * The singleton instance.
	 */
	private static final EmptyIterator<Object> INSTANCE = new EmptyIterator<Object>();
	
	/**
	 * Gets a singleton instance of the empty iterator.
	 *  
	 * @param <E> The type of the objects (not) returned by the iterator.
	 * @return An instance of the iterator.
	 */
	public static <E> EmptyIterator<E> get() {
		@SuppressWarnings("unchecked")
		EmptyIterator<E> iter = (EmptyIterator<E>) INSTANCE;
		return iter;
	}
	
	/**
	 * Always returns false, since this iterator is empty.
	 * 
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext() {
		return false;
	}

	/**
	 * Always throws a {@link java.util.NoSuchElementException}.
	 *  
	 * @see java.util.Iterator#next()
	 */
	@Override
	public E next() {
		throw new NoSuchElementException();
	}

	/**
	 * Throws a {@link java.lang.UnsupportedOperationException}.
	 * 
	 * @see java.util.Iterator#remove()
	 */
	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<E> iterator() {
		return this;
	}
}
