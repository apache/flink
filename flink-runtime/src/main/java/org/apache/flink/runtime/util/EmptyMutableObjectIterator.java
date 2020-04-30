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

import org.apache.flink.util.MutableObjectIterator;

/**
 * An empty mutable object iterator that never returns anything.
 */
public final class EmptyMutableObjectIterator<E> implements MutableObjectIterator<E> {

	/**
	 * The singleton instance.
	 */
	private static final EmptyMutableObjectIterator<Object> INSTANCE = new EmptyMutableObjectIterator<Object>();
	
	/**
	 * Gets a singleton instance of the empty iterator.
	 *  
	 * @param <E> The type of the objects (not) returned by the iterator.
	 * @return An instance of the iterator.
	 */
	public static <E> MutableObjectIterator<E> get() {
		@SuppressWarnings("unchecked")
		MutableObjectIterator<E> iter = (MutableObjectIterator<E>) INSTANCE;
		return iter;
	}
	
	/**
	 * Always returns null.
	 *  
	 * @see MutableObjectIterator#next(Object)
	 */
	@Override
	public E next(E target) {
		return null;
	}

	/**
	 * Always returns null.
	 *
	 * @see MutableObjectIterator#next()
	 */
	@Override
	public E next() {
		return null;
	}
}
