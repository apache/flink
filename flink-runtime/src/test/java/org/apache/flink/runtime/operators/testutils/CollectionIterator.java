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

package org.apache.flink.runtime.operators.testutils;

import org.apache.flink.runtime.util.ResettableMutableObjectIterator;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;


public class CollectionIterator<T> implements ResettableMutableObjectIterator<T> {

	private final Collection<T> collection;
	private Iterator<T> iterator;

	public CollectionIterator(Collection<T> collection) {
		this.collection = collection;
		this.iterator = collection.iterator();
	}

	@Override
	public T next(T reuse) throws IOException {
		return next();
	}

	@Override
	public T next() throws IOException {
		if (!iterator.hasNext()) {
			return null;
		} else {
			return iterator.next();
		}
	}

	@Override
	public void reset() throws IOException {
		iterator = collection.iterator();
	}

	public static <T> CollectionIterator<T> of(T... values) {
		return new CollectionIterator<T>(Arrays.asList(values));
	}
}
