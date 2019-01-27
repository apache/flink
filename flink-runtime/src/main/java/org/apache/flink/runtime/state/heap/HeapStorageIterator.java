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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.StorageIterator;
import org.apache.flink.runtime.state.heap.HeapStateStorage;
import org.apache.flink.types.Pair;
import org.apache.flink.util.Preconditions;

import java.util.Iterator;
import java.util.Map;

/**
 * Implementation of {@link StorageIterator} for {@link HeapStateStorage}.
 */
public class HeapStorageIterator<K, V> implements StorageIterator<K, V> {

	private final Iterator<Map.Entry<K, V>> iterator;

	public HeapStorageIterator(Iterator<Map.Entry<K, V>> iterator) {
		this.iterator = Preconditions.checkNotNull(iterator);
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public Pair<K, V> next() {
		return new HeapPair<>(iterator.next());
	}

	@Override
	public void close() {

	}

	/**
	 * Implementation of {@link Pair}.
	 */
	public static class HeapPair<K, V> implements Pair<K, V> {

		private final Map.Entry<K, V> entry;

		public HeapPair(Map.Entry<K, V> entry) {
			this.entry = Preconditions.checkNotNull(entry);
		}

		@Override
		public K getKey() {
			return entry.getKey();
		}

		@Override
		public V getValue() {
			return entry.getValue();
		}

		@Override
		public V setValue(V value) {
			return entry.setValue(value);
		}
	}
}
