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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Set;

/**
 * Map which stores values under two different indices.
 *
 * @param <A> Type of key A
 * @param <B> Type of key B
 * @param <V> Type of the value
 */
class DualKeyLinkedMap<A, B, V> {

	private final LinkedHashMap<A, Tuple2<B, V>> aMap;

	private final LinkedHashMap<B, A> bMap;

	private transient Collection<V> values;

	DualKeyLinkedMap(int initialCapacity) {
		this.aMap = new LinkedHashMap<>(initialCapacity);
		this.bMap = new LinkedHashMap<>(initialCapacity);
	}

	int size() {
		return aMap.size();
	}

	V getKeyA(A aKey) {
		final Tuple2<B, V> value = aMap.get(aKey);

		if (value != null) {
			return value.f1;
		} else {
			return null;
		}
	}

	V getKeyB(B bKey) {
		final A aKey = bMap.get(bKey);

		if (aKey != null) {
			return aMap.get(aKey).f1;
		} else {
			return null;
		}
	}

	V put(A aKey, B bKey, V value) {
		final V removedValue = removeKeyA(aKey);
		removeKeyB(bKey);

		aMap.put(aKey, Tuple2.of(bKey, value));
		bMap.put(bKey, aKey);

		if (removedValue != null) {
			return removedValue;
		} else {
			return null;
		}
	}

	boolean containsKeyA(A aKey) {
		return aMap.containsKey(aKey);
	}

	boolean containsKeyB(B bKey) {
		return bMap.containsKey(bKey);
	}

	V removeKeyA(A aKey) {
		Tuple2<B, V> aValue = aMap.remove(aKey);

		if (aValue != null) {
			bMap.remove(aValue.f0);
			return aValue.f1;
		} else {
			return null;
		}
	}

	V removeKeyB(B bKey) {
		A aKey = bMap.remove(bKey);

		if (aKey != null) {
			Tuple2<B, V> aValue = aMap.remove(aKey);
			if (aValue != null) {
				return aValue.f1;
			} else {
				return null;
			}
		} else {
			return null;
		}
	}

	Collection<V> values() {
		Collection<V> vs = values;

		if (vs == null) {
			vs = new Values();
			values = vs;
		}

		return vs;
	}

	Set<A> keySetA() {
		return aMap.keySet();
	}

	Set<B> keySetB() {
		return bMap.keySet();
	}

	void clear() {
		aMap.clear();
		bMap.clear();
	}

	// -----------------------------------------------------------------------
	// Inner classes
	// -----------------------------------------------------------------------

	/**
	 * Collection which contains the values of the dual key map.
	 */
	private final class Values extends AbstractCollection<V> {

		@Override
		public Iterator<V> iterator() {
			return new ValueIterator();
		}

		@Override
		public int size() {
			return aMap.size();
		}
	}

	/**
	 * Iterator which iterates over the values of the dual key map.
	 */
	private final class ValueIterator implements Iterator<V> {

		private final Iterator<Tuple2<B, V>> iterator = aMap.values().iterator();

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public V next() {
			Tuple2<B, V> value = iterator.next();

			return value.f1;
		}
	}
}
