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

import javax.annotation.Nullable;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Map which stores values under two different indices. The mapping of the primary key to the
 * value is backed by {@link LinkedHashMap} so that the iteration order over the values and
 * the primary key set is the insertion order. Note that the insertion order is not affected
 * if a primary key is re-inserted into the map. Also note that there is no contract of the
 * iteration order over the secondary key set.
 *
 *
 * @param <A> Type of key A. Key A is the primary key.
 * @param <B> Type of key B. Key B is the secondary key.
 * @param <V> Type of the value
 */
class DualKeyLinkedMap<A, B, V> {

	private final LinkedHashMap<A, Tuple2<B, V>> aMap;

	private final Map<B, A> bMap;

	private transient Collection<V> values;

	DualKeyLinkedMap(int initialCapacity) {
		this.aMap = new LinkedHashMap<>(initialCapacity);
		this.bMap = new HashMap<>(initialCapacity);
	}

	int size() {
		return aMap.size();
	}

	@Nullable
	V getValueByKeyA(A aKey) {
		final Tuple2<B, V> value = aMap.get(aKey);

		if (value != null) {
			return value.f1;
		} else {
			return null;
		}
	}

	@Nullable
	V getValueByKeyB(B bKey) {
		final A aKey = bMap.get(bKey);

		if (aKey != null) {
			return aMap.get(aKey).f1;
		} else {
			return null;
		}
	}

	@Nullable
	A getKeyAByKeyB(B bKey) {
		return bMap.get(bKey);
	}

	@Nullable
	B getKeyBByKeyA(A aKey) {
		final Tuple2<B, V> value = aMap.get(aKey);

		if (value != null) {
			return value.f0;
		} else {
			return null;
		}
	}

	@Nullable
	V put(A aKey, B bKey, V value) {
		final V oldValue = getValueByKeyA(aKey);

		// cleanup orphaned keys if the given primary key and secondary key were not matched previously
		if (!Objects.equals(aKey, getKeyAByKeyB(bKey))) {
			// remove legacy secondary key as well as its corresponding primary key and value
			removeKeyB(bKey);

			// remove the secondary key that the primary key once pointed to
			final B oldBKeyOfAKey = getKeyBByKeyA(aKey);
			if (oldBKeyOfAKey != null) {
				bMap.remove(oldBKeyOfAKey);
			}
		}

		aMap.put(aKey, Tuple2.of(bKey, value));
		bMap.put(bKey, aKey);

		if (oldValue != null) {
			return oldValue;
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

	@Nullable
	V removeKeyA(A aKey) {
		Tuple2<B, V> aValue = aMap.remove(aKey);

		if (aValue != null) {
			bMap.remove(aValue.f0);
			return aValue.f1;
		} else {
			return null;
		}
	}

	@Nullable
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
