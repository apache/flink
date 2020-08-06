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
public class DualKeyLinkedMap<A, B, V> {

	private final LinkedHashMap<A, Tuple2<B, V>> aMap;

	private final Map<B, A> bMap;

	private Collection<V> values;

	public DualKeyLinkedMap(int initialCapacity) {
		this.aMap = new LinkedHashMap<>(initialCapacity);
		this.bMap = new HashMap<>(initialCapacity);
	}

	public int size() {
		return aMap.size();
	}

	@Nullable
	public V getValueByKeyA(A aKey) {
		final Tuple2<B, V> value = aMap.get(aKey);
		return value != null ? value.f1 : null;
	}

	@Nullable
	public V getValueByKeyB(B bKey) {
		final A aKey = bMap.get(bKey);
		return aKey != null ? aMap.get(aKey).f1 : null;
	}

	@Nullable
	public A getKeyAByKeyB(B bKey) {
		return bMap.get(bKey);
	}

	@Nullable
	public B getKeyBByKeyA(A aKey) {
		final Tuple2<B, V> value = aMap.get(aKey);
		return value != null ? value.f0 : null;
	}

	@Nullable
	public V put(A aKey, B bKey, V value) {
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

		return oldValue;
	}

	public boolean containsKeyA(A aKey) {
		return aMap.containsKey(aKey);
	}

	public boolean containsKeyB(B bKey) {
		return bMap.containsKey(bKey);
	}

	@Nullable
	public V removeKeyA(A aKey) {
		Tuple2<B, V> aValue = aMap.remove(aKey);

		if (aValue != null) {
			bMap.remove(aValue.f0);
			return aValue.f1;
		} else {
			return null;
		}
	}

	@Nullable
	public V removeKeyB(B bKey) {
		A aKey = bMap.remove(bKey);

		if (aKey != null) {
			Tuple2<B, V> aValue = aMap.remove(aKey);
			return aValue != null ? aValue.f1 : null;
		} else {
			return null;
		}
	}

	public Collection<V> values() {
		Collection<V> vs = values;

		if (vs == null) {
			vs = new Values();
			values = vs;
		}

		return vs;
	}

	public Set<A> keySetA() {
		return aMap.keySet();
	}

	public Set<B> keySetB() {
		return bMap.keySet();
	}

	public void clear() {
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
