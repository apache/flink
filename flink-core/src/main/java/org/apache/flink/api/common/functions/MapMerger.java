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

package org.apache.flink.api.common.functions;

import org.apache.flink.util.Preconditions;

import java.util.Map;

/**
 * A {@link Merger} for maps which merges maps by putting the pairs in the
 * second map into  the first one and merging the values under the same key.
 *
 * @param <K> Type of the keys in the maps to be merged.
 * @param <V> Type of the values in the maps to be merged.
 */
public final class MapMerger<K, V> implements Merger<Map<K, V>> {

	private static final long serialVersionUID = 1L;

	/**
	 * The merger for the values in the map.
	 */
	private final Merger<V> valueMerger;

	/**
	 * Constructor with the merger for the values in the map.
	 *
	 * @param valueMerger The merger for the values in the map.
	 */
	public MapMerger(Merger<V> valueMerger) {
		Preconditions.checkNotNull(valueMerger);
		this.valueMerger = valueMerger;
	}

	/**
	 * Returns the merger for the values in the map.
	 *
	 * @return The merger for the values in the map.
	 */
	public Merger<V> getValueMerger() {
		return valueMerger;
	}

	@Override
	public Map<K, V> merge(
		final Map<K, V> map1,
		final Map<K, V> map2
	) {
		Preconditions.checkNotNull(map1);
		Preconditions.checkNotNull(map2);

		for (Map.Entry<K, V> entry : map2.entrySet()) {
			V oldValue = map1.get(entry.getKey());
			if (oldValue == null) {
				map1.put(entry.getKey(), entry.getValue());
			} else {
				V newValue = valueMerger.merge(oldValue, entry.getValue());
				map1.put(entry.getKey(), newValue);
			}
		}

		return map1;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		MapMerger<?, ?> mapMerger = (MapMerger<?, ?>) o;
		return valueMerger.equals(mapMerger.valueMerger);
	}

	@Override
	public int hashCode() {
		return valueMerger.hashCode();
	}

	@Override
	public String toString() {
		return "MapMerger{" +
			"valueMerger=" + valueMerger +
			"}";
	}
}

