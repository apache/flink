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

package org.apache.flink.table.api.dataview;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.dataview.MapViewTypeInfoFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * A {@link MapView} provides Map functionality for accumulators used by user-defined aggregate
 * functions [[AggregateFunction]].
 *
 * <p>A {@link MapView} can be backed by a Java HashMap or a state backend, depending on the
 * context in which the aggregation function is used.
 *
 * <p>At runtime {@link MapView} will be replaced by a state MapView which is backed by a
 * {@link org.apache.flink.api.common.state.MapState} instead of {@link HashMap} if it works
 * in streaming.
 *
 * <p>Example of an accumulator type with a {@link MapView} and an aggregate function that uses it:
 * <pre>{@code
 *
 *  public class MyAccum {
 *    public MapView<String, Integer> map;
 *    public long count;
 *  }
 *
 *  public class MyAgg extends AggregateFunction<Long, MyAccum> {
 *
 *    @Override
 *    public MyAccum createAccumulator() {
 *      MyAccum accum = new MyAccum();
 *      accum.map = new MapView<>(Types.STRING, Types.INT);
 *      accum.count = 0L;
 *      return accum;
 *    }
 *
 *    public void accumulate(MyAccum accumulator, String id) {
 *      try {
 *          if (!accumulator.map.contains(id)) {
 *            accumulator.map.put(id, 1);
 *            accumulator.count++;
 *          }
 *      } catch (Exception e) {
 *        e.printStackTrace();
 *      }
 *    }
 *
 *    @Override
 *    public Long getValue(MyAccum accumulator) {
 *      return accumulator.count;
 *    }
 *  }
 *
 * }</pre>
 *
 */
@TypeInfo(MapViewTypeInfoFactory.class)
@PublicEvolving
public class MapView<K, V> implements DataView {

	private static final long serialVersionUID = -6185595470714822744L;

	public transient TypeInformation<?> keyType;
	public transient TypeInformation<?> valueType;
	public final Map<K, V> map;

	public MapView(TypeInformation<?> keyType, TypeInformation<?> valueType, Map<K, V> map) {
		this.keyType = keyType;
		this.valueType = valueType;
		this.map = map;
	}

	/**
	 * Creates a MapView with the specified key and value types.
	 *
	 * @param keyType The type of keys of the MapView.
	 * @param valueType The type of the values of the MapView.
	 */
	public MapView(TypeInformation<?> keyType, TypeInformation<?> valueType) {
		this(keyType, valueType, new HashMap<>());
	}

	/**
	 * Creates a MapView.
	 */
	public MapView() {
		this(null, null);
	}

	/**
	 * Return the value for the specified key or {@code null} if the key is not in the map view.
	 *
	 * @param key The look up key.
	 * @return The value for the specified key.
	 * @throws Exception Thrown if the system cannot get data.
	 */
	public V get(K key) throws Exception {
		return map.get(key);
	}

	/**
	 * Inserts a value for the given key into the map view.
	 * If the map view already contains a value for the key, the existing value is overwritten.
	 *
	 * @param key   The key for which the value is inserted.
	 * @param value The value that is inserted for the key.
	 * @throws Exception Thrown if the system cannot put data.
	 */
	public void put(K key, V value) throws Exception {
		map.put(key, value);
	}

	/**
	 * Inserts all mappings from the specified map to this map view.
	 *
	 * @param map The map whose entries are inserted into this map view.
	 * @throws Exception Thrown if the system cannot access the map.
	 */
	public void putAll(Map<K, V> map) throws Exception {
		this.map.putAll(map);
	}

	/**
	 * Deletes the value for the given key.
	 *
	 * @param key The key for which the value is deleted.
	 * @throws Exception Thrown if the system cannot access the map.
	 */
	public void remove(K key) throws Exception {
		map.remove(key);
	}

	/**
	 * Checks if the map view contains a value for a given key.
	 *
	 * @param key The key to check.
	 * @return True if there exists a value for the given key, false otherwise.
	 * @throws Exception Thrown if the system cannot access the map.
	 */
	public boolean contains(K key) throws Exception {
		return map.containsKey(key);
	}

	/**
	 * Returns all entries of the map view.
	 *
	 * @return An iterable of all the key-value pairs in the map view.
	 * @throws Exception Thrown if the system cannot access the map.
	 */
	public Iterable<Map.Entry<K, V>> entries() throws Exception {
		return map.entrySet();
	}

	/**
	 * Returns all the keys in the map view.
	 *
	 * @return An iterable of all the keys in the map.
	 * @throws Exception Thrown if the system cannot access the map.
	 */
	public Iterable<K> keys() throws Exception {
		return map.keySet();
	}

	/**
	 * Returns all the values in the map view.
	 *
	 * @return An iterable of all the values in the map.
	 * @throws Exception Thrown if the system cannot access the map.
	 */
	public Iterable<V> values() throws Exception {
		return map.values();
	}

	/**
	 * Returns an iterator over all entries of the map view.
	 *
	 * @return An iterator over all the mappings in the map.
	 * @throws Exception Thrown if the system cannot access the map.
	 */
	public Iterator<Map.Entry<K, V>> iterator() throws Exception {
		return map.entrySet().iterator();
	}

	/**
	 * Returns true if the map view contains no key-value mappings, otherwise false.
	 *
	 * @return True if the map view contains no key-value mappings, otherwise false.
	 *
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	public boolean isEmpty() throws Exception {
		return map.isEmpty();
	}

	/**
	 * Removes all entries of this map.
	 */
	@Override
	public void clear() {
		map.clear();
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof MapView) {
			return map.equals(((MapView) other).map);
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return map.hashCode();
	}

}
