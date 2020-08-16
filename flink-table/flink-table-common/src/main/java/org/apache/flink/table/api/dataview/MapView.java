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
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.dataview.MapViewTypeInfoFactory;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.types.DataType;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/**
 * A {@link DataView} that provides {@link Map}-like functionality in the accumulator of an {@link AggregateFunction}
 * or {@link TableAggregateFunction} when large amounts of data are expected.
 *
 * <p>A {@link MapView} can be backed by a Java {@link HashMap} or can leverage Flink's state backends
 * depending on the context in which the aggregate function is used. In many unbounded data scenarios,
 * the {@link MapView} delegates all calls to a {@link MapState} instead of the {@link HashMap}.
 *
 * <p>Note: Keys of a {@link MapView} must not be null. Nulls in values are supported. For heap-based
 * state backends, {@code hashCode/equals} of the original (i.e. external) class are used. However,
 * the serialization format will use internal data structures.
 *
 * <p>The {@link DataType}s of the view's keys and values are reflectively extracted from the accumulator
 * definition. This includes the generic argument {@code K} and {@code V} of this class. If reflective
 * extraction is not successful, it is possible to use a {@link DataTypeHint} on top the accumulator field.
 * It will be mapped to the underlying collection.
 *
 * <p>The following examples show how to specify an {@link AggregateFunction} with a {@link MapView}:
 * <pre>{@code
 *
 *  public class MyAccumulator {
 *
 *    public MapView<String, Integer> map = new MapView<>();
 *
 *    // or explicit:
 *    // {@literal @}DataTypeHint("MAP<STRING, INT>")
 *    // public MapView<String, Integer> map = new MapView<>();
 *
 *    public long count;
 *  }
 *
 *  public class MyAggregateFunction extends AggregateFunction<Long, MyAccumulator> {
 *
 *   {@literal @}Override
 *   public MyAccumulator createAccumulator() {
 *     return new MyAccumulator();
 *   }
 *
 *    public void accumulate(MyAccumulator accumulator, String id) {
 *      if (!accumulator.map.contains(id)) {
 *        accumulator.map.put(id, 1);
 *        accumulator.count++;
 *      }
 *    }
 *
 *   {@literal @}Override
 *    public Long getValue(MyAccumulator accumulator) {
 *      return accumulator.count;
 *    }
 *  }
 *
 * }</pre>
 *
 * @param <K> key type
 * @param <V> value type
 */
@TypeInfo(MapViewTypeInfoFactory.class)
@PublicEvolving
public class MapView<K, V> implements DataView {

	private Map<K, V> map = new HashMap<>();

	/**
	 * Creates a map view.
	 *
	 * <p>The {@link DataType} of keys and values is reflectively extracted.
	 */
	public MapView() {
		// default constructor
	}

	/**
	 * Returns the entire view's content as an instance of {@link Map}.
	 */
	public Map<K, V> getMap() {
		return map;
	}

	/**
	 * Replaces the entire view's content with the content of the given {@link Map}.
	 */
	public void setMap(Map<K, V> map) {
		this.map = map;
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
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof MapView)) {
			return false;
		}
		final MapView<?, ?> mapView = (MapView<?, ?>) o;
		return getMap().equals(mapView.getMap());
	}

	@Override
	public int hashCode() {
		return Objects.hash(getMap());
	}

	// --------------------------------------------------------------------------------------------
	// Utilities
	// --------------------------------------------------------------------------------------------

	/**
	 * Utility method for creating a {@link DataType} of {@link MapView} explicitly.
	 */
	public static DataType newMapViewDataType(DataType keyDataType, DataType valueDataType) {
		return DataTypes.STRUCTURED(
			MapView.class,
			DataTypes.FIELD(
				"map",
				DataTypes.MAP(keyDataType, valueDataType).bridgedTo(Map.class)));
	}

	// --------------------------------------------------------------------------------------------
	// Legacy
	// --------------------------------------------------------------------------------------------

	@Deprecated
	public transient TypeInformation<?> keyType;

	@Deprecated
	public transient TypeInformation<?> valueType;

	/**
	 * Creates a {@link MapView} with the specified key and value types.
	 *
	 * @param keyType The type of keys of the map view.
	 * @param valueType The type of values of the map view.
	 * @deprecated This method uses the old type system. Please use a {@link DataTypeHint}
	 *             instead if the reflective type extraction is not successful.
	 */
	@Deprecated
	public MapView(TypeInformation<?> keyType, TypeInformation<?> valueType) {
		this.keyType = keyType;
		this.valueType = valueType;
	}
}
