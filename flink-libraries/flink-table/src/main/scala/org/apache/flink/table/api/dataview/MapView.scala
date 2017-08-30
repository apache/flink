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

package org.apache.flink.table.api.dataview

import java.lang.{Iterable => JIterable}
import java.util

import org.apache.flink.api.common.typeinfo.{TypeInfo, TypeInformation}
import org.apache.flink.table.dataview.MapViewTypeInfoFactory

/**
  * A [[MapView]] provides Map functionality for accumulators used by user-defined aggregate
  * functions [[org.apache.flink.table.functions.AggregateFunction]].
  *
  * A [[MapView]] can be backed by a Java HashMap or a state backend, depending on the context in
  * which the aggregation function is used.
  *
  * At runtime [[MapView]] will be replaced by a [[org.apache.flink.table.dataview.StateMapView]]
  * if it is backed by a state backend.
  *
  * Example of an accumulator type with a [[MapView]] and an aggregate function that uses it:
  * {{{
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
  * }}}
  *
  * @param keyTypeInfo key type information
  * @param valueTypeInfo value type information
  * @tparam K key type
  * @tparam V value type
  */
@TypeInfo(classOf[MapViewTypeInfoFactory[_, _]])
class MapView[K, V](
    @transient private[flink] val keyTypeInfo: TypeInformation[K],
    @transient private[flink] val valueTypeInfo: TypeInformation[V],
    private[flink] val map: util.Map[K, V])
  extends DataView {

  /**
    * Creates a MapView with the specified key and value types.
    *
    * @param keyTypeInfo The type of keys of the MapView.
    * @param valueTypeInfo The type of the values of the MapView.
    */
  def this(keyTypeInfo: TypeInformation[K], valueTypeInfo: TypeInformation[V]) {
    this(keyTypeInfo, valueTypeInfo, new util.HashMap[K, V]())
  }

  /**
    * Creates a MapView.
    */
  def this() = this(null, null)

  /**
    * Return the value for the specified key or { @code null } if the key is not in the map view.
    *
    * @param key The look up key.
    * @return The value for the specified key.
    * @throws Exception Thrown if the system cannot get data.
    */
  @throws[Exception]
  def get(key: K): V = map.get(key)

  /**
    * Inserts a value for the given key into the map view.
    * If the map view already contains a value for the key, the existing value is overwritten.
    *
    * @param key   The key for which the value is inserted.
    * @param value The value that is inserted for the key.
    * @throws Exception Thrown if the system cannot put data.
    */
  @throws[Exception]
  def put(key: K, value: V): Unit = map.put(key, value)

  /**
    * Inserts all mappings from the specified map to this map view.
    *
    * @param map The map whose entries are inserted into this map view.
    * @throws Exception Thrown if the system cannot access the map.
    */
  @throws[Exception]
  def putAll(map: util.Map[K, V]): Unit = this.map.putAll(map)

  /**
    * Deletes the value for the given key.
    *
    * @param key The key for which the value is deleted.
    * @throws Exception Thrown if the system cannot access the map.
    */
  @throws[Exception]
  def remove(key: K): Unit = map.remove(key)

  /**
    * Checks if the map view contains a value for a given key.
    *
    * @param key The key to check.
    * @return True if there exists a value for the given key, false otherwise.
    * @throws Exception Thrown if the system cannot access the map.
    */
  @throws[Exception]
  def contains(key: K): Boolean = map.containsKey(key)

  /**
    * Returns all entries of the map view.
    *
    * @return An iterable of all the key-value pairs in the map view.
    * @throws Exception Thrown if the system cannot access the map.
    */
  @throws[Exception]
  def entries: JIterable[util.Map.Entry[K, V]] = map.entrySet()

  /**
    * Returns all the keys in the map view.
    *
    * @return An iterable of all the keys in the map.
    * @throws Exception Thrown if the system cannot access the map.
    */
  @throws[Exception]
  def keys: JIterable[K] = map.keySet()

  /**
    * Returns all the values in the map view.
    *
    * @return An iterable of all the values in the map.
    * @throws Exception Thrown if the system cannot access the map.
    */
  @throws[Exception]
  def values: JIterable[V] = map.values()

  /**
    * Returns an iterator over all entries of the map view.
    *
    * @return An iterator over all the mappings in the map.
    * @throws Exception Thrown if the system cannot access the map.
    */
  @throws[Exception]
  def iterator: util.Iterator[util.Map.Entry[K, V]] = map.entrySet().iterator()

  /**
    * Removes all entries of this map.
    */
  override def clear(): Unit = map.clear()

  override def equals(other: Any): Boolean = other match {
    case that: MapView[K, V] =>
        map.equals(that.map)
    case _ => false
  }

  override def hashCode(): Int = map.hashCode()
}
