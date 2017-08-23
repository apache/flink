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
  * MapView provides Map functionality for accumulators used by user-defined aggregate functions
  * [[org.apache.flink.table.functions.AggregateFunction]].
  *
  * A MapView can be backed by a Java HashMap or a state backend, depending on the context in
  * which the function is used.
  *
  * At runtime `MapView` will be replaced by a [[org.apache.flink.table.dataview.StateMapView]]
  * when use state backend.
  *
  * Example:
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
    @transient private[flink] val valueTypeInfo: TypeInformation[V])
  extends DataView {

  def this() = this(null, null)

  private[flink] var map = new util.HashMap[K, V]()

  /**
    * Returns the value to which the specified key is mapped, or { @code null } if this map
    * contains no mapping for the key.
    *
    * @param key The key of the mapping.
    * @return The value of the mapping with the given key.
    * @throws Exception Thrown if the system cannot get data.
    */
  @throws[Exception]
  def get(key: K): V = map.get(key)

  /**
    * Put a value with the given key into the map.
    *
    * @param key   The key of the mapping.
    * @param value The new value of the mapping.
    * @throws Exception Thrown if the system cannot put data.
    */
  @throws[Exception]
  def put(key: K, value: V): Unit = map.put(key, value)

  /**
    * Copies all of the mappings from the specified map to this map view.
    *
    * @param map The mappings to be stored in this map.
    * @throws Exception Thrown if the system cannot access the map.
    */
  @throws[Exception]
  def putAll(map: util.Map[K, V]): Unit = this.map.putAll(map)

  /**
    * Deletes the mapping of the given key.
    *
    * @param key The key of the mapping.
    * @throws Exception Thrown if the system cannot access the map.
    */
  @throws[Exception]
  def remove(key: K): Unit = map.remove(key)

  /**
    * Returns whether there exists the given mapping.
    *
    * @param key The key of the mapping.
    * @return True if there exists a mapping whose key equals to the given key.
    * @throws Exception Thrown if the system cannot access the map.
    */
  @throws[Exception]
  def contains(key: K): Boolean = map.containsKey(key)

  /**
    * Returns all the mappings in the map.
    *
    * @return An iterable view of all the key-value pairs in the map.
    * @throws Exception Thrown if the system cannot access the map.
    */
  @throws[Exception]
  def entries: JIterable[util.Map.Entry[K, V]] = map.entrySet()

  /**
    * Returns all the keys in the map.
    *
    * @return An iterable view of all the keys in the map.
    * @throws Exception Thrown if the system cannot access the map.
    */
  @throws[Exception]
  def keys: JIterable[K] = map.keySet()

  /**
    * Returns all the values in the map.
    *
    * @return An iterable view of all the values in the map.
    * @throws Exception Thrown if the system cannot access the map.
    */
  @throws[Exception]
  def values: JIterable[V] = map.values()

  /**
    * Iterates over all the mappings in the map.
    *
    * @return An iterator over all the mappings in the map.
    * @throws Exception Thrown if the system cannot access the map.
    */
  @throws[Exception]
  def iterator: util.Iterator[util.Map.Entry[K, V]] = map.entrySet().iterator()

  /**
    * Removes all of the mappings from this map (optional operation).
    *
    * The map will be empty after this call returns.
    */
  override def clear(): Unit = map.clear()

  override def equals(other: Any): Boolean = other match {
    case that: MapView[_, _] =>
        map.equals(that.map)
    case _ => false
  }

  override def hashCode(): Int = map.hashCode()
}
