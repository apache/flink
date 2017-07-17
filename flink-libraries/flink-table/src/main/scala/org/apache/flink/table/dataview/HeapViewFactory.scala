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

package org.apache.flink.table.dataview

import java.lang.{Iterable => JIterable}
import java.util

import org.apache.flink.api.common.state.{ListStateDescriptor, MapStateDescriptor, StateDescriptor}
import org.apache.flink.table.api.dataview.{ListView, MapView}

/**
  * Heap view factory to create [[HeapListView]] or [[HeapMapView]].
  *
  * @param accConfig Accumulator config.
  */
class HeapViewFactory(accConfig: Map[String, StateDescriptor[_, _]])
  extends DataViewFactory(accConfig) {

  override protected def createListView[T](id: String): ListView[T] = new HeapListView[T]

  override protected def createMapView[K, V](id: String): MapView[K, V] = new HeapMapView[K, V]
}

class HeapListView[T] extends ListView[T] {

  val list = new util.ArrayList[T]()

  def this(t: util.List[T]) = {
    this()
    list.addAll(t)
  }

  override def get: JIterable[T] = {
    if (!list.isEmpty) {
      list
    } else {
      null
    }
  }

  override def add(value: T): Unit = list.add(value)

  override def clear(): Unit = list.clear()
}

class HeapMapView[K, V] extends MapView[K, V] {

  val map = new util.HashMap[K, V]()

  def this(m: util.Map[K, V]) = {
    this()
    map.putAll(m)
  }

  override def get(key: K): V = map.get(key)

  override def put(key: K, value: V): Unit = map.put(key, value)

  override def putAll(map: util.Map[K, V]): Unit = this.map.putAll(map)

  override def remove(key: K): Unit = map.remove(key)

  override def contains(key: K): Boolean = map.containsKey(key)

  override def entries: JIterable[util.Map.Entry[K, V]] = map.entrySet()

  override def keys: JIterable[K] = map.keySet()

  override def values: JIterable[V] = map.values()

  override def iterator: util.Iterator[util.Map.Entry[K, V]] = map.entrySet().iterator()

  override def clear(): Unit = map.clear()
}
