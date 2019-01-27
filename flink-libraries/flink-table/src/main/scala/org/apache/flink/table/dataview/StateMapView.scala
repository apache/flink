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

import java.util
import java.lang.{Iterable => JIterable}
import java.util.{Iterator => JIterator, Map => JMap}

import org.apache.flink.runtime.state.subkeyed.{SubKeyedMapState, SubKeyedValueState}
import org.apache.flink.runtime.state.keyed.{KeyedMapState, KeyedValueState}
import org.apache.flink.table.api.dataview.MapView
import org.apache.flink.table.dataview.NullAwareMapIterator.NullMapEntry

// -------------------------------------------------------------------------------------
//                                State MapView
// -------------------------------------------------------------------------------------

trait StateMapView[K, MK, MV] extends MapView[MK, MV] with StateDataView[K]

/**
  * [[SubKeyedStateMapView]] is a [[SubKeyedMapState]] with [[MapView]] interface which works on
  * window aggregate.
  */
class SubKeyedStateMapView[K, N, MK, MV](state: SubKeyedMapState[K, N, MK, MV])
  extends StateMapView[K, MK, MV] {

  private var key: K = _
  private var namespace: N = _

  override def setCurrentKey(key: K): Unit = {
    this.key = key
  }

  def setCurrentNamespace(namespace: N): Unit = {
    this.namespace = namespace
  }

  override def get(mapKey: MK): MV = state.get(key, namespace, mapKey)

  override def put(mapKey: MK, mapValue: MV): Unit = state.add(key, namespace, mapKey, mapValue)

  override def putAll(map: JMap[MK, MV]): Unit = state.addAll(key, namespace, map)

  override def remove(mapKey: MK): Unit = state.remove(key, namespace, mapKey)

  override def contains(mapKey: MK): Boolean = state.contains(key, namespace, mapKey)

  override def entries: JIterable[JMap.Entry[MK, MV]] = {
    new JIterable[JMap.Entry[MK, MV]]() {
      override def iterator(): util.Iterator[JMap.Entry[MK, MV]] = state.iterator(key, namespace)
    }
  }

  override def keys: JIterable[MK] = new JIterable[MK]() {
    override def iterator(): util.Iterator[MK] = new util.Iterator[MK] {
      val iter: util.Iterator[JMap.Entry[MK, MV]] = state.iterator(key, namespace)

      override def next(): MK = iter.next().getKey

      override def hasNext: Boolean = iter.hasNext
    }
  }

  override def values: JIterable[MV] = new JIterable[MV]() {
    override def iterator(): util.Iterator[MV] = new util.Iterator[MV] {
      val iter: util.Iterator[JMap.Entry[MK, MV]] = state.iterator(key, namespace)

      override def next(): MV = iter.next().getValue

      override def hasNext: Boolean = iter.hasNext
    }
  }

  override def iterator: util.Iterator[JMap.Entry[MK, MV]] = state.iterator(key, namespace)

  override def clear(): Unit = state.remove(key, namespace)
}

/**
  * [[KeyedStateMapView]] is a [[KeyedMapState]] with [[MapView]] interface which works on
  * group aggregate.
  */
class KeyedStateMapView[K, MK, MV](state: KeyedMapState[K, MK, MV])
  extends StateMapView[K, MK, MV] {

  private var stateKey: K = null.asInstanceOf[K]

  override def setCurrentKey(key: K): Unit = {
    this.stateKey = key
  }

  override def get(key: MK): MV = {
    state.get(stateKey, key)
  }

  override def put(key: MK, value: MV): Unit = {
    state.add(stateKey, key, value)
  }

  override def putAll(map: JMap[MK, MV]): Unit = {
    state.addAll(stateKey, map)
  }

  override def remove(key: MK): Unit = {
    state.remove(stateKey, key)
  }

  override def contains(key: MK): Boolean = {
    state.contains(stateKey, key)
  }

  override def entries: JIterable[JMap.Entry[MK, MV]] = {
    new JIterable[JMap.Entry[MK, MV]] {
      override def iterator(): util.Iterator[JMap.Entry[MK, MV]] = state.iterator(stateKey)
    }
  }

  override def keys: JIterable[MK] = {
    new JIterable[MK] {
      override def iterator(): util.Iterator[MK] = new util.Iterator[MK] {
        val it: JIterator[JMap.Entry[MK, MV]] = state.iterator(stateKey)
        override def next(): MK = it.next().getKey
        override def hasNext: Boolean = it.hasNext
      }
    }
  }

  override def values: JIterable[MV] = {
    new JIterable[MV] {
      override def iterator(): util.Iterator[MV] = new util.Iterator[MV] {
        val it: JIterator[JMap.Entry[MK, MV]] = state.iterator(stateKey)
        override def next(): MV = it.next().getValue
        override def hasNext: Boolean = it.hasNext
      }
    }
  }

  override def iterator: util.Iterator[JMap.Entry[MK, MV]] = {
    state.iterator(stateKey)
  }

  override def clear(): Unit = state.remove(stateKey)
}

// -------------------------------------------------------------------------------------
//                                NullAware State MapView
// -------------------------------------------------------------------------------------


/**
  * [[NullAwareSubKeyedStateMapView]] is an implementation of [[MapView]] using
  * [[SubKeyedMapState]] and [[SubKeyedValueState]] which can handle null map key.
  */
class NullAwareSubKeyedStateMapView[K, N, MK, MV](
    mapState: SubKeyedMapState[K, N, MK, MV],
    nullState: SubKeyedValueState[K, N, MV])
  extends StateMapView[K, MK, MV] {

  private var key: K = _
  private var namespace: N = _

  override def setCurrentKey(key: K): Unit = {
    this.key = key
  }

  def setCurrentNamespace(namespace: N): Unit = {
    this.namespace = namespace
  }

  override def get(mapKey: MK): MV = {
    if (mapKey == null) {
      nullState.get(key, namespace)
    } else {
      mapState.get(key, namespace, mapKey)
    }
  }

  override def put(mapKey: MK, mapValue: MV): Unit = {
    if (mapKey == null) {
      nullState.put(key, namespace, mapValue)
    } else {
      mapState.add(key, namespace, mapKey, mapValue)
    }
  }

  override def putAll(map: JMap[MK, MV]): Unit = {
    val iter = map.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val mapKey = entry.getKey
      val mapValue = entry.getValue
      put(mapKey, mapValue)
    }
  }

  override def remove(mapKey: MK): Unit = {
    if (mapKey == null) {
      nullState.remove(key, namespace)
    } else {
      mapState.remove(key, namespace, mapKey)
    }
  }

  override def contains(mapKey: MK): Boolean = {
    if (mapKey == null) {
      nullState.contains(key, namespace)
    } else {
      mapState.contains(key, namespace, mapKey)
    }
  }

  override def entries: JIterable[JMap.Entry[MK, MV]] = {
    new JIterable[JMap.Entry[MK, MV]]() {
      override def iterator(): util.Iterator[JMap.Entry[MK, MV]] =
        NullAwareSubKeyedStateMapView.this.iterator
    }
  }

  override def keys: JIterable[MK] = new JIterable[MK]() {
    override def iterator(): util.Iterator[MK] = new util.Iterator[MK] {
      val iter: util.Iterator[JMap.Entry[MK, MV]] = NullAwareSubKeyedStateMapView.this.iterator

      override def next(): MK = iter.next().getKey

      override def hasNext: Boolean = iter.hasNext
    }
  }

  override def values: JIterable[MV] = new JIterable[MV]() {
    override def iterator(): util.Iterator[MV] = new util.Iterator[MV] {
      val iter: util.Iterator[JMap.Entry[MK, MV]] = NullAwareSubKeyedStateMapView.this.iterator

      override def next(): MV = iter.next().getValue

      override def hasNext: Boolean = iter.hasNext
    }
  }

  override def iterator: util.Iterator[JMap.Entry[MK, MV]] = {
    new NullAwareMapIterator[MK, MV](
      mapState.iterator(key, namespace),
      new NullMapEntry[MK, MV] {
        override def remove(): Unit = nullState.remove(key, namespace)

        override def getValue: MV = nullState.get(key, namespace)

        override def setValue(value: MV): MV = {
          nullState.put(key, namespace, value)
          value
        }
      })
  }

  override def clear(): Unit = {
    mapState.remove(key, namespace)
    nullState.remove(key, namespace)
  }
}

/**
  * [[NullAwareKeyedStateMapView]] is an implementation of [[MapView]] using [[KeyedStateMapView]]
  * and [[KeyedValueState]] which can handle null map key.
  */
class NullAwareKeyedStateMapView[K, MK, MV](
    mapState: KeyedMapState[K, MK, MV],
    nullState: KeyedValueState[K, MV])
  extends StateMapView[K, MK, MV] {

  private var stateKey: K = null.asInstanceOf[K]

  override def setCurrentKey(key: K): Unit = {
    this.stateKey = key
  }

  override def get(key: MK): MV = {
    if (key == null) {
      nullState.get(stateKey)
    } else {
      mapState.get(stateKey, key)
    }
  }

  override def put(key: MK, value: MV): Unit = {
    if (key == null) {
      nullState.put(stateKey, value)
    } else {
      mapState.add(stateKey, key, value)
    }
  }

  override def putAll(map: JMap[MK, MV]): Unit = {
    val iter = map.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val key = entry.getKey
      val value = entry.getValue
      put(key, value)
    }
  }

  override def remove(key: MK): Unit = {
    if (key == null) {
      nullState.remove(stateKey)
    } else {
      mapState.remove(stateKey, key)
    }
  }

  override def contains(key: MK): Boolean = {
    if (key == null) {
      nullState.contains(stateKey)
    } else {
      mapState.contains(stateKey, key)
    }
  }

  override def entries: JIterable[JMap.Entry[MK, MV]] = {
    new JIterable[JMap.Entry[MK, MV]] {
      override def iterator(): util.Iterator[JMap.Entry[MK, MV]] =
        NullAwareKeyedStateMapView.this.iterator
    }
  }

  override def keys: JIterable[MK] = {
    new JIterable[MK] {
      override def iterator(): util.Iterator[MK] = new util.Iterator[MK] {
        val it: JIterator[JMap.Entry[MK, MV]] = NullAwareKeyedStateMapView.this.iterator
        override def next(): MK = it.next().getKey
        override def hasNext: Boolean = it.hasNext
      }
    }
  }

  override def values: JIterable[MV] = {
    new JIterable[MV] {
      override def iterator(): util.Iterator[MV] = new util.Iterator[MV] {
        val it: JIterator[JMap.Entry[MK, MV]] = NullAwareKeyedStateMapView.this.iterator
        override def next(): MV = it.next().getValue
        override def hasNext: Boolean = it.hasNext
      }
    }
  }

  override def iterator: util.Iterator[JMap.Entry[MK, MV]] = {
    new NullAwareMapIterator[MK, MV](
      mapState.iterator(stateKey),
      new NullMapEntry[MK, MV] {
        override def remove(): Unit = nullState.remove(stateKey)

        override def getValue: MV = nullState.get(stateKey)

        override def setValue(value: MV): MV = {
          nullState.put(stateKey, value)
          value
        }
      })
  }

  override def clear(): Unit = {
    mapState.remove(stateKey)
    nullState.remove(stateKey)
  }
}
