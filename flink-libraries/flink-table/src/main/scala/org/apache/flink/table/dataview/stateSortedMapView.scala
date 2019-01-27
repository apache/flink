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
import java.util.Map

import org.apache.flink.runtime.state.keyed.KeyedSortedMapState
import org.apache.flink.table.api.dataview.SortedMapView

// -------------------------------------------------------------------------------------
//                                State SortedMapView
// -------------------------------------------------------------------------------------

trait StateSortedMapView[K, MK, MV] extends SortedMapView[MK, MV] with StateDataView[K]

/**
  * [[SubKeyedStateMapView]] is a [[KeyedSortedMapState]] with [[SortedMapView]] interface
  * which works on group aggregate.
  */
class KeyedStateSortedMapView[K, MK, MV](state: KeyedSortedMapState[K, MK, MV])
  extends StateSortedMapView[K, MK, MV] {

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

  override def putAll(map: util.Map[MK, MV]): Unit = {
    state.addAll(stateKey, map)
  }

  override def remove(key: MK): Unit = {
    state.remove(stateKey, key)
  }

  override def contains(key: MK): Boolean = {
    state.contains(stateKey, key)
  }

  override def entries: JIterable[Map.Entry[MK, MV]] = {
    new JIterable[Map.Entry[MK, MV]] {
      override def iterator(): util.Iterator[Map.Entry[MK, MV]] =
        new util.Iterator[Map.Entry[MK, MV]] {
          val it = state.iterator(stateKey)
          override def next(): Map.Entry[MK, MV] = it.next()
          override def hasNext: Boolean = it.hasNext
        }
    }
  }

  override def keys: JIterable[MK] = {
    new JIterable[MK] {
      override def iterator(): util.Iterator[MK] = new util.Iterator[MK] {
        val it = state.iterator(stateKey)
        override def next(): MK = it.next().getKey
        override def hasNext: Boolean = it.hasNext
      }
    }
  }

  override def values: JIterable[MV] = {
    new JIterable[MV] {
      override def iterator(): util.Iterator[MV] = new util.Iterator[MV] {
        val it = state.iterator(stateKey)
        override def next(): MV = it.next().getValue
        override def hasNext: Boolean = it.hasNext
      }
    }
  }

  override def iterator: util.Iterator[Map.Entry[MK, MV]] = {
    state.iterator(stateKey)
  }

  override def firstKey: MK = {
    val it = state.iterator(stateKey)
    if (it.hasNext) {
      it.next().getKey
    } else {
      null.asInstanceOf[MK]
    }
  }

  override def firstEntry: Map.Entry[MK, MV] = {
    val it = state.iterator(stateKey)
    if (it.hasNext) {
      it.next()
    } else {
      null.asInstanceOf[Map.Entry[MK, MV]]
    }
  }

  override def headEntries(var1: MK): JIterable[Map.Entry[MK, MV]] = {
    new JIterable[Map.Entry[MK, MV]] {
      override def iterator(): util.Iterator[Map.Entry[MK, MV]] =
        new util.Iterator[Map.Entry[MK, MV]] {
          val it = state.headIterator(stateKey, var1)
          override def next(): Map.Entry[MK, MV] = it.next()
          override def hasNext: Boolean = it.hasNext
        }
    }
  }

  override def tailEntries(var1: MK): JIterable[Map.Entry[MK, MV]] = {
    new JIterable[Map.Entry[MK, MV]] {
      override def iterator(): util.Iterator[Map.Entry[MK, MV]] =
        new util.Iterator[Map.Entry[MK, MV]] {
          val it = state.tailIterator(stateKey, var1)
          override def next(): Map.Entry[MK, MV] = it.next()
          override def hasNext: Boolean = it.hasNext
        }
    }
  }

  override def subEntries(var1: MK, var2: MK): JIterable[Map.Entry[MK, MV]] = {
    new JIterable[Map.Entry[MK, MV]] {
      override def iterator(): util.Iterator[Map.Entry[MK, MV]] =
        new util.Iterator[Map.Entry[MK, MV]] {
          val it = state.subIterator(stateKey, var1, var2)
          override def next(): Map.Entry[MK, MV] = it.next()
          override def hasNext: Boolean = it.hasNext
        }
    }
  }

  override def clear(): Unit = {
    state.remove(stateKey)
  }
}
