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

package org.apache.flink.table.functions.aggfunctions

import java.lang.{Long => JLong}
import java.lang.{Iterable => JIterable}
import java.util.{Map => JMap}

import org.apache.flink.table.api.dataview.MapView

/**
  * The base class for accumulator wrapper when applying distinct aggregation.
  * @param realAcc the actual accumulator which gets invoke after distinct filter.
  * @param mapView the [[MapView]] element used to store the distinct filter hash map.
  * @tparam E the element type for the distinct filter hash map.
  * @tparam ACC the accumulator type for the realAcc.
  */
class DistinctAccumulator[E, ACC](var realAcc: ACC, var mapView: MapView[E, JLong]) {
  def this() {
    this(null.asInstanceOf[ACC], new MapView[E, JLong]())
  }

  def this(realAcc: ACC) {
    this(realAcc, new MapView[E, JLong]())
  }

  def getRealAcc: ACC = realAcc

  def canEqual(a: Any): Boolean = a.isInstanceOf[DistinctAccumulator[E, ACC]]

  override def equals(that: Any): Boolean =
    that match {
      case that: DistinctAccumulator[E, ACC] => that.canEqual(this) &&
        this.mapView == that.mapView
      case _ => false
    }

  def add(element: E): Boolean = {
    if (element != null) {
      val currentVal = mapView.get(element)
      if (currentVal != null) {
        mapView.put(element, currentVal + 1L)
        false
      } else {
        mapView.put(element, 1L)
        true
      }
    } else {
      false
    }
  }

  def add(element: E, count: JLong): Boolean = {
    if (element != null) {
      val currentVal = mapView.get(element)
      if (currentVal != null) {
        mapView.put(element, currentVal + count)
        false
      } else {
        mapView.put(element, count)
        true
      }
    } else {
      false
    }
  }

  def remove(element: E): Boolean = {
    if (element != null) {
      val count = mapView.get(element)
      if (count == 1) {
        mapView.remove(element)
        true
      } else {
        mapView.put(element, count - 1L)
        false
      }
    } else {
      false
    }
  }

  def reset(): Unit = {
    mapView.clear()
  }

  def elements(): JIterable[JMap.Entry[E, JLong]] = {
    mapView.map.entrySet()
  }
}
