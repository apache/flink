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
import org.apache.flink.types.Row

/**
  * The base class for accumulator wrapper when applying distinct aggregation.
  * @param realAcc the actual accumulator which gets invoke after distinct filter.
  * @param distinctValueMap the [[MapView]] element used to store the distinct filter hash map.
  * @tparam E the element type for the distinct filter hash map.
  * @tparam ACC the accumulator type for the realAcc.
  */
class DistinctAccumulator[E <: AnyRef, ACC](
    var realAcc: ACC,
    var distinctValueMap: MapView[Row, JLong]) {

  def this() {
    this(null.asInstanceOf[ACC], new MapView[Row, JLong]())
  }

  def this(realAcc: ACC) {
    this(realAcc, new MapView[Row, JLong]())
  }

  def getRealAcc: ACC = realAcc

  def canEqual(a: Any): Boolean = a.isInstanceOf[DistinctAccumulator[E, ACC]]

  override def equals(that: Any): Boolean =
    that match {
      case that: DistinctAccumulator[E, ACC] => that.canEqual(this) &&
        this.distinctValueMap == that.distinctValueMap
      case _ => false
    }

  def add(element: E): Boolean = {
    val wrappedElement = Row.of(element)
    val currentVal = distinctValueMap.get(wrappedElement)
    if (currentVal != null) {
      distinctValueMap.put(wrappedElement, currentVal + 1L)
      false
    } else {
      distinctValueMap.put(wrappedElement, 1L)
      true
    }
  }

  def add(element: E, count: JLong): Boolean = {
    val wrappedElement = Row.of(element)
    val currentVal = distinctValueMap.get(wrappedElement)
    if (currentVal != null) {
      distinctValueMap.put(wrappedElement, currentVal + count)
      false
    } else {
      distinctValueMap.put(wrappedElement, count)
      true
    }
  }

  def remove(element: E): Boolean = {
    val wrappedElement = Row.of(element)
    val count = distinctValueMap.get(wrappedElement)
    if (count == 1) {
      distinctValueMap.remove(wrappedElement)
      true
    } else {
      distinctValueMap.put(wrappedElement, count - 1L)
      false
    }
  }

  def reset(): Unit = {
    distinctValueMap.clear()
  }

  def elements(): JIterable[JMap.Entry[Row, JLong]] = {
    distinctValueMap.map.entrySet()
  }
}
