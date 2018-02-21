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

import java.util

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.{PojoField, PojoTypeInfo}
import org.apache.flink.table.api.dataview.MapView
import org.apache.flink.table.dataview.MapViewTypeInfo
import org.apache.flink.table.functions.AggregateFunction

class DistinctAccumulator[E, ACC] (var mapView: MapView[E, Integer], var realAcc: ACC) {
  def this() {
    this(null, null.asInstanceOf[ACC])
  }

  def getRealAcc: ACC = realAcc

  def canEqual(a: Any): Boolean = a.isInstanceOf[DistinctAccumulator[E, ACC]]

  override def equals(that: Any): Boolean =
    that match {
      case that: DistinctAccumulator[E, ACC] => that.canEqual(this) &&
        this.mapView == that.mapView
      case _ => false
    }

}

class DistinctAggDelegateFunction[E, ACC](elementTypeInfo: TypeInformation[_],
                                          var realAgg: AggregateFunction[_, ACC])
  extends AggregateFunction[util.Map[E, Integer], DistinctAccumulator[E, ACC]] {

  def getRealAgg: AggregateFunction[_, ACC] = realAgg

  override def createAccumulator(): DistinctAccumulator[E, ACC] = {
    new DistinctAccumulator[E, ACC](
      new MapView[E, Integer](
        elementTypeInfo.asInstanceOf[TypeInformation[E]],
        BasicTypeInfo.INT_TYPE_INFO),
      realAgg.createAccumulator())
  }

  def accumulate(acc: DistinctAccumulator[E, ACC], element: E): Boolean = {
    if (element != null) {
      if (acc.mapView.contains(element)) {
        acc.mapView.put(element, acc.mapView.get(element) + 1)
        false
      } else {
        acc.mapView.put(element, 1)
        true
      }
    } else {
      false
    }
  }

  def accumulate(acc: DistinctAccumulator[E, ACC], element: E, count: Int): Boolean = {
    if (element != null) {
      if (acc.mapView.contains(element)) {
        acc.mapView.put(element, acc.mapView.get(element) + count)
        false
      } else {
        acc.mapView.put(element, count)
        true
      }
    } else {
      false
    }
  }

  def retract(acc: DistinctAccumulator[E, ACC], element: E): Boolean = {
    if (element != null) {
      val count = acc.mapView.get(element)
      if (count == 1) {
        acc.mapView.remove(element)
        true
      } else {
        acc.mapView.put(element, count - 1)
        false
      }
    } else {
      false
    }
  }

  def resetAccumulator(acc: DistinctAccumulator[E, ACC]): Unit = {
    acc.mapView.clear()
  }

  override def getValue(acc: DistinctAccumulator[E, ACC]): util.Map[E, Integer] = {
    acc.mapView.map
  }

  override def getAccumulatorType: TypeInformation[DistinctAccumulator[E, ACC]] = {
    val clazz = classOf[DistinctAccumulator[E, ACC]]
    val pojoFields = new util.ArrayList[PojoField]
    pojoFields.add(new PojoField(clazz.getDeclaredField("mapView"),
      new MapViewTypeInfo[E, Integer](
        elementTypeInfo.asInstanceOf[TypeInformation[E]], BasicTypeInfo.INT_TYPE_INFO)))
    pojoFields.add(new PojoField(clazz.getDeclaredField("realAcc"),
      realAgg.getAccumulatorType))
    new PojoTypeInfo[DistinctAccumulator[E, ACC]](clazz, pojoFields)
  }
}
