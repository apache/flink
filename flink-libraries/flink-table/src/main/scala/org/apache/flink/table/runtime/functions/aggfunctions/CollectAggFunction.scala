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

package org.apache.flink.table.runtime.functions.aggfunctions

import java.lang.{Iterable => JIterable}
import java.util
import org.apache.flink.table.api.dataview.MapView
import org.apache.flink.table.api.functions.AggregateFunction
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types._
import org.apache.flink.table.typeutils.MapViewTypeInfo

import scala.collection.JavaConverters._

/** The initial accumulator for Collect aggregate function */
class CollectAccumulator[E](var map: MapView[E, Integer]) {
  def this() {
    this(null)
  }

  def canEqual(a: Any): Boolean = a.isInstanceOf[CollectAccumulator[E]]

  override def equals(that: Any): Boolean =
    that match {
      case that: CollectAccumulator[E] => that.canEqual(this) && this.map == that.map
      case _ => false
    }
}

class CollectAggFunction[E](valueType: DataType)
  extends AggregateFunction[util.Map[E, Integer], CollectAccumulator[E]] {

  override def createAccumulator(): CollectAccumulator[E] = {
    new CollectAccumulator[E](
      new MapView[E, Integer](valueType, DataTypes.INT))
  }

  def accumulate(accumulator: CollectAccumulator[E], value: E): Unit = {
    if (value != null) {
      val currVal = accumulator.map.get(value)
      if (currVal != null) {
        accumulator.map.put(value, currVal + 1)
      } else {
        accumulator.map.put(value, 1)
      }
    }
  }

  override def getValue(accumulator: CollectAccumulator[E]): util.Map[E, Integer] = {
    val iterator = accumulator.map.iterator
    if (iterator.hasNext) {
      val map = new util.HashMap[E, Integer]()
      while (iterator.hasNext) {
        val entry = iterator.next()
        map.put(entry.getKey, entry.getValue)
      }
      map
    } else {
      Map[E, Integer]().asJava
    }
  }

  def resetAccumulator(acc: CollectAccumulator[E]): Unit = {
    acc.map.clear()
  }

  override def getAccumulatorType: DataType = {
    DataTypes.pojoBuilder(classOf[CollectAccumulator[E]]).field("map",
      new MapViewTypeInfo(valueType, DataTypes.INT)).build()
  }

  override def getResultType: DataType =
    DataTypes.createMapType(valueType, DataTypes.INT)

  def merge(acc: CollectAccumulator[E], its: JIterable[CollectAccumulator[E]]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      val mapViewIterator = iter.next().map.iterator
      while (mapViewIterator.hasNext) {
        val entry = mapViewIterator.next()
        val k = entry.getKey
        val oldValue = acc.map.get(k)
        if (oldValue == null) {
          acc.map.put(k, entry.getValue)
        } else {
          acc.map.put(k, entry.getValue + oldValue)
        }
      }
    }
  }

  def retract(acc: CollectAccumulator[E], value: E): Unit = {
    if (value != null) {
      val count = acc.map.get(value)
      if (count == 1) {
        acc.map.remove(value)
      } else {
        acc.map.put(value, count - 1)
      }
    }
  }

  override def getUserDefinedInputTypes(signature: Array[Class[_]]): Array[DataType] = {
    if (signature.length == 1) {
      Array[DataType](valueType)
    } else {
      throw new UnsupportedOperationException
    }
  }
}
