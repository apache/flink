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

import java.lang.{Iterable => JIterable}
import java.util
import java.util.function.BiFunction

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils._
import org.apache.flink.table.api.dataview.MapView
import org.apache.flink.table.dataview.MapViewTypeInfo
import org.apache.flink.table.functions.AggregateFunction


/** The initial accumulator for Collect aggregate function */
class CollectAccumulator[E](var f0:MapView[E, Integer]) {
  def this() {
    this(null)
  }

  def canEqual(a: Any) = a.isInstanceOf[CollectAccumulator[E]]

  override def equals(that: Any): Boolean =
    that match {
      case that: CollectAccumulator[E] => that.canEqual(this) && this.f0 == that.f0
      case _ => false
    }
}

abstract class CollectAggFunction[E]
  extends AggregateFunction[util.Map[E, Integer], CollectAccumulator[E]] {

  override def createAccumulator(): CollectAccumulator[E] = {
    val acc = new CollectAccumulator[E](new MapView[E, Integer](
      getValueTypeInfo.asInstanceOf[TypeInformation[E]], BasicTypeInfo.INT_TYPE_INFO))
    acc
  }

  def accumulate(accumulator: CollectAccumulator[E], value: E): Unit = {
    if (value != null) {
      if (accumulator.f0.contains(value)) {
        accumulator.f0.put(value, accumulator.f0.get(value) + 1)
      } else {
        accumulator.f0.put(value, 1)
      }
    }
  }

  override def getValue(accumulator: CollectAccumulator[E]): util.Map[E, Integer] = {
    val iterator = accumulator.f0.iterator
    if (iterator.hasNext) {
      val map = new util.HashMap[E, Integer]()
      while (iterator.hasNext) {
        val entry = iterator.next()
        map.put(entry.getKey, entry.getValue)
      }
      map
    } else {
      null.asInstanceOf[util.Map[E, Integer]]
    }
  }

  def resetAccumulator(acc: CollectAccumulator[E]): Unit = {
    acc.f0.clear()
  }

  override def getAccumulatorType: TypeInformation[CollectAccumulator[E]] = {
    val clazz = classOf[CollectAccumulator[E]]
    val pojoFields = new util.ArrayList[PojoField]
    pojoFields.add(new PojoField(clazz.getDeclaredField("f0"),
      new MapViewTypeInfo[E, Integer](
        getValueTypeInfo.asInstanceOf[TypeInformation[E]], BasicTypeInfo.INT_TYPE_INFO)))
    new PojoTypeInfo[CollectAccumulator[E]](clazz, pojoFields)
  }

  def merge(acc: CollectAccumulator[E], its: JIterable[CollectAccumulator[E]]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      val mapViewIterator = iter.next().f0.iterator
      while (mapViewIterator.hasNext) {
        val entry = mapViewIterator.next()
        val k = entry.getKey
        val oldValue = acc.f0.get(k)
        if (oldValue == null) {
          acc.f0.put(k, entry.getValue)
        } else {
          acc.f0.put(k, entry.getValue + oldValue)
        }
      }
    }
  }

  def retract(acc: CollectAccumulator[E], value: E): Unit = {
    if (value != null) {
      val count = acc.f0.get(value)
      if (count == 1) {
        acc.f0.remove(value)
      } else {
        acc.f0.put(value, count - 1)
      }
    }
  }

  def getValueTypeInfo: TypeInformation[_]
}

class IntCollectAggFunction extends CollectAggFunction[Int] {
  override def getValueTypeInfo: TypeInformation[_] = BasicTypeInfo.INT_TYPE_INFO
}

class LongCollectAggFunction extends CollectAggFunction[Long] {
  override def getValueTypeInfo: TypeInformation[_] = BasicTypeInfo.LONG_TYPE_INFO
}

class StringCollectAggFunction extends CollectAggFunction[String] {
  override def getValueTypeInfo: TypeInformation[_] = BasicTypeInfo.STRING_TYPE_INFO
}

class ByteCollectAggFunction extends CollectAggFunction[Byte] {
  override def getValueTypeInfo: TypeInformation[_] = BasicTypeInfo.BYTE_TYPE_INFO
}

class ShortCollectAggFunction extends CollectAggFunction[Short] {
  override def getValueTypeInfo: TypeInformation[_] = BasicTypeInfo.SHORT_TYPE_INFO
}

class FloatCollectAggFunction extends CollectAggFunction[Float] {
  override def getValueTypeInfo: TypeInformation[_] = BasicTypeInfo.FLOAT_TYPE_INFO
}

class DoubleCollectAggFunction extends CollectAggFunction[Double] {
  override def getValueTypeInfo: TypeInformation[_] = BasicTypeInfo.DOUBLE_TYPE_INFO
}

class BooleanCollectAggFunction extends CollectAggFunction[Boolean] {
  override def getValueTypeInfo: TypeInformation[_] = BasicTypeInfo.BOOLEAN_TYPE_INFO
}

class ObjectCollectAggFunction extends CollectAggFunction[Object] {
  override def getValueTypeInfo: TypeInformation[_] = new GenericTypeInfo[Object](classOf[Object])
}
