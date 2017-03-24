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

import java.math.BigDecimal
import java.util.{HashMap => JHashMap, List => JList}

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.{MapTypeInfo, TupleTypeInfo}
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}

/** The initial accumulator for Min with retraction aggregate function */
class MinWithRetractAccumulator[T] extends JTuple2[T, JHashMap[T, Long]] with Accumulator

/**
  * Base class for built-in Min with retraction aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class MinWithRetractAggFunction[T](implicit ord: Ordering[T])
  extends AggregateFunction[T] {

  override def createAccumulator(): Accumulator = {
    val acc = new MinWithRetractAccumulator[T]
    acc.f0 = getInitValue //min
    acc.f1 = new JHashMap[T, Long]() //store the count for each value
    acc
  }

  override def accumulate(accumulator: Accumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[T]
      val a = accumulator.asInstanceOf[MinWithRetractAccumulator[T]]

      if (a.f1.size() == 0 || (ord.compare(a.f0, v) > 0)) {
        a.f0 = v
      }

      if (!a.f1.containsKey(v)) {
        a.f1.put(v, 1L)
      } else {
        var count = a.f1.get(v)
        count += 1L
        a.f1.put(v, count)
      }
    }
  }

  override def retract(accumulator: Accumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[T]
      val a = accumulator.asInstanceOf[MinWithRetractAccumulator[T]]

      var count = a.f1.get(v)
      count -= 1L
      if (count == 0) {
        //remove the key v from the map if the number of appearance of the value v is 0
        a.f1.remove(v)
        //if the total count is 0, we could just simply set the f0(min) to the initial value
        if (a.f1.size() == 0) {
          a.f0 = getInitValue
          return
        }
        //if v is the current min value, we have to iterate the map to find the 2nd smallest
        // value to replace v as the min value
        if (v == a.f0) {
          val iterator = a.f1.keySet().iterator()
          var key = iterator.next()
          a.f0 = key
          while (iterator.hasNext()) {
            key = iterator.next()
            if (ord.compare(a.f0, key) > 0) {
              a.f0 = key
            }
          }
        }
      } else {
        a.f1.put(v, count)
      }
    }

  }

  override def getValue(accumulator: Accumulator): T = {
    val a = accumulator.asInstanceOf[MinWithRetractAccumulator[T]]
    if (a.f1.size() != 0) {
      a.f0
    } else {
      null.asInstanceOf[T]
    }
  }

  override def merge(accumulators: JList[Accumulator]): Accumulator = {
    val ret = accumulators.get(0).asInstanceOf[MinWithRetractAccumulator[T]]
    var i: Int = 1
    while (i < accumulators.size()) {
      val a = accumulators.get(i).asInstanceOf[MinWithRetractAccumulator[T]]
      if (a.f1.size() != 0) {
        // set min element
        if (ord.compare(ret.f0, a.f0) > 0) {
          ret.f0 = a.f0
        }
        // merge the count for each key
        val iterator = a.f1.keySet().iterator()
        while (iterator.hasNext()) {
          val key = iterator.next()
          if (ret.f1.containsKey(key)) {
            ret.f1.put(key, ret.f1.get(key) + a.f1.get(key))
          } else {
            ret.f1.put(key, a.f1.get(key))
          }
        }
      }
      i += 1
    }
    ret
  }

  override def resetAccumulator(accumulator: Accumulator): Unit = {
    accumulator.asInstanceOf[MinWithRetractAccumulator[T]].f0 = getInitValue
    accumulator.asInstanceOf[MinWithRetractAccumulator[T]].f1.clear()
  }

  override def getAccumulatorType(): TypeInformation[_] = {
    new TupleTypeInfo(
      new MinWithRetractAccumulator[T].getClass,
      getValueTypeInfo,
      new MapTypeInfo(getValueTypeInfo, BasicTypeInfo.LONG_TYPE_INFO))
  }

  def getInitValue: T

  def getValueTypeInfo: TypeInformation[_]
}

/**
  * Built-in Byte Min with retraction aggregate function
  */
class ByteMinWithRetractAggFunction extends MinWithRetractAggFunction[Byte] {
  override def getInitValue: Byte = 0.toByte
  override def getValueTypeInfo = BasicTypeInfo.BYTE_TYPE_INFO
}

/**
  * Built-in Short Min with retraction aggregate function
  */
class ShortMinWithRetractAggFunction extends MinWithRetractAggFunction[Short] {
  override def getInitValue: Short = 0.toShort
  override def getValueTypeInfo = BasicTypeInfo.SHORT_TYPE_INFO
}

/**
  * Built-in Int Min with retraction aggregate function
  */
class IntMinWithRetractAggFunction extends MinWithRetractAggFunction[Int] {
  override def getInitValue: Int = 0
  override def getValueTypeInfo = BasicTypeInfo.INT_TYPE_INFO
}

/**
  * Built-in Long Min with retraction aggregate function
  */
class LongMinWithRetractAggFunction extends MinWithRetractAggFunction[Long] {
  override def getInitValue: Long = 0L
  override def getValueTypeInfo = BasicTypeInfo.LONG_TYPE_INFO
}

/**
  * Built-in Float Min with retraction aggregate function
  */
class FloatMinWithRetractAggFunction extends MinWithRetractAggFunction[Float] {
  override def getInitValue: Float = 0.0f
  override def getValueTypeInfo = BasicTypeInfo.FLOAT_TYPE_INFO
}

/**
  * Built-in Double Min with retraction aggregate function
  */
class DoubleMinWithRetractAggFunction extends MinWithRetractAggFunction[Double] {
  override def getInitValue: Double = 0.0d
  override def getValueTypeInfo = BasicTypeInfo.DOUBLE_TYPE_INFO
}

/**
  * Built-in Boolean Min with retraction aggregate function
  */
class BooleanMinWithRetractAggFunction extends MinWithRetractAggFunction[Boolean] {
  override def getInitValue: Boolean = false
  override def getValueTypeInfo = BasicTypeInfo.BOOLEAN_TYPE_INFO
}

/**
  * Built-in Big Decimal Min with retraction aggregate function
  */
class DecimalMinWithRetractAggFunction extends MinWithRetractAggFunction[BigDecimal] {
  override def getInitValue: BigDecimal = BigDecimal.ZERO
  override def getValueTypeInfo = BasicTypeInfo.BIG_DEC_TYPE_INFO
}
