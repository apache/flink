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
import java.lang.{Iterable => JIterable, Long => JLong}
import java.sql.{Date, Time, Timestamp}

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation, Types}
import org.apache.flink.table.api.dataview.MapView
import org.apache.flink.table.functions.aggfunctions.Ordering._
import org.apache.flink.table.functions.AggregateFunction

/** The initial accumulator for Max with retraction aggregate function */
class MaxWithRetractAccumulator[T] {
  var max: T = _
  var distinctCount: JLong = _
  var map: MapView[T, JLong] = _
}

/**
  * Base class for built-in Max with retraction aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class MaxWithRetractAggFunction[T](implicit ord: Ordering[T])
  extends AggregateFunction[T, MaxWithRetractAccumulator[T]] {

  override def createAccumulator(): MaxWithRetractAccumulator[T] = {
    val acc = new MaxWithRetractAccumulator[T]
    acc.max = getInitValue //max
    acc.distinctCount = 0L
    acc.map = new MapView(getValueTypeInfo, Types.LONG)
      .asInstanceOf[MapView[T, JLong]] //store the count for each value
    acc
  }

  def accumulate(acc: MaxWithRetractAccumulator[T], value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[T]

      if (acc.distinctCount == 0 || (ord.compare(acc.max, v) < 0)) {
        acc.max = v
      }

      var count = acc.map.get(v)
      if (count == null) {
        acc.map.put(v, 1L)
        acc.distinctCount += 1
      } else {
        count += 1L
        acc.map.put(v, count)
      }
    }
  }

  def retract(acc: MaxWithRetractAccumulator[T], value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[T]

      val count = acc.map.get(v)
      if (count == null || count == 1) {
        //remove the key v from the map if the number of appearance of the value v is 0
        if (count != null) {
          acc.map.remove(v)
        }
        //if the total count is 0, we could just simply set the f0(max) to the initial value
        acc.distinctCount -= 1
        if (acc.distinctCount == 0) {
          acc.max = getInitValue
          return
        }
        //if v is the current max value, we have to iterate the map to find the 2nd biggest
        // value to replace v as the max value
        if (v == acc.max) {
          val iterator = acc.map.keys.iterator()
          var hasMax = false
          while (iterator.hasNext) {
            val key = iterator.next()
            if (!hasMax || ord.compare(acc.max, key) < 0) {
              acc.max = key
              hasMax = true
            }
          }

          if (!hasMax) {
            acc.distinctCount = 0L
          }
        }
      } else {
        acc.map.put(v, count - 1)
      }
    }

  }

  override def getValue(acc: MaxWithRetractAccumulator[T]): T = {
    if (acc.distinctCount != 0) {
      acc.max
    } else {
      null.asInstanceOf[T]
    }
  }

  def merge(acc: MaxWithRetractAccumulator[T],
      its: JIterable[MaxWithRetractAccumulator[T]]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      val a = iter.next()
      if (a.distinctCount != 0) {
        // set max element
        if (ord.compare(acc.max, a.max) < 0) {
          acc.max = a.max
        }
        // merge the count for each key
        val iterator = a.map.entries.iterator()
        while (iterator.hasNext) {
          val entry = iterator.next()
          val key = entry.getKey
          val value = entry.getValue
          val count = acc.map.get(key)
          if (count != null) {
            acc.map.put(key, count + value)
          } else {
            acc.map.put(key, value)
            acc.distinctCount += 1
          }
        }
      }
    }
  }

  def resetAccumulator(acc: MaxWithRetractAccumulator[T]): Unit = {
    acc.max = getInitValue
    acc.distinctCount = 0L
    acc.map.clear()
  }

  def getInitValue: T

  def getValueTypeInfo: TypeInformation[_]
}

/**
  * Built-in Byte Max with retraction aggregate function
  */
class ByteMaxWithRetractAggFunction extends MaxWithRetractAggFunction[Byte] {
  override def getInitValue: Byte = 0.toByte
  override def getValueTypeInfo = BasicTypeInfo.BYTE_TYPE_INFO
}

/**
  * Built-in Short Max with retraction aggregate function
  */
class ShortMaxWithRetractAggFunction extends MaxWithRetractAggFunction[Short] {
  override def getInitValue: Short = 0.toShort
  override def getValueTypeInfo = BasicTypeInfo.SHORT_TYPE_INFO
}

/**
  * Built-in Int Max with retraction aggregate function
  */
class IntMaxWithRetractAggFunction extends MaxWithRetractAggFunction[Int] {
  override def getInitValue: Int = 0
  override def getValueTypeInfo = BasicTypeInfo.INT_TYPE_INFO
}

/**
  * Built-in Long Max with retraction aggregate function
  */
class LongMaxWithRetractAggFunction extends MaxWithRetractAggFunction[Long] {
  override def getInitValue: Long = 0L
  override def getValueTypeInfo = BasicTypeInfo.LONG_TYPE_INFO
}

/**
  * Built-in Float Max with retraction aggregate function
  */
class FloatMaxWithRetractAggFunction extends MaxWithRetractAggFunction[Float] {
  override def getInitValue: Float = 0.0f
  override def getValueTypeInfo = BasicTypeInfo.FLOAT_TYPE_INFO
}

/**
  * Built-in Double Max with retraction aggregate function
  */
class DoubleMaxWithRetractAggFunction extends MaxWithRetractAggFunction[Double] {
  override def getInitValue: Double = 0.0d
  override def getValueTypeInfo = BasicTypeInfo.DOUBLE_TYPE_INFO
}

/**
  * Built-in Boolean Max with retraction aggregate function
  */
class BooleanMaxWithRetractAggFunction extends MaxWithRetractAggFunction[Boolean] {
  override def getInitValue: Boolean = false
  override def getValueTypeInfo = BasicTypeInfo.BOOLEAN_TYPE_INFO
}

/**
  * Built-in Big Decimal Max with retraction aggregate function
  */
class DecimalMaxWithRetractAggFunction extends MaxWithRetractAggFunction[BigDecimal] {
  override def getInitValue: BigDecimal = BigDecimal.ZERO
  override def getValueTypeInfo = BasicTypeInfo.BIG_DEC_TYPE_INFO
}

/**
  * Built-in String Max with retraction aggregate function
  */
class StringMaxWithRetractAggFunction extends MaxWithRetractAggFunction[String] {
  override def getInitValue: String = ""
  override def getValueTypeInfo = BasicTypeInfo.STRING_TYPE_INFO
}

/**
  * Built-in Timestamp Max with retraction aggregate function
  */
class TimestampMaxWithRetractAggFunction extends MaxWithRetractAggFunction[Timestamp] {
  override def getInitValue: Timestamp = new Timestamp(0)
  override def getValueTypeInfo = Types.SQL_TIMESTAMP
}

/**
  * Built-in Date Max with retraction aggregate function
  */
class DateMaxWithRetractAggFunction extends MaxWithRetractAggFunction[Date] {
  override def getInitValue: Date = new Date(0)
  override def getValueTypeInfo = Types.SQL_DATE
}

/**
  * Built-in Time Max with retraction aggregate function
  */
class TimeMaxWithRetractAggFunction extends MaxWithRetractAggFunction[Time] {
  override def getInitValue: Time = new Time(0)
  override def getValueTypeInfo = Types.SQL_TIME
}
