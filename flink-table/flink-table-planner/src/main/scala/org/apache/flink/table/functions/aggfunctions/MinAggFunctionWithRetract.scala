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

/** The initial accumulator for Min with retraction aggregate function */
class MinWithRetractAccumulator[T] {
  var min: T = _
  var distinctCount: JLong = _
  var map: MapView[T, JLong] = _
}

/**
  * Base class for built-in Min with retraction aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class MinWithRetractAggFunction[T](implicit ord: Ordering[T])
  extends AggregateFunction[T, MinWithRetractAccumulator[T]] {

  override def createAccumulator(): MinWithRetractAccumulator[T] = {
    val acc = new MinWithRetractAccumulator[T]
    acc.min = getInitValue //min
    acc.distinctCount = 0L
    acc.map = new MapView(getValueTypeInfo, Types.LONG)
      .asInstanceOf[MapView[T, JLong]] //store the count for each value
    acc
  }

  def accumulate(acc: MinWithRetractAccumulator[T], value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[T]

      if (acc.distinctCount == 0 || (ord.compare(acc.min, v) > 0)) {
        acc.min = v
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

  def retract(acc: MinWithRetractAccumulator[T], value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[T]

      val count = acc.map.get(v)
      if (count == null || count == 1) {
        //remove the key v from the map if the number of appearance of the value v is 0
        if (count != null) {
          acc.map.remove(v)
        }
        //if the total count is 0, we could just simply set the f0(min) to the initial value
        acc.distinctCount -= 1
        if (acc.distinctCount == 0) {
          acc.min = getInitValue
          return
        }
        //if v is the current min value, we have to iterate the map to find the 2nd smallest
        // value to replace v as the min value
        if (v == acc.min) {
          val iterator = acc.map.keys.iterator()
          var hasMin = false
          while (iterator.hasNext) {
            val key = iterator.next()
            if (!hasMin || ord.compare(acc.min, key) > 0) {
              acc.min = key
              hasMin = true
            }
          }

          if (!hasMin) {
            acc.distinctCount = 0L
          }
        }
      } else {
        acc.map.put(v, count - 1)
      }
    }

  }

  override def getValue(acc: MinWithRetractAccumulator[T]): T = {
    if (acc.distinctCount != 0) {
      acc.min
    } else {
      null.asInstanceOf[T]
    }
  }

  def merge(acc: MinWithRetractAccumulator[T],
      its: JIterable[MinWithRetractAccumulator[T]]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      val a = iter.next()
      if (a.distinctCount != 0) {
        // set min element
        if (ord.compare(acc.min, a.min) > 0) {
          acc.min = a.min
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

  def resetAccumulator(acc: MinWithRetractAccumulator[T]): Unit = {
    acc.min = getInitValue
    acc.distinctCount = 0L
    acc.map.clear()
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

/**
  * Built-in String Min with retraction aggregate function
  */
class StringMinWithRetractAggFunction extends MinWithRetractAggFunction[String] {
  override def getInitValue: String = ""
  override def getValueTypeInfo = BasicTypeInfo.STRING_TYPE_INFO
}

/**
  * Built-in Timestamp Min with retraction aggregate function
  */
class TimestampMinWithRetractAggFunction extends MinWithRetractAggFunction[Timestamp] {
  override def getInitValue: Timestamp = new Timestamp(0)
  override def getValueTypeInfo = Types.SQL_TIMESTAMP
}

/**
  * Built-in Date Min with retraction aggregate function
  */
class DateMinWithRetractAggFunction extends MinWithRetractAggFunction[Date] {
  override def getInitValue: Date = new Date(0)
  override def getValueTypeInfo = Types.SQL_DATE
}

/**
  * Built-in Time Min with retraction aggregate function
  */
class TimeMinWithRetractAggFunction extends MinWithRetractAggFunction[Time] {
  override def getInitValue: Time = new Time(0)
  override def getValueTypeInfo = Types.SQL_TIME
}
