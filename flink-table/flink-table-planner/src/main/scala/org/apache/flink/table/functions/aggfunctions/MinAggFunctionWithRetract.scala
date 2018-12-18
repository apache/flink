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
import java.util.{HashMap => JHashMap}
import java.lang.{Iterable => JIterable}
import java.sql.{Date, Time, Timestamp}

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.{MapTypeInfo, TupleTypeInfo}
import org.apache.flink.table.api.Types
import org.apache.flink.table.functions.aggfunctions.Ordering._
import org.apache.flink.table.functions.AggregateFunction

/** The initial accumulator for Min with retraction aggregate function */
class MinWithRetractAccumulator[T] extends JTuple2[T, JHashMap[T, Long]]

/**
  * Base class for built-in Min with retraction aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class MinWithRetractAggFunction[T](implicit ord: Ordering[T])
  extends AggregateFunction[T, MinWithRetractAccumulator[T]] {

  override def createAccumulator(): MinWithRetractAccumulator[T] = {
    val acc = new MinWithRetractAccumulator[T]
    acc.f0 = getInitValue //min
    acc.f1 = new JHashMap[T, Long]() //store the count for each value
    acc
  }

  def accumulate(acc: MinWithRetractAccumulator[T], value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[T]

      if (acc.f1.size() == 0 || (ord.compare(acc.f0, v) > 0)) {
        acc.f0 = v
      }

      if (!acc.f1.containsKey(v)) {
        acc.f1.put(v, 1L)
      } else {
        var count = acc.f1.get(v)
        count += 1L
        acc.f1.put(v, count)
      }
    }
  }

  def retract(acc: MinWithRetractAccumulator[T], value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[T]

      var count = acc.f1.get(v)
      count -= 1L
      if (count == 0) {
        //remove the key v from the map if the number of appearance of the value v is 0
        acc.f1.remove(v)
        //if the total count is 0, we could just simply set the f0(min) to the initial value
        if (acc.f1.size() == 0) {
          acc.f0 = getInitValue
          return
        }
        //if v is the current min value, we have to iterate the map to find the 2nd smallest
        // value to replace v as the min value
        if (v == acc.f0) {
          val iterator = acc.f1.keySet().iterator()
          var key = iterator.next()
          acc.f0 = key
          while (iterator.hasNext) {
            key = iterator.next()
            if (ord.compare(acc.f0, key) > 0) {
              acc.f0 = key
            }
          }
        }
      } else {
        acc.f1.put(v, count)
      }
    }

  }

  override def getValue(acc: MinWithRetractAccumulator[T]): T = {
    if (acc.f1.size() != 0) {
      acc.f0
    } else {
      null.asInstanceOf[T]
    }
  }

  def merge(acc: MinWithRetractAccumulator[T],
      its: JIterable[MinWithRetractAccumulator[T]]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      val a = iter.next()
      if (a.f1.size() != 0) {
        // set min element
        if (ord.compare(acc.f0, a.f0) > 0) {
          acc.f0 = a.f0
        }
        // merge the count for each key
        val iterator = a.f1.keySet().iterator()
        while (iterator.hasNext) {
          val key = iterator.next()
          if (acc.f1.containsKey(key)) {
            acc.f1.put(key, acc.f1.get(key) + a.f1.get(key))
          } else {
            acc.f1.put(key, a.f1.get(key))
          }
        }
      }
    }
  }

  def resetAccumulator(acc: MinWithRetractAccumulator[T]): Unit = {
    acc.f0 = getInitValue
    acc.f1.clear()
  }

  override def getAccumulatorType: TypeInformation[MinWithRetractAccumulator[T]] = {
    new TupleTypeInfo(
      classOf[MinWithRetractAccumulator[T]],
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
