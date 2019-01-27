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

import java.math.BigDecimal
import java.util.{HashMap => JHashMap}
import java.lang.{Iterable => JIterable}

import org.apache.flink.api.java.tuple.{Tuple3 => JTuple3}
import org.apache.flink.table.api.functions.AggregateFunction
import org.apache.flink.table.api.types._
import org.apache.flink.table.dataformat.BinaryString
import org.apache.flink.table.typeutils.BinaryStringTypeInfo

/** The initial accumulator for Max2nd with retraction aggregate function */
class Max2ndWithRetractAccumulator[T] extends JTuple3[T, T, JHashMap[T, Long]]

/**
  * Base class for built-in Max2nd with retraction aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class Max2ndWithRetractAggFunction[T](implicit ord: Ordering[T])
  extends AggregateFunction[T, Max2ndWithRetractAccumulator[T]] {

  override def createAccumulator(): Max2ndWithRetractAccumulator[T] = {
    val acc = new Max2ndWithRetractAccumulator[T]
    acc.f0 = getInitValue //max
    acc.f1 = getInitValue //max
    acc.f2 = new JHashMap[T, Long]() //store the count for each value
    acc
  }

  def accumulate(acc: Max2ndWithRetractAccumulator[T], value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[T]

      if (acc.f2.size() == 0) {
        acc.f0 = v
        acc.f1 = v
      } else if ((ord.compare(acc.f0, v) < 0)) {
        acc.f1 = acc.f0
        acc.f0 = v
      } else if ((ord.compare(acc.f1, v) < 0)) {
        acc.f1 = v
      }

      if (!acc.f2.containsKey(v)) {
        acc.f2.put(v, 1L)
      } else {
        var count = acc.f2.get(v)
        count += 1L
        acc.f2.put(v, count)
      }
    }
  }

  def retract(acc: Max2ndWithRetractAccumulator[T], value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[T]

      var count = acc.f2.get(v)
      count -= 1L
      if (count == 0) {
        //remove the key v from the map if the number of appearance of the value v is 0
        acc.f2.remove(v)
        //if the total count is 0, we could just simply set the f0(max) to the initial value
        if (acc.f2.size() == 0) {
          acc.f0 = getInitValue
          acc.f1 = getInitValue
          return
        }
        //if v is the current max value, we have to iterate the map to find the 2nd biggest
        // value to replace v as the max value
        if (v == acc.f0) {
          acc.f0 = acc.f1
          val iterator = acc.f2.keySet().iterator()
          var key = iterator.next()
          acc.f1 = key
          while (iterator.hasNext) {
            key = iterator.next()
            if (ord.compare(acc.f1, key) < 0) {
              acc.f1 = key
            }
          }
        }
      } else {
        acc.f2.put(v, count)
      }
    }

  }

  override def getValue(acc: Max2ndWithRetractAccumulator[T]): T = {
    if (acc.f2.size() != 0) {
      acc.f1
    } else {
      null.asInstanceOf[T]
    }
  }

  def merge(
      acc: Max2ndWithRetractAccumulator[T],
      its: JIterable[Max2ndWithRetractAccumulator[T]]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      val a = iter.next()
      if (a.f2.size() != 0) {
        // set max element
        if (ord.compare(acc.f0, a.f0) < 0) {
          acc.f0 = a.f0
          acc.f1 = acc.f0
        } else if(ord.compare(acc.f1, a.f0) < 0) {
          acc.f1 = a.f0
        } else if(ord.compare(acc.f1, a.f0) < 0) {
          acc.f1 = a.f1
        }
        // merge the count for each key
        val iterator = a.f2.keySet().iterator()
        while (iterator.hasNext) {
          val key = iterator.next()
          if (acc.f2.containsKey(key)) {
            acc.f2.put(key, acc.f2.get(key) + a.f2.get(key))
          } else {
            acc.f2.put(key, a.f2.get(key))
          }
        }
      }
    }
  }

  def resetAccumulator(acc: Max2ndWithRetractAccumulator[T]): Unit = {
    acc.f0 = getInitValue
    acc.f1 = getInitValue
    acc.f2.clear()
  }

  override def getAccumulatorType: DataType = {
    DataTypes.createTupleType(
      classOf[Max2ndWithRetractAccumulator[T]],
      getValueTypeInfo,
      getValueTypeInfo,
      new MapType(getValueTypeInfo, DataTypes.LONG))
  }

  def getInitValue: T

  def getValueTypeInfo: InternalType
}

/**
  * Built-in Byte Max with retraction aggregate function
  */
class ByteMax2ndWithRetractAggFunction extends Max2ndWithRetractAggFunction[Byte] {
  override def getInitValue: Byte = 0.toByte
  override def getValueTypeInfo = DataTypes.BYTE
}

/**
  * Built-in Short Max with retraction aggregate function
  */
class ShortMax2ndWithRetractAggFunction extends Max2ndWithRetractAggFunction[Short] {
  override def getInitValue: Short = 0.toShort
  override def getValueTypeInfo = DataTypes.SHORT
}

/**
  * Built-in Int Max with retraction aggregate function
  */
class IntMax2ndWithRetractAggFunction extends Max2ndWithRetractAggFunction[Int] {
  override def getInitValue: Int = 0
  override def getValueTypeInfo = DataTypes.INT
}

/**
  * Built-in Long Max with retraction aggregate function
  */
class LongMax2ndWithRetractAggFunction extends Max2ndWithRetractAggFunction[Long] {
  override def getInitValue: Long = 0L
  override def getValueTypeInfo = DataTypes.LONG
}

/**
  * Built-in Float Max with retraction aggregate function
  */
class FloatMax2ndWithRetractAggFunction extends Max2ndWithRetractAggFunction[Float] {
  override def getInitValue: Float = 0.0f
  override def getValueTypeInfo = DataTypes.FLOAT
}

/**
  * Built-in Double Max with retraction aggregate function
  */
class DoubleMax2ndWithRetractAggFunction extends Max2ndWithRetractAggFunction[Double] {
  override def getInitValue: Double = 0.0d
  override def getValueTypeInfo = DataTypes.DOUBLE
}

/**
  * Built-in Boolean Max with retraction aggregate function
  */
class BooleanMax2ndWithRetractAggFunction extends Max2ndWithRetractAggFunction[Boolean] {
  override def getInitValue: Boolean = false
  override def getValueTypeInfo = DataTypes.BOOLEAN
}

/**
  * Built-in Big Decimal Max with retraction aggregate function
  */
class DecimalMax2ndWithRetractAggFunction(decimalType: DecimalType)
  extends Max2ndWithRetractAggFunction[BigDecimal] {
  override def getInitValue: BigDecimal = BigDecimal.ZERO
  override def getValueTypeInfo = decimalType
}

/**
  * Built-in String Max with retraction aggregate function
  */
class StringMax2ndWithRetractAggFunction extends Max2ndWithRetractAggFunction[BinaryString] {

  override def getValueTypeInfo: InternalType = DataTypes.createGenericType(
    BinaryStringTypeInfo.INSTANCE)

  override def getInitValue = BinaryString.EMPTY_UTF8

  override def accumulate(acc: Max2ndWithRetractAccumulator[BinaryString], value: Any): Unit = {
    if (null != value) {
      accumulate(acc, value.asInstanceOf[BinaryString].copy())
    }
  }
}
