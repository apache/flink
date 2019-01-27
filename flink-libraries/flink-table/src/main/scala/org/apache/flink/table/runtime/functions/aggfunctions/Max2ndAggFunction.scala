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
import java.math.BigDecimal
import org.apache.flink.api.java.tuple.{Tuple3 => JTuple3}
import org.apache.flink.table.api.functions.AggregateFunction
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.{DataType, DataTypes, DecimalType}
import org.apache.flink.table.dataformat.BinaryString
import org.apache.flink.table.typeutils.BinaryStringTypeInfo

/** The initial accumulator for Max aggregate function */
class Max2ndAccumulator[T] extends JTuple3[T, T, Boolean]

/**
  * Base class for built-in Max2nd aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class Max2ndAggFunction[T](implicit ord: Ordering[T])
  extends AggregateFunction[T, Max2ndAccumulator[T]] {

  override def createAccumulator(): Max2ndAccumulator[T] = {
    val acc = new Max2ndAccumulator[T]
    acc.f0 = getInitValue
    acc.f1 = getInitValue
    acc.f2 = false
    acc
  }

  def accumulate(acc: Max2ndAccumulator[T], value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[T]
      if (!acc.f2) {
        acc.f1 = v
        acc.f0 = v
        acc.f2 = true
      } else if (ord.compare(acc.f0, v) < 0) {
        acc.f1 = acc.f0
        acc.f0 = v
      } else if (ord.compare(acc.f1, v) < 0) {
        acc.f1 = v
      }
    }
  }

  override def getValue(acc: Max2ndAccumulator[T]): T = {
    if (acc.f2) {
      acc.f1
    } else {
      null.asInstanceOf[T]
    }
  }

  def merge(acc: Max2ndAccumulator[T], its: JIterable[Max2ndAccumulator[T]]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      val a = iter.next()
      if (a.f2) {
        accumulate(acc, a.f0)
        accumulate(acc, a.f1)
      }
    }
  }

  def resetAccumulator(acc: Max2ndAccumulator[T]): Unit = {
    acc.f0 = getInitValue
    acc.f1 = getInitValue
    acc.f2 = false
  }

  override def getAccumulatorType: DataType = {
    DataTypes.createTupleType(
      classOf[Max2ndAccumulator[T]],
      getValueTypeInfo,
      getValueTypeInfo,
      DataTypes.BOOLEAN)
  }

  def getInitValue: T

  def getValueTypeInfo: DataType
}
/**
  * Built-in Byte Max aggregate function
  */
class ByteMax2ndAggFunction extends Max2ndAggFunction[Byte] {
  override def getInitValue: Byte = 0.toByte
  override def getValueTypeInfo = DataTypes.BYTE
}

/**
  * Built-in Short Max aggregate function
  */
class ShortMax2ndAggFunction extends Max2ndAggFunction[Short] {
  override def getInitValue: Short = 0.toShort
  override def getValueTypeInfo = DataTypes.SHORT
}

/**
  * Built-in Int Max aggregate function
  */
class IntMax2ndAggFunction extends Max2ndAggFunction[Int] {
  override def getInitValue: Int = 0
  override def getValueTypeInfo = DataTypes.INT
}

/**
  * Built-in Long Max aggregate function
  */
class LongMax2ndAggFunction extends Max2ndAggFunction[Long] {
  override def getInitValue: Long = 0L
  override def getValueTypeInfo = DataTypes.LONG
}

/**
  * Built-in Float Max aggregate function
  */
class FloatMax2ndAggFunction extends Max2ndAggFunction[Float] {
  override def getInitValue: Float = 0.0f
  override def getValueTypeInfo = DataTypes.FLOAT
}

/**
  * Built-in Double Max aggregate function
  */
class DoubleMax2ndAggFunction extends Max2ndAggFunction[Double] {
  override def getInitValue: Double = 0.0d
  override def getValueTypeInfo = DataTypes.DOUBLE
}

/**
  * Built-in Boolean Max aggregate function
  */
class BooleanMax2ndAggFunction extends Max2ndAggFunction[Boolean] {
  override def getInitValue = false
  override def getValueTypeInfo = DataTypes.BOOLEAN
}

/**
  * Built-in Big Decimal Max aggregate function
  */
class DecimalMax2ndAggFunction(decimalType: DecimalType)
  extends Max2ndAggFunction[BigDecimal] {
  override def getInitValue = BigDecimal.ZERO
  override def getValueTypeInfo = decimalType
}

/**
  * Built-in String Max aggregate function
  */
class StringMax2ndAggFunction extends Max2ndAggFunction[BinaryString] {

  override def getValueTypeInfo: DataType = BinaryStringTypeInfo.INSTANCE
  override def getInitValue = BinaryString.EMPTY_UTF8

  override def accumulate(acc: Max2ndAccumulator[BinaryString], value: Any): Unit = {
    if (null != value) {
      accumulate(acc, value.asInstanceOf[BinaryString].copy())
    }
  }
}
