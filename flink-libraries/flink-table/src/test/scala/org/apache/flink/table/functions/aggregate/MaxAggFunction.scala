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
package org.apache.flink.table.functions.aggregate

import java.lang.{Iterable => JIterable}
import java.math.BigDecimal

import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.table.api.functions.AggregateFunction
import org.apache.flink.table.api.types.{DataType, DataTypes, DecimalType}

/** The initial accumulator for Max aggregate function */
class MaxAccumulator[T] extends JTuple2[T, Boolean]

/**
  * Base class for built-in Max aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class MaxAggFunction[T](implicit ord: Ordering[T])
  extends AggregateFunction[T, MaxAccumulator[T]] {

  override def createAccumulator(): MaxAccumulator[T] = {
    val acc = new MaxAccumulator[T]
    acc.f0 = getInitValue
    acc.f1 = false
    acc
  }

  def accumulate(acc: MaxAccumulator[T], value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[T]
      if (!acc.f1 || ord.compare(acc.f0, v) < 0) {
        acc.f0 = v
        acc.f1 = true
      }
    }
  }

  override def getValue(acc: MaxAccumulator[T]): T = {
    if (acc.f1) {
      acc.f0
    } else {
      null.asInstanceOf[T]
    }
  }

  def merge(acc: MaxAccumulator[T], its: JIterable[MaxAccumulator[T]]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      val a = iter.next()
      if (a.f1) {
        accumulate(acc, a.f0)
      }
    }
  }

  def resetAccumulator(acc: MaxAccumulator[T]): Unit = {
    acc.f0 = getInitValue
    acc.f1 = false
  }

  override def getAccumulatorType: DataType = {
    DataTypes.createTupleType(
      classOf[MaxAccumulator[T]],
      getValueTypeInfo,
      DataTypes.BOOLEAN)
  }

  def getInitValue: T

  def getValueTypeInfo: DataType
}

/**
  * Built-in Byte Max aggregate function
  */
class ByteMaxAggFunction extends MaxAggFunction[Byte] {
  override def getInitValue: Byte = 0.toByte
  override def getValueTypeInfo = DataTypes.BYTE
}

/**
  * Built-in Short Max aggregate function
  */
class ShortMaxAggFunction extends MaxAggFunction[Short] {
  override def getInitValue: Short = 0.toShort
  override def getValueTypeInfo = DataTypes.SHORT
}

/**
  * Built-in Int Max aggregate function
  */
class IntMaxAggFunction extends MaxAggFunction[Int] {
  override def getInitValue: Int = 0
  override def getValueTypeInfo = DataTypes.INT
}

/**
  * Built-in Long Max aggregate function
  */
class LongMaxAggFunction extends MaxAggFunction[Long] {
  override def getInitValue: Long = 0L
  override def getValueTypeInfo = DataTypes.LONG
}

/**
  * Built-in Float Max aggregate function
  */
class FloatMaxAggFunction extends MaxAggFunction[Float] {
  override def getInitValue: Float = 0.0f
  override def getValueTypeInfo = DataTypes.FLOAT
}

/**
  * Built-in Double Max aggregate function
  */
class DoubleMaxAggFunction extends MaxAggFunction[Double] {
  override def getInitValue: Double = 0.0d
  override def getValueTypeInfo = DataTypes.DOUBLE
}

/**
  * Built-in Boolean Max aggregate function
  */
class BooleanMaxAggFunction extends MaxAggFunction[Boolean] {
  override def getInitValue = false
  override def getValueTypeInfo = DataTypes.BOOLEAN
}

/**
  * Built-in Big Decimal Max aggregate function
  */
class DecimalMaxAggFunction(decimalType: DecimalType)
  extends MaxAggFunction[BigDecimal] {
  override def getInitValue = BigDecimal.ZERO
  override def getValueTypeInfo = decimalType
}

/**
  * Built-in String Max aggregate function
  */
class StringMaxAggFunction extends MaxAggFunction[String] {
  override def getInitValue = ""
  override def getValueTypeInfo = DataTypes.STRING
}
