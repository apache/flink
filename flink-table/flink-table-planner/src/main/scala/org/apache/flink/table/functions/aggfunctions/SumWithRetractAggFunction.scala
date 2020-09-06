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
import java.lang.{Iterable => JIterable}

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.table.functions.AggregateFunction

/** The initial accumulator for Sum with retract aggregate function */
class SumWithRetractAccumulator[T] extends JTuple2[T, Long]

/**
  * Base class for built-in Sum with retract aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class SumWithRetractAggFunction[T: Numeric]
  extends AggregateFunction[T, SumWithRetractAccumulator[T]] {

  private val numeric = implicitly[Numeric[T]]

  override def createAccumulator(): SumWithRetractAccumulator[T] = {
    val acc = new SumWithRetractAccumulator[T]()
    acc.f0 = numeric.zero //sum
    acc.f1 = 0L //total count
    acc
  }

  def accumulate(acc: SumWithRetractAccumulator[T], value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[T]
      acc.f0 = numeric.plus(acc.f0, v)
      acc.f1 += 1
    }
  }

  def retract(acc: SumWithRetractAccumulator[T], value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[T]
      acc.f0 = numeric.minus(acc.f0, v)
      acc.f1 -= 1
    }
  }

  override def getValue(acc: SumWithRetractAccumulator[T]): T = {
    if (acc.f1 > 0) {
      acc.f0
    } else {
      null.asInstanceOf[T]
    }
  }

  def merge(acc: SumWithRetractAccumulator[T],
      its: JIterable[SumWithRetractAccumulator[T]]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      val a = iter.next()
      acc.f0 = numeric.plus(acc.f0, a.f0)
      acc.f1 += a.f1
    }
  }

  def resetAccumulator(acc: SumWithRetractAccumulator[T]): Unit = {
    acc.f0 = numeric.zero
    acc.f1 = 0L
  }

  override def getAccumulatorType: TypeInformation[SumWithRetractAccumulator[T]] = {
    new TupleTypeInfo(
      classOf[SumWithRetractAccumulator[T]],
      getValueTypeInfo,
      BasicTypeInfo.LONG_TYPE_INFO)
  }

  def getValueTypeInfo: TypeInformation[_]
}

/**
  * Built-in Byte Sum with retract aggregate function
  */
class ByteSumWithRetractAggFunction extends SumWithRetractAggFunction[Byte] {
  override def getValueTypeInfo = BasicTypeInfo.BYTE_TYPE_INFO
}

/**
  * Built-in Short Sum with retract aggregate function
  */
class ShortSumWithRetractAggFunction extends SumWithRetractAggFunction[Short] {
  override def getValueTypeInfo = BasicTypeInfo.SHORT_TYPE_INFO
}

/**
  * Built-in Int Sum with retract aggregate function
  */
class IntSumWithRetractAggFunction extends SumWithRetractAggFunction[Int] {
  override def getValueTypeInfo = BasicTypeInfo.INT_TYPE_INFO
}

/**
  * Built-in Long Sum with retract aggregate function
  */
class LongSumWithRetractAggFunction extends SumWithRetractAggFunction[Long] {
  override def getValueTypeInfo = BasicTypeInfo.LONG_TYPE_INFO
}

/**
  * Built-in Float Sum with retract aggregate function
  */
class FloatSumWithRetractAggFunction extends SumWithRetractAggFunction[Float] {
  override def getValueTypeInfo = BasicTypeInfo.FLOAT_TYPE_INFO
}

/**
  * Built-in Double Sum with retract aggregate function
  */
class DoubleSumWithRetractAggFunction extends SumWithRetractAggFunction[Double] {
  override def getValueTypeInfo = BasicTypeInfo.DOUBLE_TYPE_INFO
}

/** The initial accumulator for Big Decimal Sum with retract aggregate function */
class DecimalSumWithRetractAccumulator extends JTuple2[BigDecimal, Long] {
  f0 = BigDecimal.ZERO
  f1 = 0L
}

/**
  * Built-in Big Decimal Sum with retract aggregate function
  */
class DecimalSumWithRetractAggFunction
  extends AggregateFunction[BigDecimal, DecimalSumWithRetractAccumulator] {

  override def createAccumulator(): DecimalSumWithRetractAccumulator = {
    new DecimalSumWithRetractAccumulator
  }

  def accumulate(acc: DecimalSumWithRetractAccumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[BigDecimal]
      acc.f0 = acc.f0.add(v)
      acc.f1 += 1L
    }
  }

  def retract(acc: DecimalSumWithRetractAccumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[BigDecimal]
      acc.f0 = acc.f0.subtract(v)
      acc.f1 -= 1L
    }
  }

  override def getValue(acc: DecimalSumWithRetractAccumulator): BigDecimal = {
    if (acc.f1 == 0) {
      null.asInstanceOf[BigDecimal]
    } else {
      acc.f0
    }
  }

  def merge(acc: DecimalSumWithRetractAccumulator,
      its: JIterable[DecimalSumWithRetractAccumulator]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      val a = iter.next()
      acc.f0 = acc.f0.add(a.f0)
      acc.f1 += a.f1
    }
  }

  def resetAccumulator(acc: DecimalSumWithRetractAccumulator): Unit = {
    acc.f0 = BigDecimal.ZERO
    acc.f1 = 0L
  }

  override def getAccumulatorType: TypeInformation[DecimalSumWithRetractAccumulator] = {
    new TupleTypeInfo(
      classOf[DecimalSumWithRetractAccumulator],
      BasicTypeInfo.BIG_DEC_TYPE_INFO,
      BasicTypeInfo.LONG_TYPE_INFO)
  }
}
