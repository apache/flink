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
package org.apache.flink.table.planner.utils

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.table.functions.AggregateFunction

import java.lang.{Iterable => JIterable}
import java.math.BigDecimal

/** The initial accumulator for Sum aggregate function */
class SumAccumulator[T] extends JTuple2[T, Boolean]

/**
  * Base class for built-in Sum aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class SumAggFunction[T: Numeric] extends AggregateFunction[T, SumAccumulator[T]] {

  private val numeric = implicitly[Numeric[T]]

  override def createAccumulator(): SumAccumulator[T] = {
    val acc = new SumAccumulator[T]()
    acc.f0 = numeric.zero //sum
    acc.f1 = false
    acc
  }

  def accumulate(accumulator: SumAccumulator[T], value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[T]
      accumulator.f0 = numeric.plus(v, accumulator.f0)
      accumulator.f1 = true
    }
  }

  override def getValue(accumulator: SumAccumulator[T]): T = {
    if (accumulator.f1) {
      accumulator.f0
    } else {
      null.asInstanceOf[T]
    }
  }

  def merge(acc: SumAccumulator[T], its: JIterable[SumAccumulator[T]]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      val a = iter.next()
      if (a.f1) {
        acc.f0 = numeric.plus(acc.f0, a.f0)
        acc.f1 = true
      }
    }
  }

  override def getAccumulatorType: TypeInformation[SumAccumulator[T]] = {
    new TupleTypeInfo(
      classOf[SumAccumulator[T]],
      getValueTypeInfo,
      BasicTypeInfo.BOOLEAN_TYPE_INFO)
  }

  def getValueTypeInfo: TypeInformation[_]
}

/**
  * Built-in Byte Sum aggregate function
  */
class ByteSumAggFunction extends SumAggFunction[Byte] {
  override def getValueTypeInfo = BasicTypeInfo.BYTE_TYPE_INFO
}

/**
  * Built-in Short Sum aggregate function
  */
class ShortSumAggFunction extends SumAggFunction[Short] {
  override def getValueTypeInfo = BasicTypeInfo.SHORT_TYPE_INFO
}

/**
  * Built-in Int Sum aggregate function
  */
class IntSumAggFunction extends SumAggFunction[Int] {
  override def getValueTypeInfo = BasicTypeInfo.INT_TYPE_INFO
}

/**
  * Built-in Long Sum aggregate function
  */
class LongSumAggFunction extends SumAggFunction[Long] {
  override def getValueTypeInfo = BasicTypeInfo.LONG_TYPE_INFO
}

/**
  * Built-in Float Sum aggregate function
  */
class FloatSumAggFunction extends SumAggFunction[Float] {
  override def getValueTypeInfo = BasicTypeInfo.FLOAT_TYPE_INFO
}

/**
  * Built-in Double Sum aggregate function
  */
class DoubleSumAggFunction extends SumAggFunction[Double] {
  override def getValueTypeInfo = BasicTypeInfo.DOUBLE_TYPE_INFO
}

/** The initial accumulator for Big Decimal Sum aggregate function */
class DecimalSumAccumulator extends JTuple2[BigDecimal, Boolean] {
  f0 = BigDecimal.ZERO
  f1 = false
}

/**
  * Built-in Big Decimal Sum aggregate function
  */
class DecimalSumAggFunction extends AggregateFunction[BigDecimal, DecimalSumAccumulator] {

  override def createAccumulator(): DecimalSumAccumulator = {
    new DecimalSumAccumulator
  }

  def accumulate(acc: DecimalSumAccumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[BigDecimal]
      acc.f0 = acc.f0.add(v)
      acc.f1 = true
    }
  }

  override def getValue(acc: DecimalSumAccumulator): BigDecimal = {
    if (acc.f1) {
      acc.f0
    } else {
      null.asInstanceOf[BigDecimal]
    }
  }

  def merge(acc: DecimalSumAccumulator, its: JIterable[DecimalSumAccumulator]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      val a = iter.next()
      if (a.f1) {
        acc.f0 = acc.f0.add(a.f0)
        acc.f1 = true
      }
    }
  }

  override def getAccumulatorType: TypeInformation[DecimalSumAccumulator] = {
    new TupleTypeInfo(
      classOf[DecimalSumAccumulator],
      BasicTypeInfo.BIG_DEC_TYPE_INFO,
      BasicTypeInfo.BOOLEAN_TYPE_INFO)
  }
}
