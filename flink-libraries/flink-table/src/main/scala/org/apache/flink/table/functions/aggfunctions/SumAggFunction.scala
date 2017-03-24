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
import java.util.{List => JList}

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}

/** The initial accumulator for Sum aggregate function */
class SumAccumulator[T] extends JTuple2[T, Boolean] with Accumulator

/**
  * Base class for built-in Sum aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class SumAggFunction[T: Numeric] extends AggregateFunction[T] {

  private val numeric = implicitly[Numeric[T]]

  override def createAccumulator(): Accumulator = {
    val acc = new SumAccumulator[T]()
    acc.f0 = numeric.zero //sum
    acc.f1 = false
    acc
  }

  override def accumulate(accumulator: Accumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[T]
      val a = accumulator.asInstanceOf[SumAccumulator[T]]
      a.f0 = numeric.plus(v, a.f0)
      a.f1 = true
    }
  }

  override def getValue(accumulator: Accumulator): T = {
    val a = accumulator.asInstanceOf[SumAccumulator[T]]
    if (a.f1) {
      a.f0
    } else {
      null.asInstanceOf[T]
    }
  }

  override def merge(accumulators: JList[Accumulator]): Accumulator = {
    val ret = accumulators.get(0).asInstanceOf[SumAccumulator[T]]
    var i: Int = 1
    while (i < accumulators.size()) {
      val a = accumulators.get(i).asInstanceOf[SumAccumulator[T]]
      if (a.f1) {
        ret.f0 = numeric.plus(ret.f0, a.f0)
        ret.f1 = true
      }
      i += 1
    }
    ret
  }

  override def resetAccumulator(accumulator: Accumulator): Unit = {
    accumulator.asInstanceOf[SumAccumulator[T]].f0 = numeric.zero
    accumulator.asInstanceOf[SumAccumulator[T]].f1 = false
  }

  override def getAccumulatorType(): TypeInformation[_] = {
    new TupleTypeInfo(
      (new SumAccumulator).getClass,
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
class DecimalSumAccumulator extends JTuple2[BigDecimal, Boolean] with Accumulator {
  f0 = BigDecimal.ZERO
  f1 = false
}

/**
  * Built-in Big Decimal Sum aggregate function
  */
class DecimalSumAggFunction extends AggregateFunction[BigDecimal] {

  override def createAccumulator(): Accumulator = {
    new DecimalSumAccumulator
  }

  override def accumulate(accumulator: Accumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[BigDecimal]
      val accum = accumulator.asInstanceOf[DecimalSumAccumulator]
      accum.f0 = accum.f0.add(v)
      accum.f1 = true
    }
  }

  override def getValue(accumulator: Accumulator): BigDecimal = {
    if (!accumulator.asInstanceOf[DecimalSumAccumulator].f1) {
      null.asInstanceOf[BigDecimal]
    } else {
      accumulator.asInstanceOf[DecimalSumAccumulator].f0
    }
  }

  override def merge(accumulators: JList[Accumulator]): Accumulator = {
    val ret = accumulators.get(0).asInstanceOf[DecimalSumAccumulator]
    var i: Int = 1
    while (i < accumulators.size()) {
      val a = accumulators.get(i).asInstanceOf[DecimalSumAccumulator]
      if (a.f1) {
        ret.f0 = ret.f0.add(a.f0)
        ret.f1 = true
      }
      i += 1
    }
    ret
  }

  override def resetAccumulator(accumulator: Accumulator): Unit = {
    accumulator.asInstanceOf[DecimalSumAccumulator].f0 = BigDecimal.ZERO
    accumulator.asInstanceOf[DecimalSumAccumulator].f1 = false
  }

  override def getAccumulatorType(): TypeInformation[_] = {
    new TupleTypeInfo(
      (new DecimalSumAccumulator).getClass,
      BasicTypeInfo.BIG_DEC_TYPE_INFO,
      BasicTypeInfo.BOOLEAN_TYPE_INFO)
  }
}
