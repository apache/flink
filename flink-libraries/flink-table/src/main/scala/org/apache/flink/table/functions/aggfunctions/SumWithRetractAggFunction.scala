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

/** The initial accumulator for Sum with retract aggregate function */
class SumWithRetractAccumulator[T] extends JTuple2[T, Long] with Accumulator

/**
  * Base class for built-in Sum with retract aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class SumWithRetractAggFunction[T: Numeric] extends AggregateFunction[T] {

  private val numeric = implicitly[Numeric[T]]

  override def createAccumulator(): Accumulator = {
    val acc = new SumWithRetractAccumulator[T]()
    acc.f0 = numeric.zero //sum
    acc.f1 = 0L //total count
    acc
  }

  override def accumulate(accumulator: Accumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[T]
      val a = accumulator.asInstanceOf[SumWithRetractAccumulator[T]]
      a.f0 = numeric.plus(a.f0, v)
      a.f1 += 1
    }
  }

  override def retract(accumulator: Accumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[T]
      val a = accumulator.asInstanceOf[SumWithRetractAccumulator[T]]
      a.f0 = numeric.minus(a.f0, v)
      a.f1 -= 1
    }
  }

  override def getValue(accumulator: Accumulator): T = {
    val a = accumulator.asInstanceOf[SumWithRetractAccumulator[T]]
    if (a.f1 > 0) {
      a.f0
    } else {
      null.asInstanceOf[T]
    }
  }

  override def merge(accumulators: JList[Accumulator]): Accumulator = {
    val ret = accumulators.get(0).asInstanceOf[SumWithRetractAccumulator[T]]
    var i: Int = 1
    while (i < accumulators.size()) {
      val a = accumulators.get(i).asInstanceOf[SumWithRetractAccumulator[T]]
      ret.f0 = numeric.plus(ret.f0, a.f0)
      ret.f1 += a.f1
      i += 1
    }
    ret
  }

  override def resetAccumulator(accumulator: Accumulator): Unit = {
    accumulator.asInstanceOf[SumWithRetractAccumulator[T]].f0 = numeric.zero
    accumulator.asInstanceOf[SumWithRetractAccumulator[T]].f1 = 0L
  }

  override def getAccumulatorType(): TypeInformation[_] = {
    new TupleTypeInfo(
      (new SumWithRetractAccumulator).getClass,
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
class DecimalSumWithRetractAccumulator extends JTuple2[BigDecimal, Long] with Accumulator {
  f0 = BigDecimal.ZERO
  f1 = 0L
}

/**
  * Built-in Big Decimal Sum with retract aggregate function
  */
class DecimalSumWithRetractAggFunction extends AggregateFunction[BigDecimal] {

  override def createAccumulator(): Accumulator = {
    new DecimalSumWithRetractAccumulator
  }

  override def accumulate(accumulator: Accumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[BigDecimal]
      val accum = accumulator.asInstanceOf[DecimalSumWithRetractAccumulator]
      accum.f0 = accum.f0.add(v)
      accum.f1 += 1L
    }
  }

  override def retract(accumulator: Accumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[BigDecimal]
      val accum = accumulator.asInstanceOf[DecimalSumWithRetractAccumulator]
      accum.f0 = accum.f0.subtract(v)
      accum.f1 -= 1L
    }
  }

  override def getValue(accumulator: Accumulator): BigDecimal = {
    if (accumulator.asInstanceOf[DecimalSumWithRetractAccumulator].f1 == 0) {
      null.asInstanceOf[BigDecimal]
    } else {
      accumulator.asInstanceOf[DecimalSumWithRetractAccumulator].f0
    }
  }

  override def merge(accumulators: JList[Accumulator]): Accumulator = {
    val ret = accumulators.get(0).asInstanceOf[DecimalSumWithRetractAccumulator]
    var i: Int = 1
    while (i < accumulators.size()) {
      val a = accumulators.get(i).asInstanceOf[DecimalSumWithRetractAccumulator]
      ret.f0 = ret.f0.add(a.f0)
      ret.f1 += a.f1
      i += 1
    }
    ret
  }

  override def resetAccumulator(accumulator: Accumulator): Unit = {
    accumulator.asInstanceOf[DecimalSumWithRetractAccumulator].f0 = BigDecimal.ZERO
    accumulator.asInstanceOf[DecimalSumWithRetractAccumulator].f1 = 0L
  }

  override def getAccumulatorType(): TypeInformation[_] = {
    new TupleTypeInfo(
      (new DecimalSumWithRetractAccumulator).getClass,
      BasicTypeInfo.BIG_DEC_TYPE_INFO,
      BasicTypeInfo.LONG_TYPE_INFO)
  }
}
