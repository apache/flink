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
package org.apache.flink.table.functions.builtInAggFuncs

import java.math.BigDecimal
import java.util.List

import org.apache.flink.table.functions.{Accumulator, AggregateFunction}

/**
  * Base class for built-in Sum aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class SumAggFunction[T: Numeric] extends AggregateFunction[T] {
  /** The initial accumulator for Sum aggregate function */
  class SumAccumulator[T] extends Accumulator {
    var sum: Option[T] = None
  }

  private val numeric = implicitly[Numeric[T]]

  override def createAccumulator(): Accumulator = {
    new SumAccumulator[T]
  }

  override def accumulate(accumulator: Accumulator, value: Any) = {
    if (value != null) {
      val v = value.asInstanceOf[T]
      val accum = accumulator.asInstanceOf[SumAccumulator[T]]
      if (accum.sum.isEmpty) {
        accum.sum = Some(v)
      } else {
        accum.sum = Some(numeric.plus(v, accum.sum.get))
      }
    }
  }

  override def getValue(accumulator: Accumulator): T = {
    val sum = accumulator.asInstanceOf[SumAccumulator[T]].sum
    if (sum.isEmpty) {
      null.asInstanceOf[T]
    } else {
      sum match {
        case Some(i) => i.asInstanceOf[T]
      }
    }
  }

  override def merge(accumulators: List[Accumulator]): Accumulator = {
    val ret = createAccumulator().asInstanceOf[SumAccumulator[T]]
    var i: Int = 0
    while (i < accumulators.size()) {
      val a = accumulators.get(i).asInstanceOf[SumAccumulator[T]]
      if (ret.sum.isEmpty) {
        ret.sum = a.sum
      } else if (!a.sum.isEmpty) {
        ret.sum = Some(numeric.plus(ret.sum.get, a.sum.get))
      }
      i += 1
    }
    ret
  }
}

/**
  * Built-in Byte Sum aggregate function
  */
class ByteSumAggFunction extends SumAggFunction[Byte]

/**
  * Built-in Short Sum aggregate function
  */
class ShortSumAggFunction extends SumAggFunction[Short]

/**
  * Built-in Int Sum aggregate function
  */
class IntSumAggFunction extends SumAggFunction[Int]

/**
  * Built-in Long Sum aggregate function
  */
class LongSumAggFunction extends SumAggFunction[Long]

/**
  * Built-in Float Sum aggregate function
  */
class FloatSumAggFunction extends SumAggFunction[Float]

/**
  * Built-in Double Sum aggregate function
  */
class DoubleSumAggFunction extends SumAggFunction[Double]


/**
  * Built-in Big Decimal Sum aggregate function
  */
class DecimalSumAggFunction extends AggregateFunction[BigDecimal] {
  /** The initial accumulator for Big Decimal Sum aggregate function */
  class DecimalSumAccumulator extends Accumulator {
    var sum: BigDecimal = null
  }

  override def createAccumulator(): Accumulator = {
    new DecimalSumAccumulator
  }

  override def accumulate(accumulator: Accumulator, value: Any) = {
    if (value != null) {
      val v = value.asInstanceOf[BigDecimal]
      val accum = accumulator.asInstanceOf[DecimalSumAccumulator]
      if (accum.sum == null) {
        accum.sum = v
      } else {
        accum.sum = accum.sum.add(v)
      }
    }
  }

  override def getValue(accumulator: Accumulator): BigDecimal = {
    val sum = accumulator.asInstanceOf[DecimalSumAccumulator].sum
    if (sum == null) {
      null.asInstanceOf[BigDecimal]
    } else {
      sum
    }
  }

  override def merge(accumulators: List[Accumulator]): Accumulator = {
    val ret = accumulators.get(0)
    var i: Int = 1
    while (i < accumulators.size()) {
      accumulate(ret.asInstanceOf[DecimalSumAccumulator],
                 accumulators.get(i).asInstanceOf[DecimalSumAccumulator].sum)
      i += 1
    }
    ret
  }
}
