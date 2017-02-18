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
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}

/**
  * Base class for built-in Min aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class MinAggFunction[T](implicit ord: Ordering[T]) extends AggregateFunction[T] {
  /** The initial accumulator for Min aggregate function */
  class MinAccumulator[T] extends Accumulator {
    var max: T = null.asInstanceOf[T]
  }

  override def createAccumulator(): Accumulator = {
    new MinAccumulator[T]
  }

  override def accumulate(accumulator: Accumulator, value: Any) = {
    if (value != null) {
      val v = value.asInstanceOf[T]
      val accum = accumulator.asInstanceOf[MinAccumulator[T]]
      if (accum.max == null || ord.compare(accum.max, v) > 0) {
        accum.max = v
      }
    }
  }

  override def getValue(accumulator: Accumulator): T = {
    accumulator.asInstanceOf[MinAccumulator[T]].max
  }

  override def merge(a: Accumulator, b: Accumulator): Accumulator = {
    accumulate(a, b.asInstanceOf[MinAccumulator[T]].max)
    a
  }
}

/**
  * Built-in Byte Min aggregate function
  */
class ByteMinAggFunction extends MinAggFunction[Byte]

/**
  * Built-in Short Min aggregate function
  */
class ShortMinAggFunction extends MinAggFunction[Short]

/**
  * Built-in Int Min aggregate function
  */
class IntMinAggFunction extends MinAggFunction[Int]

/**
  * Built-in Long Min aggregate function
  */
class LongMinAggFunction extends MinAggFunction[Long]

/**
  * Built-in Float Min aggregate function
  */
class FloatMinAggFunction extends MinAggFunction[Float]

/**
  * Built-in Double Min aggregate function
  */
class DoubleMinAggFunction extends MinAggFunction[Double]

/**
  * Built-in Boolean Min aggregate function
  */
class BooleanMinAggFunction extends MinAggFunction[Boolean]

/**
  * Built-in Big Decimal Min aggregate function
  */
class DecimalMinAggFunction extends MinAggFunction[BigDecimal] {

  override def accumulate(accumulator: Accumulator, value: Any) = {
    if (value != null) {
      val v = value.asInstanceOf[BigDecimal]
      val accum = accumulator.asInstanceOf[MinAccumulator[BigDecimal]]
      if (accum.max == null || accum.max.compareTo(v) > 0) {
        accum.max = v
      }
    }
  }
}
