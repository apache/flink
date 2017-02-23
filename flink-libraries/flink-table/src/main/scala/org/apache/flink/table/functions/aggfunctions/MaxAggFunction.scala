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
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}

/**
  * Base class for built-in Max aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class MaxAggFunction[T](implicit ord: Ordering[T]) extends AggregateFunction[T] {

  /** The initial accumulator for Max aggregate function */
  class MaxAccumulator[T] extends JTuple2[T, Boolean] with Accumulator {
    f0 = 0.asInstanceOf[T] //max
    f1 = false
  }

  override def createAccumulator(): Accumulator = {
    new MaxAccumulator[T]
  }

  override def accumulate(accumulator: Accumulator, value: Any) = {
    if (value != null) {
      val v = value.asInstanceOf[T]
      val a = accumulator.asInstanceOf[MaxAccumulator[T]]
      if (!a.f1 || ord.compare(a.f0, v) < 0) {
        a.f0 = v
        if (!a.f1) {
          a.f1 = true
        }
      }
    }
  }

  override def getValue(accumulator: Accumulator): T = {
    val a = accumulator.asInstanceOf[MaxAccumulator[T]]
    if (a.f1) {
      a.f0
    } else {
      null.asInstanceOf[T]
    }
  }

  override def merge(accumulators: JList[Accumulator]): Accumulator = {
    val ret = accumulators.get(0)
    var i: Int = 1
    while (i < accumulators.size()) {
      val a = accumulators.get(i).asInstanceOf[MaxAccumulator[T]]
      if (a.f1) {
        accumulate(ret.asInstanceOf[MaxAccumulator[T]], a.f0)
      }
      i += 1
    }
    ret
  }
}

/**
  * Built-in Byte Max aggregate function
  */
class ByteMaxAggFunction extends MaxAggFunction[Byte]

/**
  * Built-in Short Max aggregate function
  */
class ShortMaxAggFunction extends MaxAggFunction[Short]

/**
  * Built-in Int Max aggregate function
  */
class IntMaxAggFunction extends MaxAggFunction[Int]

/**
  * Built-in Long Max aggregate function
  */
class LongMaxAggFunction extends MaxAggFunction[Long]

/**
  * Built-in Float Max aggregate function
  */
class FloatMaxAggFunction extends MaxAggFunction[Float]

/**
  * Built-in Double Max aggregate function
  */
class DoubleMaxAggFunction extends MaxAggFunction[Double]

/**
  * Built-in Boolean Max aggregate function
  */
class BooleanMaxAggFunction extends MaxAggFunction[Boolean]

/**
  * Built-in Big Decimal Max aggregate function
  */
class DecimalMaxAggFunction extends MaxAggFunction[BigDecimal] {

  override def accumulate(accumulator: Accumulator, value: Any) = {
    if (value != null) {
      val v = value.asInstanceOf[BigDecimal]
      val accum = accumulator.asInstanceOf[MaxAccumulator[BigDecimal]]
      if (!accum.f1 || accum.f0.compareTo(v) < 0) {
        accum.f0 = v
        if (!accum.f1) {
          accum.f1 = true
        }
      }
    }
  }
}
