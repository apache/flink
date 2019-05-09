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

package org.apache.flink.table.runtime.aggfunctions

import java.math.{BigDecimal, MathContext}

import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.functions.aggfunctions._

/**
  * Test case for built-in average aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class AvgAggFunctionTestBase[T: Numeric, ACC] extends AggFunctionTestBase[T, ACC] {

  private val numeric: Numeric[T] = implicitly[Numeric[T]]

  def minVal: T

  def maxVal: T

  override def inputValueSets: Seq[Seq[T]] = Seq(
    Seq(
      minVal,
      minVal,
      null.asInstanceOf[T],
      minVal,
      minVal,
      null.asInstanceOf[T],
      minVal,
      minVal,
      minVal
    ),
    Seq(
      maxVal,
      maxVal,
      null.asInstanceOf[T],
      maxVal,
      maxVal,
      null.asInstanceOf[T],
      maxVal,
      maxVal,
      maxVal
    ),
    Seq(
      minVal,
      maxVal,
      null.asInstanceOf[T],
      numeric.fromInt(0),
      numeric.negate(maxVal),
      numeric.negate(minVal),
      null.asInstanceOf[T]
    ),
    Seq(
      numeric.fromInt(1),
      numeric.fromInt(2),
      null.asInstanceOf[T],
      numeric.fromInt(3),
      numeric.fromInt(4),
      numeric.fromInt(5),
      null.asInstanceOf[T]
    ),
    Seq(
      null.asInstanceOf[T],
      null.asInstanceOf[T],
      null.asInstanceOf[T],
      null.asInstanceOf[T],
      null.asInstanceOf[T],
      null.asInstanceOf[T]
    )
  )

  override def expectedResults: Seq[T] = Seq(
    minVal,
    maxVal,
    numeric.fromInt(0),
    numeric.fromInt(3),
    null.asInstanceOf[T]
  )

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}

class ByteAvgAggFunctionTest extends AvgAggFunctionTestBase[Byte, IntegralAvgAccumulator] {

  override def minVal = (Byte.MinValue + 1).toByte

  override def maxVal = (Byte.MaxValue - 1).toByte

  override def aggregator = new ByteAvgAggFunction()
}

class ShortAvgAggFunctionTest extends AvgAggFunctionTestBase[Short, IntegralAvgAccumulator] {

  override def minVal = (Short.MinValue + 1).toShort

  override def maxVal = (Short.MaxValue - 1).toShort

  override def aggregator = new ShortAvgAggFunction()
}

class IntAvgAggFunctionTest extends AvgAggFunctionTestBase[Int, IntegralAvgAccumulator] {

  override def minVal = Int.MinValue + 1

  override def maxVal = Int.MaxValue - 1

  override def aggregator = new IntAvgAggFunction()
}

class LongAvgAggFunctionTest extends AvgAggFunctionTestBase[Long, BigIntegralAvgAccumulator] {

  override def minVal = Long.MinValue + 1

  override def maxVal = Long.MaxValue - 1

  override def aggregator = new LongAvgAggFunction()
}

class FloatAvgAggFunctionTest extends AvgAggFunctionTestBase[Float, FloatingAvgAccumulator] {

  override def minVal = Float.MinValue

  override def maxVal = Float.MaxValue

  override def aggregator = new FloatAvgAggFunction()
}

class DoubleAvgAggFunctionTest extends AvgAggFunctionTestBase[Double, FloatingAvgAccumulator] {

  override def minVal = Float.MinValue

  override def maxVal = Float.MaxValue

  override def aggregator = new DoubleAvgAggFunction()
}

class DecimalAvgAggFunctionTest extends AggFunctionTestBase[BigDecimal, DecimalAvgAccumulator] {

  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq(
      new BigDecimal("987654321000000"),
      new BigDecimal("-0.000000000012345"),
      null,
      new BigDecimal("0.000000000012345"),
      new BigDecimal("-987654321000000"),
      null,
      new BigDecimal("0")
    ),
    Seq(
      new BigDecimal("987654321000000"),
      new BigDecimal("-0.000000000012345"),
      null,
      new BigDecimal("0.000000000012345"),
      new BigDecimal("-987654321000000"),
      null,
      new BigDecimal("5")
    ),
    Seq(
      null,
      null,
      null,
      null
    ),
    Seq(
      new BigDecimal("0.3"),
      new BigDecimal("0.3"),
      new BigDecimal("0.4")
    )
  )

  override def expectedResults: Seq[BigDecimal] = Seq(
    BigDecimal.ZERO,
    BigDecimal.ONE,
    null,
    BigDecimal.ONE.divide(new BigDecimal("3"), MathContext.DECIMAL128)
  )

  override def aggregator: AggregateFunction[BigDecimal, DecimalAvgAccumulator] =
    new DecimalAvgAggFunction(MathContext.DECIMAL128)

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}
