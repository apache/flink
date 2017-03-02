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

import java.lang.{Byte => JByte, Short => JShort, Integer => JInt, Long => JLong, Float => JFloat, Double => JDouble}
import java.math.BigDecimal
import org.apache.flink.table.functions.AggregateFunction

/**
  * Test case for built-in average aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class AvgAggFunctionTestBase[T] extends AggFunctionTestBase[T] {

  def minVal: T

  def maxVal: T

  def negMinVal: T

  def negMaxVal: T

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
      prepareValue(0),
      negMaxVal,
      negMinVal,
      null.asInstanceOf[T]
    ),
    Seq(
      prepareValue(1),
      prepareValue(2),
      null.asInstanceOf[T],
      prepareValue(3),
      prepareValue(4),
      prepareValue(5),
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
    prepareValue(0),
    prepareValue(3),
    null.asInstanceOf[T]
  )

  def prepareValue(v: Number): T
}

class ByteAvgAggFunctionTest extends AvgAggFunctionTestBase[JByte] {

  override def minVal = (JByte.MIN_VALUE + 1).toByte

  override def maxVal = (JByte.MAX_VALUE - 1).toByte

  override def negMinVal = (0.toByte - minVal).toByte

  override def negMaxVal = (0.toByte - maxVal).toByte

  override def aggregator = new ByteAvgAggFunction()

  override def prepareValue(v: Number): JByte = {
    v.byteValue()
  }
}

class ShortAvgAggFunctionTest extends AvgAggFunctionTestBase[JShort] {

  override def minVal = (JShort.MIN_VALUE + 1).toShort

  override def maxVal = (JShort.MAX_VALUE - 1).toShort

  override def negMinVal = (0.toShort - minVal).toShort

  override def negMaxVal = (0.toShort - maxVal).toShort

  override def aggregator = new ShortAvgAggFunction()

  override def prepareValue(v: Number): JShort = {
    v.shortValue()
  }
}

class IntAvgAggFunctionTest extends AvgAggFunctionTestBase[JInt] {

  override def minVal = JInt.MIN_VALUE + 1

  override def maxVal = JInt.MAX_VALUE - 1

  override def negMinVal = 0 - minVal

  override def negMaxVal = 0 - maxVal

  override def aggregator = new IntAvgAggFunction()

  override def prepareValue(v: Number): JInt = {
    v.intValue()
  }
}

class LongAvgAggFunctionTest extends AvgAggFunctionTestBase[JLong] {

  override def minVal = JLong.MIN_VALUE + 1

  override def maxVal = JLong.MAX_VALUE - 1

  override def negMinVal = 0.toLong - minVal

  override def negMaxVal = 0.toLong - maxVal

  override def aggregator = new LongAvgAggFunction()

  override def prepareValue(v: Number): JLong = {
    v.longValue()
  }
}

class FloatAvgAggFunctionTest extends AvgAggFunctionTestBase[JFloat] {

  private val numeric: Numeric[Float] = implicitly[Numeric[Float]]

  override def minVal = Float.MinValue

  override def maxVal = Float.MaxValue

  override def negMinVal = numeric.negate(minVal)

  override def negMaxVal = numeric.negate(maxVal)

  override def aggregator = new FloatAvgAggFunction()

  override def prepareValue(v: Number): JFloat = {
    numeric.fromInt(v.asInstanceOf[Int])
  }
}

class DoubleAvgAggFunctionTest extends AvgAggFunctionTestBase[JDouble] {

  private val numeric: Numeric[Double] = implicitly[Numeric[Double]]

  override def minVal = Float.MinValue.toDouble

  override def maxVal = Float.MaxValue.toDouble

  override def negMinVal = numeric.negate(minVal)

  override def negMaxVal = numeric.negate(maxVal)

  override def aggregator = new DoubleAvgAggFunction()

  override def prepareValue(v: Number): JDouble = {
    numeric.fromInt(v.asInstanceOf[Int])
  }
}

class DecimalAvgAggFunctionTest extends AggFunctionTestBase[BigDecimal] {

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
    )
  )

  override def expectedResults: Seq[BigDecimal] = Seq(
    BigDecimal.ZERO,
    BigDecimal.ONE,
    null
  )

  override def aggregator: AggregateFunction[BigDecimal] = new DecimalAvgAggFunction()
}
