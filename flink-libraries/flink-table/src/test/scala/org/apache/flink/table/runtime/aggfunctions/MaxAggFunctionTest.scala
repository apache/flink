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

import java.math.BigDecimal
import java.sql.{Date, Time, Timestamp}

import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.functions.aggfunctions._

/**
  * Test case for built-in max aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class MaxAggFunctionTest[T: Numeric] extends AggFunctionTestBase[T, MaxAccumulator[T]] {

  private val numeric: Numeric[T] = implicitly[Numeric[T]]

  def minVal: T

  def maxVal: T

  override def inputValueSets: Seq[Seq[T]] = Seq(
    Seq(
      numeric.fromInt(1),
      null.asInstanceOf[T],
      maxVal,
      numeric.fromInt(-99),
      numeric.fromInt(3),
      numeric.fromInt(56),
      numeric.fromInt(0),
      minVal,
      numeric.fromInt(-20),
      numeric.fromInt(17),
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
    maxVal,
    null.asInstanceOf[T]
  )
}

class ByteMaxAggFunctionTest extends MaxAggFunctionTest[Byte] {

  override def minVal = (Byte.MinValue + 1).toByte

  override def maxVal = (Byte.MaxValue - 1).toByte

  override def aggregator: AggregateFunction[Byte, MaxAccumulator[Byte]] =
    new ByteMaxAggFunction()
}

class ShortMaxAggFunctionTest extends MaxAggFunctionTest[Short] {

  override def minVal = (Short.MinValue + 1).toShort

  override def maxVal = (Short.MaxValue - 1).toShort

  override def aggregator: AggregateFunction[Short, MaxAccumulator[Short]] =
    new ShortMaxAggFunction()
}

class IntMaxAggFunctionTest extends MaxAggFunctionTest[Int] {

  override def minVal = Int.MinValue + 1

  override def maxVal = Int.MaxValue - 1

  override def aggregator: AggregateFunction[Int, MaxAccumulator[Int]] =
    new IntMaxAggFunction()
}

class LongMaxAggFunctionTest extends MaxAggFunctionTest[Long] {

  override def minVal = Long.MinValue + 1

  override def maxVal = Long.MaxValue - 1

  override def aggregator: AggregateFunction[Long, MaxAccumulator[Long]] =
    new LongMaxAggFunction()
}

class FloatMaxAggFunctionTest extends MaxAggFunctionTest[Float] {

  override def minVal = Float.MinValue / 2

  override def maxVal = Float.MaxValue / 2

  override def aggregator: AggregateFunction[Float, MaxAccumulator[Float]] =
    new FloatMaxAggFunction()
}

class DoubleMaxAggFunctionTest extends MaxAggFunctionTest[Double] {

  override def minVal = Double.MinValue / 2

  override def maxVal = Double.MaxValue / 2

  override def aggregator: AggregateFunction[Double, MaxAccumulator[Double]] =
    new DoubleMaxAggFunction()
}

class BooleanMaxAggFunctionTest extends AggFunctionTestBase[Boolean, MaxAccumulator[Boolean]] {

  override def inputValueSets: Seq[Seq[Boolean]] = Seq(
    Seq(
      false,
      false,
      false
    ),
    Seq(
      true,
      true,
      true
    ),
    Seq(
      true,
      false,
      null.asInstanceOf[Boolean],
      true,
      false,
      true,
      null.asInstanceOf[Boolean]
    ),
    Seq(
      null.asInstanceOf[Boolean],
      null.asInstanceOf[Boolean],
      null.asInstanceOf[Boolean]
    )
  )

  override def expectedResults: Seq[Boolean] = Seq(
    false,
    true,
    true,
    null.asInstanceOf[Boolean]
  )

  override def aggregator: AggregateFunction[Boolean, MaxAccumulator[Boolean]] =
    new BooleanMaxAggFunction()
}

class DecimalMaxAggFunctionTest
  extends AggFunctionTestBase[BigDecimal, MaxAccumulator[BigDecimal]] {

  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq(
      new BigDecimal("1"),
      new BigDecimal("1000.000001"),
      new BigDecimal("-1"),
      new BigDecimal("-999.998999"),
      null,
      new BigDecimal("0"),
      new BigDecimal("-999.999"),
      null,
      new BigDecimal("999.999")
    ),
    Seq(
      null,
      null,
      null,
      null,
      null
    )
  )

  override def expectedResults: Seq[BigDecimal] = Seq(
    new BigDecimal("1000.000001"),
    null
  )

  override def aggregator: AggregateFunction[BigDecimal, MaxAccumulator[BigDecimal]] =
    new DecimalMaxAggFunction()
}

class StringMaxAggFunctionTest extends AggFunctionTestBase[String, MaxAccumulator[String]] {
  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq(
      new String("a"),
      new String("b"),
      new String("c"),
      null.asInstanceOf[String],
      new String("d")
    ),
    Seq(
      null.asInstanceOf[String],
      null.asInstanceOf[String],
      null.asInstanceOf[String]
    ),
    Seq(
      new String("1House"),
      new String("Household"),
      new String("house"),
      new String("household")
    )
  )

  override def expectedResults: Seq[String] = Seq(
    new String("d"),
    null.asInstanceOf[String],
    new String("household")
  )

  override def aggregator: AggregateFunction[String, MaxAccumulator[String]] =
    new StringMaxAggFunction()
}

class TimestampMaxAggFunctionTest
  extends AggFunctionTestBase[Timestamp, MaxAccumulator[Timestamp]] {
  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq(
      new Timestamp(0),
      new Timestamp(1000),
      new Timestamp(100),
      null.asInstanceOf[Timestamp],
      new Timestamp(10)
    ),
    Seq(
      null.asInstanceOf[Timestamp],
      null.asInstanceOf[Timestamp],
      null.asInstanceOf[Timestamp]
    )
  )

  override def expectedResults: Seq[Timestamp] = Seq(
    new Timestamp(1000),
    null.asInstanceOf[Timestamp]
  )

  override def aggregator: AggregateFunction[Timestamp, MaxAccumulator[Timestamp]] =
    new TimestampMaxAggFunction()
}

class DateMaxAggFunctionTest
  extends AggFunctionTestBase[Date, MaxAccumulator[Date]] {
  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq(
      new Date(0),
      new Date(1000),
      new Date(100),
      null.asInstanceOf[Date],
      new Date(10)
    ),
    Seq(
      null.asInstanceOf[Date],
      null.asInstanceOf[Date],
      null.asInstanceOf[Date]
    )
  )

  override def expectedResults: Seq[Date] = Seq(
    new Date(1000),
    null.asInstanceOf[Date]
  )

  override def aggregator: AggregateFunction[Date, MaxAccumulator[Date]] =
    new DateMaxAggFunction()
}

class TimeMaxAggFunctionTest
  extends AggFunctionTestBase[Time, MaxAccumulator[Time]] {
  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq(
      new Time(0),
      new Time(1000),
      new Time(100),
      null.asInstanceOf[Time],
      new Time(10)
    ),
    Seq(
      null.asInstanceOf[Time],
      null.asInstanceOf[Time],
      null.asInstanceOf[Time]
    )
  )

  override def expectedResults: Seq[Time] = Seq(
    new Time(1000),
    null.asInstanceOf[Time]
  )

  override def aggregator: AggregateFunction[Time, MaxAccumulator[Time]] =
    new TimeMaxAggFunction()
}
