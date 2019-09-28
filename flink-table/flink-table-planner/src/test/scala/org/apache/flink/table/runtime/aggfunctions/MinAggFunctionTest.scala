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
  * Test case for built-in min aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class MinAggFunctionTest[T: Numeric] extends AggFunctionTestBase[T, MinAccumulator[T]] {

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
    minVal,
    null.asInstanceOf[T]
  )
}

class ByteMinAggFunctionTest extends MinAggFunctionTest[Byte] {

  override def minVal = (Byte.MinValue + 1).toByte

  override def maxVal = (Byte.MaxValue - 1).toByte

  override def aggregator: AggregateFunction[Byte, MinAccumulator[Byte]] =
    new ByteMinAggFunction()
}

class ShortMinAggFunctionTest extends MinAggFunctionTest[Short] {

  override def minVal = (Short.MinValue + 1).toShort

  override def maxVal = (Short.MaxValue - 1).toShort

  override def aggregator: AggregateFunction[Short, MinAccumulator[Short]] =
    new ShortMinAggFunction()
}

class IntMinAggFunctionTest extends MinAggFunctionTest[Int] {

  override def minVal = Int.MinValue + 1

  override def maxVal = Int.MaxValue - 1

  override def aggregator: AggregateFunction[Int, MinAccumulator[Int]] =
    new IntMinAggFunction()
}

class LongMinAggFunctionTest extends MinAggFunctionTest[Long] {

  override def minVal = Long.MinValue + 1

  override def maxVal = Long.MaxValue - 1

  override def aggregator: AggregateFunction[Long, MinAccumulator[Long]] =
    new LongMinAggFunction()
}

class FloatMinAggFunctionTest extends MinAggFunctionTest[Float] {

  override def minVal = Float.MinValue / 2

  override def maxVal = Float.MaxValue / 2

  override def aggregator: AggregateFunction[Float, MinAccumulator[Float]] =
    new FloatMinAggFunction()
}

class DoubleMinAggFunctionTest extends MinAggFunctionTest[Double] {

  override def minVal = Double.MinValue / 2

  override def maxVal = Double.MaxValue / 2

  override def aggregator: AggregateFunction[Double, MinAccumulator[Double]] =
    new DoubleMinAggFunction()
}

class BooleanMinAggFunctionTest extends AggFunctionTestBase[Boolean, MinAccumulator[Boolean]] {

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
    false,
    null.asInstanceOf[Boolean]
  )

  override def aggregator: AggregateFunction[Boolean, MinAccumulator[Boolean]] =
    new BooleanMinAggFunction()
}

class DecimalMinAggFunctionTest
  extends AggFunctionTestBase[BigDecimal, MinAccumulator[BigDecimal]] {

  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq(
      new BigDecimal("1"),
      new BigDecimal("1000"),
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
    new BigDecimal("-999.999"),
    null
  )

  override def aggregator: AggregateFunction[BigDecimal, MinAccumulator[BigDecimal]] =
    new DecimalMinAggFunction()
}

class StringMinAggFunctionTest
  extends AggFunctionTestBase[String, MinAccumulator[String]] {
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
    new String("a"),
    null.asInstanceOf[String],
    new String("1House")
  )

  override def aggregator: AggregateFunction[String, MinAccumulator[String]] =
    new StringMinAggFunction()
}

class TimestampMinAggFunctionTest
  extends AggFunctionTestBase[Timestamp, MinAccumulator[Timestamp]] {
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
    new Timestamp(0),
    null.asInstanceOf[Timestamp]
  )

  override def aggregator: AggregateFunction[Timestamp, MinAccumulator[Timestamp]] =
    new TimestampMinAggFunction()
}

class DateMinAggFunctionTest
  extends AggFunctionTestBase[Date, MinAccumulator[Date]] {
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
    new Date(0),
    null.asInstanceOf[Date]
  )

  override def aggregator: AggregateFunction[Date, MinAccumulator[Date]] =
    new DateMinAggFunction()
}

class TimeMinAggFunctionTest
  extends AggFunctionTestBase[Time, MinAccumulator[Time]] {
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
    new Time(0),
    null.asInstanceOf[Time]
  )

  override def aggregator: AggregateFunction[Time, MinAccumulator[Time]] =
    new TimeMinAggFunction()
}
