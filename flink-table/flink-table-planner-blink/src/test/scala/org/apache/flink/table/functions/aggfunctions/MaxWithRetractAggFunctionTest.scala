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

import org.apache.flink.table.dataformat.Decimal
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.functions.aggfunctions.MaxWithRetractAggFunction._
import org.apache.flink.table.typeutils.DecimalTypeInfo

import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Long => JLong, Short => JShort}
import java.sql.{Date, Time, Timestamp}

/**
  * Test case for built-in max with retraction aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class MaxWithRetractAggFunctionTest[T]
  extends AggFunctionTestBase[T, MaxWithRetractAccumulator[T]] {

  override def accType: Class[_] = classOf[MaxWithRetractAccumulator[T]]

  def minVal: T

  def maxVal: T

  def getValue(v: String): T

  override def inputValueSets: Seq[Seq[T]] = Seq(
    Seq(
      getValue("1"),
      null.asInstanceOf[T],
      maxVal,
      getValue("-99"),
      getValue("3"),
      getValue("56"),
      getValue("0"),
      minVal,
      getValue("-20"),
      getValue("17"),
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

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}

class ByteMaxWithRetractAggFunctionTest extends MaxWithRetractAggFunctionTest[JByte] {

  override def minVal = (Byte.MinValue + 1).toByte

  override def maxVal = (Byte.MaxValue - 1).toByte

  override def getValue(v: String): JByte = JByte.valueOf(v)

  override def aggregator: AggregateFunction[JByte, MaxWithRetractAccumulator[JByte]] =
    new ByteMaxWithRetractAggFunction()
}

class ShortMaxWithRetractAggFunctionTest extends MaxWithRetractAggFunctionTest[JShort] {

  override def minVal = (Short.MinValue + 1).toShort

  override def maxVal = (Short.MaxValue - 1).toShort

  override def getValue(v: String): JShort = JShort.valueOf(v)

  override def aggregator: AggregateFunction[JShort, MaxWithRetractAccumulator[JShort]] =
    new ShortMaxWithRetractAggFunction()
}

class IntMaxWithRetractAggFunctionTest extends MaxWithRetractAggFunctionTest[Integer] {

  override def minVal = Int.MinValue + 1

  override def maxVal = Int.MaxValue - 1

  override def getValue(v: String): Integer = Integer.valueOf(v)

  override def aggregator: AggregateFunction[Integer, MaxWithRetractAccumulator[Integer]] =
    new IntMaxWithRetractAggFunction()
}

class LongMaxWithRetractAggFunctionTest extends MaxWithRetractAggFunctionTest[JLong] {

  override def minVal = Long.MinValue + 1

  override def maxVal = Long.MaxValue - 1

  override def getValue(v: String): JLong = JLong.valueOf(v)

  override def aggregator: AggregateFunction[JLong, MaxWithRetractAccumulator[JLong]] =
    new LongMaxWithRetractAggFunction()
}

class FloatMaxWithRetractAggFunctionTest extends MaxWithRetractAggFunctionTest[JFloat] {

  override def minVal = Float.MinValue / 2

  override def maxVal = Float.MaxValue / 2

  override def getValue(v: String): JFloat = JFloat.valueOf(v)

  override def aggregator: AggregateFunction[JFloat, MaxWithRetractAccumulator[JFloat]] =
    new FloatMaxWithRetractAggFunction()
}

class DoubleMaxWithRetractAggFunctionTest extends MaxWithRetractAggFunctionTest[JDouble] {

  override def minVal = Double.MinValue / 2

  override def maxVal = Double.MaxValue / 2

  override def getValue(v: String): JDouble = JDouble.valueOf(v)

  override def aggregator: AggregateFunction[JDouble, MaxWithRetractAccumulator[JDouble]] =
    new DoubleMaxWithRetractAggFunction()
}

class BooleanMaxWithRetractAggFunctionTest
  extends AggFunctionTestBase[JBoolean, MaxWithRetractAccumulator[JBoolean]] {

  override def accType: Class[_] = classOf[MaxWithRetractAccumulator[JBoolean]]

  override def inputValueSets: Seq[Seq[JBoolean]] = Seq(
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
      null.asInstanceOf[JBoolean],
      true,
      false,
      true,
      null.asInstanceOf[JBoolean]
    ),
    Seq(
      null.asInstanceOf[JBoolean],
      null.asInstanceOf[JBoolean],
      null.asInstanceOf[JBoolean]
    )
  )

  override def expectedResults: Seq[JBoolean] = Seq(
    false,
    true,
    true,
    null.asInstanceOf[JBoolean]
  )

  override def aggregator: AggregateFunction[JBoolean, MaxWithRetractAccumulator[JBoolean]] =
    new BooleanMaxWithRetractAggFunction()

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}

class DecimalMaxWithRetractAggFunctionTest
  extends AggFunctionTestBase[Decimal, MaxWithRetractAccumulator[Decimal]] {
  private val precision = 20
  private val scale = 6

  override def accType: Class[_] = classOf[MaxWithRetractAccumulator[Decimal]]

  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq(
      Decimal.castFrom("1", precision, scale),
      Decimal.castFrom("1000.000001", precision, scale),
      Decimal.castFrom("-1", precision, scale),
      Decimal.castFrom("-999.998999", precision, scale),
      null,
      Decimal.castFrom("0", precision, scale),
      Decimal.castFrom("-999.999", precision, scale),
      null,
      Decimal.castFrom("999.999", precision, scale)
    ),
    Seq(
      null,
      null,
      null,
      null,
      null
    )
  )

  override def expectedResults: Seq[Decimal] = Seq(
    Decimal.castFrom("1000.000001", precision, scale),
    null.asInstanceOf[Decimal]
  )

  override def aggregator: AggregateFunction[Decimal, MaxWithRetractAccumulator[Decimal]] =
    new DecimalMaxWithRetractAggFunction(DecimalTypeInfo.of(precision, scale))

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}

class StringMaxWithRetractAggFunctionTest
  extends AggFunctionTestBase[String, MaxWithRetractAccumulator[String]] {

  override def accType: Class[_] = classOf[MaxWithRetractAccumulator[String]]

  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq(
      "abc",
      "def",
      "ghi",
      null,
      "jkl",
      null,
      "zzz"
    ),
    Seq(
      null,
      null
    ),
    Seq(
      "x",
      null,
      "e"
    )
  )

  override def expectedResults: Seq[String] = Seq(
    "zzz",
    null,
    "x"
  )

  override def aggregator: AggregateFunction[String, MaxWithRetractAccumulator[String]] =
    new StringMaxWithRetractAggFunction()

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}

class TimestampMaxWithRetractAggFunctionTest
  extends AggFunctionTestBase[Timestamp, MaxWithRetractAccumulator[Timestamp]] {

  override def accType: Class[_] = classOf[MaxWithRetractAccumulator[Timestamp]]

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

  override def aggregator: AggregateFunction[Timestamp, MaxWithRetractAccumulator[Timestamp]] =
    new TimestampMaxWithRetractAggFunction()

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}

class DateMaxWithRetractAggFunctionTest
  extends AggFunctionTestBase[Date, MaxWithRetractAccumulator[Date]] {

  override def accType: Class[_] = classOf[MaxWithRetractAccumulator[Date]]

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

  override def aggregator: AggregateFunction[Date, MaxWithRetractAccumulator[Date]] =
    new DateMaxWithRetractAggFunction()

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}

class TimeMaxWithRetractAggFunctionTest
  extends AggFunctionTestBase[Time, MaxWithRetractAccumulator[Time]] {

  override def accType: Class[_] = classOf[MaxWithRetractAccumulator[Time]]

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

  override def aggregator: AggregateFunction[Time, MaxWithRetractAccumulator[Time]] =
    new TimeMaxWithRetractAggFunction()

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}
