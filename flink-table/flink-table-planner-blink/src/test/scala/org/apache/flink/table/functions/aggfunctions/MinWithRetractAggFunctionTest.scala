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
import org.apache.flink.table.functions.aggfunctions.MinWithRetractAggFunction._
import org.apache.flink.table.typeutils.DecimalTypeInfo

import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Long => JLong, Short => JShort}
import java.sql.{Date, Time, Timestamp}

/**
  * Test case for built-in Min with retraction aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class MinWithRetractAggFunctionTest[T]
  extends AggFunctionTestBase[T, MinWithRetractAccumulator[T]] {

  override def accType: Class[_] = classOf[MinWithRetractAccumulator[T]]

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
    minVal,
    null.asInstanceOf[T]
  )

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}

class ByteMinWithRetractAggFunctionTest extends MinWithRetractAggFunctionTest[JByte] {

  override def minVal: JByte = (Byte.MinValue + 1).toByte

  override def maxVal: JByte = (Byte.MaxValue - 1).toByte

  override def getValue(v: String): JByte = JByte.valueOf(v)

  override def aggregator: AggregateFunction[JByte, MinWithRetractAccumulator[JByte]] =
    new ByteMinWithRetractAggFunction()
}

class ShortMinWithRetractAggFunctionTest extends MinWithRetractAggFunctionTest[JShort] {

  override def minVal = (Short.MinValue + 1).toShort

  override def maxVal = (Short.MaxValue - 1).toShort

  override def getValue(v: String): JShort = JShort.valueOf(v)

  override def aggregator: AggregateFunction[JShort, MinWithRetractAccumulator[JShort]] =
    new ShortMinWithRetractAggFunction()
}

class IntMinWithRetractAggFunctionTest extends MinWithRetractAggFunctionTest[Integer] {

  override def minVal = Int.MinValue + 1

  override def maxVal = Int.MaxValue - 1

  override def getValue(v: String): Integer = Integer.valueOf(v)

  override def aggregator: AggregateFunction[Integer, MinWithRetractAccumulator[Integer]] =
    new IntMinWithRetractAggFunction()
}

class LongMinWithRetractAggFunctionTest extends MinWithRetractAggFunctionTest[JLong] {

  override def minVal = Long.MinValue + 1

  override def maxVal = Long.MaxValue - 1

  override def getValue(v: String): JLong = JLong.valueOf(v)

  override def aggregator: AggregateFunction[JLong, MinWithRetractAccumulator[JLong]] =
    new LongMinWithRetractAggFunction()
}

class FloatMinWithRetractAggFunctionTest extends MinWithRetractAggFunctionTest[JFloat] {

  override def minVal = Float.MinValue / 2

  override def maxVal = Float.MaxValue / 2

  override def getValue(v: String): JFloat = JFloat.valueOf(v)

  override def aggregator: AggregateFunction[JFloat, MinWithRetractAccumulator[JFloat]] =
    new FloatMinWithRetractAggFunction()
}

class DoubleMinWithRetractAggFunctionTest extends MinWithRetractAggFunctionTest[JDouble] {

  override def minVal = Double.MinValue / 2

  override def maxVal = Double.MaxValue / 2

  override def getValue(v: String): JDouble = JDouble.valueOf(v)

  override def aggregator: AggregateFunction[JDouble, MinWithRetractAccumulator[JDouble]] =
    new DoubleMinWithRetractAggFunction()
}

class BooleanMinWithRetractAggFunctionTest
  extends AggFunctionTestBase[JBoolean, MinWithRetractAccumulator[JBoolean]] {

  override def accType: Class[_] = classOf[MinWithRetractAccumulator[JBoolean]]

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
    false,
    null.asInstanceOf[JBoolean]
  )

  override def aggregator: AggregateFunction[JBoolean, MinWithRetractAccumulator[JBoolean]] =
    new BooleanMinWithRetractAggFunction()

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}

class DecimalMinWithRetractAggFunctionTest
  extends AggFunctionTestBase[Decimal, MinWithRetractAccumulator[Decimal]] {
  private val precision = 20
  private val scale = 6

  override def accType: Class[_] = classOf[MinWithRetractAccumulator[Decimal]]

  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq(
      Decimal.castFrom("1", precision, scale),
      Decimal.castFrom("1000", precision, scale),
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
    Decimal.castFrom("-999.999", precision, scale),
    null.asInstanceOf[Decimal]
  )

  override def aggregator: AggregateFunction[Decimal, MinWithRetractAccumulator[Decimal]] =
    new DecimalMinWithRetractAggFunction(DecimalTypeInfo.of(precision, scale))

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}

class StringMinWithRetractAggFunctionTest
  extends AggFunctionTestBase[String, MinWithRetractAccumulator[String]] {

  override def accType: Class[_] = classOf[MinWithRetractAccumulator[String]]

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
    "abc",
    null,
    "e"
  )

  override def aggregator: AggregateFunction[String, MinWithRetractAccumulator[String]] =
    new StringMinWithRetractAggFunction()

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}

class TimestampMinWithRetractAggFunctionTest
  extends AggFunctionTestBase[Timestamp, MinWithRetractAccumulator[Timestamp]] {

  override def accType: Class[_] = classOf[MinWithRetractAccumulator[Timestamp]]

  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq(
      new Timestamp(0),
      new Timestamp(1000),
      new Timestamp(100),
      null.asInstanceOf[Timestamp],
      new Timestamp(10)
    ),
    Seq(
      null,
      null,
      null,
      null,
      null
    )
  )

  override def expectedResults: Seq[Timestamp] = Seq(
    new Timestamp(0),
    null
  )

  override def aggregator: AggregateFunction[Timestamp, MinWithRetractAccumulator[Timestamp]] =
    new TimestampMinWithRetractAggFunction()

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}

class DateMinWithRetractAggFunctionTest
  extends AggFunctionTestBase[Date, MinWithRetractAccumulator[Date]] {

  override def accType: Class[_] = classOf[MinWithRetractAccumulator[Date]]

  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq(
      new Date(0),
      new Date(1000),
      new Date(100),
      null.asInstanceOf[Date],
      new Date(10)
    ),
    Seq(
      null,
      null,
      null,
      null,
      null
    )
  )

  override def expectedResults: Seq[Date] = Seq(
    new Date(0),
    null
  )

  override def aggregator: AggregateFunction[Date, MinWithRetractAccumulator[Date]] =
    new DateMinWithRetractAggFunction()

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}

class TimeMinWithRetractAggFunctionTest
  extends AggFunctionTestBase[Time, MinWithRetractAccumulator[Time]] {

  override def accType: Class[_] = classOf[MinWithRetractAccumulator[Time]]

  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq(
      new Time(0),
      new Time(1000),
      new Time(100),
      null.asInstanceOf[Time],
      new Time(10)
    ),
    Seq(
      null,
      null,
      null,
      null,
      null
    )
  )

  override def expectedResults: Seq[Time] = Seq(
    new Time(0),
    null
  )

  override def aggregator: AggregateFunction[Time, MinWithRetractAccumulator[Time]] =
    new TimeMinWithRetractAggFunction()

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}
