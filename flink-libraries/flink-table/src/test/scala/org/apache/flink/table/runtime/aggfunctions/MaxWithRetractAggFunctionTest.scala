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
import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Integer => JInt, Long => JLong, Short => JShort}

import org.apache.flink.table.api.functions.AggregateFunction
import org.apache.flink.table.api.types.DecimalType
import org.apache.flink.table.dataformat.{BinaryString, Decimal, GenericRow}
import org.apache.flink.table.runtime.functions.aggfunctions._

/**
  * Test case for built-in max with retraction aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class MaxWithRetractAggFunctionTest[T, N: Numeric]
  extends AggFunctionTestBase[T, GenericRow] {

  private val numeric: Numeric[N] = implicitly[Numeric[N]]

  def minVal: T

  def maxVal: T

  override def inputValueSets: Seq[Seq[T]] = Seq(
    Seq(
      numeric.fromInt(1).asInstanceOf[T],
      null.asInstanceOf[T],
      maxVal,
      numeric.fromInt(-99).asInstanceOf[T],
      numeric.fromInt(3).asInstanceOf[T],
      numeric.fromInt(56).asInstanceOf[T],
      numeric.fromInt(0).asInstanceOf[T],
      minVal,
      numeric.fromInt(-20).asInstanceOf[T],
      numeric.fromInt(17).asInstanceOf[T],
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

class ByteMaxWithRetractAggFunctionTest extends MaxWithRetractAggFunctionTest[JByte, Byte] {

  override def minVal = (Byte.MinValue + 1).toByte

  override def maxVal = (Byte.MaxValue - 1).toByte

  override def aggregator: AggregateFunction[JByte, GenericRow] =
    new ByteMaxWithRetractAggFunction()
}

class ShortMaxWithRetractAggFunctionTest extends MaxWithRetractAggFunctionTest[JShort, Short] {

  override def minVal = (Short.MinValue + 1).toShort

  override def maxVal = (Short.MaxValue - 1).toShort

  override def aggregator: AggregateFunction[JShort, GenericRow] =
    new ShortMaxWithRetractAggFunction()
}

class IntMaxWithRetractAggFunctionTest extends MaxWithRetractAggFunctionTest[JInt, Int] {

  override def minVal = Int.MinValue + 1

  override def maxVal = Int.MaxValue - 1

  override def aggregator: AggregateFunction[JInt, GenericRow] =
    new IntMaxWithRetractAggFunction()
}

class LongMaxWithRetractAggFunctionTest extends MaxWithRetractAggFunctionTest[JLong, Long] {

  override def minVal = Long.MinValue + 1

  override def maxVal = Long.MaxValue - 1

  override def aggregator: AggregateFunction[JLong, GenericRow] =
    new LongMaxWithRetractAggFunction()
}

class FloatMaxWithRetractAggFunctionTest extends MaxWithRetractAggFunctionTest[JFloat, Float] {

  override def minVal = Float.MinValue / 2

  override def maxVal = Float.MaxValue / 2

  override def aggregator: AggregateFunction[JFloat, GenericRow] =
    new FloatMaxWithRetractAggFunction()
}

class DoubleMaxWithRetractAggFunctionTest extends MaxWithRetractAggFunctionTest[JDouble, Double] {

  override def minVal = Double.MinValue / 2

  override def maxVal = Double.MaxValue / 2

  override def aggregator: AggregateFunction[JDouble, GenericRow] =
    new DoubleMaxWithRetractAggFunction()
}

class BooleanMaxWithRetractAggFunctionTest
  extends AggFunctionTestBase[JBoolean, GenericRow] {

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

  override def aggregator: AggregateFunction[JBoolean, GenericRow] =
    new BooleanMaxWithRetractAggFunction()

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}

class DecimalMaxWithRetractAggFunctionTest
  extends AggFunctionTestBase[Decimal, GenericRow] {

  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq(
      Decimal.fromBigDecimal(new BigDecimal("1"), 38, 19),
      Decimal.fromBigDecimal(new BigDecimal("1000.000001"), 38, 19),
      Decimal.fromBigDecimal(new BigDecimal("-1"), 38, 19),
      Decimal.fromBigDecimal(new BigDecimal("-999.998999"), 38, 19),
      null,
      Decimal.fromBigDecimal(new BigDecimal("0"), 38, 19),
      Decimal.fromBigDecimal(new BigDecimal("-999.999"), 38, 19),
      null,
      Decimal.fromBigDecimal(new BigDecimal("999.999"), 38, 19)
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
    Decimal.fromBigDecimal(new BigDecimal("1000.000001"), 38, 19),
    null
  )

  override def aggregator: AggregateFunction[Decimal, GenericRow] =
    new DecimalMaxWithRetractAggFunction(new DecimalType(38, 19))

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}

class StringMaxWithRetractAggFunctionTest
  extends AggFunctionTestBase[BinaryString, GenericRow] {

  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq(
      BinaryString.fromString("abc"),
      BinaryString.fromString("def"),
      BinaryString.fromString("ghi"),
      null,
      BinaryString.fromString("jkl"),
      null,
      BinaryString.fromString("zzz")
    ),
    Seq(
      null,
      null
    ),
    Seq(
      BinaryString.fromString("x"),
      null,
      BinaryString.fromString("e")
    )
  )

  override def expectedResults: Seq[BinaryString] = Seq(
    BinaryString.fromString("zzz"),
    null,
    BinaryString.fromString("x")
  )

  override def aggregator: AggregateFunction[BinaryString, GenericRow] =
    new StringMaxWithRetractAggFunction()

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}
