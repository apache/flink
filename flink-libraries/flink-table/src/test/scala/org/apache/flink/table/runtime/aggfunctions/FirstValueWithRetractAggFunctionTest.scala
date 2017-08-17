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

import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.functions.aggfunctions._

/**
  * Test case for built-in first value with retraction aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class FirstValueWithRetractAggFunctionTest[T: Numeric]
  extends AggFunctionTestBase[T, FirstValueWithRetractAccumulator[T]] {

  private val numeric: Numeric[T] = implicitly[Numeric[T]]

  override def inputValueSets: Seq[Seq[T]] = Seq(
    Seq(
      numeric.fromInt(1),
      null.asInstanceOf[T],
      numeric.fromInt(2),
      numeric.fromInt(-10),
      numeric.fromInt(17),
      null.asInstanceOf[T]
    )
  )

  override def expectedResults: Seq[T] = Seq(
    inputValueSets(0)(0)
  )

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}

class ByteFirstValueWithRetractAggFunctionTest extends FirstValueWithRetractAggFunctionTest[Byte] {

  override def aggregator: AggregateFunction[Byte, FirstValueWithRetractAccumulator[Byte]] =
    new ByteFirstValueWithRetractAggFunction
}

class ShortFirstValueWithRetractAggFunctionTest
  extends FirstValueWithRetractAggFunctionTest[Short] {

  override def aggregator: AggregateFunction[Short, FirstValueWithRetractAccumulator[Short]] =
    new ShortFirstValueWithRetractAggFunction
}

class IntFirstValueWithRetractAggFunctionTest extends FirstValueWithRetractAggFunctionTest[Int] {

  override def aggregator: AggregateFunction[Int, FirstValueWithRetractAccumulator[Int]] =
    new IntFirstValueWithRetractAggFunction
}

class LongFirstValueWithRetractAggFunctionTest extends FirstValueWithRetractAggFunctionTest[Long] {

  override def aggregator: AggregateFunction[Long, FirstValueWithRetractAccumulator[Long]] =
    new LongFirstValueWithRetractAggFunction
}

class FloatFirstValueWithRetractAggFunctionTest
  extends FirstValueWithRetractAggFunctionTest[Float] {

  override def aggregator: AggregateFunction[Float, FirstValueWithRetractAccumulator[Float]] =
    new FloatFirstValueWithRetractAggFunction
}

class DoubleFirstValueWithRetractAggFunctionTest
  extends FirstValueWithRetractAggFunctionTest[Double] {

  override def aggregator: AggregateFunction[Double, FirstValueWithRetractAccumulator[Double]] =
    new DoubleFirstValueWithRetractAggFunction
}


class DecimalFirstValueWithRetractAggFunctionTest
  extends AggFunctionTestBase[BigDecimal, FirstValueWithRetractAccumulator[BigDecimal]] {

  override def inputValueSets: Seq[Seq[BigDecimal]] = Seq(
    Seq(
      new BigDecimal("1"),
      new BigDecimal("3"),
      null,
      new BigDecimal("0"),
      new BigDecimal("-1000"),
      null
    )
  )

  override def expectedResults: Seq[BigDecimal] = Seq(
    new BigDecimal("1")
  )

  override def aggregator: AggregateFunction[BigDecimal,
    FirstValueWithRetractAccumulator[BigDecimal]] =
    new DecimalFirstValueWithRetractAggFunction()

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}

class StringFirstValueWithRetractAggFunctionTest
  extends AggFunctionTestBase[String, FirstValueWithRetractAccumulator[String]] {

  override def inputValueSets: Seq[Seq[String]] = Seq(
    Seq(
      "one",
      null,
      "hello",
      null
    )
  )

  override def expectedResults: Seq[String] = Seq(
    inputValueSets(0)(0)
  )

  override def aggregator: AggregateFunction[String, FirstValueWithRetractAccumulator[String]] =
    new StringFirstValueWithRetractAggFunction()

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}
