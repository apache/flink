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

package org.apache.flink.table.planner.utils

import org.apache.flink.table.planner.plan.stats._
import org.apache.flink.table.planner.plan.utils.ColumnIntervalUtil._

import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

import java.lang
import java.math.BigInteger
import java.util.Date

class ColumnIntervalUtilTest {

  @Test
  def testConvertNumberToString(): Unit = {

    assertEquals(Some("1"), convertNumberToString(1))
    assertEquals(Some("1"), convertNumberToString(new lang.Integer(1)))
    assertEquals(Some("1"), convertNumberToString(1L))
    assertEquals(Some("1"), convertNumberToString(new lang.Long(1L)))
    assertEquals(Some("1.11"), convertNumberToString(1.11f))
    assertEquals(Some("1.11"), convertNumberToString(new lang.Float(1.11f)))
    assertEquals(Some("1.11"), convertNumberToString(1.11))
    assertEquals(Some("1.11"), convertNumberToString(new lang.Double(1.11)))
    assertEquals(Some("1"), convertNumberToString(new BigInt(new BigInteger("1"))))
    assertEquals(Some("1"), convertNumberToString(new BigInteger("1")))
    assertEquals(Some("1.11"),
      convertNumberToString(new BigDecimal(new java.math.BigDecimal("1.11"))))
    assertEquals(Some("1.11"), convertNumberToString(new java.math.BigDecimal("1.11")))

    assertEquals(None, convertNumberToString("123"))
    assertEquals(None, convertNumberToString(new Date()))
  }

  @Test
  def testNegativeValueInterval(): Unit = {
    assertEquals(
      toBigDecimalInterval(ValueInterval(-2, -1, includeLower = true, includeUpper = true)),
      getNegativeOfValueInterval(ValueInterval(1, 2, includeLower = true, includeUpper = true))
    )
    assertEquals(
      toBigDecimalInterval(ValueInterval(-2, -1, includeLower = true, includeUpper = false)),
      getNegativeOfValueInterval(ValueInterval(1, 2, includeLower = false, includeUpper = true))
    )
    assertEquals(
      toBigDecimalInterval(RightSemiInfiniteValueInterval(-2, includeLower = true)),
      getNegativeOfValueInterval(LeftSemiInfiniteValueInterval(2, includeUpper = true))
    )
    assertEquals(
      toBigDecimalInterval(LeftSemiInfiniteValueInterval(2, includeUpper = true)),
      getNegativeOfValueInterval(RightSemiInfiniteValueInterval(-2, includeLower = true))
    )
    assertEquals(
      null,
      getNegativeOfValueInterval(ValueInterval("1", "2", includeLower = true, includeUpper = true))
    )
    assertEquals(
      null,
      getNegativeOfValueInterval(
        ValueInterval(new Date(), new Date(), includeLower = true, includeUpper = true))
    )
    assertEquals(
      toBigDecimalInterval(ValueInterval(-2.2f, -1.1f, includeLower = true, includeUpper = true)),
      getNegativeOfValueInterval(
        ValueInterval(1.1f, 2.2f, includeLower = true, includeUpper = true))
    )
  }

  @Test
  def testGetValueIntervalOfPlus(): Unit = {
    assertEquals(
      toBigDecimalInterval(ValueInterval(2, 6, includeLower = true, includeUpper = false)),
      getValueIntervalOfPlus(
        ValueInterval(-1, 2, includeLower = true, includeUpper = false),
        ValueInterval(3, 4, includeLower = true, includeUpper = true))
    )
    assertEquals(
      toBigDecimalInterval(LeftSemiInfiniteValueInterval(5, includeUpper = false)),
      getValueIntervalOfPlus(
        ValueInterval(-1, 2, includeLower = true, includeUpper = false),
        LeftSemiInfiniteValueInterval(3, includeUpper = true))
    )
    assertEquals(
      toBigDecimalInterval(LeftSemiInfiniteValueInterval(2, includeUpper = true)),
      getValueIntervalOfPlus(
        LeftSemiInfiniteValueInterval(-1, includeUpper = true),
        LeftSemiInfiniteValueInterval(3, includeUpper = true))
    )
    assertEquals(
      toBigDecimalInterval(RightSemiInfiniteValueInterval(2, includeLower = false)),
      getValueIntervalOfPlus(
        ValueInterval(-1, 2, includeLower = true, includeUpper = false),
        RightSemiInfiniteValueInterval(3, includeLower = false))
    )
    assertEquals(
      toBigDecimalInterval(RightSemiInfiniteValueInterval(6, includeLower = false)),
      getValueIntervalOfPlus(
        RightSemiInfiniteValueInterval(3, includeLower = true),
        RightSemiInfiniteValueInterval(3, includeLower = false))
    )
    assertEquals(
      null,
      getValueIntervalOfPlus(
        EmptyValueInterval,
        ValueInterval(-1, 2, includeLower = true, includeUpper = false))
    )
    assertEquals(
      null,
      getValueIntervalOfPlus(
        EmptyValueInterval,
        LeftSemiInfiniteValueInterval(3, includeUpper = true))
    )
    assertEquals(
      null,
      getValueIntervalOfPlus(
        EmptyValueInterval,
        RightSemiInfiniteValueInterval(3, includeLower = false))
    )
  }

  @Test
  def testGetValueIntervalOfMultiply(): Unit = {
    assertEquals(
      toBigDecimalInterval(ValueInterval(-4, 2, includeLower = false, includeUpper = true)),
      getValueIntervalOfMultiply(
        ValueInterval(-1, 2, includeLower = true, includeUpper = false),
        ValueInterval(-2, 1, includeLower = true, includeUpper = false))
    )

    assertEquals(
      toBigDecimalInterval(ValueInterval(-2, 4, includeLower = false, includeUpper = false)),
      getValueIntervalOfMultiply(
        ValueInterval(-1, 2, includeLower = true, includeUpper = true),
        ValueInterval(1, 2, includeLower = true, includeUpper = false))
    )

    assertEquals(
      toBigDecimalInterval(ValueInterval(1, 4, includeLower = false, includeUpper = false)),
      getValueIntervalOfMultiply(
        ValueInterval(-2, -1, includeLower = false, includeUpper = false),
        ValueInterval(-2, -1, includeLower = false, includeUpper = false))
    )

    assertEquals(
      null,
      getValueIntervalOfMultiply(
        ValueInterval(-2, -1, includeLower = false, includeUpper = false),
        EmptyValueInterval)
    )

    assertEquals(
      null,
      getValueIntervalOfMultiply(
        ValueInterval(-2, -1, includeLower = false, includeUpper = false),
        LeftSemiInfiniteValueInterval(1, includeUpper = false))
    )

    assertEquals(
      null,
      getValueIntervalOfMultiply(
        ValueInterval(-2, -1, includeLower = false, includeUpper = false),
        RightSemiInfiniteValueInterval(1, includeLower = false))
    )

    assertEquals(
      null,
      getValueIntervalOfMultiply(
        LeftSemiInfiniteValueInterval(1, includeUpper = false),
        RightSemiInfiniteValueInterval(1, includeLower = false))
    )
  }

  @Test
  def testConvertStringToNumber(): Unit = {
    assertEqualsWithType(java.lang.Byte.valueOf("1"), "1")
    assertEqualsWithType(java.lang.Short.valueOf("1"), "1")
    assertEqualsWithType(java.lang.Integer.valueOf("1"), "1")
    assertEqualsWithType(java.lang.Float.valueOf("1"), "1")
    assertEqualsWithType(java.lang.Long.valueOf("1"), "1")
    assertEqualsWithType(java.lang.Double.valueOf("1"), "1")
    assertEqualsWithType(new java.math.BigDecimal("1"), "1")
    assertEqualsWithType(new java.math.BigInteger("1"), "1")
    assertEqualsWithType("1".toByte, "1")
    assertEqualsWithType("1".toShort, "1")
    assertEqualsWithType("1".toInt, "1")
    assertEqualsWithType("1".toLong, "1")
    assertEqualsWithType("1".toFloat, "1")
    assertEqualsWithType("1".toDouble, "1")
    assertEquals(None, convertStringToNumber("1", classOf[java.util.Date]))
  }

  private def assertEqualsWithType(number: Comparable[_], numberStr: String): Unit = {
    val n = convertStringToNumber(numberStr, number.getClass)
    assertTrue(n.isDefined)
    assertTrue(number.getClass == n.get.getClass)
    assertEquals(number, n.get)
  }

}
