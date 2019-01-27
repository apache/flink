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

package org.apache.flink.table.util

import org.apache.flink.table.plan.stats._
import org.apache.flink.table.plan.util.ColumnIntervalUtil._

import java.lang
import java.math.BigInteger
import java.util.Date

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class ColumnIntervalUtilTest {

  @Test
  def testConvertNumberToString(): Unit = {

    assertEquals(
      Some("1"),
      convertNumberToString(1)
    )
    assertEquals(
      Some("1"),
      convertNumberToString(new lang.Integer(1))
    )
    assertEquals(
      Some("1"),
      convertNumberToString(1L)
    )
    assertEquals(
      Some("1"),
      convertNumberToString(new lang.Long(1L))
    )
    assertEquals(
      Some("1.11"),
      convertNumberToString(1.11f)
    )
    assertEquals(
      Some("1.11"),
      convertNumberToString(new lang.Float(1.11f))
    )
    assertEquals(
      Some("1.11"),
      convertNumberToString(1.11)
    )
    assertEquals(
      Some("1.11"),
      convertNumberToString(new lang.Double(1.11))
    )
    assertEquals(
      Some("1"),
      convertNumberToString(new BigInt(new BigInteger("1")))
    )
    assertEquals(
      Some("1"),
      convertNumberToString(new BigInteger("1"))
    )
    assertEquals(
      Some("1.11"),
      convertNumberToString(new BigDecimal(new java.math.BigDecimal("1.11")))
    )
    assertEquals(
      Some("1.11"),
      convertNumberToString(new java.math.BigDecimal("1.11"))
    )

    assertEquals(
      None,
      convertNumberToString("123")
    )
    assertEquals(
      None,
      convertNumberToString(new Date())
    )

  }

  @Test
  def testNegativeValueInterval(): Unit = {
    assertEquals(
      getNegativeOfValueInterval(ValueInterval(1, 2, true, true)),
      toBigDecimalInterval(ValueInterval(-2, -1, true, true))
    )
    assertEquals(
      getNegativeOfValueInterval(ValueInterval(1, 2, false, true)),
      toBigDecimalInterval(ValueInterval(-2, -1, true, false))
    )
    assertEquals(
      getNegativeOfValueInterval(LeftSemiInfiniteValueInterval(2, true)),
      toBigDecimalInterval(RightSemiInfiniteValueInterval(-2, true))
    )
    assertEquals(
      getNegativeOfValueInterval(RightSemiInfiniteValueInterval(-2, true)),
      toBigDecimalInterval(LeftSemiInfiniteValueInterval(2, true))
    )
    assertEquals(
      getNegativeOfValueInterval(ValueInterval("1", "2", true, true)),
      null
    )
    assertEquals(
      getNegativeOfValueInterval(ValueInterval(new Date(), new Date(), true, true)),
      null
    )
    assertEquals(
      getNegativeOfValueInterval(ValueInterval(1.1f, 2.2f, true, true)),
      toBigDecimalInterval(ValueInterval(-2.2f, -1.1f, true, true))
    )

  }

  @Test
  def testGetValueIntervalOfPlus(): Unit = {
    assertEquals(
      getValueIntervalOfPlus(
        ValueInterval(-1, 2, true, false),
        ValueInterval(3, 4, true, true)
      ),
      toBigDecimalInterval(ValueInterval(2, 6, true, false))
    )
    assertEquals(
      getValueIntervalOfPlus(
        ValueInterval(-1, 2, true, false),
        LeftSemiInfiniteValueInterval(3, true)
      ),
      toBigDecimalInterval(LeftSemiInfiniteValueInterval(5, false))
    )
    assertEquals(
      getValueIntervalOfPlus(
        LeftSemiInfiniteValueInterval(-1, true),
        LeftSemiInfiniteValueInterval(3, true)
      ),
      toBigDecimalInterval(LeftSemiInfiniteValueInterval(2, true))
    )
    assertEquals(
      getValueIntervalOfPlus(
        ValueInterval(-1, 2, true, false),
        RightSemiInfiniteValueInterval(3, false)
      ),
      toBigDecimalInterval(RightSemiInfiniteValueInterval(2, false))
    )
    assertEquals(
      getValueIntervalOfPlus(
        RightSemiInfiniteValueInterval(3, true),
        RightSemiInfiniteValueInterval(3, false)
      ),
      toBigDecimalInterval(RightSemiInfiniteValueInterval(6, false))
    )
    assertEquals(
      getValueIntervalOfPlus(
        EmptyValueInterval,
        ValueInterval(-1, 2, true, false)
      ),
      null
    )
    assertEquals(
      getValueIntervalOfPlus(
        EmptyValueInterval,
        LeftSemiInfiniteValueInterval(3, true)
      ),
      null
    )
    assertEquals(
      getValueIntervalOfPlus(
        EmptyValueInterval,
        RightSemiInfiniteValueInterval(3, false)
      ),
      null
    )
  }

  @Test
  def testGetValueIntervalOfMultiply(): Unit = {
    assertEquals(
      getValueIntervalOfMultiply(
        ValueInterval(-1, 2, true, false),
        ValueInterval(-2, 1, true, false)
      ),
      toBigDecimalInterval(ValueInterval(-4, 2, false, true))
    )

    assertEquals(
      getValueIntervalOfMultiply(
        ValueInterval(-1, 2, true, true),
        ValueInterval(1, 2, true, false)
      ),
      toBigDecimalInterval(ValueInterval(-2, 4, false, false))
    )

    assertEquals(
      getValueIntervalOfMultiply(
        ValueInterval(-2, -1, false, false),
        ValueInterval(-2, -1, false, false)
      ),
      toBigDecimalInterval(ValueInterval(1, 4, false, false))
    )

    assertEquals(
      getValueIntervalOfMultiply(
        ValueInterval(-2, -1, false, false),
        EmptyValueInterval
      ),
      null
    )

    assertEquals(
      getValueIntervalOfMultiply(
        ValueInterval(-2, -1, false, false),
        LeftSemiInfiniteValueInterval(1, false)
      ),
      null
    )

    assertEquals(
      getValueIntervalOfMultiply(
        ValueInterval(-2, -1, false, false),
        RightSemiInfiniteValueInterval(1, false)
      ),
      null
    )

    assertEquals(
      getValueIntervalOfMultiply(
        LeftSemiInfiniteValueInterval(1, false),
        RightSemiInfiniteValueInterval(1, false)
      ),
      null
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
