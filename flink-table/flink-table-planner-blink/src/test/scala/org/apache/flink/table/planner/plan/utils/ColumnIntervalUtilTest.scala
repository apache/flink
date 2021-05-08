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

package org.apache.flink.table.planner.plan.utils

import org.apache.flink.table.planner.calcite.{FlinkRexBuilder, FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.planner.plan.stats._
import org.apache.flink.table.planner.plan.utils.ColumnIntervalUtil._

import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.junit.Assert.{assertEquals, assertNull}
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
  def testGetColumnIntervalWithFilter(): Unit = {
    val typeFactory: FlinkTypeFactory = new FlinkTypeFactory(new FlinkTypeSystem)
    val rexBuilder: RexBuilder = new FlinkRexBuilder(typeFactory)

    // ($1 >= 1 and $1 < 10) or (not($1 > 5)
    val predicate = rexBuilder.makeCall(
      SqlStdOperatorTable.OR,
      rexBuilder.makeCall(
        SqlStdOperatorTable.AND,
        rexBuilder.makeCall(
          SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
          rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 1),
          rexBuilder.makeBigintLiteral(java.math.BigDecimal.valueOf(1))),
        rexBuilder.makeCall(
          SqlStdOperatorTable.LESS_THAN,
          rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 1),
          rexBuilder.makeBigintLiteral(java.math.BigDecimal.valueOf(10)))),
      rexBuilder.makeCall(
        SqlStdOperatorTable.NOT,
        rexBuilder.makeCall(
          SqlStdOperatorTable.GREATER_THAN,
          rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 1),
          rexBuilder.makeBigintLiteral(java.math.BigDecimal.valueOf(5))))
    )

    assertEquals(
      toBigDecimalInterval(ValueInterval.apply(null, 10L, includeUpper = false)),
      ColumnIntervalUtil.getColumnIntervalWithFilter(
        None,
        predicate,
        1,
        rexBuilder))

    assertEquals(
      toBigDecimalInterval(ValueInterval.apply(3L, 8L, includeLower = false, includeUpper = false)),
      ColumnIntervalUtil.getColumnIntervalWithFilter(
        Some(toBigDecimalInterval(
          ValueInterval.apply(3L, 8L, includeLower = false, includeUpper = false))),
        predicate,
        1,
        rexBuilder))

    assertEquals(
      ValueInterval.empty,
      ColumnIntervalUtil.getColumnIntervalWithFilter(
        None,
        rexBuilder.makeLiteral(false),
        0,
        rexBuilder))

    assertEquals(
      ValueInterval.empty,
      ColumnIntervalUtil.getColumnIntervalWithFilter(
        Some(ValueInterval.apply(1L, 10L)),
        rexBuilder.makeLiteral(false),
        0,
        rexBuilder))

    assertNull(
      ColumnIntervalUtil.getColumnIntervalWithFilter(
        None,
        rexBuilder.makeLiteral(true),
        0,
        rexBuilder))

    assertEquals(
      ValueInterval.apply(1L, 10L),
      ColumnIntervalUtil.getColumnIntervalWithFilter(
        Some(ValueInterval.apply(1L, 10L)),
        rexBuilder.makeLiteral(true),
        0,
        rexBuilder))

    assertNull(
      ColumnIntervalUtil.getColumnIntervalWithFilter(
        None,
        rexBuilder.makeBigintLiteral(java.math.BigDecimal.ONE),
        0,
        rexBuilder))
  }
}
