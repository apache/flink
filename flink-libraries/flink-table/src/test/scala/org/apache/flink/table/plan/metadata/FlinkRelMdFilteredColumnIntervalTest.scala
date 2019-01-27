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
package org.apache.flink.table.plan.metadata

import org.apache.flink.table.api.Types
import org.apache.flink.table.functions.sql.ScalarSqlFunctions
import org.apache.flink.table.plan.stats.ValueInterval

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.fun.SqlStdOperatorTable.{EQUALS, GREATER_THAN, IS_FALSE, IS_TRUE, LESS_THAN, LESS_THAN_OR_EQUAL}
import org.junit.Assert.assertEquals
import org.junit.{Before, Test}

class FlinkRelMdFilteredColumnIntervalTest extends FlinkRelMdHandlerTestBase {

  var ts: RelNode = _
  var expr1, expr2, expr3, expr4, expr5, expr6, expr7, expr8, expr9: RexNode = _
  var projects: List[RexNode] = _

  @Before
  def before(): Unit = {
    ts = relBuilder.scan("t1").build()
    relBuilder.push(ts)
    expr1 = relBuilder.call(
      LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2))
    expr2 = relBuilder.call(
      GREATER_THAN, relBuilder.field(0), relBuilder.literal(-1))
    expr3 = relBuilder.call(
      GREATER_THAN,
      relBuilder.call(
        ScalarSqlFunctions.DIV, relBuilder.field(0), relBuilder.literal(2)),
      relBuilder.literal(3))
    expr4 = relBuilder.call(
      LESS_THAN, relBuilder.field(1), relBuilder.literal(1.1D))
    expr5 = relBuilder.call(
      GREATER_THAN, relBuilder.field(0), relBuilder.literal(90))
    expr6 = relBuilder.call(
      LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(-1))
    expr7 = relBuilder.call(
      GREATER_THAN, relBuilder.field(1), relBuilder.literal(1.9D))
    expr8 = relBuilder.call(IS_TRUE, expr4)
    expr9 = relBuilder.call(IS_FALSE, expr4)

    // f0, f1, true, f0 = 1, f0 <= 2, f0 > -1, (f0/2) > 3, f1 < 1.1, f0 > 90, f0 <= -1, f1 > 1.9
    projects = List(
      relBuilder.field(0),
      relBuilder.field(1),
      relBuilder.literal(true),
      relBuilder.call(EQUALS, relBuilder.field(0), relBuilder.literal(1)),
      expr1,
      expr2,
      expr3,
      expr4,
      expr5,
      expr6,
      expr7,
      expr8,
      expr9)
  }

  @Test
  def testGetColumnIntervalOnCalc(): Unit = {
    val outputRowType = typeFactory.buildLogicalRowType(
      Array("f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10", "f11", "f12"),
      Array(Types.INT, Types.DOUBLE, Types.BOOLEAN, Types.BOOLEAN, Types.BOOLEAN, Types.BOOLEAN,
            Types.BOOLEAN, Types.BOOLEAN, Types.BOOLEAN, Types.BOOLEAN,
            Types.BOOLEAN, Types.BOOLEAN, Types.BOOLEAN))
    val calc1 = buildCalc(ts, outputRowType, projects, List(expr1))
    assertEquals(
      ValueInterval(-5, 2), mq.getFilteredColumnInterval(calc1, 0, -1))
    assertEquals(
      ValueInterval(0D, 6.1D), mq.getFilteredColumnInterval(calc1, 1, -1))
    assertEquals(
      ValueInterval(-5, 2), mq.getFilteredColumnInterval(calc1, 0, 2))
    assertEquals(
      ValueInterval(1, 1), mq.getFilteredColumnInterval(calc1, 0, 3))
    assertEquals(
      ValueInterval(-1, 2, includeLower = false), mq.getFilteredColumnInterval(calc1, 0, 5))
    assertEquals(
      ValueInterval(0D, 1.1D, includeUpper = false), mq.getFilteredColumnInterval(calc1, 1, 7))
    assertEquals(
      ValueInterval(0D, 6.1D), mq.getFilteredColumnInterval(calc1, 1, 8))
    assertEquals(
      ValueInterval(-5, -1), mq.getFilteredColumnInterval(calc1, 0, 9))
    assertEquals(
      ValueInterval(1.9D, 6.1D, includeLower = false), mq.getFilteredColumnInterval(calc1, 1, 10))
    assertEquals(
      ValueInterval(0D, 1.1D, includeUpper = false), mq.getFilteredColumnInterval(calc1, 1, 11))
    assertEquals(
      ValueInterval(1.1D, 6.1D), mq.getFilteredColumnInterval(calc1, 1, 12))
  }

  @Test
  def testGetColumnIntervalOnProject(): Unit = {
    val p = relBuilder.project(projects: _*).build()

    assertEquals(
      ValueInterval(-5, 5), mq.getFilteredColumnInterval(p, 0, -1))
    assertEquals(
      ValueInterval(0D, 6.1D), mq.getFilteredColumnInterval(p, 1, -1))
    assertEquals(
      ValueInterval(-5, 5), mq.getFilteredColumnInterval(p, 0, 2))
    assertEquals(
      ValueInterval(1, 1), mq.getFilteredColumnInterval(p, 0, 3))
    assertEquals(
      ValueInterval(-1, 5, includeLower = false), mq.getFilteredColumnInterval(p, 0, 5))
    assertEquals(
      ValueInterval(0D, 1.1D, includeUpper = false), mq.getFilteredColumnInterval(p, 1, 7))
    assertEquals(
      ValueInterval(0D, 6.1D), mq.getFilteredColumnInterval(p, 1, 8))
    assertEquals(
      ValueInterval(-5, -1), mq.getFilteredColumnInterval(p, 0, 9))
    assertEquals(
      ValueInterval(1.9D, 6.1D, includeLower = false), mq.getFilteredColumnInterval(p, 1, 10))
    assertEquals(
      ValueInterval(0D, 1.1D, includeUpper = false), mq.getFilteredColumnInterval(p, 1, 11))
    assertEquals(
      ValueInterval(1.1D, 6.1D), mq.getFilteredColumnInterval(p, 1, 12))
  }

  @Test
  def testGetColumnIntervalOnFilter(): Unit = {
    val filter = relBuilder.project(projects: _*).filter(expr1).build()

    assertEquals(
      ValueInterval(-5, 2), mq.getFilteredColumnInterval(filter, 0, -1))
    assertEquals(
      ValueInterval(0D, 6.1D), mq.getFilteredColumnInterval(filter, 1, -1))
    assertEquals(
      ValueInterval(-5, 2), mq.getFilteredColumnInterval(filter, 0, 2))
    assertEquals(
      ValueInterval(1, 1), mq.getFilteredColumnInterval(filter, 0, 3))
    assertEquals(
      ValueInterval(-1, 2, includeLower = false), mq.getFilteredColumnInterval(filter, 0, 5))
    assertEquals(
      ValueInterval(0D, 1.1D, includeUpper = false), mq.getFilteredColumnInterval(filter, 1, 7))
    assertEquals(
      ValueInterval(0D, 6.1D), mq.getFilteredColumnInterval(filter, 1, 8))
    assertEquals(
      ValueInterval(-5, -1), mq.getFilteredColumnInterval(filter, 0, 9))
    assertEquals(
      ValueInterval(1.9D, 6.1D, includeLower = false), mq.getFilteredColumnInterval(filter, 1, 10))
    assertEquals(
      ValueInterval(0D, 1.1D, includeUpper = false), mq.getFilteredColumnInterval(filter, 1, 11))
    assertEquals(
      ValueInterval(1.1D, 6.1D), mq.getFilteredColumnInterval(filter, 1, 12))
  }

}
