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
package org.apache.flink.table.planner.plan.metadata

import org.apache.flink.table.planner.plan.stats.{RightSemiInfiniteValueInterval,ValueInterval}
import org.apache.flink.table.types.logical._

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.fun.SqlStdOperatorTable.{EQUALS, GREATER_THAN, IS_FALSE, IS_TRUE, LESS_THAN, LESS_THAN_OR_EQUAL, DIVIDE}
import org.junit.Assert.{assertEquals, assertNull}
import org.junit.{Before, Test}

import scala.collection.JavaConversions._

class FlinkRelMdFilteredColumnIntervalTest extends FlinkRelMdHandlerTestBase {
  private var ts: RelNode = _
  private var expr1, expr2, expr3, expr4, expr5, expr6, expr7, expr8, expr9: RexNode = _
  private var projects: List[RexNode] = _

  @Before
  def before(): Unit = {
    ts = relBuilder.scan("MyTable3").build()
    relBuilder.push(ts)
    // a <= 2
    expr1 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2))
    // a > -1
    expr2 = relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(-1))
    // a / 2 > 3
    expr3 = relBuilder.call(GREATER_THAN,
      relBuilder.call(DIVIDE, relBuilder.field(0), relBuilder.literal(2)),
      relBuilder.literal(3))
    // b < 1.1
    expr4 = relBuilder.call(LESS_THAN, relBuilder.field(1), relBuilder.literal(1.1D))
    // a > 90
    expr5 = relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(90))
    // a <= -1
    expr6 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(-1))
    // b > 1.9
    expr7 = relBuilder.call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(1.9D))
    // (b < 1.1) is true
    expr8 = relBuilder.call(IS_TRUE, expr4)
    // (b < 1.1) is false
    expr9 = relBuilder.call(IS_FALSE, expr4)

    // a, b, true, a = 1, a <= 2, a > -1, (a /2 ) > 3, b < 1.1, a > 90, a <= -1, b > 1.9,
    // (b < 1.1) is true, (b < 1.1) is false
    projects = List(
      relBuilder.field(0),
      relBuilder.field(1),
      relBuilder.literal(true),
      relBuilder.call(EQUALS, relBuilder.field(0), relBuilder.literal(1)),
      expr1, expr2, expr3, expr4, expr5, expr6, expr7, expr8, expr9)
  }


  @Test
  def testGetColumnIntervalOnProject(): Unit = {
    val p = relBuilder.project(projects: _*).build()

    assertEquals(ValueInterval(-5, 5), mq.getFilteredColumnInterval(p, 0, -1))
    assertEquals(ValueInterval(0D, 6.1D), mq.getFilteredColumnInterval(p, 1, -1))
    assertEquals(ValueInterval(-5, 5), mq.getFilteredColumnInterval(p, 0, 2))
    assertEquals(ValueInterval(1, 1), mq.getFilteredColumnInterval(p, 0, 3))
    assertEquals(ValueInterval(-1, 5, includeLower = false), mq.getFilteredColumnInterval(p, 0, 5))
    assertEquals(
      ValueInterval(0D, 1.1D, includeUpper = false), mq.getFilteredColumnInterval(p, 1, 7))
    assertEquals(ValueInterval(0D, 6.1D), mq.getFilteredColumnInterval(p, 1, 8))
    assertEquals(ValueInterval(-5, -1), mq.getFilteredColumnInterval(p, 0, 9))
    assertEquals(
      ValueInterval(1.9D, 6.1D, includeLower = false), mq.getFilteredColumnInterval(p, 1, 10))
    assertEquals(
      ValueInterval(0D, 1.1D, includeUpper = false), mq.getFilteredColumnInterval(p, 1, 11))
    assertEquals(ValueInterval(1.1D, 6.1D), mq.getFilteredColumnInterval(p, 1, 12))
  }

  @Test
  def testGetColumnIntervalOnFilter(): Unit = {
    val filter = relBuilder.project(projects: _*).filter(expr1).build()

    assertEquals(ValueInterval(-5, 2), mq.getFilteredColumnInterval(filter, 0, -1))
    assertEquals(ValueInterval(0D, 6.1D), mq.getFilteredColumnInterval(filter, 1, -1))
    assertEquals(ValueInterval(-5, 2), mq.getFilteredColumnInterval(filter, 0, 2))
    assertEquals(ValueInterval(1, 1), mq.getFilteredColumnInterval(filter, 0, 3))
    assertEquals(
      ValueInterval(-1, 2, includeLower = false), mq.getFilteredColumnInterval(filter, 0, 5))
    assertEquals(
      ValueInterval(0D, 1.1D, includeUpper = false), mq.getFilteredColumnInterval(filter, 1, 7))
    assertEquals(ValueInterval(0D, 6.1D), mq.getFilteredColumnInterval(filter, 1, 8))
    assertEquals(ValueInterval(-5, -1), mq.getFilteredColumnInterval(filter, 0, 9))
    assertEquals(
      ValueInterval(1.9D, 6.1D, includeLower = false), mq.getFilteredColumnInterval(filter, 1, 10))
    assertEquals(
      ValueInterval(0D, 1.1D, includeUpper = false), mq.getFilteredColumnInterval(filter, 1, 11))
    assertEquals(ValueInterval(1.1D, 6.1D), mq.getFilteredColumnInterval(filter, 1, 12))
  }

  @Test
  def testGetColumnIntervalOnCalc(): Unit = {
    val outputRowType = typeFactory.buildRelNodeRowType(
      Array("f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10", "f11", "f12"),
      Array(new IntType(), new DoubleType(), new BooleanType(), new BooleanType(),
        new BooleanType(), new BooleanType(), new BooleanType(), new BooleanType(),
        new BooleanType(), new BooleanType(), new BooleanType(), new BooleanType(),
        new BooleanType()))
    val calc = createLogicalCalc(ts, outputRowType, projects, List(expr1))
    assertEquals(ValueInterval(-5, 2), mq.getFilteredColumnInterval(calc, 0, -1))
    assertEquals(ValueInterval(0D, 6.1D), mq.getFilteredColumnInterval(calc, 1, -1))
    assertEquals(ValueInterval(-5, 2), mq.getFilteredColumnInterval(calc, 0, 2))
    assertEquals(ValueInterval(1, 1), mq.getFilteredColumnInterval(calc, 0, 3))
    assertEquals(
      ValueInterval(-1, 2, includeLower = false), mq.getFilteredColumnInterval(calc, 0, 5))
    assertEquals(
      ValueInterval(0D, 1.1D, includeUpper = false), mq.getFilteredColumnInterval(calc, 1, 7))
    assertEquals(ValueInterval(0D, 6.1D), mq.getFilteredColumnInterval(calc, 1, 8))
    assertEquals(ValueInterval(-5, -1), mq.getFilteredColumnInterval(calc, 0, 9))
    assertEquals(
      ValueInterval(1.9D, 6.1D, includeLower = false), mq.getFilteredColumnInterval(calc, 1, 10))
    assertEquals(
      ValueInterval(0D, 1.1D, includeUpper = false), mq.getFilteredColumnInterval(calc, 1, 11))
    assertEquals(ValueInterval(1.1D, 6.1D), mq.getFilteredColumnInterval(calc, 1, 12))
  }

  @Test
  def testGetColumnIntervalOnAggregate(): Unit = {
    Array(logicalAgg, flinkLogicalAgg, batchGlobalAggWithoutLocal, batchGlobalAggWithLocal,
      streamGlobalAggWithoutLocal, streamGlobalAggWithLocal).foreach { agg =>
      assertEquals(ValueInterval(12, 18), mq.getFilteredColumnInterval(agg, 0, -1))
      assertNull(mq.getFilteredColumnInterval(agg, 1, -1))
      assertEquals(ValueInterval(2.7, null), mq.getFilteredColumnInterval(agg, 2, -1))
    }
    Array(streamLocalAgg, batchLocalAgg).foreach { agg =>
      assertEquals(ValueInterval(12, 18), mq.getFilteredColumnInterval(streamLocalAgg, 0, -1))
      assertNull(mq.getFilteredColumnInterval(streamLocalAgg, 1, -1))
      assertNull(mq.getFilteredColumnInterval(streamLocalAgg, 2, -1))
      assertEquals(ValueInterval(2.7, null), mq.getFilteredColumnInterval(streamLocalAgg, 3, -1))
    }

    Array(logicalAggWithAuxGroup, flinkLogicalAggWithAuxGroup,
      batchGlobalAggWithoutLocalWithAuxGroup, batchGlobalAggWithLocalWithAuxGroup).foreach { agg =>
      assertEquals(ValueInterval(0, null), mq.getFilteredColumnInterval(agg, 0, -1))
      assertNull(mq.getFilteredColumnInterval(agg, 1, -1))
      assertEquals(ValueInterval(161.0, 172.1), mq.getFilteredColumnInterval(agg, 2, -1))
      assertNull(mq.getFilteredColumnInterval(agg, 3, -1))
    }
  }

  @Test
  def testGetColumnIntervalOnTableAggregate(): Unit = {
    Array(logicalTableAgg, flinkLogicalTableAgg, streamExecTableAgg).foreach {
      agg =>
        assertEquals(
          RightSemiInfiniteValueInterval(0, true),
          mq.getFilteredColumnInterval(agg, 0, -1))
        assertNull(mq.getFilteredColumnInterval(agg, 1, -1))
        assertNull(mq.getFilteredColumnInterval(agg, 2, -1))
    }
  }

  @Test
  def testGetColumnIntervalOnWindowTableAggregate(): Unit = {
    Array(logicalWindowTableAgg, flinkLogicalWindowTableAgg, streamWindowTableAgg).foreach {
      agg =>
        assertEquals(ValueInterval(5, 45), mq.getFilteredColumnInterval(agg, 0, -1))
        assertNull(mq.getFilteredColumnInterval(agg, 1, -1))
        assertNull(mq.getFilteredColumnInterval(agg, 2, -1))
        assertNull(mq.getFilteredColumnInterval(agg, 3, -1))
        assertNull(mq.getFilteredColumnInterval(agg, 4, -1))
        assertNull(mq.getFilteredColumnInterval(agg, 5, -1))
        assertNull(mq.getFilteredColumnInterval(agg, 6, -1))
    }
  }

  @Test
  def testGetColumnIntervalOnUnion(): Unit = {
    Array(logicalUnion, logicalUnionAll).foreach { union =>
      assertNull(mq.getFilteredColumnInterval(union, 0, -1))
      assertNull(mq.getFilteredColumnInterval(union, 1, -1))
      assertNull(mq.getFilteredColumnInterval(union, 2, -1))
    }

    val filter1 = relBuilder.push(ts).project(projects: _*).filter(expr1).build()
    val filter2 = relBuilder.push(ts).project(projects: _*).filter(expr7).build()
    val union = relBuilder.push(filter1).push(filter2).union(true).build()
    assertEquals(ValueInterval(-5, 5), mq.getFilteredColumnInterval(union, 0, -1))
    assertEquals(ValueInterval(0D, 6.1D), mq.getFilteredColumnInterval(union, 1, -1))
    assertEquals(ValueInterval(-5, 5), mq.getFilteredColumnInterval(union, 0, 2))
    assertEquals(ValueInterval(1, 1), mq.getFilteredColumnInterval(union, 0, 3))
  }

  @Test
  def testGetColumnIntervalOnDefault(): Unit = {
    assertNull(mq.getFilteredColumnInterval(testRel, 0, -1))
    assertNull(mq.getFilteredColumnInterval(testRel, 0, 1))
  }

}
