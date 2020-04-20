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

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecRank
import org.apache.flink.table.planner.plan.stats._
import org.apache.flink.table.planner.plan.utils.ColumnIntervalUtil
import org.apache.flink.table.planner.{JBoolean, JDouble}
import org.apache.flink.table.types.logical.IntType

import org.apache.calcite.rel.RelDistributions
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.logical.LogicalExchange
import org.apache.calcite.rex.{RexCall, RexUtil}
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.util.{DateString, TimeString, TimestampString}
import org.junit.Assert._
import org.junit.Test

import java.sql.{Date, Time, Timestamp}

import scala.collection.JavaConversions._

class FlinkRelMdColumnIntervalTest extends FlinkRelMdHandlerTestBase {

  @Test
  def testGetColumnIntervalOnTableScan(): Unit = {
    Array(studentLogicalScan, studentFlinkLogicalScan, studentBatchScan, studentStreamScan)
      .foreach { scan =>
        assertEquals(ValueInterval(0, null), mq.getColumnInterval(scan, 0))
        assertNull(mq.getColumnInterval(scan, 1))
        assertEquals(ValueInterval(2.7D, 4.8D), mq.getColumnInterval(scan, 2))
        assertEquals(ValueInterval(12, 18), mq.getColumnInterval(scan, 3))
        assertEquals(ValueInterval(161.0D, 172.1D), mq.getColumnInterval(scan, 4))
        assertNull(mq.getColumnInterval(scan, 5))
        assertNull(mq.getColumnInterval(scan, 6))
      }

    Array(empLogicalScan, empFlinkLogicalScan, empBatchScan, empStreamScan).foreach { scan =>
      (0 until 8).foreach { index =>
        assertNull(mq.getColumnInterval(scan, index))
      }
    }
  }

  @Test
  def testGetColumnIntervalOnValues(): Unit = {
    (0 until emptyValues.getRowType.getFieldCount).foreach { idx =>
      assertEquals(ValueInterval.empty, mq.getColumnInterval(emptyValues, idx))
    }

    assertEquals(ValueInterval(1L, 3L), mq.getColumnInterval(logicalValues, 0))
    assertEquals(ValueInterval(false, true), mq.getColumnInterval(logicalValues, 1))
    assertEquals(ValueInterval(
      new Date(new DateString(2017, 9, 1).getMillisSinceEpoch),
      new Date(new DateString(2017, 10, 2).getMillisSinceEpoch)),
      mq.getColumnInterval(logicalValues, 2))
    assertEquals(ValueInterval(
      new Time(new TimeString(9, 59, 59).toCalendar.getTimeInMillis),
      new Time(new TimeString(10, 0, 2).toCalendar.getTimeInMillis)),
      mq.getColumnInterval(logicalValues, 3))
    assertEquals(ValueInterval(
      new Timestamp(new TimestampString(2017, 7, 1, 1, 0, 0).getMillisSinceEpoch),
      new Timestamp(new TimestampString(2017, 10, 1, 1, 0, 0).getMillisSinceEpoch)),
      mq.getColumnInterval(logicalValues, 4))
    assertEquals(ValueInterval(-1D, 3.12D), mq.getColumnInterval(logicalValues, 5))
    assertEquals(ValueInterval.empty, mq.getColumnInterval(logicalValues, 6))
    assertEquals(ValueInterval("F", "xyz"), mq.getColumnInterval(logicalValues, 7))
  }

  @Test
  def testGetColumnIntervalOnSnapshot(): Unit = {
    (0 until flinkLogicalSnapshot.getRowType.getFieldCount).foreach { idx =>
      assertNull(mq.getColumnInterval(flinkLogicalSnapshot, idx))
    }
  }

  @Test
  def testGetColumnIntervalOnProject(): Unit = {
    assertEquals(ValueInterval(0, null), mq.getColumnInterval(logicalProject, 0))
    assertNull(mq.getColumnInterval(logicalProject, 1))
    assertEqualsAsDouble(ValueInterval(2.9, 5.0), mq.getColumnInterval(logicalProject, 2))
    assertEqualsAsDouble(ValueInterval(11, 17), mq.getColumnInterval(logicalProject, 3))
    assertEqualsAsDouble(ValueInterval(177.1, 189.31), mq.getColumnInterval(logicalProject, 4))
    assertNull(mq.getColumnInterval(logicalProject, 5))
    assertEqualsAsDouble(ValueInterval(161.0D, 172.1), mq.getColumnInterval(logicalProject, 6))
    assertEquals(ValueInterval(1, 2), mq.getColumnInterval(logicalProject, 7))
    assertEquals(ValueInterval(true, true), mq.getColumnInterval(logicalProject, 8))
    assertEquals(ValueInterval(2.1D, 2.1D), mq.getColumnInterval(logicalProject, 9))
    assertEquals(ValueInterval(2L, 2L), mq.getColumnInterval(logicalProject, 10))
    assertNull(mq.getColumnInterval(logicalProject, 11))

    // 3 * (score - 2)
    val project = relBuilder.scan("student")
      .project(
        relBuilder.call(
          MULTIPLY,
          relBuilder.literal(3),
          relBuilder.call(MINUS, relBuilder.field(2), relBuilder.literal(2))
        )
      ).build()

    assertEqualsAsDouble(ValueInterval(2.1, 8.4), mq.getColumnInterval(project, 0))
  }

  @Test
  def testGetColumnIntervalOnFilter(): Unit = {
    val ts = relBuilder.scan("student").build()
    relBuilder.push(ts)
    // id > 10
    val expr0 = relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(-1))
    // id <= 20
    val expr1 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(20))
    // id > 10
    val expr2 = relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(10))
    // DIV(id, 2) > 3
    val expr3 = relBuilder.call(GREATER_THAN,
      relBuilder.call(DIVIDE, relBuilder.field(0), relBuilder.literal(2)),
      relBuilder.literal(3))
    // score < 4.1
    val expr4 = relBuilder.call(LESS_THAN, relBuilder.field(2), relBuilder.literal(4.1D))
    // score > 6.0
    val expr5 = relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(6.0))
    // score <= 4.0
    val expr6 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(2), relBuilder.literal(4.0))
    // score > 1.9
    val expr7 = relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(1.9D))

    // id > -1
    val filter0 = relBuilder.push(ts).filter(expr0).build
    assertEquals(ValueInterval(0, null), mq.getColumnInterval(filter0, 0))

    // id <= 20
    val filter1 = relBuilder.push(ts).filter(expr1).build
    assertEquals(ValueInterval(0, 20), mq.getColumnInterval(filter1, 0))

    // id <= 20 AND id > 10 AND DIV(id, 2) > 3
    val filter2 = relBuilder.push(ts).filter(expr1, expr2, expr3).build
    assertEquals(ValueInterval(10, 20, includeLower = false), mq.getColumnInterval(filter2, 0))

    // id <= 20 AND id > 10 AND score < 4.1
    val filter3 = relBuilder.push(ts).filter(expr1, expr2, expr4).build
    assertEquals(ValueInterval(10, 20, includeLower = false), mq.getColumnInterval(filter3, 0))

    // score > 6.0 OR score <= 4.0
    val filter4 = relBuilder.push(ts).filter(relBuilder.call(OR, expr5, expr6)).build
    assertEquals(ValueInterval(2.7, 4.0), mq.getColumnInterval(filter4, 2))

    // score > 6.0 OR score <= 4.0 OR id < 20
    val filter5 = relBuilder.push(ts).filter(relBuilder.call(OR, expr5, expr6, expr1)).build
    assertEquals(ValueInterval(2.7, 4.8), mq.getColumnInterval(filter5, 2))

    // (id <= 20 AND score < 4.1) OR NOT(DIV(id, 2) > 3 OR score > 1.9)
    val filter6 = relBuilder.push(ts).filter(relBuilder.call(OR,
      relBuilder.call(AND, expr1, expr4),
      relBuilder.call(NOT, relBuilder.call(OR, expr3, expr7)))).build
    assertEquals(ValueInterval(0, null), mq.getColumnInterval(filter6, 0))

    // (id <= 20 AND score < 4.1) OR NOT(id <= 20 OR score > 1.9)
    val filter7 = relBuilder.push(ts).filter(relBuilder.call(OR,
      relBuilder.call(AND, expr1, expr4),
      relBuilder.call(NOT,
        relBuilder.call(OR,
          RexUtil.negate(relBuilder.getRexBuilder, expr1.asInstanceOf[RexCall]),
          expr7)))).build
    assertEquals(ValueInterval(0, 20), mq.getColumnInterval(filter7, 0))
  }

  @Test
  def testGetColumnIntervalOnCalc(): Unit = {
    relBuilder.push(studentLogicalScan)
    val outputRowType = logicalProject.getRowType
    // id, name, score + 0.2, age - 1, height * 1.1 as h1, height / 0.9 as h2,
    // case sex = 'M' then 1 else 2, true, 2.1, 2, cast(score as double not null) as s
    val projects = logicalProject.getProjects

    // id <= 20
    val expr1 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(20))
    // id > 10
    val expr2 = relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(10))
    // DIV(id, 2) > 3
    val expr3 = relBuilder.call(GREATER_THAN,
      relBuilder.call(DIVIDE, relBuilder.field(0), relBuilder.literal(2)),
      relBuilder.literal(3))
    // score < 4.1
    val expr4 = relBuilder.call(LESS_THAN, relBuilder.field(2), relBuilder.literal(4.1D))
    // score > 6.0
    val expr5 = relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(6.0))
    // score <= 4.0
    val expr6 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(2), relBuilder.literal(4.0))
    // score > 1.9
    val expr7 = relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(1.9D))

    // calc => projects + filter(id <= 20)
    val calc1 = createLogicalCalc(studentLogicalScan, outputRowType, projects, List(expr1))
    assertEquals(ValueInterval(0, 20), mq.getColumnInterval(calc1, 0))
    assertNull(mq.getColumnInterval(calc1, 1))
    assertEqualsAsDouble(ValueInterval(2.9, 5.0), mq.getColumnInterval(calc1, 2))
    assertEqualsAsDouble(ValueInterval(11, 17), mq.getColumnInterval(calc1, 3))
    assertEqualsAsDouble(ValueInterval(177.1, 189.31), mq.getColumnInterval(calc1, 4))
    assertNull(mq.getColumnInterval(calc1, 5))
    assertEqualsAsDouble(ValueInterval(161.0D, 172.1), mq.getColumnInterval(calc1, 6))
    assertEquals(ValueInterval(1, 2), mq.getColumnInterval(calc1, 7))
    assertEquals(ValueInterval(true, true), mq.getColumnInterval(calc1, 8))
    assertEquals(ValueInterval(2.1D, 2.1D), mq.getColumnInterval(calc1, 9))
    assertEquals(ValueInterval(2L, 2L), mq.getColumnInterval(calc1, 10))
    assertNull(mq.getColumnInterval(calc1, 11))

    // calc => project + filter(id <= 20 AND id > 10 AND DIV(id, 2) > 3)
    val calc2 = createLogicalCalc(
      studentLogicalScan, outputRowType, projects, List(expr1, expr2, expr3))
    assertEquals(ValueInterval(10, 20, includeLower = false), mq.getColumnInterval(calc2, 0))
    assertNull(mq.getColumnInterval(calc2, 1))

    // calc => project + filter(id <= 20 AND id > 10 AND score < 4.1)
    val calc3 = createLogicalCalc(
      studentLogicalScan, outputRowType, projects, List(expr1, expr2, expr4))
    assertEquals(ValueInterval(10, 20, includeLower = false), mq.getColumnInterval(calc3, 0))

    // calc => project + filter(score > 6.0 OR score <= 4.0)
    val calc4 = createLogicalCalc(
      studentLogicalScan, outputRowType, projects, List(relBuilder.call(OR, expr5, expr6)))
    assertEqualsAsDouble(ValueInterval(2.9, 5.0), mq.getColumnInterval(calc4, 2))

    // calc => project + filter(score > 6.0 OR score <= 4.0 OR id < 20)
    val calc5 = createLogicalCalc(studentLogicalScan, outputRowType, projects,
      List(relBuilder.call(OR, expr5, expr6, expr1)))
    assertEqualsAsDouble(ValueInterval(2.9, 5.0), mq.getColumnInterval(calc5, 2))

    // calc => project + filter((id <= 20 AND score < 4.1) OR NOT(DIV(id, 2) > 3 OR score > 1.9))
    val calc6 = createLogicalCalc(studentLogicalScan, outputRowType, projects,
      List(relBuilder.call(OR,
        relBuilder.call(AND, expr1, expr4),
        relBuilder.call(NOT, relBuilder.call(OR, expr3, expr7)))))
    assertEquals(ValueInterval(0, null), mq.getColumnInterval(calc6, 0))

    // calc => project + filter: ($0 <=2 and $1 < 1.1) or not( $0>2 or $1 > 1.9)
    val calc7 = createLogicalCalc(studentLogicalScan, outputRowType, projects,
      List(relBuilder.call(OR,
        relBuilder.call(AND, expr1, expr4),
        relBuilder.call(NOT,
          relBuilder.call(OR,
            RexUtil.negate(relBuilder.getRexBuilder, expr1.asInstanceOf[RexCall]),
            expr7)))))
    assertEquals(ValueInterval(0, 20), mq.getColumnInterval(calc7, 0))

    relBuilder.push(studentLogicalScan)
    val expr8 = relBuilder.call(CASE, expr5, relBuilder.literal(1), relBuilder.literal(0))
    val expr9 = relBuilder.call(CASE, expr5, relBuilder.literal(11),
      expr7, relBuilder.literal(10), relBuilder.literal(12))
    val expr10 = relBuilder.call(CASE, expr2, expr9, expr4, expr8, relBuilder.literal(null))
    val expr11 = relBuilder.call(CASE, expr5, relBuilder.literal(1), relBuilder.field(3))
    // TODO add tests for IF
    val rowType = typeFactory.buildRelNodeRowType(
      Array("f0", "f1", "f2", "f3"),
      Array(new IntType(), new IntType(), new IntType(), new IntType()))
    val calc8 = createLogicalCalc(
      studentLogicalScan, rowType, List(expr8, expr9, expr10, expr11), List())

    assertEquals(ValueInterval(0, 1), mq.getColumnInterval(calc8, 0))
    assertEquals(ValueInterval(10, 12), mq.getColumnInterval(calc8, 1))
    assertEquals(ValueInterval(0, 12), mq.getColumnInterval(calc8, 2))
    assertEquals(ValueInterval(1, 18), mq.getColumnInterval(calc8, 3))
  }

  @Test
  def testGetColumnIntervalOnExpand(): Unit = {
    Array(logicalExpand, flinkLogicalExpand, batchExpand, streamExpand).foreach {
      expand =>
        assertEquals(ValueInterval(0, null), mq.getColumnInterval(expand, 0))
        assertNull(mq.getColumnInterval(expand, 1))
        assertEquals(ValueInterval(2.7, 4.8), mq.getColumnInterval(expand, 2))
        assertEquals(ValueInterval(12, 18), mq.getColumnInterval(expand, 3))
        assertEquals(ValueInterval(161.0, 172.1), mq.getColumnInterval(expand, 4))
        assertEquals(null, mq.getColumnInterval(expand, 5))
        assertEquals(null, mq.getColumnInterval(expand, 6))
        assertEquals(ValueInterval(0, 5), mq.getColumnInterval(expand, 7))
    }
  }

  @Test
  def testGetColumnIntervalOnSort(): Unit = {
    Array(logicalSort, flinkLogicalSort, batchSort, streamSort,
      logicalLimit, flinkLogicalLimit, batchLimit, batchLocalLimit, batchGlobalLimit, streamLimit,
      logicalSortLimit, flinkLogicalSortLimit, batchSortLimit, batchLocalSortLimit,
      batchGlobalSortLimit, streamSortLimit).foreach {
      sort =>
        assertEquals(ValueInterval(0, null), mq.getColumnInterval(sort, 0))
        assertNull(mq.getColumnInterval(sort, 1))
        assertEquals(ValueInterval(2.7D, 4.8D), mq.getColumnInterval(sort, 2))
        assertEquals(ValueInterval(12, 18), mq.getColumnInterval(sort, 3))
        assertEquals(ValueInterval(161.0D, 172.1D), mq.getColumnInterval(sort, 4))
        assertNull(mq.getColumnInterval(sort, 5))
        assertNull(mq.getColumnInterval(sort, 6))
    }
  }

  @Test
  def testGetColumnIntervalOnRank(): Unit = {
    Array(logicalRank, flinkLogicalRank, batchLocalRank, batchGlobalRank, streamRank).foreach {
      rank =>
        assertEquals(ValueInterval(0, null), mq.getColumnInterval(rank, 0))
        assertNull(mq.getColumnInterval(rank, 1))
        assertEquals(ValueInterval(2.7D, 4.8D), mq.getColumnInterval(rank, 2))
        assertEquals(ValueInterval(12, 18), mq.getColumnInterval(rank, 3))
        assertEquals(ValueInterval(161.0D, 172.1D), mq.getColumnInterval(rank, 4))
        assertNull(mq.getColumnInterval(rank, 5))
        assertNull(mq.getColumnInterval(rank, 6))
        rank match {
          case r: BatchExecRank if !r.isGlobal => // local batch rank does not output rank function
          case _ => assertEquals(ValueInterval(1, 5), mq.getColumnInterval(rank, 7))
        }
    }

    Array(logicalRankWithVariableRange, flinkLogicalRankWithVariableRange,
      streamRankWithVariableRange).foreach {
      rank =>
        assertEquals(ValueInterval(0, null), mq.getColumnInterval(logicalRankWithVariableRange, 0))
        assertNull(mq.getColumnInterval(logicalRankWithVariableRange, 1))
        assertEquals(ValueInterval(2.7D, 4.8D), mq.getColumnInterval
        (logicalRankWithVariableRange, 2))
        assertEquals(ValueInterval(12, 18), mq.getColumnInterval(logicalRankWithVariableRange, 3))
        assertEquals(ValueInterval(161.0D, 172.1D),
          mq.getColumnInterval(logicalRankWithVariableRange, 4))
        assertNull(mq.getColumnInterval(logicalRankWithVariableRange, 5))
        assertNull(mq.getColumnInterval(logicalRankWithVariableRange, 6))
        assertEquals(ValueInterval(1, 18), mq.getColumnInterval(logicalRankWithVariableRange, 7))
    }

    Array(logicalRowNumber, flinkLogicalRowNumber, streamRowNumber).foreach {
      rank =>
        assertEquals(ValueInterval(0, null), mq.getColumnInterval(rank, 0))
        assertNull(mq.getColumnInterval(rank, 1))
        assertEquals(ValueInterval(2.7D, 4.8D), mq.getColumnInterval(rank, 2))
        assertEquals(ValueInterval(12, 18), mq.getColumnInterval(rank, 3))
        assertEquals(ValueInterval(161.0D, 172.1D), mq.getColumnInterval(rank, 4))
        assertNull(mq.getColumnInterval(rank, 5))
        assertNull(mq.getColumnInterval(rank, 6))
        assertEquals(ValueInterval(3, 6), mq.getColumnInterval(rank, 7))
    }
  }

  @Test
  def testGetColumnIntervalOnExchange(): Unit = {
    val exchange = LogicalExchange.create(studentLogicalScan, RelDistributions.SINGLETON)
    assertEquals(ValueInterval(0, null), mq.getColumnInterval(exchange, 0))
    assertNull(mq.getColumnInterval(exchange, 1))
    assertEquals(ValueInterval(2.7D, 4.8D), mq.getColumnInterval(exchange, 2))
    assertEquals(ValueInterval(12, 18), mq.getColumnInterval(exchange, 3))
    assertEquals(ValueInterval(161.0D, 172.1D), mq.getColumnInterval(exchange, 4))
    assertNull(mq.getColumnInterval(exchange, 5))
    assertNull(mq.getColumnInterval(exchange, 6))
  }

  @Test
  def testGetColumnIntervalOnAggregate(): Unit = {
    Array(logicalAgg, flinkLogicalAgg).foreach {
      agg =>
        assertEquals(ValueInterval(12, 18), mq.getColumnInterval(agg, 0))
        assertNull(mq.getColumnInterval(agg, 1))
        assertEquals(ValueInterval(2.7, null), mq.getColumnInterval(agg, 2))
        assertNull(mq.getColumnInterval(agg, 3))
        assertNull(mq.getColumnInterval(agg, 4))
        assertEquals(ValueInterval(0, null), mq.getColumnInterval(agg, 5))
    }

    Array(logicalAggWithAuxGroup, flinkLogicalAggWithAuxGroup).foreach {
      agg =>
        assertEquals(ValueInterval(0, null), mq.getColumnInterval(agg, 0))
        assertNull(mq.getColumnInterval(agg, 1))
        assertEquals(ValueInterval(161.0, 172.1), mq.getColumnInterval(agg, 2))
        assertNull(mq.getColumnInterval(agg, 3))
        assertEquals(ValueInterval(2.7, null), mq.getColumnInterval(agg, 4))
        assertEquals(ValueInterval(0, null), mq.getColumnInterval(agg, 5))
    }
  }

  @Test
  def testGetColumnIntervalOnBatchExecAggregate(): Unit = {
    Array(batchGlobalAggWithLocal, batchGlobalAggWithoutLocal).foreach {
      agg =>
        assertEquals(ValueInterval(12, 18), mq.getColumnInterval(agg, 0))
        assertNull(mq.getColumnInterval(agg, 1))
        assertEquals(ValueInterval(2.7, null), mq.getColumnInterval(agg, 2))
        assertNull(mq.getColumnInterval(agg, 3))
        assertNull(mq.getColumnInterval(agg, 4))
        assertEquals(ValueInterval(0, null), mq.getColumnInterval(agg, 5))
    }

    assertEquals(ValueInterval(12, 18), mq.getColumnInterval(batchLocalAgg, 0))
    assertNull(mq.getColumnInterval(batchLocalAgg, 1))
    assertNull(mq.getColumnInterval(batchLocalAgg, 2))
    assertEquals(ValueInterval(2.7, null), mq.getColumnInterval(batchLocalAgg, 3))
    assertNull(mq.getColumnInterval(batchLocalAgg, 4))
    assertNull(mq.getColumnInterval(batchLocalAgg, 5))
    assertEquals(ValueInterval(0, null), mq.getColumnInterval(batchLocalAgg, 6))

    assertEquals(ValueInterval(0, null), mq.getColumnInterval(batchLocalAggWithAuxGroup, 0))
    assertNull(mq.getColumnInterval(batchLocalAggWithAuxGroup, 1))
    assertEquals(ValueInterval(161.0, 172.1),
      mq.getColumnInterval(batchLocalAggWithAuxGroup, 2))
    assertNull(mq.getColumnInterval(batchLocalAggWithAuxGroup, 3))
    assertNull(mq.getColumnInterval(batchLocalAggWithAuxGroup, 4))
    assertEquals(ValueInterval(2.7, null), mq.getColumnInterval(batchLocalAggWithAuxGroup, 5))
    assertEquals(ValueInterval(0, null), mq.getColumnInterval(batchLocalAggWithAuxGroup, 6))

    Array(batchGlobalAggWithLocalWithAuxGroup, batchGlobalAggWithoutLocalWithAuxGroup)
      .foreach {
        agg =>
          assertEquals(ValueInterval(0, null), mq.getColumnInterval(agg, 0))
          assertNull(mq.getColumnInterval(agg, 1))
          assertEquals(ValueInterval(161.0, 172.1), mq.getColumnInterval(agg, 2))
          assertNull(mq.getColumnInterval(agg, 3))
          assertEquals(ValueInterval(2.7, null), mq.getColumnInterval(agg, 4))
          assertEquals(ValueInterval(0, null), mq.getColumnInterval(agg, 5))
      }
  }

  @Test
  def testGetColumnIntervalOnStreamExecAggregate(): Unit = {
    Array(streamGlobalAggWithLocal, streamGlobalAggWithoutLocal).foreach {
      agg =>
        assertEquals(ValueInterval(12, 18), mq.getColumnInterval(agg, 0))
        assertNull(mq.getColumnInterval(agg, 1))
        assertEquals(ValueInterval(2.7, null), mq.getColumnInterval(agg, 2))
        assertNull(mq.getColumnInterval(agg, 3))
        assertNull(mq.getColumnInterval(agg, 4))
        assertEquals(ValueInterval(0, null), mq.getColumnInterval(agg, 5))
    }

    assertEquals(ValueInterval(12, 18), mq.getColumnInterval(streamLocalAgg, 0))
    assertNull(mq.getColumnInterval(streamLocalAgg, 1))
    assertNull(mq.getColumnInterval(streamLocalAgg, 2))
    assertEquals(ValueInterval(2.7, null), mq.getColumnInterval(streamLocalAgg, 3))
    assertNull(mq.getColumnInterval(streamLocalAgg, 4))
    assertNull(mq.getColumnInterval(streamLocalAgg, 5))
    assertEquals(ValueInterval(0, null), mq.getColumnInterval(streamLocalAgg, 6))
  }

  @Test
  def testGetColumnIntervalOnTableAggregate(): Unit = {
    Array(logicalTableAgg, flinkLogicalTableAgg, streamExecTableAgg).foreach {
      agg =>
        assertEquals(RightSemiInfiniteValueInterval(0, true), mq.getColumnInterval(agg, 0))
        assertNull(mq.getColumnInterval(agg, 1))
        assertNull(mq.getColumnInterval(agg, 2))
    }
  }

  @Test
  def testGetColumnIntervalOnWindowTableAgg(): Unit = {
    Array(logicalWindowTableAgg, flinkLogicalWindowTableAgg, streamWindowTableAgg).foreach { agg =>
      assertEquals(ValueInterval(5, 45), mq.getColumnInterval(agg, 0))
      assertEquals(null, mq.getColumnInterval(agg, 1))
      assertEquals(null, mq.getColumnInterval(agg, 2))
      assertEquals(null, mq.getColumnInterval(agg, 3))
      assertEquals(null, mq.getColumnInterval(agg, 4))
      assertEquals(null, mq.getColumnInterval(agg, 5))
      assertEquals(null, mq.getColumnInterval(agg, 6))
    }
  }

  @Test
  def testGetColumnIntervalOnWindowAgg(): Unit = {
    Array(logicalWindowAgg, flinkLogicalWindowAgg, batchGlobalWindowAggWithLocalAgg,
      batchGlobalWindowAggWithoutLocalAgg, streamWindowAgg).foreach { agg =>
      assertEquals(ValueInterval(5, 45), mq.getColumnInterval(agg, 0))
      assertEquals(null, mq.getColumnInterval(agg, 1))
      assertEquals(RightSemiInfiniteValueInterval(0), mq.getColumnInterval(agg, 2))
      assertEquals(null, mq.getColumnInterval(agg, 3))
    }
    assertEquals(ValueInterval(5, 45), mq.getColumnInterval(batchLocalWindowAgg, 0))
    assertEquals(null, mq.getColumnInterval(batchLocalWindowAgg, 1))
    assertEquals(null, mq.getColumnInterval(batchLocalWindowAgg, 2))
    assertEquals(RightSemiInfiniteValueInterval(0), mq.getColumnInterval(batchLocalWindowAgg, 3))
    assertEquals(null, mq.getColumnInterval(batchLocalWindowAgg, 4))

    Array(logicalWindowAggWithAuxGroup, flinkLogicalWindowAggWithAuxGroup,
      batchGlobalWindowAggWithLocalAggWithAuxGroup,
      batchGlobalWindowAggWithoutLocalAggWithAuxGroup).foreach { agg =>
      assertEquals(ValueInterval(5, 55), mq.getColumnInterval(agg, 0))
      assertEquals(ValueInterval(0, 50), mq.getColumnInterval(agg, 1))
      assertEquals(ValueInterval(0, null), mq.getColumnInterval(agg, 2))
      assertEquals(null, mq.getColumnInterval(agg, 3))
    }
    assertEquals(ValueInterval(5, 55), mq.getColumnInterval(batchLocalWindowAggWithAuxGroup, 0))
    assertEquals(null, mq.getColumnInterval(batchLocalWindowAggWithAuxGroup, 1))
    assertEquals(ValueInterval(0, 50), mq.getColumnInterval(batchLocalWindowAggWithAuxGroup, 2))
    assertEquals(ValueInterval(0, null), mq.getColumnInterval(batchLocalWindowAggWithAuxGroup, 3))
    assertEquals(null, mq.getColumnInterval(batchLocalWindowAggWithAuxGroup, 4))
  }

  @Test
  def testGetColumnIntervalOnOverAgg(): Unit = {
    Array(flinkLogicalOverAgg, batchOverAgg).foreach {
      agg =>
        assertEquals(ValueInterval(0, null), mq.getColumnInterval(agg, 0))
        assertEquals(null, mq.getColumnInterval(agg, 1))
        assertEquals(ValueInterval(2.7, 4.8), mq.getColumnInterval(agg, 2))
        assertEquals(ValueInterval(12, 18), mq.getColumnInterval(agg, 3))
        assertNull(mq.getColumnInterval(agg, 4))
        assertNull(mq.getColumnInterval(agg, 5))
        assertNull(mq.getColumnInterval(agg, 6))
        assertNull(mq.getColumnInterval(agg, 7))
        assertNull(mq.getColumnInterval(agg, 8))
        assertNull(mq.getColumnInterval(agg, 9))
        assertNull(mq.getColumnInterval(agg, 10))
    }

    assertEquals(ValueInterval(0, null), mq.getColumnInterval(streamOverAgg, 0))
    assertEquals(null, mq.getColumnInterval(streamOverAgg, 1))
    assertEquals(ValueInterval(2.7, 4.8), mq.getColumnInterval(streamOverAgg, 2))
    assertEquals(ValueInterval(12, 18), mq.getColumnInterval(streamOverAgg, 3))
    assertNull(mq.getColumnInterval(streamOverAgg, 4))
    assertNull(mq.getColumnInterval(streamOverAgg, 5))
    assertNull(mq.getColumnInterval(streamOverAgg, 6))
    assertNull(mq.getColumnInterval(streamOverAgg, 7))
  }

  @Test
  def testGetColumnIntervalOnJoin(): Unit = {
    val left = relBuilder.scan("MyTable1").build()
    val right = relBuilder.scan("MyTable2").build()
    // join condition is MyTable1.a=MyTable1.a and MyTable1.a > 100 and MyTable2.b <= 1000
    val join = relBuilder.push(left).push(right).join(JoinRelType.INNER,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)),
      relBuilder.call(GREATER_THAN, relBuilder.field(2, 0, 0), relBuilder.literal(100)),
      relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(2, 1, 1),
        rexBuilder.makeLiteral(1000L, longType, false))
    ).build

    assertEquals(ValueInterval(100, null, includeLower = false), mq.getColumnInterval(join, 0))
    assertEquals(ValueInterval(1L, 800000000L), mq.getColumnInterval(join, 1))
    assertNull(mq.getColumnInterval(join, 2))
    assertNull(mq.getColumnInterval(join, 3))
    assertEquals(ValueInterval(1L, 100L), mq.getColumnInterval(join, 4))
    assertNull(mq.getColumnInterval(join, 5))
    assertEquals(ValueInterval(8L, 1000L), mq.getColumnInterval(join, 6))
    assertNull(mq.getColumnInterval(join, 7))
    assertNull(mq.getColumnInterval(join, 8))

    assertEquals(ValueInterval(0, null, includeLower = true),
      mq.getColumnInterval(logicalSemiJoinNotOnUniqueKeys, 0))
    assertEquals(ValueInterval(1L, 800000000L),
      mq.getColumnInterval(logicalSemiJoinNotOnUniqueKeys, 1))
    assertNull(mq.getColumnInterval(logicalSemiJoinNotOnUniqueKeys, 2))
    assertNull(mq.getColumnInterval(logicalSemiJoinNotOnUniqueKeys, 3))
    assertEquals(ValueInterval(1L, 100L), mq.getColumnInterval(logicalSemiJoinNotOnUniqueKeys, 4))

    assertEquals(ValueInterval(0, null, includeLower = true),
      mq.getColumnInterval(logicalAntiJoinWithoutEquiCond, 0))
    assertEquals(ValueInterval(1L, 800000000L),
      mq.getColumnInterval(logicalAntiJoinWithoutEquiCond, 1))
    assertNull(mq.getColumnInterval(logicalAntiJoinWithoutEquiCond, 2))
    assertNull(mq.getColumnInterval(logicalAntiJoinWithoutEquiCond, 3))
    assertEquals(ValueInterval(1L, 100L), mq.getColumnInterval(logicalAntiJoinWithoutEquiCond, 4))
  }

  @Test
  def testGetColumnIntervalOnUnion(): Unit = {
    val ts1 = relBuilder.scan("MyTable1").build()
    val ts2 = relBuilder.scan("MyTable2").build()
    val union = relBuilder.push(ts1).push(ts2).union(true).build()
    assertNull(mq.getColumnInterval(union, 0))
    assertEquals(ValueInterval(1L, 800000000L), mq.getColumnInterval(union, 1))
    assertNull(mq.getColumnInterval(union, 2))
    assertNull(mq.getColumnInterval(union, 3))
  }

  @Test
  def testGetColumnIntervalOnDefault(): Unit = {
    (0 until testRel.getRowType.getFieldCount).foreach { idx =>
      assertNull(mq.getColumnInterval(testRel, idx))
    }
  }

  def assertEqualsAsDouble(
      expected: ValueInterval,
      actual: ValueInterval,
      delta: Double = 1e-6): Unit = {
    if (expected == null || actual == null) {
      assertTrue(s"expected: $expected, actual: $actual", expected == null && actual == null)
      return
    }

    def toDouble(number: Any): JDouble = {
      val v = ColumnIntervalUtil.convertNumberToString(number)
        .getOrElse(throw new TableException(""))
      java.lang.Double.valueOf(v)
    }

    def decompose(v: ValueInterval): (JDouble, JDouble, JBoolean, JBoolean) = {
      v match {
        case EmptyValueInterval => (null, null, false, false)
        case InfiniteValueInterval =>
          (Double.NegativeInfinity, Double.PositiveInfinity, false, false)
        case f: FiniteValueInterval =>
          (toDouble(f.lower), toDouble(f.upper), f.includeLower, f.includeUpper)
        case l: LeftSemiInfiniteValueInterval =>
          (Double.NegativeInfinity, toDouble(l.upper), false, l.includeUpper)
        case r: RightSemiInfiniteValueInterval =>
          (toDouble(r.lower), Double.PositiveInfinity, r.includeLower, false)
      }
    }

    val (lower1, upper1, includeLower1, includeUpper1) = decompose(expected)
    val (lower2, upper2, includeLower2, includeUpper2) = decompose(actual)

    assertEquals(lower1, lower2, delta)
    assertEquals(upper1, upper2, delta)
    assertEquals(includeLower1, includeLower2)
    assertEquals(includeUpper1, includeUpper2)
  }
}
