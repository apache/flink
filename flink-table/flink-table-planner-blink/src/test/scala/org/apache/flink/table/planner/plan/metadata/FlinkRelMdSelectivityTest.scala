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

import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalExpand
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalDataStreamTableScan, FlinkLogicalExpand, FlinkLogicalOverAggregate}
import org.apache.flink.table.planner.plan.nodes.physical.batch.{BatchPhysicalRank, BatchPhysicalCalc}
import org.apache.flink.table.planner.plan.utils.ExpandUtil

import com.google.common.collect.{ImmutableList, Lists}
import org.apache.calcite.rel.core.{AggregateCall, CorrelationId, JoinRelType, Window}
import org.apache.calcite.rel.logical.LogicalJoin
import org.apache.calcite.rel.{RelCollationImpl, RelFieldCollation}
import org.apache.calcite.rex.{RexInputRef, RexNode, RexProgram, RexUtil, RexWindowBound}
import org.apache.calcite.sql.SqlWindow
import org.apache.calcite.sql.`type`.SqlTypeName.{BIGINT, DOUBLE}
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.sql.fun.{SqlCountAggFunction, SqlStdOperatorTable}
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.util.ImmutableBitSet
import org.junit.Assert._
import org.junit.Test

import java.util
import java.util.Collections

import scala.collection.JavaConversions._

class FlinkRelMdSelectivityTest extends FlinkRelMdHandlerTestBase {

  @Test
  def testGetSelectivityOnTableScan(): Unit = {
    Array(studentLogicalScan, studentBatchScan, studentStreamScan).foreach { scan =>
      assertEquals(1.0, mq.getSelectivity(scan, null))
      // age = 16
      val condition1 = relBuilder.push(studentLogicalScan)
        .call(EQUALS, relBuilder.field(3), relBuilder.literal(16))
      assertEquals(1.0 / 7.0, mq.getSelectivity(scan, condition1))

      // age = 16 AND score >= 4.0
      val condition2 = relBuilder.call(AND,
        relBuilder.call(EQUALS, relBuilder.field(3), relBuilder.literal(16)),
        relBuilder.call(GREATER_THAN_OR_EQUAL, relBuilder.field(2), relBuilder.literal(4.0)))
      assertEquals((1.0 / 7.0) * (4.8 - 4.0) / (4.8 - 2.7), mq.getSelectivity(scan, condition2))
    }
  }

  @Test
  def testGetSelectivityOnProject(): Unit = {
    relBuilder.scan("MyTable3")
    // underlying filter: $0 <= 2 and $0 > -1 and $1 < 1.1
    val expr1 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2))
    val expr2 = relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(-1))
    val expr3 = relBuilder.call(LESS_THAN, relBuilder.field(1), relBuilder.literal(1.1D))
    relBuilder.filter(List(expr1, expr2, expr3))
    // top projects: $0==1, $0, $1, true, 2.1, 2
    val projects = List(
      relBuilder.call(EQUALS, relBuilder.field(0), relBuilder.literal(1)),
      relBuilder.field(0),
      relBuilder.field(1),
      relBuilder.literal(true),
      relBuilder.getRexBuilder.makeLiteral(
        2.1D, relBuilder.getTypeFactory.createSqlType(DOUBLE), true),
      relBuilder.getRexBuilder.makeLiteral(
        2L, relBuilder.getTypeFactory.createSqlType(BIGINT), true))
    val project = relBuilder.project(projects).build()
    relBuilder.push(project)
    val pred1 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(1), relBuilder.literal(1))
    assertEquals((1.0 + 1.0) / (2.0 + 1.0), mq.getSelectivity(project, pred1))
    val pred2 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(2), relBuilder.literal(1))
    assertEquals(1 / 1.1, mq.getSelectivity(project, pred2))
    val pred3 = relBuilder.call(EQUALS, relBuilder.field(4), relBuilder.literal(1))
    assertEquals(0D, mq.getSelectivity(project, pred3))
  }

  @Test
  def testGetSelectivityOnFilter(): Unit = {
    relBuilder.scan("MyTable3")
    // filter: $0 <= 2 and $0 > -1 and $1 < 1.1
    val expr1 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2))
    val expr2 = relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(-1))
    val expr3 = relBuilder.call(LESS_THAN, relBuilder.field(1), relBuilder.literal(1.1D))
    val filter = relBuilder.filter(List(expr1, expr2, expr3)).build()
    relBuilder.push(filter)
    val pred1 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(1))
    assertEquals((1.0 + 1.0) / (2.0 + 1.0), mq.getSelectivity(filter, pred1))
    val pred2 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(1), relBuilder.literal(1))
    assertEquals(1 / 1.1, mq.getSelectivity(filter, pred2))
  }

  @Test
  def testGetSelectivityOnCalc(): Unit = {
    val ts = relBuilder.scan("MyTable3").build()

    relBuilder.push(ts)
    // projects: $0==1, $0, $1, true, 2.1, 2
    val projects = List(
      relBuilder.call(EQUALS, relBuilder.field(0), relBuilder.literal(1)),
      relBuilder.field(0),
      relBuilder.field(1),
      relBuilder.literal(true),
      relBuilder.getRexBuilder.makeLiteral(
        2.1D, relBuilder.getTypeFactory.createSqlType(DOUBLE), true),
      relBuilder.getRexBuilder.makeLiteral(
        2L, relBuilder.getTypeFactory.createSqlType(BIGINT), true))
    val outputRowType = relBuilder.project(projects).build().getRowType
    // calc => project + filter: $0 <= 2 and $0 > -1 and $1 < 1.1
    relBuilder.push(ts)
    val expr1 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2))
    val expr2 = relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(-1))
    val expr3 = relBuilder.call(LESS_THAN, relBuilder.field(1), relBuilder.literal(1.1D))
    val rexBuilder = relBuilder.getRexBuilder
    val predicate = RexUtil.composeConjunction(rexBuilder, List(expr1, expr2, expr3), true)
    val program = RexProgram.create(
      ts.getRowType,
      projects,
      predicate,
      outputRowType,
      rexBuilder)

    val calc = new BatchPhysicalCalc(cluster, batchPhysicalTraits, ts, program, outputRowType)
    // pop scan
    relBuilder.build()
    // push calc
    relBuilder.push(calc)
    val pred1 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(1), relBuilder.literal(1))
    assertEquals((1.0 + 1.0) / (2.0 + 1.0), mq.getSelectivity(calc, pred1))
    val pred2 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(2), relBuilder.literal(1))
    assertEquals(1 / 1.1, mq.getSelectivity(calc, pred2))
    val pred3 = relBuilder.call(EQUALS, relBuilder.field(4), relBuilder.literal(1))
    assertEquals(0D, mq.getSelectivity(calc, pred3))
  }

  @Test
  def testGetSelectivityOnExpand(): Unit = {
    val ts = relBuilder.scan("MyTable3").build()
    val expandOutputType = ExpandUtil.buildExpandRowType(
      ts.getCluster.getTypeFactory, ts.getRowType, Array.empty[Integer])
    val expandProjects = ExpandUtil.createExpandProjects(
      ts.getCluster.getRexBuilder,
      ts.getRowType,
      expandOutputType,
      ImmutableBitSet.of(0, 1),
      ImmutableList.of(ImmutableBitSet.of(0), ImmutableBitSet.of(1)), Array.empty[Integer])
    val expand = new FlinkLogicalExpand(
      ts.getCluster, ts.getTraitSet, ts, expandOutputType, expandProjects, 2)

    relBuilder.push(expand)
    val predicate1 = relBuilder
      .call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(2))
    assertEquals((5 - 2.0) / (5 - (-5)), mq.getSelectivity(expand, predicate1))

    val predicate2 = relBuilder.and(
      relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(2)),
      relBuilder.equals(relBuilder.field(2), relBuilder.literal(1))
    )
    assertEquals(0.075, mq.getSelectivity(expand, predicate2))
  }

  @Test
  def testGetSelectivityOnExchange(): Unit = {
    Array(batchExchange, streamExchange).foreach { exchange =>
      assertEquals(1.0, mq.getSelectivity(exchange, null))
      // age = 16
      val condition1 = relBuilder.push(studentLogicalScan)
        .call(EQUALS, relBuilder.field(3), relBuilder.literal(16))
      assertEquals(1.0 / 7.0, mq.getSelectivity(exchange, condition1))
    }
  }

  @Test
  def testGetSelectivityOnRank(): Unit = {
    Array(logicalRank, flinkLogicalRank, batchGlobalRank, batchLocalRank, streamRank).foreach {
      rank =>
        assertEquals(1.0, mq.getSelectivity(rank, null))
        relBuilder.push(rank)
        // age = 16
        val condition1 = relBuilder.call(EQUALS, relBuilder.field(3), relBuilder.literal(16))
        assertEquals(1.0 / 7.0, mq.getSelectivity(rank, condition1))

        rank match {
          case r: BatchPhysicalRank if !r.isGlobal => // batch local rank does not output rank fun
          case _ =>
            // rk > 2
            val condition2 =
              relBuilder.call(GREATER_THAN, relBuilder.field(7), relBuilder.literal(2))
            assertEquals(0.75, mq.getSelectivity(rank, condition2))

            // age <= 15 and rk > 2
            val condition3 = relBuilder.call(AND,
              relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(3), relBuilder.literal(15)),
              relBuilder.call(GREATER_THAN, relBuilder.field(7), relBuilder.literal(2))
            )
            assertEquals(0.375, mq.getSelectivity(rank, condition3))
        }
    }

    Array(logicalRank2, flinkLogicalRank2, batchGlobalRank2, streamRank2).foreach {
      rank =>
        // rk <= 2
        val condition1 = relBuilder.push(rank)
          .call(LESS_THAN_OR_EQUAL, relBuilder.field(7), relBuilder.literal(2))
        assertEquals(0.0, mq.getSelectivity(rank, condition1))
        // rk >= 2
        val condition2 = relBuilder.push(rank)
          .call(GREATER_THAN_OR_EQUAL, relBuilder.field(7), relBuilder.literal(2))
        assertEquals(1.0, mq.getSelectivity(rank, condition2))
        // rk = 4
        val condition3 = relBuilder.push(rank)
          .call(EQUALS, relBuilder.field(7), relBuilder.literal(4))
        assertEquals(1.0 / 3.0, mq.getSelectivity(rank, condition3))
    }
  }

  @Test
  def testGetSelectivityOnSort(): Unit = {
    Array(logicalSort, flinkLogicalSort, batchSort, streamSort, logicalSortLimit,
      flinkLogicalSortLimit, batchGlobalSortLimit, streamSortLimit,
      logicalLimit, flinkLogicalLimit, batchLimit, streamLimit).foreach { sort =>
      assertEquals(1.0, mq.getSelectivity(sort, null))
      // age = 16
      val condition1 = relBuilder.push(studentLogicalScan)
        .call(EQUALS, relBuilder.field(3), relBuilder.literal(16))
      assertEquals(1.0 / 7.0, mq.getSelectivity(sort, condition1))

      // age = 16 AND score >= 4.0
      val condition2 = relBuilder.call(AND,
        relBuilder.call(EQUALS, relBuilder.field(3), relBuilder.literal(16)),
        relBuilder.call(GREATER_THAN_OR_EQUAL, relBuilder.field(2), relBuilder.literal(4.0)))
      assertEquals((1.0 / 7.0) * (4.8 - 4.0) / (4.8 - 2.7), mq.getSelectivity(sort, condition2))
    }
  }

  @Test
  def testGetSelectivityOnAggregate(): Unit = {
    // select c, sum(b) as sum_b, max(d) as max_d from MyTable4 group by c
    val agg = relBuilder.scan("MyTable4").aggregate(
      relBuilder.groupKey(relBuilder.field("c")),
      relBuilder.sum(false, "sum_b", relBuilder.field("b")),
      relBuilder.max("max_d", relBuilder.field("d"))).build()
    relBuilder.push(agg)
    // sum_d > 5
    val pred1 = relBuilder.call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(5))
    assertEquals((10.2 - 5) / 10.2, mq.getSelectivity(agg, pred1))

    // max_f < 165
    val pred2 = relBuilder.call(LESS_THAN, relBuilder.field(2), relBuilder.literal(165))
    assertEquals((165 - 161.0) / (172.1 - 161.0), mq.getSelectivity(agg, pred2))

    // e < 20 and sum_d > 5
    val pred3 = relBuilder.and(
      relBuilder.call(LESS_THAN, relBuilder.field(0), relBuilder.literal(20)),
      relBuilder.call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(5)))
    assertEquals(((20.0 - 0.0) / (46.0 - 0.0)) * ((10.2 - 5) / 10.2), mq.getSelectivity(agg, pred3))

    relBuilder.clear()
    // select a, c, sum(b) as sum_b, max(d) as max_d from MyTable4 group by a, c
    val agg1 = relBuilder.scan("MyTable4").aggregate(
      relBuilder.groupKey(relBuilder.field("a")),
      relBuilder.aggregateCall(
        FlinkSqlOperatorTable.AUXILIARY_GROUP, false, false, null, "c", relBuilder.field("c")),
      relBuilder.sum(false, "sum_b", relBuilder.field("b")),
      relBuilder.max("max_d", relBuilder.field("d"))).build()

    // c < 20 and sum_b > 5
    relBuilder.push(agg1)
    val pred4 = relBuilder.and(
      relBuilder.call(LESS_THAN, relBuilder.field(1), relBuilder.literal(20)),
      relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(5)))
    assertEquals(((20.0 - 0.0) / (46.0 - 0.0)) * ((5.1 - 5) / 5.1),
      mq.getSelectivity(agg1, pred4))

    relBuilder.clear()
    val ts = relBuilder.scan("MyTable4").build()
    val expandOutputType = ExpandUtil.buildExpandRowType(
      ts.getCluster.getTypeFactory, ts.getRowType, Array.empty[Integer])
    val expandProjects = ExpandUtil.createExpandProjects(
      ts.getCluster.getRexBuilder,
      ts.getRowType,
      expandOutputType,
      ImmutableBitSet.of(0, 1, 2),
      ImmutableList.of(ImmutableBitSet.of(0, 1), ImmutableBitSet.of(0, 2)), Array.empty[Integer])
    val expand = new LogicalExpand(
      ts.getCluster, ts.getTraitSet, ts, expandOutputType, expandProjects, 4)

    // agg output type: a, $e, b, c, count(d)
    val aggWithAuxGroupAndExpand = relBuilder.push(expand).aggregate(
      relBuilder.groupKey(relBuilder.fields(Seq[Integer](0, 4).toList)),
      Lists.newArrayList(
        AggregateCall.create(FlinkSqlOperatorTable.AUXILIARY_GROUP, false, false,
          List[Integer](1), -1, 1, ts, null, "b"),
        AggregateCall.create(FlinkSqlOperatorTable.AUXILIARY_GROUP, false, false,
          List[Integer](2), -1, 1, ts, null, "c"),
        AggregateCall.create(
          new SqlCountAggFunction("COUNT"), false, false, List[Integer](3), -1, 2, ts, null, "a")
      )).build()

    relBuilder.push(aggWithAuxGroupAndExpand)
    val predicate3 = relBuilder
      .call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(2.0))
    assertEquals((5.1 - 2.0) / (5.1 - 0), mq.getSelectivity(aggWithAuxGroupAndExpand, predicate3))
    val predicate4 = relBuilder.and(
      relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(2.0)),
      relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(3), relBuilder.literal(23))
    )
    assertEquals(((5.1 - 2.0) / (5.1 - 0)) * ((23.0 - 0) / (46.0 - 0)),
      mq.getSelectivity(aggWithAuxGroupAndExpand, predicate4))

    val predicate5 = relBuilder.and(
      relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(5)),
      relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(2.0)),
      relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(3), relBuilder.literal(23))
    )
    assertEquals(
      ((50.0 - 5.0) / (50.0 - 1)) * ((5.1 - 2.0) / (5.1 - 0)) * ((23.0 - 0) / (46.0 - 0)),
      mq.getSelectivity(aggWithAuxGroupAndExpand, predicate5))

    Array(logicalAgg, flinkLogicalAgg, batchGlobalAggWithLocal, batchGlobalAggWithoutLocal)
      .foreach { agg =>
        relBuilder.clear()
        relBuilder.push(agg)
        // age <= 15 and sum_score > 10
        val predicate = relBuilder.and(
          relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(15)),
          relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(10.0))
        )
        assertEquals(0.495, mq.getSelectivity(agg, predicate))
      }

    Array(logicalAggWithAuxGroup, flinkLogicalAggWithAuxGroup,
      batchGlobalAggWithLocalWithAuxGroup, batchGlobalAggWithoutLocalWithAuxGroup).foreach { agg =>
      // height > 170.0 and avg_score < 10
      val predicate = relBuilder.and(
        relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(170.0)),
        relBuilder.call(LESS_THAN, relBuilder.field(3), relBuilder.literal(10.0))
      )
      assertEquals(0.187297, mq.getSelectivity(agg, predicate), 1e-6)
    }
  }

  @Test
  def testGetSelectivityOnWindowAgg(): Unit = {
    Array(logicalWindowAgg, flinkLogicalWindowAgg, batchGlobalWindowAggWithoutLocalAgg,
      batchGlobalWindowAggWithLocalAgg).foreach { agg =>
      relBuilder.clear()
      relBuilder.push(agg)
      // predicate without time fields and aggCall fields
      // a > 15
      val predicate1 = relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(15))
      assertEquals(0.75D, mq.getSelectivity(agg, predicate1))

      // predicate with time fields only
      // a > 15 and w$end = 1000000
      val predicate2 = relBuilder.and(
        relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(15)),
        relBuilder.call(EQUALS, relBuilder.field(4), relBuilder.literal(1000000))
      )
      assertEquals(0.75D * 0.15D, mq.getSelectivity(agg, predicate2))

      // predicate with time fields and aggCall fields
      // a > 15 and count(c) > 100 and w$end = 1000000
      val predicate3 = relBuilder.and(
        relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(15)),
        relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(100)),
        relBuilder.call(EQUALS, relBuilder.field(4), relBuilder.literal(1000000))
      )
      assertEquals(0.75D * 0.15D * 0.01D, mq.getSelectivity(agg, predicate3))
    }

    Array(logicalWindowAggWithAuxGroup, flinkLogicalWindowAggWithAuxGroup,
      batchGlobalWindowAggWithoutLocalAggWithAuxGroup,
      batchGlobalWindowAggWithLocalAggWithAuxGroup).foreach { agg =>
      relBuilder.clear()
      relBuilder.push(agg)
      // a > 15
      val predicate4 = relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(15))
      assertEquals(0.8D, mq.getSelectivity(agg, predicate4))
      // b > 15
      val predicate5 = relBuilder.call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(15))
      assertEquals(0.7D, mq.getSelectivity(agg, predicate5))
      // a > 15 and b > 15 and count(c) > 100 and w$end = 1000000
      val predicate6 = relBuilder.and(
        relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(15)),
        relBuilder.call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(15)),
        relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(100)),
        relBuilder.call(EQUALS, relBuilder.field(4), relBuilder.literal(1000000))
      )
      assertEquals(0.8D * 0.7D * 0.15D * 0.01D, mq.getSelectivity(agg, predicate6))
    }
  }

  @Test
  def testGetSelectivityOnOverAgg(): Unit = {
    // select a, b, c, d,
    // rank() over (partition by c order by d) as rk,
    // max(d) over(partition by c order by d) as max_d from MyTable4
    val rankAggCall = AggregateCall.create(SqlStdOperatorTable.RANK, false,
      ImmutableList.of(), -1, longType, "rk")
    val maxAggCall = AggregateCall.create(SqlStdOperatorTable.MAX, false,
      ImmutableList.of(Integer.valueOf(3)), -1, doubleType, "max_d")
    val overAggGroups = ImmutableList.of(new Window.Group(
      ImmutableBitSet.of(2),
      true,
      RexWindowBound.create(SqlWindow.createUnboundedPreceding(new SqlParserPos(0, 0)), null),
      RexWindowBound.create(SqlWindow.createCurrentRow(new SqlParserPos(0, 0)), null),
      RelCollationImpl.of(new RelFieldCollation(
        1, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.FIRST)),
      ImmutableList.of(
        new Window.RexWinAggCall(SqlStdOperatorTable.RANK, longType,
          ImmutableList.of[RexNode](), 0, false),
        new Window.RexWinAggCall(SqlStdOperatorTable.MAX, doubleType,
          util.Arrays.asList(new RexInputRef(3, doubleType)), 1, false)
      )
    ))
    val scan: FlinkLogicalDataStreamTableScan =
      createDataStreamScan(List("MyTable4"), flinkLogicalTraits)
    val builder = typeFactory.builder
    scan.getRowType.getFieldList.foreach(f => builder.add(f.getName, f.getType))
    builder.add(rankAggCall.getName, rankAggCall.getType)
    builder.add(maxAggCall.getName, maxAggCall.getType)
    val overWindow = new FlinkLogicalOverAggregate(cluster, flinkLogicalTraits, scan,
      ImmutableList.of(), builder.build(), overAggGroups)

    relBuilder.push(overWindow)
    //  a <= 10
    val pred = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(10))
    assertEquals((10.0 - 1) / (50.0 - 1), mq.getSelectivity(overWindow, pred))
    // c == 10
    val pred1 = relBuilder.call(EQUALS, relBuilder.field(2), relBuilder.literal(10))
    assertEquals(1 / 25.0, mq.getSelectivity(overWindow, pred1))
    // a <= 10 and c = 10
    val pred2 = relBuilder.call(AND, pred, pred1)
    assertEquals(1 / 25.0 * ((10.0 - 1.0) / (50 - 1.0)), mq.getSelectivity(overWindow, pred2))
    // a <= 2 and b = 10 and rk < 2
    val pred3 = relBuilder.call(AND, pred, pred1,
      relBuilder.call(LESS_THAN, relBuilder.field(4), relBuilder.literal(2)))
    assertEquals(1 / 25.0 * ((10.0 - 1.0) / (50.0 - 1)) * 0.5, mq.getSelectivity(overWindow, pred3))

    Array(flinkLogicalOverAgg, batchOverAgg).foreach { agg =>
      relBuilder.clear()
      relBuilder.push(agg)

      // score > 4.0
      val pred1 = relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(4.0))
      assertEquals((4.8 - 4.0) / (4.8 - 2.7), mq.getSelectivity(agg, pred1))

      // score > 4.0 and max_score < 4.5
      val pred2 = relBuilder.call(AND, pred1,
        relBuilder.call(GREATER_THAN, relBuilder.field(9), relBuilder.literal(4.5)))
      assertEquals((4.8 - 4.0) / (4.8 - 2.7) * 0.5, mq.getSelectivity(agg, pred2))
    }
  }

  @Test
  def testGetSelectivityOnJoin(): Unit = {
    val ts = relBuilder.scan("MyTable3").build()
    // right is $0 <= 2 and $1 < 1.1
    val right = relBuilder.push(ts).filter(
      relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2)),
      relBuilder.call(LESS_THAN, relBuilder.field(1), relBuilder.literal(1.1D))).build()
    // join condition is left.a=right.a and left.a > -1 and right.b > 0.1
    relBuilder.push(ts).push(right)
    val joinCondition = RexUtil.composeConjunction(rexBuilder, List(
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)),
      relBuilder.call(GREATER_THAN, relBuilder.field(2, 0, 0), relBuilder.literal(-1)),
      relBuilder.call(GREATER_THAN, relBuilder.field(2, 1, 1), relBuilder.literal(0.1D))
    ), true)
    val join = LogicalJoin.create(
      ts, right, Collections.emptyList(),
      joinCondition, Set.empty[CorrelationId], JoinRelType.INNER)

    relBuilder.push(join)
    val pred1 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(0))
    assertEquals((0D - (-1)) / (5 - (-1)), mq.getSelectivity(join, pred1))
    val pred2 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(1), relBuilder.literal(1))
    assertEquals((1D - 0) / (6.1D - 0), mq.getSelectivity(join, pred2))
    val pred3 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(2), relBuilder.literal(3))
    assertEquals(1D, mq.getSelectivity(join, pred3))
    val pred4 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(3), relBuilder.literal(0))
    assertEquals(0D, mq.getSelectivity(join, pred4))

    assertEquals(3.125E-8, mq.getSelectivity(logicalSemiJoinOnUniqueKeys, pred1))
    val pred5 = relBuilder.push(logicalSemiJoinNotOnUniqueKeys)
        .call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(100000000L))
    assertEquals(0.5, mq.getSelectivity(logicalSemiJoinNotOnUniqueKeys, pred5))

    val pred6 = relBuilder.push(logicalAntiJoinWithoutEquiCond)
        .call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(100L))
    assertEquals(0.375, mq.getSelectivity(logicalAntiJoinWithoutEquiCond, pred6))
    val pred7 = relBuilder.push(logicalAntiJoinNotOnUniqueKeys)
        .call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(100000000L))
    assertEquals(0.05, mq.getSelectivity(logicalAntiJoinNotOnUniqueKeys, pred7))
  }

  @Test
  def testGetSelectivityOnUnion(): Unit = {
    val union = relBuilder
      .scan("MyTable4").project(relBuilder.fields().subList(0, 2))
      .scan("MyTable3")
      .union(true).build()
    // a <= 2
    val pred = relBuilder.push(union).call(
      LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2))
    assertEquals(
      ((2.0 - (-5)) / (5.0 - (-5)) * 100 + (2.0 - 1.0) / (50.0 - 1.0) * 50) / (100 + 50),
      mq.getSelectivity(union, pred))
  }

  @Test
  def testGetSelectivityOnDefault(): Unit = {
    // id <= 2
    val pred = relBuilder.push(testRel).call(
      LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2))
    assertEquals(0.5, mq.getSelectivity(testRel, pred))
  }

}
