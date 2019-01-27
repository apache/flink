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

import org.apache.flink.table.calcite.FlinkRelBuilder
import org.apache.flink.table.functions.sql.internal.SqlAuxiliaryGroupAggFunction
import org.apache.flink.table.plan.nodes.physical.batch._
import org.apache.flink.table.plan.schema.FlinkRelOptTable

import com.google.common.collect.ImmutableList
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataTypeField
import org.apache.calcite.rel.core.{JoinInfo, JoinRelType}
import org.apache.calcite.rex.{RexNode, RexProgram, RexUtil}
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.sql.validate.SqlValidatorUtil
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConversions._

class FlinkRelMdSelectivityTest extends FlinkRelMdHandlerTestBase {

  @Test
  def testGetSelectivityOnTableScan(): Unit = {
    // filter: $0 <= 2
    val pred = relBuilder.push(scanOfT1).call(
      LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2))
    assertEquals((2.0 + 5.0) / (5.0 + 5.0), mq.getSelectivity(scanOfT1, pred))
  }

  @Test
  def testGetSelectivityOnSortLimitBatchExec(): Unit = {
    // filter: $0 <= 2
    val pred = relBuilder.push(sortLimitBatchExec).call(
      LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2))
    assertEquals((2.0 + 5.0) / (5.0 + 5.0), mq.getSelectivity(sortLimitBatchExec, pred))
  }

  @Test
  def testGetSelectivityOnLimitBatchExec(): Unit = {
    // filter: $0 <= 2
    val pred = relBuilder.push(limitBatchExec).call(
      LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2))
    assertEquals((2.0 + 5.0) / (5.0 + 5.0), mq.getSelectivity(limitBatchExec, pred))
  }

  @Test
  def testGetSelectivityOnSortBatchExec(): Unit = {
    // filter: $0 <= 2
    val pred = relBuilder.push(sortBatchExec).call(
      LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2))
    assertEquals((2.0 + 5.0) / (5.0 + 5.0), mq.getSelectivity(sortBatchExec, pred))
  }

  @Test
  def testGetSelectivityOnUnionBatchExec(): Unit = {
    val union = new BatchExecUnion(
      cluster,
      batchExecTraits,
      Array(scanOfT1, scanOfT2).toList,
      scanOfT1.getRowType,
      true)
    // filter: $0 <= 2
    val pred = relBuilder.push(union).call(
      LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2))
    assertEquals((0.7 * 100 + 0.2 * 50) / (100 + 50), mq.getSelectivity(union, pred))
  }

  @Test
  def testGetSelectivityOnFilter(): Unit = {
    relBuilder.scan("t1")
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
  def testGetSelectivityOnProject(): Unit = {
    relBuilder.scan("t1")
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
  def testGetSelectivityOnCalc(): Unit = {
    // projects: $0==1, $0, $1, true, 2.1, 2
    relBuilder.push(scanOfT1)
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
    relBuilder.push(scanOfT1)
    val expr1 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2))
    val expr2 = relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(-1))
    val expr3 = relBuilder.call(LESS_THAN, relBuilder.field(1), relBuilder.literal(1.1D))
    val rexBuilder = relBuilder.getRexBuilder
    val predicate = RexUtil.composeConjunction(rexBuilder, List(expr1, expr2, expr3), true)
    val program = RexProgram.create(
      scanOfT1.getRowType,
      projects,
      predicate,
      outputRowType,
      rexBuilder)
    val calc = new BatchExecCalc(cluster, batchExecTraits, scanOfT1, outputRowType, program, "")
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
  def testGetSelectivityOnJoinBatchExec(): Unit = {
    // right is $0 <= 2 and $1 < 1.1
    val right = relBuilder.push(scanOfT1).filter(
      relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2)),
      relBuilder.call(LESS_THAN, relBuilder.field(1), relBuilder.literal(1.1D))).build()
    // join condition is left.$0=right.$0 and left.$0 > -1 and right.$1 > 0.1
    relBuilder.push(scanOfT1).push(right)
    val joinCondition = RexUtil.composeConjunction(relBuilder.getRexBuilder, List(
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)),
      relBuilder.call(GREATER_THAN, relBuilder.field(2, 0, 0), relBuilder.literal(-1)),
      relBuilder.call(GREATER_THAN, relBuilder.field(2, 1, 1), relBuilder.literal(0.1D))
    ), true)
    val joinType = JoinRelType.INNER
    val joinRowType = SqlValidatorUtil.deriveJoinRowType(
      scanOfT1.getRowType,
      right.getRowType,
      joinType,
      typeFactory,
      null,
      List[RelDataTypeField]())
    val join = new BatchExecHashJoin(
      cluster,
      batchExecTraits,
      scanOfT1,
      right,
      false,
      joinCondition,
      JoinRelType.INNER,
      false,
      "")
    relBuilder.push(join)
    val pred1 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(0))
    assertEquals((0D + 1) / (5 + 1), mq.getSelectivity(join, pred1))
    val pred2 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(1), relBuilder.literal(1))
    assertEquals((1D - 0) / (6.1D - 0), mq.getSelectivity(join, pred2))
    val pred3 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(2), relBuilder.literal(3))
    assertEquals(1D, mq.getSelectivity(join, pred3))
    val pred4 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(3), relBuilder.literal(0))
    assertEquals(0D, mq.getSelectivity(join, pred4))
  }

  @Test
  def testGetSelectivityOnAggregate(): Unit = {
    val agg = relBuilder.scan("student").aggregate(
      relBuilder.groupKey(relBuilder.field("age")),
      relBuilder.sum(false, "s", relBuilder.field("score")),
      relBuilder.max("h", relBuilder.field("height"))).build()
    relBuilder.push(agg)
    // sum(score) > 5
    val pred1 = relBuilder.call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(5))
    assertEquals((10.2 - 5) / 10.2, mq.getSelectivity(agg, pred1))

    // max(height) < 165
    val pred2 = relBuilder.call(LESS_THAN, relBuilder.field(2), relBuilder.literal(165))
    assertEquals((165 - 161.0) / (172.1 - 161.0), mq.getSelectivity(agg, pred2))

    // age < 20 and sum(score) > 5
    val pred3 = relBuilder.and(
      relBuilder.call(LESS_THAN, relBuilder.field(0), relBuilder.literal(20)),
      relBuilder.call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(5)))
    assertEquals(((20.0 - 0.0) / (46.0 - 0.0)) * ((10.2 - 5) / 10.2), mq.getSelectivity(agg, pred3))

    relBuilder.clear()
    val agg1 = relBuilder.scan("student").aggregate(
      relBuilder.groupKey(relBuilder.field("id")),
      relBuilder.aggregateCall(
        SqlAuxiliaryGroupAggFunction, false, false, null, "age", relBuilder.field("age")),
      relBuilder.sum(false, "s", relBuilder.field("score")),
      relBuilder.max("h", relBuilder.field("height"))).build()

    // age < 20 and sum(score) > 5
    relBuilder.push(agg1)
    val pred4 = relBuilder.and(
      relBuilder.call(LESS_THAN, relBuilder.field(1), relBuilder.literal(20)),
      relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(5)))
    assertEquals(((20.0 - 0.0) / (46.0 - 0.0)) * ((5.1 - 5) / 5.1),
      mq.getSelectivity(agg1, pred4))
  }

  @Test
  def testGetSelectivityOnAggregateBatchExec(): Unit = {
    // score > 1
    val pred = relBuilder
      .push(unSplittableGlobalAggWithLocalAgg)
      .call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(1))
    assertEquals(1 / 6.1, mq.getSelectivity(unSplittableGlobalAggWithLocalAgg, pred))

    // score > 1 and count(id) > 20
    val pred1 = relBuilder
      .push(unSplittableGlobalAggWithLocalAgg)
      .and(
        relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(1)),
        relBuilder.call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(20)))
    val maxCnt = (100.0 / 5.0) * 2
    val minCnt = (100.0 / 5.0) / 2
    assertEquals((1 / 6.1) * (maxCnt - 20) / (maxCnt - minCnt),
      mq.getSelectivity(unSplittableGlobalAggWithLocalAgg, pred1), 1e-6)

    // age > 10
    val pred2 = relBuilder
      .push(unSplittableGlobalAggWithLocalAggAndAuxGrouping)
      .call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(10))
    assertEquals((46 - 10) / 46.0, mq.getSelectivity(
      unSplittableGlobalAggWithLocalAggAndAuxGrouping, pred2))

    // age > 10 and max(score) < 3
    val pred3 = relBuilder
      .push(unSplittableGlobalAggWithLocalAggAndAuxGrouping)
      .and(
        relBuilder.call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(10)),
        relBuilder.call(LESS_THAN, relBuilder.field(2), relBuilder.literal(3)))
    assertEquals(((46 - 10) / 46.0) * ((3.0 - 0.0) / (5.1 - 0.0)), mq.getSelectivity(
      unSplittableGlobalAggWithLocalAggAndAuxGrouping, pred3))

    // id < 5 && age > 10
    val pred4 = relBuilder
      .push(unSplittableGlobalAggWithLocalAggAndAuxGrouping)
      .and(relBuilder.call(LESS_THAN, relBuilder.field(0), relBuilder.literal(5)),
        relBuilder.call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(10)))
    assertEquals(((5 - 0) / 10.0) * ((46 - 10) / 46.0), mq.getSelectivity(
      unSplittableGlobalAggWithLocalAggAndAuxGrouping, pred4))

    // age > 10 and max(score) < 3
    val pred5 = relBuilder
      .push(unSplittableGlobalAggWithoutLocalAggWithAuxGrouping)
      .and(
        relBuilder.call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(10)),
        relBuilder.call(LESS_THAN, relBuilder.field(2), relBuilder.literal(3)))
    assertEquals(((46 - 10) / 46.0) * ((3.0 - 0.0) / (5.1 - 0.0)), mq.getSelectivity(
      unSplittableGlobalAggWithoutLocalAggWithAuxGrouping, pred5))
  }

  @Test
  def testGetSelectivityOnSemiJoin(): Unit = {
    val left = new BatchExecTableSourceScan(
      cluster,
      batchExecTraits,
      catalogReader.getTable(ImmutableList.of("t1")).asInstanceOf[FlinkRelOptTable])
    val right = new BatchExecTableSourceScan(
      cluster,
      batchExecTraits,
      catalogReader.getTable(ImmutableList.of("t2")).asInstanceOf[FlinkRelOptTable])
    relBuilder.push(left).push(right)
    // join condition is left.$0=right.$0
    val joinCondition1 = RexUtil.composeConjunction(relBuilder.getRexBuilder, List(
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0))
    ), true)
    val semiJoin1 = createSemiJoin(left, right, joinCondition1, false)
    val predicate = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(0))
    assertEquals(0.5, mq.getSelectivity(semiJoin1, predicate))

    // join condition is left.$0<>right.$0
    val joinCondition2 = RexUtil.composeConjunction(relBuilder.getRexBuilder, List(
      relBuilder.call(LESS_THAN, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0))
    ), true)
    val semiJoin2 = createSemiJoin(left, right, joinCondition2, false)
    assertEquals(0.25, mq.getSelectivity(semiJoin2, predicate))

    val semiJoin3 = createSemiJoin(left, right, joinCondition1, true)
    assertEquals(0.05, mq.getSelectivity(semiJoin3, predicate))

    val semiJoin4 = createSemiJoin(left, right, joinCondition2, true)
    assertEquals(0.25, mq.getSelectivity(semiJoin4, predicate))

    assertEquals(1.0, mq.getSelectivity(semiJoin4, null))
  }

  private def createSemiJoin(
      left: RelNode,
      right: RelNode,
      joinCondition: RexNode,
      isAnti: Boolean): BatchExecHashSemiJoin = {
    val joinInfo = JoinInfo.of(left, right, joinCondition)
    new BatchExecHashSemiJoin(
      cluster,
      batchExecTraits,
      left,
      right,
      false,
      joinCondition,
      joinInfo.leftKeys,
      joinInfo.rightKeys,
      isAnti,
      false,
      false,
      "")
  }

  @Test
  def testGetSelectivityOnExpand(): Unit = {
    relBuilder.clear()
    relBuilder.push(aggWithExpand)
    val predicate1 = relBuilder
      .call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(2))
    assertEquals((5 - 2.0) / (5 - (-5)), mq.getSelectivity(aggWithExpand, predicate1))

    val predicate2 = relBuilder.and(
      relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(2)),
      relBuilder.equals(relBuilder.field(2), relBuilder.literal(1))
    )
    assertEquals(0.075, mq.getSelectivity(aggWithExpand, predicate2))

    relBuilder.clear()
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
    assertEquals(((5.0 - 0) / (10.0 - 0)) * ((5.1 - 2.0) / (5.1 - 0)) * ((23.0 - 0) / (46.0 - 0)),
      mq.getSelectivity(aggWithAuxGroupAndExpand, predicate5))
  }

  val builder: FlinkRelBuilder = relBuilder

  @Test
  def testGetSelectivityOnOverWindowAggBatchExec(): Unit = {
    relBuilder.clear()
    relBuilder.push(overWindowAgg)
    // filter: id <= 2
    val pred = relBuilder.call(
      LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2))
    assertEquals((2.0 - 0) / (10 - 0), mq.getSelectivity(overWindowAgg, pred))
    // filter: age == 10
    val pred1 = relBuilder.call(
      EQUALS, relBuilder.field(2), relBuilder.literal(10))
    assertEquals(1 / 25.0, mq.getSelectivity(overWindowAgg, pred1))
    val pred2 = relBuilder.call(
      AND,
      relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2)),
      relBuilder.call(EQUALS, relBuilder.field(2), relBuilder.literal(10)))
    assertEquals(1 / 25.0 * ((2.0 - 0) / (10 - 0)), mq.getSelectivity(overWindowAgg, pred2))
    val pred3 = relBuilder.call(
      AND,
      relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2)),
      relBuilder.call(EQUALS, relBuilder.field(2), relBuilder.literal(10)),
      relBuilder.call(LESS_THAN, relBuilder.field(4), relBuilder.literal(2)))
    assertEquals(1 / 25.0 * ((2.0 - 0) / (10 - 0)) * 0.5, mq.getSelectivity(overWindowAgg, pred3))
  }

  @Test
  def testGetSelectivityOnLogicalOverWindow(): Unit = {
    relBuilder.clear()
    relBuilder.push(logicalOverWindow)
    // filter: id <= 2
    val pred = relBuilder.call(
      LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2))
    assertEquals((2.0 - 0) / (10 - 0), mq.getSelectivity(logicalOverWindow, pred))
    // filter: age == 10
    val pred1 = relBuilder.call(
      EQUALS, relBuilder.field(2), relBuilder.literal(10))
    assertEquals(1 / 25.0, mq.getSelectivity(logicalOverWindow, pred1))
    val pred2 = relBuilder.call(
      AND,
      relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2)),
      relBuilder.call(EQUALS, relBuilder.field(2), relBuilder.literal(10)))
    assertEquals(1 / 25.0 * ((2.0 - 0) / (10 - 0)), mq.getSelectivity(logicalOverWindow, pred2))
    val pred3 = relBuilder.call(
      AND,
      relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2)),
      relBuilder.call(EQUALS, relBuilder.field(2), relBuilder.literal(10)),
      relBuilder.call(LESS_THAN, relBuilder.field(4), relBuilder.literal(2)))
    assertEquals(1 / 25.0 * ((2.0 - 0) / (10 - 0)) * 0.5,
      mq.getSelectivity(logicalOverWindow, pred3))
  }

  @Test
  def testGetSelectivityOnFlinkLogicalWindowAggregate(): Unit = {
    relBuilder.clear()
    relBuilder.push(flinkLogicalWindowAgg)
    // predicate without time fields and aggCall fields
    val predicate1 = relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(15))
    assertEquals(0.75D, mq.getSelectivity(flinkLogicalWindowAgg, predicate1))

    // predicate with time fields only
    // a > 15 and w$end = 1000000
    val predicate2 = relBuilder.and(
      relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(15)),
      relBuilder.call(EQUALS, relBuilder.field(4), relBuilder.literal(1000000))
    )
    assertEquals(0.75D * 0.15D, mq.getSelectivity(flinkLogicalWindowAgg, predicate2))

    // predicate with time fields and aggCall fields
    // a > 15 and count(c) > 100 and w$end = 1000000
    val predicate3 = relBuilder.and(
      relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(15)),
      relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(100)),
      relBuilder.call(EQUALS, relBuilder.field(4), relBuilder.literal(1000000))
    )
    assertEquals(0.75D * 0.15D * 0.01D, mq.getSelectivity(flinkLogicalWindowAgg, predicate3))

    relBuilder.clear()
    relBuilder.push(flinkLogicalWindowAggWithAuxGroup)
    // a > 15
    val predicate4 = relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(15))
    assertEquals(0.8D, mq.getSelectivity(flinkLogicalWindowAggWithAuxGroup, predicate4))
    // b > 15
    val predicate5 = relBuilder.call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(15))
    assertEquals(0.7D, mq.getSelectivity(flinkLogicalWindowAggWithAuxGroup, predicate5))
    // a > 15 and b > 15 and count(c) > 100 and w$end = 1000000
    val predicate6 = relBuilder.and(
      relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(15)),
      relBuilder.call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(15)),
      relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(100)),
      relBuilder.call(EQUALS, relBuilder.field(4), relBuilder.literal(1000000))
    )
    assertEquals(0.8D * 0.7D * 0.15D * 0.01D,
      mq.getSelectivity(flinkLogicalWindowAggWithAuxGroup, predicate6))
  }

  @Test
  def testGetSelectivityOnLogicalWindowAggregate(): Unit = {
    relBuilder.clear()
    relBuilder.push(logicalWindowAgg)
    // predicate without time fields and aggCall fields
    // a > 15
    val predicate1 = relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(15))
    assertEquals(0.75D, mq.getSelectivity(logicalWindowAgg, predicate1))

    // predicate with time fields only
    // a > 15 and w$end = 1000000
    val predicate2 = relBuilder.and(
      relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(15)),
      relBuilder.call(EQUALS, relBuilder.field(4), relBuilder.literal(1000000))
    )
    assertEquals(0.75D * 0.15D, mq.getSelectivity(logicalWindowAgg, predicate2))

    // predicate with time fields and aggCall fields
    // a > 15 and count(c) > 100 and w$end = 1000000
    val predicate3 = relBuilder.and(
      relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(15)),
      relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(100)),
      relBuilder.call(EQUALS, relBuilder.field(4), relBuilder.literal(1000000))
    )
    assertEquals(0.75D * 0.15D * 0.01D, mq.getSelectivity(logicalWindowAgg, predicate3))

    relBuilder.clear()
    relBuilder.push(logicalWindowAggWithAuxGroup)
    // a > 15
    val predicate4 = relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(15))
    assertEquals(0.8D, mq.getSelectivity(logicalWindowAggWithAuxGroup, predicate4))
    // b > 15
    val predicate5 = relBuilder.call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(15))
    assertEquals(0.7D, mq.getSelectivity(logicalWindowAggWithAuxGroup, predicate5))
    // a > 15 and b > 15 and count(c) > 100 and w$end = 1000000
    val predicate6 = relBuilder.and(
      relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(15)),
      relBuilder.call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(15)),
      relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(100)),
      relBuilder.call(EQUALS, relBuilder.field(4), relBuilder.literal(1000000))
    )
    assertEquals(0.8D * 0.7D * 0.15D * 0.01D,
      mq.getSelectivity(logicalWindowAggWithAuxGroup, predicate6))
  }

  @Test
  def testGetSelectivityOnWindowAggregateBatchExec(): Unit = {
    relBuilder.clear()
    relBuilder.push(globalWindowAggWithLocalAgg)
    // predicate without time fields and aggCall fields
    // a > 15
    val predicate1 = relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(15))
    assertEquals(0.75D, mq.getSelectivity(globalWindowAggWithLocalAgg, predicate1))
    assertEquals(0.75D, mq.getSelectivity(globalWindowAggWithoutLocalAgg, predicate1))

    // predicate with time fields only
    // a > 15 and w$end = 1000000
    val predicate2 = relBuilder.and(
      relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(15)),
      relBuilder.call(EQUALS, relBuilder.field(4), relBuilder.literal(1000000))
    )
    assertEquals(0.75D * 0.15D, mq.getSelectivity(globalWindowAggWithLocalAgg, predicate2))
    assertEquals(0.75D * 0.15D, mq.getSelectivity(globalWindowAggWithoutLocalAgg, predicate2))

    // predicate with time fields and aggCall fields
    // a > 15 and count(c) > 100 and w$end = 1000000
    val predicate3 = relBuilder.and(
      relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(15)),
      relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(100)),
      relBuilder.call(EQUALS, relBuilder.field(4), relBuilder.literal(1000000))
    )
    assertEquals(0.75D * 0.15D * 0.01D, mq.getSelectivity(globalWindowAggWithLocalAgg, predicate3))
    assertEquals(0.75D * 0.15D * 0.01D,
      mq.getSelectivity(globalWindowAggWithoutLocalAgg, predicate3))

    relBuilder.clear()
    relBuilder.push(globalWindowAggWithoutLocalAggWithAuxGrouping)
    // a > 15
    val predicate4 = relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(15))
    assertEquals(0.8D, mq.getSelectivity(globalWindowAggWithoutLocalAggWithAuxGrouping, predicate4))
    // a > 15
    val predicate5 = relBuilder.call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(15))
    assertEquals(0.7D, mq.getSelectivity(globalWindowAggWithoutLocalAggWithAuxGrouping, predicate5))
    // a > 15 and b > 15 and count(c) > 100 and w$end = 1000000
    val predicate6 = relBuilder.and(
      relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(15)),
      relBuilder.call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(15)),
      relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(100)),
      relBuilder.call(EQUALS, relBuilder.field(4), relBuilder.literal(1000000))
    )
    assertEquals(0.8D * 0.7D * 0.15D * 0.01D,
      mq.getSelectivity(globalWindowAggWithoutLocalAggWithAuxGrouping, predicate6))
  }

  @Test
  def testGetSelectivityOnFlinkLogicalRank(): Unit = {
    relBuilder.clear()
    relBuilder.push(flinkLogicalRank)
    // age > 23
    val pred1 = relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(23))
    // rk < 2
    val pred2 = relBuilder.call(LESS_THAN, relBuilder.field(4), relBuilder.literal(2))
    // age > 23 and rk < 2
    val pred3 = relBuilder.and(
      relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(23)),
      relBuilder.call(LESS_THAN, relBuilder.field(4), relBuilder.literal(2)))

    assertEquals(0.5D, mq.getSelectivity(flinkLogicalRank, pred1))
    assertEquals(0.25D, mq.getSelectivity(flinkLogicalRank, pred2))
    assertEquals(0.5D * 0.25D, mq.getSelectivity(flinkLogicalRank, pred3))
  }

  @Test
  def testGetSelectivityOnBatchExecRank(): Unit = {
    relBuilder.clear()
    relBuilder.push(globalBatchExecRank)
    // age > 23
    val pred1 = relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(23))
    // rk < 2
    val pred2 = relBuilder.call(LESS_THAN, relBuilder.field(4), relBuilder.literal(2))
    // rk < 5
    val pred3 = relBuilder.call(LESS_THAN, relBuilder.field(4), relBuilder.literal(5))
    // age > 23 and rk < 2
    val pred4 = relBuilder.and(
      relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(23)),
      relBuilder.call(LESS_THAN, relBuilder.field(4), relBuilder.literal(2)))

    assertEquals(0.5D, mq.getSelectivity(globalBatchExecRank, pred1))
    assertEquals(0.0D, mq.getSelectivity(globalBatchExecRank, pred2))
    assertEquals(2.0D / 3.0D, mq.getSelectivity(globalBatchExecRank, pred3), 1e-6)
    assertEquals(0.5D * 0.0D, mq.getSelectivity(globalBatchExecRank, pred4))
  }
}
