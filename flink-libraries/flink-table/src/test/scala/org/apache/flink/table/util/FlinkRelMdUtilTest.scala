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

import org.apache.flink.table.plan.metadata.FlinkRelMdHandlerTestBase
import org.apache.flink.table.plan.nodes.calcite.LogicalWindowAggregate
import org.apache.flink.table.plan.nodes.logical.{
  FlinkLogicalExpand, FlinkLogicalRank,
  FlinkLogicalWindowAggregate
}
import org.apache.flink.table.plan.nodes.physical.batch.{
  BatchExecHashWindowAggregate,
  BatchExecLocalHashWindowAggregate, BatchExecWindowAggregateBase
}
import org.apache.flink.table.plan.rules.logical.DecomposeGroupingSetsRule.{
  buildExpandRowType,
  createExpandProjects
}
import org.apache.flink.table.plan.util.FlinkRelMdUtil

import com.google.common.collect.{ImmutableList, ImmutableSet}
import org.apache.calcite.rel.SingleRel
import org.apache.calcite.rel.metadata.RelMdUtil
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.fun.SqlStdOperatorTable.{AND, EQUALS, GREATER_THAN, LESS_THAN}
import org.apache.calcite.util.ImmutableBitSet
import org.junit.Assert.assertEquals
import org.junit.Test

import java.math.BigDecimal

class FlinkRelMdUtilTest {

  @Test
  def testMakeNamePropertiesSelectivityRexNodeOnWinAgg(): Unit = {
    val wrapper = new RelMdHandlerTestWrapper()
    val relBuilder = wrapper.relBuilder
    val rexBuilder = relBuilder.getRexBuilder

    def doMakeNamePropertiesSelectivityRexNode(winAgg: SingleRel, pred: RexNode): RexNode = {
      winAgg match {
        case agg: LogicalWindowAggregate =>
          FlinkRelMdUtil.makeNamePropertiesSelectivityRexNode(agg, pred)
        case agg: FlinkLogicalWindowAggregate =>
          FlinkRelMdUtil.makeNamePropertiesSelectivityRexNode(agg, pred)
        case agg: BatchExecWindowAggregateBase =>
          FlinkRelMdUtil.makeNamePropertiesSelectivityRexNode(agg, pred)
        case _ => throw new IllegalArgumentException()
      }
    }

    def doTestMakeNamePropertiesSelectivityRexNodeOnWinAgg(winAgg: SingleRel): Unit = {
      relBuilder.clear()
      relBuilder.push(winAgg)

      val namePropertiesSelectivityNode = rexBuilder.makeCall(
        RelMdUtil.ARTIFICIAL_SELECTIVITY_FUNC,
        rexBuilder.makeApproxLiteral(new BigDecimal(0.5)))
      val pred = relBuilder.call(
        GREATER_THAN, relBuilder.field(3), relBuilder.literal(1000000))

      val newPred = doMakeNamePropertiesSelectivityRexNode(winAgg, pred)
      assertEquals(namePropertiesSelectivityNode.toString, newPred.toString)

      val pred1 = relBuilder.call(AND,
        relBuilder.call(LESS_THAN, relBuilder.field(0), relBuilder.literal(10)),
        relBuilder.call(EQUALS, relBuilder.field(2), relBuilder.literal(10)),
        relBuilder.call(GREATER_THAN, relBuilder.field(3), relBuilder.literal(1000000)))
      val newPred1 = doMakeNamePropertiesSelectivityRexNode(winAgg, pred1)
      assertEquals(relBuilder.call(AND,
        relBuilder.call(LESS_THAN, relBuilder.field(0), relBuilder.literal(10)),
        relBuilder.call(EQUALS, relBuilder.field(2), relBuilder.literal(10)),
        namePropertiesSelectivityNode).toString, newPred1.toString)
    }

    doTestMakeNamePropertiesSelectivityRexNodeOnWinAgg(wrapper.getLogicalWindowAgg)
    doTestMakeNamePropertiesSelectivityRexNodeOnWinAgg(wrapper.getFlinkLogicalWindowAgg)
    doTestMakeNamePropertiesSelectivityRexNodeOnWinAgg(wrapper.getGlobalWindowAggWithLocalAgg)
    doTestMakeNamePropertiesSelectivityRexNodeOnWinAgg(wrapper.getGlobalWindowAggWithoutLocalAgg)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testMakeNamePropertiesSelectivityRexNodeOnBatchExecLocalWinAgg(): Unit = {
    val wrapper = new RelMdHandlerTestWrapper()
    val relBuilder = wrapper.relBuilder
    val rexBuilder = relBuilder.getRexBuilder
    val winAggWithLocalAgg = wrapper.getLocalWindowAgg
    relBuilder.push(winAggWithLocalAgg)

    val pred = relBuilder.call(
      GREATER_THAN, relBuilder.field(3), relBuilder.literal(1000000))
    FlinkRelMdUtil.makeNamePropertiesSelectivityRexNode(winAggWithLocalAgg, pred)
  }

  @Test
  def testMakeNamePropertiesSelectivityRexNodeOnWinAggWithAuxGrouping(): Unit = {
    val wrapper = new RelMdHandlerTestWrapper()
    val relBuilder = wrapper.relBuilder
    val rexBuilder = relBuilder.getRexBuilder
    val winAgg = wrapper.getGlobalWindowAggWithLocalAggWithAuxGrouping
    relBuilder.push(winAgg)

    val namePropertiesSelectivityNode = rexBuilder.makeCall(
      RelMdUtil.ARTIFICIAL_SELECTIVITY_FUNC,
      rexBuilder.makeApproxLiteral(new BigDecimal(0.5)))
    val pred = relBuilder.call(
      GREATER_THAN, relBuilder.field(3), relBuilder.literal(1000000))

    val newPred = FlinkRelMdUtil.makeNamePropertiesSelectivityRexNode(winAgg, pred)
    assertEquals(namePropertiesSelectivityNode.toString, newPred.toString)

    val pred1 = relBuilder.call(AND,
      relBuilder.call(LESS_THAN, relBuilder.field(0), relBuilder.literal(10)),
      relBuilder.call(EQUALS, relBuilder.field(2), relBuilder.literal(10)),
      relBuilder.call(GREATER_THAN, relBuilder.field(3), relBuilder.literal(1000000)))
    val newPred1 = FlinkRelMdUtil.makeNamePropertiesSelectivityRexNode(winAgg, pred1)
    assertEquals(relBuilder.call(AND,
      relBuilder.call(LESS_THAN, relBuilder.field(0), relBuilder.literal(10)),
      relBuilder.call(EQUALS, relBuilder.field(2), relBuilder.literal(10)),
      namePropertiesSelectivityNode).toString, newPred1.toString)
  }

  @Test
  def testSplitPredicateOnRank(): Unit = {
    val wrapper = new RelMdHandlerTestWrapper()
    val relBuilder = wrapper.relBuilder
    val rank = wrapper.getFlinkLogicalRank
    relBuilder.push(rank)

    // age > 23
    val pred1 = relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(23))
    val (nonRankPred1, rankPred1) = FlinkRelMdUtil.splitPredicateOnRank(rank, pred1)
    assertEquals(pred1.toString, nonRankPred1.get.toString)
    assertEquals(None, rankPred1)

    // rk < 2
    val pred2 = relBuilder.call(LESS_THAN, relBuilder.field(4), relBuilder.literal(2))
    val (nonRankPred2, rankPred2) = FlinkRelMdUtil.splitPredicateOnRank(rank, pred2)
    assertEquals(None, nonRankPred2)
    assertEquals(pred2.toString, rankPred2.get.toString)

    // age > 23 and rk < 2
    val pred3 = relBuilder.and(
      relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(23)),
      relBuilder.call(LESS_THAN, relBuilder.field(4), relBuilder.literal(2)))
    val (nonRankPred3, rankPred3) = FlinkRelMdUtil.splitPredicateOnRank(rank, pred3)
    assertEquals(
      relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(23)).toString,
      nonRankPred3.get.toString)
    assertEquals(
      relBuilder.call(LESS_THAN, relBuilder.field(4), relBuilder.literal(2)).toString,
      rankPred3.get.toString)
  }

  @Test
  def testGetInputRefIndicesFromExpand(): Unit = {
    val wrapper = new RelMdHandlerTestWrapper()
    val relBuilder = wrapper.relBuilder
    val ts = relBuilder.scan("student").build()
    val expandOutputType = buildExpandRowType(
      ts.getCluster.getTypeFactory, ts.getRowType, Array.empty[Integer])
    val expandProjects1 = createExpandProjects(
      ts.getCluster.getRexBuilder,
      ts.getRowType,
      expandOutputType,
      ImmutableBitSet.of(0, 1, 2, 3),
      ImmutableList.of(
        ImmutableBitSet.of(0),
        ImmutableBitSet.of(1),
        ImmutableBitSet.of(2),
        ImmutableBitSet.of(3)
      ), Array.empty[Integer])
    val expand1 = new FlinkLogicalExpand(
      ts.getCluster, ts.getTraitSet, ts, expandOutputType, expandProjects1, 4)
    assertEquals(ImmutableSet.of(0, -1), FlinkRelMdUtil.getInputRefIndices(0, expand1))
    assertEquals(ImmutableSet.of(1, -1), FlinkRelMdUtil.getInputRefIndices(1, expand1))
    assertEquals(ImmutableSet.of(2, -1), FlinkRelMdUtil.getInputRefIndices(2, expand1))
    assertEquals(ImmutableSet.of(3, -1), FlinkRelMdUtil.getInputRefIndices(3, expand1))
    assertEquals(ImmutableSet.of(-1), FlinkRelMdUtil.getInputRefIndices(4, expand1))

    val expandProjects2 = createExpandProjects(
      ts.getCluster.getRexBuilder,
      ts.getRowType,
      expandOutputType,
      ImmutableBitSet.of(0, 1, 2, 3),
      ImmutableList.of(
        ImmutableBitSet.of(0, 1),
        ImmutableBitSet.of(0, 1, 2),
        ImmutableBitSet.of(0, 2, 3)
      ), Array.empty[Integer])
    val expand2 = new FlinkLogicalExpand(
      ts.getCluster, ts.getTraitSet, ts, expandOutputType, expandProjects2, 4)
    assertEquals(ImmutableSet.of(0), FlinkRelMdUtil.getInputRefIndices(0, expand2))
    assertEquals(ImmutableSet.of(1, -1), FlinkRelMdUtil.getInputRefIndices(1, expand2))
    assertEquals(ImmutableSet.of(2, -1), FlinkRelMdUtil.getInputRefIndices(2, expand2))
    assertEquals(ImmutableSet.of(3, -1), FlinkRelMdUtil.getInputRefIndices(3, expand2))
    assertEquals(ImmutableSet.of(-1), FlinkRelMdUtil.getInputRefIndices(4, expand2))
  }

  private class RelMdHandlerTestWrapper extends FlinkRelMdHandlerTestBase {
    super.setUp()

    def getLogicalWindowAgg: LogicalWindowAggregate = logicalWindowAgg

    def getFlinkLogicalWindowAgg: FlinkLogicalWindowAggregate = flinkLogicalWindowAgg

    def getGlobalWindowAggWithLocalAgg: BatchExecHashWindowAggregate = globalWindowAggWithLocalAgg

    def getGlobalWindowAggWithoutLocalAgg: BatchExecHashWindowAggregate =
      globalWindowAggWithoutLocalAgg

    def getLocalWindowAgg: BatchExecLocalHashWindowAggregate = localWindowAgg

    def getGlobalWindowAggWithLocalAggWithAuxGrouping: BatchExecHashWindowAggregate =
      globalWindowAggWithoutLocalAggWithAuxGrouping

    def getGlobalWindowAggWithoutLocalAggWithAuxGrouping: BatchExecHashWindowAggregate =
      globalWindowAggWithoutLocalAggWithAuxGrouping

    def getFlinkLogicalRank: FlinkLogicalRank = flinkLogicalRank
  }

}
