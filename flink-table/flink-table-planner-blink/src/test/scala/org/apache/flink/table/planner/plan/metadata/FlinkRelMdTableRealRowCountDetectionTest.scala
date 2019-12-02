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

import org.apache.flink.table.planner.plan.nodes.calcite.{LogicalExpand, LogicalRank}
import org.apache.flink.table.planner.plan.utils.ExpandUtil
import org.apache.flink.table.runtime.operators.rank.{ConstantRankRange, RankType}

import com.google.common.collect.ImmutableList
import org.apache.calcite.rel.`type`.RelDataTypeFieldImpl
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.logical.{LogicalAggregate, LogicalProject}
import org.apache.calcite.rel.{RelCollations, RelNode}
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.fun.SqlStdOperatorTable.{EQUALS, LESS_THAN}
import org.apache.calcite.util.ImmutableBitSet
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConversions._

class FlinkRelMdTableRealRowCountDetectionTest extends FlinkRelMdHandlerTestBase {

  @Test
  def testHasRealRowCountOnTableScan(): Unit = {
    Array(studentLogicalScan, studentBatchScan, studentStreamScan).foreach { scan =>
      assertTrue(mq.hasRealRowCount(scan))
    }
    // the TableStats of emp is UNKNOWN
    Array(empLogicalScan, empBatchScan, empStreamScan).foreach { scan =>
      assertFalse(mq.hasRealRowCount(scan))
    }
  }

  @Test
  def testHasRealRowCountOnValues(): Unit = {
    assertTrue(mq.hasRealRowCount(logicalValues))
    assertTrue(mq.hasRealRowCount(emptyValues))
  }

  @Test
  def testHasRealRowCountOnProject(): Unit = {
    assertTrue(mq.hasRealRowCount(logicalProject))

    // the TableStats of emp is UNKNOWN
    relBuilder.push(empLogicalScan)
    val projects = List[RexNode](relBuilder.field(0), relBuilder.field(1))
    val project = relBuilder.project(projects).build().asInstanceOf[LogicalProject]
    assertFalse(mq.hasRealRowCount(project))
  }

  @Test
  def testHasRealRowCountOnFilter(): Unit = {
    assertTrue(mq.hasRealRowCount(logicalFilter))

    // the TableStats of emp is UNKNOWN
    relBuilder.push(empLogicalScan)
    // empno < 10
    val expr = relBuilder.call(LESS_THAN, relBuilder.field(0), relBuilder.literal(10))
    val filter = relBuilder.filter(expr).build
    assertFalse(mq.hasRealRowCount(filter))
  }

  @Test
  def testHasRealRowCountOnCalc(): Unit = {
    assertTrue(mq.hasRealRowCount(logicalCalc))
  }

  @Test
  def testHasRealRowCountOnExpand(): Unit = {
    Array(logicalExpand, flinkLogicalExpand, batchExpand, streamExpand).foreach {
      expand => assertTrue(mq.hasRealRowCount(expand))
    }

    // the TableStats of emp is UNKNOWN
    val cluster = empLogicalScan.getCluster
    val expandOutputType = ExpandUtil.buildExpandRowType(
      cluster.getTypeFactory, empLogicalScan.getRowType, Array.empty[Integer])
    val expandProjects = ExpandUtil.createExpandProjects(
      empLogicalScan.getCluster.getRexBuilder,
      empLogicalScan.getRowType,
      expandOutputType,
      ImmutableBitSet.of(1, 3),
      ImmutableList.of(
        ImmutableBitSet.of(1, 3),
        ImmutableBitSet.of(1)),
      Array.empty[Integer])
    val expand = new LogicalExpand(cluster, empLogicalScan.getTraitSet,
      empLogicalScan, expandOutputType, expandProjects, 8)
    assertFalse(mq.hasRealRowCount(expand))
  }

  @Test
  def testHasRealRowCountOnExchange(): Unit = {
    Array(batchExchange, streamExchange).foreach {
      exchange => assertTrue(mq.hasRealRowCount(exchange))
    }
  }

  @Test
  def testHasRealRowCountOnRank(): Unit = {
    Array(logicalRank, flinkLogicalRank, batchLocalRank, streamRank,
      logicalRank2, flinkLogicalRank2, batchLocalRank2, streamRank2,
      logicalRowNumber, flinkLogicalRowNumber, streamRowNumber,
      logicalRankWithVariableRange, flinkLogicalRankWithVariableRange,
      streamRankWithVariableRange).foreach {
      rank => assertTrue(mq.hasRealRowCount(rank))
    }

    // the TableStats of emp is UNKNOWN
    val rank = new LogicalRank(
      cluster,
      logicalTraits,
      empLogicalScan,
      ImmutableBitSet.of(3),
      RelCollations.of(5),
      RankType.RANK,
      new ConstantRankRange(1, 5),
      new RelDataTypeFieldImpl("rk", 8, longType),
      outputRankNumber = true
    )
    assertFalse(mq.hasRealRowCount(rank))
  }

  @Test
  def testHasRealRowCountOnSort(): Unit = {
    Array(logicalSort, flinkLogicalSort, batchSort, streamSort,
      logicalSortLimit, flinkLogicalSortLimit, batchSortLimit, streamSortLimit,
      batchGlobalSortLimit, logicalLimit, flinkLogicalLimit, batchLimit, batchGlobalLimit,
      streamLimit, batchLocalSortLimit, batchLocalLimit).foreach { sort =>
      assertTrue(mq.hasRealRowCount(sort))
    }
  }

  @Test
  def testHasRealRowCountOnAggregate(): Unit = {
    Array(logicalAgg, flinkLogicalAgg, batchGlobalAggWithLocal, batchGlobalAggWithoutLocal,
      batchLocalAgg, streamGlobalAggWithLocal, streamGlobalAggWithoutLocal,
      logicalAggWithAuxGroup, flinkLogicalAggWithAuxGroup,
      batchGlobalAggWithoutLocalWithAuxGroup, batchGlobalAggWithLocalWithAuxGroup,
      batchLocalAggWithAuxGroup).foreach {
      agg => assertTrue(mq.hasRealRowCount(agg))
    }

    // the TableStats of emp is UNKNOWN
    val agg = relBuilder.push(empLogicalScan).aggregate(
      relBuilder.groupKey(relBuilder.field(3)),
      relBuilder.avg(false, "avg_sal", relBuilder.field(5))
    ).build().asInstanceOf[LogicalAggregate]
    assertFalse(mq.hasRealRowCount(agg))

    // group key is empty
    val aggWithEmptyGroup = relBuilder.push(empLogicalScan).aggregate(
      relBuilder.groupKey(),
      relBuilder.avg(false, "avg_sal", relBuilder.field(5))
    ).build().asInstanceOf[LogicalAggregate]
    assertTrue(mq.hasRealRowCount(aggWithEmptyGroup))
  }

  @Test
  def testHasRealRowCountOnWindowAgg(): Unit = {
    Array(logicalWindowAgg, flinkLogicalWindowAgg, batchLocalWindowAgg,
      batchGlobalWindowAggWithoutLocalAgg,
      batchGlobalWindowAggWithLocalAgg, streamWindowAgg,
      logicalWindowAggWithAuxGroup, flinkLogicalWindowAggWithAuxGroup,
      batchLocalWindowAggWithAuxGroup,
      batchGlobalWindowAggWithoutLocalAggWithAuxGroup,
      batchGlobalWindowAggWithLocalAggWithAuxGroup).foreach {
      agg => assertTrue(mq.hasRealRowCount(agg))
    }
  }

  @Test
  def testHasRealRowCountOnOverAgg(): Unit = {
    Array(flinkLogicalOverAgg, batchOverAgg).foreach {
      agg => assertTrue(mq.hasRealRowCount(agg))
    }
  }

  @Test
  def testHasRealRowCountOnJoin(): Unit = {
    assertTrue(mq.hasRealRowCount(logicalInnerJoinOnUniqueKeys))
    assertTrue(mq.hasRealRowCount(logicalLeftJoinOnLHSUniqueKeys))
    assertTrue(mq.hasRealRowCount(logicalRightJoinOnLHSUniqueKeys))
    assertTrue(mq.hasRealRowCount(logicalFullJoinOnRHSUniqueKeys))
    assertTrue(mq.hasRealRowCount(logicalSemiJoinWithEquiAndNonEquiCond))
    assertTrue(mq.hasRealRowCount(logicalAntiJoinOnDisjointKeys))

    //  the TableStats from both sides are UNKNOWN
    val join1: RelNode = relBuilder
      .scan("emp")
      .scan("emp")
      .join(JoinRelType.INNER,
        relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 3)))
      .build
    assertFalse(mq.hasRealRowCount(join1))

    //  the TableStats from one side is UNKNOWN
    val join2: RelNode = relBuilder
      .scan("student")
      .scan("emp")
      .join(JoinRelType.INNER,
        relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 1)))
      .build
    assertFalse(mq.hasRealRowCount(join2))
  }

  @Test
  def testHasRealRowCountOnOverUnion(): Unit = {
    assertTrue(mq.hasRealRowCount(logicalUnion))
    assertTrue(mq.hasRealRowCount(logicalUnionAll))

    //  the TableStats from both sides are UNKNOWN
    val union1: RelNode = relBuilder
      .scan("emp").project(relBuilder.field(0), relBuilder.field(1))
      .scan("emp").project(relBuilder.field(0), relBuilder.field(2))
      .union(false).build()
    assertFalse(mq.hasRealRowCount(union1))

    //  the TableStats from one side is UNKNOWN
    val union2: RelNode = relBuilder
      .scan("emp").project(relBuilder.field(0), relBuilder.field(1))
      .scan("student").project(relBuilder.field(0), relBuilder.field(1))
      .union(false).build()
    assertFalse(mq.hasRealRowCount(union2))
  }

  @Test
  def testHasRealRowCountOnOverIntersect(): Unit = {
    assertTrue(mq.hasRealRowCount(logicalIntersect))
    assertTrue(mq.hasRealRowCount(logicalIntersectAll))
  }

  @Test
  def testHasRealRowCountOnOverMinus(): Unit = {
    assertTrue(mq.hasRealRowCount(logicalMinus))
    assertTrue(mq.hasRealRowCount(logicalMinusAll))
  }

  @Test
  def testHasRealRowCountOnOverDefault(): Unit = {
    assertTrue(mq.hasRealRowCount(testRel))
  }

}
