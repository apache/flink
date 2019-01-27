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

import org.apache.flink.table.plan.nodes.physical.batch._

import org.apache.calcite.rex.{RexProgram, RexUtil}
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.util.ImmutableBitSet
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConversions._

class FlinkRelMdPopulationSizeTest extends FlinkRelMdHandlerTestBase {

  @Test
  def testGetPopulationSizeOnTableScan(): Unit = {
    val ts = relBuilder.scan("t1").build()
    assertEquals(3D, mq.getPopulationSize(ts, ImmutableBitSet.of(0)))
    assertEquals(5D, mq.getPopulationSize(ts, ImmutableBitSet.of(1)))
    assertEquals(15D, mq.getPopulationSize(ts, ImmutableBitSet.of(0, 1)))

    val ts2 = relBuilder.scan("student").build()
    assertEquals(50D, mq.getPopulationSize(ts2, ImmutableBitSet.of(0)))
  }

  @Test
  def testGetPopulationSizeOnSortBatchExec(): Unit = {
    assertEquals(3D, mq.getPopulationSize(sortBatchExec, ImmutableBitSet.of(0)))
    assertEquals(5D, mq.getPopulationSize(sortBatchExec, ImmutableBitSet.of(1)))
    assertEquals(15D, mq.getPopulationSize(sortBatchExec, ImmutableBitSet.of(0, 1)))
  }

  @Test
  def testGetPopulationSizeOnLimitBatchExec(): Unit = {
    assertEquals(3D, mq.getPopulationSize(limitBatchExec, ImmutableBitSet.of(0)))
    assertEquals(5D, mq.getPopulationSize(limitBatchExec, ImmutableBitSet.of(1)))
    assertEquals(15D, mq.getPopulationSize(limitBatchExec, ImmutableBitSet.of(0, 1)))
  }

  @Test
  def testGetPopulationSizeOnSortLimitBatchExec(): Unit = {
    assertEquals(3D, mq.getPopulationSize(sortLimitBatchExec, ImmutableBitSet.of(0)))
    assertEquals(5D, mq.getPopulationSize(sortLimitBatchExec, ImmutableBitSet.of(1)))
    assertEquals(15D, mq.getPopulationSize(sortLimitBatchExec, ImmutableBitSet.of(0, 1)))
  }

  @Test
  def testGetPopulationSizeOnUnionBatchExec(): Unit = {
    assertEquals(8D, mq.getPopulationSize(unionBatchExec, ImmutableBitSet.of(0)))
    assertEquals(12D, mq.getPopulationSize(unionBatchExec, ImmutableBitSet.of(1)))
  }

  @Test
  def testGetPopulationSizeOnAggregateBatchExec(): Unit = {
    assertEquals(5D, mq.getPopulationSize(unSplittableGlobalAggWithLocalAgg, ImmutableBitSet.of(0)))
    assertEquals(10D,
      mq.getPopulationSize(unSplittableGlobalAgg2WithLocalAgg, ImmutableBitSet.of(1)))
    assertEquals(10D,
      mq.getPopulationSize(unSplittableGlobalAgg2WithoutLocalAgg, ImmutableBitSet.of(1)))

    assertEquals(50D, mq.getPopulationSize(
      unSplittableGlobalAggWithLocalAggAndAuxGrouping, ImmutableBitSet.of(0)))
    assertEquals(25D, mq.getPopulationSize(
      unSplittableGlobalAggWithLocalAggAndAuxGrouping, ImmutableBitSet.of(1)))
    assertEquals(1D, mq.getPopulationSize(
      unSplittableGlobalAggWithLocalAggAndAuxGrouping, ImmutableBitSet.of(2)))
    assertEquals(50D, mq.getPopulationSize(
      unSplittableGlobalAggWithLocalAggAndAuxGrouping, ImmutableBitSet.of(0, 1)))
    assertEquals(25D, mq.getPopulationSize(
      unSplittableGlobalAggWithLocalAggAndAuxGrouping, ImmutableBitSet.of(1, 2)))
    assertEquals(50D, mq.getPopulationSize(
      unSplittableGlobalAggWithLocalAggAndAuxGrouping, ImmutableBitSet.of(0, 1, 2)))
  }

  @Test
  def testGetPopulationSizeOnOverWindowAggBatchExec(): Unit = {
    assertEquals(50D, mq.getPopulationSize(overWindowAgg, ImmutableBitSet.of(0)))
    // cannot estimate population size if groupKeys contains aggCall position
    assertNull(mq.getPopulationSize(overWindowAgg, ImmutableBitSet.of(0, 4)))
  }

  @Test
  def testGetPopulationSizeOnLogicalOverWindow(): Unit = {
    assertEquals(50D, mq.getPopulationSize(logicalOverWindow, ImmutableBitSet.of(0)))
    // cannot estimate population size if groupKeys contains aggCall position
    assertNull(mq.getPopulationSize(logicalOverWindow, ImmutableBitSet.of(0, 4)))
  }

  @Test
  def testGetPopulationSizeOnCalc(): Unit = {
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
    // project + filter
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
    assertEquals(1D, mq.getPopulationSize(calc, ImmutableBitSet.of(0)))
    assertEquals(3D, mq.getPopulationSize(calc, ImmutableBitSet.of(1)))
    assertEquals(5D, mq.getPopulationSize(calc, ImmutableBitSet.of(2)))
    assertEquals(15D, mq.getPopulationSize(calc, ImmutableBitSet.of(1, 2)))
    assertEquals(4.541715249554787, mq.getPopulationSize(calc, ImmutableBitSet.of(0, 1, 2)))

    // no filter
    val program2 = RexProgram.create(scanOfT1.getRowType, projects, null, outputRowType, rexBuilder)
    val calc2 = new BatchExecCalc(cluster, batchExecTraits, scanOfT1, outputRowType, program2, "")
    assertEquals(3D, mq.getPopulationSize(calc2, ImmutableBitSet.of(1)))
    assertEquals(5D, mq.getPopulationSize(calc2, ImmutableBitSet.of(2)))
    assertEquals(15D, mq.getPopulationSize(calc2, ImmutableBitSet.of(1, 2)))
  }

  @Test
  def testGetPopulationSizeOnExpand(): Unit = {
    assertEquals(3D, mq.getPopulationSize(aggWithExpand, ImmutableBitSet.of(0)))
    assertEquals(5D, mq.getPopulationSize(aggWithExpand, ImmutableBitSet.of(1)))
    assertEquals(15D, mq.getPopulationSize(aggWithExpand, ImmutableBitSet.of(0, 1)))
    assertEquals(4D, mq.getPopulationSize(aggWithExpand, ImmutableBitSet.of(0, 2)))
    assertEquals(6D, mq.getPopulationSize(aggWithExpand, ImmutableBitSet.of(1, 2)))
    assertEquals(8D, mq.getPopulationSize(aggWithExpand, ImmutableBitSet.of(0, 1, 2)))

    assertEquals(50D, mq.getPopulationSize(aggWithAuxGroupAndExpand, ImmutableBitSet.of(0)))
    assertEquals(2D, mq.getPopulationSize(aggWithAuxGroupAndExpand, ImmutableBitSet.of(1)))
    assertEquals(7D, mq.getPopulationSize(aggWithAuxGroupAndExpand, ImmutableBitSet.of(2)))
    assertEquals(25D, mq.getPopulationSize(aggWithAuxGroupAndExpand, ImmutableBitSet.of(3)))
    assertEquals(Math.sqrt(200),
                 mq.getPopulationSize(aggWithAuxGroupAndExpand, ImmutableBitSet.of(4)))
    assertEquals(50D, mq.getPopulationSize(aggWithAuxGroupAndExpand, ImmutableBitSet.of(0, 2)))
    assertEquals(50D, mq.getPopulationSize(aggWithAuxGroupAndExpand, ImmutableBitSet.of(0, 3)))
    assertEquals(50D, mq.getPopulationSize(aggWithAuxGroupAndExpand, ImmutableBitSet.of(0, 2, 3)))
    assertEquals(100D, mq.getPopulationSize(
      aggWithAuxGroupAndExpand, ImmutableBitSet.of(0, 2, 3, 4)))
  }

  @Test
  def testGetPopulationSizeOnFlinkLogicalWindowAggregate(): Unit = {
    assertEquals(30D, mq.getPopulationSize(flinkLogicalWindowAgg, ImmutableBitSet.of(0)))
    assertEquals(5D, mq.getPopulationSize(flinkLogicalWindowAgg, ImmutableBitSet.of(1)))
    assertEquals(50D, mq.getPopulationSize(flinkLogicalWindowAgg, ImmutableBitSet.of(0, 1)))
    assertEquals(50D, mq.getPopulationSize(flinkLogicalWindowAgg, ImmutableBitSet.of(0, 2)))
    assertEquals(null, mq.getPopulationSize(flinkLogicalWindowAgg, ImmutableBitSet.of(3)))
    assertEquals(null, mq.getPopulationSize(flinkLogicalWindowAgg, ImmutableBitSet.of(0, 3)))
    assertEquals(null, mq.getPopulationSize(flinkLogicalWindowAgg, ImmutableBitSet.of(1, 3)))
    assertEquals(null, mq.getPopulationSize(flinkLogicalWindowAgg, ImmutableBitSet.of(2, 3)))

    assertEquals(50D, mq.getPopulationSize(
      flinkLogicalWindowAggWithAuxGroup, ImmutableBitSet.of(0)))
    assertEquals(48D, mq.getPopulationSize(
      flinkLogicalWindowAggWithAuxGroup, ImmutableBitSet.of(1)))
    assertEquals(10D, mq.getPopulationSize(
      flinkLogicalWindowAggWithAuxGroup, ImmutableBitSet.of(2)))
    assertEquals(null, mq.getPopulationSize(
      flinkLogicalWindowAggWithAuxGroup, ImmutableBitSet.of(3)))
    assertEquals(50D, mq.getPopulationSize(
      flinkLogicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1)))
    assertEquals(50D, mq.getPopulationSize(
      flinkLogicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1, 2)))
    assertEquals(null, mq.getPopulationSize(
      flinkLogicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1, 3)))
  }

  @Test
  def testGetPopulationSizeOnLogicalWindowAggregate(): Unit = {
    assertEquals(30D, mq.getPopulationSize(logicalWindowAgg, ImmutableBitSet.of(0)))
    assertEquals(5D, mq.getPopulationSize(logicalWindowAgg, ImmutableBitSet.of(1)))
    assertEquals(50D, mq.getPopulationSize(logicalWindowAgg, ImmutableBitSet.of(0, 1)))
    assertEquals(50D, mq.getPopulationSize(logicalWindowAgg, ImmutableBitSet.of(0, 2)))
    assertEquals(null, mq.getPopulationSize(logicalWindowAgg, ImmutableBitSet.of(3)))
    assertEquals(null, mq.getPopulationSize(logicalWindowAgg, ImmutableBitSet.of(0, 3)))
    assertEquals(null, mq.getPopulationSize(logicalWindowAgg, ImmutableBitSet.of(1, 3)))
    assertEquals(null, mq.getPopulationSize(logicalWindowAgg, ImmutableBitSet.of(2, 3)))

    assertEquals(50D, mq.getPopulationSize(logicalWindowAggWithAuxGroup, ImmutableBitSet.of(0)))
    assertEquals(48D, mq.getPopulationSize(logicalWindowAggWithAuxGroup, ImmutableBitSet.of(1)))
    assertEquals(10D, mq.getPopulationSize(logicalWindowAggWithAuxGroup, ImmutableBitSet.of(2)))
    assertEquals(null, mq.getPopulationSize(logicalWindowAggWithAuxGroup, ImmutableBitSet.of(3)))
    assertEquals(50D, mq.getPopulationSize(logicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1)))
    assertEquals(50D, mq.getPopulationSize(
      logicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1, 2)))
    assertEquals(null, mq.getPopulationSize(
      logicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1, 3)))
  }

  @Test
  def testGetPopulationSizeOnWindowAggregateBatchExec(): Unit = {
    assertEquals(30D, mq.getPopulationSize(globalWindowAggWithLocalAgg, ImmutableBitSet.of(0)))
    assertEquals(5D, mq.getPopulationSize(globalWindowAggWithLocalAgg, ImmutableBitSet.of(1)))
    assertEquals(50D, mq.getPopulationSize(globalWindowAggWithLocalAgg, ImmutableBitSet.of(0, 1)))
    assertEquals(50D, mq.getPopulationSize(globalWindowAggWithLocalAgg, ImmutableBitSet.of(0, 2)))
    assertEquals(null, mq.getPopulationSize(globalWindowAggWithLocalAgg, ImmutableBitSet.of(3)))
    assertEquals(null, mq.getPopulationSize(globalWindowAggWithLocalAgg, ImmutableBitSet.of(0, 3)))
    assertEquals(null, mq.getPopulationSize(globalWindowAggWithLocalAgg, ImmutableBitSet.of(1, 3)))
    assertEquals(null, mq.getPopulationSize(globalWindowAggWithLocalAgg, ImmutableBitSet.of(2, 3)))

    assertEquals(30D, mq.getPopulationSize(globalWindowAggWithoutLocalAgg, ImmutableBitSet.of(0)))
    assertEquals(5D, mq.getPopulationSize(globalWindowAggWithoutLocalAgg, ImmutableBitSet.of(1)))
    assertEquals(50D,
      mq.getPopulationSize(globalWindowAggWithoutLocalAgg, ImmutableBitSet.of(0, 1)))
    assertEquals(50D,
      mq.getPopulationSize(globalWindowAggWithoutLocalAgg, ImmutableBitSet.of(0, 2)))
    assertEquals(null,
      mq.getPopulationSize(globalWindowAggWithoutLocalAgg, ImmutableBitSet.of(3)))
    assertEquals(null,
      mq.getPopulationSize(globalWindowAggWithoutLocalAgg, ImmutableBitSet.of(0, 3)))
    assertEquals(null,
      mq.getPopulationSize(globalWindowAggWithoutLocalAgg, ImmutableBitSet.of(1, 3)))
    assertEquals(null,
      mq.getPopulationSize(globalWindowAggWithoutLocalAgg, ImmutableBitSet.of(2, 3)))

    assertEquals(50D, mq.getPopulationSize(
      globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(0)))
    assertEquals(48D, mq.getPopulationSize(
      globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(1)))
    assertEquals(10D, mq.getPopulationSize(
      globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(2)))
    assertEquals(null, mq.getPopulationSize(
      globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(3)))
    assertEquals(50D, mq.getPopulationSize(
      globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(0, 1)))
    assertEquals(50D, mq.getPopulationSize(
      globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(0, 1, 2)))
    assertEquals(null, mq.getPopulationSize(
      globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(0, 1, 3)))

    assertEquals(50D, mq.getPopulationSize(
      globalWindowAggWithLocalAggWithAuxGrouping, ImmutableBitSet.of(0)))
    assertEquals(48D, mq.getPopulationSize(
      globalWindowAggWithLocalAggWithAuxGrouping, ImmutableBitSet.of(1)))
    assertEquals(10D, mq.getPopulationSize(
      globalWindowAggWithLocalAggWithAuxGrouping, ImmutableBitSet.of(2)))
    assertEquals(null, mq.getPopulationSize(
      globalWindowAggWithLocalAggWithAuxGrouping, ImmutableBitSet.of(3)))
    assertEquals(50D, mq.getPopulationSize(
      globalWindowAggWithLocalAggWithAuxGrouping, ImmutableBitSet.of(0, 1)))
    assertEquals(50D, mq.getPopulationSize(
      globalWindowAggWithLocalAggWithAuxGrouping, ImmutableBitSet.of(0, 1, 2)))
    assertEquals(null, mq.getPopulationSize(
      globalWindowAggWithLocalAggWithAuxGrouping, ImmutableBitSet.of(0, 1, 3)))
  }

  @Test
  def testGetPopulationSizeOnFlinkLogicalRank(): Unit = {
    assertEquals(50D, mq.getPopulationSize(flinkLogicalRank, ImmutableBitSet.of(0)))
    assertEquals(7D, mq.getPopulationSize(flinkLogicalRank, ImmutableBitSet.of(1)))
    assertEquals(5D, mq.getPopulationSize(flinkLogicalRank, ImmutableBitSet.of(4)))
    assertEquals(50D, mq.getPopulationSize(flinkLogicalRank, ImmutableBitSet.of(0, 1)))
    assertEquals(50D, mq.getPopulationSize(flinkLogicalRank, ImmutableBitSet.of(0, 2)))
    assertEquals(50D, mq.getPopulationSize(flinkLogicalRank, ImmutableBitSet.of(0, 4)))
    assertEquals(35D, mq.getPopulationSize(flinkLogicalRank, ImmutableBitSet.of(1, 4)))
    assertEquals(50D, mq.getPopulationSize(flinkLogicalRank, ImmutableBitSet.of(0, 1, 4)))
  }

  @Test
  def testGetPopulationSizeOnBatchExecRank(): Unit = {
    assertEquals(50D, mq.getPopulationSize(globalBatchExecRank, ImmutableBitSet.of(0)))
    assertEquals(7D, mq.getPopulationSize(globalBatchExecRank, ImmutableBitSet.of(1)))
    assertEquals(3D, mq.getPopulationSize(globalBatchExecRank, ImmutableBitSet.of(4)))
    assertEquals(50D, mq.getPopulationSize(globalBatchExecRank, ImmutableBitSet.of(0, 1)))
    assertEquals(50D, mq.getPopulationSize(globalBatchExecRank, ImmutableBitSet.of(0, 2)))
    assertEquals(50D, mq.getPopulationSize(globalBatchExecRank, ImmutableBitSet.of(0, 4)))
    assertEquals(21D, mq.getPopulationSize(globalBatchExecRank, ImmutableBitSet.of(1, 4)))
    assertEquals(50D, mq.getPopulationSize(globalBatchExecRank, ImmutableBitSet.of(0, 1, 4)))

    assertEquals(50D, mq.getPopulationSize(localBatchExecRank, ImmutableBitSet.of(0)))
    assertEquals(7D, mq.getPopulationSize(localBatchExecRank, ImmutableBitSet.of(1)))
    assertEquals(50D, mq.getPopulationSize(localBatchExecRank, ImmutableBitSet.of(0, 1)))
  }

  @Test
  def testGetPopulationSizeOnStreamExecRank(): Unit = {
    assertEquals(50D, mq.getPopulationSize(streamExecRowNumber, ImmutableBitSet.of(0)))
    assertEquals(7D, mq.getPopulationSize(streamExecRowNumber, ImmutableBitSet.of(1)))
    assertEquals(50D, mq.getPopulationSize(streamExecRowNumber, ImmutableBitSet.of(0, 1)))
  }
}
