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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.table.plan.nodes.physical.batch._
import org.apache.flink.table.plan.util.FlinkRelMdUtil

import com.google.common.collect.ImmutableList
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rex.{RexProgram, RexUtil}
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.util.ImmutableBitSet
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConversions._

class FlinkRelMdDistinctRowCountTest extends FlinkRelMdHandlerTestBase {

  @Test
  def testGetDistinctRowCountOnAggregate(): Unit = {
    // aggCall: min(age) as min_age
    val minAgeAggCall = AggregateCall.create(
      SqlStdOperatorTable.MIN,
      false,
      ImmutableList.of(Integer.valueOf(2)),
      -1,
      typeFactory.createTypeFromTypeInfo(BasicTypeInfo.INT_TYPE_INFO, isNullable = true),
      "min_age")
    // aggCall: count(height) as count_height
    val countHeightAggCall = AggregateCall.create(
      SqlStdOperatorTable.COUNT,
      false,
      ImmutableList.of(Integer.valueOf(3)),
      -1,
      typeFactory.createTypeFromTypeInfo(BasicTypeInfo.LONG_TYPE_INFO, isNullable = false),
      "count_height")
    val agg = relBuilder.scan("student").aggregate(
      relBuilder.groupKey(0), Seq(minAgeAggCall, countHeightAggCall).toList).build()
    assertEquals(25D, mq.getDistinctRowCount(agg, ImmutableBitSet.of(1, 2), null))
  }

  @Test
  def testGetDistinctRowCountOnTableScan(): Unit = {
    val ts = relBuilder.scan("t1").build()
    assertEquals(3D, mq.getDistinctRowCount(ts, ImmutableBitSet.of(0), null))
    assertEquals(5D, mq.getDistinctRowCount(ts, ImmutableBitSet.of(1), null))
    assertEquals(15D, mq.getDistinctRowCount(ts, ImmutableBitSet.of(0, 1), null))
    val ts2 = relBuilder.scan("student").build()
    val condition = relBuilder.push(ts2).call(EQUALS, relBuilder.field(2), relBuilder.literal(2))
    assertEquals(2D, mq.getDistinctRowCount(ts2, ImmutableBitSet.of(0), condition))
    // If groupKey is uniqueKeys and predicate is null, return rowCount of table as ndv if available
    val ts3 = relBuilder.scan("t3").build()
    assertEquals(100D, mq.getDistinctRowCount(ts3, ImmutableBitSet.of(0), null))
    // If groupKey is uniqueKeys and predicate is null, return null when rowCount is not available
    val ts4 = relBuilder.scan("t4").build()
    assertNull(mq.getDistinctRowCount(ts4, ImmutableBitSet.of(0), null))
    // the distinct row count of table stats is equal to row count
    val ts5 = relBuilder.scan("t5").build()
    assertEquals(100D, mq.getDistinctRowCount(ts5, ImmutableBitSet.of(0, 1), null))
    val ts8 = relBuilder.scan("t8").build()
    assertEquals(50D, mq.getDistinctRowCount(ts8, ImmutableBitSet.of(1, 2), null))
  }

  @Test
  def testGetDistinctRowCountOnSortBatchExec(): Unit = {
    assertEquals(3D, mq.getDistinctRowCount(sortBatchExec, ImmutableBitSet.of(0), null))
    assertEquals(5D, mq.getDistinctRowCount(sortBatchExec, ImmutableBitSet.of(1), null))
    assertEquals(15D, mq.getDistinctRowCount(sortBatchExec, ImmutableBitSet.of(0, 1), null))
  }

  @Test
  def testGetDistinctRowCountOnLimitBatchExec(): Unit = {
    assertEquals(3D, mq.getDistinctRowCount(limitBatchExec, ImmutableBitSet.of(0), null))
    assertEquals(5D, mq.getDistinctRowCount(limitBatchExec, ImmutableBitSet.of(1), null))
    assertEquals(10D, mq.getDistinctRowCount(limitBatchExec, ImmutableBitSet.of(0, 1), null))
  }

  @Test
  def testGetDistinctRowCountOnSortLimitBatchExec(): Unit = {
    assertEquals(3D, mq.getDistinctRowCount(sortLimitBatchExec, ImmutableBitSet.of(0), null))
    assertEquals(5D, mq.getDistinctRowCount(sortLimitBatchExec, ImmutableBitSet.of(1), null))
    assertEquals(10D, mq.getDistinctRowCount(sortLimitBatchExec, ImmutableBitSet.of(0, 1), null))
  }

  @Test
  def testGetDistinctRowCountOnSort(): Unit = {
    assertEquals(3D, mq.getDistinctRowCount(sort, ImmutableBitSet.of(0), null))
    assertEquals(5D, mq.getDistinctRowCount(sort, ImmutableBitSet.of(1), null))
    assertEquals(10D, mq.getDistinctRowCount(sort, ImmutableBitSet.of(0, 1), null))
  }

  @Test
  def testGetDistinctRowCountOnUnionBatchExec(): Unit = {
    assertEquals(8D, mq.getDistinctRowCount(unionBatchExec, ImmutableBitSet.of(0), null))
    assertEquals(12D, mq.getDistinctRowCount(unionBatchExec, ImmutableBitSet.of(1), null))
  }

  @Test
  def testGetDistinctRowCountOnUnion(): Unit = {
    assertEquals(8D, mq.getDistinctRowCount(union, ImmutableBitSet.of(0), null))
    assertEquals(12D, mq.getDistinctRowCount(union, ImmutableBitSet.of(1), null))
    assertEquals(8D, mq.getDistinctRowCount(unionAll, ImmutableBitSet.of(0), null))
    assertEquals(12D, mq.getDistinctRowCount(unionAll, ImmutableBitSet.of(1), null))
  }

  @Test
  def testGetDistinctRowCountOnOverWindowAgg(): Unit = {
    assertEquals(50D, mq.getDistinctRowCount(overWindowAgg, ImmutableBitSet.of(0), null))
    val pred = relBuilder
      .push(overWindowAgg)
      .call(EQUALS, relBuilder.field(2), relBuilder.literal(2))
    assertEquals(2D, mq.getDistinctRowCount(overWindowAgg, ImmutableBitSet.of(0), pred))
    // cannot estimate ndv if groupKeys contains aggCall position
    assertNull(mq.getDistinctRowCount(overWindowAgg, ImmutableBitSet.of(0, 4), pred))
  }

  @Test
  def testGetDistinctRowCountOnLogicalOverWindow(): Unit = {
    assertEquals(50D, mq.getDistinctRowCount(logicalOverWindow, ImmutableBitSet.of(0), null))
    val pred = relBuilder
      .push(logicalOverWindow)
      .call(EQUALS, relBuilder.field(2), relBuilder.literal(2))
    assertEquals(2D, mq.getDistinctRowCount(logicalOverWindow, ImmutableBitSet.of(0), pred))
    // cannot estimate ndv if groupKeys contains aggCall position
    assertNull(mq.getDistinctRowCount(logicalOverWindow, ImmutableBitSet.of(0, 4), pred))
  }

  @Test
  def testGetDistinctRowCountOnAggregateBatchExec(): Unit = {
    // score <= 1
    val pred = relBuilder
      .push(unSplittableGlobalAggWithLocalAgg)
      .call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(1))
    val pushDownPred = relBuilder
      .push(unSplittableGlobalAggWithLocalAgg)
      .call(LESS_THAN_OR_EQUAL, relBuilder.field(1), relBuilder.literal(1))
    val selectivity = mq.getSelectivity(scanOfT1, pushDownPred)
    assertEquals(
      FlinkRelMdUtil.adaptNdvBasedOnSelectivity(100D, 5D, selectivity),
      mq.getDistinctRowCount(unSplittableGlobalAggWithLocalAgg, ImmutableBitSet.of(0), pred), 1e-6)
    // count(id) <= 20
    val pred1 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(1), relBuilder.literal(20))
    assertEquals(5D * ((20.0 - 10.0) / (40.0 - 10.0)),
      mq.getDistinctRowCount(unSplittableGlobalAggWithLocalAgg, ImmutableBitSet.of(0), pred1), 1e-6)
    assertEquals(10D,
      mq.getDistinctRowCount(unSplittableGlobalAgg2WithLocalAgg, ImmutableBitSet.of(1), null))
    assertEquals(10D,
      mq.getDistinctRowCount(unSplittableGlobalAgg2WithoutLocalAgg, ImmutableBitSet.of(1), null))

    // id <= 1
    val pred2 = relBuilder
      .push(unSplittableGlobalAggWithLocalAggAndAuxGrouping)
      .call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(1))
    assertEquals(5D, mq.getDistinctRowCount(
      unSplittableGlobalAggWithLocalAggAndAuxGrouping, ImmutableBitSet.of(0), pred2))
    assertEquals(50D, mq.getDistinctRowCount(
      unSplittableGlobalAggWithLocalAggAndAuxGrouping, ImmutableBitSet.of(0), null))
    // age > 23
    val pred3 = relBuilder
      .push(unSplittableGlobalAggWithLocalAggAndAuxGrouping)
      .call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(23))
    assertEquals(
      FlinkRelMdUtil.adaptNdvBasedOnSelectivity(50D, 25D, 0.5D),
      mq.getDistinctRowCount(
        unSplittableGlobalAggWithLocalAggAndAuxGrouping, ImmutableBitSet.of(1), pred3))
    assertEquals(25D, mq.getDistinctRowCount(
      unSplittableGlobalAggWithLocalAggAndAuxGrouping, ImmutableBitSet.of(1), null))
    assertEquals(5D, mq.getDistinctRowCount(
      unSplittableGlobalAggWithLocalAggAndAuxGrouping, ImmutableBitSet.of(0, 1), pred2))
    assertEquals(25D, mq.getDistinctRowCount(
      unSplittableGlobalAggWithLocalAggAndAuxGrouping, ImmutableBitSet.of(0, 1), pred3))
    assertEquals(50D, mq.getDistinctRowCount(
      unSplittableGlobalAggWithLocalAggAndAuxGrouping, ImmutableBitSet.of(0, 1), null))
    assertEquals(5D, mq.getDistinctRowCount(
      unSplittableGlobalAggWithLocalAggAndAuxGrouping, ImmutableBitSet.of(0, 1, 2), pred2))
    assertEquals(25D, mq.getDistinctRowCount(
      unSplittableGlobalAggWithLocalAggAndAuxGrouping, ImmutableBitSet.of(0, 1, 2), pred3))
    assertEquals(50D, mq.getDistinctRowCount(
      unSplittableGlobalAggWithLocalAggAndAuxGrouping, ImmutableBitSet.of(0, 1, 2), null))
    assertEquals(18.75D, mq.getDistinctRowCount(
      unSplittableGlobalAggWithLocalAggAndAuxGrouping, ImmutableBitSet.of(1, 2), pred3))
    assertEquals(25D, mq.getDistinctRowCount(
      unSplittableGlobalAggWithLocalAggAndAuxGrouping, ImmutableBitSet.of(1, 2), null))

    // id < 5 and max(score) < 3
    val maxScoreSel = (3.0 - 0.0) / (5.1 - 0.0)
    val pred4 = relBuilder
      .push(unSplittableGlobalAggWithLocalAggAndAuxGrouping)
      .and(relBuilder.call(LESS_THAN, relBuilder.field(0), relBuilder.literal(5)),
        relBuilder.call(LESS_THAN, relBuilder.field(2), relBuilder.literal(3)))
    assertEquals(
      FlinkRelMdUtil.adaptNdvBasedOnSelectivity(50D, 18.75, maxScoreSel),
      mq.getDistinctRowCount(
        unSplittableGlobalAggWithLocalAggAndAuxGrouping, ImmutableBitSet.of(1), pred4))
    assertEquals(
      FlinkRelMdUtil.adaptNdvBasedOnSelectivity(50D, 18.75, maxScoreSel),
      mq.getDistinctRowCount(
        unSplittableGlobalAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(1), pred4))
  }

  @Test
  def testGetDistinctRowCountOnCalc(): Unit = {
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

    // calc => project without filter
    val program2 = RexProgram.create(scanOfT1.getRowType, projects, null, outputRowType, rexBuilder)
    val calc2 = new BatchExecCalc(cluster, batchExecTraits, scanOfT1, outputRowType, program2, "")
    assertEquals(15D, mq.getDistinctRowCount(calc2, ImmutableBitSet.of(1, 2), null))
  }

  @Test
  def testGetDistinctRowCountOnProject(): Unit = {
    relBuilder.scan("t1")
    // filter: $0 <= 2 and $0 > -1 and $1 < 1.1
    val expr1 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2))
    val expr2 = relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(-1))
    val expr3 = relBuilder.call(LESS_THAN, relBuilder.field(1), relBuilder.literal(1.1D))
    relBuilder.filter(List(expr1, expr2, expr3))
    val projects = List(
      relBuilder.call(EQUALS, relBuilder.field(0), relBuilder.literal(1)),
      relBuilder.field(0),
      relBuilder.field(1),
      relBuilder.literal(true),
      relBuilder.getRexBuilder.makeLiteral(
        2.1D, relBuilder.getTypeFactory.createSqlType(DOUBLE), true),
      relBuilder.getRexBuilder.makeLiteral(
        2L, relBuilder.getTypeFactory.createSqlType(BIGINT), true))
    // projects: $0==1, $0, $1, true, 2.1, 2
    relBuilder.project(projects)
    val prj1 = relBuilder.scan("t1").project(projects).build()
    assertEquals(15D, mq.getDistinctRowCount(prj1, ImmutableBitSet.of(1, 2), null))
  }

  @Test
  def testGetDistinctRowCountOnExpand(): Unit = {
    assertEquals(3D, mq.getDistinctRowCount(aggWithExpand, ImmutableBitSet.of(0), null))
    assertEquals(5D, mq.getDistinctRowCount(aggWithExpand, ImmutableBitSet.of(1), null))
    assertEquals(15D, mq.getDistinctRowCount(aggWithExpand, ImmutableBitSet.of(0, 1), null))
    assertEquals(4D, mq.getDistinctRowCount(aggWithExpand, ImmutableBitSet.of(0, 2), null))
    assertEquals(6D, mq.getDistinctRowCount(aggWithExpand, ImmutableBitSet.of(1, 2), null))
    assertEquals(8D, mq.getDistinctRowCount(aggWithExpand, ImmutableBitSet.of(0, 1, 2), null))

    // $3 > 23
    val pred = relBuilder
      .push(aggWithAuxGroupAndExpand)
      .call(GREATER_THAN, relBuilder.field(3), relBuilder.literal(23))
    assertEquals(50D, mq.getDistinctRowCount(aggWithAuxGroupAndExpand, ImmutableBitSet.of(0), null))
    assertEquals(2D, mq.getDistinctRowCount(aggWithAuxGroupAndExpand, ImmutableBitSet.of(1), null))
    assertEquals(7D, mq.getDistinctRowCount(aggWithAuxGroupAndExpand, ImmutableBitSet.of(2), null))
    assertEquals(25D, mq.getDistinctRowCount(aggWithAuxGroupAndExpand, ImmutableBitSet.of(3), null))
    assertEquals(Math.sqrt(200),
      mq.getDistinctRowCount(aggWithAuxGroupAndExpand, ImmutableBitSet.of(4), null), 1e-6)
    assertEquals(50D,
      mq.getDistinctRowCount(aggWithAuxGroupAndExpand, ImmutableBitSet.of(0, 2), null))
    assertEquals(25D,
      mq.getDistinctRowCount(aggWithAuxGroupAndExpand, ImmutableBitSet.of(0, 3), pred))
    assertEquals(25D,
      mq.getDistinctRowCount(aggWithAuxGroupAndExpand, ImmutableBitSet.of(0, 2, 3), pred))
    assertEquals(100D,
      mq.getDistinctRowCount(aggWithAuxGroupAndExpand, ImmutableBitSet.of(0, 3, 4), pred))
  }

  @Test
  def testGetDistinctRowCountOnFlinkLogicalWindowAggregate(): Unit = {
    assertEquals(30D, mq.getDistinctRowCount(flinkLogicalWindowAgg, ImmutableBitSet.of(0), null))
    assertEquals(5D, mq.getDistinctRowCount(flinkLogicalWindowAgg, ImmutableBitSet.of(1), null))
    assertEquals(50D, mq.getDistinctRowCount(flinkLogicalWindowAgg, ImmutableBitSet.of(0, 1), null))
    assertEquals(50D, mq.getDistinctRowCount(flinkLogicalWindowAgg, ImmutableBitSet.of(0, 2), null))
    assertEquals(null, mq.getDistinctRowCount(flinkLogicalWindowAgg, ImmutableBitSet.of(3), null))
    assertEquals(null,
      mq.getDistinctRowCount(flinkLogicalWindowAgg, ImmutableBitSet.of(0, 3), null))
    assertEquals(
      null, mq.getDistinctRowCount(flinkLogicalWindowAgg, ImmutableBitSet.of(1, 3), null))
    assertEquals(
      null, mq.getDistinctRowCount(flinkLogicalWindowAgg, ImmutableBitSet.of(2, 3), null))

    assertEquals(50D, mq.getDistinctRowCount(
      flinkLogicalWindowAggWithAuxGroup, ImmutableBitSet.of(0), null))
    assertEquals(48D, mq.getDistinctRowCount(
      flinkLogicalWindowAggWithAuxGroup, ImmutableBitSet.of(1), null))
    assertEquals(50D, mq.getDistinctRowCount(
      flinkLogicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1), null))
    assertEquals(50D, mq.getDistinctRowCount(
      flinkLogicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 2), null))
    assertEquals(50D, mq.getDistinctRowCount(
      flinkLogicalWindowAggWithAuxGroup, ImmutableBitSet.of(1, 2), null))
    assertEquals(null, mq.getDistinctRowCount(
      flinkLogicalWindowAggWithAuxGroup, ImmutableBitSet.of(3), null))
    // b > 10
    val pred = relBuilder
      .push(flinkLogicalWindowAggWithAuxGroup)
      .call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(10))
    assertEquals(
      FlinkRelMdUtil.adaptNdvBasedOnSelectivity(50.0D, 48.0D, 0.8D),
      mq.getDistinctRowCount(
        flinkLogicalWindowAggWithAuxGroup, ImmutableBitSet.of(1), pred), 1e-6)
    assertEquals(40D, mq.getDistinctRowCount(
      flinkLogicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1), pred))

    // b > 10 and count(c) > 1 and w$end = 100000
    val pred1 = relBuilder
      .push(flinkLogicalWindowAggWithAuxGroup)
      .and(
        relBuilder.call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(10)),
        relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(1)),
        relBuilder.call(EQUALS, relBuilder.field(4), relBuilder.literal(100000))
      )
    assertEquals(
      FlinkRelMdUtil.adaptNdvBasedOnSelectivity(50.0D, 48.0D, 0.12D),
      mq.getDistinctRowCount(
        flinkLogicalWindowAggWithAuxGroup, ImmutableBitSet.of(1), pred1), 1e-6)
    assertEquals(40D * 0.15D * 1.0D, mq.getDistinctRowCount(
      flinkLogicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1), pred1))
  }

  @Test
  def testGetDistinctRowCountOnLogicalWindowAggregate(): Unit = {
    assertEquals(30D, mq.getDistinctRowCount(logicalWindowAgg, ImmutableBitSet.of(0), null))
    assertEquals(5D, mq.getDistinctRowCount(logicalWindowAgg, ImmutableBitSet.of(1), null))
    assertEquals(50D, mq.getDistinctRowCount(logicalWindowAgg, ImmutableBitSet.of(0, 1), null))
    assertEquals(50D, mq.getDistinctRowCount(logicalWindowAgg, ImmutableBitSet.of(0, 2), null))
    assertEquals(null, mq.getDistinctRowCount(logicalWindowAgg, ImmutableBitSet.of(3), null))
    assertEquals(null, mq.getDistinctRowCount(logicalWindowAgg, ImmutableBitSet.of(0, 3), null))
    assertEquals(null, mq.getDistinctRowCount(logicalWindowAgg, ImmutableBitSet.of(1, 3), null))
    assertEquals(null, mq.getDistinctRowCount(logicalWindowAgg, ImmutableBitSet.of(2, 3), null))

    assertEquals(50D, mq.getDistinctRowCount(
      logicalWindowAggWithAuxGroup, ImmutableBitSet.of(0), null))
    assertEquals(48D, mq.getDistinctRowCount(
      logicalWindowAggWithAuxGroup, ImmutableBitSet.of(1), null))
    assertEquals(50D, mq.getDistinctRowCount(
      logicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1), null))
    assertEquals(50D, mq.getDistinctRowCount(
      logicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 2), null))
    assertEquals(50D, mq.getDistinctRowCount(
      logicalWindowAggWithAuxGroup, ImmutableBitSet.of(1, 2), null))
    assertEquals(null, mq.getDistinctRowCount(
      logicalWindowAggWithAuxGroup, ImmutableBitSet.of(3), null))
    // $1 > 10
    val pred = relBuilder
      .push(logicalWindowAggWithAuxGroup)
      .call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(10))
    assertEquals(
      FlinkRelMdUtil.adaptNdvBasedOnSelectivity(50.0D, 48.0D, 0.8D),
      mq.getDistinctRowCount(
        logicalWindowAggWithAuxGroup, ImmutableBitSet.of(1), pred), 1e-6)
    assertEquals(40D, mq.getDistinctRowCount(
      logicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1), pred))

    // b > 10 and count(c) > 1 and w$end = 100000
    val pred1 = relBuilder
      .push(logicalWindowAggWithAuxGroup)
      .and(
        relBuilder.call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(10)),
        relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(1)),
        relBuilder.call(EQUALS, relBuilder.field(4), relBuilder.literal(100000))
      )
    assertEquals(
      FlinkRelMdUtil.adaptNdvBasedOnSelectivity(50.0D, 48.0D, 0.12D),
      mq.getDistinctRowCount(
        logicalWindowAggWithAuxGroup, ImmutableBitSet.of(1), pred1), 1e-6)
    assertEquals(40D * 0.15D * 1.0D, mq.getDistinctRowCount(
      logicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1), pred1))
  }

  @Test
  def testGetDistinctRowCountOnWindowAggregateBatchExec(): Unit = {
    assertEquals(30D, mq.getDistinctRowCount(globalWindowAggWithLocalAgg,
      ImmutableBitSet.of(0), null))
    assertEquals(5D, mq.getDistinctRowCount(globalWindowAggWithLocalAgg,
      ImmutableBitSet.of(1), null))
    assertEquals(50D, mq.getDistinctRowCount(globalWindowAggWithLocalAgg,
      ImmutableBitSet.of(0, 1), null))
    assertEquals(50D, mq.getDistinctRowCount(globalWindowAggWithLocalAgg,
      ImmutableBitSet.of(0, 2), null))
    assertEquals(null, mq.getDistinctRowCount(globalWindowAggWithLocalAgg,
      ImmutableBitSet.of(3), null))
    assertEquals(null, mq.getDistinctRowCount(globalWindowAggWithLocalAgg,
      ImmutableBitSet.of(0, 3), null))
    assertEquals(null, mq.getDistinctRowCount(globalWindowAggWithLocalAgg,
      ImmutableBitSet.of(1, 3), null))
    assertEquals(null, mq.getDistinctRowCount(globalWindowAggWithLocalAgg,
      ImmutableBitSet.of(2, 3), null))

    assertEquals(30D, mq.getDistinctRowCount(globalWindowAggWithoutLocalAgg,
      ImmutableBitSet.of(0), null))
    assertEquals(5D, mq.getDistinctRowCount(globalWindowAggWithoutLocalAgg,
      ImmutableBitSet.of(1), null))
    assertEquals(50D, mq.getDistinctRowCount(globalWindowAggWithoutLocalAgg,
      ImmutableBitSet.of(0, 1), null))
    assertEquals(50D, mq.getDistinctRowCount(globalWindowAggWithoutLocalAgg,
      ImmutableBitSet.of(0, 2), null))
    assertEquals(null, mq.getDistinctRowCount(globalWindowAggWithoutLocalAgg,
      ImmutableBitSet.of(3), null))
    assertEquals(null, mq.getDistinctRowCount(globalWindowAggWithoutLocalAgg,
      ImmutableBitSet.of(0, 3), null))
    assertEquals(null, mq.getDistinctRowCount(globalWindowAggWithoutLocalAgg,
      ImmutableBitSet.of(1, 3), null))
    assertEquals(null, mq.getDistinctRowCount(globalWindowAggWithoutLocalAgg,
      ImmutableBitSet.of(2, 3), null))

    assertEquals(50D, mq.getDistinctRowCount(
      globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(0), null))
    assertEquals(48D, mq.getDistinctRowCount(
      globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(1), null))
    assertEquals(50D, mq.getDistinctRowCount(
      globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(0, 1), null))
    assertEquals(50D, mq.getDistinctRowCount(
      globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(0, 2), null))
    assertEquals(50D, mq.getDistinctRowCount(
      globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(1, 2), null))
    assertEquals(null, mq.getDistinctRowCount(
      globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(3), null))
    // $1 > 10
    val pred = relBuilder
      .push(globalWindowAggWithoutLocalAggWithAuxGrouping)
      .call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(10))
    assertEquals(
      FlinkRelMdUtil.adaptNdvBasedOnSelectivity(50.0D, 48.0D, 0.8D),
      mq.getDistinctRowCount(
        globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(1), pred), 1e-6)
    assertEquals(40D, mq.getDistinctRowCount(
      globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(0, 1), pred))

    assertEquals(50D, mq.getDistinctRowCount(
      globalWindowAggWithLocalAggWithAuxGrouping, ImmutableBitSet.of(0), null))
    assertEquals(48D, mq.getDistinctRowCount(
      globalWindowAggWithLocalAggWithAuxGrouping, ImmutableBitSet.of(1), null))
    assertEquals(50D, mq.getDistinctRowCount(
      globalWindowAggWithLocalAggWithAuxGrouping, ImmutableBitSet.of(0, 1), null))
    assertEquals(50D, mq.getDistinctRowCount(
      globalWindowAggWithLocalAggWithAuxGrouping, ImmutableBitSet.of(0, 2), null))
    assertEquals(50D, mq.getDistinctRowCount(
      globalWindowAggWithLocalAggWithAuxGrouping, ImmutableBitSet.of(1, 2), null))
    assertEquals(null, mq.getDistinctRowCount(
      globalWindowAggWithLocalAggWithAuxGrouping, ImmutableBitSet.of(3), null))
    // b > 10
    val pred1 = relBuilder
      .push(globalWindowAggWithLocalAggWithAuxGrouping)
      .call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(10))
    assertEquals(
      FlinkRelMdUtil.adaptNdvBasedOnSelectivity(50.0D, 48.0D, 0.8D),
      mq.getDistinctRowCount(
        globalWindowAggWithLocalAggWithAuxGrouping, ImmutableBitSet.of(1), pred1), 1e-6)
    assertEquals(40D, mq.getDistinctRowCount(
      globalWindowAggWithLocalAggWithAuxGrouping, ImmutableBitSet.of(0, 1), pred1))

    // b > 10 and count(c) > 1 and w$end = 100000
    val pred2 = relBuilder
      .push(globalWindowAggWithLocalAggWithAuxGrouping)
      .and(
        relBuilder.call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(10)),
        relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(1)),
        relBuilder.call(EQUALS, relBuilder.field(4), relBuilder.literal(100000))
      )

    assertEquals(
      FlinkRelMdUtil.adaptNdvBasedOnSelectivity(50.0D, 48.0D, 0.12D),
      mq.getDistinctRowCount(
        globalWindowAggWithLocalAggWithAuxGrouping, ImmutableBitSet.of(1), pred2), 1e-6)
    assertEquals(40D * 0.15D * 1.0D, mq.getDistinctRowCount(
      globalWindowAggWithLocalAggWithAuxGrouping, ImmutableBitSet.of(0, 1), pred2))
    assertEquals(
      FlinkRelMdUtil.adaptNdvBasedOnSelectivity(50.0D, 48.0D, 0.12D),
      mq.getDistinctRowCount(
        globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(1), pred2), 1e-6)
    assertEquals(40D * 0.15D * 1.0D, mq.getDistinctRowCount(
      globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(0, 1), pred2))
  }

  @Test
  def testGetDistinctRowCountOnFlinkLogicalRank(): Unit = {
    assertEquals(50D, mq.getDistinctRowCount(flinkLogicalRank, ImmutableBitSet.of(0), null))
    assertEquals(7D, mq.getDistinctRowCount(flinkLogicalRank, ImmutableBitSet.of(1), null))
    assertEquals(25D, mq.getDistinctRowCount(flinkLogicalRank, ImmutableBitSet.of(2), null))
    assertEquals(5D, mq.getDistinctRowCount(flinkLogicalRank, ImmutableBitSet.of(4), null))

    assertEquals(50D, mq.getDistinctRowCount(flinkLogicalRank, ImmutableBitSet.of(0, 4), null))
    assertEquals(35D, mq.getDistinctRowCount(flinkLogicalRank, ImmutableBitSet.of(1, 4), null))
    assertEquals(50D, mq.getDistinctRowCount(flinkLogicalRank, ImmutableBitSet.of(2, 4), null))

    relBuilder.clear()
    relBuilder.push(flinkLogicalRank)
    // age > 23
    val pred1 = relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(23))
    // rk < 3
    val pred2 = relBuilder.call(LESS_THAN, relBuilder.field(4), relBuilder.literal(3))
    // age > 23 and rk < 3
    val pred3 = relBuilder.and(
      relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(23)),
      relBuilder.call(LESS_THAN, relBuilder.field(4), relBuilder.literal(3)))

    val inputNdv = FlinkRelMdUtil.adaptNdvBasedOnSelectivity(50D, 25D, 0.5D)  // 18.75
    assertEquals(inputNdv, mq.getDistinctRowCount(flinkLogicalRank, ImmutableBitSet.of(2), pred1))
    assertEquals(inputNdv, mq.getDistinctRowCount(flinkLogicalRank, ImmutableBitSet.of(2), pred2))
    assertEquals(FlinkRelMdUtil.adaptNdvBasedOnSelectivity(50D, inputNdv, 0.5D),
      mq.getDistinctRowCount(flinkLogicalRank, ImmutableBitSet.of(2), pred3))

    assertEquals(50D, mq.getDistinctRowCount(flinkLogicalRank, ImmutableBitSet.of(2, 4), pred1))
    assertEquals(25D, mq.getDistinctRowCount(flinkLogicalRank, ImmutableBitSet.of(2, 4), pred2))
    assertEquals(FlinkRelMdUtil.adaptNdvBasedOnSelectivity(50D, inputNdv * 5, 0.5D),
      mq.getDistinctRowCount(flinkLogicalRank, ImmutableBitSet.of(2, 4), pred3))

    assertEquals(50D,
      mq.getDistinctRowCount(flinkLogicalRankWithVariableRankRange, ImmutableBitSet.of(0), null))
    assertEquals(7D,
      mq.getDistinctRowCount(flinkLogicalRankWithVariableRankRange, ImmutableBitSet.of(1), null))
    assertEquals(25D,
      mq.getDistinctRowCount(flinkLogicalRankWithVariableRankRange, ImmutableBitSet.of(2), null))
    assertEquals(50D,
      mq.getDistinctRowCount(flinkLogicalRankWithVariableRankRange, ImmutableBitSet.of(4), null))

    assertEquals(50D,
      mq.getDistinctRowCount(flinkLogicalRankWithVariableRankRange, ImmutableBitSet.of(0, 4), null))
    assertEquals(50D,
      mq.getDistinctRowCount(flinkLogicalRankWithVariableRankRange, ImmutableBitSet.of(1, 4), null))
    assertEquals(50D,
      mq.getDistinctRowCount(flinkLogicalRankWithVariableRankRange, ImmutableBitSet.of(2, 4), null))
  }

  @Test
  def testGetDistinctRowCountOnBatchExecRank(): Unit = {
    assertEquals(50D, mq.getDistinctRowCount(globalBatchExecRank, ImmutableBitSet.of(0), null))
    assertEquals(7D, mq.getDistinctRowCount(globalBatchExecRank, ImmutableBitSet.of(1), null))
    assertEquals(25D, mq.getDistinctRowCount(globalBatchExecRank, ImmutableBitSet.of(2), null))
    assertEquals(3D, mq.getDistinctRowCount(globalBatchExecRank, ImmutableBitSet.of(4), null))

    assertEquals(50D, mq.getDistinctRowCount(globalBatchExecRank, ImmutableBitSet.of(0, 4), null))
    assertEquals(21D, mq.getDistinctRowCount(globalBatchExecRank, ImmutableBitSet.of(1, 4), null))
    assertEquals(50D, mq.getDistinctRowCount(globalBatchExecRank, ImmutableBitSet.of(2, 4), null))

    assertEquals(50D, mq.getDistinctRowCount(localBatchExecRank, ImmutableBitSet.of(0), null))
    assertEquals(7D, mq.getDistinctRowCount(localBatchExecRank, ImmutableBitSet.of(1), null))
    assertEquals(25D, mq.getDistinctRowCount(localBatchExecRank, ImmutableBitSet.of(2), null))

    relBuilder.clear()
    relBuilder.push(globalBatchExecRank)

    // age > 23
    val pred1 = relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(23))
    // rk < 3 (selectivity is 0.0)
    val pred2 = relBuilder.call(LESS_THAN, relBuilder.field(4), relBuilder.literal(3))
    // age > 23 and rk < 3 (selectivity is 0.0)
    val pred3 = relBuilder.and(
      relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(23)),
      relBuilder.call(LESS_THAN, relBuilder.field(4), relBuilder.literal(3)))

    assertEquals(FlinkRelMdUtil.adaptNdvBasedOnSelectivity(50D, 25D, 0.5D), // 18.75
      mq.getDistinctRowCount(globalBatchExecRank, ImmutableBitSet.of(2), pred1))
    assertEquals(1D, mq.getDistinctRowCount(globalBatchExecRank, ImmutableBitSet.of(2), pred2))
    assertEquals(1D, mq.getDistinctRowCount(globalBatchExecRank, ImmutableBitSet.of(2), pred3))

    assertEquals(50D, mq.getDistinctRowCount(globalBatchExecRank, ImmutableBitSet.of(2, 4), pred1))
    assertEquals(1D, mq.getDistinctRowCount(globalBatchExecRank, ImmutableBitSet.of(2, 4), pred2))
    assertEquals(1D, mq.getDistinctRowCount(globalBatchExecRank, ImmutableBitSet.of(2, 4), pred3))
  }

  @Test
  def testGetDistinctRowCountOnStreamExecRank(): Unit = {
    assertEquals(3D, mq.getDistinctRowCount(streamExecRowNumber, ImmutableBitSet.of(0), null))
    assertEquals(3D, mq.getDistinctRowCount(streamExecRowNumber, ImmutableBitSet.of(1), null))
    assertEquals(3D, mq.getDistinctRowCount(streamExecRowNumber, ImmutableBitSet.of(2), null))
  }
}
