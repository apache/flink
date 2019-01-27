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

import org.apache.flink.table.plan.nodes.logical.FlinkLogicalExpand
import org.apache.flink.table.plan.rules.logical.DecomposeGroupingSetsRule._

import com.google.common.collect.ImmutableList
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.util.ImmutableBitSet
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConversions._

class FlinkRelMdColumnUniquenessTest extends FlinkRelMdHandlerTestBase {

  @Test
  def testAreColumnsUniqueOnTableScan(): Unit = {
    val ts = relBuilder.scan("student").build()
    assertTrue(mq.areColumnsUnique(ts, ImmutableBitSet.of(0, 1)))
    assertTrue(mq.areColumnsUnique(ts, ImmutableBitSet.of(0)))
    assertFalse(mq.areColumnsUnique(ts, ImmutableBitSet.of(1)))

    val ts1 = relBuilder.scan("t1").build()
    assertNull(mq.areColumnsUnique(ts1, ImmutableBitSet.of(0)))

    val ts2 = relBuilder.scan("t2").build()
    assertFalse(mq.areColumnsUnique(ts2, ImmutableBitSet.of(0)))
    assertFalse(mq.areColumnsUnique(ts2, ImmutableBitSet.of(1)))
    assertFalse(mq.areColumnsUnique(ts2, ImmutableBitSet.of(0, 1)))
  }

  @Test
  def testAreColumnsUniqueOnAgg(): Unit = {
    assertTrue(mq.areColumnsUnique(aggWithAuxGroup, ImmutableBitSet.of(0)))
    assertFalse(mq.areColumnsUnique(aggWithAuxGroup, ImmutableBitSet.of(1)))
    assertFalse(mq.areColumnsUnique(aggWithAuxGroup, ImmutableBitSet.of(2)))
    assertTrue(mq.areColumnsUnique(aggWithAuxGroup, ImmutableBitSet.of(0, 1)))
    assertTrue(mq.areColumnsUnique(aggWithAuxGroup, ImmutableBitSet.of(0, 2)))
    assertFalse(mq.areColumnsUnique(aggWithAuxGroup, ImmutableBitSet.of(1, 2)))
    assertTrue(mq.areColumnsUnique(aggWithAuxGroup, ImmutableBitSet.of(0, 1, 2)))

    assertFalse(mq.areColumnsUnique(aggWithAuxGroupAndExpand, ImmutableBitSet.of(0)))
    assertFalse(mq.areColumnsUnique(aggWithAuxGroupAndExpand, ImmutableBitSet.of(1)))
    assertFalse(mq.areColumnsUnique(aggWithAuxGroupAndExpand, ImmutableBitSet.of(2)))
    assertFalse(mq.areColumnsUnique(aggWithAuxGroupAndExpand, ImmutableBitSet.of(3)))
    assertFalse(mq.areColumnsUnique(aggWithAuxGroupAndExpand, ImmutableBitSet.of(4)))
    assertTrue(mq.areColumnsUnique(aggWithAuxGroupAndExpand, ImmutableBitSet.of(0, 1)))
    assertTrue(mq.areColumnsUnique(aggWithAuxGroupAndExpand, ImmutableBitSet.of(0, 1, 2)))
    assertTrue(mq.areColumnsUnique(aggWithAuxGroupAndExpand, ImmutableBitSet.of(0, 1, 3)))
    assertTrue(mq.areColumnsUnique(aggWithAuxGroupAndExpand, ImmutableBitSet.of(0, 1, 2, 3)))
    assertTrue(mq.areColumnsUnique(aggWithAuxGroupAndExpand, ImmutableBitSet.of(0, 1, 4)))
    assertTrue(mq.areColumnsUnique(aggWithAuxGroupAndExpand, ImmutableBitSet.of(0, 1, 2, 3, 4)))
  }

  @Test
  def testAreColumnsUniqueOnExpand(): Unit = {
    // column 0 is unique key
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
    assertFalse(mq.areColumnsUnique(expand1, ImmutableBitSet.of(0, 1)))
    assertFalse(mq.areColumnsUnique(expand1, ImmutableBitSet.of(0, 1, 4)))
    assertFalse(mq.areColumnsUnique(expand1, ImmutableBitSet.of(0)))
    assertFalse(mq.areColumnsUnique(expand1, ImmutableBitSet.of(0, 4)))
    assertFalse(mq.areColumnsUnique(expand1, ImmutableBitSet.of(1)))
    assertFalse(mq.areColumnsUnique(expand1, ImmutableBitSet.of(1, 4)))
    assertFalse(mq.areColumnsUnique(expand1, ImmutableBitSet.of(4)))

    val expandProjects2 = createExpandProjects(
      ts.getCluster.getRexBuilder,
      ts.getRowType,
      expandOutputType,
      ImmutableBitSet.of(0, 1, 2, 3),
      ImmutableList.of(
        ImmutableBitSet.of(0, 1),
        ImmutableBitSet.of(0, 2),
        ImmutableBitSet.of(0, 3)
      ), Array.empty[Integer])
    val expand2 = new FlinkLogicalExpand(
      ts.getCluster, ts.getTraitSet, ts, expandOutputType, expandProjects2, 4)
    assertFalse(mq.areColumnsUnique(expand2, ImmutableBitSet.of(0, 1)))
    assertTrue(mq.areColumnsUnique(expand2, ImmutableBitSet.of(0, 1, 4)))
    assertFalse(mq.areColumnsUnique(expand2, ImmutableBitSet.of(0)))
    assertTrue(mq.areColumnsUnique(expand2, ImmutableBitSet.of(0, 4)))
    assertFalse(mq.areColumnsUnique(expand2, ImmutableBitSet.of(1)))
    assertFalse(mq.areColumnsUnique(expand2, ImmutableBitSet.of(1, 4)))
    assertFalse(mq.areColumnsUnique(expand2, ImmutableBitSet.of(4)))

    val expandProjects3 = createExpandProjects(
      ts.getCluster.getRexBuilder,
      ts.getRowType,
      expandOutputType,
      ImmutableBitSet.of(0, 1, 2, 3),
      ImmutableList.of(
        ImmutableBitSet.of(0, 1),
        ImmutableBitSet.of(1, 2),
        ImmutableBitSet.of(1, 3)
      ), Array.empty[Integer])
    val expand3 = new FlinkLogicalExpand(
      ts.getCluster, ts.getTraitSet, ts, expandOutputType, expandProjects3, 4)
    assertFalse(mq.areColumnsUnique(expand3, ImmutableBitSet.of(0, 1)))
    assertFalse(mq.areColumnsUnique(expand3, ImmutableBitSet.of(0, 1, 4)))
    assertFalse(mq.areColumnsUnique(expand3, ImmutableBitSet.of(0)))
    assertFalse(mq.areColumnsUnique(expand3, ImmutableBitSet.of(0, 4)))
    assertFalse(mq.areColumnsUnique(expand3, ImmutableBitSet.of(1)))
    assertFalse(mq.areColumnsUnique(expand3, ImmutableBitSet.of(1, 4)))
    assertFalse(mq.areColumnsUnique(expand3, ImmutableBitSet.of(4)))
  }

  @Test
  def testAreColumnsUniqueOnProject(): Unit = {
    // PRJ ($0=1, $0, cast($0 AS Int), $1, 2.1, 2)
    val ts = relBuilder.scan("student").build()
    relBuilder.push(ts)
    val exprs = List(
      relBuilder.call(EQUALS, relBuilder.field(0), relBuilder.literal(1)),
      relBuilder.field(0),
      relBuilder.cast(relBuilder.field(0), INTEGER),
      relBuilder.field(1),
      relBuilder.getRexBuilder.makeLiteral(
        2.1D, relBuilder.getTypeFactory.createSqlType(DOUBLE), true),
      relBuilder.getRexBuilder.makeLiteral(
        2L, relBuilder.getTypeFactory.createSqlType(BIGINT), true))
    val prj = relBuilder.project(exprs).build()
    assertTrue(mq.areColumnsUnique(prj, ImmutableBitSet.of(1)))
    assertNull(mq.areColumnsUnique(prj, ImmutableBitSet.of(2)))
    assertTrue(mq.areColumnsUnique(prj, ImmutableBitSet.of(2), true))
    assertNull(mq.areColumnsUnique(prj, ImmutableBitSet.of(0, 2)))
    assertTrue(mq.areColumnsUnique(prj, ImmutableBitSet.of(0, 2), true))
    assertNull(mq.areColumnsUnique(prj, ImmutableBitSet.of(0)))
    assertFalse(mq.areColumnsUnique(prj, ImmutableBitSet.of(3)))
  }

  @Test
  def testAreColumnsUniqueOnValues(): Unit = {
    assertTrue(mq.areColumnsUnique(emptyValues, ImmutableBitSet.of(1)))
    assertTrue(mq.areColumnsUnique(values, ImmutableBitSet.of(0)))
    assertFalse(mq.areColumnsUnique(values, ImmutableBitSet.of(1)))
    assertTrue(mq.areColumnsUnique(values, ImmutableBitSet.of(0, 1)))
    assertTrue(mq.areColumnsUnique(values, ImmutableBitSet.of(2)))
    assertTrue(mq.areColumnsUnique(values, ImmutableBitSet.of(3)))
    assertTrue(mq.areColumnsUnique(values, ImmutableBitSet.of(4)))
    assertTrue(mq.areColumnsUnique(values, ImmutableBitSet.of(5)))
    assertFalse(mq.areColumnsUnique(values, ImmutableBitSet.of(6)))
  }

  @Test
  def testAreColumnsUniqueOnJoin(): Unit = {
    assertTrue(mq.areColumnsUnique(innerJoin, ImmutableBitSet.of(0)))
    assertTrue(mq.areColumnsUnique(innerJoin, ImmutableBitSet.of(0, 1)))
    assertFalse(mq.areColumnsUnique(innerJoin, ImmutableBitSet.of(1)))
    assertTrue(mq.areColumnsUnique(innerJoin, ImmutableBitSet.of(2)))
    assertFalse(mq.areColumnsUnique(fullJoin, ImmutableBitSet.of(0)))
    assertFalse(mq.areColumnsUnique(fullJoin, ImmutableBitSet.of(2)))
    assertTrue(mq.areColumnsUnique(fullJoin, ImmutableBitSet.of(0, 2)))
    assertTrue(mq.areColumnsUnique(leftJoin, ImmutableBitSet.of(0)))
    assertFalse(mq.areColumnsUnique(leftJoin, ImmutableBitSet.of(2)))
    assertTrue(mq.areColumnsUnique(leftJoin, ImmutableBitSet.of(0, 2)))
  }

  @Test
  def testAreColumnsUniqueOnIntersect(): Unit = {
    assertTrue(mq.areColumnsUnique(intersect, ImmutableBitSet.of(0)))
    assertTrue(mq.areColumnsUnique(intersect, ImmutableBitSet.of(0, 1)))
    assertFalse(mq.areColumnsUnique(intersect, ImmutableBitSet.of(1)))
    assertTrue(mq.areColumnsUnique(intersectAll, ImmutableBitSet.of(0)))
    assertTrue(mq.areColumnsUnique(intersectAll, ImmutableBitSet.of(0, 1)))
    assertFalse(mq.areColumnsUnique(intersectAll, ImmutableBitSet.of(1)))
  }

  @Test
  def testAreColumnsUniqueOnWindow(): Unit = {
    assertTrue(mq.areColumnsUnique(logicalOverWindow, ImmutableBitSet.of(0, 1)))
    assertTrue(mq.areColumnsUnique(logicalOverWindow, ImmutableBitSet.of(0)))
    assertFalse(mq.areColumnsUnique(logicalOverWindow, ImmutableBitSet.of(1)))
    assertNull(mq.areColumnsUnique(logicalOverWindow, ImmutableBitSet.of(4)))
    assertTrue(mq.areColumnsUnique(logicalOverWindow, ImmutableBitSet.of(0, 4)))
    assertNull(mq.areColumnsUnique(logicalOverWindow, ImmutableBitSet.of(1, 4)))
    assertTrue(mq.areColumnsUnique(overWindowAgg, ImmutableBitSet.of(0, 1)))
    assertTrue(mq.areColumnsUnique(overWindowAgg, ImmutableBitSet.of(0)))
    assertFalse(mq.areColumnsUnique(overWindowAgg, ImmutableBitSet.of(1)))
    assertNull(mq.areColumnsUnique(overWindowAgg, ImmutableBitSet.of(4)))
    assertTrue(mq.areColumnsUnique(overWindowAgg, ImmutableBitSet.of(0, 4)))
    assertNull(mq.areColumnsUnique(overWindowAgg, ImmutableBitSet.of(1, 4)))
  }

  @Test
  def testAreColumnsUniqueOnWindowAggregateBatchExec(): Unit = {
    assertFalse(mq.areColumnsUnique(globalWindowAggWithLocalAgg, ImmutableBitSet.of(0, 1)))
    assertFalse(mq.areColumnsUnique(globalWindowAggWithoutLocalAgg, ImmutableBitSet.of(0, 1)))

    assertTrue(mq.areColumnsUnique(
      globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(0, 3)))
    assertTrue(mq.areColumnsUnique(
      globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(0, 4)))
    assertTrue(mq.areColumnsUnique(
      globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(0, 1, 3)))
    assertTrue(mq.areColumnsUnique(
      globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(0, 1, 4)))
    assertFalse(mq.areColumnsUnique(
      globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(1, 3)))
  }

  @Test
  def testAreColumnsUniqueOnLogicalWindowAggregate(): Unit = {
    assertFalse(mq.areColumnsUnique(logicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1)))
    assertFalse(mq.areColumnsUnique(logicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1)))

    assertTrue(mq.areColumnsUnique(logicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 3)))
    assertTrue(mq.areColumnsUnique(logicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 4)))
    assertTrue(mq.areColumnsUnique(logicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1, 3)))
    assertTrue(mq.areColumnsUnique(logicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1, 4)))
    assertFalse(mq.areColumnsUnique(logicalWindowAggWithAuxGroup, ImmutableBitSet.of(1, 3)))
  }

  @Test
  def testAreColumnsUniqueOnFlinkLogicalWindowAggregate(): Unit = {
    assertFalse(mq.areColumnsUnique(flinkLogicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1)))
    assertFalse(mq.areColumnsUnique(flinkLogicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1)))

    assertTrue(mq.areColumnsUnique(flinkLogicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 3)))
    assertTrue(mq.areColumnsUnique(flinkLogicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 4)))
    assertTrue(mq.areColumnsUnique(flinkLogicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1, 3)))
    assertTrue(mq.areColumnsUnique(flinkLogicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1, 4)))
    assertFalse(mq.areColumnsUnique(flinkLogicalWindowAggWithAuxGroup, ImmutableBitSet.of(1, 3)))
  }

  @Test
  def testAreColumnsUniqueOnFlinkLogicalRank(): Unit = {
    assertTrue(mq.areColumnsUnique(flinkLogicalRank, ImmutableBitSet.of(0)))
    assertFalse(mq.areColumnsUnique(flinkLogicalRank, ImmutableBitSet.of(1)))
    assertTrue(mq.areColumnsUnique(flinkLogicalRank, ImmutableBitSet.of(0, 1)))
    assertFalse(mq.areColumnsUnique(flinkLogicalRank, ImmutableBitSet.of(3)))
    assertFalse(mq.areColumnsUnique(flinkLogicalRank, ImmutableBitSet.of(4)))
    assertTrue(mq.areColumnsUnique(flinkLogicalRank, ImmutableBitSet.of(0, 4)))
    assertFalse(mq.areColumnsUnique(flinkLogicalRank, ImmutableBitSet.of(2, 4)))
    assertFalse(mq.areColumnsUnique(flinkLogicalRank, ImmutableBitSet.of(3, 4)))

    assertTrue(mq.areColumnsUnique(flinkLogicalRowNumberWithOutput, ImmutableBitSet.of(0)))
    assertFalse(mq.areColumnsUnique(flinkLogicalRowNumberWithOutput, ImmutableBitSet.of(2)))
    assertFalse(mq.areColumnsUnique(flinkLogicalRowNumberWithOutput, ImmutableBitSet.of(3)))
    assertFalse(mq.areColumnsUnique(flinkLogicalRowNumberWithOutput, ImmutableBitSet.of(4)))
    assertTrue(mq.areColumnsUnique(flinkLogicalRowNumberWithOutput, ImmutableBitSet.of(0, 4)))
    assertTrue(mq.areColumnsUnique(flinkLogicalRowNumberWithOutput, ImmutableBitSet.of(2, 4)))
    assertFalse(mq.areColumnsUnique(flinkLogicalRowNumberWithOutput, ImmutableBitSet.of(3, 4)))
  }

  @Test
  def testAreColumnsUniqueOnBatchExecRank(): Unit = {
    assertTrue(mq.areColumnsUnique(globalBatchExecRank, ImmutableBitSet.of(0)))
    assertFalse(mq.areColumnsUnique(globalBatchExecRank, ImmutableBitSet.of(2)))
    assertFalse(mq.areColumnsUnique(globalBatchExecRank, ImmutableBitSet.of(4)))
    assertTrue(mq.areColumnsUnique(globalBatchExecRank, ImmutableBitSet.of(0, 4)))
    assertFalse(mq.areColumnsUnique(globalBatchExecRank, ImmutableBitSet.of(2, 4)))

    assertTrue(mq.areColumnsUnique(localBatchExecRank, ImmutableBitSet.of(0)))
    assertFalse(mq.areColumnsUnique(localBatchExecRank, ImmutableBitSet.of(2)))
    assertTrue(mq.areColumnsUnique(localBatchExecRank, ImmutableBitSet.of(0, 2)))
  }

  @Test
  def testAreColumnsUniqueOnStreamExecRank(): Unit = {
    assertTrue(mq.areColumnsUnique(streamExecRowNumber, ImmutableBitSet.of(0)))
    assertFalse(mq.areColumnsUnique(streamExecRowNumber, ImmutableBitSet.of(2)))
    assertTrue(mq.areColumnsUnique(streamExecRowNumber, ImmutableBitSet.of(0, 2)))
  }
}
