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

import org.apache.flink.table.planner.plan.nodes.calcite.LogicalExpand
import org.apache.flink.table.planner.plan.utils.ExpandUtil

import com.google.common.collect.{ImmutableList, ImmutableSet}
import org.apache.calcite.sql.fun.SqlStdOperatorTable.{EQUALS, LESS_THAN}
import org.apache.calcite.util.ImmutableBitSet
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConversions._

class FlinkRelMdUniqueKeysTest extends FlinkRelMdHandlerTestBase {

  @Test
  def testGetUniqueKeysOnTableScan(): Unit = {
    Array(studentLogicalScan, studentBatchScan, studentStreamScan).foreach { scan =>
      assertEquals(uniqueKeys(Array(0)), mq.getUniqueKeys(scan).toSet)
    }

    Array(empLogicalScan, empBatchScan, empStreamScan).foreach { scan =>
      assertNull(mq.getUniqueKeys(scan))
    }
  }

  @Test
  def testGetUniqueKeysOnValues(): Unit = {
    assertNull(mq.getUniqueKeys(logicalValues))
    assertNull(mq.getUniqueKeys(emptyValues))
  }

  @Test
  def testGetUniqueKeysOnProject(): Unit = {
    assertEquals(uniqueKeys(Array(0)), mq.getUniqueKeys(logicalProject).toSet)

    relBuilder.push(studentLogicalScan)
    // id=1, id, cast(id AS bigint not null), cast(id AS int), $1
    val exprs = List(
      relBuilder.call(EQUALS, relBuilder.field(0), relBuilder.literal(1)),
      relBuilder.field(0),
      rexBuilder.makeCast(longType, relBuilder.field(0)),
      rexBuilder.makeCast(intType, relBuilder.field(0)),
      relBuilder.field(1))
    val project1 = relBuilder.project(exprs).build()
    assertEquals(uniqueKeys(Array(1)), mq.getUniqueKeys(project1).toSet)
    assertEquals(uniqueKeys(Array(1), Array(2)), mq.getUniqueKeys(project1, true).toSet)
  }

  @Test
  def testGetUniqueKeysOnFilter(): Unit = {
    assertEquals(uniqueKeys(Array(0)), mq.getUniqueKeys(logicalFilter).toSet)
  }

  @Test
  def testGetUniqueKeysOnWatermark(): Unit = {
    assertEquals(uniqueKeys(Array(0)), mq.getUniqueKeys(logicalWatermarkAssigner).toSet)
  }

  @Test
  def testGetUniqueKeysOnCalc(): Unit = {
    relBuilder.push(studentLogicalScan)
    // id < 100
    val expr = relBuilder.call(LESS_THAN, relBuilder.field(0), relBuilder.literal(100))
    val calc1 = createLogicalCalc(
      studentLogicalScan, logicalProject.getRowType, logicalProject.getProjects, List(expr))
    assertEquals(uniqueKeys(Array(0)), mq.getUniqueKeys(logicalCalc).toSet)

    // id=1, id, cast(id AS bigint not null), cast(id AS int), $1
    val exprs = List(
      relBuilder.call(EQUALS, relBuilder.field(0), relBuilder.literal(1)),
      relBuilder.field(0),
      rexBuilder.makeCast(longType, relBuilder.field(0)),
      rexBuilder.makeCast(intType, relBuilder.field(0)),
      relBuilder.field(1))
    val rowType = relBuilder.project(exprs).build().getRowType
    val calc2 = createLogicalCalc(studentLogicalScan, rowType, exprs, List(expr))
    assertEquals(uniqueKeys(Array(1)), mq.getUniqueKeys(calc2).toSet)
    assertEquals(uniqueKeys(Array(1), Array(2)), mq.getUniqueKeys(calc2, true).toSet)
  }

  @Test
  def testGetUniqueKeysOnExpand(): Unit = {
    Array(logicalExpand, flinkLogicalExpand, batchExpand, streamExpand).foreach {
      expand => assertEquals(uniqueKeys(Array(0, 7)), mq.getUniqueKeys(expand).toSet)
    }

    val expandOutputType = ExpandUtil.buildExpandRowType(
      cluster.getTypeFactory, studentLogicalScan.getRowType, Array.empty[Integer])
    val expandProjects = ExpandUtil.createExpandProjects(
      studentLogicalScan.getCluster.getRexBuilder,
      studentLogicalScan.getRowType,
      expandOutputType,
      ImmutableBitSet.of(0, 1, 2, 3),
      ImmutableList.of(
        ImmutableBitSet.of(0),
        ImmutableBitSet.of(1),
        ImmutableBitSet.of(2),
        ImmutableBitSet.of(3)),
      Array.empty[Integer])
    val expand = new LogicalExpand(cluster, studentLogicalScan.getTraitSet,
      studentLogicalScan, expandOutputType, expandProjects, 7)
    assertNull(mq.getUniqueKeys(expand))
  }

  @Test
  def testGetUniqueKeysOnExchange(): Unit = {
    Array(batchExchange, streamExchange).foreach { exchange =>
      assertEquals(uniqueKeys(Array(0)), mq.getUniqueKeys(exchange).toSet)
    }
  }

  @Test
  def testGetUniqueKeysOnRank(): Unit = {
    Array(logicalRank, flinkLogicalRank, batchLocalRank, batchGlobalRank, streamRank).foreach {
      rank =>
        assertEquals(uniqueKeys(Array(0)), mq.getUniqueKeys(rank).toSet)
    }

    Array(logicalRowNumber, flinkLogicalRowNumber, streamRowNumber)
      .foreach { rank =>
        assertEquals(uniqueKeys(Array(0), Array(7)), mq.getUniqueKeys(rank).toSet)
      }
  }

  @Test
  def testGetUniqueKeysOnSort(): Unit = {
    Array(logicalSort, flinkLogicalSort, batchSort, streamSort,
      logicalSortLimit, flinkLogicalSortLimit, batchSortLimit, streamSortLimit,
      batchGlobalSortLimit, batchLocalSortLimit, logicalLimit, flinkLogicalLimit, batchLimit,
      streamLimit).foreach { sort =>
      assertEquals(uniqueKeys(Array(0)), mq.getUniqueKeys(sort).toSet)
    }
  }

  @Test
  def testGetUniqueKeysOnStreamExecDeduplicate(): Unit = {
    assertEquals(uniqueKeys(Array(1)), mq.getUniqueKeys(streamProcTimeDeduplicateFirstRow).toSet)
    assertEquals(uniqueKeys(Array(1, 2)), mq.getUniqueKeys(streamProcTimeDeduplicateLastRow).toSet)
    assertEquals(uniqueKeys(Array(1)), mq.getUniqueKeys(streamRowTimeDeduplicateFirstRow).toSet)
    assertEquals(uniqueKeys(Array(1, 2)), mq.getUniqueKeys(streamRowTimeDeduplicateLastRow).toSet)
  }

  @Test
  def testGetUniqueKeysOnStreamExecChangelogNormalize(): Unit = {
    assertEquals(uniqueKeys(Array(1, 0)), mq.getUniqueKeys(streamChangelogNormalize).toSet)
  }

  @Test
  def testGetUniqueKeysOnStreamExecDropUpdateBefore(): Unit = {
    assertEquals(uniqueKeys(Array(0)), mq.getUniqueKeys(streamDropUpdateBefore).toSet)
  }

  @Test
  def testGetUniqueKeysOnAggregate(): Unit = {
    Array(logicalAgg, flinkLogicalAgg, batchGlobalAggWithLocal, batchGlobalAggWithoutLocal,
      streamGlobalAggWithLocal, streamGlobalAggWithoutLocal).foreach { agg =>
      assertEquals(uniqueKeys(Array(0)), mq.getUniqueKeys(agg).toSet)
    }
    assertNull(mq.getUniqueKeys(batchLocalAgg))
    assertNull(mq.getUniqueKeys(streamLocalAgg))

    Array(logicalAggWithAuxGroup, flinkLogicalAggWithAuxGroup, batchGlobalAggWithLocalWithAuxGroup,
      batchGlobalAggWithoutLocalWithAuxGroup).foreach { agg =>
      assertEquals(uniqueKeys(Array(0)), mq.getUniqueKeys(agg).toSet)
    }
    assertNull(mq.getUniqueKeys(batchLocalAggWithAuxGroup))
  }

  @Test
  def testGetUniqueKeysOnWindowAgg(): Unit = {
    Array(logicalWindowAgg, flinkLogicalWindowAgg, batchGlobalWindowAggWithoutLocalAgg,
      batchGlobalWindowAggWithLocalAgg).foreach { agg =>
      assertEquals(ImmutableSet.of(ImmutableBitSet.of(0, 1, 3), ImmutableBitSet.of(0, 1, 4),
        ImmutableBitSet.of(0, 1, 5), ImmutableBitSet.of(0, 1, 6)),
        mq.getUniqueKeys(agg))
    }
    assertNull(mq.getUniqueKeys(batchLocalWindowAgg))

    Array(logicalWindowAggWithAuxGroup, flinkLogicalWindowAggWithAuxGroup,
      batchGlobalWindowAggWithoutLocalAggWithAuxGroup,
      batchGlobalWindowAggWithLocalAggWithAuxGroup).foreach { agg =>
      assertEquals(ImmutableSet.of(ImmutableBitSet.of(0, 3), ImmutableBitSet.of(0, 4),
        ImmutableBitSet.of(0, 5), ImmutableBitSet.of(0, 6)),
        mq.getUniqueKeys(agg))
    }
    assertNull(mq.getUniqueKeys(batchLocalWindowAggWithAuxGroup))
  }

  @Test
  def testGetUniqueKeysOnOverAgg(): Unit = {
    Array(flinkLogicalOverAgg, batchOverAgg).foreach { agg =>
      assertEquals(uniqueKeys(Array(0)), mq.getUniqueKeys(agg).toSet)
    }

    assertEquals(uniqueKeys(Array(0)), mq.getUniqueKeys(streamOverAgg).toSet)
  }

  @Test
  def testGetUniqueKeysOnJoin(): Unit = {
    assertEquals(uniqueKeys(Array(1), Array(5), Array(1, 5), Array(5, 6), Array(1, 5, 6)),
      mq.getUniqueKeys(logicalInnerJoinOnUniqueKeys).toSet)
    assertEquals(uniqueKeys(), mq.getUniqueKeys(logicalInnerJoinNotOnUniqueKeys).toSet)
    assertEquals(uniqueKeys(), mq.getUniqueKeys(logicalInnerJoinOnRHSUniqueKeys).toSet)
    assertEquals(uniqueKeys(), mq.getUniqueKeys(logicalInnerJoinWithoutEquiCond).toSet)
    assertEquals(uniqueKeys(), mq.getUniqueKeys(logicalInnerJoinWithEquiAndNonEquiCond).toSet)

    assertEquals(uniqueKeys(Array(1), Array(1, 5), Array(1, 5, 6)),
      mq.getUniqueKeys(logicalLeftJoinOnUniqueKeys).toSet)
    assertEquals(uniqueKeys(), mq.getUniqueKeys(logicalLeftJoinNotOnUniqueKeys).toSet)
    assertEquals(uniqueKeys(), mq.getUniqueKeys(logicalLeftJoinOnRHSUniqueKeys).toSet)
    assertEquals(uniqueKeys(), mq.getUniqueKeys(logicalLeftJoinWithoutEquiCond).toSet)
    assertEquals(uniqueKeys(), mq.getUniqueKeys(logicalLeftJoinWithEquiAndNonEquiCond).toSet)

    assertEquals(uniqueKeys(Array(5), Array(1, 5), Array(5, 6), Array(1, 5, 6)),
      mq.getUniqueKeys(logicalRightJoinOnUniqueKeys).toSet)
    assertEquals(uniqueKeys(), mq.getUniqueKeys(logicalRightJoinNotOnUniqueKeys).toSet)
    assertEquals(uniqueKeys(), mq.getUniqueKeys(logicalRightJoinOnLHSUniqueKeys).toSet)
    assertEquals(uniqueKeys(), mq.getUniqueKeys(logicalRightJoinWithoutEquiCond).toSet)
    assertEquals(uniqueKeys(), mq.getUniqueKeys(logicalRightJoinWithEquiAndNonEquiCond).toSet)

    assertEquals(uniqueKeys(Array(1, 5), Array(1, 5, 6)),
      mq.getUniqueKeys(logicalFullJoinOnUniqueKeys).toSet)
    assertEquals(uniqueKeys(), mq.getUniqueKeys(logicalFullJoinNotOnUniqueKeys).toSet)
    assertEquals(uniqueKeys(), mq.getUniqueKeys(logicalFullJoinOnRHSUniqueKeys).toSet)
    assertEquals(uniqueKeys(), mq.getUniqueKeys(logicalFullJoinWithoutEquiCond).toSet)
    assertEquals(uniqueKeys(), mq.getUniqueKeys(logicalFullJoinWithEquiAndNonEquiCond).toSet)

    assertEquals(uniqueKeys(Array(1)),
      mq.getUniqueKeys(logicalSemiJoinOnUniqueKeys).toSet)
    assertEquals(uniqueKeys(Array(1)), mq.getUniqueKeys(logicalSemiJoinNotOnUniqueKeys).toSet)
    assertNull(mq.getUniqueKeys(logicalSemiJoinOnRHSUniqueKeys))
    assertEquals(uniqueKeys(Array(1)), mq.getUniqueKeys(logicalSemiJoinWithoutEquiCond).toSet)
    assertEquals(uniqueKeys(Array(1)),
      mq.getUniqueKeys(logicalSemiJoinWithEquiAndNonEquiCond).toSet)

    assertEquals(uniqueKeys(Array(1)),
      mq.getUniqueKeys(logicalAntiJoinOnUniqueKeys).toSet)
    assertEquals(uniqueKeys(Array(1)), mq.getUniqueKeys(logicalAntiJoinNotOnUniqueKeys).toSet)
    assertNull(mq.getUniqueKeys(logicalAntiJoinOnRHSUniqueKeys))
    assertEquals(uniqueKeys(Array(1)), mq.getUniqueKeys(logicalAntiJoinWithoutEquiCond).toSet)
    assertEquals(uniqueKeys(Array(1)),
      mq.getUniqueKeys(logicalAntiJoinWithEquiAndNonEquiCond).toSet)
  }

  @Test
  def testGetUniqueKeysOnLookupJoin(): Unit = {
    Array(batchLookupJoin, streamLookupJoin).foreach { join =>
      assertEquals(uniqueKeys(), mq.getUniqueKeys(join).toSet)
    }
  }

  @Test
  def testGetUniqueKeysOnSetOp(): Unit = {
    Array(logicalUnionAll, logicalIntersectAll, logicalMinusAll).foreach { setOp =>
      assertEquals(uniqueKeys(), mq.getUniqueKeys(setOp).toSet)
    }

    Array(logicalUnion, logicalIntersect, logicalMinus).foreach { setOp =>
      assertEquals(uniqueKeys(Array(0, 1, 2, 3, 4)), mq.getUniqueKeys(setOp).toSet)
    }
  }

  @Test
  def testGetUniqueKeysOnMultipleInput(): Unit = {
    assertEquals(uniqueKeys(Array(0), Array(2), Array(0, 2)),
      mq.getUniqueKeys(batchMultipleInput).toSet)
  }

  @Test
  def testGetUniqueKeysOnDefault(): Unit = {
    assertNull(mq.getUniqueKeys(testRel))
  }

  private def uniqueKeys(keys: Array[Int]*): Set[ImmutableBitSet] = {
    keys.map(k => ImmutableBitSet.of(k: _*)).toSet
  }
}
