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
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan
import org.apache.flink.table.planner.plan.schema.TableSourceTable
import org.apache.flink.table.planner.plan.utils.ExpandUtil

import com.google.common.collect.{ImmutableList, ImmutableSet}
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.sql.fun.SqlStdOperatorTable.{EQUALS, LESS_THAN}
import org.apache.calcite.util.ImmutableBitSet
import org.junit.Assert._
import org.junit.Test

import java.util.Collections

import scala.collection.JavaConversions._

class FlinkRelMdChangeLogUpsertKeysTest extends FlinkRelMdHandlerTestBase {

  @Test
  def testGetChangeLogUpsertKeysOnTableScan(): Unit = {
    Array(studentLogicalScan, studentBatchScan, studentStreamScan).foreach { scan =>
      assertEquals(toBitSet(Array(0)), mq.getChangeLogUpsertKeys(scan).toSet)
    }

    Array(empLogicalScan, empBatchScan, empStreamScan).foreach { scan =>
      assertNull(mq.getChangeLogUpsertKeys(scan))
    }

    val table = relBuilder
      .getRelOptSchema
      .asInstanceOf[CalciteCatalogReader]
      .getTable(Seq("projected_table_source_table"))
      .asInstanceOf[TableSourceTable]
    val tableSourceScan = new StreamPhysicalTableSourceScan(
      cluster,
      streamPhysicalTraits,
      Collections.emptyList[RelHint](),
      table)
    assertEquals(toBitSet(Array(0, 2)), mq.getChangeLogUpsertKeys(tableSourceScan).toSet)
  }

  @Test
  def testGetChangeLogUpsertKeysOnProjectedTableScanWithPartialCompositePrimaryKey(): Unit = {
    val table = relBuilder
      .getRelOptSchema
      .asInstanceOf[CalciteCatalogReader]
      .getTable(Seq("projected_table_source_table_with_partial_pk"))
      .asInstanceOf[TableSourceTable]
    val tableSourceScan = new StreamPhysicalTableSourceScan(
      cluster,
      streamPhysicalTraits,
      Collections.emptyList[RelHint](),
      table)
    assertNull(mq.getChangeLogUpsertKeys(tableSourceScan))
  }

  @Test
  def testGetChangeLogUpsertKeysOnValues(): Unit = {
    assertNull(mq.getChangeLogUpsertKeys(logicalValues))
    assertNull(mq.getChangeLogUpsertKeys(emptyValues))
  }

  @Test
  def testGetChangeLogUpsertKeysOnProject(): Unit = {
    assertEquals(toBitSet(Array(0)), mq.getChangeLogUpsertKeys(logicalProject).toSet)

    relBuilder.push(studentLogicalScan)
    // id=1, id, cast(id AS bigint not null), cast(id AS int), $1
    val exprs = List(
      relBuilder.call(EQUALS, relBuilder.field(0), relBuilder.literal(1)),
      relBuilder.field(0),
      rexBuilder.makeCast(longType, relBuilder.field(0)),
      rexBuilder.makeCast(intType, relBuilder.field(0)),
      relBuilder.field(1))
    val project1 = relBuilder.project(exprs).build()
    assertEquals(toBitSet(Array(1)), mq.getChangeLogUpsertKeys(project1).toSet)
  }

  @Test
  def testGetChangeLogUpsertKeysOnFilter(): Unit = {
    assertEquals(toBitSet(Array(0)), mq.getChangeLogUpsertKeys(logicalFilter).toSet)
  }

  @Test
  def testGetChangeLogUpsertKeysOnWatermark(): Unit = {
    assertEquals(toBitSet(Array(0)), mq.getChangeLogUpsertKeys(logicalWatermarkAssigner).toSet)
  }

  @Test
  def testGetChangeLogUpsertKeysOnCalc(): Unit = {
    relBuilder.push(studentLogicalScan)
    // id < 100
    val expr = relBuilder.call(LESS_THAN, relBuilder.field(0), relBuilder.literal(100))
    val calc1 = createLogicalCalc(
      studentLogicalScan, logicalProject.getRowType, logicalProject.getProjects, List(expr))
    assertEquals(toBitSet(Array(0)), mq.getChangeLogUpsertKeys(logicalCalc).toSet)

    // id=1, id, cast(id AS bigint not null), cast(id AS int), $1
    val exprs = List(
      relBuilder.call(EQUALS, relBuilder.field(0), relBuilder.literal(1)),
      relBuilder.field(0),
      rexBuilder.makeCast(longType, relBuilder.field(0)),
      rexBuilder.makeCast(intType, relBuilder.field(0)),
      relBuilder.field(1))
    val rowType = relBuilder.project(exprs).build().getRowType
    val calc2 = createLogicalCalc(studentLogicalScan, rowType, exprs, List(expr))
    assertEquals(toBitSet(Array(1)), mq.getChangeLogUpsertKeys(calc2).toSet)
  }

  @Test
  def testGetChangeLogUpsertKeysOnExpand(): Unit = {
    Array(logicalExpand, flinkLogicalExpand, batchExpand, streamExpand).foreach {
      expand => assertEquals(toBitSet(Array(0, 7)), mq.getChangeLogUpsertKeys(expand).toSet)
    }

    val expandProjects = ExpandUtil.createExpandProjects(
      studentLogicalScan.getCluster.getRexBuilder,
      studentLogicalScan.getRowType,
      ImmutableBitSet.of(0, 1, 2, 3),
      ImmutableList.of(
        ImmutableBitSet.of(0),
        ImmutableBitSet.of(1),
        ImmutableBitSet.of(2),
        ImmutableBitSet.of(3)),
      Array.empty[Integer])
    val expand = new LogicalExpand(cluster, studentLogicalScan.getTraitSet,
      studentLogicalScan, expandProjects, 7)
    assertNull(mq.getChangeLogUpsertKeys(expand))
  }

  @Test
  def testGetChangeLogUpsertKeysOnExchange(): Unit = {
    Array(batchExchange, streamExchange).foreach { exchange =>
      assertEquals(toBitSet(), mq.getChangeLogUpsertKeys(exchange).toSet)
    }

    Array(batchExchangeById, streamExchangeById).foreach { exchange =>
      assertEquals(toBitSet(Array(0)), mq.getChangeLogUpsertKeys(exchange).toSet)
    }
  }

  @Test
  def testGetChangeLogUpsertKeysOnRank(): Unit = {
    Array(logicalRank, flinkLogicalRank, batchLocalRank, batchGlobalRank, streamRank).foreach {
      rank =>
        assertEquals(toBitSet(), mq.getChangeLogUpsertKeys(rank).toSet)
    }

    Array(logicalRankById, flinkLogicalRankById,
      batchLocalRankById, batchGlobalRankById, streamRankById).foreach {
      rank =>
        assertEquals(toBitSet(Array(0)), mq.getChangeLogUpsertKeys(rank).toSet)
    }

    Array(logicalRowNumber, flinkLogicalRowNumber, streamRowNumber)
      .foreach { rank =>
        assertEquals(toBitSet(Array(0), Array(7)), mq.getChangeLogUpsertKeys(rank).toSet)
      }
  }

  @Test
  def testGetChangeLogUpsertKeysOnSort(): Unit = {
    def testWithoutKey(rel: RelNode): Unit = {
      assertEquals(toBitSet(), mq.getChangeLogUpsertKeys(rel).toSet)
    }

    def testWithKey(rel: RelNode): Unit = {
      assertEquals(toBitSet(Array(0)), mq.getChangeLogUpsertKeys(rel).toSet)
    }

    testWithoutKey(logicalSort)
    testWithoutKey(flinkLogicalSort)
    testWithoutKey(batchSort)
    testWithoutKey(streamSort)
    testWithoutKey(logicalSortLimit)
    testWithoutKey(flinkLogicalSortLimit)
    testWithoutKey(batchSortLimit)
    testWithoutKey(streamSortLimit)
    testWithoutKey(batchGlobalSortLimit)
    testWithoutKey(batchLocalSortLimit)

    testWithKey(logicalSortById)
    testWithKey(flinkLogicalSortById)
    testWithKey(batchSortById)
    testWithKey(streamSortById)
    testWithKey(logicalSortLimitById)
    testWithKey(flinkLogicalSortLimitById)
    testWithKey(batchSortLimitById)
    testWithKey(streamSortLimitById)
    testWithKey(batchGlobalSortLimitById)
    testWithKey(batchLocalSortLimitById)

    testWithKey(logicalLimit)
    testWithKey(flinkLogicalLimit)
    testWithKey(batchLimit)
    testWithKey(streamLimit)
  }

  @Test
  def testGetChangeLogUpsertKeysOnStreamExecDeduplicate(): Unit = {
    assertEquals(
      toBitSet(Array(1)),
      mq.getChangeLogUpsertKeys(streamProcTimeDeduplicateFirstRow).toSet)
    assertEquals(
      toBitSet(Array(1, 2)),
      mq.getChangeLogUpsertKeys(streamProcTimeDeduplicateLastRow).toSet)
    assertEquals(
      toBitSet(Array(1)),
      mq.getChangeLogUpsertKeys(streamRowTimeDeduplicateFirstRow).toSet)
    assertEquals(
      toBitSet(Array(1, 2)),
      mq.getChangeLogUpsertKeys(streamRowTimeDeduplicateLastRow).toSet)
  }

  @Test
  def testGetChangeLogUpsertKeysOnStreamExecChangelogNormalize(): Unit = {
    assertEquals(toBitSet(Array(1, 0)), mq.getChangeLogUpsertKeys(streamChangelogNormalize).toSet)
  }

  @Test
  def testGetChangeLogUpsertKeysOnStreamExecDropUpdateBefore(): Unit = {
    assertEquals(toBitSet(Array(0)), mq.getChangeLogUpsertKeys(streamDropUpdateBefore).toSet)
  }

  @Test
  def testGetChangeLogUpsertKeysOnAggregate(): Unit = {
    Array(logicalAgg, flinkLogicalAgg, batchGlobalAggWithLocal, batchGlobalAggWithoutLocal,
      streamGlobalAggWithLocal, streamGlobalAggWithoutLocal).foreach { agg =>
      assertEquals(toBitSet(Array(0)), mq.getChangeLogUpsertKeys(agg).toSet)
    }
    assertNull(mq.getChangeLogUpsertKeys(batchLocalAgg))
    assertNull(mq.getChangeLogUpsertKeys(streamLocalAgg))

    Array(logicalAggWithAuxGroup, flinkLogicalAggWithAuxGroup, batchGlobalAggWithLocalWithAuxGroup,
      batchGlobalAggWithoutLocalWithAuxGroup).foreach { agg =>
      assertEquals(toBitSet(Array(0)), mq.getChangeLogUpsertKeys(agg).toSet)
    }
    assertNull(mq.getChangeLogUpsertKeys(batchLocalAggWithAuxGroup))
  }

  @Test
  def testGetChangeLogUpsertKeysOnWindowAgg(): Unit = {
    Array(logicalWindowAgg, flinkLogicalWindowAgg, batchGlobalWindowAggWithoutLocalAgg,
      batchGlobalWindowAggWithLocalAgg).foreach { agg =>
      assertEquals(ImmutableSet.of(ImmutableBitSet.of(0, 1, 3), ImmutableBitSet.of(0, 1, 4),
        ImmutableBitSet.of(0, 1, 5), ImmutableBitSet.of(0, 1, 6)),
        mq.getChangeLogUpsertKeys(agg))
    }
    assertNull(mq.getChangeLogUpsertKeys(batchLocalWindowAgg))

    Array(logicalWindowAggWithAuxGroup, flinkLogicalWindowAggWithAuxGroup,
      batchGlobalWindowAggWithoutLocalAggWithAuxGroup,
      batchGlobalWindowAggWithLocalAggWithAuxGroup).foreach { agg =>
      assertEquals(ImmutableSet.of(ImmutableBitSet.of(0, 3), ImmutableBitSet.of(0, 4),
        ImmutableBitSet.of(0, 5), ImmutableBitSet.of(0, 6)),
        mq.getChangeLogUpsertKeys(agg))
    }
    assertNull(mq.getChangeLogUpsertKeys(batchLocalWindowAggWithAuxGroup))
  }

  @Test
  def testGetChangeLogUpsertKeysOnOverAgg(): Unit = {
    Array(flinkLogicalOverAgg, batchOverAgg, streamOverAgg).foreach { agg =>
      assertEquals(toBitSet(), mq.getChangeLogUpsertKeys(agg).toSet)
    }

    assertEquals(toBitSet(Array(0)), mq.getChangeLogUpsertKeys(streamOverAggById).toSet)
  }

  @Test
  def testGetChangeLogUpsertKeysOnJoin(): Unit = {
    assertEquals(toBitSet(Array(1), Array(5), Array(1, 5), Array(5, 6), Array(1, 5, 6)),
      mq.getChangeLogUpsertKeys(logicalInnerJoinOnUniqueKeys).toSet)
    assertEquals(toBitSet(), mq.getChangeLogUpsertKeys(logicalInnerJoinNotOnUniqueKeys).toSet)
    assertEquals(toBitSet(), mq.getChangeLogUpsertKeys(logicalInnerJoinOnRHSUniqueKeys).toSet)
    assertEquals(toBitSet(), mq.getChangeLogUpsertKeys(logicalInnerJoinWithoutEquiCond).toSet)
    assertEquals(
      toBitSet(), mq.getChangeLogUpsertKeys(logicalInnerJoinWithEquiAndNonEquiCond).toSet)

    assertEquals(toBitSet(Array(1), Array(1, 5), Array(1, 5, 6)),
      mq.getChangeLogUpsertKeys(logicalLeftJoinOnUniqueKeys).toSet)
    assertEquals(toBitSet(), mq.getChangeLogUpsertKeys(logicalLeftJoinNotOnUniqueKeys).toSet)
    assertEquals(toBitSet(), mq.getChangeLogUpsertKeys(logicalLeftJoinOnRHSUniqueKeys).toSet)
    assertEquals(toBitSet(), mq.getChangeLogUpsertKeys(logicalLeftJoinWithoutEquiCond).toSet)
    assertEquals(toBitSet(), mq.getChangeLogUpsertKeys(logicalLeftJoinWithEquiAndNonEquiCond).toSet)

    assertEquals(toBitSet(Array(5), Array(1, 5), Array(5, 6), Array(1, 5, 6)),
      mq.getChangeLogUpsertKeys(logicalRightJoinOnUniqueKeys).toSet)
    assertEquals(toBitSet(), mq.getChangeLogUpsertKeys(logicalRightJoinNotOnUniqueKeys).toSet)
    assertEquals(toBitSet(), mq.getChangeLogUpsertKeys(logicalRightJoinOnLHSUniqueKeys).toSet)
    assertEquals(toBitSet(), mq.getChangeLogUpsertKeys(logicalRightJoinWithoutEquiCond).toSet)
    assertEquals(
      toBitSet(), mq.getChangeLogUpsertKeys(logicalRightJoinWithEquiAndNonEquiCond).toSet)

    assertEquals(toBitSet(Array(1, 5), Array(1, 5, 6)),
      mq.getChangeLogUpsertKeys(logicalFullJoinOnUniqueKeys).toSet)
    assertEquals(toBitSet(), mq.getChangeLogUpsertKeys(logicalFullJoinNotOnUniqueKeys).toSet)
    assertEquals(toBitSet(), mq.getChangeLogUpsertKeys(logicalFullJoinOnRHSUniqueKeys).toSet)
    assertEquals(toBitSet(), mq.getChangeLogUpsertKeys(logicalFullJoinWithoutEquiCond).toSet)
    assertEquals(toBitSet(), mq.getChangeLogUpsertKeys(logicalFullJoinWithEquiAndNonEquiCond).toSet)

    assertEquals(toBitSet(Array(1)), mq.getChangeLogUpsertKeys(logicalSemiJoinOnUniqueKeys).toSet)
    assertEquals(toBitSet(), mq.getChangeLogUpsertKeys(logicalSemiJoinNotOnUniqueKeys).toSet)
    assertNull(mq.getChangeLogUpsertKeys(logicalSemiJoinOnRHSUniqueKeys))
    assertEquals(
      toBitSet(Array(1)), mq.getChangeLogUpsertKeys(logicalSemiJoinWithoutEquiCond).toSet)
    assertEquals(toBitSet(Array(1)),
      mq.getChangeLogUpsertKeys(logicalSemiJoinWithEquiAndNonEquiCond).toSet)

    assertEquals(toBitSet(Array(1)),
      mq.getChangeLogUpsertKeys(logicalAntiJoinOnUniqueKeys).toSet)
    assertEquals(toBitSet(), mq.getChangeLogUpsertKeys(logicalAntiJoinNotOnUniqueKeys).toSet)
    assertNull(mq.getChangeLogUpsertKeys(logicalAntiJoinOnRHSUniqueKeys))
    assertEquals(
      toBitSet(Array(1)), mq.getChangeLogUpsertKeys(logicalAntiJoinWithoutEquiCond).toSet)
    assertEquals(toBitSet(), mq.getChangeLogUpsertKeys(logicalAntiJoinWithEquiAndNonEquiCond).toSet)
  }

  @Test
  def testGetChangeLogUpsertKeysOnLookupJoin(): Unit = {
    Array(batchLookupJoin, streamLookupJoin).foreach { join =>
      assertEquals(toBitSet(), mq.getChangeLogUpsertKeys(join).toSet)
    }
  }

  @Test
  def testGetChangeLogUpsertKeysOnSetOp(): Unit = {
    Array(logicalUnionAll, logicalIntersectAll, logicalMinusAll).foreach { setOp =>
      assertEquals(toBitSet(), mq.getChangeLogUpsertKeys(setOp).toSet)
    }

    Array(logicalUnion, logicalIntersect, logicalMinus).foreach { setOp =>
      assertEquals(toBitSet(Array(0, 1, 2, 3, 4)), mq.getChangeLogUpsertKeys(setOp).toSet)
    }
  }

  @Test
  def testGetChangeLogUpsertKeysOnDefault(): Unit = {
    assertNull(mq.getChangeLogUpsertKeys(testRel))
  }

  @Test
  def testGetChangeLogUpsertKeysOnIntermediateScan(): Unit = {
    assertEquals(toBitSet(Array(0)), mq.getChangeLogUpsertKeys(intermediateScan).toSet)
  }

  private def toBitSet(keys: Array[Int]*): Set[ImmutableBitSet] = {
    keys.map(k => ImmutableBitSet.of(k: _*)).toSet
  }
}
