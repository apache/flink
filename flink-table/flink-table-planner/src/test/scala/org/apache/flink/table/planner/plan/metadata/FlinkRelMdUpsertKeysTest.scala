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

class FlinkRelMdUpsertKeysTest extends FlinkRelMdHandlerTestBase {

  @Test
  def testGetUpsertKeysOnTableScan(): Unit = {
    Array(studentLogicalScan, studentBatchScan, studentStreamScan).foreach { scan =>
      assertEquals(toBitSet(Array(0)), mq.getUpsertKeys(scan).toSet)
    }

    Array(empLogicalScan, empBatchScan, empStreamScan).foreach { scan =>
      assertNull(mq.getUpsertKeys(scan))
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
    assertEquals(toBitSet(Array(0, 2)), mq.getUpsertKeys(tableSourceScan).toSet)
  }

  @Test
  def testGetUpsertKeysOnProjectedTableScanWithPartialCompositePrimaryKey(): Unit = {
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
    assertNull(mq.getUpsertKeys(tableSourceScan))
  }

  @Test
  def testGetUpsertKeysOnValues(): Unit = {
    assertNull(mq.getUpsertKeys(logicalValues))
    assertNull(mq.getUpsertKeys(emptyValues))
  }

  @Test
  def testGetUpsertKeysOnProject(): Unit = {
    assertEquals(toBitSet(Array(0)), mq.getUpsertKeys(logicalProject).toSet)

    relBuilder.push(studentLogicalScan)
    // id=1, id, cast(id AS bigint not null), cast(id AS int), $1
    val exprs = List(
      relBuilder.call(EQUALS, relBuilder.field(0), relBuilder.literal(1)),
      relBuilder.field(0),
      rexBuilder.makeCast(longType, relBuilder.field(0)),
      rexBuilder.makeCast(intType, relBuilder.field(0)),
      relBuilder.field(1))
    val project1 = relBuilder.project(exprs).build()
    assertEquals(toBitSet(Array(1)), mq.getUpsertKeys(project1).toSet)
  }

  @Test
  def testGetUpsertKeysOnFilter(): Unit = {
    assertEquals(toBitSet(Array(0)), mq.getUpsertKeys(logicalFilter).toSet)
  }

  @Test
  def testGetUpsertKeysOnWatermark(): Unit = {
    assertEquals(toBitSet(Array(0)), mq.getUpsertKeys(logicalWatermarkAssigner).toSet)
  }

  @Test
  def testGetUpsertKeysOnCalc(): Unit = {
    relBuilder.push(studentLogicalScan)
    // id < 100
    val expr = relBuilder.call(LESS_THAN, relBuilder.field(0), relBuilder.literal(100))
    val calc1 = createLogicalCalc(
      studentLogicalScan, logicalProject.getRowType, logicalProject.getProjects, List(expr))
    assertEquals(toBitSet(Array(0)), mq.getUpsertKeys(logicalCalc).toSet)

    // id=1, id, cast(id AS bigint not null), cast(id AS int), $1
    val exprs = List(
      relBuilder.call(EQUALS, relBuilder.field(0), relBuilder.literal(1)),
      relBuilder.field(0),
      rexBuilder.makeCast(longType, relBuilder.field(0)),
      rexBuilder.makeCast(intType, relBuilder.field(0)),
      relBuilder.field(1))
    val rowType = relBuilder.project(exprs).build().getRowType
    val calc2 = createLogicalCalc(studentLogicalScan, rowType, exprs, List(expr))
    assertEquals(toBitSet(Array(1)), mq.getUpsertKeys(calc2).toSet)
  }

  @Test
  def testGetUpsertKeysOnExpand(): Unit = {
    Array(logicalExpand, flinkLogicalExpand, batchExpand, streamExpand).foreach {
      expand => assertEquals(toBitSet(Array(0, 7)), mq.getUpsertKeys(expand).toSet)
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
    assertNull(mq.getUpsertKeys(expand))
  }

  @Test
  def testGetUpsertKeysOnExchange(): Unit = {
    Array(batchExchange, streamExchange).foreach { exchange =>
      assertEquals(toBitSet(), mq.getUpsertKeys(exchange).toSet)
    }

    Array(batchExchangeById, streamExchangeById).foreach { exchange =>
      assertEquals(toBitSet(Array(0)), mq.getUpsertKeys(exchange).toSet)
    }
  }

  @Test
  def testGetUpsertKeysOnRank(): Unit = {
    Array(logicalRank, flinkLogicalRank, batchLocalRank, batchGlobalRank, streamRank).foreach {
      rank =>
        assertEquals(toBitSet(), mq.getUpsertKeys(rank).toSet)
    }

    Array(logicalRankById, flinkLogicalRankById,
      batchLocalRankById, batchGlobalRankById, streamRankById).foreach {
      rank =>
        assertEquals(toBitSet(Array(0)), mq.getUpsertKeys(rank).toSet)
    }

    Array(logicalRowNumber, flinkLogicalRowNumber, streamRowNumber)
      .foreach { rank =>
        assertEquals(toBitSet(Array(0), Array(7)), mq.getUpsertKeys(rank).toSet)
      }
  }

  @Test
  def testGetUpsertKeysOnSort(): Unit = {
    def testWithoutKey(rel: RelNode): Unit = {
      assertEquals(toBitSet(), mq.getUpsertKeys(rel).toSet)
    }

    def testWithKey(rel: RelNode): Unit = {
      assertEquals(toBitSet(Array(0)), mq.getUpsertKeys(rel).toSet)
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
  def testGetUpsertKeysOnStreamExecDeduplicate(): Unit = {
    assertEquals(
      toBitSet(Array(1)),
      mq.getUpsertKeys(streamProcTimeDeduplicateFirstRow).toSet)
    assertEquals(
      toBitSet(Array(1, 2)),
      mq.getUpsertKeys(streamProcTimeDeduplicateLastRow).toSet)
    assertEquals(
      toBitSet(Array(1)),
      mq.getUpsertKeys(streamRowTimeDeduplicateFirstRow).toSet)
    assertEquals(
      toBitSet(Array(1, 2)),
      mq.getUpsertKeys(streamRowTimeDeduplicateLastRow).toSet)
  }

  @Test
  def testGetUpsertKeysOnStreamExecChangelogNormalize(): Unit = {
    assertEquals(toBitSet(Array(1, 0)), mq.getUpsertKeys(streamChangelogNormalize).toSet)
  }

  @Test
  def testGetUpsertKeysOnStreamExecDropUpdateBefore(): Unit = {
    assertEquals(toBitSet(Array(0)), mq.getUpsertKeys(streamDropUpdateBefore).toSet)
  }

  @Test
  def testGetUpsertKeysOnAggregate(): Unit = {
    Array(logicalAgg, flinkLogicalAgg, batchGlobalAggWithLocal, batchGlobalAggWithoutLocal,
      streamGlobalAggWithLocal, streamGlobalAggWithoutLocal).foreach { agg =>
      assertEquals(toBitSet(Array(0)), mq.getUpsertKeys(agg).toSet)
    }
    assertNull(mq.getUpsertKeys(batchLocalAgg))
    assertNull(mq.getUpsertKeys(streamLocalAgg))

    Array(logicalAggWithAuxGroup, flinkLogicalAggWithAuxGroup, batchGlobalAggWithLocalWithAuxGroup,
      batchGlobalAggWithoutLocalWithAuxGroup).foreach { agg =>
      assertEquals(toBitSet(Array(0)), mq.getUpsertKeys(agg).toSet)
    }
    assertNull(mq.getUpsertKeys(batchLocalAggWithAuxGroup))
  }

  @Test
  def testGetUpsertKeysOnWindowAgg(): Unit = {
    Array(logicalWindowAgg, flinkLogicalWindowAgg, batchGlobalWindowAggWithoutLocalAgg,
      batchGlobalWindowAggWithLocalAgg).foreach { agg =>
      assertEquals(ImmutableSet.of(ImmutableBitSet.of(0, 1, 3), ImmutableBitSet.of(0, 1, 4),
        ImmutableBitSet.of(0, 1, 5), ImmutableBitSet.of(0, 1, 6)),
        mq.getUpsertKeys(agg))
    }
    assertNull(mq.getUpsertKeys(batchLocalWindowAgg))

    Array(logicalWindowAggWithAuxGroup, flinkLogicalWindowAggWithAuxGroup,
      batchGlobalWindowAggWithoutLocalAggWithAuxGroup,
      batchGlobalWindowAggWithLocalAggWithAuxGroup).foreach { agg =>
      assertEquals(ImmutableSet.of(ImmutableBitSet.of(0, 3), ImmutableBitSet.of(0, 4),
        ImmutableBitSet.of(0, 5), ImmutableBitSet.of(0, 6)),
        mq.getUpsertKeys(agg))
    }
    assertNull(mq.getUpsertKeys(batchLocalWindowAggWithAuxGroup))
  }

  @Test
  def testGetUpsertKeysOnOverAgg(): Unit = {
    Array(flinkLogicalOverAgg, batchOverAgg, streamOverAgg).foreach { agg =>
      assertEquals(toBitSet(), mq.getUpsertKeys(agg).toSet)
    }

    assertEquals(toBitSet(Array(0)), mq.getUpsertKeys(streamOverAggById).toSet)
  }

  @Test
  def testGetUpsertKeysOnJoin(): Unit = {
    assertEquals(toBitSet(Array(1), Array(5), Array(1, 5), Array(5, 6), Array(1, 5, 6)),
      mq.getUpsertKeys(logicalInnerJoinOnUniqueKeys).toSet)
    assertEquals(toBitSet(), mq.getUpsertKeys(logicalInnerJoinNotOnUniqueKeys).toSet)
    assertEquals(toBitSet(), mq.getUpsertKeys(logicalInnerJoinOnRHSUniqueKeys).toSet)
    assertEquals(toBitSet(), mq.getUpsertKeys(logicalInnerJoinWithoutEquiCond).toSet)
    assertEquals(
      toBitSet(), mq.getUpsertKeys(logicalInnerJoinWithEquiAndNonEquiCond).toSet)

    assertEquals(toBitSet(Array(1), Array(1, 5), Array(1, 5, 6)),
      mq.getUpsertKeys(logicalLeftJoinOnUniqueKeys).toSet)
    assertEquals(toBitSet(), mq.getUpsertKeys(logicalLeftJoinNotOnUniqueKeys).toSet)
    assertEquals(toBitSet(), mq.getUpsertKeys(logicalLeftJoinOnRHSUniqueKeys).toSet)
    assertEquals(toBitSet(), mq.getUpsertKeys(logicalLeftJoinWithoutEquiCond).toSet)
    assertEquals(toBitSet(), mq.getUpsertKeys(logicalLeftJoinWithEquiAndNonEquiCond).toSet)

    assertEquals(toBitSet(Array(5), Array(1, 5), Array(5, 6), Array(1, 5, 6)),
      mq.getUpsertKeys(logicalRightJoinOnUniqueKeys).toSet)
    assertEquals(toBitSet(), mq.getUpsertKeys(logicalRightJoinNotOnUniqueKeys).toSet)
    assertEquals(toBitSet(), mq.getUpsertKeys(logicalRightJoinOnLHSUniqueKeys).toSet)
    assertEquals(toBitSet(), mq.getUpsertKeys(logicalRightJoinWithoutEquiCond).toSet)
    assertEquals(
      toBitSet(), mq.getUpsertKeys(logicalRightJoinWithEquiAndNonEquiCond).toSet)

    assertEquals(toBitSet(Array(1, 5), Array(1, 5, 6)),
      mq.getUpsertKeys(logicalFullJoinOnUniqueKeys).toSet)
    assertEquals(toBitSet(), mq.getUpsertKeys(logicalFullJoinNotOnUniqueKeys).toSet)
    assertEquals(toBitSet(), mq.getUpsertKeys(logicalFullJoinOnRHSUniqueKeys).toSet)
    assertEquals(toBitSet(), mq.getUpsertKeys(logicalFullJoinWithoutEquiCond).toSet)
    assertEquals(toBitSet(), mq.getUpsertKeys(logicalFullJoinWithEquiAndNonEquiCond).toSet)

    assertEquals(toBitSet(Array(1)), mq.getUpsertKeys(logicalSemiJoinOnUniqueKeys).toSet)
    assertEquals(toBitSet(), mq.getUpsertKeys(logicalSemiJoinNotOnUniqueKeys).toSet)
    assertNull(mq.getUpsertKeys(logicalSemiJoinOnRHSUniqueKeys))
    assertEquals(
      toBitSet(Array(1)), mq.getUpsertKeys(logicalSemiJoinWithoutEquiCond).toSet)
    assertEquals(toBitSet(Array(1)),
      mq.getUpsertKeys(logicalSemiJoinWithEquiAndNonEquiCond).toSet)

    assertEquals(toBitSet(Array(1)),
      mq.getUpsertKeys(logicalAntiJoinOnUniqueKeys).toSet)
    assertEquals(toBitSet(), mq.getUpsertKeys(logicalAntiJoinNotOnUniqueKeys).toSet)
    assertNull(mq.getUpsertKeys(logicalAntiJoinOnRHSUniqueKeys))
    assertEquals(
      toBitSet(Array(1)), mq.getUpsertKeys(logicalAntiJoinWithoutEquiCond).toSet)
    assertEquals(toBitSet(), mq.getUpsertKeys(logicalAntiJoinWithEquiAndNonEquiCond).toSet)
  }

  @Test
  def testGetUpsertKeysOnLookupJoin(): Unit = {
    Array(batchLookupJoin, streamLookupJoin).foreach { join =>
      assertEquals(toBitSet(), mq.getUpsertKeys(join).toSet)
    }
  }

  @Test
  def testGetUpsertKeysOnSetOp(): Unit = {
    Array(logicalUnionAll, logicalIntersectAll, logicalMinusAll).foreach { setOp =>
      assertEquals(toBitSet(), mq.getUpsertKeys(setOp).toSet)
    }

    Array(logicalUnion, logicalIntersect, logicalMinus).foreach { setOp =>
      assertEquals(toBitSet(Array(0, 1, 2, 3, 4)), mq.getUpsertKeys(setOp).toSet)
    }
  }

  @Test
  def testGetUpsertKeysOnDefault(): Unit = {
    assertNull(mq.getUpsertKeys(testRel))
  }

  @Test
  def testGetUpsertKeysOnIntermediateScan(): Unit = {
    assertEquals(toBitSet(Array(0)), mq.getUpsertKeys(intermediateScan).toSet)
  }

  private def toBitSet(keys: Array[Int]*): Set[ImmutableBitSet] = {
    keys.map(k => ImmutableBitSet.of(k: _*)).toSet
  }
}
