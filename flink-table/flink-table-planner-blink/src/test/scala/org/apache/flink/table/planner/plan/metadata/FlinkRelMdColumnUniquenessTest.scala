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
import org.apache.calcite.rel.{RelCollations, RelNode}
import org.apache.calcite.sql.`type`.SqlTypeName.{BIGINT, VARCHAR}
import org.apache.calcite.util.ImmutableBitSet
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConversions._

class FlinkRelMdColumnUniquenessTest extends FlinkRelMdHandlerTestBase {

  @Test
  def testAreColumnsUniqueOnTableScan(): Unit = {
    Array(studentLogicalScan, studentFlinkLogicalScan, studentBatchScan, studentStreamScan)
      .foreach { scan =>
        assertFalse(mq.areColumnsUnique(scan, ImmutableBitSet.of()))
        assertTrue(mq.areColumnsUnique(scan, ImmutableBitSet.of(0)))
        (1 until scan.getRowType.getFieldCount).foreach { idx =>
          assertFalse(mq.areColumnsUnique(scan, ImmutableBitSet.of(idx)))
        }
        assertTrue(mq.areColumnsUnique(scan, ImmutableBitSet.of(0, 1)))
        assertTrue(mq.areColumnsUnique(scan, ImmutableBitSet.of(0, 2)))
        assertFalse(mq.areColumnsUnique(scan, ImmutableBitSet.of(1, 2)))
      }

    Array(empLogicalScan, empFlinkLogicalScan, empBatchScan, empStreamScan).foreach {
      scan =>
        (0 until scan.getRowType.getFieldCount).foreach { idx =>
          assertNull(mq.areColumnsUnique(scan, ImmutableBitSet.of(idx)))
        }
    }
  }

  @Test
  def testAreColumnsUniqueOnValues(): Unit = {
    assertTrue(mq.areColumnsUnique(logicalValues, ImmutableBitSet.of(0)))
    assertFalse(mq.areColumnsUnique(logicalValues, ImmutableBitSet.of(1)))
    assertTrue(mq.areColumnsUnique(logicalValues, ImmutableBitSet.of(2)))
    assertTrue(mq.areColumnsUnique(logicalValues, ImmutableBitSet.of(3)))
    assertTrue(mq.areColumnsUnique(logicalValues, ImmutableBitSet.of(4)))
    assertTrue(mq.areColumnsUnique(logicalValues, ImmutableBitSet.of(5)))
    assertFalse(mq.areColumnsUnique(logicalValues, ImmutableBitSet.of(6)))
    assertTrue(mq.areColumnsUnique(logicalValues, ImmutableBitSet.of(7)))

    (0 until emptyValues.getRowType.getFieldCount).foreach { idx =>
      assertTrue(mq.areColumnsUnique(emptyValues, ImmutableBitSet.of(idx)))
    }
  }

  @Test
  def testAreColumnsUniqueOnProject(): Unit = {
    assertTrue(mq.areColumnsUnique(logicalProject, ImmutableBitSet.of(0)))
    assertFalse(mq.areColumnsUnique(logicalProject, ImmutableBitSet.of(1)))
    assertNull(mq.areColumnsUnique(logicalProject, ImmutableBitSet.of(2)))
    assertNull(mq.areColumnsUnique(logicalProject, ImmutableBitSet.of(3)))
    assertNull(mq.areColumnsUnique(logicalProject, ImmutableBitSet.of(4)))
    assertNull(mq.areColumnsUnique(logicalProject, ImmutableBitSet.of(5)))
    assertFalse(mq.areColumnsUnique(logicalProject, ImmutableBitSet.of(6)))
    assertNull(mq.areColumnsUnique(logicalProject, ImmutableBitSet.of(7)))
    assertNull(mq.areColumnsUnique(logicalProject, ImmutableBitSet.of(8)))
    assertNull(mq.areColumnsUnique(logicalProject, ImmutableBitSet.of(9)))
    assertNull(mq.areColumnsUnique(logicalProject, ImmutableBitSet.of(10)))
    assertNull(mq.areColumnsUnique(logicalProject, ImmutableBitSet.of(11)))
    assertTrue(mq.areColumnsUnique(logicalProject, ImmutableBitSet.of(0, 1)))
    assertTrue(mq.areColumnsUnique(logicalProject, ImmutableBitSet.of(0, 2)))
    assertFalse(mq.areColumnsUnique(logicalProject, ImmutableBitSet.of(1, 2)))
    assertNull(mq.areColumnsUnique(logicalProject, ImmutableBitSet.of(2, 3)))

    // project: id, cast(id as long not null), name, cast(name as varchar not null)
    relBuilder.push(studentLogicalScan)
    val exprs = List(
      relBuilder.field(0),
      relBuilder.cast(relBuilder.field(0), BIGINT),
      relBuilder.field(1),
      relBuilder.cast(relBuilder.field(1), VARCHAR)
    )
    val project = relBuilder.project(exprs).build()
    assertTrue(mq.areColumnsUnique(project, ImmutableBitSet.of(0)))
    assertNull(mq.areColumnsUnique(project, ImmutableBitSet.of(1)))
    assertTrue(mq.areColumnsUnique(project, ImmutableBitSet.of(1), true))
    assertFalse(mq.areColumnsUnique(project, ImmutableBitSet.of(2)))
    assertNull(mq.areColumnsUnique(project, ImmutableBitSet.of(3)))
    assertTrue(mq.areColumnsUnique(project, ImmutableBitSet.of(1, 2), true))
    assertTrue(mq.areColumnsUnique(project, ImmutableBitSet.of(1, 3), true))
  }

  @Test
  def testAreColumnsUniqueOnFilter(): Unit = {
    assertFalse(mq.areColumnsUnique(logicalFilter, ImmutableBitSet.of()))
    assertTrue(mq.areColumnsUnique(logicalFilter, ImmutableBitSet.of(0)))
    (1 until logicalFilter.getRowType.getFieldCount).foreach { idx =>
      assertFalse(mq.areColumnsUnique(logicalFilter, ImmutableBitSet.of(idx)))
    }
    assertTrue(mq.areColumnsUnique(logicalFilter, ImmutableBitSet.of(0, 1)))
    assertTrue(mq.areColumnsUnique(logicalFilter, ImmutableBitSet.of(0, 2)))
    assertFalse(mq.areColumnsUnique(logicalFilter, ImmutableBitSet.of(1, 2)))
  }

  @Test
  def testAreColumnsUniqueOnCalc(): Unit = {
    assertTrue(mq.areColumnsUnique(logicalCalc, ImmutableBitSet.of(0)))
    assertFalse(mq.areColumnsUnique(logicalCalc, ImmutableBitSet.of(1)))
    assertNull(mq.areColumnsUnique(logicalCalc, ImmutableBitSet.of(2)))
    assertNull(mq.areColumnsUnique(logicalCalc, ImmutableBitSet.of(3)))
    assertNull(mq.areColumnsUnique(logicalCalc, ImmutableBitSet.of(4)))
    assertNull(mq.areColumnsUnique(logicalCalc, ImmutableBitSet.of(5)))
    assertFalse(mq.areColumnsUnique(logicalCalc, ImmutableBitSet.of(6)))
    assertNull(mq.areColumnsUnique(logicalCalc, ImmutableBitSet.of(7)))
    assertNull(mq.areColumnsUnique(logicalCalc, ImmutableBitSet.of(8)))
    assertNull(mq.areColumnsUnique(logicalCalc, ImmutableBitSet.of(9)))
    assertNull(mq.areColumnsUnique(logicalCalc, ImmutableBitSet.of(10)))
    assertNull(mq.areColumnsUnique(logicalCalc, ImmutableBitSet.of(11)))
    assertTrue(mq.areColumnsUnique(logicalCalc, ImmutableBitSet.of(0, 1)))
    assertTrue(mq.areColumnsUnique(logicalCalc, ImmutableBitSet.of(0, 2)))
    assertFalse(mq.areColumnsUnique(logicalCalc, ImmutableBitSet.of(1, 2)))
    assertNull(mq.areColumnsUnique(logicalCalc, ImmutableBitSet.of(2, 3)))
  }

  @Test
  def testAreColumnsUniqueOnExpand(): Unit = {
    Array(logicalExpand, flinkLogicalExpand, batchExpand, streamExpand).foreach {
      expand =>
        assertFalse(mq.areColumnsUnique(expand, ImmutableBitSet.of()))
        (0 until expand.getRowType.getFieldCount).foreach { idx =>
          assertFalse(mq.areColumnsUnique(expand, ImmutableBitSet.of(idx)))
        }
        assertTrue(mq.areColumnsUnique(expand, ImmutableBitSet.of(0, 7)))
        (1 until expand.getRowType.getFieldCount - 1).foreach { idx =>
          assertFalse(mq.areColumnsUnique(expand, ImmutableBitSet.of(idx, 7)))
        }
    }

    val expandProjects = ExpandUtil.createExpandProjects(
      studentLogicalScan.getCluster.getRexBuilder,
      studentLogicalScan.getRowType,
      ImmutableBitSet.of(0, 3, 5),
      ImmutableList.of(
        ImmutableBitSet.of(0, 3, 5),
        ImmutableBitSet.of(3, 5),
        ImmutableBitSet.of(3)),
      Array.empty[Integer])
    val logicalExpand2 = new LogicalExpand(cluster, studentLogicalScan.getTraitSet,
      studentLogicalScan, expandProjects, 7)
    (0 until logicalExpand2.getRowType.getFieldCount - 1).foreach { idx =>
      assertFalse(mq.areColumnsUnique(logicalExpand2, ImmutableBitSet.of(idx, 7)))
    }
  }

  @Test
  def testAreColumnsUniqueOnExchange(): Unit = {
    Array(batchExchange, streamExchange).foreach {
      exchange =>
        assertFalse(mq.areColumnsUnique(exchange, ImmutableBitSet.of()))
        assertTrue(mq.areColumnsUnique(exchange, ImmutableBitSet.of(0)))
        (1 until exchange.getRowType.getFieldCount).foreach { idx =>
          assertFalse(mq.areColumnsUnique(exchange, ImmutableBitSet.of(idx)))
        }
        assertTrue(mq.areColumnsUnique(exchange, ImmutableBitSet.of(0, 1)))
        assertTrue(mq.areColumnsUnique(exchange, ImmutableBitSet.of(0, 2)))
        assertFalse(mq.areColumnsUnique(exchange, ImmutableBitSet.of(1, 2)))
    }
  }

  @Test
  def testAreColumnsUniqueOnRank(): Unit = {
    Array(logicalRank, flinkLogicalRank, batchLocalRank, batchGlobalRank, streamRank,
      logicalRankWithVariableRange, flinkLogicalRankWithVariableRange, streamRankWithVariableRange)
      .foreach {
        rank =>
          assertTrue(mq.areColumnsUnique(rank, ImmutableBitSet.of(0)))
          (1 until rank.getRowType.getFieldCount).foreach { idx =>
            assertFalse(mq.areColumnsUnique(rank, ImmutableBitSet.of(idx)))
          }
          assertTrue(mq.areColumnsUnique(rank, ImmutableBitSet.of(0, 1)))
          assertTrue(mq.areColumnsUnique(rank, ImmutableBitSet.of(0, 2)))
          assertFalse(mq.areColumnsUnique(rank, ImmutableBitSet.of(1, 2)))
      }
    Array(logicalRowNumber, flinkLogicalRowNumber, streamRowNumber).foreach {
      rank =>
        assertTrue(mq.areColumnsUnique(rank, ImmutableBitSet.of(0)))
        val rankFunColumn = rank.getRowType.getFieldCount - 1
        (1 until rankFunColumn).foreach { idx =>
          assertFalse(mq.areColumnsUnique(rank, ImmutableBitSet.of(idx)))
        }
        assertTrue(mq.areColumnsUnique(rank, ImmutableBitSet.of(rankFunColumn)))
        assertTrue(mq.areColumnsUnique(rank, ImmutableBitSet.of(0, rankFunColumn)))
        assertTrue(mq.areColumnsUnique(rank, ImmutableBitSet.of(0, 1)))
        assertTrue(mq.areColumnsUnique(rank, ImmutableBitSet.of(0, 2)))
        assertFalse(mq.areColumnsUnique(rank, ImmutableBitSet.of(1, 2)))
        assertTrue(mq.areColumnsUnique(rank, ImmutableBitSet.of(1, rankFunColumn)))
        assertTrue(mq.areColumnsUnique(rank, ImmutableBitSet.of(2, rankFunColumn)))
    }

    val rowNumber = new LogicalRank(
      cluster,
      logicalTraits,
      studentLogicalScan,
      ImmutableBitSet.of(6),
      RelCollations.of(4),
      RankType.ROW_NUMBER,
      new ConstantRankRange(3, 6),
      new RelDataTypeFieldImpl("rn", 7, longType),
      outputRankNumber = true
    )
    assertTrue(mq.areColumnsUnique(rowNumber, ImmutableBitSet.of(0)))
    (1 until rowNumber.getRowType.getFieldCount).foreach { idx =>
      assertFalse(mq.areColumnsUnique(rowNumber, ImmutableBitSet.of(idx)))
    }
    assertTrue(mq.areColumnsUnique(rowNumber, ImmutableBitSet.of(0, 7)))
    assertFalse(mq.areColumnsUnique(rowNumber, ImmutableBitSet.of(1, 7)))
    // partition key and row number
    assertTrue(mq.areColumnsUnique(rowNumber, ImmutableBitSet.of(6, 7)))
  }

  @Test
  def testAreColumnsUniqueCountOnSort(): Unit = {
    Array(logicalSort, flinkLogicalSort, batchSort, streamSort,
      logicalLimit, flinkLogicalLimit, batchLimit, batchLocalLimit, batchGlobalLimit, streamLimit,
      logicalSortLimit, flinkLogicalSortLimit, batchSortLimit, batchLocalSortLimit,
      batchGlobalSortLimit, streamSortLimit).foreach {
      sort =>
        assertFalse(mq.areColumnsUnique(sort, ImmutableBitSet.of()))
        assertTrue(mq.areColumnsUnique(sort, ImmutableBitSet.of(0)))
        (1 until sort.getRowType.getFieldCount).foreach { idx =>
          assertFalse(mq.areColumnsUnique(sort, ImmutableBitSet.of(idx)))
        }
        assertTrue(mq.areColumnsUnique(sort, ImmutableBitSet.of(0, 1)))
        assertTrue(mq.areColumnsUnique(sort, ImmutableBitSet.of(0, 2)))
        assertFalse(mq.areColumnsUnique(sort, ImmutableBitSet.of(1, 2)))
    }
  }

  @Test
  def testAreColumnsUniqueCountOnStreamExecDeduplicate(): Unit = {
    assertTrue(mq.areColumnsUnique(streamProcTimeDeduplicateFirstRow, ImmutableBitSet.of(1)))
    assertFalse(mq.areColumnsUnique(streamProcTimeDeduplicateFirstRow, ImmutableBitSet.of(0, 2)))
    assertFalse(mq.areColumnsUnique(streamProcTimeDeduplicateFirstRow, ImmutableBitSet.of(0, 1, 2)))
    assertFalse(mq.areColumnsUnique(streamProcTimeDeduplicateLastRow, ImmutableBitSet.of(0)))
    assertFalse(mq.areColumnsUnique(streamProcTimeDeduplicateLastRow, ImmutableBitSet.of(0, 1)))
    assertFalse(mq.areColumnsUnique(streamProcTimeDeduplicateLastRow, ImmutableBitSet.of(0, 1, 2)))

    assertTrue(mq.areColumnsUnique(streamRowTimeDeduplicateFirstRow, ImmutableBitSet.of(1)))
    assertFalse(mq.areColumnsUnique(streamRowTimeDeduplicateFirstRow, ImmutableBitSet.of(0, 2)))
    assertFalse(mq.areColumnsUnique(streamRowTimeDeduplicateFirstRow, ImmutableBitSet.of(0, 1, 2)))
    assertFalse(mq.areColumnsUnique(streamRowTimeDeduplicateLastRow, ImmutableBitSet.of(0)))
    assertFalse(mq.areColumnsUnique(streamRowTimeDeduplicateLastRow, ImmutableBitSet.of(0, 1)))
    assertFalse(mq.areColumnsUnique(streamRowTimeDeduplicateLastRow, ImmutableBitSet.of(0, 1, 2)))
  }

  @Test
  def testAreColumnsUniqueCountOnStreamExecChangelogNormalize(): Unit = {
    assertTrue(mq.areColumnsUnique(streamChangelogNormalize, ImmutableBitSet.of(0, 1)))
    assertTrue(mq.areColumnsUnique(streamChangelogNormalize, ImmutableBitSet.of(1, 0)))
    assertFalse(mq.areColumnsUnique(streamChangelogNormalize, ImmutableBitSet.of(1)))
    assertFalse(mq.areColumnsUnique(streamChangelogNormalize, ImmutableBitSet.of(2)))
    assertFalse(mq.areColumnsUnique(streamChangelogNormalize, ImmutableBitSet.of(1, 2)))
  }

  @Test
  def testAreColumnsUniqueCountOnStreamExecDropUpdateBefore(): Unit = {
    assertFalse(mq.areColumnsUnique(streamDropUpdateBefore, ImmutableBitSet.of()))
    assertTrue(mq.areColumnsUnique(streamDropUpdateBefore, ImmutableBitSet.of(0)))
    assertTrue(mq.areColumnsUnique(streamDropUpdateBefore, ImmutableBitSet.of(0, 1)))
    assertTrue(mq.areColumnsUnique(streamDropUpdateBefore, ImmutableBitSet.of(0, 2)))
    assertFalse(mq.areColumnsUnique(streamDropUpdateBefore, ImmutableBitSet.of(1, 2)))
  }

  @Test
  def testAreColumnsUniqueOnAggregate(): Unit = {
    Array(logicalAgg, flinkLogicalAgg).foreach { agg =>
      assertTrue(mq.areColumnsUnique(agg, ImmutableBitSet.of(0)))
      val fieldCnt = agg.getRowType.getFieldCount
      (1 until fieldCnt).foreach { idx =>
        assertFalse(mq.areColumnsUnique(agg, ImmutableBitSet.of(idx)))
      }
      assertTrue(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 1)))
      assertTrue(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 2)))
      assertFalse(mq.areColumnsUnique(agg, ImmutableBitSet.of(1, 2)))
      assertTrue(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 1, 2)))
      assertFalse(mq.areColumnsUnique(agg, ImmutableBitSet.range(1, fieldCnt)))
    }

    Array(logicalAggWithAuxGroup, flinkLogicalAggWithAuxGroup).foreach { agg =>
      assertTrue(mq.areColumnsUnique(agg, ImmutableBitSet.of(0)))
      val fieldCnt = agg.getRowType.getFieldCount
      (1 until fieldCnt).foreach { idx =>
        assertFalse(mq.areColumnsUnique(agg, ImmutableBitSet.of(idx)))
      }
      assertTrue(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 1)))
      assertTrue(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 2)))
      assertFalse(mq.areColumnsUnique(agg, ImmutableBitSet.of(1, 2)))
      assertTrue(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 1, 2)))
      assertFalse(mq.areColumnsUnique(agg, ImmutableBitSet.range(1, fieldCnt)))
    }
  }

  @Test
  def testAreColumnsUniqueOnBatchExecAggregate(): Unit = {
    Array(batchGlobalAggWithLocal, batchGlobalAggWithoutLocal).foreach { agg =>
      assertTrue(mq.areColumnsUnique(agg, ImmutableBitSet.of(0)))
      val fieldCnt = agg.getRowType.getFieldCount
      (1 until fieldCnt).foreach { idx =>
        assertFalse(mq.areColumnsUnique(agg, ImmutableBitSet.of(idx)))
      }
      assertTrue(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 1)))
      assertFalse(mq.areColumnsUnique(agg, ImmutableBitSet.of(1, 2)))
      assertFalse(mq.areColumnsUnique(agg, ImmutableBitSet.range(1, fieldCnt)))
    }
    Array(batchGlobalAggWithLocalWithAuxGroup, batchGlobalAggWithoutLocalWithAuxGroup)
      .foreach { agg =>
        assertTrue(mq.areColumnsUnique(agg, ImmutableBitSet.of(0)))
        val fieldCnt = agg.getRowType.getFieldCount
        (1 until fieldCnt).foreach { idx =>
          assertFalse(mq.areColumnsUnique(agg, ImmutableBitSet.of(idx)))
        }
        assertTrue(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 1)))
        assertFalse(mq.areColumnsUnique(agg, ImmutableBitSet.of(1, 2)))
        assertFalse(mq.areColumnsUnique(agg, ImmutableBitSet.range(1, fieldCnt)))
      }
    // always return null for local agg
    Array(batchLocalAgg, batchLocalAggWithAuxGroup).foreach { agg =>
      (0 until agg.getRowType.getFieldCount).foreach { idx =>
        assertNull(mq.areColumnsUnique(agg, ImmutableBitSet.of(idx)))
      }
    }
  }

  @Test
  def testAreColumnsUniqueOnStreamExecAggregate(): Unit = {
    Array(streamGlobalAggWithLocal, streamGlobalAggWithoutLocal).foreach { agg =>
      assertTrue(mq.areColumnsUnique(agg, ImmutableBitSet.of(0)))
      val fieldCnt = agg.getRowType.getFieldCount
      (1 until fieldCnt).foreach { idx =>
        assertFalse(mq.areColumnsUnique(agg, ImmutableBitSet.of(idx)))
      }
      assertTrue(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 1)))
      assertFalse(mq.areColumnsUnique(agg, ImmutableBitSet.of(1, 2)))
      assertFalse(mq.areColumnsUnique(agg, ImmutableBitSet.range(1, fieldCnt)))
    }
    // always return null for local agg
    (0 until streamLocalAgg.getRowType.getFieldCount).foreach { idx =>
      assertNull(mq.areColumnsUnique(streamLocalAgg, ImmutableBitSet.of(idx)))
    }
  }

  @Test
  def testAreColumnsUniqueOnWindowAgg(): Unit = {
    Array(logicalWindowAgg, flinkLogicalWindowAgg, batchGlobalWindowAggWithLocalAgg,
      batchGlobalWindowAggWithoutLocalAgg, streamWindowAgg).foreach { agg =>
      assertFalse(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 1)))
      assertFalse(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 2)))
      assertFalse(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 3)))
      assertFalse(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 1, 2)))
      assertTrue(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 1, 3)))
      assertTrue(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 1, 4)))
      assertTrue(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 1, 5)))
      assertTrue(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 1, 6)))
      assertTrue(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 1, 3, 4, 5, 6)))
      assertFalse(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 2, 3)))
    }
    assertNull(mq.areColumnsUnique(batchLocalWindowAgg, ImmutableBitSet.of(0, 1)))
    assertNull(mq.areColumnsUnique(batchLocalWindowAgg, ImmutableBitSet.of(0, 1, 3)))

    Array(logicalWindowAgg2, flinkLogicalWindowAgg2, batchGlobalWindowAggWithLocalAgg2,
      batchGlobalWindowAggWithoutLocalAgg2, streamWindowAgg2).foreach { agg =>
      assertFalse(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 1)))
      assertTrue(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 2)))
      assertTrue(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 3)))
      assertTrue(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 4)))
      assertTrue(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 5)))
      assertTrue(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 2, 3, 4, 5)))
      assertFalse(mq.areColumnsUnique(agg, ImmutableBitSet.of(1, 2)))
      assertFalse(mq.areColumnsUnique(agg, ImmutableBitSet.of(1, 3)))
    }
    assertNull(mq.areColumnsUnique(batchLocalWindowAgg2, ImmutableBitSet.of(0, 1)))
    assertNull(mq.areColumnsUnique(batchLocalWindowAgg2, ImmutableBitSet.of(0, 2)))

    Array(logicalWindowAggWithAuxGroup, flinkLogicalWindowAggWithAuxGroup,
      batchGlobalWindowAggWithLocalAggWithAuxGroup, batchGlobalWindowAggWithoutLocalAggWithAuxGroup
    ).foreach { agg =>
      assertFalse(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 1)))
      assertFalse(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 2)))
      assertFalse(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 1, 2)))
      assertTrue(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 3)))
      assertTrue(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 4)))
      assertTrue(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 5)))
      assertTrue(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 6)))
      assertTrue(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 3, 4, 5, 6)))
      assertFalse(mq.areColumnsUnique(agg, ImmutableBitSet.of(1, 3)))
    }
    assertNull(mq.areColumnsUnique(batchLocalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1)))
    assertNull(mq.areColumnsUnique(batchLocalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 3)))
  }

  @Test
  def testAreColumnsUniqueOnOverAgg(): Unit = {
    Array(flinkLogicalOverAgg, batchOverAgg).foreach { agg =>
      assertTrue(mq.areColumnsUnique(agg, ImmutableBitSet.of(0)))
      assertFalse(mq.areColumnsUnique(agg, ImmutableBitSet.of(1)))
      assertFalse(mq.areColumnsUnique(agg, ImmutableBitSet.of(2)))
      assertFalse(mq.areColumnsUnique(agg, ImmutableBitSet.of(3)))
      assertFalse(mq.areColumnsUnique(agg, ImmutableBitSet.of(4)))
      assertNull(mq.areColumnsUnique(agg, ImmutableBitSet.of(5)))
      assertNull(mq.areColumnsUnique(agg, ImmutableBitSet.of(6)))
      assertNull(mq.areColumnsUnique(agg, ImmutableBitSet.of(7)))
      assertNull(mq.areColumnsUnique(agg, ImmutableBitSet.of(8)))
      assertNull(mq.areColumnsUnique(agg, ImmutableBitSet.of(9)))
      assertNull(mq.areColumnsUnique(agg, ImmutableBitSet.of(10)))
      assertTrue(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 1)))
      assertTrue(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 2)))
      assertFalse(mq.areColumnsUnique(agg, ImmutableBitSet.of(1, 2)))
      assertTrue(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 5)))
      assertTrue(mq.areColumnsUnique(agg, ImmutableBitSet.of(0, 10)))
      assertNull(mq.areColumnsUnique(agg, ImmutableBitSet.of(5, 10)))
    }
    assertTrue(mq.areColumnsUnique(streamOverAgg, ImmutableBitSet.of(0)))
    assertFalse(mq.areColumnsUnique(streamOverAgg, ImmutableBitSet.of(1)))
    assertFalse(mq.areColumnsUnique(streamOverAgg, ImmutableBitSet.of(2)))
    assertFalse(mq.areColumnsUnique(streamOverAgg, ImmutableBitSet.of(3)))
    assertFalse(mq.areColumnsUnique(streamOverAgg, ImmutableBitSet.of(4)))
    assertNull(mq.areColumnsUnique(streamOverAgg, ImmutableBitSet.of(5)))
    assertNull(mq.areColumnsUnique(streamOverAgg, ImmutableBitSet.of(6)))
    assertNull(mq.areColumnsUnique(streamOverAgg, ImmutableBitSet.of(7)))
    assertTrue(mq.areColumnsUnique(streamOverAgg, ImmutableBitSet.of(0, 1)))
    assertTrue(mq.areColumnsUnique(streamOverAgg, ImmutableBitSet.of(0, 2)))
    assertFalse(mq.areColumnsUnique(streamOverAgg, ImmutableBitSet.of(1, 2)))
    assertTrue(mq.areColumnsUnique(streamOverAgg, ImmutableBitSet.of(0, 5)))
    assertTrue(mq.areColumnsUnique(streamOverAgg, ImmutableBitSet.of(0, 7)))
    assertNull(mq.areColumnsUnique(streamOverAgg, ImmutableBitSet.of(5, 7)))
  }

  @Test
  def testAreColumnsUniqueOnJoin(): Unit = {
    // inner join
    assertFalse(mq.areColumnsUnique(logicalInnerJoinOnUniqueKeys, ImmutableBitSet.of(0)))
    assertTrue(mq.areColumnsUnique(logicalInnerJoinOnUniqueKeys, ImmutableBitSet.of(1)))
    assertFalse(mq.areColumnsUnique(logicalInnerJoinOnUniqueKeys, ImmutableBitSet.of(2)))
    assertTrue(mq.areColumnsUnique(logicalInnerJoinOnUniqueKeys, ImmutableBitSet.of(5)))
    assertFalse(mq.areColumnsUnique(logicalInnerJoinOnUniqueKeys, ImmutableBitSet.of(6)))
    assertTrue(mq.areColumnsUnique(logicalInnerJoinOnUniqueKeys, ImmutableBitSet.of(0, 1)))
    assertFalse(mq.areColumnsUnique(logicalInnerJoinOnUniqueKeys, ImmutableBitSet.of(0, 2)))
    assertTrue(mq.areColumnsUnique(logicalInnerJoinOnUniqueKeys, ImmutableBitSet.of(1, 2)))
    assertFalse(mq.areColumnsUnique(logicalInnerJoinOnUniqueKeys, ImmutableBitSet.of(0, 5)))
    assertTrue(mq.areColumnsUnique(logicalInnerJoinOnUniqueKeys, ImmutableBitSet.of(1, 5)))
    assertFalse(mq.areColumnsUnique(logicalInnerJoinOnUniqueKeys, ImmutableBitSet.of(0, 6)))
    assertFalse(mq.areColumnsUnique(logicalInnerJoinOnUniqueKeys, ImmutableBitSet.of(1, 6)))
    assertTrue(mq.areColumnsUnique(logicalInnerJoinOnUniqueKeys, ImmutableBitSet.of(5, 6)))
    assertTrue(mq.areColumnsUnique(logicalInnerJoinOnUniqueKeys, ImmutableBitSet.of(0, 1, 5, 6)))

    // left join
    assertFalse(mq.areColumnsUnique(logicalLeftJoinOnUniqueKeys, ImmutableBitSet.of(0)))
    assertTrue(mq.areColumnsUnique(logicalLeftJoinOnUniqueKeys, ImmutableBitSet.of(1)))
    assertFalse(mq.areColumnsUnique(logicalLeftJoinOnUniqueKeys, ImmutableBitSet.of(2)))
    assertFalse(mq.areColumnsUnique(logicalLeftJoinOnUniqueKeys, ImmutableBitSet.of(5)))
    assertFalse(mq.areColumnsUnique(logicalLeftJoinOnUniqueKeys, ImmutableBitSet.of(6)))
    assertTrue(mq.areColumnsUnique(logicalLeftJoinOnUniqueKeys, ImmutableBitSet.of(0, 1)))
    assertFalse(mq.areColumnsUnique(logicalLeftJoinOnUniqueKeys, ImmutableBitSet.of(0, 2)))
    assertTrue(mq.areColumnsUnique(logicalLeftJoinOnUniqueKeys, ImmutableBitSet.of(1, 5)))
    assertFalse(mq.areColumnsUnique(logicalLeftJoinOnUniqueKeys, ImmutableBitSet.of(1, 6)))
    assertFalse(mq.areColumnsUnique(logicalLeftJoinOnUniqueKeys, ImmutableBitSet.of(5, 6)))
    assertTrue(mq.areColumnsUnique(logicalLeftJoinOnUniqueKeys, ImmutableBitSet.of(0, 1, 5, 6)))

    // right join
    assertFalse(mq.areColumnsUnique(logicalRightJoinOnUniqueKeys, ImmutableBitSet.of(0)))
    assertFalse(mq.areColumnsUnique(logicalRightJoinOnUniqueKeys, ImmutableBitSet.of(1)))
    assertFalse(mq.areColumnsUnique(logicalRightJoinOnUniqueKeys, ImmutableBitSet.of(2)))
    assertTrue(mq.areColumnsUnique(logicalRightJoinOnUniqueKeys, ImmutableBitSet.of(5)))
    assertFalse(mq.areColumnsUnique(logicalRightJoinOnUniqueKeys, ImmutableBitSet.of(6)))
    assertFalse(mq.areColumnsUnique(logicalRightJoinOnUniqueKeys, ImmutableBitSet.of(0, 1)))
    assertFalse(mq.areColumnsUnique(logicalRightJoinOnUniqueKeys, ImmutableBitSet.of(0, 2)))
    assertTrue(mq.areColumnsUnique(logicalRightJoinOnUniqueKeys, ImmutableBitSet.of(1, 5)))
    assertFalse(mq.areColumnsUnique(logicalRightJoinOnUniqueKeys, ImmutableBitSet.of(1, 6)))
    assertTrue(mq.areColumnsUnique(logicalRightJoinOnUniqueKeys, ImmutableBitSet.of(5, 6)))
    assertTrue(mq.areColumnsUnique(logicalRightJoinOnUniqueKeys, ImmutableBitSet.of(0, 1, 5, 6)))

    // full join
    assertFalse(mq.areColumnsUnique(logicalFullJoinOnUniqueKeys, ImmutableBitSet.of(0)))
    assertFalse(mq.areColumnsUnique(logicalFullJoinOnUniqueKeys, ImmutableBitSet.of(1)))
    assertFalse(mq.areColumnsUnique(logicalFullJoinOnUniqueKeys, ImmutableBitSet.of(2)))
    assertFalse(mq.areColumnsUnique(logicalFullJoinOnUniqueKeys, ImmutableBitSet.of(5)))
    assertFalse(mq.areColumnsUnique(logicalFullJoinOnUniqueKeys, ImmutableBitSet.of(6)))
    assertFalse(mq.areColumnsUnique(logicalFullJoinOnUniqueKeys, ImmutableBitSet.of(0, 1)))
    assertFalse(mq.areColumnsUnique(logicalFullJoinOnUniqueKeys, ImmutableBitSet.of(0, 2)))
    assertTrue(mq.areColumnsUnique(logicalFullJoinOnUniqueKeys, ImmutableBitSet.of(1, 5)))
    assertFalse(mq.areColumnsUnique(logicalFullJoinOnUniqueKeys, ImmutableBitSet.of(1, 6)))
    assertFalse(mq.areColumnsUnique(logicalFullJoinOnUniqueKeys, ImmutableBitSet.of(5, 6)))
    assertTrue(mq.areColumnsUnique(logicalFullJoinOnUniqueKeys, ImmutableBitSet.of(0, 1, 5, 6)))

    // semi/anti join
    Array(logicalSemiJoinOnUniqueKeys, logicalSemiJoinNotOnUniqueKeys,
      logicalSemiJoinOnDisjointKeys, logicalAntiJoinOnUniqueKeys, logicalAntiJoinNotOnUniqueKeys,
      logicalAntiJoinOnDisjointKeys).foreach { join =>
      assertFalse(mq.areColumnsUnique(join, ImmutableBitSet.of(0)))
      assertTrue(mq.areColumnsUnique(join, ImmutableBitSet.of(1)))
      assertFalse(mq.areColumnsUnique(join, ImmutableBitSet.of(2)))
      assertFalse(mq.areColumnsUnique(join, ImmutableBitSet.of(3)))
      assertFalse(mq.areColumnsUnique(join, ImmutableBitSet.of(4)))
      assertTrue(mq.areColumnsUnique(join, ImmutableBitSet.of(0, 1)))
      assertFalse(mq.areColumnsUnique(join, ImmutableBitSet.of(0, 2)))
    }
  }

  @Test
  def testAreColumnsUniqueOnLookupJoin(): Unit = {
    Array(batchLookupJoin, streamLookupJoin).foreach { join =>
      assertFalse(mq.areColumnsUnique(join, ImmutableBitSet.of()))
      assertNull(mq.areColumnsUnique(join, ImmutableBitSet.of(0)))
      assertNull(mq.areColumnsUnique(join, ImmutableBitSet.of(1)))
      assertNull(mq.areColumnsUnique(join, ImmutableBitSet.of(2)))
      assertNull(mq.areColumnsUnique(join, ImmutableBitSet.of(3)))
      assertNull(mq.areColumnsUnique(join, ImmutableBitSet.of(4)))
      assertNull(mq.areColumnsUnique(join, ImmutableBitSet.of(5)))
      assertNull(mq.areColumnsUnique(join, ImmutableBitSet.of(6)))
      assertNull(mq.areColumnsUnique(join, ImmutableBitSet.of(7)))
      assertNull(mq.areColumnsUnique(join, ImmutableBitSet.of(8)))
      assertNull(mq.areColumnsUnique(join, ImmutableBitSet.of(9)))
      assertNull(mq.areColumnsUnique(join, ImmutableBitSet.of(0, 1)))
      assertNull(mq.areColumnsUnique(join, ImmutableBitSet.of(1, 2)))
      assertNull(mq.areColumnsUnique(join, ImmutableBitSet.of(0, 7)))
      assertNull(mq.areColumnsUnique(join, ImmutableBitSet.of(1, 7)))
      assertNull(mq.areColumnsUnique(join, ImmutableBitSet.of(0, 8)))
      assertNull(mq.areColumnsUnique(join, ImmutableBitSet.of(7, 8)))
      assertNull(mq.areColumnsUnique(join, ImmutableBitSet.of(8, 9)))
    }
  }

  @Test
  def testAreColumnsUniqueOnUnion(): Unit = {
    val fieldCnt = logicalUnionAll.getRowType.getFieldCount
    (0 until fieldCnt).foreach { idx =>
      assertFalse(mq.areColumnsUnique(logicalUnionAll, ImmutableBitSet.of(idx)))
    }
    assertFalse(mq.areColumnsUnique(logicalUnionAll, ImmutableBitSet.range(fieldCnt)))

    (0 until fieldCnt).foreach { idx =>
      assertFalse(mq.areColumnsUnique(logicalUnion, ImmutableBitSet.of(idx)))
    }
    assertTrue(mq.areColumnsUnique(logicalUnion, ImmutableBitSet.range(fieldCnt)))
  }

  @Test
  def testAreColumnsUniqueOnIntersect(): Unit = {
    assertFalse(mq.areColumnsUnique(logicalIntersectAll, ImmutableBitSet.of(0)))
    assertTrue(mq.areColumnsUnique(logicalIntersectAll, ImmutableBitSet.of(1)))
    assertFalse(mq.areColumnsUnique(logicalIntersectAll, ImmutableBitSet.of(2)))
    assertTrue(mq.areColumnsUnique(logicalIntersectAll, ImmutableBitSet.of(1, 2)))
    assertFalse(mq.areColumnsUnique(logicalIntersectAll, ImmutableBitSet.of(0, 2)))
    assertTrue(mq.areColumnsUnique(logicalIntersectAll, ImmutableBitSet.of(1, 2)))

    assertFalse(mq.areColumnsUnique(logicalIntersect, ImmutableBitSet.of(0)))
    assertTrue(mq.areColumnsUnique(logicalIntersect, ImmutableBitSet.of(1)))
    assertFalse(mq.areColumnsUnique(logicalIntersect, ImmutableBitSet.of(2)))
    assertTrue(mq.areColumnsUnique(logicalIntersect, ImmutableBitSet.of(1, 2)))
    assertFalse(mq.areColumnsUnique(logicalIntersect, ImmutableBitSet.of(0, 2)))
    assertTrue(mq.areColumnsUnique(logicalIntersect, ImmutableBitSet.of(1, 2)))
    assertTrue(mq.areColumnsUnique(logicalIntersect,
      ImmutableBitSet.range(logicalIntersect.getRowType.getFieldCount)))
  }

  @Test
  def testAreColumnsUniqueOnMinus(): Unit = {
    assertFalse(mq.areColumnsUnique(logicalMinusAll, ImmutableBitSet.of(0)))
    assertTrue(mq.areColumnsUnique(logicalMinusAll, ImmutableBitSet.of(1)))
    assertFalse(mq.areColumnsUnique(logicalMinusAll, ImmutableBitSet.of(2)))
    assertTrue(mq.areColumnsUnique(logicalMinusAll, ImmutableBitSet.of(1, 2)))
    assertFalse(mq.areColumnsUnique(logicalMinusAll, ImmutableBitSet.of(0, 2)))
    assertTrue(mq.areColumnsUnique(logicalMinusAll, ImmutableBitSet.of(1, 2)))

    assertFalse(mq.areColumnsUnique(logicalMinus, ImmutableBitSet.of(0)))
    assertTrue(mq.areColumnsUnique(logicalMinus, ImmutableBitSet.of(1)))
    assertFalse(mq.areColumnsUnique(logicalMinus, ImmutableBitSet.of(2)))
    assertTrue(mq.areColumnsUnique(logicalMinus, ImmutableBitSet.of(1, 2)))
    assertFalse(mq.areColumnsUnique(logicalMinus, ImmutableBitSet.of(0, 2)))
    assertTrue(mq.areColumnsUnique(logicalMinus, ImmutableBitSet.of(1, 2)))
    assertTrue(mq.areColumnsUnique(logicalMinus,
      ImmutableBitSet.range(logicalMinus.getRowType.getFieldCount)))

    // SELECT * FROM MyTable2 MINUS SELECT * MyTable1
    val logicalMinus2: RelNode = relBuilder
      .scan("MyTable2")
      .scan("MyTable1")
      .minus(false).build()
    assertNull(mq.areColumnsUnique(logicalMinus2, ImmutableBitSet.of(0)))
    assertNull(mq.areColumnsUnique(logicalMinus2, ImmutableBitSet.of(1)))
    assertNull(mq.areColumnsUnique(logicalMinus2, ImmutableBitSet.of(2)))
    assertNull(mq.areColumnsUnique(logicalMinus2, ImmutableBitSet.of(1, 2)))
    assertNull(mq.areColumnsUnique(logicalMinus2, ImmutableBitSet.of(0, 2)))
    assertNull(mq.areColumnsUnique(logicalMinus2, ImmutableBitSet.of(1, 2)))
    assertTrue(mq.areColumnsUnique(logicalMinus2,
      ImmutableBitSet.range(logicalMinus2.getRowType.getFieldCount)))
  }

  @Test
  def testGetColumnNullCountOnDefault(): Unit = {
    (0 until testRel.getRowType.getFieldCount).foreach { idx =>
      assertNull(mq.areColumnsUnique(testRel, ImmutableBitSet.of(idx)))
    }
  }

}
