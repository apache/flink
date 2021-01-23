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

import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalRank

import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.util.ImmutableBitSet
import org.junit.Assert._
import org.junit.Test

class FlinkRelMdPopulationSizeTest extends FlinkRelMdHandlerTestBase {

  @Test
  def testGetPopulationSizeOnTableScan(): Unit = {
    Array(studentLogicalScan, studentBatchScan, studentStreamScan).foreach { scan =>
      assertEquals(1.0, mq.getPopulationSize(scan, ImmutableBitSet.of()))
      assertEquals(50.0, mq.getPopulationSize(scan, ImmutableBitSet.of(0)))
      assertEquals(48.0, mq.getPopulationSize(scan, ImmutableBitSet.of(1)))
      assertEquals(20.0, mq.getPopulationSize(scan, ImmutableBitSet.of(2)))
      assertEquals(7.0, mq.getPopulationSize(scan, ImmutableBitSet.of(3)))
      assertEquals(35.0, mq.getPopulationSize(scan, ImmutableBitSet.of(4)))
      assertEquals(2.0, mq.getPopulationSize(scan, ImmutableBitSet.of(5)))
      assertNull(mq.getPopulationSize(scan, ImmutableBitSet.of(6)))
      assertEquals(50.0, mq.getPopulationSize(scan, ImmutableBitSet.of(0, 2)))
      assertEquals(50.0, mq.getPopulationSize(scan, ImmutableBitSet.of(2, 3)))
      assertEquals(14.0, mq.getPopulationSize(scan, ImmutableBitSet.of(3, 5)))
      assertEquals(50.0, mq.getPopulationSize(scan, ImmutableBitSet.of(0, 6)))
    }

    Array(empLogicalScan, empBatchScan, empStreamScan).foreach { scan =>
      assertEquals(1.0, mq.getPopulationSize(scan, ImmutableBitSet.of()))
      assertNull(mq.getPopulationSize(scan, ImmutableBitSet.of(0)))
    }
  }

  @Test
  def testGetPopulationSizeOnValues(): Unit = {
    assertEquals(2.0, mq.getPopulationSize(logicalValues, ImmutableBitSet.of()))
    assertEquals(2.0, mq.getPopulationSize(logicalValues, ImmutableBitSet.of(0)))
    assertEquals(2.0, mq.getPopulationSize(logicalValues, ImmutableBitSet.of(1)))
    assertEquals(2.0, mq.getPopulationSize(logicalValues, ImmutableBitSet.of(0, 1)))

    assertEquals(1.0, mq.getPopulationSize(emptyValues, ImmutableBitSet.of(0)))
    assertEquals(1.0, mq.getPopulationSize(emptyValues, ImmutableBitSet.of(1)))
    assertEquals(1.0, mq.getPopulationSize(emptyValues, ImmutableBitSet.of(0, 1)))
  }

  @Test
  def testGetPopulationSizeOnProject(): Unit = {
    assertEquals(1.0, mq.getPopulationSize(logicalProject, ImmutableBitSet.of()))
    assertEquals(50.0, mq.getPopulationSize(logicalProject, ImmutableBitSet.of(0)))
    assertEquals(48.0, mq.getPopulationSize(logicalProject, ImmutableBitSet.of(1)))
    assertEquals(16.43,
      mq.getPopulationSize(logicalProject, ImmutableBitSet.of(2)), 1e-2)
    assertEquals(6.99,
      mq.getPopulationSize(logicalProject, ImmutableBitSet.of(3)), 1e-2)
    assertEquals(20.37,
      mq.getPopulationSize(logicalProject, ImmutableBitSet.of(4)), 1e-2)
    assertEquals(20.37,
      mq.getPopulationSize(logicalProject, ImmutableBitSet.of(5)), 1e-2)
    assertEquals(35.0, mq.getPopulationSize(logicalProject, ImmutableBitSet.of(6)))
    assertEquals(5.0, mq.getPopulationSize(logicalProject, ImmutableBitSet.of(7)), 1e-2)
    assertEquals(1.0, mq.getPopulationSize(logicalProject, ImmutableBitSet.of(8)))
    assertEquals(1.0, mq.getPopulationSize(logicalProject, ImmutableBitSet.of(9)))
    assertEquals(1.0, mq.getPopulationSize(logicalProject, ImmutableBitSet.of(10)))
    assertEquals(16.43,
      mq.getPopulationSize(logicalProject, ImmutableBitSet.of(11)), 1e-2)

    assertEquals(50.0, mq.getPopulationSize(logicalProject, ImmutableBitSet.of(0, 1)))
    assertEquals(31.24,
      mq.getPopulationSize(logicalProject, ImmutableBitSet.of(1, 8)), 1e-2)
  }

  @Test
  def testGetPopulationSizeOnFilter(): Unit = {
    assertEquals(1.0, mq.getPopulationSize(logicalFilter, ImmutableBitSet.of()))
    assertEquals(50.0, mq.getPopulationSize(logicalFilter, ImmutableBitSet.of(0)))
    assertEquals(48.0, mq.getPopulationSize(logicalFilter, ImmutableBitSet.of(1)))
    assertEquals(20.0, mq.getPopulationSize(logicalFilter, ImmutableBitSet.of(2)))
    assertEquals(7.0, mq.getPopulationSize(logicalFilter, ImmutableBitSet.of(3)))
    assertEquals(35.0, mq.getPopulationSize(logicalFilter, ImmutableBitSet.of(4)))
    assertEquals(2.0, mq.getPopulationSize(logicalFilter, ImmutableBitSet.of(5)))
    assertNull(mq.getPopulationSize(logicalFilter, ImmutableBitSet.of(6)))
    assertEquals(50.0, mq.getPopulationSize(logicalFilter, ImmutableBitSet.of(0, 2)))
    assertEquals(50.0, mq.getPopulationSize(logicalFilter, ImmutableBitSet.of(2, 3)))
    assertEquals(14.0, mq.getPopulationSize(logicalFilter, ImmutableBitSet.of(3, 5)))
    assertEquals(50.0, mq.getPopulationSize(logicalFilter, ImmutableBitSet.of(0, 6)))
  }

  @Test
  def testGetPopulationSizeOnCalc(): Unit = {
    assertEquals(1.0, mq.getPopulationSize(logicalCalc, ImmutableBitSet.of()))
    assertEquals(50.0, mq.getPopulationSize(logicalCalc, ImmutableBitSet.of(0)))
    assertEquals(48.0, mq.getPopulationSize(logicalCalc, ImmutableBitSet.of(1)))
    assertEquals(11.22,
      mq.getPopulationSize(logicalCalc, ImmutableBitSet.of(2)), 1e-2)
    assertEquals(6.67,
      mq.getPopulationSize(logicalCalc, ImmutableBitSet.of(3)), 1e-2)
    assertEquals(12.30,
      mq.getPopulationSize(logicalCalc, ImmutableBitSet.of(4)), 1e-2)
    assertEquals(12.30,
      mq.getPopulationSize(logicalCalc, ImmutableBitSet.of(5)), 1e-2)
    assertEquals(35.0, mq.getPopulationSize(logicalCalc, ImmutableBitSet.of(6)))
    assertEquals(2.5, mq.getPopulationSize(logicalCalc, ImmutableBitSet.of(7)), 1e-2)
    assertEquals(1.0, mq.getPopulationSize(logicalCalc, ImmutableBitSet.of(8)))
    assertEquals(1.0, mq.getPopulationSize(logicalCalc, ImmutableBitSet.of(9)))
    assertEquals(1.0, mq.getPopulationSize(logicalCalc, ImmutableBitSet.of(10)))
    assertEquals(11.22,
      mq.getPopulationSize(logicalCalc, ImmutableBitSet.of(11)), 1e-2)

    assertEquals(50.0, mq.getPopulationSize(logicalCalc, ImmutableBitSet.of(0, 1)))
    assertEquals(19.64,
      mq.getPopulationSize(logicalCalc, ImmutableBitSet.of(1, 8)), 1e-2)
  }

  @Test
  def testGetPopulationSizeOnExpand(): Unit = {
    assertEquals(1.0, mq.getPopulationSize(logicalExpand, ImmutableBitSet.of()))
    assertEquals(50.0, mq.getPopulationSize(logicalExpand, ImmutableBitSet.of(0)))
    assertEquals(48.0, mq.getPopulationSize(logicalExpand, ImmutableBitSet.of(1)))
    assertEquals(20.0, mq.getPopulationSize(logicalExpand, ImmutableBitSet.of(2)))
    assertEquals(7.0, mq.getPopulationSize(logicalExpand, ImmutableBitSet.of(3)))
    assertEquals(35.0, mq.getPopulationSize(logicalExpand, ImmutableBitSet.of(4)))
    assertEquals(2.0, mq.getPopulationSize(logicalExpand, ImmutableBitSet.of(5)))
    assertNull(mq.getPopulationSize(logicalExpand, ImmutableBitSet.of(6)))
    assertEquals(3.0, mq.getPopulationSize(logicalExpand, ImmutableBitSet.of(7)))

    assertEquals(50.0, mq.getPopulationSize(logicalExpand, ImmutableBitSet.of(0, 1)))
    assertEquals(14.0, mq.getPopulationSize(logicalExpand, ImmutableBitSet.of(3, 5)))
  }

  @Test
  def testGetPopulationSizeOnExchange(): Unit = {
    Array(batchExchange, streamExchange).foreach {
      exchange =>
        assertEquals(1.0, mq.getPopulationSize(exchange, ImmutableBitSet.of()))
        assertEquals(50.0, mq.getPopulationSize(exchange, ImmutableBitSet.of(0)))
        assertEquals(48.0, mq.getPopulationSize(exchange, ImmutableBitSet.of(1)))
        assertEquals(20.0, mq.getPopulationSize(exchange, ImmutableBitSet.of(2)))
        assertEquals(7.0, mq.getPopulationSize(exchange, ImmutableBitSet.of(3)))
        assertEquals(35.0, mq.getPopulationSize(exchange, ImmutableBitSet.of(4)))
        assertEquals(2.0, mq.getPopulationSize(exchange, ImmutableBitSet.of(5)))
        assertNull(mq.getPopulationSize(exchange, ImmutableBitSet.of(6)))
        assertEquals(50.0, mq.getPopulationSize(exchange, ImmutableBitSet.of(0, 2)))
        assertEquals(50.0, mq.getPopulationSize(exchange, ImmutableBitSet.of(2, 3)))
        assertEquals(14.0, mq.getPopulationSize(exchange, ImmutableBitSet.of(3, 5)))
        assertEquals(50.0, mq.getPopulationSize(exchange, ImmutableBitSet.of(0, 6)))
    }
  }

  @Test
  def testGetPopulationSizeOnRank(): Unit = {
    Array(logicalRank, flinkLogicalRank, batchLocalRank, batchGlobalRank, streamRank).foreach {
      rank =>
        assertEquals(1.0, mq.getPopulationSize(rank, ImmutableBitSet.of()))
        assertEquals(50.0, mq.getPopulationSize(rank, ImmutableBitSet.of(0)))
        assertEquals(48.0, mq.getPopulationSize(rank, ImmutableBitSet.of(1)))
        assertEquals(20.0, mq.getPopulationSize(rank, ImmutableBitSet.of(2)))
        assertEquals(7.0, mq.getPopulationSize(rank, ImmutableBitSet.of(3)))
        assertEquals(35.0, mq.getPopulationSize(rank, ImmutableBitSet.of(4)))
        assertEquals(2.0, mq.getPopulationSize(rank, ImmutableBitSet.of(5)))
        assertNull(mq.getPopulationSize(rank, ImmutableBitSet.of(6)))
        assertEquals(50.0, mq.getPopulationSize(rank, ImmutableBitSet.of(0, 2)))
        rank match {
          case r: BatchPhysicalRank =>
            // local batch rank does not output rank func
            // TODO re-check this
            if (r.isGlobal) {
              assertEquals(1.0, mq.getPopulationSize(rank, ImmutableBitSet.of(7)))
              assertEquals(1.0, mq.getPopulationSize(rank, ImmutableBitSet.of(7)))
              assertEquals(1.0, mq.getPopulationSize(rank, ImmutableBitSet.of(0, 7)))
              assertEquals(1.0, mq.getPopulationSize(rank, ImmutableBitSet.of(3, 7)))
            }
          case _ =>
            assertEquals(5.0, mq.getPopulationSize(rank, ImmutableBitSet.of(7)))
            assertEquals(5.0, mq.getPopulationSize(rank, ImmutableBitSet.of(0, 7)))
            assertEquals(5.0, mq.getPopulationSize(rank, ImmutableBitSet.of(3, 7)))
        }
    }
  }

  @Test
  def testGetPopulationSizeOnSort(): Unit = {
    Array(logicalSort, flinkLogicalSort, batchSort, streamSort,
      logicalLimit, flinkLogicalLimit, batchLimit, batchLocalLimit, batchGlobalLimit, streamLimit,
      logicalSortLimit, flinkLogicalSortLimit, batchSortLimit, batchLocalSortLimit,
      batchGlobalSortLimit, streamSortLimit).foreach {
      sort =>
        assertEquals(1.0, mq.getPopulationSize(sort, ImmutableBitSet.of()))
        assertEquals(50.0, mq.getPopulationSize(sort, ImmutableBitSet.of(0)))
        assertEquals(48.0, mq.getPopulationSize(sort, ImmutableBitSet.of(1)))
        assertEquals(20.0, mq.getPopulationSize(sort, ImmutableBitSet.of(2)))
        assertEquals(7.0, mq.getPopulationSize(sort, ImmutableBitSet.of(3)))
        assertEquals(35.0, mq.getPopulationSize(sort, ImmutableBitSet.of(4)))
        assertEquals(2.0, mq.getPopulationSize(sort, ImmutableBitSet.of(5)))
        assertNull(mq.getPopulationSize(sort, ImmutableBitSet.of(6)))
        assertEquals(50.0, mq.getPopulationSize(sort, ImmutableBitSet.of(0, 2)))
        assertEquals(50.0, mq.getPopulationSize(sort, ImmutableBitSet.of(2, 3)))
        assertEquals(14.0, mq.getPopulationSize(sort, ImmutableBitSet.of(3, 5)))
        assertEquals(50.0, mq.getPopulationSize(sort, ImmutableBitSet.of(0, 6)))
    }
  }

  @Test
  def testGetPopulationSizeOnAggregate(): Unit = {
    Array(logicalAgg, flinkLogicalAgg, batchGlobalAggWithLocal, batchGlobalAggWithoutLocal,
      batchLocalAgg).foreach { agg =>
      assertEquals(1.0, mq.getPopulationSize(agg, ImmutableBitSet.of()))
      assertEquals(7.0, mq.getPopulationSize(agg, ImmutableBitSet.of(0)))
      assertEquals(2.0, mq.getPopulationSize(agg, ImmutableBitSet.of(1)))
      assertEquals(2.0, mq.getPopulationSize(agg, ImmutableBitSet.of(2)))
      assertEquals(3.5, mq.getPopulationSize(agg, ImmutableBitSet.of(3)))
      assertEquals(3.5, mq.getPopulationSize(agg, ImmutableBitSet.of(4)))
      assertEquals(10.0, mq.getPopulationSize(agg, ImmutableBitSet.of(5)))
      assertEquals(7.0, mq.getPopulationSize(agg, ImmutableBitSet.of(0, 1)))
      assertEquals(7.0, mq.getPopulationSize(agg, ImmutableBitSet.of(0, 5)))
    }
  }

  @Test
  def testGetPopulationSizeOnWindowAgg(): Unit = {
    Array(logicalWindowAgg, flinkLogicalWindowAgg, batchGlobalWindowAggWithoutLocalAgg,
      batchGlobalWindowAggWithLocalAgg).foreach { agg =>
      assertEquals(30D, mq.getPopulationSize(agg, ImmutableBitSet.of(0)))
      assertEquals(5D, mq.getPopulationSize(agg, ImmutableBitSet.of(1)))
      assertEquals(50D, mq.getPopulationSize(agg, ImmutableBitSet.of(0, 1)))
      assertEquals(50D, mq.getPopulationSize(agg, ImmutableBitSet.of(0, 2)))
      assertEquals(null, mq.getPopulationSize(agg, ImmutableBitSet.of(3)))
      assertEquals(null, mq.getPopulationSize(agg, ImmutableBitSet.of(0, 3)))
      assertEquals(null, mq.getPopulationSize(agg, ImmutableBitSet.of(1, 3)))
      assertEquals(null, mq.getPopulationSize(agg, ImmutableBitSet.of(2, 3)))
    }
    assertEquals(30D, mq.getPopulationSize(batchLocalWindowAgg, ImmutableBitSet.of(0)))
    assertEquals(5D, mq.getPopulationSize(batchLocalWindowAgg, ImmutableBitSet.of(1)))
    assertEquals(null, mq.getPopulationSize(batchLocalWindowAgg, ImmutableBitSet.of(2)))
    assertEquals(50D, mq.getPopulationSize(batchLocalWindowAgg, ImmutableBitSet.of(0, 1)))
    assertEquals(null, mq.getPopulationSize(batchLocalWindowAgg, ImmutableBitSet.of(0, 2)))
    assertEquals(10D, mq.getPopulationSize(batchLocalWindowAgg, ImmutableBitSet.of(3)))
    assertEquals(50D, mq.getPopulationSize(batchLocalWindowAgg, ImmutableBitSet.of(0, 3)))
    assertEquals(50D, mq.getPopulationSize(batchLocalWindowAgg, ImmutableBitSet.of(1, 3)))
    assertEquals(null, mq.getPopulationSize(batchLocalWindowAgg, ImmutableBitSet.of(2, 3)))

    Array(logicalWindowAggWithAuxGroup, flinkLogicalWindowAggWithAuxGroup,
      batchGlobalWindowAggWithoutLocalAggWithAuxGroup,
      batchGlobalWindowAggWithLocalAggWithAuxGroup).foreach { agg =>
      assertEquals(50D, mq.getPopulationSize(agg, ImmutableBitSet.of(0)))
      assertEquals(48D, mq.getPopulationSize(agg, ImmutableBitSet.of(1)))
      assertEquals(10D, mq.getPopulationSize(agg, ImmutableBitSet.of(2)))
      assertEquals(null, mq.getPopulationSize(agg, ImmutableBitSet.of(3)))
      assertEquals(50D, mq.getPopulationSize(agg, ImmutableBitSet.of(0, 1)))
      assertEquals(50D, mq.getPopulationSize(agg, ImmutableBitSet.of(0, 1, 2)))
      assertEquals(null, mq.getPopulationSize( agg, ImmutableBitSet.of(0, 1, 3)))
    }
    assertEquals(50D, mq.getPopulationSize(batchLocalWindowAggWithAuxGroup, ImmutableBitSet.of(0)))
    assertNull(mq.getPopulationSize(batchLocalWindowAggWithAuxGroup, ImmutableBitSet.of(1)))
    assertEquals(48D, mq.getPopulationSize(batchLocalWindowAggWithAuxGroup, ImmutableBitSet.of(2)))
    assertEquals(10D, mq.getPopulationSize(batchLocalWindowAggWithAuxGroup, ImmutableBitSet.of(3)))
    assertNull(mq.getPopulationSize(batchLocalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1)))
    assertEquals(50D,
      mq.getPopulationSize(batchLocalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 2)))
    assertNull(mq.getPopulationSize(batchLocalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1, 3)))
  }

  @Test
  def testGetPopulationSizeOnOverAgg(): Unit = {
    Array(flinkLogicalOverAgg, batchOverAgg).foreach { agg =>
      assertEquals(1.0, mq.getPopulationSize(agg, ImmutableBitSet.of()))
      assertEquals(50.0, mq.getPopulationSize(agg, ImmutableBitSet.of(0)))
      assertEquals(48.0, mq.getPopulationSize(agg, ImmutableBitSet.of(1)))
      assertEquals(20.0, mq.getPopulationSize(agg, ImmutableBitSet.of(2)))
      assertEquals(7.0, mq.getPopulationSize(agg, ImmutableBitSet.of(3)))
      (4 until 11).foreach { idx =>
        assertNull(mq.getPopulationSize(agg, ImmutableBitSet.of(idx)))
      }
      assertNull(mq.getPopulationSize(agg, ImmutableBitSet.of(0, 6)))
    }
  }

  @Test
  def testGetPopulationSizeOnJoin(): Unit = {
    assertEquals(1.0, mq.getPopulationSize(logicalInnerJoinOnUniqueKeys, ImmutableBitSet.of()))
    assertEquals(49.999938,
      mq.getPopulationSize(logicalInnerJoinOnUniqueKeys, ImmutableBitSet.of(0)), 1e-6)
    assertEquals(49.999998,
      mq.getPopulationSize(logicalInnerJoinOnUniqueKeys, ImmutableBitSet.of(1)), 1e-6)
    assertEquals(50.0,
      mq.getPopulationSize(logicalInnerJoinOnUniqueKeys, ImmutableBitSet.of(1, 5)), 1e-6)
    assertEquals(49.999991,
      mq.getPopulationSize(logicalInnerJoinOnUniqueKeys, ImmutableBitSet.of(0, 6)), 1e-6)

    assertEquals(1.0, mq.getPopulationSize(logicalLeftJoinNotOnUniqueKeys, ImmutableBitSet.of()))
    assertEquals(2.0E7, mq.getPopulationSize(logicalLeftJoinNotOnUniqueKeys, ImmutableBitSet.of(0)))
    assertEquals(5.0569644545E8,
      mq.getPopulationSize(logicalLeftJoinNotOnUniqueKeys, ImmutableBitSet.of(1)), 1e-2)
    assertEquals(8.0E8,
      mq.getPopulationSize(logicalLeftJoinNotOnUniqueKeys, ImmutableBitSet.of(1, 5)), 1e-2)
    assertEquals(7.9377199253E8,
      mq.getPopulationSize(logicalLeftJoinNotOnUniqueKeys, ImmutableBitSet.of(0, 6)), 1e-2)

    assertEquals(1.0,
      mq.getPopulationSize(logicalRightJoinOnLHSUniqueKeys, ImmutableBitSet.of()))
    assertEquals(1.264241136E7,
      mq.getPopulationSize(logicalRightJoinOnLHSUniqueKeys, ImmutableBitSet.of(0)), 1e-2)
    assertEquals(1.975207027E7,
      mq.getPopulationSize(logicalRightJoinOnLHSUniqueKeys, ImmutableBitSet.of(1)), 1e-2)
    assertEquals(2.0E7,
      mq.getPopulationSize(logicalRightJoinOnLHSUniqueKeys, ImmutableBitSet.of(1, 5)), 1e-2)
    assertEquals(1.999606902E7,
      mq.getPopulationSize(logicalRightJoinOnLHSUniqueKeys, ImmutableBitSet.of(0, 6)), 1e-2)

    assertEquals(1.0, mq.getPopulationSize(logicalFullJoinWithoutEquiCond, ImmutableBitSet.of()))
    assertEquals(2.0E7, mq.getPopulationSize(logicalFullJoinWithoutEquiCond, ImmutableBitSet.of(0)))
    assertEquals(8.0E8, mq.getPopulationSize(logicalFullJoinWithoutEquiCond, ImmutableBitSet.of(1)))
    assertEquals(8.0E15,
      mq.getPopulationSize(logicalFullJoinWithoutEquiCond, ImmutableBitSet.of(1, 5)))
    assertEquals(5.112E10,
      mq.getPopulationSize(logicalFullJoinWithoutEquiCond, ImmutableBitSet.of(0, 6)))

    assertEquals(1.0, mq.getPopulationSize(logicalSemiJoinOnUniqueKeys, ImmutableBitSet.of()))
    assertEquals(2.0E7, mq.getPopulationSize(logicalSemiJoinOnLHSUniqueKeys, ImmutableBitSet.of(0)))
    assertEquals(8.0E8, mq.getPopulationSize(logicalSemiJoinNotOnUniqueKeys, ImmutableBitSet.of(1)))
    assertEquals(8.0E8, mq.getPopulationSize(logicalSemiJoinOnUniqueKeys, ImmutableBitSet.of(0, 1)))
    assertEquals(8.0E8,
      mq.getPopulationSize(logicalSemiJoinNotOnUniqueKeys, ImmutableBitSet.of(0, 2)))

    assertEquals(1.0, mq.getPopulationSize(logicalAntiJoinNotOnUniqueKeys, ImmutableBitSet.of()))
    assertEquals(2.0E7, mq.getPopulationSize(logicalAntiJoinOnUniqueKeys, ImmutableBitSet.of(0)))
    assertEquals(8.0E8, mq.getPopulationSize(logicalAntiJoinOnLHSUniqueKeys, ImmutableBitSet.of(1)))
    assertEquals(8.0E8, mq.getPopulationSize(logicalAntiJoinOnUniqueKeys, ImmutableBitSet.of(0, 1)))
    assertEquals(8.0E8,
      mq.getPopulationSize(logicalAntiJoinNotOnUniqueKeys, ImmutableBitSet.of(0, 2)))
  }

  @Test
  def testGetPopulationSizeOnUnion(): Unit = {
    Array(logicalUnion, logicalUnionAll).foreach { unoin =>
      assertEquals(2.0, mq.getPopulationSize(unoin, ImmutableBitSet.of()))
      assertEquals(4.0E7, mq.getPopulationSize(unoin, ImmutableBitSet.of(0)))
      assertEquals(8.00002556E8, mq.getPopulationSize(unoin, ImmutableBitSet.of(1)))
      assertEquals(2263.0, mq.getPopulationSize(unoin, ImmutableBitSet.of(2)))
      assertEquals(8.2E8, mq.getPopulationSize(unoin, ImmutableBitSet.of(0, 2)))
    }
  }

  @Test
  def testGetPopulationSizeOnDefault(): Unit = {
    assertNull(mq.getPopulationSize(testRel, ImmutableBitSet.of()))
    assertNull(mq.getPopulationSize(testRel, ImmutableBitSet.of(1)))
  }

  @Test
  def testGetPopulationSizeOnLargeDomainSize(): Unit = {
    relBuilder.clear()
    val rel = relBuilder
      .scan("MyTable1")
      .project(
        relBuilder.field(0),
        relBuilder.field(1),
        relBuilder.call(SqlStdOperatorTable.SUBSTRING, relBuilder.field(3), relBuilder.literal(10)))
      .build()
    assertEquals(
      7.999999964933156E8,
      mq.getPopulationSize(rel, ImmutableBitSet.of(0, 1, 2)),
      1e-2)
  }
}
