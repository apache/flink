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

import org.apache.flink.table.plan.util.FlinkRelMdUtil

import org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConversions._

class FlinkRelMdRowCountTest extends FlinkRelMdHandlerTestBase {

  @Test
  def testGetRowCountOnTableScan(): Unit = {
    Array(studentLogicalScan, studentBatchScan, studentStreamScan).foreach { scan =>
      assertEquals(50.0, mq.getRowCount(scan))
    }
    Array(empLogicalScan, empBatchScan, empStreamScan).foreach { scan =>
      assertEquals(1e8, mq.getRowCount(scan))
    }
  }

  @Test
  def testGetRowCountOnValues(): Unit = {
    assertEquals(4.0, mq.getRowCount(logicalValues))
    assertEquals(1.0, mq.getRowCount(emptyValues))
  }

  @Test
  def testGetRowCountOnProject(): Unit = {
    assertEquals(50.0, mq.getRowCount(logicalProject))
  }

  @Test
  def testGetRowCountOnFilter(): Unit = {
    assertEquals(25.0, mq.getRowCount(logicalFilter))
  }

  @Test
  def testGetRowCountOnCalc(): Unit = {
    assertEquals(25.0, mq.getRowCount(logicalCalc))
  }

  @Test
  def testGetRowCountOnExpand(): Unit = {
    Array(logicalExpand, flinkLogicalExpand, batchExpand, streamExpand).foreach {
      expand => assertEquals(150.0, mq.getRowCount(expand))
    }
  }

  @Test
  def testGetRowCountOnExchange(): Unit = {
    Array(batchExchange, streamExchange).foreach {
      exchange => assertEquals(50.0, mq.getRowCount(exchange))
    }
  }

  @Test
  def testGetRowCountOnRank(): Unit = {
    Array(logicalRank, flinkLogicalRank, batchLocalRank, streamRank).foreach {
      rank => assertEquals(5.0, mq.getRowCount(rank))
    }
    // TODO FLINK-12282
    assertEquals(1.0, mq.getRowCount(batchGlobalRank))

    Array(logicalRank2, flinkLogicalRank2, batchLocalRank2, streamRank2).foreach {
      rank =>
        assertEquals(7.0 * FlinkRelMdUtil.getRankRangeNdv(rank.rankRange),
          mq.getRowCount(rank))
    }
    assertEquals(21.0, mq.getRowCount(batchGlobalRank2))

    Array(logicalRowNumber, flinkLogicalRowNumber, streamRowNumber).foreach {
      rank => assertEquals(4.0, mq.getRowCount(rank))
    }

    Array(logicalRankWithVariableRange, flinkLogicalRankWithVariableRange,
      streamRankWithVariableRange).foreach {
      rank => assertEquals(5.0, mq.getRowCount(rank))
    }
  }

  @Test
  def testGetRowCountOnSort(): Unit = {
    Array(logicalSort, flinkLogicalSort, batchSort, streamSort).foreach { sort =>
      assertEquals(50.0, mq.getRowCount(sort))
    }

    Array(logicalSortLimit, flinkLogicalSortLimit, batchSortLimit, streamSortLimit,
      batchGlobalSortLimit, logicalLimit, flinkLogicalLimit, batchLimit, batchGlobalLimit,
      streamLimit).foreach { sort =>
      assertEquals(20.0, mq.getRowCount(sort))
    }

    Array(batchLocalSortLimit, batchLocalLimit).foreach { sort =>
      assertEquals(30.0, mq.getRowCount(sort))
    }
  }

  @Test
  def testGetRowCountOnAggregate(): Unit = {
    Array(logicalAgg, flinkLogicalAgg, batchGlobalAggWithLocal, batchGlobalAggWithoutLocal,
      batchLocalAgg).foreach {
      agg => assertEquals(7.0, mq.getRowCount(agg))
    }

    // TODO re-check this
    Array(streamGlobalAggWithLocal, streamGlobalAggWithoutLocal).foreach {
      agg => assertEquals(50.0, mq.getRowCount(agg))
    }

    Array(logicalAggWithAuxGroup, flinkLogicalAggWithAuxGroup,
      batchGlobalAggWithoutLocalWithAuxGroup, batchGlobalAggWithLocalWithAuxGroup,
      batchLocalAggWithAuxGroup).foreach {
      agg => assertEquals(50.0, mq.getRowCount(agg))
    }
  }

  @Test
  def testGetRowCountOnOverWindow(): Unit = {
    Array(flinkLogicalOverWindow, batchOverWindowAgg).foreach { agg =>
      assertEquals(50.0, mq.getRowCount(agg))
    }
  }

  @Test
  def testGetRowCountOnJoin(): Unit = {
    assertEquals(50.0, mq.getRowCount(logicalInnerJoinOnUniqueKeys))
    assertEquals(8.0E8, mq.getRowCount(logicalInnerJoinNotOnUniqueKeys))
    assertEquals(2.0E7, mq.getRowCount(logicalInnerJoinOnRHSUniqueKeys))
    assertEquals(1.0E7, mq.getRowCount(logicalInnerJoinWithEquiAndNonEquiCond))
    assertEquals(8.0E15, mq.getRowCount(logicalInnerJoinWithoutEquiCond))
    assertEquals(1.0, mq.getRowCount(logicalInnerJoinOnDisjointKeys))

    assertEquals(8.0E8, mq.getRowCount(logicalLeftJoinOnUniqueKeys))
    assertEquals(8.0E8, mq.getRowCount(logicalLeftJoinNotOnUniqueKeys))
    assertEquals(8.0E8, mq.getRowCount(logicalLeftJoinOnLHSUniqueKeys))
    assertEquals(2.0E7, mq.getRowCount(logicalLeftJoinOnRHSUniqueKeys))
    assertEquals(8.0E8, mq.getRowCount(logicalLeftJoinWithEquiAndNonEquiCond))
    assertEquals(8.0E15, mq.getRowCount(logicalLeftJoinWithoutEquiCond))
    assertEquals(8.0E8, mq.getRowCount(logicalLeftJoinOnDisjointKeys))

    assertEquals(50.0, mq.getRowCount(logicalRightJoinOnUniqueKeys))
    assertEquals(8.0E8, mq.getRowCount(logicalRightJoinNotOnUniqueKeys))
    assertEquals(2.0E7, mq.getRowCount(logicalRightJoinOnLHSUniqueKeys))
    assertEquals(8.0E8, mq.getRowCount(logicalRightJoinOnRHSUniqueKeys))
    assertEquals(2.0E7, mq.getRowCount(logicalRightJoinWithEquiAndNonEquiCond))
    assertEquals(8.0E15, mq.getRowCount(logicalRightJoinWithoutEquiCond))
    assertEquals(2.0E7, mq.getRowCount(logicalRightJoinOnDisjointKeys))

    assertEquals(8.0E8, mq.getRowCount(logicalFullJoinOnUniqueKeys))
    assertEquals(8.0E8, mq.getRowCount(logicalFullJoinNotOnUniqueKeys))
    assertEquals(8.0E8, mq.getRowCount(logicalFullJoinOnRHSUniqueKeys))
    assertEquals(8.1E8, mq.getRowCount(logicalFullJoinWithEquiAndNonEquiCond))
    assertEquals(8.0E15, mq.getRowCount(logicalFullJoinWithoutEquiCond))
    assertEquals(8.2E8, mq.getRowCount(logicalFullJoinOnDisjointKeys))
  }

  @Test
  def testGetRowCountOnOverUnion(): Unit = {
    assertEquals(8.2E8, mq.getRowCount(logicalUnion))
    assertEquals(8.2E8, mq.getRowCount(logicalUnionAll))
  }

  @Test
  def testGetRowCountOnOverIntersect(): Unit = {
    assertEquals(2.0E7, mq.getRowCount(logicalIntersect))
    assertEquals(2.0E7, mq.getRowCount(logicalIntersectAll))
  }

  @Test
  def testGetRowCountOnOverMinus(): Unit = {
    assertEquals(2.0E7, mq.getRowCount(logicalMinus))
    assertEquals(2.0E7, mq.getRowCount(logicalMinusAll))
  }

  @Test
  def testGetRowCountOnOverDefault(): Unit = {
    assertEquals(50.0, mq.getRowCount(testRel))
  }

}
