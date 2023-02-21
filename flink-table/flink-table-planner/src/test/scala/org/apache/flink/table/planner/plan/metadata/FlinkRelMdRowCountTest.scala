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

import org.apache.flink.table.planner.plan.nodes.calcite.LogicalWindowAggregate
import org.apache.flink.table.planner.plan.utils.FlinkRelMdUtil

import com.google.common.collect.Lists
import org.apache.calcite.rel.RelCollations
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.sql.fun.SqlCountAggFunction
import org.apache.calcite.util.ImmutableBitSet
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConversions._

class FlinkRelMdRowCountTest extends FlinkRelMdHandlerTestBase {

  @Test
  def testGetRowCountOnTableScan(): Unit = {
    Array(studentLogicalScan, studentBatchScan, studentStreamScan).foreach {
      scan => assertEquals(50.0, mq.getRowCount(scan))
    }
    Array(empLogicalScan, empBatchScan, empStreamScan).foreach {
      scan => assertEquals(1e8, mq.getRowCount(scan))
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
        assertEquals(7.0 * FlinkRelMdUtil.getRankRangeNdv(rank.rankRange), mq.getRowCount(rank))
    }
    assertEquals(21.0, mq.getRowCount(batchGlobalRank2))

    Array(logicalRowNumber, flinkLogicalRowNumber, streamRowNumber).foreach {
      rank => assertEquals(4.0, mq.getRowCount(rank))
    }

    Array(
      logicalRankWithVariableRange,
      flinkLogicalRankWithVariableRange,
      streamRankWithVariableRange).foreach(rank => assertEquals(5.0, mq.getRowCount(rank)))
  }

  @Test
  def testGetRowCountOnSort(): Unit = {
    Array(logicalSort, flinkLogicalSort, batchSort, streamSort).foreach {
      sort => assertEquals(50.0, mq.getRowCount(sort))
    }

    Array(
      logicalSortLimit,
      flinkLogicalSortLimit,
      batchSortLimit,
      streamSortLimit,
      batchGlobalSortLimit,
      logicalLimit,
      flinkLogicalLimit,
      batchLimit,
      batchGlobalLimit,
      streamLimit
    ).foreach(sort => assertEquals(20.0, mq.getRowCount(sort)))

    Array(batchLocalSortLimit, batchLocalLimit).foreach {
      sort => assertEquals(30.0, mq.getRowCount(sort))
    }
  }

  @Test
  def testGetRowCountOnAggregate(): Unit = {
    Array(
      logicalAgg,
      flinkLogicalAgg,
      batchGlobalAggWithLocal,
      batchGlobalAggWithoutLocal,
      batchLocalAgg).foreach(agg => assertEquals(7.0, mq.getRowCount(agg)))

    // TODO re-check this
    Array(streamGlobalAggWithLocal, streamGlobalAggWithoutLocal).foreach {
      agg => assertEquals(50.0, mq.getRowCount(agg))
    }

    Array(
      logicalAggWithAuxGroup,
      flinkLogicalAggWithAuxGroup,
      batchGlobalAggWithoutLocalWithAuxGroup,
      batchGlobalAggWithLocalWithAuxGroup,
      batchLocalAggWithAuxGroup
    ).foreach(agg => assertEquals(50.0, mq.getRowCount(agg)))
  }

  @Test
  def testGetRowCountOnWindowAgg(): Unit = {
    Array(
      logicalWindowAgg,
      flinkLogicalWindowAgg,
      batchLocalWindowAgg,
      batchGlobalWindowAggWithoutLocalAgg,
      batchGlobalWindowAggWithLocalAgg,
      streamWindowAgg).foreach(agg => assertEquals(50d, mq.getRowCount(agg)))

    Array(
      logicalWindowAggWithAuxGroup,
      flinkLogicalWindowAggWithAuxGroup,
      batchLocalWindowAggWithAuxGroup,
      batchGlobalWindowAggWithoutLocalAggWithAuxGroup,
      batchGlobalWindowAggWithLocalAggWithAuxGroup
    ).foreach(agg => assertEquals(50d, mq.getRowCount(agg)))

    relBuilder.clear()
    val ts = relBuilder.scan("TemporalTable3").peek()
    val aggCallOfWindowAgg = Lists.newArrayList(
      AggregateCall.create(
        new SqlCountAggFunction("COUNT"),
        false,
        false,
        false,
        List[Integer](3),
        -1,
        null,
        RelCollations.EMPTY,
        2,
        ts,
        null,
        "s"))
    val windowAgg = new LogicalWindowAggregate(
      ts.getCluster,
      ts.getTraitSet,
      ts,
      ImmutableBitSet.of(0, 1),
      aggCallOfWindowAgg,
      tumblingGroupWindow,
      namedPropertiesOfWindowAgg)
    assertEquals(4000000000d, mq.getRowCount(windowAgg))
  }

  @Test
  def testGetRowCountOnOverAgg(): Unit = {
    Array(flinkLogicalOverAgg, batchOverAgg).foreach {
      agg => assertEquals(50.0, mq.getRowCount(agg))
    }
  }

  @Test
  def testGetRowCountOnJoin(): Unit = {
    assertEquals(50.0, mq.getRowCount(logicalInnerJoinOnUniqueKeys))
    assertEquals(8.0e8, mq.getRowCount(logicalInnerJoinNotOnUniqueKeys))
    assertEquals(2.0e7, mq.getRowCount(logicalInnerJoinOnRHSUniqueKeys))
    assertEquals(1.0e7, mq.getRowCount(logicalInnerJoinWithEquiAndNonEquiCond))
    assertEquals(8.0e15, mq.getRowCount(logicalInnerJoinWithoutEquiCond))
    assertEquals(1.0, mq.getRowCount(logicalInnerJoinOnDisjointKeys))

    assertEquals(8.0e8, mq.getRowCount(logicalLeftJoinOnUniqueKeys))
    assertEquals(8.0e8, mq.getRowCount(logicalLeftJoinNotOnUniqueKeys))
    assertEquals(8.0e8, mq.getRowCount(logicalLeftJoinOnLHSUniqueKeys))
    assertEquals(2.0e7, mq.getRowCount(logicalLeftJoinOnRHSUniqueKeys))
    assertEquals(8.0e8, mq.getRowCount(logicalLeftJoinWithEquiAndNonEquiCond))
    assertEquals(8.0e15, mq.getRowCount(logicalLeftJoinWithoutEquiCond))
    assertEquals(8.0e8, mq.getRowCount(logicalLeftJoinOnDisjointKeys))

    assertEquals(50.0, mq.getRowCount(logicalRightJoinOnUniqueKeys))
    assertEquals(8.0e8, mq.getRowCount(logicalRightJoinNotOnUniqueKeys))
    assertEquals(2.0e7, mq.getRowCount(logicalRightJoinOnLHSUniqueKeys))
    assertEquals(8.0e8, mq.getRowCount(logicalRightJoinOnRHSUniqueKeys))
    assertEquals(2.0e7, mq.getRowCount(logicalRightJoinWithEquiAndNonEquiCond))
    assertEquals(8.0e15, mq.getRowCount(logicalRightJoinWithoutEquiCond))
    assertEquals(2.0e7, mq.getRowCount(logicalRightJoinOnDisjointKeys))

    assertEquals(8.0e8, mq.getRowCount(logicalFullJoinOnUniqueKeys))
    assertEquals(8.0e8, mq.getRowCount(logicalFullJoinNotOnUniqueKeys))
    assertEquals(8.0e8, mq.getRowCount(logicalFullJoinOnLHSUniqueKeys))
    assertEquals(8.0e8, mq.getRowCount(logicalFullJoinOnRHSUniqueKeys))
    assertEquals(8.1e8, mq.getRowCount(logicalFullJoinWithEquiAndNonEquiCond))
    assertEquals(8.0e15, mq.getRowCount(logicalFullJoinWithoutEquiCond))
    assertEquals(8.2e8, mq.getRowCount(logicalFullJoinOnDisjointKeys))

    assertEquals(50.0, mq.getRowCount(logicalSemiJoinOnUniqueKeys))
    assertEquals(8.0e8, mq.getRowCount(logicalSemiJoinNotOnUniqueKeys))
    assertEquals(2556.0, mq.getRowCount(logicalSemiJoinOnLHSUniqueKeys))
    assertEquals(2.0e7, mq.getRowCount(logicalSemiJoinOnRHSUniqueKeys))
    assertEquals(1278.0, mq.getRowCount(logicalSemiJoinWithEquiAndNonEquiCond))
    assertEquals(4.0e8, mq.getRowCount(logicalSemiJoinWithoutEquiCond))
    assertEquals(8.0e8, mq.getRowCount(logicalSemiJoinOnDisjointKeys))

    assertEquals(7.9999995e8, mq.getRowCount(logicalAntiJoinOnUniqueKeys))
    assertEquals(8.0e7, mq.getRowCount(logicalAntiJoinNotOnUniqueKeys))
    assertEquals(7.99997444e8, mq.getRowCount(logicalAntiJoinOnLHSUniqueKeys))
    assertEquals(2000000.0, mq.getRowCount(logicalAntiJoinOnRHSUniqueKeys))
    assertEquals(6.0e8, mq.getRowCount(logicalAntiJoinWithEquiAndNonEquiCond))
    assertEquals(6.0e8, mq.getRowCount(logicalAntiJoinWithoutEquiCond))
    assertEquals(8.0e7, mq.getRowCount(logicalAntiJoinOnDisjointKeys))
  }

  @Test
  def testGetRowCountOnOverUnion(): Unit = {
    assertEquals(8.2e8, mq.getRowCount(logicalUnion))
    assertEquals(8.2e8, mq.getRowCount(logicalUnionAll))
  }

  @Test
  def testGetRowCountOnOverIntersect(): Unit = {
    assertEquals(2.0e7, mq.getRowCount(logicalIntersect))
    assertEquals(2.0e7, mq.getRowCount(logicalIntersectAll))
  }

  @Test
  def testGetRowCountOnOverMinus(): Unit = {
    assertEquals(2.0e7, mq.getRowCount(logicalMinus))
    assertEquals(2.0e7, mq.getRowCount(logicalMinusAll))
  }

  @Test
  def testGetRowCountOnOverDefault(): Unit = {
    assertEquals(50.0, mq.getRowCount(testRel))
  }

  @Test
  def testGetRowCountOnWindowTableFunction(): Unit = {
    Array(batchTumbleWindowTVFRel, streamTumbleWindowTVFRel).foreach {
      agg => assertEquals(50d, mq.getRowCount(agg))
    }
    Array(
      batchHopWindowTVFRel,
      batchCumulateWindowTVFRel,
      streamHopWindowTVFRel,
      streamCumulateWindowTVFRel).foreach(agg => assertEquals(300d, mq.getRowCount(agg)))
  }
}
