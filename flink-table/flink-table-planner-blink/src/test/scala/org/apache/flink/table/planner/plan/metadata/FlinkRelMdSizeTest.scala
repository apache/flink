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

import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.JavaConversions._

class FlinkRelMdSizeTest extends FlinkRelMdHandlerTestBase {

  @Test
  def testAverageRowSizeOnTableScan(): Unit = {
    Array(studentLogicalScan, studentFlinkLogicalScan, studentBatchScan, studentStreamScan)
      .foreach { scan =>
        assertEquals(40.2, mq.getAverageRowSize(scan))
      }

    Array(empLogicalScan, empFlinkLogicalScan, empBatchScan, empStreamScan).foreach { scan =>
      assertEquals(64.0, mq.getAverageRowSize(scan))
    }
  }

  @Test
  def testAverageRowSizeOnDefault(): Unit = {
    assertEquals(56.0, mq.getAverageRowSize(testRel))
  }

  @Test
  def testAverageColumnSizeOnTableScan(): Unit = {
    Array(studentLogicalScan, studentFlinkLogicalScan, studentBatchScan, studentStreamScan)
      .foreach { scan =>
        assertEquals(Seq(8.0, 7.2, 8.0, 4.0, 8.0, 1.0, 4.0), mq.getAverageColumnSizes(scan).toList)
      }

    Array(empLogicalScan, empBatchScan, empStreamScan).foreach { scan =>
      assertEquals(Seq(4.0, 12.0, 12.0, 4.0, 12.0, 8.0, 8.0, 4.0),
        mq.getAverageColumnSizes(scan).toList)
    }
  }

  @Test
  def testAverageColumnSizeOnValues(): Unit = {
    assertEquals(Seq(6.25, 1.0, 9.25, 12.0, 9.25, 8.0, 1.0, 3.75),
      mq.getAverageColumnSizes(logicalValues).toList)
    assertEquals(Seq(8.0, 1.0, 12.0, 12.0, 12.0, 8.0, 4.0, 12.0),
      mq.getAverageColumnSizes(emptyValues).toList)
  }

  @Test
  def testAverageColumnSizeOnProject(): Unit = {
    assertEquals(Seq(8.0, 7.2, 8.0, 4.0, 8.0, 8.0, 8.0, 4.0, 1.0, 8.0, 8.0, 8.0),
      mq.getAverageColumnSizes(logicalProject).toList)
  }

  @Test
  def testAverageColumnSizeOnFilter(): Unit = {
    assertEquals(Seq(8.0, 7.2, 8.0, 4.0, 8.0, 1.0, 4.0),
      mq.getAverageColumnSizes(logicalFilter).toList)
  }

  @Test
  def testAverageColumnSizeOnCalc(): Unit = {
    assertEquals(Seq(8.0, 7.2, 8.0, 4.0, 8.0, 8.0, 8.0, 4.0, 1.0, 8.0, 8.0, 8.0),
      mq.getAverageColumnSizes(logicalCalc).toList)
  }

  @Test
  def testAverageColumnSizeOnExpand(): Unit = {
    Array(logicalExpand, flinkLogicalExpand, batchExpand, streamExpand).foreach {
      expand =>
        assertEquals(Seq(8.0, 7.2, 8.0, 4.0, 8.0, 1.0, 4.0, 8.0),
          mq.getAverageColumnSizes(expand).toList)
    }
  }

  @Test
  def testAverageColumnSizeOnExchange(): Unit = {
    Array(batchExchange, streamExchange).foreach { exchange =>
      assertEquals(Seq(8.0, 7.2, 8.0, 4.0, 8.0, 1.0, 4.0),
        mq.getAverageColumnSizes(exchange).toList)
    }
  }

  @Test
  def testAverageColumnSizeOnRank(): Unit = {
    Array(logicalRank, flinkLogicalRank, batchGlobalRank, streamRank).foreach { rank =>
      assertEquals(Seq(8.0, 7.2, 8.0, 4.0, 8.0, 1.0, 4.0, 8.0),
        mq.getAverageColumnSizes(rank).toList)
    }
    assertEquals(Seq(8.0, 7.2, 8.0, 4.0, 8.0, 1.0, 4.0),
      mq.getAverageColumnSizes(batchLocalRank).toList)
  }

  @Test
  def testAverageColumnSizeOnSort(): Unit = {
    Array(logicalSort, flinkLogicalSort, batchSort, streamSort,
      logicalSortLimit, flinkLogicalSortLimit, batchSortLimit, streamSortLimit,
      batchGlobalSortLimit,
      batchLocalSortLimit, logicalLimit, flinkLogicalLimit, batchLimit, streamLimit).foreach {
      sort =>
        assertEquals(Seq(8.0, 7.2, 8.0, 4.0, 8.0, 1.0, 4.0),
          mq.getAverageColumnSizes(sort).toList)
    }
  }

  @Test
  def testAverageColumnSizeOnAggregate(): Unit = {
    Array(logicalAgg, flinkLogicalAgg, batchGlobalAggWithLocal, batchGlobalAggWithoutLocal,
      streamGlobalAggWithLocal, streamGlobalAggWithoutLocal).foreach { agg =>
      assertEquals(Seq(4.0, 8.0, 8.0, 8.0, 8.0, 8.0), mq.getAverageColumnSizes(agg).toList)
    }

    Array(logicalAggWithAuxGroup, flinkLogicalAggWithAuxGroup,
      batchGlobalAggWithLocalWithAuxGroup, batchGlobalAggWithoutLocalWithAuxGroup).foreach {
      agg =>
        assertEquals(Seq(8.0, 7.2, 8.0, 8.0, 8.0, 8.0), mq.getAverageColumnSizes(agg).toList)
    }
  }

  @Test
  def testAverageColumnSizeOnWindowAgg(): Unit = {
    Array(logicalWindowAgg, flinkLogicalWindowAgg, batchGlobalWindowAggWithoutLocalAgg,
      batchGlobalWindowAggWithLocalAgg).foreach { agg =>
      assertEquals(Seq(4D, 32D, 8D, 12D, 12D, 12D, 12D), mq.getAverageColumnSizes(agg).toSeq)
    }
    assertEquals(Seq(4.0, 32.0, 8.0, 8.0),
      mq.getAverageColumnSizes(batchLocalWindowAgg).toSeq)

    Array(logicalWindowAggWithAuxGroup, flinkLogicalWindowAggWithAuxGroup,
      batchGlobalWindowAggWithoutLocalAggWithAuxGroup,
      batchGlobalWindowAggWithLocalAggWithAuxGroup).foreach { agg =>
      assertEquals(Seq(8D, 4D, 8D, 12D, 12D, 12D, 12D), mq.getAverageColumnSizes(agg).toSeq)
    }
    assertEquals(Seq(8D, 8D, 4D, 8D),
      mq.getAverageColumnSizes(batchLocalWindowAggWithAuxGroup).toSeq)
  }

  @Test
  def testAverageColumnSizeOnOverAgg(): Unit = {
    Array(flinkLogicalOverAgg, batchOverAgg).foreach { agg =>
      assertEquals(Seq(8.0, 7.2, 8.0, 4.0, 4.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0),
        mq.getAverageColumnSizes(agg).toList)
    }
    assertEquals(Seq(8.0, 12.0, 8.0, 4.0, 4.0, 8.0, 8.0, 8.0),
      mq.getAverageColumnSizes(streamOverAgg).toList)
  }

  @Test
  def testAverageColumnSizeOnJoin(): Unit = {
    assertEquals(Seq(4.0, 8.0, 12.0, 88.8, 4.0, 8.0, 8.0, 4.0, 8.0),
      mq.getAverageColumnSizes(logicalInnerJoinOnUniqueKeys).toList)
    Array(logicalInnerJoinOnDisjointKeys, logicalLeftJoinNotOnUniqueKeys,
      logicalRightJoinOnLHSUniqueKeys, logicalFullJoinWithoutEquiCond).foreach { join =>
      assertEquals(Seq(4.0, 8.0, 12.0, 88.8, 4.0, 4.0, 8.0, 12.0, 10.52, 4.0),
        mq.getAverageColumnSizes(join).toList)
    }

    Array(logicalSemiJoinOnUniqueKeys, logicalAntiJoinNotOnUniqueKeys).foreach { join =>
      assertEquals(Seq(4.0, 8.0, 12.0, 88.8, 4.0),
        mq.getAverageColumnSizes(join).toList)
    }
  }

  @Test
  def testAverageColumnSizeOnUnion(): Unit = {
    Array(logicalUnion, logicalUnionAll).foreach { union =>
      assertEquals(Seq(4.0, 8.0, 12.0, 49.66, 4.0),
        mq.getAverageColumnSizes(union).toList)
    }
  }

  @Test
  def testAverageColumnSizeOnIntersect(): Unit = {
    Array(logicalIntersect, logicalIntersectAll).foreach { union =>
      assertEquals(Seq(4.0, 8.0, 12.0, 88.8, 4.0), mq.getAverageColumnSizes(union).toList)
    }
  }

  @Test
  def testAverageColumnSizeOnMinus(): Unit = {
    Array(logicalMinus, logicalMinusAll).foreach { union =>
      assertEquals(Seq(4.0, 8.0, 12.0, 88.8, 4.0), mq.getAverageColumnSizes(union).toList)
    }
  }

  @Test
  def testAverageColumnSizeOnDefault(): Unit = {
    assertEquals(Seq(8.0, 12.0, 8.0, 4.0, 8.0, 12.0, 4.0),
      mq.getAverageColumnSizes(testRel).toList)
  }

}
