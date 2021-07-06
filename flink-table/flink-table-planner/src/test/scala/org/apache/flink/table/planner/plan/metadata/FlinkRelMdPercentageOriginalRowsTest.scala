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

import org.junit.Assert._
import org.junit.Test

class FlinkRelMdPercentageOriginalRowsTest extends FlinkRelMdHandlerTestBase {

  @Test
  def testGetPercentageOriginalRowsOnTableScan(): Unit = {
    Array(studentLogicalScan, studentBatchScan, studentStreamScan).foreach { scan =>
      assertEquals(1.0, mq.getPercentageOriginalRows(scan))
    }

    Array(empLogicalScan, empBatchScan, empStreamScan).foreach { scan =>
      assertEquals(1.0, mq.getPercentageOriginalRows(scan))
    }
  }

  @Test
  def testGetPercentageOriginalRowsOnExpand(): Unit = {
    Array(logicalExpand, flinkLogicalExpand, batchExpand, streamExpand).foreach { expand =>
      assertEquals(1.0, mq.getPercentageOriginalRows(expand))
    }
  }

  @Test
  def testGetPercentageOriginalRowsOnRank(): Unit = {
    Array(logicalRank, flinkLogicalRank, batchLocalRank, batchGlobalRank, streamRank,
      logicalRowNumber, flinkLogicalRowNumber, streamRowNumber).foreach { rank =>
      assertEquals(1.0, mq.getPercentageOriginalRows(rank))
    }
  }

  @Test
  def testGetPercentageOriginalRowsOnAggregate(): Unit = {
    Array(logicalAgg, flinkLogicalAgg, batchGlobalAggWithLocal, batchGlobalAggWithoutLocal,
      streamGlobalAggWithLocal, streamGlobalAggWithoutLocal, logicalAggWithAuxGroup,
      flinkLogicalAggWithAuxGroup, batchGlobalAggWithLocalWithAuxGroup,
      batchGlobalAggWithoutLocalWithAuxGroup).foreach { agg =>
      assertEquals(1.0, mq.getPercentageOriginalRows(agg))
    }
  }

  @Test
  def testGetPercentageOriginalRowsOnJoin(): Unit = {
    assertEquals(1.0, mq.getPercentageOriginalRows(logicalInnerJoinOnUniqueKeys))
    assertEquals(1.0, mq.getPercentageOriginalRows(logicalInnerJoinNotOnUniqueKeys))
    assertEquals(1.0, mq.getPercentageOriginalRows(logicalLeftJoinWithEquiAndNonEquiCond))
    assertEquals(1.0, mq.getPercentageOriginalRows(logicalLeftJoinWithoutEquiCond))
    assertEquals(1.0, mq.getPercentageOriginalRows(logicalRightJoinOnLHSUniqueKeys))
    assertEquals(1.0, mq.getPercentageOriginalRows(logicalRightJoinOnDisjointKeys))
    assertEquals(1.0, mq.getPercentageOriginalRows(logicalFullJoinOnUniqueKeys))
    assertEquals(1.0, mq.getPercentageOriginalRows(logicalFullJoinNotOnUniqueKeys))
    assertEquals(1.0, mq.getPercentageOriginalRows(logicalSemiJoinOnUniqueKeys))
    assertEquals(1.0, mq.getPercentageOriginalRows(logicalSemiJoinNotOnUniqueKeys))
    assertEquals(1.0, mq.getPercentageOriginalRows(logicalAntiJoinOnUniqueKeys))
    assertEquals(1.0, mq.getPercentageOriginalRows(logicalAntiJoinNotOnUniqueKeys))
  }

  @Test
  def testGetPercentageOriginalRowsOnUnion(): Unit = {
    assertEquals(1.0, mq.getPercentageOriginalRows(logicalUnion))
    assertEquals(1.0, mq.getPercentageOriginalRows(logicalUnionAll))
  }

  @Test
  def testGetPercentageOriginalRowsOnDefault(): Unit = {
    assertEquals(1.0, mq.getPercentageOriginalRows(testRel))
  }
}
