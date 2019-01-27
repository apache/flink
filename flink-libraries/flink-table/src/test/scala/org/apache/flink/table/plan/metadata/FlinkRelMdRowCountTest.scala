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

import org.junit.Assert._
import org.junit.Test

class FlinkRelMdRowCountTest extends FlinkRelMdHandlerTestBase {

  @Test
  def testGetRowCountOnSort(): Unit = {
    assertEquals(10D, mq.getRowCount(sort))
  }

  @Test
  def testGetRowCountOnWindowAgg(): Unit = {
    assertEquals(50D, mq.getRowCount(logicalWindowAgg))
    assertEquals(50D, mq.getRowCount(flinkLogicalWindowAgg))
    assertEquals(50D, mq.getRowCount(globalWindowAggWithLocalAgg))
    assertEquals(50D, mq.getRowCount(globalWindowAggWithoutLocalAgg))
  }

  @Test
  def testGetRowCountOnJoin(): Unit = {
    assertEquals(50.0, mq.getRowCount(innerJoin))
    assertEquals(100.0, mq.getRowCount(leftJoin))
    assertEquals(50.0, mq.getRowCount(rightJoin))
    assertEquals(100.0, mq.getRowCount(fullJoin))
    assertEquals(2500.0, mq.getRowCount(innerJoinWithoutEquiCond))
    assertEquals(2500.0, mq.getRowCount(leftJoinWithoutEquiCond))
    assertEquals(2500.0, mq.getRowCount(rightJoinWithoutEquiCond))
    assertEquals(5000.0, mq.getRowCount(fullJoinWithoutEquiCond))
    assertEquals(1.0, mq.getRowCount(innerJoinDisjoint))
    assertEquals(100.0, mq.getRowCount(leftJoinDisjoint))
    assertEquals(80.0, mq.getRowCount(rightJoinDisjoint))
    assertEquals(180.0, mq.getRowCount(fullJoinDisjoint))
    assertEquals(15.0, mq.getRowCount(innerJoinWithNonEquiCond))
    assertEquals(100.0, mq.getRowCount(leftJoinWithNonEquiCond))
    assertEquals(100.0, mq.getRowCount(rightJoinWithNonEquiCond))
    assertEquals(185.0, mq.getRowCount(fullJoinWithNonEquiCond))
  }

  @Test
  def testGetRowCountOnLocalAgg(): Unit = {
    assertEquals(5.0, mq.getRowCount(unSplittableLocalAgg))
    assertEquals(50.0, mq.getRowCount(unSplittableLocalAgg2))
    assertEquals(3.0, mq.getRowCount(splittableLocalAgg))
    assertEquals((1 - Math.pow(1 - 1.0 / 134, 4)) * 134 * 512 * 512 * 128,
                 mq.getRowCount(localAggOnBigTable))
  }

  @Test
  def testGetRowCountOnLocalWindowAgg(): Unit = {
    assertEquals(50.0, mq.getRowCount(localWindowAgg))
    assertEquals(512 * 512 * 512D, mq.getRowCount(logicalWindowAggOnBigTimeTable))
  }

  @Test
  def testGetRowCountOnRank(): Unit = {
    assertEquals(50.0, mq.getRowCount(flinkLogicalRank))
    assertEquals(50.0, mq.getRowCount(flinkLogicalRankWithVariableRankRange))
    assertEquals(3.0, mq.getRowCount(flinkLogicalRowNumber))
    assertEquals(50.0, mq.getRowCount(flinkLogicalRowNumberWithOutput))

    assertEquals(50.0, mq.getRowCount(localBatchExecRank))
    assertEquals(50.0, mq.getRowCount(globalBatchExecRank))

    assertEquals(3.0, mq.getRowCount(streamExecRowNumber))
  }
}
