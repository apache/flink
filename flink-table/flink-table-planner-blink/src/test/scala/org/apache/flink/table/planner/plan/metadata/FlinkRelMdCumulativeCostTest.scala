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

import org.apache.flink.table.planner.plan.cost.FlinkCost

import org.junit.Assert._
import org.junit.Test

class FlinkRelMdCumulativeCostTest extends FlinkRelMdHandlerTestBase {

  @Test
  def testGetCumulativeCostOnTableScan(): Unit = {
    assertTrue(FlinkCost.FACTORY.makeCost(50.0, 51.0, 0.0, 0.0, 0.0).equals(
      mq.getCumulativeCost(studentLogicalScan)))

    Array(studentBatchScan, studentStreamScan).foreach { scan =>
      val expectedCost = FlinkCost.FACTORY.makeCost(50.0, 50.0, 50.0 * 40.2, 0.0, 0.0)
      assertTrue(expectedCost.equals(mq.getCumulativeCost(scan)))
    }

    assertTrue(FlinkCost.FACTORY.makeCost(1.0E8, 1.00000001E8, 0.0, 0.0, 0.0).equals(
      mq.getCumulativeCost(empLogicalScan)))
    Array(empBatchScan, empStreamScan).foreach { scan =>
      val expectedCost = FlinkCost.FACTORY.makeCost(1.0E8, 1.0E8, 1.0E8 * 64.0, 0.0, 0.0)
      assertTrue(expectedCost.equals(mq.getCumulativeCost(scan)))
    }
  }

  @Test
  def testGetCumulativeCostOnDefault(): Unit = {
    val expectedCost = FlinkCost.FACTORY.makeCost(51.0, 52.0, 1.0, 0.0, 0.0)
    assertTrue(expectedCost.equals(mq.getCumulativeCost(testRel)))
  }

}
