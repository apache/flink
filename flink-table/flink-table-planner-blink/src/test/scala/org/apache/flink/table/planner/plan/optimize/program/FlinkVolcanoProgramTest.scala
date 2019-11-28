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

package org.apache.flink.table.planner.plan.optimize.program

import org.apache.calcite.plan.Convention
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.rules._
import org.apache.calcite.tools.RuleSets
import org.junit.Test

/**
  * Tests for [[FlinkVolcanoProgramTest]].
  */
class FlinkVolcanoProgramTest {

  @Test
  def testBuildFlinkVolcanoProgram(): Unit = {
    val TEST = new Convention.Impl("TEST", classOf[RelNode])
    FlinkVolcanoProgramBuilder.newBuilder
      .add(RuleSets.ofList(
        ReduceExpressionsRule.FILTER_INSTANCE,
        ReduceExpressionsRule.PROJECT_INSTANCE,
        ReduceExpressionsRule.CALC_INSTANCE,
        ReduceExpressionsRule.JOIN_INSTANCE
      ))
      .setRequiredOutputTraits(Array(TEST))
      .build()
  }

  @Test(expected = classOf[NullPointerException])
  def testNullRequiredOutputTraits(): Unit = {
    FlinkVolcanoProgramBuilder.newBuilder.setRequiredOutputTraits(null)
  }

}
