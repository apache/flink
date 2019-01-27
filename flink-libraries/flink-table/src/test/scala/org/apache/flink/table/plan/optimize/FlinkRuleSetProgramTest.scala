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

package org.apache.flink.table.plan.optimize

import org.apache.calcite.rel.rules._
import org.apache.calcite.tools.RuleSets
import org.junit.Assert._
import org.junit.Test

/**
  * Tests for [[FlinkRuleSetProgram]].
  */
class FlinkRuleSetProgramTest {

  @Test
  def testRulesOperations(): Unit = {

    val program = FlinkHepRuleSetProgramBuilder.newBuilder
      .add(RuleSets.ofList(
        ReduceExpressionsRule.FILTER_INSTANCE,
        ReduceExpressionsRule.PROJECT_INSTANCE,
        ReduceExpressionsRule.CALC_INSTANCE,
        ReduceExpressionsRule.JOIN_INSTANCE
      )).build()

    assertTrue(program.contains(ReduceExpressionsRule.FILTER_INSTANCE))
    assertTrue(program.contains(ReduceExpressionsRule.PROJECT_INSTANCE))
    assertTrue(program.contains(ReduceExpressionsRule.CALC_INSTANCE))
    assertTrue(program.contains(ReduceExpressionsRule.JOIN_INSTANCE))
    assertFalse(program.contains(SubQueryRemoveRule.FILTER))

    program.remove(RuleSets.ofList(
      ReduceExpressionsRule.FILTER_INSTANCE,
      ReduceExpressionsRule.PROJECT_INSTANCE))
    assertFalse(program.contains(ReduceExpressionsRule.FILTER_INSTANCE))
    assertFalse(program.contains(ReduceExpressionsRule.PROJECT_INSTANCE))
    assertTrue(program.contains(ReduceExpressionsRule.CALC_INSTANCE))
    assertTrue(program.contains(ReduceExpressionsRule.JOIN_INSTANCE))

    program.replaceAll(RuleSets.ofList(SubQueryRemoveRule.FILTER))
    assertFalse(program.contains(ReduceExpressionsRule.CALC_INSTANCE))
    assertFalse(program.contains(ReduceExpressionsRule.JOIN_INSTANCE))
    assertTrue(program.contains(SubQueryRemoveRule.FILTER))

    program.add(RuleSets.ofList(
      SubQueryRemoveRule.PROJECT,
      SubQueryRemoveRule.JOIN))
    assertTrue(program.contains(SubQueryRemoveRule.FILTER))
    assertTrue(program.contains(SubQueryRemoveRule.PROJECT))
    assertTrue(program.contains(SubQueryRemoveRule.JOIN))
  }

  @Test(expected = classOf[NullPointerException])
  def testIllegalOperations_NullRuleSets(): Unit = {
    FlinkHepRuleSetProgramBuilder.newBuilder.add(null)
  }

}
