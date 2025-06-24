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

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.rel.rules._
import org.apache.calcite.tools.RuleSets
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Assertions.{assertFalse, assertTrue}
import org.junit.jupiter.api.Test

/** Tests for [[FlinkHepRuleSetProgram]]. */
class FlinkHepRuleSetProgramTest {

  @Test
  def testBuildFlinkHepRuleSetProgram(): Unit = {
    FlinkHepRuleSetProgramBuilder.newBuilder
      .add(
        RuleSets.ofList(
          CoreRules.FILTER_REDUCE_EXPRESSIONS,
          CoreRules.PROJECT_REDUCE_EXPRESSIONS,
          CoreRules.CALC_REDUCE_EXPRESSIONS,
          CoreRules.JOIN_REDUCE_EXPRESSIONS
        ))
      .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
      .setMatchLimit(10)
      .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
      .build()
  }

  @Test
  def testMatchLimitLessThan1(): Unit = {
    assertThatThrownBy(() => FlinkHepRuleSetProgramBuilder.newBuilder.setMatchLimit(0))
      .isInstanceOf(classOf[IllegalArgumentException])
  }

  @Test
  def testNullHepMatchOrder(): Unit = {
    assertThatThrownBy(() => FlinkHepRuleSetProgramBuilder.newBuilder.setHepMatchOrder(null))
      .isInstanceOf(classOf[NullPointerException])
  }

  @Test
  def testNullHepRulesExecutionType(): Unit = {
    assertThatThrownBy(
      () => FlinkHepRuleSetProgramBuilder.newBuilder.setHepRulesExecutionType(null))
      .isInstanceOf(classOf[NullPointerException])
  }

  @Test
  def testRuleOperations(): Unit = {
    val program = FlinkHepRuleSetProgramBuilder.newBuilder
      .add(
        RuleSets.ofList(
          CoreRules.FILTER_REDUCE_EXPRESSIONS,
          CoreRules.PROJECT_REDUCE_EXPRESSIONS,
          CoreRules.CALC_REDUCE_EXPRESSIONS,
          CoreRules.JOIN_REDUCE_EXPRESSIONS
        ))
      .build()

    assertTrue(program.contains(CoreRules.FILTER_REDUCE_EXPRESSIONS))
    assertTrue(program.contains(CoreRules.PROJECT_REDUCE_EXPRESSIONS))
    assertTrue(program.contains(CoreRules.CALC_REDUCE_EXPRESSIONS))
    assertTrue(program.contains(CoreRules.JOIN_REDUCE_EXPRESSIONS))
    assertFalse(program.contains(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE))

    program.remove(
      RuleSets.ofList(CoreRules.FILTER_REDUCE_EXPRESSIONS, CoreRules.PROJECT_REDUCE_EXPRESSIONS))
    assertFalse(program.contains(CoreRules.FILTER_REDUCE_EXPRESSIONS))
    assertFalse(program.contains(CoreRules.PROJECT_REDUCE_EXPRESSIONS))
    assertTrue(program.contains(CoreRules.CALC_REDUCE_EXPRESSIONS))
    assertTrue(program.contains(CoreRules.JOIN_REDUCE_EXPRESSIONS))

    program.replaceAll(RuleSets.ofList(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE))
    assertFalse(program.contains(CoreRules.CALC_REDUCE_EXPRESSIONS))
    assertFalse(program.contains(CoreRules.JOIN_REDUCE_EXPRESSIONS))
    assertTrue(program.contains(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE))

    program.add(
      RuleSets
        .ofList(CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE, CoreRules.JOIN_SUB_QUERY_TO_CORRELATE))
    assertTrue(program.contains(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE))
    assertTrue(program.contains(CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE))
    assertTrue(program.contains(CoreRules.JOIN_SUB_QUERY_TO_CORRELATE))
  }

  @Test
  def testNullRuleSets(): Unit = {
    assertThatThrownBy(() => FlinkHepRuleSetProgramBuilder.newBuilder.add(null))
      .isInstanceOf(classOf[NullPointerException])
  }
}
