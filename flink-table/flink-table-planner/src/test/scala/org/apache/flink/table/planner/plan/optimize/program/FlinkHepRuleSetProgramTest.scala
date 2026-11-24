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

import org.apache.flink.table.planner.plan.cost.FlinkCostFactory

import org.apache.calcite.jdbc.JavaTypeFactoryImpl
import org.apache.calcite.plan.{RelOptCluster, RelOptCostFactory, RelOptCostImpl, RelOptRule, RelOptRuleCall}
import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.plan.volcano.VolcanoPlanner
import org.apache.calcite.rel.logical.LogicalValues
import org.apache.calcite.rel.rules._
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.tools.RuleSets
import org.assertj.core.api.Assertions.{assertThat, assertThatThrownBy}
import org.junit.jupiter.api.Assertions.{assertFalse, assertTrue}
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock

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
  def testHepCostFactoryUsesRowCount(): Unit = {
    val context = mock(classOf[FlinkOptimizeContext])
    val planner = new VolcanoPlanner
    val cluster = RelOptCluster.create(planner, new RexBuilder(new JavaTypeFactoryImpl))
    val root = LogicalValues.createOneRow(cluster)

    var costFactory: RelOptCostFactory = null
    val rule = new RelOptRule(
      RelOptRule.operand(classOf[LogicalValues], RelOptRule.none()),
      "CaptureHepCostFactory") {
      override def onMatch(call: RelOptRuleCall): Unit = {
        costFactory = call.getPlanner.getCostFactory
      }
    }
    FlinkHepRuleSetProgramBuilder
      .newBuilder[FlinkOptimizeContext]
      .add(RuleSets.ofList(rule))
      .build()
      .optimize(root, context)

    assertThat(costFactory).isInstanceOf(classOf[FlinkCostFactory])
    val expected = RelOptCostImpl.FACTORY
    val flinkFactory = costFactory.asInstanceOf[FlinkCostFactory]
    assertThat(flinkFactory.makeCost(10, 20, 30, 40, 50)).isEqualTo(expected.makeCost(10, 0, 0))
    assertThat(costFactory.makeCost(10, 20, 30)).isEqualTo(expected.makeCost(10, 0, 0))
    assertThat(costFactory.makeTinyCost()).isEqualTo(expected.makeTinyCost())
    assertThat(costFactory.makeHugeCost()).isEqualTo(expected.makeHugeCost())
    assertThat(costFactory.makeInfiniteCost()).isEqualTo(expected.makeInfiniteCost())
    assertThat(costFactory.makeZeroCost()).isEqualTo(expected.makeZeroCost())
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
