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

import org.apache.flink.table.api.{TableException, ValidationException}

import org.apache.calcite.plan.Convention
import org.apache.calcite.plan.RelOptPlanner.CannotPlanException
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.rules._
import org.apache.calcite.tools.RuleSets
import org.assertj.core.api.Assertions.{assertThat, assertThatThrownBy}
import org.junit.jupiter.api.Test

/** Tests for [[FlinkVolcanoProgramTest]]. */
class FlinkVolcanoProgramTest {

  @Test
  def testBuildFlinkVolcanoProgram(): Unit = {
    val TEST = new Convention.Impl("TEST", classOf[RelNode])
    FlinkVolcanoProgramBuilder.newBuilder
      .add(
        RuleSets.ofList(
          CoreRules.FILTER_REDUCE_EXPRESSIONS,
          CoreRules.PROJECT_REDUCE_EXPRESSIONS,
          CoreRules.CALC_REDUCE_EXPRESSIONS,
          CoreRules.JOIN_REDUCE_EXPRESSIONS
        ))
      .setRequiredOutputTraits(Array(TEST))
      .build()
  }

  @Test
  def testNullRequiredOutputTraits(): Unit = {
    assertThatThrownBy(() => FlinkVolcanoProgramBuilder.newBuilder.setRequiredOutputTraits(null))
      .isInstanceOf(classOf[NullPointerException])
  }

  @Test
  def testUnwrapRuleExceptionReturnsFlinkExceptionsAsIs(): Unit = {
    val program = FlinkVolcanoProgramBuilder.newBuilder.build()
    val tableException = new TableException("rejected by rule")
    val validationException = new ValidationException("invalid for rule")

    assertThat(program.unwrapRuleException(tableException)).isSameAs(tableException)
    assertThat(program.unwrapRuleException(validationException)).isSameAs(validationException)
  }

  @Test
  def testUnwrapRuleExceptionLooksThroughCalciteWrappers(): Unit = {
    val program = FlinkVolcanoProgramBuilder.newBuilder.build()
    val cause = new ValidationException("invalid for rule")
    val wrappedOnce = new RuntimeException("Error while applying rule", cause)
    val wrappedTwice = new RuntimeException("Error occurred while applying rule", wrappedOnce)

    assertThat(program.unwrapRuleException(wrappedOnce)).isSameAs(cause)
    assertThat(program.unwrapRuleException(wrappedTwice)).isSameAs(cause)
  }

  @Test
  def testUnwrapRuleExceptionKeepsOtherExceptionsWrapped(): Unit = {
    val program = FlinkVolcanoProgramBuilder.newBuilder.build()
    val wrappedBug = new RuntimeException("Error while applying rule", new NullPointerException())
    val wrappedCannotPlan =
      new RuntimeException("Error while applying rule", new CannotPlanException("no plan"))
    val subclassWrapper = new IllegalStateException("wrapped", new TableException("rejected"))
    val withoutCause = new RuntimeException("Error while applying rule")

    assertThat(program.unwrapRuleException(wrappedBug)).isSameAs(wrappedBug)
    assertThat(program.unwrapRuleException(wrappedCannotPlan)).isSameAs(wrappedCannotPlan)
    assertThat(program.unwrapRuleException(subclassWrapper)).isSameAs(subclassWrapper)
    assertThat(program.unwrapRuleException(withoutCause)).isSameAs(withoutCause)
  }

}
