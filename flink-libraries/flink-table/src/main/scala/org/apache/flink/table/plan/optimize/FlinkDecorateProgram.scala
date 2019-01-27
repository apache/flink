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

import org.apache.calcite.plan.RelTrait
import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.rel.RelNode
import org.apache.calcite.tools.RuleSet
import org.apache.flink.table.plan.`trait`.UpdateAsRetractionTrait
import org.apache.flink.table.plan.optimize.HEP_RULES_EXECUTION_TYPE.HEP_RULES_EXECUTION_TYPE

/**
  * A FlinkHepProgram that deals with retraction.
  */
class FlinkDecorateProgram extends FlinkHepRuleSetProgram[StreamOptimizeContext] {

  override def optimize(input: RelNode, context: StreamOptimizeContext): RelNode = {
    val output = if (context.updateAsRetraction()) {
      input.copy(
        input.getTraitSet.plus(new UpdateAsRetractionTrait(true)),
        input.getInputs)
    } else {
      input
    }
    super.optimize(output, context)
  }
}

class FlinkDecorateProgramBuilder {
  private val decorateProgram = new FlinkDecorateProgram

  def setHepRulesExecutionType(
    executionType: HEP_RULES_EXECUTION_TYPE)
  : FlinkDecorateProgramBuilder = {
    decorateProgram.setHepRulesExecutionType(executionType)
    this
  }

  def setHepMatchOrder(matchOrder: HepMatchOrder): FlinkDecorateProgramBuilder = {
    decorateProgram.setHepMatchOrder(matchOrder)
    this
  }

  def setMatchLimit(matchLimit: Int): FlinkDecorateProgramBuilder = {
    decorateProgram.setMatchLimit(matchLimit)
    this
  }

  def add(ruleSet: RuleSet): FlinkDecorateProgramBuilder = {
    decorateProgram.add(ruleSet)
    this
  }

  def setTargetTraits(relTraits: Array[RelTrait]): FlinkDecorateProgramBuilder = {
    decorateProgram.setTargetTraits(relTraits)
    this
  }

  def build(): FlinkDecorateProgram = decorateProgram

}

object FlinkDecorateProgramBuilder {
  def newBuilder = new FlinkDecorateProgramBuilder
}
