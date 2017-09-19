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

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.RelOptPlanner.CannotPlanException
import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.tools.{Programs, RuleSet}
import org.apache.flink.table.api.TableException
import org.apache.flink.util.Preconditions

/**
  * A FlinkRuleSetProgram that runs with [[org.apache.calcite.plan.volcano.VolcanoPlanner]].
  *
  * @tparam OC OptimizeContext
  */
class FlinkVolcanoProgram[OC <: OptimizeContext] extends FlinkRuleSetProgram[OC] {

  override def optimize(input: RelNode, context: OC): RelNode = {
    if (rules.isEmpty) {
      return input
    }

    val planner = Preconditions.checkNotNull(context.getRelOptPlanner)

    val optProgram = Programs.ofRules(rules: _*)

    val targetTraitSet = if (targetTraits.isEmpty) {
      input.getTraitSet
    } else {
      input.getTraitSet.plusAll(targetTraits).simplify()
    }

    val output = try {
      optProgram.run(planner, input, targetTraitSet, ImmutableList.of(), ImmutableList.of())
    } catch {
      case e: CannotPlanException =>
        throw new TableException(
          s"Cannot generate a valid execution plan for the given query: \n\n" +
            s"${RelOptUtil.toString(input)}\n" +
            s"This exception indicates that the query uses an unsupported SQL feature.\n" +
            s"Please check the documentation for the set of currently supported SQL features.")
      case t: TableException =>
        throw new TableException(
          s"Cannot generate a valid execution plan for the given query: \n\n" +
            s"${RelOptUtil.toString(input)}\n" +
            s"${t.msg}\n" +
            s"Please check the documentation for the set of currently supported SQL features.")
      case a: AssertionError =>
        throw a
    }
    output
  }

}

class FlinkVolcanoProgramBuilder[OC <: OptimizeContext] {
  private val volcanoProgram = new FlinkVolcanoProgram[OC]

  def add(ruleSet: RuleSet): FlinkVolcanoProgramBuilder[OC] = {
    volcanoProgram.add(ruleSet)
    this
  }

  def setTargetTraits(relTraits: Array[RelTrait]): FlinkVolcanoProgramBuilder[OC] = {
    volcanoProgram.setTargetTraits(relTraits)
    this
  }

  def build(): FlinkVolcanoProgram[OC] = {
    volcanoProgram
  }
}

object FlinkVolcanoProgramBuilder {
  def newBuilder[OC <: OptimizeContext] = new FlinkVolcanoProgramBuilder[OC]
}
