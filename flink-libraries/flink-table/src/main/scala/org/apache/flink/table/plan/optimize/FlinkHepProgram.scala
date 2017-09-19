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
import org.apache.calcite.plan.hep.{HepMatchOrder, HepPlanner, HepProgramBuilder}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.tools.RuleSet
import org.apache.flink.util.Preconditions

/**
  * A FlinkRuleSetProgram that runs with [[HepPlanner]].
  *
  * @tparam OC OptimizeContext
  */
class FlinkHepProgram[OC <: OptimizeContext] extends FlinkRuleSetProgram[OC] {

  private var matchOrder: Option[HepMatchOrder] = None
  private var matchLimit: Option[Int] = None

  override def optimize(input: RelNode, context: OC): RelNode = {
    if (rules.isEmpty) {
      return input
    }

    val builder = new HepProgramBuilder
    if (matchOrder.isDefined) {
      builder.addMatchOrder(matchOrder.get)
    }

    if (matchLimit.isDefined) {
      Preconditions.checkArgument(matchLimit.get > 0)
      builder.addMatchLimit(matchLimit.get)
    }

    rules.foreach(builder.addRuleInstance)

    val planner = new HepPlanner(builder.build, context.getContext)
    planner.setRoot(input)

    if (targetTraits.nonEmpty) {
      val targetTraitSet = input.getTraitSet.plusAll(targetTraits)
      if (!input.getTraitSet.equals(targetTraitSet)) {
        planner.changeTraits(input, targetTraitSet.simplify)
      }
    }

    planner.findBestExp
  }

  def setHepMatchOrder(matchOrder: HepMatchOrder): Unit = {
    this.matchOrder = Option(matchOrder)
  }

  def setMatchLimit(matchLimit: Int): Unit = {
    this.matchLimit = Option(matchLimit)
  }
}

class FlinkHepProgramBuilder[OC <: OptimizeContext] {
  private val hepProgram = new FlinkHepProgram[OC]

  def setHepMatchOrder(matchOrder: HepMatchOrder): FlinkHepProgramBuilder[OC] = {
    hepProgram.setHepMatchOrder(matchOrder)
    this
  }

  def setMatchLimit(matchLimit: Int): FlinkHepProgramBuilder[OC] = {
    hepProgram.setMatchLimit(matchLimit)
    this
  }

  def add(ruleSet: RuleSet): FlinkHepProgramBuilder[OC] = {
    hepProgram.add(ruleSet)
    this
  }

  def setTargetTraits(relTraits: Array[RelTrait]): FlinkHepProgramBuilder[OC] = {
    hepProgram.setTargetTraits(relTraits)
    this
  }

  def build(): FlinkHepProgram[OC] = {
    hepProgram
  }
}

object FlinkHepProgramBuilder {
  def newBuilder[OC <: OptimizeContext] = new FlinkHepProgramBuilder[OC]
}
