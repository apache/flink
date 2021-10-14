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

import org.apache.flink.table.planner.plan.optimize.program.HEP_RULES_EXECUTION_TYPE.HEP_RULES_EXECUTION_TYPE
import org.apache.flink.util.Preconditions

import org.apache.calcite.plan.RelTrait
import org.apache.calcite.plan.hep.{HepMatchOrder, HepPlanner, HepProgramBuilder}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.tools.RuleSet

import scala.collection.JavaConversions._

/**
  * A FlinkRuleSetProgram that runs with [[HepPlanner]].
  *
  * <p>In most case this program could meet our requirements, otherwise we could choose
  * [[FlinkHepProgram]] for some advanced features.
  *
  * <p>Currently, default hep execution type is [[HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE]].
  * (Please refer to [[HEP_RULES_EXECUTION_TYPE]] for more info about execution types)
  *
  * @tparam OC OptimizeContext
  */
class FlinkHepRuleSetProgram[OC <: FlinkOptimizeContext] extends FlinkRuleSetProgram[OC] {

  /**
    * The order of graph traversal when looking for rule matches,
    * default match order is ARBITRARY.
    */
  private var matchOrder: HepMatchOrder = HepMatchOrder.ARBITRARY

  /**
    * The limit of pattern matches for this program,
    * default match limit is Integer.MAX_VALUE.
    */
  private var matchLimit: Int = Integer.MAX_VALUE

  /**
    * Hep rule execution type. This is a required item,
    * default execution type is RULE_SEQUENCE.
    */
  private var executionType: HEP_RULES_EXECUTION_TYPE = HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE

  /**
    * Requested root traits, this is an optional item.
    */
  private var requestedRootTraits: Option[Array[RelTrait]] = None

  override def optimize(input: RelNode, context: OC): RelNode = {
    if (rules.isEmpty) {
      return input
    }

    // build HepProgram
    val builder = new HepProgramBuilder
    builder.addMatchOrder(matchOrder)
    builder.addMatchLimit(matchLimit)
    executionType match {
      case HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE =>
        rules.foreach(builder.addRuleInstance)
      case HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION =>
        builder.addRuleCollection(rules)
      case _ =>
        throw new RuntimeException(s"Unsupported HEP_RULES_EXECUTION_TYPE: $executionType")
    }

    // optimize with HepProgram
    val flinkHepProgram = FlinkHepProgram[OC](builder.build(), requestedRootTraits)
    flinkHepProgram.optimize(input, context)
  }

  /**
    * Sets rules match order.
    */
  def setHepMatchOrder(matchOrder: HepMatchOrder): Unit = {
    this.matchOrder = Preconditions.checkNotNull(matchOrder)
  }

  /**
    * Sets the limit of pattern matches.
    */
  def setMatchLimit(matchLimit: Int): Unit = {
    Preconditions.checkArgument(matchLimit > 0)
    this.matchLimit = matchLimit
  }

  /**
    * Sets hep rule execution type.
    */
  def setHepRulesExecutionType(executionType: HEP_RULES_EXECUTION_TYPE): Unit = {
    this.executionType = Preconditions.checkNotNull(executionType)
  }

  /**
    * Sets requested root traits.
    */
  def setRequestedRootTraits(relTraits: Array[RelTrait]): Unit = {
    requestedRootTraits = Option.apply(relTraits)
  }
}

/**
  * An enumeration of hep rule execution type, to tell the [[HepPlanner]]
  * how exactly execute the rules.
  */
object HEP_RULES_EXECUTION_TYPE extends Enumeration {
  type HEP_RULES_EXECUTION_TYPE = Value

  /**
    * Rules in RULE_SEQUENCE type are executed with RuleInstance.
    * RuleInstance is an instruction that matches a specific rule, each rule in the rule
    * collection is associated with one RuleInstance. Each RuleInstance will be executed
    * only once according to the order defined by the rule collection, but a rule may be applied
    * more than once. If arbitrary order is needed, use RULE_COLLECTION instead.
    *
    * Please refer to [[HepProgramBuilder#addRuleInstance]] for more info about RuleInstance.
    */
  val RULE_SEQUENCE: HEP_RULES_EXECUTION_TYPE.Value = Value

  /**
    * Rules in RULE_COLLECTION type are executed with RuleCollection.
    * RuleCollection is an instruction that matches any rules in a given collection.
    * The order in which the rules within a collection will be attempted is arbitrary,
    * so if more control is needed, use RULE_SEQUENCE instead.
    *
    * Please refer to [[HepProgramBuilder#addRuleCollection]] for more info about RuleCollection.
    */
  val RULE_COLLECTION: HEP_RULES_EXECUTION_TYPE.Value = Value
}

class FlinkHepRuleSetProgramBuilder[OC <: FlinkOptimizeContext] {
  private val hepRuleSetProgram = new FlinkHepRuleSetProgram[OC]

  def setHepRulesExecutionType(
      executionType: HEP_RULES_EXECUTION_TYPE): FlinkHepRuleSetProgramBuilder[OC] = {
    hepRuleSetProgram.setHepRulesExecutionType(executionType)
    this
  }

  /**
    * Sets rules match order.
    */
  def setHepMatchOrder(matchOrder: HepMatchOrder): FlinkHepRuleSetProgramBuilder[OC] = {
    hepRuleSetProgram.setHepMatchOrder(matchOrder)
    this
  }

  /**
    * Sets the limit of pattern matches.
    */
  def setMatchLimit(matchLimit: Int): FlinkHepRuleSetProgramBuilder[OC] = {
    hepRuleSetProgram.setMatchLimit(matchLimit)
    this
  }

  /**
    * Adds rules for this program.
    */
  def add(ruleSet: RuleSet): FlinkHepRuleSetProgramBuilder[OC] = {
    hepRuleSetProgram.add(ruleSet)
    this
  }

  /**
    * Sets requested root traits.
    */
  def setRequestedRootTraits(relTraits: Array[RelTrait]): FlinkHepRuleSetProgramBuilder[OC] = {
    hepRuleSetProgram.setRequestedRootTraits(relTraits)
    this
  }

  def build(): FlinkHepRuleSetProgram[OC] = hepRuleSetProgram

}

object FlinkHepRuleSetProgramBuilder {
  def newBuilder[OC <: FlinkOptimizeContext] = new FlinkHepRuleSetProgramBuilder[OC]
}
