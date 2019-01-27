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
import org.apache.flink.table.plan.optimize.HEP_RULES_EXECUTION_TYPE.HEP_RULES_EXECUTION_TYPE
import org.apache.flink.util.Preconditions

import scala.collection.JavaConverters._

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
class FlinkHepRuleSetProgram[OC <: OptimizeContext] extends FlinkRuleSetProgram[OC] {

  private var matchOrder: Option[HepMatchOrder] = None
  private var matchLimit: Option[Int] = None
  // default execution type is RULE_SEQUENCE
  private var executionType: HEP_RULES_EXECUTION_TYPE = HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE

  override def optimize(input: RelNode, context: OC): RelNode = {
    if (rules.isEmpty) {
      return input
    }

    // build HepProgram
    val builder = new HepProgramBuilder
    matchOrder.foreach(builder.addMatchOrder)
    matchLimit.foreach(builder.addMatchLimit)
    executionType match {
      case HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE =>
        rules.foreach(builder.addRuleInstance)
      case HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION =>
        builder.addRuleCollection(rules.asJava)
      case _ =>
        throw new RuntimeException(s"Unsupported HEP_RULES_EXECUTION_TYPE: $executionType")
    }

    // optimize with HepProgram
    val flinkHepProgram = FlinkHepProgram[OC](builder.build(), targetTraits)
    flinkHepProgram.optimize(input, context)
  }

  def setHepMatchOrder(matchOrder: HepMatchOrder): Unit = {
    Preconditions.checkNotNull(matchOrder)
    this.matchOrder = Some(matchOrder)
  }

  def setMatchLimit(matchLimit: Int): Unit = {
    Preconditions.checkArgument(matchLimit > 0)
    this.matchLimit = Some(matchLimit)
  }

  def setHepRulesExecutionType(executionType: HEP_RULES_EXECUTION_TYPE): Unit = {
    Preconditions.checkNotNull(executionType)
    this.executionType = executionType
  }
}

/**
  * An enumeration of hep rule execution type, usable to tell the [[HepPlanner]] how exactly
  * execute the rules.
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
  val RULE_SEQUENCE = Value

  /**
    * Rules in RULE_COLLECTION type are executed with RuleCollection.
    * RuleCollection is an instruction that matches any rules in a given collection.
    * The order in which the rules within a collection will be attempted is arbitrary,
    * so if more control is needed, use RULE_SEQUENCE instead.
    *
    * Please refer to [[HepProgramBuilder#addRuleCollection]] for more info about RuleCollection.
    */
  val RULE_COLLECTION = Value
}

class FlinkHepRuleSetProgramBuilder[OC <: OptimizeContext] {
  private val flinkHepProgram = new FlinkHepRuleSetProgram[OC]

  def setHepRulesExecutionType(
    executionType: HEP_RULES_EXECUTION_TYPE)
  : FlinkHepRuleSetProgramBuilder[OC] = {
    flinkHepProgram.setHepRulesExecutionType(executionType)
    this
  }

  def setHepMatchOrder(matchOrder: HepMatchOrder): FlinkHepRuleSetProgramBuilder[OC] = {
    flinkHepProgram.setHepMatchOrder(matchOrder)
    this
  }

  def setMatchLimit(matchLimit: Int): FlinkHepRuleSetProgramBuilder[OC] = {
    flinkHepProgram.setMatchLimit(matchLimit)
    this
  }

  def add(ruleSet: RuleSet): FlinkHepRuleSetProgramBuilder[OC] = {
    flinkHepProgram.add(ruleSet)
    this
  }

  def setTargetTraits(relTraits: Array[RelTrait]): FlinkHepRuleSetProgramBuilder[OC] = {
    flinkHepProgram.setTargetTraits(relTraits)
    this
  }

  def build(): FlinkHepRuleSetProgram[OC] = flinkHepProgram

}

object FlinkHepRuleSetProgramBuilder {
  def newBuilder[OC <: OptimizeContext] = new FlinkHepRuleSetProgramBuilder[OC]
}
