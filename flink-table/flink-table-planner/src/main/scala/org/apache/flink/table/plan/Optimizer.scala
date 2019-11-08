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

package org.apache.flink.table.plan

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.RelOptPlanner.CannotPlanException
import org.apache.calcite.plan.hep.{HepMatchOrder, HepPlanner, HepProgram, HepProgramBuilder}
import org.apache.calcite.plan.{Context, Convention, RelOptPlanner, RelOptUtil, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.tools.{Programs, RuleSet, RuleSets}
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.api.internal.TableEnvImpl
import org.apache.flink.table.calcite.CalciteConfig
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.rules.FlinkRuleSets
import org.apache.flink.table.planner.PlanningConfigurationBuilder

import scala.collection.JavaConverters._

/**
  * Common functionalities for both [[StreamOptimizer]] and [[BatchOptimizer]]. An [[Optimizer]]
  * should be used to create an optimized tree from a logical input tree.
  * See [[StreamOptimizer.optimize]] and [[BatchOptimizer.optimize]]
  *
  * @param calciteConfig                provider for [[CalciteConfig]]. It is a provider because the
  *                                     [[TableConfig]] in a [[TableEnvImpl]] is mutable.
  * @param planningConfigurationBuilder provider for [[RelOptPlanner]] and [[Context]]
  */
abstract class Optimizer(
  calciteConfig: () => CalciteConfig,
  planningConfigurationBuilder: PlanningConfigurationBuilder) {

  protected def materializedConfig: CalciteConfig = calciteConfig.apply()

  /**
    * Returns the normalization rule set for this optimizer
    * including a custom RuleSet configuration.
    */
  protected def getNormRuleSet: RuleSet = {
    materializedConfig.normRuleSet match {

      case None =>
        getBuiltInNormRuleSet

      case Some(ruleSet) =>
        if (materializedConfig.replacesNormRuleSet) {
          ruleSet
        } else {
          RuleSets.ofList((getBuiltInNormRuleSet.asScala ++ ruleSet.asScala).asJava)
        }
    }
  }

  /**
    * Returns the logical optimization rule set for this optimizer
    * including a custom RuleSet configuration.
    */
  protected def getLogicalOptRuleSet: RuleSet = {
    materializedConfig.logicalOptRuleSet match {

      case None =>
        getBuiltInLogicalOptRuleSet

      case Some(ruleSet) =>
        if (materializedConfig.replacesLogicalOptRuleSet) {
          ruleSet
        } else {
          RuleSets.ofList((getBuiltInLogicalOptRuleSet.asScala ++ ruleSet.asScala).asJava)
        }
    }
  }

  /**
    * Returns the logical rewrite rule set for this optimizer
    * including a custom RuleSet configuration.
    */
  protected def getLogicalRewriteRuleSet: RuleSet = {
    materializedConfig.logicalRewriteRuleSet match {

      case None =>
        getBuiltInLogicalRewriteRuleSet

      case Some(ruleSet) =>
        if (materializedConfig.replacesLogicalRewriteRuleSet) {
          ruleSet
        } else {
          RuleSets.ofList((getBuiltInLogicalRewriteRuleSet.asScala ++ ruleSet.asScala).asJava)
        }
    }
  }

  /**
    * Returns the physical optimization rule set for this optimizer
    * including a custom RuleSet configuration.
    */
  protected def getPhysicalOptRuleSet: RuleSet = {
    materializedConfig.physicalOptRuleSet match {

      case None =>
        getBuiltInPhysicalOptRuleSet

      case Some(ruleSet) =>
        if (materializedConfig.replacesPhysicalOptRuleSet) {
          ruleSet
        } else {
          RuleSets.ofList((getBuiltInPhysicalOptRuleSet.asScala ++ ruleSet.asScala).asJava)
        }
    }
  }

  /**
    * Returns the built-in normalization rules that are defined by the optimizer.
    */
  protected def getBuiltInNormRuleSet: RuleSet

  /**
    * Returns the built-in logical optimization rules that are defined by the optimizer.
    */
  protected def getBuiltInLogicalOptRuleSet: RuleSet = {
    FlinkRuleSets.LOGICAL_OPT_RULES
  }

  /**
    * Returns the built-in logical rewrite rules that are defined by the optimizer.
    */
  protected def getBuiltInLogicalRewriteRuleSet: RuleSet = {
    FlinkRuleSets.LOGICAL_REWRITE_RULES
  }

  /**
    * Returns the built-in physical optimization rules that are defined by the optimizer.
    */
  protected def getBuiltInPhysicalOptRuleSet: RuleSet

  protected def optimizeConvertSubQueries(relNode: RelNode): RelNode = {
    runHepPlannerSequentially(
      HepMatchOrder.BOTTOM_UP,
      FlinkRuleSets.TABLE_SUBQUERY_RULES,
      relNode,
      relNode.getTraitSet)
  }

  protected def optimizeExpandPlan(relNode: RelNode): RelNode = {
    val result = runHepPlannerSimultaneously(
      HepMatchOrder.TOP_DOWN,
      FlinkRuleSets.EXPAND_PLAN_RULES,
      relNode,
      relNode.getTraitSet)

    runHepPlannerSequentially(
      HepMatchOrder.TOP_DOWN,
      FlinkRuleSets.POST_EXPAND_CLEAN_UP_RULES,
      result,
      result.getTraitSet)
  }

  protected def optimizeNormalizeLogicalPlan(relNode: RelNode): RelNode = {
    val normRuleSet = getNormRuleSet
    if (normRuleSet.iterator().hasNext) {
      runHepPlannerSequentially(HepMatchOrder.BOTTOM_UP, normRuleSet, relNode, relNode.getTraitSet)
    } else {
      relNode
    }
  }

  protected def optimizeLogicalRewritePlan(relNode: RelNode): RelNode = {
    val logicalRewriteRuleSet = getLogicalRewriteRuleSet
    if (logicalRewriteRuleSet.iterator().hasNext) {
      runHepPlannerSequentially(
        HepMatchOrder.TOP_DOWN,
        logicalRewriteRuleSet,
        relNode,
        relNode.getTraitSet)
    } else {
      relNode
    }
  }

  protected def optimizeLogicalPlan(relNode: RelNode): RelNode = {
    val logicalOptRuleSet = getLogicalOptRuleSet
    val logicalOutputProps = relNode.getTraitSet.replace(FlinkConventions.LOGICAL).simplify()
    if (logicalOptRuleSet.iterator().hasNext) {
      runVolcanoPlanner(logicalOptRuleSet, relNode, logicalOutputProps)
    } else {
      relNode
    }
  }

  protected def optimizePhysicalPlan(relNode: RelNode, convention: Convention): RelNode = {
    val physicalOptRuleSet = getPhysicalOptRuleSet
    val physicalOutputProps = relNode.getTraitSet.replace(convention).simplify()
    if (physicalOptRuleSet.iterator().hasNext) {
      runVolcanoPlanner(physicalOptRuleSet, relNode, physicalOutputProps)
    } else {
      relNode
    }
  }

  /**
    * run HEP planner with rules applied one by one. First apply one rule to all of the nodes
    * and only then apply the next rule. If a rule creates a new node preceding rules will not
    * be applied to the newly created node.
    */
  protected def runHepPlannerSequentially(
    hepMatchOrder: HepMatchOrder,
    ruleSet: RuleSet,
    input: RelNode,
    targetTraits: RelTraitSet): RelNode = {

    val builder = new HepProgramBuilder
    builder.addMatchOrder(hepMatchOrder)

    val it = ruleSet.iterator()
    while (it.hasNext) {
      builder.addRuleInstance(it.next())
    }
    runHepPlanner(builder.build(), input, targetTraits)
  }

  /**
    * run HEP planner with rules applied simultaneously. Apply all of the rules to the given
    * node before going to the next one. If a rule creates a new node all of the rules will
    * be applied to this new node.
    */
  protected def runHepPlannerSimultaneously(
    hepMatchOrder: HepMatchOrder,
    ruleSet: RuleSet,
    input: RelNode,
    targetTraits: RelTraitSet): RelNode = {

    val builder = new HepProgramBuilder
    builder.addMatchOrder(hepMatchOrder)

    builder.addRuleCollection(ruleSet.asScala.toList.asJava)
    runHepPlanner(builder.build(), input, targetTraits)
  }

  /**
    * run HEP planner
    */
  protected def runHepPlanner(
    hepProgram: HepProgram,
    input: RelNode,
    targetTraits: RelTraitSet): RelNode = {

    val planner = new HepPlanner(hepProgram, planningConfigurationBuilder.getContext)
    planner.setRoot(input)
    if (input.getTraitSet != targetTraits) {
      planner.changeTraits(input, targetTraits.simplify)
    }
    planner.findBestExp
  }

  /**
    * run VOLCANO planner
    */
  protected def runVolcanoPlanner(
    ruleSet: RuleSet,
    input: RelNode,
    targetTraits: RelTraitSet): RelNode = {
    val optProgram = Programs.ofRules(ruleSet)

    val output = try {
      optProgram.run(planningConfigurationBuilder.getPlanner, input, targetTraits,
        ImmutableList.of(), ImmutableList.of())
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
            s"${t.getMessage}\n" +
            s"Please check the documentation for the set of currently supported SQL features.")
      case a: AssertionError =>
        // keep original exception stack for caller
        throw a
    }
    output
  }

}
