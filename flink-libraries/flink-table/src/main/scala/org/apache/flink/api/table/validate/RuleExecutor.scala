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
package org.apache.flink.api.table.validate

import grizzled.slf4j.Logger

import org.apache.flink.api.table.trees.TreeNode

abstract class Rule[A <: TreeNode[_]] {
  val ruleName: String = getClass.getSimpleName

  def apply(plan: A): A
}

abstract class RuleExecutor[A <: TreeNode[_]] {

  val log = Logger(getClass)

  /**
    * An execution strategy for rules that indicates the maximum number of executions. If the
    * execution reaches fix point (i.e. converge) before maxIterations, it will stop.
    */
  abstract class Strategy { def maxIterations: Int }

  /** A strategy that only runs once. */
  case object Once extends Strategy { val maxIterations = 1 }

  /** A strategy that runs until fix point or maxIterations times, whichever comes first. */
  case class FixedPoint(maxIterations: Int) extends Strategy

  /** A batch of rules. */
  protected case class Batch(name: String, strategy: Strategy, rules: Rule[A]*)

  /** Defines a sequence of rule batches, to be overridden by the implementation. */
  protected val batches: Seq[Batch]


  /**
    * Executes the batches of rules defined by the subclass. The batches are executed serially
    * using the defined execution strategy. Within each batch, rules are also executed serially.
    */
  def execute(plan: A): A = {
    var curPlan = plan

    batches.foreach { batch =>
      val batchStartPlan = curPlan
      var iteration = 1
      var lastPlan = curPlan
      var continue = true

      // Run until fix point (or the max number of iterations as specified in the strategy.
      while (continue) {
        curPlan = batch.rules.foldLeft(curPlan) {
          case (plan, rule) =>
            val result = rule(plan)

            if (!result.fastEquals(plan)) {
              log.debug(
                s"""
                   |=== Applying Rule ${rule.ruleName} ===
                   | Origin:
                   | ${plan.treeString}
                   | After Rule:
                   | ${result.treeString}
                """.stripMargin)
            }

            result
        }
        iteration += 1
        if (iteration > batch.strategy.maxIterations) {
          // Only log if this is a rule that is supposed to run more than once.
          if (iteration != 2) {
            log.debug(s"Max iterations (${iteration - 1}) reached for batch ${batch.name}")
          }
          continue = false
        }

        if (curPlan.fastEquals(lastPlan)) {
          log.debug(
            s"Fixed point reached for batch ${batch.name} after ${iteration - 1} iterations.")
          continue = false
        }
        lastPlan = curPlan
      }

      if (!batchStartPlan.fastEquals(curPlan)) {
        log.debug(
          s"""
             |=== Result of Batch ${batch.name} ===
             | Origin:
             | ${plan.treeString}
             | After Rule:
             | ${curPlan.treeString}
        """.stripMargin)
      } else {
        log.debug(s"Batch ${batch.name} has no effect.")
      }
    }

    curPlan
  }
}
