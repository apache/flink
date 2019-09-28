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

import org.apache.flink.util.Preconditions

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.tools.RuleSet

import java.util

import scala.collection.JavaConversions._

/**
  * A FlinkOptimizeProgram that transforms a relational expression into
  * another relational expression with [[RuleSet]].
  */
abstract class FlinkRuleSetProgram[OC <: FlinkOptimizeContext] extends FlinkOptimizeProgram[OC] {

  /**
    * All [[RelOptRule]]s for optimizing associated with this program.
    */
  protected val rules: util.List[RelOptRule] = new util.ArrayList[RelOptRule]()

  /**
    * Adds specified rules to this program.
    */
  def add(ruleSet: RuleSet): Unit = {
    Preconditions.checkNotNull(ruleSet)
    ruleSet.foreach { rule =>
      if (!contains(rule)) {
        rules.add(rule)
      }
    }
  }

  /**
    * Removes specified rules from this program.
    */
  def remove(ruleSet: RuleSet): Unit = {
    Preconditions.checkNotNull(ruleSet)
    ruleSet.foreach(rules.remove)
  }

  /**
    * Removes all rules from this program first, and then adds specified rules to this program.
    */
  def replaceAll(ruleSet: RuleSet): Unit = {
    Preconditions.checkNotNull(ruleSet)
    rules.clear()
    ruleSet.foreach(rules.add)
  }

  /**
    * Checks whether this program contains the specified rule.
    */
  def contains(rule: RelOptRule): Boolean = {
    Preconditions.checkNotNull(rule)
    rules.contains(rule)
  }
}
