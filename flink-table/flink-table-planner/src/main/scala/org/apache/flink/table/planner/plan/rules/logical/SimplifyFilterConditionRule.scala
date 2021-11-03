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

package org.apache.flink.table.planner.plan.rules.logical

import org.apache.flink.table.planner.plan.utils.FlinkRexUtil

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.core.Filter
import org.apache.calcite.rel.logical.LogicalFilter
import org.apache.calcite.rel.{RelNode, RelShuttleImpl}
import org.apache.calcite.rex._

/**
  * Planner rule that apply various simplifying transformations on filter condition.
  *
  * if `simplifySubQuery` is true, this rule will also simplify the filter condition
  * in [[RexSubQuery]].
  */
class SimplifyFilterConditionRule(
    simplifySubQuery: Boolean,
    description: String)
  extends RelOptRule(
    operand(classOf[Filter], any()),
    description) {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val filter: Filter = call.rel(0)
    val changed = Array(false)
    val newFilter = simplify(filter, changed)
    newFilter match {
      case Some(f) =>
        call.transformTo(f)
        call.getPlanner.prune(filter)
      case _ => // do nothing
    }
  }

  def simplify(filter: Filter, changed: Array[Boolean]): Option[Filter] = {
    val condition = if (simplifySubQuery) {
      simplifyFilterConditionInSubQuery(filter.getCondition, changed)
    } else {
      filter.getCondition
    }

    val rexBuilder = filter.getCluster.getRexBuilder
    val simplifiedCondition = FlinkRexUtil.simplify(
      rexBuilder,
      condition,
      filter.getCluster.getPlanner.getExecutor)
    val newCondition = RexUtil.pullFactors(rexBuilder, simplifiedCondition)

    if (!changed.head && !condition.equals(newCondition)) {
      changed(0) = true
    }

    // just replace modified RexNode
    if (changed.head) {
      Some(filter.copy(filter.getTraitSet, filter.getInput, newCondition))
    } else {
      None
    }
  }

  def simplifyFilterConditionInSubQuery(condition: RexNode, changed: Array[Boolean]): RexNode = {
    condition.accept(new RexShuttle() {
      override def visitSubQuery(subQuery: RexSubQuery): RexNode = {
        val newRel = subQuery.rel.accept(new RelShuttleImpl() {
          override def visit(filter: LogicalFilter): RelNode = {
            simplify(filter, changed).getOrElse(filter)
          }
        })
        if (changed.head) {
          subQuery.clone(newRel)
        } else {
          subQuery
        }
      }
    })
  }

}

object SimplifyFilterConditionRule {
  val INSTANCE = new SimplifyFilterConditionRule(
    false, "SimplifyFilterConditionRule")

  val EXTENDED = new SimplifyFilterConditionRule(
    true, "SimplifyFilterConditionRule:simplifySubQuery")
}
