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

package org.apache.flink.table.plan.rules.logical

import org.apache.flink.table.plan.util.FlinkRexUtil

import org.apache.calcite.plan.RelOptRule.{operand, any}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptRuleOperand}
import org.apache.calcite.rel.core.Filter
import org.apache.calcite.rel.logical.LogicalFilter
import org.apache.calcite.rel.{RelNode, RelShuttleImpl}
import org.apache.calcite.rex._

class FilterSimplifyExpressionsRule(
    operand: RelOptRuleOperand,
    simplifySubQuery: Boolean,
    description: String)
  extends RelOptRule(operand, description){

  override def onMatch(call: RelOptRuleCall): Unit = {
    val filter: Filter = call.rel(0)
    val changed = Array(false)
    val newFilter = simplify(filter, changed)
    newFilter match {
      case Some(f) =>
        call.transformTo(f)
        call.getPlanner.setImportance(filter, 0.0)
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
    val simplifiedCondition = FlinkRexUtil.simplify(rexBuilder, condition)
    val newCondition = RexUtil.pullFactors(rexBuilder, simplifiedCondition)

    if (!changed.head && !RexUtil.eq(condition, newCondition)) {
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

object FilterSimplifyExpressionsRule {
  val INSTANCE = new FilterSimplifyExpressionsRule(
    operand(classOf[Filter], any()),
    false,
    "FilterSimplifyExpressionsRule")

  val EXTENDED = new FilterSimplifyExpressionsRule(
    operand(classOf[Filter], any()),
    true,
    "FilterSimplifyExpressionsRule:simplifySubQuery")
}
