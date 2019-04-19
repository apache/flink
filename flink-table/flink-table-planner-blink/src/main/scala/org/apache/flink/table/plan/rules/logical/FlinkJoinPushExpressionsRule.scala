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

import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.core.{Join, RelFactories}
import org.apache.calcite.rel.rules.JoinPushExpressionsRule
import org.apache.calcite.rex.{RexInputRef, RexVisitorImpl}
import org.apache.calcite.tools.RelBuilderFactory
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.rules.logical.FlinkJoinPushExpressionsRule.TimeIndicatorExprFinder

/**
  * Planner rule that pushes down expressions in "equal" join condition when the expressions
  * don't contain time indicator calls.
  *
  * <p>For example, given
  * "emp JOIN dept ON emp.deptno + 1 = dept.deptno", adds a project above
  * "emp" that computes the expression
  * "emp.deptno + 1". The resulting join condition is a simple combination
  * of AND, equals, and input fields, plus the remaining non-equal conditions.
  */
class FlinkJoinPushExpressionsRule(
    clazz: Class[_ <: Join],
    relBuilderFactory: RelBuilderFactory)
  extends JoinPushExpressionsRule(clazz, relBuilderFactory) {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join = call.rel[Join](0)
    val condition = join.getCondition
    val containsTimeIndicator = condition.accept(new TimeIndicatorExprFinder)
    // skip time indicator expressions to make sure we handle time indicator expressions correctly
    // TODO: only push down expressions which don't contain time indicators
    !containsTimeIndicator && super.matches(call)
  }

}

object FlinkJoinPushExpressionsRule {

  val INSTANCE = new FlinkJoinPushExpressionsRule(classOf[Join], RelFactories.LOGICAL_BUILDER)

  class TimeIndicatorExprFinder extends RexVisitorImpl[Boolean](true) {
    override def visitInputRef(inputRef: RexInputRef): Boolean = {
      FlinkTypeFactory.isTimeIndicatorType(inputRef.getType)
    }
  }

}
