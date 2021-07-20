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

import org.apache.flink.table.planner.plan.utils.SetOpRewriteUtil.generateEqualsCondition

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.core._

import scala.collection.JavaConversions._

/**
  * Planner rule that replaces distinct [[Minus]] (SQL keyword: EXCEPT) with
  * a distinct [[Aggregate]] on an ANTI [[Join]].
  *
  * Only handle the case of input size 2.
  */
class ReplaceMinusWithAntiJoinRule extends RelOptRule(
  operand(classOf[Minus], any),
  RelFactories.LOGICAL_BUILDER,
  "ReplaceMinusWithAntiJoinRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val minus: Minus = call.rel(0)
    !minus.all && minus.getInputs.size() == 2
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val minus: Minus = call.rel(0)
    val left = minus.getInput(0)
    val right = minus.getInput(1)

    val relBuilder = call.builder
    val keys = 0 until left.getRowType.getFieldCount
    val conditions = generateEqualsCondition(relBuilder, left, right, keys)

    relBuilder.push(left)
    relBuilder.push(right)
    relBuilder.join(JoinRelType.ANTI, conditions).aggregate(relBuilder.groupKey(keys: _*))
    val rel = relBuilder.build()
    call.transformTo(rel)
  }
}

object ReplaceMinusWithAntiJoinRule {
  val INSTANCE: RelOptRule = new ReplaceMinusWithAntiJoinRule
}
