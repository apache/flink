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

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.core.{Aggregate, Intersect, RelFactories, SemiJoin}
import org.apache.flink.table.plan.rules.logical.ReplaceExceptWithAntiJoinRule.generateCondition

import scala.collection.JavaConversions._

/**
  * Replaces distinct [[Intersect]] with a left-semi [[SemiJoin]] and
  * distinct [[Aggregate]].
  *
  * <p>Note: Not support Intersect All.
  */
class ReplaceIntersectWithSemiJoinRule extends RelOptRule(
  operand(classOf[Intersect], any),
  RelFactories.LOGICAL_BUILDER,
  "ReplaceIntersectWithSemiJoinRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val intersect = call.rel(0).asInstanceOf[Intersect]
    // not support intersect all now.
    intersect.isDistinct
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val intersect = call.rel(0).asInstanceOf[Intersect]
    val left = intersect.getInput(0)
    val right = intersect.getInput(1)

    val relBuilder = call.builder
    val keys = 0 until left.getRowType.getFieldCount
    val conditions = generateCondition(relBuilder, left, right, keys)

    relBuilder.push(left)
    relBuilder.push(right)
    relBuilder.semiJoin(conditions).aggregate(relBuilder.groupKey(keys: _*))
    call.transformTo(relBuilder.build())
  }
}

object ReplaceIntersectWithSemiJoinRule {
  val INSTANCE: RelOptRule = new ReplaceIntersectWithSemiJoinRule
}
