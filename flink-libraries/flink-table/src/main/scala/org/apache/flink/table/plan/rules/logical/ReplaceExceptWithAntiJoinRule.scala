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
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core._
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.table.plan.rules.logical.ReplaceExceptWithAntiJoinRule.generateCondition

import scala.collection.JavaConversions._
import scala.collection.immutable

/**
  * Replaces distinct [[Minus]] with a left-anti [[SemiJoin]] and
  * distinct [[Aggregate]].
  *
  * <p>Note: Not support Minus All.
  */
class ReplaceExceptWithAntiJoinRule extends RelOptRule(
  operand(classOf[Minus], any),
  RelFactories.LOGICAL_BUILDER,
  "ReplaceExceptWithAntiJoinRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val minus = call.rel(0).asInstanceOf[Minus]
    // not support minus all now.
    minus.isDistinct
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val minus = call.rel(0).asInstanceOf[Minus]
    val left = minus.getInput(0)
    val right = minus.getInput(1)

    val relBuilder = call.builder
    val keys = 0 until left.getRowType.getFieldCount
    val conditions = generateCondition(relBuilder, left, right, keys)

    relBuilder.push(left)
    relBuilder.push(right)
    relBuilder.antiJoin(conditions).aggregate(relBuilder.groupKey(keys: _*))
    call.transformTo(relBuilder.build())
  }
}

object ReplaceExceptWithAntiJoinRule {
  val INSTANCE: RelOptRule = new ReplaceExceptWithAntiJoinRule

  def generateCondition(relBuilder: RelBuilder, left: RelNode, right: RelNode, keys: Range)
  : immutable.IndexedSeq[RexNode] = {
    val rexBuilder = relBuilder.getRexBuilder
    val leftTypes = RelOptUtil.getFieldTypeList(left.getRowType)
    val rightTypes = RelOptUtil.getFieldTypeList(right.getRowType)
    val conditions = keys.map { case (key) =>
      val leftRex = rexBuilder.makeInputRef(leftTypes.get(key), key)
      val rightRex = rexBuilder.makeInputRef(rightTypes.get(key), leftTypes.size + key)
      val equalCond = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, leftRex, rightRex)
      relBuilder.or(
        equalCond,
        relBuilder.and(relBuilder.isNull(leftRex), relBuilder.isNull(rightRex)))
    }
    conditions
  }
}
