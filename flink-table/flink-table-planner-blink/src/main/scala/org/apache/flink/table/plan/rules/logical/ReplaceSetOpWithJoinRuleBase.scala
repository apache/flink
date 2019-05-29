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
import org.apache.calcite.plan.{RelOptRule, RelOptUtil}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{RelFactories, SetOp}
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.tools.RelBuilder

/**
  * Base class that replace [[SetOp]] to [[org.apache.calcite.rel.core.Join]].
  */
abstract class ReplaceSetOpWithJoinRuleBase[T <: SetOp](
    clazz: Class[T],
    description: String)
  extends RelOptRule(
    operand(clazz, any),
    RelFactories.LOGICAL_BUILDER,
    description) {

  protected def generateCondition(
      relBuilder: RelBuilder,
      left: RelNode,
      right: RelNode,
      keys: Seq[Int]): Seq[RexNode] = {
    val rexBuilder = relBuilder.getRexBuilder
    val leftTypes = RelOptUtil.getFieldTypeList(left.getRowType)
    val rightTypes = RelOptUtil.getFieldTypeList(right.getRowType)
    val conditions = keys.map { key =>
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
