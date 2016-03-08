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

package org.apache.flink.api.table.plan.rules.logical

import org.apache.calcite.plan.RelOptRule.{any, operand, convert => convertTrait}
import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.plan.RelOptRuleOperand
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.RelNode
import java.util.ArrayList
import scala.collection.JavaConversions._
import org.apache.calcite.rel.logical.LogicalJoin
import org.apache.calcite.rel.logical.LogicalUnion
import org.apache.calcite.rel.core.Join
import org.apache.calcite.rel.core.Union

/**
 * This rule is a copy of Calcite's JoinUnionTransposeRule.
 * Calcite's implementation checks whether one of the operands is a LogicalUnion,
 * which fails in our case, when it matches with a DataSetUnion.
 * This rule changes this check to match Union, instead of LogicalUnion only.
 * The rest of the rule's logic has not been changed.
 */
class FlinkJoinUnionTransposeRule(
    operand: RelOptRuleOperand,
    description: String) extends RelOptRule(operand, description) {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join = call.rel(0).asInstanceOf[Join]
    val (unionRel: Union, otherInput: RelNode, unionOnLeft: Boolean) = {
      if (call.rel(1).isInstanceOf[Union]) {
        (call.rel(1).asInstanceOf[Union], call.rel(2).asInstanceOf[RelNode], true)
      }
      else {
        (call.rel(2).asInstanceOf[Union], call.rel(1).asInstanceOf[RelNode], false)
      }
    }
    
    if (!unionRel.all) {
      return
    }
    if (!join.getVariablesStopped.isEmpty) {
      return
    }
    // The UNION ALL cannot be on the null generating side
    // of an outer join (otherwise we might generate incorrect
    // rows for the other side for join keys which lack a match
    // in one or both branches of the union)
    if (unionOnLeft) {
      if (join.getJoinType.generatesNullsOnLeft) {
        return
      }
    }
    else {
      if (join.getJoinType.generatesNullsOnRight) {
        return
      }
    }
    val newUnionInputs = new ArrayList[RelNode]
    for (input <- unionRel.getInputs) {
      val (joinLeft: RelNode, joinRight: RelNode) = {
      if (unionOnLeft) {
        (input, otherInput)
      }
      else {
        (otherInput, input)
      }
    }

      newUnionInputs.add(
          join.copy(
              join.getTraitSet,
              join.getCondition,
              joinLeft,
              joinRight,
              join.getJoinType,
              join.isSemiJoinDone))
    }
    val newUnionRel = unionRel.copy(unionRel.getTraitSet, newUnionInputs, true)
    call.transformTo(newUnionRel)
  }
}

object FlinkJoinUnionTransposeRule {
  val LEFT_UNION = new FlinkJoinUnionTransposeRule(
      operand(classOf[LogicalJoin], operand(classOf[LogicalUnion], any),
          operand(classOf[RelNode], any)),
          "JoinUnionTransposeRule(Union-Other)")

    val RIGHT_UNION = new FlinkJoinUnionTransposeRule(
      operand(classOf[LogicalJoin], operand(classOf[RelNode], any),
          operand(classOf[LogicalUnion], any)),
          "JoinUnionTransposeRule(Other-Union)")
}
