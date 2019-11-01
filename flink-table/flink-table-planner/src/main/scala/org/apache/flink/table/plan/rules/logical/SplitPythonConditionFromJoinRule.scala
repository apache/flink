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

import org.apache.calcite.plan.RelOptRule.{none, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rex.{RexProgram, RexProgramBuilder, RexUtil}
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalCalc, FlinkLogicalJoin}
import org.apache.flink.table.plan.util.PythonUtil.containsPythonCall

import scala.collection.JavaConversions._

/**
  * Rule will splits the [[FlinkLogicalJoin]] which contains Python Functions in join condition
  * into a [[FlinkLogicalJoin]] and a [[FlinkLogicalCalc]] with python Functions. Currently, only
  * inner join is supported.
  *
  * After this rule is applied, there will be no Python Functions in the condition of the
  * [[FlinkLogicalJoin]].
  */
class SplitPythonConditionFromJoinRule extends RelOptRule(
  operand(classOf[FlinkLogicalJoin], none),
  "SplitPythonConditionFromJoinRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: FlinkLogicalJoin = call.rel(0).asInstanceOf[FlinkLogicalJoin]
    val joinType: JoinRelType = join.getJoinType
    // matches if it is inner join and it contains Python functions in condition
    joinType == JoinRelType.INNER && Option(join.getCondition).exists(containsPythonCall)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join: FlinkLogicalJoin = call.rel(0).asInstanceOf[FlinkLogicalJoin]
    val rexBuilder = join.getCluster.getRexBuilder

    val joinFilters = RelOptUtil.conjunctions(join.getCondition)
    val pythonFilters = joinFilters.filter(containsPythonCall)
    val remainingFilters = joinFilters.filter(!containsPythonCall(_))

    val newJoinCondition = RexUtil.composeConjunction(rexBuilder, remainingFilters)
    val bottomJoin = new FlinkLogicalJoin(
      join.getCluster,
      join.getTraitSet,
      join.getLeft,
      join.getRight,
      newJoinCondition,
      join.getJoinType)

    val rexProgram = new RexProgramBuilder(bottomJoin.getRowType, rexBuilder).getProgram
    val topCalcCondition = RexUtil.composeConjunction(rexBuilder, pythonFilters)

    val topCalc = new FlinkLogicalCalc(
      join.getCluster,
      join.getTraitSet,
      bottomJoin,
      RexProgram.create(
        bottomJoin.getRowType,
        rexProgram.getExprList,
        topCalcCondition,
        bottomJoin.getRowType,
        rexBuilder))

    call.transformTo(topCalc)
  }
}

object SplitPythonConditionFromJoinRule {
  val INSTANCE = new SplitPythonConditionFromJoinRule
}
