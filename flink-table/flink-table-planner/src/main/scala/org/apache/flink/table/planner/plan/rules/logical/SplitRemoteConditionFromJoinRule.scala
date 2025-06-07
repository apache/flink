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

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalCalc, FlinkLogicalJoin}

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}
import org.apache.calcite.plan.RelOptRule.{none, operand}
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rex.{RexProgram, RexProgramBuilder, RexUtil}

import scala.collection.JavaConversions._

/**
 * Rule will splits the [[FlinkLogicalJoin]] which contains Remote Functions in join condition into
 * a [[FlinkLogicalJoin]] and a [[FlinkLogicalCalc]] with Remote Functions. Currently, only inner
 * join is supported.
 *
 * After this rule is applied, there will be no Remote Functions in the condition of the
 * [[FlinkLogicalJoin]].
 */
class SplitRemoteConditionFromJoinRule(
    protected val callFinder: RemoteCallFinder,
    protected val errorOnUnsplittableRemoteCall: Option[String])
  extends RelOptRule(operand(classOf[FlinkLogicalJoin], none), "SplitRemoteConditionFromJoinRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: FlinkLogicalJoin = call.rel(0).asInstanceOf[FlinkLogicalJoin]
    val joinType: JoinRelType = join.getJoinType
    // matches if it is inner join and it contains Remote functions in condition
    if (Option(join.getCondition).exists(callFinder.containsRemoteCall)) {
      if (joinType == JoinRelType.INNER) {
        return true
      } else if (errorOnUnsplittableRemoteCall.nonEmpty) {
        throw new TableException(errorOnUnsplittableRemoteCall.get)
      }
    }
    false
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join: FlinkLogicalJoin = call.rel(0).asInstanceOf[FlinkLogicalJoin]
    val rexBuilder = join.getCluster.getRexBuilder

    val joinFilters = RelOptUtil.conjunctions(join.getCondition)
    val remoteFilters = joinFilters.filter(callFinder.containsRemoteCall)
    val remainingFilters = joinFilters.filter(!callFinder.containsRemoteCall(_))

    val newJoinCondition = RexUtil.composeConjunction(rexBuilder, remainingFilters)
    val bottomJoin = new FlinkLogicalJoin(
      join.getCluster,
      join.getTraitSet,
      join.getLeft,
      join.getRight,
      newJoinCondition,
      join.getHints,
      join.getJoinType)

    val rexProgram = new RexProgramBuilder(bottomJoin.getRowType, rexBuilder).getProgram
    val topCalcCondition = RexUtil.composeConjunction(rexBuilder, remoteFilters)

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

  // Consider the rules to be equal if they are the same class and their call finders are the same
  // class.
  override def equals(obj: Any): Boolean = {
    obj match {
      case rule: SplitRemoteConditionFromJoinRule =>
        super.equals(rule) && callFinder.getClass.equals(rule.callFinder.getClass) &&
        errorOnUnsplittableRemoteCall.equals(rule.errorOnUnsplittableRemoteCall)
      case _ => false
    }
  }
}
