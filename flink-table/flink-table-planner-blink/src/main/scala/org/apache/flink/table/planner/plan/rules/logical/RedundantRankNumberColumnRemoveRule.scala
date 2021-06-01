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

import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalCalc, FlinkLogicalRank}
import org.apache.flink.table.planner.plan.utils.RankUtil

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}
import org.apache.calcite.rex.{RexNode, RexProgram}

import scala.collection.JavaConversions._

/**
 * Planner rule that removes the output column of rank number
 * iff the rank number column is not used by successor calc.
 */
class RedundantRankNumberColumnRemoveRule
  extends RelOptRule(
    operand(classOf[FlinkLogicalCalc],
      operand(classOf[FlinkLogicalRank], any())),
    "RedundantRankNumberColumnRemoveRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc: FlinkLogicalCalc = call.rel(0)
    val usedFields = getUsedFields(calc.getProgram)
    val rank: FlinkLogicalRank = call.rel(1)
    val rankFunFieldIndex = RankUtil.getRankNumberColumnIndex(rank).getOrElse(-1)
    rank.outputRankNumber && !usedFields.contains(rankFunFieldIndex)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val calc: FlinkLogicalCalc = call.rel(0)
    val rank: FlinkLogicalRank = call.rel(1)
    val newRank = new FlinkLogicalRank(
      rank.getCluster,
      rank.getTraitSet,
      rank.getInput,
      rank.partitionKey,
      rank.orderKey,
      rank.rankType,
      rank.rankRange,
      rank.rankNumberType,
      outputRankNumber = false)

    val oldProgram = calc.getProgram
    val (projects, condition) = getProjectsAndCondition(calc.getProgram)
    val newProgram = RexProgram.create(
      newRank.getRowType,
      projects,
      condition,
      oldProgram.getOutputRowType,
      rank.getCluster.getRexBuilder)
    val newCalc = FlinkLogicalCalc.create(newRank, newProgram)
    call.transformTo(newCalc)
  }

  private def getUsedFields(program: RexProgram): Array[Int] = {
    val (projects, condition) = getProjectsAndCondition(program)
    RelOptUtil.InputFinder.bits(projects, condition).toArray
  }

  private def getProjectsAndCondition(program: RexProgram): (Seq[RexNode], RexNode) = {
    val projects = program.getProjectList.map(program.expandLocalRef)
    val condition = if (program.getCondition != null) {
      program.expandLocalRef(program.getCondition)
    } else {
      null
    }
    (projects, condition)
  }
}

object RedundantRankNumberColumnRemoveRule {
  val INSTANCE = new RedundantRankNumberColumnRemoveRule
}
