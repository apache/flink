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
import org.apache.calcite.rex.RexUtil
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalCalc, FlinkLogicalRank}

import scala.collection.JavaConversions._

/**
  * Planner rule that matches a [[FlinkLogicalCalc]] (all fields are from its input's input)
  * on a [[FlinkLogicalRank]] (outputRankFunColumn is true), and merge them into a new
  * [[FlinkLogicalRank]] (outputRankFunColumn is false).
  */
class CalcRankMergeRule
  extends RelOptRule(
    operand(classOf[FlinkLogicalCalc],
      operand(classOf[FlinkLogicalRank], any())),
    "CalcRankMergeRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc: FlinkLogicalCalc = call.rel(0)
    val rank: FlinkLogicalRank = call.rel(1)

    val program = calc.getProgram
    if (program.getCondition != null) {
      return false
    }

    val projects = program.getProjectList.map(program.expandLocalRef)
    val allFieldsFromRankInput = RexUtil.isIdentity(projects, rank.getInput.getRowType)
    allFieldsFromRankInput && rank.outputRankFunColumn
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val rank: FlinkLogicalRank = call.rel(1)

    val newRank = new FlinkLogicalRank(
      rank.getCluster,
      rank.getTraitSet,
      rank.getInput,
      rank.rankFunction,
      rank.partitionKey,
      rank.sortCollation,
      rank.rankRange,
      outputRankFunColumn = false)

    call.transformTo(newRank)
  }
}

object CalcRankMergeRule {
  val INSTANCE = new CalcRankMergeRule
}
