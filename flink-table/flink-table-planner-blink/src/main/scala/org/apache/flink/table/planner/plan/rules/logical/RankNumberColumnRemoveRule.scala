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
import org.apache.flink.table.runtime.operators.rank.{ConstantRankRange, RankType}

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rex.RexProgramBuilder

import java.math.{BigDecimal => JBigDecimal}

/**
  * Planner rule that removes the output column of rank number
  * iff there is a equality condition for the rank column.
  */
class RankNumberColumnRemoveRule
  extends RelOptRule(
    operand(classOf[FlinkLogicalRank], any()),
    "RankFunctionColumnRemoveRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val rank: FlinkLogicalRank = call.rel(0)
    val isRowNumber = rank.rankType == RankType.ROW_NUMBER
    val constantRowNumber = rank.rankRange match {
      case range: ConstantRankRange => range.getRankStart == range.getRankEnd
      case _ => false
    }
    isRowNumber && constantRowNumber && rank.outputRankNumber
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val rank: FlinkLogicalRank = call.rel(0)
    val rowNumber = rank.rankRange.asInstanceOf[ConstantRankRange].getRankStart
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

    val rexBuilder = rank.getCluster.getRexBuilder
    val programBuilder = new RexProgramBuilder(newRank.getRowType, rexBuilder)
    val fieldCount = rank.getRowType.getFieldCount
    val fieldNames = rank.getRowType.getFieldNames
    for (i <- 0 until fieldCount) {
      if (i < fieldCount - 1) {
        programBuilder.addProject(i, i, fieldNames.get(i))
      } else {
        val rowNumberLiteral = rexBuilder.makeBigintLiteral(new JBigDecimal(rowNumber))
        programBuilder.addProject(i, rowNumberLiteral, fieldNames.get(i))
      }
    }

    val rexProgram = programBuilder.getProgram
    val calc = FlinkLogicalCalc.create(newRank, rexProgram)
    call.transformTo(calc)
  }
}

object RankNumberColumnRemoveRule {
  val INSTANCE = new RankNumberColumnRemoveRule
}
