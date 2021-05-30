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
import org.apache.flink.table.planner.plan.utils.InputRefVisitor

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rex.RexProgramBuilder

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
    val rank: FlinkLogicalRank = call.rel(1)
    val rankNumberColumnIdx = rank.getRowType.getFieldCount - 1
    rank.outputRankNumber && !isFieldUsedByCalc(calc, rankNumberColumnIdx)
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

    val rexBuilder = rank.getCluster.getRexBuilder
    val oldProgram = calc.getProgram
    val programBuilder = new RexProgramBuilder(newRank.getRowType, rexBuilder)
    oldProgram.getNamedProjects.foreach { pair =>
      programBuilder.addProject(oldProgram.expandLocalRef(pair.left), pair.right)
    }
    if (oldProgram.getCondition != null) {
      programBuilder.addCondition(oldProgram.expandLocalRef(oldProgram.getCondition))
    }
    val rexProgram = programBuilder.getProgram
    val newCalc = FlinkLogicalCalc.create(newRank, rexProgram)
    call.transformTo(newCalc)
  }

  private def isFieldUsedByCalc(calc: FlinkLogicalCalc, inputRefIdx: Int): Boolean = {
    val projectsAndConditions = calc.getProgram.split()
    val projects = projectsAndConditions.left
    val conditions = projectsAndConditions.right
    val visitor = new InputRefVisitor
    // extract referenced input fields from projections
    projects.foreach(exp => exp.accept(visitor))
    // extract referenced input fields from condition
    conditions.foreach(_.accept(visitor))
    visitor.getFields.contains(inputRefIdx)
  }
}

object RedundantRankNumberColumnRemoveRule {
  val INSTANCE = new RedundantRankNumberColumnRemoveRule
}
