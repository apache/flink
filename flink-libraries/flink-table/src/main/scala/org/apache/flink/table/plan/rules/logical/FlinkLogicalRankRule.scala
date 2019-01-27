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
import org.apache.calcite.rex.{RexProgramBuilder, RexUtil}
import org.apache.calcite.sql.{SqlKind, SqlRankFunction}
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalCalc, FlinkLogicalOverWindow, FlinkLogicalRank}
import org.apache.flink.table.plan.util.{ConstantRankRange, ConstantRankRangeWithoutEnd, InputRefVisitor, RankUtil}

import scala.collection.JavaConversions._

/**
  * Planner rule that matches a [[FlinkLogicalCalc]] on a [[FlinkLogicalOverWindow]],
  * and converts them into a [[FlinkLogicalRank]].
  */
class FlinkLogicalRankRule
  extends RelOptRule(
    operand(classOf[FlinkLogicalCalc],
      operand(classOf[FlinkLogicalOverWindow], any())),
    "FlinkLogicalRankRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc: FlinkLogicalCalc = call.rel(0)
    val window: FlinkLogicalOverWindow = call.rel(1)

    if (window.groups.size > 1) {
      // only accept one window
      return false
    }

    val group = window.groups.get(0)
    if (group.aggCalls.size > 1) {
      // only accept one agg call
      return false
    }

    val agg = group.aggCalls.get(0)
    if (!agg.getOperator.isInstanceOf[SqlRankFunction]) {
      // only accept SqlRankFunction for Rank
      return false
    }

    if (group.lowerBound.isUnbounded && group.upperBound.isCurrentRow) {
      val condition = calc.getProgram.getCondition
      if (condition != null) {
        val predicate = calc.getProgram.expandLocalRef(condition)
        // the rank function is the last field of FlinkLogicalOverWindow
        val rankFieldIndex = window.getRowType.getFieldCount - 1
        val (rankRange, remainingPreds) = RankUtil.extractRankRange(
          predicate,
          rankFieldIndex,
          calc.getCluster.getRexBuilder,
          TableConfig.DEFAULT)

        // remaining predicate must not access rank field attributes
        val remainingPredsAccessRank = remainingPreds.isDefined &&
          RankUtil.accessesRankField(remainingPreds.get, rankFieldIndex)

        rankRange match {
          case Some(_: ConstantRankRangeWithoutEnd) =>
            throw new TableException(
              "Rank end is not specified. Currently rank only support TopN, " +
              "which means the rank end must be specified.")
          case _ => // ignore
        }

        rankRange.isDefined && !remainingPredsAccessRank
      } else {
        false
      }
    } else {
      false
    }
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val calc: FlinkLogicalCalc = call.rel(0)
    val window: FlinkLogicalOverWindow = call.rel(1)
    val group = window.groups.get(0)
    val rankFun = group.aggCalls.get(0).getOperator.asInstanceOf[SqlRankFunction]

    // the rank function is the last field of LogicalWindow
    val rankFieldIndex = window.getRowType.getFieldCount - 1
    val condition = calc.getProgram.getCondition
    val predicate = calc.getProgram.expandLocalRef(condition)

    val (rankRange, remainingPreds) = RankUtil.extractRankRange(
      predicate,
      rankFieldIndex,
      calc.getCluster.getRexBuilder,
      TableConfig.DEFAULT)
    require(rankRange.isDefined)

    val cluster = window.getCluster
    val rexBuilder = cluster.getRexBuilder

    val calcProgram = calc.getProgram
    val projectList = calcProgram.getProjectList
    val exprList = projectList.map(calcProgram.expandLocalRef)

    val visitor = new InputRefVisitor
    exprList.foreach(_.accept(visitor))
    val inputFields = visitor.getFields
    val outputRankFunColumn = inputFields.contains(rankFieldIndex)

    val rankRowType = if (outputRankFunColumn) {
      window.getRowType
    } else {
      val typeBuilder = rexBuilder.getTypeFactory.builder()
      window.getRowType.getFieldList.dropRight(1).foreach(typeBuilder.add)
      typeBuilder.build()
    }

    rankRange match {
      case Some(ConstantRankRange(_, rankEnd)) if rankEnd <= 0 =>
        throw new TableException(s"Rank end should not less than zero, but now is $rankEnd")

      case _ =>
        val rank = new FlinkLogicalRank(
          cluster,
          window.getTraitSet,
          window.getInput,
          rankFun,
          group.keys,
          group.orderKeys,
          rankRange.get,
          outputRankFunColumn)

        if (RexUtil.isIdentity(exprList, rankRowType) && remainingPreds.isEmpty) {
          // project is trivial and filter is empty, remove the Calc
          call.transformTo(rank)
        } else {
          val programBuilder = RexProgramBuilder.create(
            rexBuilder,
            rankRowType,
            calcProgram.getExprList,
            calcProgram.getProjectList,
            remainingPreds.orNull,
            calc.getRowType,
            true, // normalize
            null) // simplify

          val newCalc = calc.copy(calc.getTraitSet, rank, programBuilder.getProgram)
          call.transformTo(newCalc)
        }
    }
  }
}

/**
  * This rule only handles RANK function and constant rank range.
  */
class FlinkLogicalConstantRankRule extends FlinkLogicalRankRule {
  override def matches(call: RelOptRuleCall): Boolean = {
    val calc: FlinkLogicalCalc = call.rel(0)
    val window: FlinkLogicalOverWindow = call.rel(1)

    if (window.groups.size > 1) {
      // only accept one window
      return false
    }

    val group = window.groups.get(0)
    if (group.aggCalls.size > 1) {
      // only accept one agg call
      return false
    }

    val agg = group.aggCalls.get(0)
    if (agg.getOperator.kind != SqlKind.RANK) {
      // only accept RANK function
      return false
    }

    if (group.lowerBound.isUnbounded && group.upperBound.isCurrentRow) {
      val condition = calc.getProgram.getCondition
      if (condition != null) {
        val predicate = calc.getProgram.expandLocalRef(condition)
        // the rank function is the last field of FlinkLogicalOverWindow
        val rankFieldIndex = window.getRowType.getFieldCount - 1
        val (rankRange, remainingPreds) = RankUtil.extractRankRange(
          predicate,
          rankFieldIndex,
          calc.getCluster.getRexBuilder,
          TableConfig.DEFAULT)

        // remaining predicate must not access rank field attributes
        val remainingPredsAccessRank = remainingPreds.isDefined &&
          RankUtil.accessesRankField(remainingPreds.get, rankFieldIndex)

        // only support constant rank range
        rankRange.exists(_.isInstanceOf[ConstantRankRange]) && !remainingPredsAccessRank
      } else {
        false
      }
    } else {
      false
    }
  }
}

object FlinkLogicalRankRule {
  val INSTANCE = new FlinkLogicalRankRule
  val CONSTANT_RANK = new FlinkLogicalConstantRankRule
}
