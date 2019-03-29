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

import org.apache.flink.table.api.TableException
import org.apache.flink.table.calcite.FlinkContext
import org.apache.flink.table.plan.nodes.calcite.{ConstantRankRange, ConstantRankRangeWithoutEnd}
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalCalc, FlinkLogicalOverWindow, FlinkLogicalRank}
import org.apache.flink.table.plan.util.FlinkRelOptUtil

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}
import org.apache.calcite.rex.{RexProgramBuilder, RexUtil}
import org.apache.calcite.sql.{SqlKind, SqlRankFunction}

import scala.collection.JavaConversions._

/**
  * Planner rule that matches a [[FlinkLogicalCalc]] on a [[FlinkLogicalOverWindow]],
  * and converts them into a [[FlinkLogicalRank]].
  */
abstract class FlinkLogicalRankRuleBase
  extends RelOptRule(
    operand(classOf[FlinkLogicalCalc],
      operand(classOf[FlinkLogicalOverWindow], any()))) {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val calc: FlinkLogicalCalc = call.rel(0)
    val window: FlinkLogicalOverWindow = call.rel(1)
    val group = window.groups.get(0)
    val rankFun = group.aggCalls.get(0).getOperator.asInstanceOf[SqlRankFunction]

    // the rank function is the last field of LogicalWindow
    val rankFieldIndex = window.getRowType.getFieldCount - 1
    val condition = calc.getProgram.getCondition
    val predicate = calc.getProgram.expandLocalRef(condition)

    val config = calc.getCluster.getPlanner.getContext.asInstanceOf[FlinkContext].getTableConfig
    val (rankRange, remainingPreds) = FlinkRelOptUtil.extractRankRange(
      predicate,
      rankFieldIndex,
      calc.getCluster.getRexBuilder,
      config)
    require(rankRange.isDefined)

    val cluster = window.getCluster
    val rexBuilder = cluster.getRexBuilder

    val calcProgram = calc.getProgram
    val exprList = calcProgram.getProjectList.map(calcProgram.expandLocalRef)

    val inputFields = RelOptUtil.InputFinder.bits(exprList, null).toList
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
      case _ => // do nothing
    }

    val rank = new FlinkLogicalRank(
      cluster,
      window.getTraitSet,
      window.getInput,
      rankFun,
      group.keys,
      group.orderKeys,
      rankRange.get,
      outputRankFunColumn)

    val newRel = if (RexUtil.isIdentity(exprList, rankRowType) && remainingPreds.isEmpty) {
      // project is trivial and filter is empty, remove the Calc
      rank
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

      calc.copy(calc.getTraitSet, rank, programBuilder.getProgram)
    }
    call.transformTo(newRel)
  }
}

/**
  * This rule handles [[SqlRankFunction]] and rank range with end.
  *
  * The following two example queries could be converted to Rank by this rule:
  * 1. constant range (rn <= 2):
  * {{{
  * SELECT * FROM (
  *   SELECT a, b, ROW_NUMBER() OVER (PARTITION BY b ORDER BY a) rn FROM MyTable) t
  * WHERE rn <= 2
  * }}}
  * 2. variable range (rk < a):
  * {{{
  * SELECT * FROM (
  *   SELECT a, b, RANK() OVER (PARTITION BY b ORDER BY c) rk FROM MyTable) t
  * WHERE rk < a
  * }}}
  */
class FlinkLogicalRankRuleForRangeEnd extends FlinkLogicalRankRuleBase {

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
        val config = calc.getCluster.getPlanner.getContext.asInstanceOf[FlinkContext].getTableConfig
        val (rankRange, remainingPreds) = FlinkRelOptUtil.extractRankRange(
          predicate,
          rankFieldIndex,
          calc.getCluster.getRexBuilder,
          config)

        rankRange match {
          case Some(_: ConstantRankRangeWithoutEnd) =>
            throw new TableException(
              "Rank end is not specified. Currently rank only support TopN, " +
                "which means the rank end must be specified.")
          case _ => // do nothing
        }

        // remaining predicate must not access rank field attributes
        val remainingPredsAccessRank = remainingPreds.isDefined &&
          FlinkRelOptUtil.accessesRankField(remainingPreds.get, rankFieldIndex)

        rankRange.isDefined && !remainingPredsAccessRank
      } else {
        false
      }
    } else {
      false
    }
  }
}

/**
  * This rule only handles RANK function and constant rank range.
  *
  * The following example query could be converted to Rank by this rule:
  * SELECT * FROM (
  * SELECT a, b, RANK() OVER (PARTITION BY b ORDER BY a) rn FROM MyTable) t
  * WHERE rn <= 2
  */
class FlinkLogicalRankRuleForConstantRange extends FlinkLogicalRankRuleBase {
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
        val config = calc.getCluster.getPlanner.getContext.asInstanceOf[FlinkContext].getTableConfig
        val (rankRange, remainingPreds) = FlinkRelOptUtil.extractRankRange(
          predicate,
          rankFieldIndex,
          calc.getCluster.getRexBuilder,
          config)

        // remaining predicate must not access rank field attributes
        val remainingPredsAccessRank = remainingPreds.isDefined &&
          FlinkRelOptUtil.accessesRankField(remainingPreds.get, rankFieldIndex)

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
  val INSTANCE = new FlinkLogicalRankRuleForRangeEnd
  val CONSTANT_RANGE_INSTANCE = new FlinkLogicalRankRuleForConstantRange
}
