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

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rex._
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalCalc, FlinkLogicalCorrelate, FlinkLogicalRel, FlinkLogicalTableFunctionScan}
import org.apache.flink.table.planner.plan.rules.physical.stream.StreamExecCorrelateRule
import org.apache.flink.table.planner.plan.utils.PythonUtil.{containsPythonCall, isNonPythonCall}
import org.apache.flink.table.planner.plan.utils.RexDefaultVisitor

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Rule will split a [[FlinkLogicalCalc]] which is the upstream of a [[FlinkLogicalCorrelate]]
  * and contains Python Functions in condition into two [[FlinkLogicalCalc]]s. One of the
  * [[FlinkLogicalCalc]] without python function condition is the upstream of the
  * [[FlinkLogicalCorrelate]], but the other [[[FlinkLogicalCalc]] with python function conditions
  * is the downstream of the [[FlinkLogicalCorrelate]]. Currently, only inner join is supported.
  *
  * After this rule is applied, there will be no Python Functions in the condition of the upstream
  * [[FlinkLogicalCalc]].
  */
class SplitPythonConditionFromCorrelateRule
  extends RelOptRule(
    operand(
      classOf[FlinkLogicalCorrelate],
      operand(classOf[FlinkLogicalRel], any),
      operand(classOf[FlinkLogicalCalc], any)),
    "SplitPythonConditionFromCorrelateRule") {
  override def matches(call: RelOptRuleCall): Boolean = {
    val correlate: FlinkLogicalCorrelate = call.rel(0).asInstanceOf[FlinkLogicalCorrelate]
    val right: FlinkLogicalCalc = call.rel(2).asInstanceOf[FlinkLogicalCalc]
    val joinType: JoinRelType = correlate.getJoinType
    val mergedCalc = StreamExecCorrelateRule.getMergedCalc(right)
    val tableScan = StreamExecCorrelateRule
      .getTableScan(mergedCalc)
      .asInstanceOf[FlinkLogicalTableFunctionScan]
    joinType == JoinRelType.INNER &&
      isNonPythonCall(tableScan.getCall) &&
      Option(mergedCalc.getProgram.getCondition)
        .map(mergedCalc.getProgram.expandLocalRef)
        .exists(containsPythonCall(_))
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val correlate: FlinkLogicalCorrelate = call.rel(0).asInstanceOf[FlinkLogicalCorrelate]
    val right: FlinkLogicalCalc = call.rel(2).asInstanceOf[FlinkLogicalCalc]
    val rexBuilder = call.builder().getRexBuilder
    val mergedCalc = StreamExecCorrelateRule.getMergedCalc(right)
    val mergedCalcProgram = mergedCalc.getProgram
    val input = mergedCalc.getInput

    val correlateFilters = RelOptUtil
      .conjunctions(mergedCalcProgram.expandLocalRef(mergedCalcProgram.getCondition))

    val remainingFilters = correlateFilters.filter(!containsPythonCall(_))

    val bottomCalcCondition = RexUtil.composeConjunction(rexBuilder, remainingFilters)

    val newBottomCalc = new FlinkLogicalCalc(
      mergedCalc.getCluster,
      mergedCalc.getTraitSet,
      input,
      RexProgram.create(
        input.getRowType,
        mergedCalcProgram.getProjectList,
        bottomCalcCondition,
        mergedCalc.getRowType,
        rexBuilder))

    val newCorrelate = new FlinkLogicalCorrelate(
      correlate.getCluster,
      correlate.getTraitSet,
      correlate.getLeft,
      newBottomCalc,
      correlate.getCorrelationId,
      correlate.getRequiredColumns,
      correlate.getJoinType)

    val inputRefRewriter = new InputRefRewriter(
      correlate.getRowType.getFieldCount - mergedCalc.getRowType.getFieldCount)

    val pythonFilters = correlateFilters
      .filter(containsPythonCall(_))
      .map(_.accept(inputRefRewriter))

    val topCalcCondition = RexUtil.composeConjunction(rexBuilder, pythonFilters)

    val rexProgram = new RexProgramBuilder(newCorrelate.getRowType, rexBuilder).getProgram
    val newTopCalc = new FlinkLogicalCalc(
      newCorrelate.getCluster,
      newCorrelate.getTraitSet,
      newCorrelate,
      RexProgram.create(
        newCorrelate.getRowType,
        rexProgram.getExprList,
        topCalcCondition,
        newCorrelate.getRowType,
        rexBuilder))

    call.transformTo(newTopCalc)
  }
}

/**
  * Because the inputRef is from the upstream calc node of the correlate node, so after the inputRef
  * is pushed to the downstream calc node of the correlate node, the inputRef need to rewrite the
  * index.
  *
  * @param offset the start offset of the inputRef in the downstream calc.
  */
private class InputRefRewriter(offset: Int)
  extends RexDefaultVisitor[RexNode] {

  override def visitInputRef(inputRef: RexInputRef): RexNode = {
    new RexInputRef(inputRef.getIndex + offset, inputRef.getType)
  }

  override def visitCall(call: RexCall): RexNode = {
    call.clone(call.getType, call.getOperands.asScala.map(_.accept(this)))
  }

  override def visitNode(rexNode: RexNode): RexNode = rexNode
}

object SplitPythonConditionFromCorrelateRule {
  val INSTANCE: SplitPythonConditionFromCorrelateRule = new SplitPythonConditionFromCorrelateRule
}
