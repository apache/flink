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

import org.apache.flink.table.api._
import org.apache.flink.table.expressions.FieldReferenceExpression
import org.apache.flink.table.expressions.utils.ApiExpressionUtils.intervalOfMillis
import org.apache.flink.table.planner.calcite.FlinkRelBuilder.PlannerNamedWindowProperty
import org.apache.flink.table.planner.expressions.PlannerWindowReference
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable
import org.apache.flink.table.planner.plan.logical.{LogicalWindow, SessionGroupWindow, SlidingGroupWindow, TumblingGroupWindow}
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalWindowAggregate
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan._
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Aggregate.Group
import org.apache.calcite.rel.core.{Aggregate, AggregateCall}
import org.apache.calcite.rel.logical.{LogicalAggregate, LogicalProject}
import org.apache.calcite.rex._
import org.apache.calcite.sql.`type`.SqlTypeUtil
import org.apache.calcite.util.ImmutableBitSet

import _root_.scala.collection.JavaConversions._

/**
  * Planner rule that transforms simple [[LogicalAggregate]] on a [[LogicalProject]]
  * with windowing expression to [[LogicalWindowAggregate]].
  */
abstract class LogicalWindowAggregateRuleBase(description: String)
  extends RelOptRule(
    operand(classOf[LogicalAggregate],
      operand(classOf[LogicalProject], none())),
    description) {

  override def matches(call: RelOptRuleCall): Boolean = {
    val agg: LogicalAggregate = call.rel(0)

    val windowExpressions = getWindowExpressions(agg)
    if (windowExpressions.length > 1) {
      throw new TableException("Only a single window group function may be used in GROUP BY")
    }

    // check if we have grouping sets
    val groupSets = agg.getGroupType != Group.SIMPLE
    !groupSets && !agg.indicator && windowExpressions.nonEmpty
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val agg: LogicalAggregate = call.rel(0)
    val project: LogicalProject = call.rel(1)

    val (windowExpr, windowExprIdx) = getWindowExpressions(agg).head
    val window = translateWindow(windowExpr, windowExprIdx, project.getInput.getRowType)

    val rexBuilder = agg.getCluster.getRexBuilder

    val inAggGroupExpression = getInAggregateGroupExpression(rexBuilder, windowExpr)

    val newGroupSet = agg.getGroupSet.except(ImmutableBitSet.of(windowExprIdx))

    val builder = call.builder()

    val newProject = builder
      .push(project.getInput)
      .project(project.getChildExps.updated(windowExprIdx, inAggGroupExpression))
      .build()

    // Currently, this rule removes the window from GROUP BY operation which may lead to changes
    // of AggCall's type which brings fails on type checks.
    // To solve the problem, we change the types to the inferred types in the Aggregate and then
    // cast back in the project after Aggregate.
    val indexAndTypes = getIndexAndInferredTypesIfChanged(agg)
    val finalCalls = adjustTypes(agg, indexAndTypes)

    // we don't use the builder here because it uses RelMetadataQuery which affects the plan
    val newAgg = LogicalAggregate.create(
      newProject,
      agg.indicator,
      newGroupSet,
      ImmutableList.of(newGroupSet),
      finalCalls)

    val transformed = call.builder()
    val windowAgg = LogicalWindowAggregate.create(
      window,
      Seq[PlannerNamedWindowProperty](),
      newAgg)
    transformed.push(windowAgg)

    // The transformation adds an additional LogicalProject at the top to ensure
    // that the types are equivalent.
    // 1. ensure group key types, create an additional project to conform with types
    val outAggGroupExpression0 = getOutAggregateGroupExpression(rexBuilder, windowExpr)
    // fix up the nullability if it is changed.
    val outAggGroupExpression = if (windowExpr.getType.isNullable !=
      outAggGroupExpression0.getType.isNullable) {
      builder.getRexBuilder.makeAbstractCast(
        builder.getRexBuilder.matchNullability(outAggGroupExpression0.getType, windowExpr),
        outAggGroupExpression0)
    } else {
      outAggGroupExpression0
    }
    val projectsEnsureGroupKeyTypes =
      transformed.fields.patch(windowExprIdx, Seq(outAggGroupExpression), 0)
    // 2. ensure aggCall types
    val projectsEnsureAggCallTypes =
      projectsEnsureGroupKeyTypes.zipWithIndex.map {
        case (aggCall, index) =>
          val aggCallIndex = index - agg.getGroupCount
          if (indexAndTypes.containsKey(aggCallIndex)) {
            rexBuilder.makeCast(agg.getAggCallList.get(aggCallIndex).`type`, aggCall, true)
          } else {
            aggCall
          }
      }
    transformed.project(projectsEnsureAggCallTypes)

    val result = transformed.build()
    call.transformTo(result)
  }

  /**
   * Change the types of [[AggregateCall]] to the corresponding inferred types.
   */
  private def adjustTypes(
      agg: LogicalAggregate,
      indexAndTypes: Map[Int, RelDataType]) = {

    agg.getAggCallList.zipWithIndex.map {
      case (aggCall, index) =>
        if (indexAndTypes.containsKey(index)) {
          AggregateCall.create(
            aggCall.getAggregation,
            aggCall.isDistinct,
            aggCall.isApproximate,
            aggCall.ignoreNulls(),
            aggCall.getArgList,
            aggCall.filterArg,
            aggCall.collation,
            agg.getGroupCount,
            agg.getInput,
            indexAndTypes(index),
            aggCall.name)
        } else {
          aggCall
        }
    }
  }

  /**
   * Check if there are any types of [[AggregateCall]] that need to be changed. Return the
   * [[AggregateCall]] indexes and the corresponding inferred types.
   */
  private def getIndexAndInferredTypesIfChanged(
      agg: LogicalAggregate)
    : Map[Int, RelDataType] = {

    agg.getAggCallList.zipWithIndex.flatMap {
      case (aggCall, index) =>
        val origType = aggCall.`type`
        val aggCallBinding = new Aggregate.AggCallBinding(
          agg.getCluster.getTypeFactory,
          aggCall.getAggregation,
          SqlTypeUtil.projectTypes(agg.getInput.getRowType, aggCall.getArgList),
          0,
          aggCall.hasFilter)
        val inferredType = aggCall.getAggregation.inferReturnType(aggCallBinding)

        if (origType != inferredType && agg.getGroupCount == 1) {
          Some(index, inferredType)
        } else {
          None
        }
    }.toMap
  }

  private[table] def getWindowExpressions(agg: LogicalAggregate): Seq[(RexCall, Int)] = {
    val project = agg.getInput.asInstanceOf[HepRelVertex].getCurrentRel.asInstanceOf[LogicalProject]
    val groupKeys = agg.getGroupSet

    // get grouping expressions
    val groupExpr = project.getProjects.zipWithIndex.filter(p => groupKeys.get(p._2))

    // filter grouping expressions for window expressions
    groupExpr.filter { g =>
      g._1 match {
        case call: RexCall =>
          call.getOperator match {
            case FlinkSqlOperatorTable.TUMBLE =>
              if (call.getOperands.size() == 2) {
                true
              } else {
                throw new TableException("TUMBLE window with alignment is not supported yet.")
              }
            case FlinkSqlOperatorTable.HOP =>
              if (call.getOperands.size() == 3) {
                true
              } else {
                throw new TableException("HOP window with alignment is not supported yet.")
              }
            case FlinkSqlOperatorTable.SESSION =>
              if (call.getOperands.size() == 2) {
                true
              } else {
                throw new TableException("SESSION window with alignment is not supported yet.")
              }
            case _ => false
          }
        case _ => false
      }
    }.map(w => (w._1.asInstanceOf[RexCall], w._2))
  }

  /** Returns the expression that replaces the window expression before the aggregation. */
  private[table] def getInAggregateGroupExpression(
      rexBuilder: RexBuilder,
      windowExpression: RexCall): RexNode

  /** Returns the expression that replaces the window expression after the aggregation. */
  private[table] def getOutAggregateGroupExpression(
      rexBuilder: RexBuilder,
      windowExpression: RexCall): RexNode

  /** translate the group window expression in to a Flink Table window. */
  private[table] def translateWindow(
      windowExpr: RexCall,
      windowExprIdx: Int,
      rowType: RelDataType): LogicalWindow = {

    val timeField = getTimeFieldReference(windowExpr.getOperands.get(0), windowExprIdx, rowType)
    val resultType = Some(fromDataTypeToLogicalType(timeField.getOutputDataType))
    val windowRef = PlannerWindowReference("w$", resultType)
    windowExpr.getOperator match {
      case FlinkSqlOperatorTable.TUMBLE =>
        val interval = getOperandAsLong(windowExpr, 1)
        TumblingGroupWindow(
          windowRef,
          timeField,
          intervalOfMillis(interval))

      case FlinkSqlOperatorTable.HOP =>
        val (slide, size) = (getOperandAsLong(windowExpr, 1), getOperandAsLong(windowExpr, 2))
        SlidingGroupWindow(
          windowRef,
          timeField,
          intervalOfMillis(size),
          intervalOfMillis(slide))

      case FlinkSqlOperatorTable.SESSION =>
        val gap = getOperandAsLong(windowExpr, 1)
        SessionGroupWindow(
          windowRef,
          timeField,
          intervalOfMillis(gap))
    }
  }

  /**
    * get time field expression
    */
  private[table] def getTimeFieldReference(
      operand: RexNode,
      windowExprIdx: Int,
      rowType: RelDataType): FieldReferenceExpression

  /**
    * get operand value as Long type
    */
  def getOperandAsLong(call: RexCall, idx: Int): Long
}
