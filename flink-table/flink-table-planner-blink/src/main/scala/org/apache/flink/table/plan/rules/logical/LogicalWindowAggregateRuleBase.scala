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

import org.apache.flink.table.`type`.TypeConverters.createInternalTypeFromTypeInfo
import org.apache.flink.table.api._
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.expressions.{FieldReferenceExpression, ValueLiteralExpression, WindowReference}
import org.apache.flink.table.functions.sql.FlinkSqlOperatorTable
import org.apache.flink.table.plan.logical.{LogicalWindow, SessionGroupWindow, SlidingGroupWindow, TumblingGroupWindow}
import org.apache.flink.table.plan.nodes.calcite.LogicalWindowAggregate
import org.apache.flink.table.typeutils.TimeIntervalTypeInfo.INTERVAL_MILLIS

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan._
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Aggregate.Group
import org.apache.calcite.rel.logical.{LogicalAggregate, LogicalProject}
import org.apache.calcite.rex._
import org.apache.calcite.util.ImmutableBitSet

import _root_.java.math.BigDecimal

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

    // we don't use the builder here because it uses RelMetadataQuery which affects the plan
    val newAgg = LogicalAggregate.create(
      newProject,
      agg.indicator,
      newGroupSet,
      ImmutableList.of(newGroupSet),
      agg.getAggCallList)

    // create an additional project to conform with types
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
    val transformed = call.builder()
    val windowAgg = LogicalWindowAggregate.create(
      window,
      Seq[NamedWindowProperty](),
      newAgg)
    // The transformation adds an additional LogicalProject at the top to ensure
    // that the types are equivalent.
    transformed.push(windowAgg)
      .project(transformed.fields().patch(windowExprIdx, Seq(outAggGroupExpression), 0))

    val result = transformed.build()
    call.transformTo(result)
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
    def getOperandAsLong(call: RexCall, idx: Int): Long =
      call.getOperands.get(idx) match {
        case v: RexLiteral => v.getValue.asInstanceOf[BigDecimal].longValue()
        case _ => throw new TableException("Only constant window descriptors are supported")
      }

    val timeField = getTimeFieldReference(windowExpr.getOperands.get(0), windowExprIdx, rowType)
    val resultType = Some(createInternalTypeFromTypeInfo(timeField.getResultType))
    val windowRef = WindowReference("w$", resultType)
    windowExpr.getOperator match {
      case FlinkSqlOperatorTable.TUMBLE =>
        val interval = getOperandAsLong(windowExpr, 1)
        TumblingGroupWindow(
          windowRef,
          timeField,
          new ValueLiteralExpression(interval, INTERVAL_MILLIS))

      case FlinkSqlOperatorTable.HOP =>
        val (slide, size) = (getOperandAsLong(windowExpr, 1), getOperandAsLong(windowExpr, 2))
        SlidingGroupWindow(
          windowRef,
          timeField,
          new ValueLiteralExpression(size, INTERVAL_MILLIS),
          new ValueLiteralExpression(slide, INTERVAL_MILLIS))

      case FlinkSqlOperatorTable.SESSION =>
        val gap = getOperandAsLong(windowExpr, 1)
        SessionGroupWindow(
          windowRef,
          timeField,
          new ValueLiteralExpression(gap, INTERVAL_MILLIS))
    }
  }

  /**
    * get time field expression
    */
  private[table] def getTimeFieldReference(
      operand: RexNode,
      windowExprIdx: Int,
      rowType: RelDataType): FieldReferenceExpression
}
