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
package org.apache.flink.table.plan.rules.common

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan._
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.logical.{LogicalAggregate, LogicalProject}
import org.apache.calcite.rex._
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.table.api._
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.plan.logical.rel.LogicalWindowAggregate
import org.apache.flink.table.validate.BasicOperatorTable

import _root_.scala.collection.JavaConversions._

abstract class LogicalWindowAggregateRule(ruleName: String)
  extends RelOptRule(
    RelOptRule.operand(classOf[LogicalAggregate],
      RelOptRule.operand(classOf[LogicalProject], RelOptRule.none())),
    ruleName) {

  override def matches(call: RelOptRuleCall): Boolean = {
    val agg = call.rel(0).asInstanceOf[LogicalAggregate]

    val groupSets = agg.getGroupSets.size() != 1 || agg.getGroupSets.get(0) != agg.getGroupSet

    val windowExpressions = getWindowExpressions(agg)
    if (windowExpressions.length > 1) {
      throw new TableException("Only a single window group function may be used in GROUP BY")
    }

    !groupSets && !agg.indicator && windowExpressions.nonEmpty
  }

  /**
    * Transform LogicalAggregate with windowing expression to LogicalProject
    * + LogicalWindowAggregate + LogicalProject.
    *
    * The transformation adds an additional LogicalProject at the top to ensure
    * that the types are equivalent.
    */
  override def onMatch(call: RelOptRuleCall): Unit = {
    val agg = call.rel[LogicalAggregate](0)
    val project = agg.getInput.asInstanceOf[HepRelVertex].getCurrentRel.asInstanceOf[LogicalProject]

    val (windowExpr, windowExprIdx) = getWindowExpressions(agg).head
    val window = translateWindowExpression(windowExpr, project.getInput.getRowType)

    val rexBuilder = call.builder().getRexBuilder

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
    val outAggGroupExpression = getOutAggregateGroupExpression(rexBuilder, windowExpr)
    val transformed = call.builder()
    transformed.push(LogicalWindowAggregate.create(
      window.toLogicalWindow,
      Seq[NamedWindowProperty](),
      newAgg))
      .project(transformed.fields().patch(windowExprIdx, Seq(outAggGroupExpression), 0))

    call.transformTo(transformed.build())
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
            case BasicOperatorTable.TUMBLE =>
              if (call.getOperands.size() == 2) {
                true
              } else {
                throw TableException("TUMBLE window with alignment is not supported yet.")
              }
            case BasicOperatorTable.HOP =>
              if (call.getOperands.size() == 3) {
                true
              } else {
                throw TableException("HOP window with alignment is not supported yet.")
              }
            case BasicOperatorTable.SESSION =>
              if (call.getOperands.size() == 2) {
                true
              } else {
                throw TableException("SESSION window with alignment is not supported yet.")
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
  private[table] def translateWindowExpression(windowExpr: RexCall, rowType: RelDataType): Window

}
