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

import org.apache.calcite.plan.RelOptRule.{any, none, operand, some}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.TableFunctionScan
import org.apache.calcite.rel.logical.LogicalCorrelate
import org.apache.calcite.rex._
import org.apache.flink.table.api.{Types, ValidationException}
import org.apache.flink.table.calcite.FlinkTypeFactory.{isProctimeIndicatorType, isTimeIndicatorType}
import org.apache.flink.table.expressions.{FieldReferenceExpression, _}
import org.apache.flink.table.functions.utils.TableSqlFunction
import org.apache.flink.table.functions.{TemporalTableFunction, TemporalTableFunctionImpl}
import org.apache.flink.table.operations.TableOperation
import org.apache.flink.table.plan.logical.LogicalNode
import org.apache.flink.table.plan.logical.rel.LogicalTemporalTableJoin
import org.apache.flink.table.plan.util.RexDefaultVisitor
import org.apache.flink.util.Preconditions.checkState

class LogicalCorrelateToTemporalTableJoinRule
  extends RelOptRule(
    operand(classOf[LogicalCorrelate],
      some(
        operand(classOf[RelNode], any()),
        operand(classOf[TableFunctionScan], none()))),
    "LogicalCorrelateToTemporalTableJoinRule") {

  private def extractNameFromTimeAttribute(timeAttribute: Expression): String = {
    timeAttribute match {
      case f : FieldReferenceExpression
        if f.getResultType == Types.LONG ||
          f.getResultType == Types.SQL_TIMESTAMP ||
          isTimeIndicatorType(f.getResultType) =>
        f.getName
      case _ => throw new ValidationException(
        s"Invalid timeAttribute [$timeAttribute] in TemporalTableFunction")
    }
  }

  private def extractNameFromPrimaryKeyAttribute(expression: Expression): String = {
    expression match {
      case f: FieldReferenceExpression =>
        f.getName
      case _ => throw new ValidationException(
        s"Unsupported expression [$expression] as primary key. " +
          s"Only top-level (not nested) field references are supported.")
    }
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val logicalCorrelate: LogicalCorrelate = call.rel(0)
    val leftNode: RelNode = call.rel(1)
    val rightTableFunctionScan: TableFunctionScan = call.rel(2)

    val cluster = logicalCorrelate.getCluster

    new GetTemporalTableFunctionCall(cluster.getRexBuilder, leftNode)
      .visit(rightTableFunctionScan.getCall) match {
      case None =>
        // Do nothing and handle standard TableFunction
      case Some(TemporalTableFunctionCall(
        rightTemporalTableFunction: TemporalTableFunctionImpl, leftTimeAttribute)) =>

        // If TemporalTableFunction was found, rewrite LogicalCorrelate to TemporalJoin
        val underlyingHistoryTable: TableOperation = rightTemporalTableFunction
          .getUnderlyingHistoryTable
        val relBuilder = this.relBuilderFactory.create(
          cluster,
          leftNode.getTable.getRelOptSchema)
        val rexBuilder = cluster.getRexBuilder

        val rightNode: RelNode = underlyingHistoryTable.asInstanceOf[LogicalNode]
          .toRelNode(relBuilder)

        val rightTimeIndicatorExpression = createRightExpression(
          rexBuilder,
          leftNode,
          rightNode,
          extractNameFromTimeAttribute(rightTemporalTableFunction.getTimeAttribute))

        val rightPrimaryKeyExpression = createRightExpression(
          rexBuilder,
          leftNode,
          rightNode,
          extractNameFromPrimaryKeyAttribute(rightTemporalTableFunction.getPrimaryKey))

        relBuilder.push(
          if (isProctimeIndicatorType(rightTemporalTableFunction.getTimeAttribute
            .asInstanceOf[FieldReferenceExpression].getResultType)) {
            LogicalTemporalTableJoin.createProctime(
              rexBuilder,
              cluster,
              logicalCorrelate.getTraitSet,
              leftNode,
              rightNode,
              leftTimeAttribute,
              rightPrimaryKeyExpression)
          }
          else {
            LogicalTemporalTableJoin.createRowtime(
              rexBuilder,
              cluster,
              logicalCorrelate.getTraitSet,
              leftNode,
              rightNode,
              leftTimeAttribute,
              rightTimeIndicatorExpression,
              rightPrimaryKeyExpression)
          })
        call.transformTo(relBuilder.build())
    }
  }

  private def createRightExpression(
      rexBuilder: RexBuilder,
      leftNode: RelNode,
      rightNode: RelNode,
      field: String): RexNode = {
    val rightReferencesOffset = leftNode.getRowType.getFieldCount
    val rightDataTypeField = rightNode.getRowType.getField(field, false, false)
    rexBuilder.makeInputRef(
      rightDataTypeField.getType, rightReferencesOffset + rightDataTypeField.getIndex)
  }
}

object LogicalCorrelateToTemporalTableJoinRule {
  val INSTANCE: RelOptRule = new LogicalCorrelateToTemporalTableJoinRule
}

/**
  * Simple pojo class for extracted [[TemporalTableFunction]] with time attribute
  * extracted from RexNode with [[TemporalTableFunction]] call.
  */
case class TemporalTableFunctionCall(
    var temporalTableFunction: TemporalTableFunction,
    var timeAttribute: RexNode) {
}

/**
  * Find [[TemporalTableFunction]] call and run [[CorrelatedFieldAccessRemoval]] on it's operand.
  */
class GetTemporalTableFunctionCall(
    var rexBuilder: RexBuilder,
    var leftSide: RelNode)
  extends RexVisitorImpl[TemporalTableFunctionCall](false) {

  def visit(node: RexNode): Option[TemporalTableFunctionCall] = {
    val result = node.accept(this)
    if (result == null) {
      return None
    }
    Some(result)
  }

  override def visitCall(rexCall: RexCall): TemporalTableFunctionCall = {
    if (!rexCall.getOperator.isInstanceOf[TableSqlFunction]) {
      return null
    }
    val tableFunction = rexCall.getOperator.asInstanceOf[TableSqlFunction]

    if (!tableFunction.getTableFunction.isInstanceOf[TemporalTableFunction]) {
      return null
    }
    val temporalTableFunction =
      tableFunction.getTableFunction.asInstanceOf[TemporalTableFunctionImpl]

    checkState(
      rexCall.getOperands.size().equals(1),
      "TemporalTableFunction call [%s] must have exactly one argument",
      rexCall)
    val correlatedFieldAccessRemoval =
      new CorrelatedFieldAccessRemoval(temporalTableFunction, rexBuilder, leftSide)
    TemporalTableFunctionCall(
      temporalTableFunction,
      rexCall.getOperands.get(0).accept(correlatedFieldAccessRemoval))
  }
}

/**
  * This converts field accesses like `$cor0.o_rowtime` to valid input references
  * for join condition context without `$cor` reference.
  */
class CorrelatedFieldAccessRemoval(
    var temporalTableFunction: TemporalTableFunctionImpl,
    var rexBuilder: RexBuilder,
    var leftSide: RelNode) extends RexDefaultVisitor[RexNode] {

  override def visitFieldAccess(fieldAccess: RexFieldAccess): RexNode = {
    val leftIndex = leftSide.getRowType.getFieldList.indexOf(fieldAccess.getField)
    if (leftIndex < 0) {
      throw new IllegalStateException(
        s"Failed to find reference to field [${fieldAccess.getField}] in node [$leftSide]")
    }
    rexBuilder.makeInputRef(leftSide, leftIndex)
  }

  override def visitInputRef(inputRef: RexInputRef): RexNode = {
    inputRef
  }

  override def visitNode(rexNode: RexNode): RexNode = {
    throw new ValidationException(
      s"Unsupported argument [$rexNode] " +
        s"in ${classOf[TemporalTableFunction].getSimpleName} call of " +
        s"[${temporalTableFunction.getUnderlyingHistoryTable}] table")
  }
}
