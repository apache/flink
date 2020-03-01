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

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.expressions.{FieldReferenceExpression, _}
import org.apache.flink.table.functions.{TemporalTableFunction, TemporalTableFunctionImpl}
import org.apache.flink.table.operations.QueryOperation
import org.apache.flink.table.planner.calcite.FlinkRelBuilder
import org.apache.flink.table.planner.functions.utils.TableSqlFunction
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecTemporalJoin
import org.apache.flink.table.planner.plan.utils.TemporalJoinUtil.{makeProcTimeTemporalJoinConditionCall, makeRowTimeTemporalJoinConditionCall}
import org.apache.flink.table.planner.plan.utils.{ExpandTableScanShuttle, RexDefaultVisitor}
import org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks.{hasRoot, isProctimeAttribute}
import org.apache.flink.util.Preconditions.checkState

import org.apache.calcite.plan.RelOptRule.{any, none, operand, some}
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptSchema}
import org.apache.calcite.rel.{BiRel, RelNode, SingleRel}
import org.apache.calcite.rel.core.{JoinRelType, TableFunctionScan}
import org.apache.calcite.rel.logical.LogicalCorrelate
import org.apache.calcite.rex._

/**
  * The initial temporal TableFunction join (LATERAL TemporalTableFunction(o.proctime)) is
  * a correlate. Rewrite it into a Join with a special temporal join condition wraps time
  * attribute and primary key information. The join will be translated into
  * [[StreamExecTemporalJoin]] in physical.
  */
class LogicalCorrelateToJoinFromTemporalTableFunctionRule
  extends RelOptRule(
    operand(classOf[LogicalCorrelate],
      some(operand(classOf[RelNode], any()),
        operand(classOf[TableFunctionScan], none()))),
    "LogicalCorrelateToJoinFromTemporalTableFunctionRule") {

  private def extractNameFromTimeAttribute(timeAttribute: Expression): String = {
    timeAttribute match {
      case f : FieldReferenceExpression
        if hasRoot(f.getOutputDataType.getLogicalType, TIMESTAMP_WITHOUT_TIME_ZONE) =>
        f.getName
      case _ => throw new ValidationException(
        s"Invalid timeAttribute [$timeAttribute] in TemporalTableFunction")
    }
  }

  private def isProctimeReference(temporalTableFunction: TemporalTableFunctionImpl): Boolean = {
    val fieldRef = temporalTableFunction.getTimeAttribute.asInstanceOf[FieldReferenceExpression]
    isProctimeAttribute(fieldRef.getOutputDataType.getLogicalType)
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
        val underlyingHistoryTable: QueryOperation = rightTemporalTableFunction
          .getUnderlyingHistoryTable
        val rexBuilder = cluster.getRexBuilder

        val relBuilder = FlinkRelBuilder.of(cluster, getRelOptSchema(leftNode))
        val temporalTable: RelNode = relBuilder.queryOperation(underlyingHistoryTable).build()
        // expand QueryOperationCatalogViewTable in TableScan
        val shuttle = new ExpandTableScanShuttle
        val rightNode = temporalTable.accept(shuttle)

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

        relBuilder.push(leftNode)
        relBuilder.push(rightNode)

        val condition =
          if (isProctimeReference(rightTemporalTableFunction)) {
            makeProcTimeTemporalJoinConditionCall(
              rexBuilder,
              leftTimeAttribute,
              rightPrimaryKeyExpression)
          } else {
            makeRowTimeTemporalJoinConditionCall(
              rexBuilder,
              leftTimeAttribute,
              rightTimeIndicatorExpression,
              rightPrimaryKeyExpression)
          }
        relBuilder.join(JoinRelType.INNER, condition)

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

  /**
    * Gets [[RelOptSchema]] from the leaf [[RelNode]] which holds a non-null [[RelOptSchema]].
    */
  private def getRelOptSchema(relNode: RelNode): RelOptSchema = relNode match {
    case hep: HepRelVertex => getRelOptSchema(hep.getCurrentRel)
    case single: SingleRel => getRelOptSchema(single.getInput)
    case bi: BiRel => getRelOptSchema(bi.getLeft)
    case _ => relNode.getTable.getRelOptSchema
  }
}

object LogicalCorrelateToJoinFromTemporalTableFunctionRule {
  val INSTANCE: RelOptRule = new LogicalCorrelateToJoinFromTemporalTableFunctionRule
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

    if (!tableFunction.udtf.isInstanceOf[TemporalTableFunction]) {
      return null
    }
    val temporalTableFunction =
      tableFunction.udtf.asInstanceOf[TemporalTableFunctionImpl]

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
