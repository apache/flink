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

package org.apache.flink.table.plan.nodes.datastream

import org.apache.calcite.rel.core.{JoinInfo, JoinRelType}
import org.apache.calcite.rex._
import org.apache.flink.api.common.functions.FlatJoinFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator
import org.apache.flink.table.api.{StreamQueryConfig, TableConfig, ValidationException}
import org.apache.flink.table.calcite.FlinkTypeFactory._
import org.apache.flink.table.codegen.GeneratedFunction
import org.apache.flink.table.plan.logical.rel.LogicalTemporalTableJoin
import org.apache.flink.table.plan.logical.rel.LogicalTemporalTableJoin._
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.plan.util.RexDefaultVisitor
import org.apache.flink.table.runtime.join.{TemporalProcessTimeJoin, TemporalRowtimeJoin}
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row
import org.apache.flink.util.Preconditions.checkState

/**
  * @param rightTimeAttributeInputReference is defined only for event time joins.
  */
class DataStreamTemporalJoinToCoProcessTranslator private (
    textualRepresentation: String,
    config: TableConfig,
    returnType: TypeInformation[Row],
    leftSchema: RowSchema,
    rightSchema: RowSchema,
    joinInfo: JoinInfo,
    rexBuilder: RexBuilder,
    leftTimeAttributeInputReference: Int,
    rightTimeAttributeInputReference: Option[Int],
    remainingNonEquiJoinPredicates: RexNode)
  extends DataStreamJoinToCoProcessTranslator(
    config,
    returnType,
    leftSchema,
    rightSchema,
    joinInfo,
    rexBuilder) {

  override val nonEquiJoinPredicates: Option[RexNode] = Some(remainingNonEquiJoinPredicates)

  override protected def createJoinOperator(
      joinType: JoinRelType,
      queryConfig: StreamQueryConfig,
      joinFunction: GeneratedFunction[FlatJoinFunction[Row, Row, Row], Row])
    : TwoInputStreamOperator[CRow, CRow, CRow] = {

    joinType match {
      case JoinRelType.INNER =>
        if (rightTimeAttributeInputReference.isDefined) {
          new TemporalRowtimeJoin(
            leftSchema.typeInfo,
            rightSchema.typeInfo,
            joinFunction.name,
            joinFunction.code,
            queryConfig,
            leftTimeAttributeInputReference,
            rightTimeAttributeInputReference.get)
        }
        else {
          new TemporalProcessTimeJoin(
            leftSchema.typeInfo,
            rightSchema.typeInfo,
            joinFunction.name,
            joinFunction.code,
            queryConfig)
        }
      case _ =>
       throw new ValidationException(
         s"Only ${JoinRelType.INNER} temporal join is supported in [$textualRepresentation]")
    }
  }
}

object DataStreamTemporalJoinToCoProcessTranslator {
  def create(
    textualRepresentation: String,
    config: TableConfig,
    returnType: TypeInformation[Row],
    leftSchema: RowSchema,
    rightSchema: RowSchema,
    joinInfo: JoinInfo,
    rexBuilder: RexBuilder): DataStreamTemporalJoinToCoProcessTranslator = {

    checkState(
      !joinInfo.isEqui,
      "Missing %s in join condition",
      TEMPORAL_JOIN_CONDITION)

    val nonEquiJoinRex: RexNode = joinInfo.getRemaining(rexBuilder)
    val temporalJoinConditionExtractor = new TemporalJoinConditionExtractor(
      textualRepresentation,
      leftSchema.typeInfo.getTotalFields,
      joinInfo,
      rexBuilder)

    val remainingNonEquiJoinPredicates = temporalJoinConditionExtractor.apply(nonEquiJoinRex)

    checkState(
      temporalJoinConditionExtractor.leftTimeAttribute.isDefined &&
        temporalJoinConditionExtractor.rightPrimaryKeyExpression.isDefined,
      "Missing %s in join condition",
      TEMPORAL_JOIN_CONDITION)

    new DataStreamTemporalJoinToCoProcessTranslator(
      textualRepresentation,
      config,
      returnType,
      leftSchema,
      rightSchema,
      joinInfo,
      rexBuilder,
      extractInputReference(
        temporalJoinConditionExtractor.leftTimeAttribute.get,
        textualRepresentation),
      temporalJoinConditionExtractor.rightTimeAttribute.map(
        rightTimeAttribute =>
          extractInputReference(rightTimeAttribute, textualRepresentation) - leftSchema.arity),
      remainingNonEquiJoinPredicates)
  }

  private def extractInputReference(rexNode: RexNode, textualRepresentation: String): Int = {
    val inputReferenceVisitor = new InputReferenceVisitor(textualRepresentation)
    rexNode.accept(inputReferenceVisitor)
    checkState(
      inputReferenceVisitor.inputReference.isDefined,
      "Failed to find input reference in [%s]",
      textualRepresentation)
    inputReferenceVisitor.inputReference.get
  }

  private class TemporalJoinConditionExtractor(
      textualRepresentation: String,
      rightKeysStartingOffset: Int,
      joinInfo: JoinInfo,
      rexBuilder: RexBuilder)

    extends RexShuttle {

    var leftTimeAttribute: Option[RexNode] = None

    var rightTimeAttribute: Option[RexNode] = None

    var rightPrimaryKeyExpression: Option[RexNode] = None

    override def visitCall(call: RexCall): RexNode = {
      if (call.getOperator != TEMPORAL_JOIN_CONDITION) {
        return super.visitCall(call)
      }

      checkState(
        leftTimeAttribute.isEmpty
          && rightPrimaryKeyExpression.isEmpty
          && rightTimeAttribute.isEmpty,
        "Multiple %s functions in [%s]",
        TEMPORAL_JOIN_CONDITION,
        textualRepresentation)

      if (LogicalTemporalTableJoin.isRowtimeCall(call)) {
        leftTimeAttribute = Some(call.getOperands.get(0))
        rightTimeAttribute = Some(call.getOperands.get(1))

        rightPrimaryKeyExpression = Some(validateRightPrimaryKey(call.getOperands.get(2)))

        if (!isRowtimeIndicatorType(rightTimeAttribute.get.getType)) {
          throw new ValidationException(
            s"Non rowtime timeAttribute [${rightTimeAttribute.get.getType}] " +
              s"used to create TemporalTableFunction")
        }
        if (!isRowtimeIndicatorType(leftTimeAttribute.get.getType)) {
          throw new ValidationException(
            s"Non rowtime timeAttribute [${leftTimeAttribute.get.getType}] " +
              s"passed as the argument to TemporalTableFunction")
        }
      }
      else if (LogicalTemporalTableJoin.isProctimeCall(call)) {
        leftTimeAttribute = Some(call.getOperands.get(0))
        rightPrimaryKeyExpression = Some(validateRightPrimaryKey(call.getOperands.get(1)))

        if (!isProctimeIndicatorType(leftTimeAttribute.get.getType)) {
          throw new ValidationException(
            s"Non processing timeAttribute [${leftTimeAttribute.get.getType}] " +
              s"passed as the argument to TemporalTableFunction")
        }
      }
      else {
        throw new IllegalStateException(
          s"Unsupported invocation $call in [$textualRepresentation]")
      }
      rexBuilder.makeLiteral(true)
    }

    private def validateRightPrimaryKey(rightPrimaryKey: RexNode): RexNode = {
      if (joinInfo.rightKeys.size() != 1) {
        throw new ValidationException(
          s"Only single column join key is supported. " +
            s"Found ${joinInfo.rightKeys} in [$textualRepresentation]")
      }
      val rightJoinKeyInputReference = joinInfo.rightKeys.get(0) + rightKeysStartingOffset

      val rightPrimaryKeyInputReference = extractInputReference(
        rightPrimaryKey,
        textualRepresentation)

      if (rightPrimaryKeyInputReference != rightJoinKeyInputReference) {
        throw new ValidationException(
          s"Join key [$rightJoinKeyInputReference] must be the same as " +
            s"temporal table's primary key [$rightPrimaryKey] " +
            s"in [$textualRepresentation]")
      }

      rightPrimaryKey
    }
  }

  /**
    * Extracts input references from RexNode.
    */
  private class InputReferenceVisitor(textualRepresentation: String)
    extends RexDefaultVisitor[RexNode] {

    var inputReference: Option[Int] = None

    override def visitInputRef(inputRef: RexInputRef): RexNode = {
      inputReference = Some(inputRef.getIndex)
      inputRef
    }

    override def visitNode(rexNode: RexNode): RexNode = {
      throw new ValidationException(
        s"Unsupported expression [$rexNode] in [$textualRepresentation]. Expected input reference")
    }
  }
}
