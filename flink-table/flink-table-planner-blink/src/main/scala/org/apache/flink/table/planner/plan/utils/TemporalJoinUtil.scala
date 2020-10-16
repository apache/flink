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
package org.apache.flink.table.planner.plan.utils

import org.apache.calcite.rel.core.JoinInfo
import org.apache.calcite.rex._
import org.apache.calcite.sql.`type`.{OperandTypes, ReturnTypes}
import org.apache.calcite.sql.{SqlFunction, SqlFunctionCategory, SqlKind}

import scala.collection.JavaConversions._

/**
  * Utilities for temporal table join.
  */
object TemporalJoinUtil {

  // ----------------------------------------------------------------------------------------
  //                          Temporal Join Condition Utilities
  // ----------------------------------------------------------------------------------------

  /**
    * [[TEMPORAL_JOIN_CONDITION]] is a specific join condition which correctly defines
    * references to rightTimeAttribute, rightPrimaryKeyExpression and leftTimeAttribute.
    * The condition is used to mark this is a temporal table join and ensure columns these
    * expressions depends on will not be pruned. The join key pair is necessary to ensure the
    * the condition will not push down.
    *
    * The rightTimeAttribute, rightPrimaryKeyExpression and leftTimeAttribute will be
    * extracted from the condition in physical phase.
    */
  val TEMPORAL_JOIN_CONDITION = new SqlFunction(
    "__TEMPORAL_JOIN_CONDITION",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.BOOLEAN_NOT_NULL,
    null,
    OperandTypes.or(
      // right time attribute and primary key are required in event-time temporal table join,
      OperandTypes.sequence(
        "'(LEFT_TIME_ATTRIBUTE, RIGHT_TIME_ATTRIBUTE, LEFT_KEY, RIGHT_KEY, PRIMARY_KEY)'",
        OperandTypes.DATETIME,
        OperandTypes.DATETIME,
        OperandTypes.ANY,
        OperandTypes.ANY,
        OperandTypes.ANY),
      // the primary key may inferred later in event-time temporal table join,
      OperandTypes.sequence(
        "'(LEFT_TIME_ATTRIBUTE, RIGHT_TIME_ATTRIBUTE, LEFT_KEY, RIGHT_KEY)'",
        OperandTypes.DATETIME,
        OperandTypes.DATETIME,
        OperandTypes.ANY,
        OperandTypes.ANY),
      // Only left time attribute is required for processing-time temporal table join,
      // primary key is optional
      OperandTypes.sequence(
        "'(LEFT_TIME_ATTRIBUTE, LEFT_KEY, RIGHT_KEY)'",
        OperandTypes.DATETIME,
        OperandTypes.ANY,
        OperandTypes.ANY,
        OperandTypes.ANY)),
    SqlFunctionCategory.SYSTEM)

  val TEMPORAL_JOIN_LEFT_KEY = new SqlFunction(
    "__TEMPORAL_JOIN_LEFT_KEY",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.BOOLEAN_NOT_NULL,
    null,
    OperandTypes.ARRAY,
    SqlFunctionCategory.SYSTEM)

  val TEMPORAL_JOIN_RIGHT_KEY = new SqlFunction(
    "__TEMPORAL_JOIN_RIGHT_KEY",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.BOOLEAN_NOT_NULL,
    null,
    OperandTypes.ARRAY,
    SqlFunctionCategory.SYSTEM)

  val TEMPORAL_JOIN_CONDITION_PRIMARY_KEY = new SqlFunction(
    "__TEMPORAL_JOIN_CONDITION_PRIMARY_KEY",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.BOOLEAN_NOT_NULL,
    null,
    OperandTypes.ARRAY,
    SqlFunctionCategory.SYSTEM)


  def makeRowTimeTemporalJoinConditionCall(
    rexBuilder: RexBuilder,
    leftTimeAttribute: RexNode,
    rightTimeAttribute: RexNode,
    leftJoinKeyExpression: Seq[RexNode],
    rightJoinKeyExpression: Seq[RexNode],
    rightPrimaryKeyExpression: Seq[RexNode]): RexNode = {
    rexBuilder.makeCall(
      TEMPORAL_JOIN_CONDITION,
      leftTimeAttribute,
      rightTimeAttribute,
      makeLeftJoinKeyCall(rexBuilder, leftJoinKeyExpression),
      makeRightJoinKeyCall(rexBuilder, rightJoinKeyExpression),
      makePrimaryKeyCall(rexBuilder, rightPrimaryKeyExpression))
  }

  def makeRowTimeTemporalJoinConditionCall(
      rexBuilder: RexBuilder,
      leftTimeAttribute: RexNode,
      rightTimeAttribute: RexNode,
      leftJoinKeyExpression: Seq[RexNode],
      rightJoinKeyExpression: Seq[RexNode]): RexNode = {
    rexBuilder.makeCall(
      TEMPORAL_JOIN_CONDITION,
      leftTimeAttribute,
      rightTimeAttribute,
      makeLeftJoinKeyCall(rexBuilder, leftJoinKeyExpression),
      makeRightJoinKeyCall(rexBuilder, rightJoinKeyExpression))
  }

  private def makePrimaryKeyCall(
      rexBuilder: RexBuilder,
      rightPrimaryKeyExpression: Seq[RexNode]): RexNode = {
    rexBuilder.makeCall(
      TEMPORAL_JOIN_CONDITION_PRIMARY_KEY,
      rightPrimaryKeyExpression)
  }

  private def makeLeftJoinKeyCall(
      rexBuilder: RexBuilder,
      keyExpression: Seq[RexNode]): RexNode = {
    rexBuilder.makeCall(
      TEMPORAL_JOIN_LEFT_KEY,
      keyExpression)
  }

  private def makeRightJoinKeyCall(
      rexBuilder: RexBuilder,
      keyExpression: Seq[RexNode]): RexNode = {
    rexBuilder.makeCall(
      TEMPORAL_JOIN_RIGHT_KEY,
      keyExpression)
  }

  def makeProcTimeTemporalJoinConditionCall(
      rexBuilder: RexBuilder,
      leftTimeAttribute: RexNode,
      leftJoinKeyExpression: Seq[RexNode],
      rightJoinKeyExpression: Seq[RexNode]): RexNode = {
    rexBuilder.makeCall(
      TEMPORAL_JOIN_CONDITION,
      leftTimeAttribute,
      makeLeftJoinKeyCall(rexBuilder, leftJoinKeyExpression),
      makeRightJoinKeyCall(rexBuilder, rightJoinKeyExpression))
  }

  def containsTemporalJoinCondition(condition: RexNode): Boolean = {
    var hasTemporalJoinCondition: Boolean = false
    condition.accept(new RexVisitorImpl[Void](true) {
      override def visitCall(call: RexCall): Void = {
        if (call.getOperator != TEMPORAL_JOIN_CONDITION) {
          super.visitCall(call)
        } else {
          hasTemporalJoinCondition = true
          null
        }
      }
    })
    hasTemporalJoinCondition
  }

  def isRowTimeJoin(rexBuilder: RexBuilder, joinInfo: JoinInfo): Boolean = {
    val nonEquiJoinRex = joinInfo.getRemaining(rexBuilder)

    var rowtimeJoin: Boolean = false
    val visitor = new RexVisitorImpl[Unit](true) {
      override def visitCall(call: RexCall): Unit = {
        if (isRowTimeTemporalJoinConditionCall(call)) {
           rowtimeJoin = true
        } else {
          super.visitCall(call)
        }
      }
    }
    nonEquiJoinRex.accept(visitor)
    rowtimeJoin
  }

  def isRowTimeTemporalJoinConditionCall(rexCall: RexCall): Boolean = {
    rexCall.getOperator == TEMPORAL_JOIN_CONDITION && rexCall.operands.length > 3
  }

}
