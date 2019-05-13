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

package org.apache.flink.table.plan.rules.dataSet

import java.math.BigDecimal

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex._
import org.apache.flink.table.api.TableException
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.expressions._
import org.apache.flink.table.plan.logical.{LogicalWindow, SessionGroupWindow, SlidingGroupWindow, TumblingGroupWindow}
import org.apache.flink.table.plan.rules.common.LogicalWindowAggregateRule
import org.apache.flink.table.typeutils.TimeIntervalTypeInfo
import org.apache.flink.table.validate.BasicOperatorTable

class DataSetLogicalWindowAggregateRule
  extends LogicalWindowAggregateRule("DataSetLogicalWindowAggregateRule") {

  /** Returns the operand of the group window function. */
  override private[table] def getInAggregateGroupExpression(
      rexBuilder: RexBuilder,
      windowExpression: RexCall): RexNode = windowExpression.getOperands.get(0)

  /** Returns a zero literal of the correct type. */
  override private[table] def getOutAggregateGroupExpression(
      rexBuilder: RexBuilder,
      windowExpression: RexCall): RexNode = {

    val literalType = windowExpression.getOperands.get(0).getType
    rexBuilder.makeZeroLiteral(literalType)
  }

  override private[table] def translateWindowExpression(
      windowExpr: RexCall,
      rowType: RelDataType): LogicalWindow = {

    def getOperandAsLong(call: RexCall, idx: Int): Long =
      call.getOperands.get(idx) match {
        case v: RexLiteral => v.getValue.asInstanceOf[BigDecimal].longValue()
        case _ => throw new TableException("Only constant window descriptors are supported")
      }

    def getFieldReference(operand: RexNode): PlannerExpression = {
      operand match {
        case ref: RexInputRef =>
          // resolve field name of window attribute
          val fieldName = rowType.getFieldList.get(ref.getIndex).getName
          val fieldType = rowType.getFieldList.get(ref.getIndex).getType
          PlannerResolvedFieldReference(fieldName, FlinkTypeFactory.toTypeInfo(fieldType))
      }
    }

    val timeField = getFieldReference(windowExpr.getOperands.get(0))
    windowExpr.getOperator match {
      case BasicOperatorTable.TUMBLE =>
        val interval = getOperandAsLong(windowExpr, 1)
        TumblingGroupWindow(
          WindowReference("w$", Some(timeField.resultType)),
          timeField,
          Literal(interval, TimeIntervalTypeInfo.INTERVAL_MILLIS)
        )

      case BasicOperatorTable.HOP =>
        val (slide, size) = (getOperandAsLong(windowExpr, 1), getOperandAsLong(windowExpr, 2))
        SlidingGroupWindow(
          WindowReference("w$", Some(timeField.resultType)),
          timeField,
          Literal(size, TimeIntervalTypeInfo.INTERVAL_MILLIS),
          Literal(slide, TimeIntervalTypeInfo.INTERVAL_MILLIS)
        )

      case BasicOperatorTable.SESSION =>
        val gap = getOperandAsLong(windowExpr, 1)
        SessionGroupWindow(
          WindowReference("w$", Some(timeField.resultType)),
          timeField,
          Literal(gap, TimeIntervalTypeInfo.INTERVAL_MILLIS)
        )
    }
  }
}

object DataSetLogicalWindowAggregateRule {
  val INSTANCE = new DataSetLogicalWindowAggregateRule
}
