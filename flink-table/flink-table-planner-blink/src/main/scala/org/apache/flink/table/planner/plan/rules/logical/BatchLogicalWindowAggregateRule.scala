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

import org.apache.flink.table.expressions.FieldReferenceExpression
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalWindowAggregate
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromLogicalTypeToDataType

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.logical.{LogicalAggregate, LogicalProject}
import org.apache.calcite.rex._

/**
  * Planner rule that transforms simple [[LogicalAggregate]] on a [[LogicalProject]]
  * with windowing expression to [[LogicalWindowAggregate]] for batch.
  */
class BatchLogicalWindowAggregateRule
  extends LogicalWindowAggregateRuleBase("BatchLogicalWindowAggregateRule") {

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

  private[table] override def getTimeFieldReference(
      operand: RexNode,
      windowExprIdx: Int,
      rowType: RelDataType): FieldReferenceExpression = {
    operand match {
      // match TUMBLE_ROWTIME and TUMBLE_PROCTIME
      case c: RexCall if c.getOperands.size() == 1 &&
        FlinkTypeFactory.isTimeIndicatorType(c.getType) =>
        new FieldReferenceExpression(
          rowType.getFieldList.get(windowExprIdx).getName,
          fromLogicalTypeToDataType(toLogicalType(c.getType)),
          0, // only one input, should always be 0
          windowExprIdx)
      case ref: RexInputRef =>
        // resolve field name of window attribute
        val fieldName = rowType.getFieldList.get(ref.getIndex).getName
        val fieldType = rowType.getFieldList.get(ref.getIndex).getType
        new FieldReferenceExpression(
          fieldName,
          fromLogicalTypeToDataType(toLogicalType(fieldType)),
          0, // only one input, should always be 0
          ref.getIndex)
    }
  }
}

object BatchLogicalWindowAggregateRule {
  val INSTANCE = new BatchLogicalWindowAggregateRule
}
