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

import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.expressions.FieldReferenceExpression
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromLogicalTypeToDataType

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.logical.{LogicalAggregate, LogicalProject}
import org.apache.calcite.rex._
import org.apache.calcite.sql.`type`.SqlTypeName

import _root_.java.math.{BigDecimal => JBigDecimal}

/**
  * Planner rule that transforms simple [[LogicalAggregate]] on a [[LogicalProject]]
  * with windowing expression to
 * [[org.apache.flink.table.planner.plan.nodes.calcite.LogicalWindowAggregate]]
 * for batch.
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
    // Create a literal with normal SqlTypeName.TIMESTAMP
    // in case we reference a rowtime field.
    rexBuilder.makeLiteral(
      0L,
      rexBuilder.getTypeFactory.createSqlType(
        SqlTypeName.TIMESTAMP, windowExpression.getType.getPrecision),
      true)
  }

  private[table] override def getTimeFieldReference(
      operand: RexNode,
      timeAttributeIndex: Int,
      rowType: RelDataType): FieldReferenceExpression = {
    if (FlinkTypeFactory.isProctimeIndicatorType(operand.getType)) {
      throw new ValidationException("Window can not be defined over "
        + "a proctime attribute column for batch mode")
    }

    val fieldName = rowType.getFieldList.get(timeAttributeIndex).getName
    val fieldType = rowType.getFieldList.get(timeAttributeIndex).getType
    new FieldReferenceExpression(
      fieldName,
      fromLogicalTypeToDataType(toLogicalType(fieldType)),
      0,
      timeAttributeIndex)
  }

  def getOperandAsLong(call: RexCall, idx: Int): Long =
    call.getOperands.get(idx) match {
      case v: RexLiteral => v.getValue.asInstanceOf[JBigDecimal].longValue()
      case _ => throw new TableException("Only constant window descriptors are supported")
    }
}

object BatchLogicalWindowAggregateRule {
  val INSTANCE = new BatchLogicalWindowAggregateRule
}
