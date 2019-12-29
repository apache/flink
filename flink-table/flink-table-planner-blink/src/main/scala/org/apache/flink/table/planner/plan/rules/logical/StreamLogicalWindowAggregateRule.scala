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
import org.apache.calcite.sql.`type`.{SqlTypeFamily, SqlTypeName}

import _root_.java.math.{BigDecimal => JBigDecimal}

/**
  * Planner rule that transforms simple [[LogicalAggregate]] on a [[LogicalProject]]
  * with windowing expression to
 * [[org.apache.flink.table.planner.plan.nodes.calcite.LogicalWindowAggregate]]
 * for stream.
  */
class StreamLogicalWindowAggregateRule
  extends LogicalWindowAggregateRuleBase("StreamLogicalWindowAggregateRule") {

  /** Returns a reference to the time attribute with a time indicator type */
  override private[table] def getInAggregateGroupExpression(
      rexBuilder: RexBuilder,
      windowExpression: RexCall): RexNode = {

    val timeAttribute = windowExpression.operands.get(0)
    if (!FlinkTypeFactory.isTimeIndicatorType(timeAttribute.getType)) {
      throw new TableException(s"Time attribute expected but ${timeAttribute.getType} encountered.")
    }
    timeAttribute
  }

  /** Returns a zero literal of a timestamp type */
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
      case v: RexInputRef if FlinkTypeFactory.isTimeIndicatorType(v.getType) =>
        new FieldReferenceExpression(
          rowType.getFieldList.get(v.getIndex).getName,
          fromLogicalTypeToDataType(toLogicalType(v.getType)),
          0, // only one input, should always be 0
          windowExprIdx)
      case _ =>
        throw new ValidationException("Window can only be defined over a time attribute column.")
    }
  }

  def getOperandAsLong(call: RexCall, idx: Int): Long =
    call.getOperands.get(idx) match {
      case v: RexLiteral if v.getTypeName.getFamily == SqlTypeFamily.INTERVAL_DAY_TIME =>
        v.getValue.asInstanceOf[JBigDecimal].longValue()
      case _: RexLiteral => throw new TableException(
        "Window aggregate only support SECOND, MINUTE, HOUR, DAY as the time unit. " +
          "MONTH and YEAR time unit are not supported yet.")
      case _ => throw new TableException("Only constant window descriptors are supported.")
    }
}

object StreamLogicalWindowAggregateRule {
  val INSTANCE = new StreamLogicalWindowAggregateRule
}
