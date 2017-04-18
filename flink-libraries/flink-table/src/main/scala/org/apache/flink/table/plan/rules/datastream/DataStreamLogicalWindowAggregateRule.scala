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

package org.apache.flink.table.plan.rules.datastream

import java.math.BigDecimal

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.{RexBuilder, RexCall, RexLiteral, RexNode}
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.flink.table.api.{TableException, Window}
import org.apache.flink.table.api.scala.{Session, Slide, Tumble}
import org.apache.flink.table.expressions.Literal
import org.apache.flink.table.functions.TimeModeTypes
import org.apache.flink.table.plan.rules.common.LogicalWindowAggregateRule
import org.apache.flink.table.typeutils.TimeIntervalTypeInfo

class DataStreamLogicalWindowAggregateRule
  extends LogicalWindowAggregateRule("DataStreamLogicalWindowAggregateRule") {

  /** Returns a zero literal of the correct time type */
  override private[table] def getInAggregateGroupExpression(
      rexBuilder: RexBuilder,
      windowExpression: RexCall): RexNode = createZeroLiteral(rexBuilder, windowExpression)

  /** Returns a zero literal of the correct time type */
  override private[table] def getOutAggregateGroupExpression(
      rexBuilder: RexBuilder,
      windowExpression: RexCall): RexNode = createZeroLiteral(rexBuilder, windowExpression)

  private def createZeroLiteral(
      rexBuilder: RexBuilder,
      windowExpression: RexCall): RexNode = {

    val timeType = windowExpression.operands.get(0).getType
    timeType match {
      case TimeModeTypes.ROWTIME =>
        rexBuilder.makeAbstractCast(
          TimeModeTypes.ROWTIME,
          rexBuilder.makeLiteral(0L, TimeModeTypes.ROWTIME, true))
      case TimeModeTypes.PROCTIME =>
        rexBuilder.makeAbstractCast(
          TimeModeTypes.PROCTIME,
          rexBuilder.makeLiteral(0L, TimeModeTypes.PROCTIME, true))
      case _ =>
        throw TableException(s"""Unexpected time type $timeType encountered""")
    }
  }

  override private[table] def translateWindowExpression(
      windowExpr: RexCall,
      rowType: RelDataType): Window = {

    def getOperandAsLong(call: RexCall, idx: Int): Long =
      call.getOperands.get(idx) match {
        case v : RexLiteral => v.getValue.asInstanceOf[BigDecimal].longValue()
        case _ => throw new TableException("Only constant window descriptors are supported")
      }

    windowExpr.getOperator match {
      case SqlStdOperatorTable.TUMBLE =>
        val interval = getOperandAsLong(windowExpr, 1)
        val w = Tumble.over(Literal(interval, TimeIntervalTypeInfo.INTERVAL_MILLIS))

        val window = windowExpr.getType match {
          case TimeModeTypes.PROCTIME => w
          case TimeModeTypes.ROWTIME => w.on("rowtime")
        }
        window.as("w$")

      case SqlStdOperatorTable.HOP =>
        val (slide, size) = (getOperandAsLong(windowExpr, 1), getOperandAsLong(windowExpr, 2))
        val w = Slide
          .over(Literal(size, TimeIntervalTypeInfo.INTERVAL_MILLIS))
          .every(Literal(slide, TimeIntervalTypeInfo.INTERVAL_MILLIS))

        val window = windowExpr.getType match {
          case TimeModeTypes.PROCTIME => w
          case TimeModeTypes.ROWTIME => w.on("rowtime")
        }
        window.as("w$")
      case SqlStdOperatorTable.SESSION =>
        val gap = getOperandAsLong(windowExpr, 1)
        val w = Session.withGap(Literal(gap, TimeIntervalTypeInfo.INTERVAL_MILLIS))

        val window = windowExpr.getType match {
          case TimeModeTypes.PROCTIME => w
          case TimeModeTypes.ROWTIME => w.on("rowtime")
        }
        window.as("w$")
    }
  }
}

object DataStreamLogicalWindowAggregateRule {
  val INSTANCE = new DataStreamLogicalWindowAggregateRule
}
