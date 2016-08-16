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

package org.apache.flink.api.table.expressions

import org.apache.calcite.avatica.util.{TimeUnit, TimeUnitRange}
import org.apache.calcite.rex._
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.{SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.api.table.FlinkRelBuilder
import org.apache.flink.api.table.expressions.ExpressionUtils.{divide, getFactor, mod}
import org.apache.flink.api.table.typeutils.{IntervalTypeInfo, TypeCheckUtils}
import org.apache.flink.api.table.validate.{ExprValidationResult, ValidationFailure, ValidationSuccess}

import scala.collection.JavaConversions._

case class Extract(timeIntervalUnit: Expression, temporal: Expression) extends Expression {

  override private[flink] def children: Seq[Expression] = timeIntervalUnit :: temporal :: Nil

  override private[flink] def resultType: TypeInformation[_] = LONG_TYPE_INFO

  override private[flink] def validateInput(): ExprValidationResult = {
    if (!TypeCheckUtils.isTemporal(temporal.resultType)) {
      return ValidationFailure(s"Extract operator requires Temporal input, " +
        s"but $temporal is of type ${temporal.resultType}")
    }

    timeIntervalUnit match {
      case SymbolExpression(TimeIntervalUnit.YEAR)
           | SymbolExpression(TimeIntervalUnit.MONTH)
           | SymbolExpression(TimeIntervalUnit.DAY)
        if temporal.resultType == SqlTimeTypeInfo.DATE
          || temporal.resultType == SqlTimeTypeInfo.TIMESTAMP
          || temporal.resultType == IntervalTypeInfo.INTERVAL_MILLIS
          || temporal.resultType == IntervalTypeInfo.INTERVAL_MONTHS =>
        ValidationSuccess

      case SymbolExpression(TimeIntervalUnit.HOUR)
           | SymbolExpression(TimeIntervalUnit.MINUTE)
           | SymbolExpression(TimeIntervalUnit.SECOND)
        if temporal.resultType == SqlTimeTypeInfo.TIME
          || temporal.resultType == SqlTimeTypeInfo.TIMESTAMP
          || temporal.resultType == IntervalTypeInfo.INTERVAL_MILLIS =>
        ValidationSuccess

      case _ =>
        ValidationFailure(s"Extract operator does not support unit '$timeIntervalUnit' for input " +
          s"of type '${temporal.resultType}'.")
    }
  }

  override def toString: String = s"($temporal).extract($timeIntervalUnit)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    // get wrapped Calcite unit
    val timeUnitRange = timeIntervalUnit
      .asInstanceOf[SymbolExpression]
      .symbol
      .enum
      .asInstanceOf[TimeUnitRange]

    // convert RexNodes
    convertExtract(
      timeIntervalUnit.toRexNode,
      timeUnitRange,
      temporal.toRexNode,
      relBuilder.asInstanceOf[FlinkRelBuilder])
  }

  /**
    * Standard conversion of the EXTRACT operator.
    * Source: [[org.apache.calcite.sql2rel.StandardConvertletTable#convertExtract()]]
    */
  private def convertExtract(
      timeUnitRangeRexNode: RexNode,
      timeUnitRange: TimeUnitRange,
      temporal: RexNode,
      relBuilder: FlinkRelBuilder)
    : RexNode = {
    val rexBuilder = relBuilder.getRexBuilder
    val resultType = relBuilder.getTypeFactory().createTypeFromTypeInfo(LONG_TYPE_INFO)
    var result = rexBuilder.makeReinterpretCast(
      resultType,
      temporal,
      rexBuilder.makeLiteral(false))

    val unit = timeUnitRange.startUnit
    val sqlTypeName = temporal.getType.getSqlTypeName
    unit match {
      case TimeUnit.YEAR | TimeUnit.MONTH | TimeUnit.DAY =>
        sqlTypeName match {
          case SqlTypeName.TIMESTAMP =>
            result = divide(rexBuilder, result, TimeUnit.DAY.multiplier)
            return rexBuilder.makeCall(
              resultType,
              SqlStdOperatorTable.EXTRACT_DATE,
              Seq(timeUnitRangeRexNode, result))

          case SqlTypeName.DATE =>
            return rexBuilder.makeCall(
              resultType,
              SqlStdOperatorTable.EXTRACT_DATE,
              Seq(timeUnitRangeRexNode, result))

          case _ => // do nothing
        }

      case _ => // do nothing
    }

    result = mod(rexBuilder, resultType, result, getFactor(unit))
    result = divide(rexBuilder, result, unit.multiplier)
    result
  }

}


