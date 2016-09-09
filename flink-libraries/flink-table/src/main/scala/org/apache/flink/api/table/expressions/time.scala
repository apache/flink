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
import org.apache.flink.api.table.expressions.TimeIntervalUnit.TimeIntervalUnit
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
        ValidationFailure(s"Extract operator does not support unit '$timeIntervalUnit' for input" +
          s" of type '${temporal.resultType}'.")
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

    // TODO convert this into Table API expressions to make the code more readable
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

abstract class TemporalCeilFloor(
    timeIntervalUnit: Expression,
    temporal: Expression)
  extends Expression {

  override private[flink] def children: Seq[Expression] = timeIntervalUnit :: temporal :: Nil

  override private[flink] def resultType: TypeInformation[_] = temporal.resultType

  override private[flink] def validateInput(): ExprValidationResult = {
    if (!TypeCheckUtils.isTimePoint(temporal.resultType)) {
      return ValidationFailure(s"Temporal ceil/floor operator requires Time Point input, " +
        s"but $temporal is of type ${temporal.resultType}")
    }
    val unit = timeIntervalUnit match {
      case SymbolExpression(u: TimeIntervalUnit) => Some(u)
      case _ => None
    }
    if (unit.isEmpty) {
      return ValidationFailure(s"Temporal ceil/floor operator requires Time Interval Unit " +
        s"input, but $timeIntervalUnit is of type ${timeIntervalUnit.resultType}")
    }

    (unit.get, temporal.resultType) match {
      case (TimeIntervalUnit.YEAR | TimeIntervalUnit.MONTH,
          SqlTimeTypeInfo.DATE | SqlTimeTypeInfo.TIMESTAMP) =>
        ValidationSuccess
      case (TimeIntervalUnit.DAY, SqlTimeTypeInfo.TIMESTAMP) =>
        ValidationSuccess
      case (TimeIntervalUnit.HOUR | TimeIntervalUnit.MINUTE | TimeIntervalUnit.SECOND,
          SqlTimeTypeInfo.TIME | SqlTimeTypeInfo.TIMESTAMP) =>
        ValidationSuccess
      case _ =>
        ValidationFailure(s"Temporal ceil/floor operator does not support " +
          s"unit '$timeIntervalUnit' for input of type '${temporal.resultType}'.")
    }
  }
}

case class TemporalFloor(
    timeIntervalUnit: Expression,
    temporal: Expression)
  extends TemporalCeilFloor(
    timeIntervalUnit,
    temporal) {

  override def toString: String = s"($temporal).floor($timeIntervalUnit)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.FLOOR, temporal.toRexNode, timeIntervalUnit.toRexNode)
  }
}

case class TemporalCeil(
    timeIntervalUnit: Expression,
    temporal: Expression)
  extends TemporalCeilFloor(
    timeIntervalUnit,
    temporal) {

  override def toString: String = s"($temporal).ceil($timeIntervalUnit)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.CEIL, temporal.toRexNode, timeIntervalUnit.toRexNode)
  }
}

abstract class CurrentTimePoint(
    targetType: TypeInformation[_],
    local: Boolean)
  extends LeafExpression {

  override private[flink] def resultType: TypeInformation[_] = targetType

  override private[flink] def validateInput(): ExprValidationResult = {
    if (!TypeCheckUtils.isTimePoint(targetType)) {
      ValidationFailure(s"CurrentTimePoint operator requires Time Point target type, " +
        s"but get $targetType.")
    } else if (local && targetType == SqlTimeTypeInfo.DATE) {
      ValidationFailure(s"Localized CurrentTimePoint operator requires Time or Timestamp target " +
        s"type, but get $targetType.")
    } else {
      ValidationSuccess
    }
  }

  override def toString: String = if (local) {
    s"local$targetType()"
  } else {
    s"current$targetType()"
  }

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    val operator = targetType match {
      case SqlTimeTypeInfo.TIME if local => SqlStdOperatorTable.LOCALTIME
      case SqlTimeTypeInfo.TIMESTAMP if local => SqlStdOperatorTable.LOCALTIMESTAMP
      case SqlTimeTypeInfo.DATE => SqlStdOperatorTable.CURRENT_DATE
      case SqlTimeTypeInfo.TIME => SqlStdOperatorTable.CURRENT_TIME
      case SqlTimeTypeInfo.TIMESTAMP => SqlStdOperatorTable.CURRENT_TIMESTAMP
    }
    relBuilder.call(operator)
  }
}

case class CurrentDate() extends CurrentTimePoint(SqlTimeTypeInfo.DATE, local = false)

case class CurrentTime() extends CurrentTimePoint(SqlTimeTypeInfo.TIME, local = false)

case class CurrentTimestamp() extends CurrentTimePoint(SqlTimeTypeInfo.TIMESTAMP, local = false)

case class LocalTime() extends CurrentTimePoint(SqlTimeTypeInfo.TIME, local = true)

case class LocalTimestamp() extends CurrentTimePoint(SqlTimeTypeInfo.TIMESTAMP, local = true)

/**
  * Extracts the quarter of a year from a SQL date.
  */
case class Quarter(child: Expression) extends UnaryExpression with InputTypeSpec {

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] = Seq(SqlTimeTypeInfo.DATE)

  override private[flink] def resultType: TypeInformation[_] = LONG_TYPE_INFO

  override def toString: String = s"($child).quarter()"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    /**
      * Standard conversion of the QUARTER operator.
      * Source: [[org.apache.calcite.sql2rel.StandardConvertletTable#convertQuarter()]]
      */
    Plus(
      Div(
        Minus(
          Extract(TimeIntervalUnit.MONTH, child),
          Literal(1L)),
        Literal(TimeUnit.QUARTER.multiplier.longValue())),
      Literal(1L)
    ).toRexNode
  }
}

