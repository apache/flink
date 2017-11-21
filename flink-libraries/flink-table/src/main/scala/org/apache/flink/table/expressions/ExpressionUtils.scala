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

package org.apache.flink.table.expressions

import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Integer => JInteger, Long => JLong, Short => JShort}
import java.math.{BigDecimal => JBigDecimal}
import java.sql.{Date, Time, Timestamp}

import org.apache.calcite.avatica.util.TimeUnit
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.{RexBuilder, RexNode}
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.typeutils.{RowIntervalTypeInfo, TimeIntervalTypeInfo}
import org.apache.flink.streaming.api.windowing.time.{Time => FlinkTime}

object ExpressionUtils {

  private[flink] def isTimeIntervalLiteral(expr: Expression): Boolean = expr match {
    case Literal(_, TimeIntervalTypeInfo.INTERVAL_MILLIS) => true
    case _ => false
  }

  private[flink] def isRowCountLiteral(expr: Expression): Boolean = expr match {
    case Literal(_, RowIntervalTypeInfo.INTERVAL_ROWS) => true
    case _ => false
  }

  private[flink] def isTimeAttribute(expr: Expression): Boolean = expr match {
    case r: ResolvedFieldReference if FlinkTypeFactory.isTimeIndicatorType(r.resultType) => true
    case _ => false
  }

  private[flink] def isRowtimeAttribute(expr: Expression): Boolean = expr match {
    case r: ResolvedFieldReference if FlinkTypeFactory.isRowtimeIndicatorType(r.resultType) => true
    case _ => false
  }

  private[flink] def isProctimeAttribute(expr: Expression): Boolean = expr match {
    case r: ResolvedFieldReference if FlinkTypeFactory.isProctimeIndicatorType(r.resultType) =>
      true
    case _ => false
  }

  private[flink] def toTime(expr: Expression): FlinkTime = expr match {
    case Literal(value: Long, TimeIntervalTypeInfo.INTERVAL_MILLIS) =>
      FlinkTime.milliseconds(value)
    case _ => throw new IllegalArgumentException()
  }

  private[flink] def toLong(expr: Expression): Long = expr match {
    case Literal(value: Long, RowIntervalTypeInfo.INTERVAL_ROWS) => value
    case _ => throw new IllegalArgumentException()
  }

  private[flink] def toMonthInterval(expr: Expression, multiplier: Int): Expression = expr match {
    case Literal(value: Int, BasicTypeInfo.INT_TYPE_INFO) =>
      Literal(value * multiplier, TimeIntervalTypeInfo.INTERVAL_MONTHS)
    case _ =>
      Cast(Mul(expr, Literal(multiplier)), TimeIntervalTypeInfo.INTERVAL_MONTHS)
  }

  private[flink] def toMilliInterval(expr: Expression, multiplier: Long): Expression = expr match {
    case Literal(value: Int, BasicTypeInfo.INT_TYPE_INFO) =>
      Literal(value * multiplier, TimeIntervalTypeInfo.INTERVAL_MILLIS)
    case Literal(value: Long, BasicTypeInfo.LONG_TYPE_INFO) =>
      Literal(value * multiplier, TimeIntervalTypeInfo.INTERVAL_MILLIS)
    case _ =>
      Cast(Mul(expr, Literal(multiplier)), TimeIntervalTypeInfo.INTERVAL_MILLIS)
  }

  private[flink] def toRowInterval(expr: Expression): Expression = expr match {
    case Literal(value: Int, BasicTypeInfo.INT_TYPE_INFO) =>
      Literal(value.toLong, RowIntervalTypeInfo.INTERVAL_ROWS)
    case Literal(value: Long, BasicTypeInfo.LONG_TYPE_INFO) =>
      Literal(value, RowIntervalTypeInfo.INTERVAL_ROWS)
    case _ =>
      throw new IllegalArgumentException("Invalid value for row interval literal.")
  }

  private[flink] def convertArray(array: Array[_]): Expression = {
    def createArray(): Expression = {
      ArrayConstructor(array.map(Literal(_)))
    }

    array match {
      // primitives
      case _: Array[Boolean] => createArray()
      case _: Array[Byte] => createArray()
      case _: Array[Short] => createArray()
      case _: Array[Int] => createArray()
      case _: Array[Long] => createArray()
      case _: Array[Float] => createArray()
      case _: Array[Double] => createArray()

      // boxed types
      case _: Array[JBoolean] => createArray()
      case _: Array[JByte] => createArray()
      case _: Array[JShort] => createArray()
      case _: Array[JInteger] => createArray()
      case _: Array[JLong] => createArray()
      case _: Array[JFloat] => createArray()
      case _: Array[JDouble] => createArray()

      // others
      case _: Array[String] => createArray()
      case _: Array[JBigDecimal] => createArray()
      case _: Array[Date] => createArray()
      case _: Array[Time] => createArray()
      case _: Array[Timestamp] => createArray()
      case bda: Array[BigDecimal] => ArrayConstructor(bda.map { bd => Literal(bd.bigDecimal) })

      case _ =>
        // nested
        if (array.length > 0 && array.head.isInstanceOf[Array[_]]) {
          ArrayConstructor(array.map { na => convertArray(na.asInstanceOf[Array[_]]) })
        } else {
          throw ValidationException("Unsupported array type.")
        }
    }
  }

  // ----------------------------------------------------------------------------------------------
  // RexNode conversion functions (see org.apache.calcite.sql2rel.StandardConvertletTable)
  // ----------------------------------------------------------------------------------------------

  /**
    * Copy of [[org.apache.calcite.sql2rel.StandardConvertletTable#getFactor()]].
    */
  private[flink] def getFactor(unit: TimeUnit): JBigDecimal = unit match {
    case TimeUnit.DAY => java.math.BigDecimal.ONE
    case TimeUnit.HOUR => TimeUnit.DAY.multiplier
    case TimeUnit.MINUTE => TimeUnit.HOUR.multiplier
    case TimeUnit.SECOND => TimeUnit.MINUTE.multiplier
    case TimeUnit.YEAR => java.math.BigDecimal.ONE
    case TimeUnit.MONTH => TimeUnit.YEAR.multiplier
    case _ => throw new IllegalArgumentException("Invalid start unit.")
  }

  /**
    * Copy of [[org.apache.calcite.sql2rel.StandardConvertletTable#mod()]].
    */
  private[flink] def mod(
      rexBuilder: RexBuilder,
      resType: RelDataType,
      res: RexNode,
      value: JBigDecimal)
    : RexNode = {
    if (value == JBigDecimal.ONE) return res
    rexBuilder.makeCall(SqlStdOperatorTable.MOD, res, rexBuilder.makeExactLiteral(value, resType))
  }

  /**
    * Copy of [[org.apache.calcite.sql2rel.StandardConvertletTable#divide()]].
    */
  private[flink] def divide(rexBuilder: RexBuilder, res: RexNode, value: JBigDecimal): RexNode = {
    if (value == JBigDecimal.ONE) return res
    if (value.compareTo(JBigDecimal.ONE) < 0 && value.signum == 1) {
      try {
        val reciprocal = JBigDecimal.ONE.divide(value, JBigDecimal.ROUND_UNNECESSARY)
        return rexBuilder.makeCall(
          SqlStdOperatorTable.MULTIPLY,
          res,
          rexBuilder.makeExactLiteral(reciprocal))
      } catch {
        case e: ArithmeticException => // ignore
      }
    }
    rexBuilder.makeCall(
      SqlStdOperatorTable.DIVIDE_INTEGER,
      res,
      rexBuilder.makeExactLiteral(value))
  }

}
