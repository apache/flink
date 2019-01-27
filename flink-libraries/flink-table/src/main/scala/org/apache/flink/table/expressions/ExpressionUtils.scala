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
import java.time.Duration

import org.apache.flink.streaming.api.windowing.time.{Time => FlinkTime}
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.api.types.{DataTypes, InternalType}

object ExpressionUtils {
  /**
    * Retrieve result type of given Expression.
    *
    * @param expr The expression which caller is interested about result type
    * @return     The result type of Expression
    */
  def getResultType(expr: Expression): InternalType = {
    expr.resultType
  }

  private[flink] def isTimeIntervalLiteral(expr: Expression): Boolean = expr match {
    case Literal(_, DataTypes.INTERVAL_MILLIS) => true
    case _ => false
  }

  private[flink] def isRowCountLiteral(expr: Expression): Boolean = expr match {
    case Literal(_, DataTypes.INTERVAL_ROWS) => true
    case _ => false
  }

  private[flink] def isTimeAttribute(expr: Expression): Boolean =
    isRowtimeAttribute(expr) || isProctimeAttribute(expr)

  private[flink] def isRowtimeAttribute(expr: Expression): Boolean = expr match {
    case r: ResolvedFieldReference if r.resultType == DataTypes.ROWTIME_INDICATOR => true
    case _ => false
  }

  private[flink] def isProctimeAttribute(expr: Expression): Boolean = expr match {
    case r: ResolvedFieldReference if r.resultType == DataTypes.PROCTIME_INDICATOR => true
    case _: Proctime => true
    case _ => false
  }

  private[flink] def toTime(expr: Expression): FlinkTime = expr match {
    case Literal(value: Long, DataTypes.INTERVAL_MILLIS) =>
      FlinkTime.milliseconds(value)
    case _ => throw new IllegalArgumentException()
  }

  private[flink] def toDuration(expr: Expression): Duration = expr match {
    case Literal(value: Long, DataTypes.INTERVAL_MILLIS) =>
      Duration.ofMillis(value)
    case _ => throw new IllegalArgumentException()
  }

  private[flink] def toLong(expr: Expression): Long = expr match {
    case Literal(value: Long, DataTypes.INTERVAL_ROWS) => value
    case _ => throw new IllegalArgumentException()
  }

  private[flink] def toMonthInterval(expr: Expression, multiplier: Int): Expression = expr match {
    case Literal(value: Int, DataTypes.INT) =>
      Literal(value * multiplier, DataTypes.INTERVAL_MONTHS)
    case _ =>
      Cast(Mul(expr, Literal(multiplier)), DataTypes.INTERVAL_MONTHS)
  }

  private[flink] def toMilliInterval(expr: Expression, multiplier: Long): Expression = expr match {
    case Literal(value: Int, DataTypes.INT) =>
      Literal(value * multiplier, DataTypes.INTERVAL_MILLIS)
    case Literal(value: Long, DataTypes.LONG) =>
      Literal(value * multiplier, DataTypes.INTERVAL_MILLIS)
    case _ =>
      Cast(Mul(expr, Literal(multiplier)), DataTypes.INTERVAL_MILLIS)
  }

  private[flink] def toRowInterval(expr: Expression): Expression = expr match {
    case Literal(value: Int, DataTypes.INT) =>
      Literal(value.toLong, DataTypes.INTERVAL_ROWS)
    case Literal(value: Long, DataTypes.LONG) =>
      Literal(value, DataTypes.INTERVAL_ROWS)
    case _ =>
      throw new IllegalArgumentException("Invalid value for row interval literal.")
  }

  private[flink] def toRangeInterval(expr: Expression): Expression = expr match {
    case Literal(value: Int, DataTypes.INT) =>
      Literal(value.toLong, DataTypes.INTERVAL_RANGE)
    case Literal(value: Long, DataTypes.LONG) =>
      Literal(value, DataTypes.INTERVAL_RANGE)
    case Literal(value: Float, DataTypes.FLOAT) =>
      Literal(value, DataTypes.INTERVAL_RANGE)
    case Literal(value: Double, DataTypes.DOUBLE) =>
      Literal(value, DataTypes.INTERVAL_RANGE)
    case _ =>
      throw new IllegalArgumentException("Invalid value for range interval literal.")
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
          throw new ValidationException("Unsupported array type.")
        }
    }
  }
}
