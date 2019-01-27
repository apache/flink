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

import java.sql.{Date, Time, Timestamp}
import java.util.{Calendar, TimeZone}

import org.apache.calcite.avatica.util.TimeUnit
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.SqlIntervalQualifier
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.NlsString
import org.apache.flink.table.api.types._
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.logical.LogicalExprVisitor
import org.apache.flink.util.TimeConvertUtils

object Literal {
  private[flink] val GMT = TimeZone.getTimeZone("GMT")

  private[flink] def apply(l: Any): Literal = l match {
    case i: Int => Literal(i, DataTypes.INT)
    case s: Short => Literal(s, DataTypes.SHORT)
    case b: Byte => Literal(b, DataTypes.BYTE)
    case l: Long => Literal(l, DataTypes.LONG)
    case d: Double => Literal(d, DataTypes.DOUBLE)
    case f: Float => Literal(f, DataTypes.FLOAT)
    case str: String => Literal(str, DataTypes.STRING)
    case bool: Boolean => Literal(bool, DataTypes.BOOLEAN)
    case javaDec: java.math.BigDecimal =>
      Literal(javaDec, DecimalType.of(javaDec))
    case scalaDec: scala.math.BigDecimal =>
      Literal(scalaDec.bigDecimal, DecimalType.of(scalaDec.bigDecimal))
    case sqlDate: Date => Literal(sqlDate, DataTypes.DATE)
    case sqlTime: Time => Literal(sqlTime, DataTypes.TIME)
    case sqlTimestamp: Timestamp => Literal(sqlTimestamp, DataTypes.TIMESTAMP)
  }
}

case class Literal(value: Any, resultType: InternalType) extends LeafExpression {

  override def toString = resultType match {
    case _@DataTypes.DATE => TimeConvertUtils.unixDateTimeToString(value) + ".toDate"
    case _@DataTypes.TIME => TimeConvertUtils.unixDateTimeToString(value) + ".toTime"
    case _@DataTypes.TIMESTAMP => TimeConvertUtils.unixDateTimeToString(value) + ".toTimestamp"
    case _@DataTypes.INTERVAL_MILLIS => value.toString + ".millis"
    case _@DataTypes.INTERVAL_MONTHS => value.toString + ".months"
    case _@DataTypes.INTERVAL_ROWS => value.toString + ".rows"
    case _: AtomicType => value.toString
    case _ => s"Literal($value, $resultType)"
  }

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    resultType match {
      case dt: DecimalType =>
        val bigDecValue = value.asInstanceOf[java.math.BigDecimal]
        val decType = relBuilder.getTypeFactory.createSqlType(SqlTypeName.DECIMAL,
          dt.precision, dt.scale)
        relBuilder.getRexBuilder.makeExactLiteral(bigDecValue, decType)

      // create BIGINT literals for long type
      case DataTypes.LONG =>
        val bigint = value match {
          case d: java.math.BigDecimal => d
          case _ => java.math.BigDecimal.valueOf(value.asInstanceOf[Long])
        }
        relBuilder.getRexBuilder.makeBigintLiteral(bigint)

      //Float/Double type should be liked as java type here.
      case DataTypes.FLOAT =>
        relBuilder.getRexBuilder.makeApproxLiteral(
          java.math.BigDecimal.valueOf(value.asInstanceOf[Number].floatValue()),
          relBuilder.getTypeFactory.createSqlType(SqlTypeName.FLOAT))
      case DataTypes.DOUBLE =>
        relBuilder.getRexBuilder.makeApproxLiteral(
          java.math.BigDecimal.valueOf(value.asInstanceOf[Number].doubleValue()),
          relBuilder.getTypeFactory.createSqlType(SqlTypeName.DOUBLE))

      // date/time
      case DataTypes.DATE =>
        relBuilder.getRexBuilder.makeDateLiteral(dateToCalendar)
      case DataTypes.TIME =>
        relBuilder.getRexBuilder.makeTimeLiteral(dateToCalendar, 0)
      case DataTypes.TIMESTAMP =>
        relBuilder.getRexBuilder.makeTimestampLiteral(dateToCalendar, 3)

      case DataTypes.INTERVAL_MONTHS =>
        val interval = java.math.BigDecimal.valueOf(value.asInstanceOf[Int])
        val intervalQualifier = new SqlIntervalQualifier(
          TimeUnit.YEAR,
          TimeUnit.MONTH,
          SqlParserPos.ZERO)
        relBuilder.getRexBuilder.makeIntervalLiteral(interval, intervalQualifier)

      case DataTypes.INTERVAL_MILLIS =>
        val interval = java.math.BigDecimal.valueOf(value.asInstanceOf[Long])
        val intervalQualifier = new SqlIntervalQualifier(
          TimeUnit.DAY,
          TimeUnit.SECOND,
          SqlParserPos.ZERO)
        relBuilder.getRexBuilder.makeIntervalLiteral(interval, intervalQualifier)

      case DataTypes.BYTE =>
        relBuilder.getRexBuilder.makeExactLiteral(
          java.math.BigDecimal.valueOf(value.asInstanceOf[Number].byteValue()),
          relBuilder.getTypeFactory.createSqlType(SqlTypeName.TINYINT))

      case DataTypes.SHORT =>
        relBuilder.getRexBuilder.makeExactLiteral(
          java.math.BigDecimal.valueOf(value.asInstanceOf[Number].shortValue()),
          relBuilder.getTypeFactory.createSqlType(SqlTypeName.SMALLINT))

      case DataTypes.STRING =>
        val strValue = value match {
          case s: NlsString => s.getValue
          case _ => value.asInstanceOf[String]
        }
        relBuilder.literal(strValue)

      case _ => relBuilder.literal(value)
    }
  }

  private def dateToCalendar: Calendar = {
    value match {
      case calendar: Calendar => calendar.asInstanceOf[java.util.Calendar]
      case _ =>
        val date = value.asInstanceOf[java.util.Date]
        val cal = Calendar.getInstance(Literal.GMT)
        val t = date.getTime
        cal.setTimeInMillis(t)
        cal
    }
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Null(resultType: InternalType) extends LeafExpression {

  override def toString = s"null"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    val rexBuilder = relBuilder.getRexBuilder
    val typeFactory = relBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    rexBuilder
      .makeCast(
        typeFactory.createTypeFromInternalType(resultType, isNullable = true),
        rexBuilder.constantNull())
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

