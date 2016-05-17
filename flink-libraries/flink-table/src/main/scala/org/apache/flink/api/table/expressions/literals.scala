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

import java.sql.{Date, Time, Timestamp}
import java.util.{Calendar, TimeZone}

import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.api.table.FlinkTypeFactory

object Literal {
  private[flink] def apply(l: Any): Literal = l match {
    case i: Int => Literal(i, BasicTypeInfo.INT_TYPE_INFO)
    case s: Short => Literal(s, BasicTypeInfo.SHORT_TYPE_INFO)
    case b: Byte => Literal(b, BasicTypeInfo.BYTE_TYPE_INFO)
    case l: Long => Literal(l, BasicTypeInfo.LONG_TYPE_INFO)
    case d: Double => Literal(d, BasicTypeInfo.DOUBLE_TYPE_INFO)
    case f: Float => Literal(f, BasicTypeInfo.FLOAT_TYPE_INFO)
    case str: String => Literal(str, BasicTypeInfo.STRING_TYPE_INFO)
    case bool: Boolean => Literal(bool, BasicTypeInfo.BOOLEAN_TYPE_INFO)
    case javaDec: java.math.BigDecimal => Literal(javaDec, BasicTypeInfo.BIG_DEC_TYPE_INFO)
    case scalaDec: scala.math.BigDecimal =>
      Literal(scalaDec.bigDecimal, BasicTypeInfo.BIG_DEC_TYPE_INFO)
    case sqlDate: Date => Literal(sqlDate, SqlTimeTypeInfo.DATE)
    case sqlTime: Time => Literal(sqlTime, SqlTimeTypeInfo.TIME)
    case sqlTimestamp: Timestamp => Literal(sqlTimestamp, SqlTimeTypeInfo.TIMESTAMP)
  }
}

case class Literal(value: Any, resultType: TypeInformation[_]) extends LeafExpression {
  override def toString = s"$value"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    resultType match {
      case BasicTypeInfo.BIG_DEC_TYPE_INFO =>
        val bigDecValue = value.asInstanceOf[java.math.BigDecimal]
        val decType = relBuilder.getTypeFactory.createSqlType(SqlTypeName.DECIMAL)
        relBuilder.getRexBuilder.makeExactLiteral(bigDecValue, decType)

      // date/time
      case SqlTimeTypeInfo.DATE =>
        relBuilder.getRexBuilder.makeDateLiteral(dateToCalendar)
      case SqlTimeTypeInfo.TIME =>
        relBuilder.getRexBuilder.makeTimeLiteral(dateToCalendar, 0)
      case SqlTimeTypeInfo.TIMESTAMP =>
        relBuilder.getRexBuilder.makeTimestampLiteral(dateToCalendar, 3)

      case _ => relBuilder.literal(value)
    }
  }

  private def dateToCalendar: Calendar = {
    val date = value.asInstanceOf[java.util.Date]
    val cal = Calendar.getInstance()
    val t = date.getTime
    // according to Calcite's SqlFunctions.internalToXXX methods
    cal.setTimeInMillis(t + TimeZone.getDefault.getOffset(t))
    cal
  }
}

case class Null(resultType: TypeInformation[_]) extends LeafExpression {
  override def toString = s"null"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    val rexBuilder = relBuilder.getRexBuilder
    val typeFactory = relBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    rexBuilder
      .makeCast(
        typeFactory.createTypeFromTypeInfo(resultType),
        rexBuilder.constantNull())
  }
}
