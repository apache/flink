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
package org.apache.flink.table.planner.expressions

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, LocalTimeTypeInfo, SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.table.typeutils.TimeIntervalTypeInfo

import java.sql.{Date, Time, Timestamp}
import java.time.{LocalDate, LocalDateTime, LocalTime => jLocalTime}
import java.util.{Calendar, TimeZone}

object Literal {
  private[flink] val UTC = TimeZone.getTimeZone("UTC")

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
    case localDate: LocalDate => Literal(localDate, LocalTimeTypeInfo.LOCAL_DATE)
    case localTime: jLocalTime => Literal(localTime, LocalTimeTypeInfo.LOCAL_TIME)
    case localDateTime: LocalDateTime => Literal(localDateTime, LocalTimeTypeInfo.LOCAL_DATE_TIME)
  }
}

case class Literal(value: Any, resultType: TypeInformation[_]) extends LeafExpression {
  override def toString: String = resultType match {
    case _: BasicTypeInfo[_] => value.toString
    case _@SqlTimeTypeInfo.DATE => value.toString + ".toDate"
    case _@SqlTimeTypeInfo.TIME => value.toString + ".toTime"
    case _@SqlTimeTypeInfo.TIMESTAMP => value.toString + ".toTimestamp"
    case _@TimeIntervalTypeInfo.INTERVAL_MILLIS => value.toString + ".millis"
    case _@TimeIntervalTypeInfo.INTERVAL_MONTHS => value.toString + ".months"
    case _ => s"Literal($value, $resultType)"
  }

  /**
    * Convert a Date value to a Calendar. Calcite's fromCalendarField functions use the
    * Calendar.get methods, so the raw values of the individual fields are preserved when
    * converted to the String formats.
    *
    * @return get the Calendar value
    */
  private def valueAsCalendar: Calendar = {
    val date = value.asInstanceOf[java.util.Date]
    val cal = Calendar.getInstance
    cal.setTime(date)
    cal
  }
}

@deprecated(
  "Use nullOf(TypeInformation) instead. It is available through the implicit Scala DSL.",
  "1.8.0")
case class Null(resultType: TypeInformation[_]) extends LeafExpression {
  override def toString = s"null"
}
