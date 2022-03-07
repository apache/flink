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

package org.apache.flink.table.planner.plan.utils

import org.apache.flink.table.data.{DecimalData, TimestampData}
import org.apache.flink.table.data.binary.BinaryStringData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.CodeGenException
import org.apache.flink.table.planner.utils.TimestampStringUtils.toLocalDateTime
import org.apache.flink.table.types.logical.{DistinctType, LogicalType}
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks

import org.apache.calcite.avatica.util.ByteString
import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.util.{DateString, NlsString, TimestampString, TimeString}

import java.lang.{Boolean => JBoolean, Integer => JInteger, Number => JNumber}
import java.math.{BigDecimal => JBigDecimal}

import scala.annotation.tailrec

/**
 * Utilities to work with [[RexLiteral]]
 */
object RexLiteralUtil {

  /**
   * See [[toFlinkInternalValue(Comparable, LogicalType)]].
   */
  def toFlinkInternalValue(literal: RexLiteral): (Any, LogicalType) = {
    val targetType = FlinkTypeFactory.toLogicalType(literal.getType)
    (toFlinkInternalValue(literal.getValueAs(classOf[Comparable[_]]), targetType), targetType)
  }

  /**
   * Convert a value from Calcite's [[Comparable]] type system to Flink internal type system,
   * and also tries to be a bit flexible by accepting usual Java types such as String and boxed
   * numerics.
   *
   * This function is essentially like [[FlinkTypeFactory.toLogicalType()]] but for values.
   *
   * Check [[RexLiteral.valueMatchesType]] for details on the [[Comparable]] type system and
   * [[org.apache.flink.table.data.RowData]] for details on Flink's internal type system.
   *
   * @param value the value in Calcite's [[Comparable]] type system
   * @param valueType the type of the value
   * @return the value in Flink's internal type system
   * @throws IllegalArgumentException in case the class of value does not match the expectations
   *                                  of valueType
   */
  @tailrec
  def toFlinkInternalValue(value: Comparable[_], valueType: LogicalType): Any = {
    if (value == null) {
      return null
    }
    valueType.getTypeRoot match {
      case CHAR | VARCHAR =>
        value match {
          case nlsString: NlsString => BinaryStringData.fromString(nlsString.getValue)
          case string: String => BinaryStringData.fromString(string)
        }

      case BOOLEAN =>
        value match {
          case boolean: JBoolean => boolean
        }

      case BINARY | VARBINARY =>
        value match {
          case byteString: ByteString => byteString.getBytes
        }

      case DECIMAL =>
        value match {
          case bigDecimal: JBigDecimal => DecimalData.fromBigDecimal(
            bigDecimal,
            LogicalTypeChecks.getPrecision(valueType),
            LogicalTypeChecks.getScale(valueType))
        }

      case TINYINT =>
        value match {
          case number: JNumber => number.byteValue()
        }

      case SMALLINT =>
        value match {
          case number: JNumber => number.shortValue()
        }

      case INTEGER =>
        value match {
          case number: JNumber => number.intValue()
        }

      case BIGINT =>
        value match {
          case number: JNumber => number.longValue()
        }

      case FLOAT =>
        value match {
          case number: JNumber => number.floatValue()
        }

      case DOUBLE =>
        value match {
          case number: JNumber => number.doubleValue()
        }

      case DATE =>
        value match {
          case dateStringValue: DateString => dateStringValue.getDaysSinceEpoch
          case intValue: JInteger => intValue
        }

      case TIME_WITHOUT_TIME_ZONE =>
        value match {
          case timeStringValue: TimeString => timeStringValue.getMillisOfDay
          case intValue: JInteger => intValue
        }

      case TIMESTAMP_WITHOUT_TIME_ZONE =>
        value match {
          case timestampString: TimestampString =>
            TimestampData.fromLocalDateTime(toLocalDateTime(timestampString))
        }

      case TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
        value match {
          case timestampString: TimestampString =>
            TimestampData.fromLocalDateTime(toLocalDateTime(timestampString))
        }

      case INTERVAL_YEAR_MONTH =>
        value match {
          case number: JNumber => number.intValue()
        }

      case INTERVAL_DAY_TIME =>
        value match {
          case number: JNumber => number.longValue()
        }

      case DISTINCT_TYPE =>
        toFlinkInternalValue(value, valueType.asInstanceOf[DistinctType].getSourceType)

      case SYMBOL =>
        value.asInstanceOf[Enum[_]]

      case TIMESTAMP_WITH_TIME_ZONE | ARRAY | MULTISET | MAP | ROW | STRUCTURED_TYPE |
           NULL | UNRESOLVED =>
        throw new CodeGenException(s"Type not supported: $valueType")

      case _ => throw new IllegalStateException(
        s"Unexpected class ${value.getClass} for value of type $valueType")
    }
  }

}
