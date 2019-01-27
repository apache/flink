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

package org.apache.flink.table.plan.util

import java.math.{BigDecimal => JBigDecimal}

import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.flink.table.api.TableException

object RexLiteralUtil {
  def literalValue(literal: RexLiteral): Object = {
    val value = literal.getValue3
    // null value with type
    if (value == null) {
      return null
    }
    // non-null values
    val ret = literal.getType.getSqlTypeName match {

      case TINYINT =>
        val decimal = BigDecimal(value.asInstanceOf[JBigDecimal])
        if (decimal.isValidByte) {
          decimal.byteValue()
        }
        else {
          throw new TableException("Decimal can not be converted to byte.")
        }

      case SMALLINT =>
        val decimal = BigDecimal(value.asInstanceOf[JBigDecimal])
        if (decimal.isValidShort) {
          decimal.shortValue()
        }
        else {
          throw new TableException("Decimal can not be converted to short.")
        }

      case INTEGER =>
        val decimal = BigDecimal(value.asInstanceOf[JBigDecimal])
        if (decimal.isValidInt) {
          decimal.intValue()
        }
        else {
          throw new TableException("Decimal can not be converted to integer.")
        }

      case BIGINT =>
        val decimal = BigDecimal(value.asInstanceOf[JBigDecimal])
        if (decimal.isValidLong) {
          decimal.longValue()
        }
        else {
          throw new TableException("Decimal can not be converted to long.")
        }

      case FLOAT =>
        val floatValue = value.asInstanceOf[JBigDecimal].floatValue()
        floatValue match {
          case Float.NaN =>
          case Float.NegativeInfinity =>
          case Float.PositiveInfinity =>
            throw new TableException(
              "Do not support java.lang.Float.NaN|NEGATIVE_INFINITY|POSITIVE_INFINITY")
          case _ => floatValue
        }

      case DOUBLE =>
        val doubleValue = value.asInstanceOf[JBigDecimal].doubleValue()
        doubleValue match {
          case Double.NaN =>
          case Double.NegativeInfinity =>
          case Double.PositiveInfinity =>
            throw new TableException(
              "Do not support java.lang.Double.NaN|NEGATIVE_INFINITY|POSITIVE_INFINITY")
          case _ => doubleValue
        }

      case DECIMAL =>
        value.asInstanceOf[JBigDecimal]

      case VARCHAR | CHAR =>
        value.toString

      case DATE =>
      case TIME =>
      case TIMESTAMP =>
        value

      case typeName if YEAR_INTERVAL_TYPES.contains(typeName) =>
        val decimal = BigDecimal(value.asInstanceOf[JBigDecimal])
        if (decimal.isValidInt) {
          decimal.intValue()
        } else {
          throw new TableException(
            s"Decimal '$decimal' can not be converted to interval of months.")
        }

      case typeName if DAY_INTERVAL_TYPES.contains(typeName) =>
        val decimal = BigDecimal(value.asInstanceOf[JBigDecimal])
        if (decimal.isValidLong) {
          decimal.longValue()
        } else {
          throw new TableException(
            s"Decimal '$decimal' can not be converted to interval of milliseconds.")
        }

      case t@_ =>
        throw new TableException(s"Type not supported: $t")
    }
    ret.asInstanceOf[Object]
  }
}
