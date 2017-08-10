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
package org.apache.flink.table.typeutils

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo._
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo
import org.apache.flink.table.typeutils.TimeIntervalTypeInfo.{INTERVAL_MILLIS, INTERVAL_MONTHS}
import org.apache.flink.table.validate._

object TypeCheckUtils {

  /**
    * Checks if type information is an advanced type that can be converted to a
    * SQL type but NOT vice versa.
    */
  def isAdvanced(dataType: TypeInformation[_]): Boolean = dataType match {
    case _: TimeIndicatorTypeInfo => false
    case _: BasicTypeInfo[_] => false
    case _: SqlTimeTypeInfo[_] => false
    case _: TimeIntervalTypeInfo[_] => false
    case _ => true
  }

  /**
    * Checks if type information is a simple type that can be converted to a
    * SQL type and vice versa.
    */
  def isSimple(dataType: TypeInformation[_]): Boolean = !isAdvanced(dataType)

  def isNumeric(dataType: TypeInformation[_]): Boolean = dataType match {
    case _: NumericTypeInfo[_] => true
    case BIG_DEC_TYPE_INFO => true
    case _ => false
  }

  def isTemporal(dataType: TypeInformation[_]): Boolean =
    isTimePoint(dataType) || isTimeInterval(dataType)

  def isTimePoint(dataType: TypeInformation[_]): Boolean =
    dataType.isInstanceOf[SqlTimeTypeInfo[_]]

  def isTimeInterval(dataType: TypeInformation[_]): Boolean =
    dataType.isInstanceOf[TimeIntervalTypeInfo[_]]

  def isString(dataType: TypeInformation[_]): Boolean = dataType == STRING_TYPE_INFO

  def isBoolean(dataType: TypeInformation[_]): Boolean = dataType == BOOLEAN_TYPE_INFO

  def isDecimal(dataType: TypeInformation[_]): Boolean = dataType == BIG_DEC_TYPE_INFO

  def isInteger(dataType: TypeInformation[_]): Boolean = dataType == INT_TYPE_INFO

  def isLong(dataType: TypeInformation[_]): Boolean = dataType == LONG_TYPE_INFO

  def isIntervalMonths(dataType: TypeInformation[_]): Boolean = dataType == INTERVAL_MONTHS

  def isIntervalMillis(dataType: TypeInformation[_]): Boolean = dataType == INTERVAL_MILLIS

  def isArray(dataType: TypeInformation[_]): Boolean = dataType match {
    case _: ObjectArrayTypeInfo[_, _] |
         _: BasicArrayTypeInfo[_, _] |
         _: PrimitiveArrayTypeInfo[_]  => true
    case _ => false
  }

  def isComparable(dataType: TypeInformation[_]): Boolean =
    classOf[Comparable[_]].isAssignableFrom(dataType.getTypeClass) && !isArray(dataType)

  def assertNumericExpr(
      dataType: TypeInformation[_],
      caller: String)
    : ValidationResult = dataType match {
    case _: NumericTypeInfo[_] =>
      ValidationSuccess
    case BIG_DEC_TYPE_INFO =>
      ValidationSuccess
    case _ =>
      ValidationFailure(s"$caller requires numeric types, get $dataType here")
  }

  def assertOrderableExpr(dataType: TypeInformation[_], caller: String): ValidationResult = {
    if (dataType.isSortKeyType) {
      ValidationSuccess
    } else {
      ValidationFailure(s"$caller requires orderable types, get $dataType here")
    }
  }
}
