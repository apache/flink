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
package org.apache.flink.api.table.typeutils

import org.apache.flink.api.common.typeinfo.BasicTypeInfo.{BIG_DEC_TYPE_INFO, BOOLEAN_TYPE_INFO, STRING_TYPE_INFO}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, SqlTimeTypeInfo, NumericTypeInfo, TypeInformation}
import org.apache.flink.api.table.validate._

object TypeCheckUtils {

  /**
    * Checks if type information is an advanced type that can be converted to a
    * SQL type but NOT vice versa.
    */
  def isAdvanced(dataType: TypeInformation[_]): Boolean = dataType match {
    case _: BasicTypeInfo[_] => false
    case _: SqlTimeTypeInfo[_] => false
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

  def isTemporal(dataType: TypeInformation[_]): Boolean = dataType.isInstanceOf[SqlTimeTypeInfo[_]]

  def isString(dataType: TypeInformation[_]): Boolean = dataType == STRING_TYPE_INFO

  def isBoolean(dataType: TypeInformation[_]): Boolean = dataType == BOOLEAN_TYPE_INFO

  def isDecimal(dataType: TypeInformation[_]): Boolean = dataType == BIG_DEC_TYPE_INFO

  def isComparable(dataType: TypeInformation[_]): Boolean =
    classOf[Comparable[_]].isAssignableFrom(dataType.getTypeClass)

  def assertNumericExpr(
      dataType: TypeInformation[_],
      caller: String)
    : ExprValidationResult = dataType match {
    case _: NumericTypeInfo[_] =>
      ValidationSuccess
    case BIG_DEC_TYPE_INFO =>
      ValidationSuccess
    case _ =>
      ValidationFailure(s"$caller requires numeric types, get $dataType here")
  }

  def assertOrderableExpr(dataType: TypeInformation[_], caller: String): ExprValidationResult = {
    if (dataType.isSortKeyType) {
      ValidationSuccess
    } else {
      ValidationFailure(s"$caller requires orderable types, get $dataType here")
    }
  }
}
