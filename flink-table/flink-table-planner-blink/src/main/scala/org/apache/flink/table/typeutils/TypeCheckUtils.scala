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

import org.apache.flink.table.codegen.GeneratedExpression
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.table.types.logical._

object TypeCheckUtils {

  def isNumeric(dataType: LogicalType): Boolean =
    dataType.getTypeRoot.getFamilies.contains(LogicalTypeFamily.NUMERIC)

  def isTemporal(dataType: LogicalType): Boolean =
    isTimePoint(dataType) || isTimeInterval(dataType)

  def isTimePoint(dataType: LogicalType): Boolean = dataType.getTypeRoot match {
    case TIME_WITHOUT_TIME_ZONE | DATE | TIMESTAMP_WITHOUT_TIME_ZONE => true
    case _ => false
  }

  def isRowTime(dataType: LogicalType): Boolean =
    dataType match {
      case t: TimestampType => t.getKind == TimestampKind.ROWTIME
      case _ => false
    }

  def isProcTime(dataType: LogicalType): Boolean =
    dataType match {
      case t: TimestampType => t.getKind == TimestampKind.PROCTIME
      case _ => false
    }

  def isTimeInterval(dataType: LogicalType): Boolean = dataType.getTypeRoot match {
    case INTERVAL_YEAR_MONTH | INTERVAL_DAY_TIME => true
    case _ => false
  }

  def isVarchar(dataType: LogicalType): Boolean = dataType.getTypeRoot == VARCHAR

  def isBinary(dataType: LogicalType): Boolean = dataType.getTypeRoot == BINARY

  def isBoolean(dataType: LogicalType): Boolean = dataType.getTypeRoot == BOOLEAN

  def isDecimal(dataType: LogicalType): Boolean = dataType.getTypeRoot == DECIMAL

  def isInteger(dataType: LogicalType): Boolean = dataType.getTypeRoot == INTEGER

  def isLong(dataType: LogicalType): Boolean = dataType.getTypeRoot == BIGINT

  def isArray(dataType: LogicalType): Boolean = dataType.getTypeRoot == ARRAY

  def isMap(dataType: LogicalType): Boolean = dataType.getTypeRoot == MAP

  def isAny(dataType: LogicalType): Boolean = dataType.getTypeRoot == ANY

  def isRow(dataType: LogicalType): Boolean = dataType.getTypeRoot == ROW

  def isComparable(dataType: LogicalType): Boolean =
    !isAny(dataType) && !isMap(dataType) && !isRow(dataType) && !isArray(dataType)

  def isMutable(dataType: LogicalType): Boolean = dataType.getTypeRoot match {
    // the internal representation of String is BinaryString which is mutable
    case VARCHAR => true
    case ARRAY | MULTISET | MAP | ROW | ANY => true
    case _ => false
  }

  def isReference(t: LogicalType): Boolean = t.getTypeRoot match {
    case INTEGER |
         TINYINT |
         SMALLINT |
         BIGINT |
         FLOAT |
         DOUBLE |
         BOOLEAN |
         DATE |
         TIME_WITHOUT_TIME_ZONE |
         TIMESTAMP_WITHOUT_TIME_ZONE => false
    case _ => true
  }

  def isReference(genExpr: GeneratedExpression): Boolean = isReference(genExpr.resultType)
}
