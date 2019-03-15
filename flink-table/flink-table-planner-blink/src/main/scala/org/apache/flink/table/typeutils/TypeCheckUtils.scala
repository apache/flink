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

import org.apache.flink.table.`type`._
import org.apache.flink.table.codegen.GeneratedExpression

object TypeCheckUtils {

  def isNumeric(dataType: InternalType): Boolean = dataType match {
    case InternalTypes.INT | InternalTypes.BYTE | InternalTypes.SHORT
         | InternalTypes.LONG | InternalTypes.FLOAT | InternalTypes.DOUBLE => true
    case _: DecimalType => true
    case _ => false
  }

  def isTemporal(dataType: InternalType): Boolean =
    isTimePoint(dataType) || isTimeInterval(dataType)

  def isTimePoint(dataType: InternalType): Boolean = dataType match {
    case InternalTypes.INTERVAL_MILLIS | InternalTypes.INTERVAL_MONTHS => false
    case _: TimeType | _: DateType | _: TimestampType => true
    case _ => false
  }

  def isRowTime(dataType: InternalType): Boolean =
    dataType == InternalTypes.ROWTIME_INDICATOR

  def isProcTime(dataType: InternalType): Boolean =
    dataType == InternalTypes.PROCTIME_INDICATOR

  def isTimeInterval(dataType: InternalType): Boolean = dataType match {
    case InternalTypes.INTERVAL_MILLIS | InternalTypes.INTERVAL_MONTHS => true
    case _ => false
  }

  def isString(dataType: InternalType): Boolean = dataType == InternalTypes.STRING

  def isBinary(dataType: InternalType): Boolean = dataType == InternalTypes.BINARY

  def isBoolean(dataType: InternalType): Boolean = dataType == InternalTypes.BOOLEAN

  def isDecimal(dataType: InternalType): Boolean = dataType.isInstanceOf[DecimalType]

  def isInteger(dataType: InternalType): Boolean = dataType == InternalTypes.INT

  def isLong(dataType: InternalType): Boolean = dataType == InternalTypes.LONG

  def isArray(dataType: InternalType): Boolean = dataType.isInstanceOf[ArrayType]

  def isMap(dataType: InternalType): Boolean = dataType.isInstanceOf[MapType]

  def isComparable(dataType: InternalType): Boolean =
    !dataType.isInstanceOf[GenericType[_]] &&
      !dataType.isInstanceOf[MapType] &&
      !dataType.isInstanceOf[RowType] &&
      !isArray(dataType)

  def isMutable(dataType: InternalType): Boolean = dataType match {
    // the internal representation of String is BinaryString which is mutable
    case InternalTypes.STRING => true
    case _: ArrayType | _: MapType | _: RowType | _: GenericType[_] => true
    case _ => false
  }

  def isReference(t: InternalType): Boolean = t match {
    case InternalTypes.INT
         | InternalTypes.LONG
         | InternalTypes.SHORT
         | InternalTypes.BYTE
         | InternalTypes.FLOAT
         | InternalTypes.DOUBLE
         | InternalTypes.BOOLEAN
         | InternalTypes.CHAR
         | _: DateType
         | InternalTypes.TIME
         | _: TimestampType => false
    case _ => true
  }

  def isReference(genExpr: GeneratedExpression): Boolean = isReference(genExpr.resultType)

}
