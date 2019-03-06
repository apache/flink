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

object TypeCheckUtils {

  def isNumeric(dataType: InternalType): Boolean = dataType match {
    case InternalTypes.INT | InternalTypes.BYTE | InternalTypes.SHORT
         | InternalTypes.LONG | InternalTypes.FLOAT | InternalTypes.DOUBLE => true
    case _: DecimalType => true
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

}
