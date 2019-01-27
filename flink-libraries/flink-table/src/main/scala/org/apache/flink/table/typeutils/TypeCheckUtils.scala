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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils.{ListTypeInfo, PojoTypeInfo}
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.api.types._
import org.apache.flink.table.dataformat.BinaryRow
import org.apache.flink.table.validate._

object TypeCheckUtils {

  def isNumeric(dataType: InternalType): Boolean = dataType match {
    case DataTypes.INT | DataTypes.BYTE | DataTypes.SHORT
         | DataTypes.LONG | DataTypes.FLOAT | DataTypes.DOUBLE => true
    case _: DecimalType => true
    case _ => false
  }

  def isTemporal(dataType: InternalType): Boolean =
    isTimePoint(dataType) || isTimeInterval(dataType)

  def isTimePoint(dataType: InternalType): Boolean = dataType match {
    case DataTypes.INTERVAL_MILLIS | DataTypes.INTERVAL_MONTHS => false
    case _: TimeType | _: DateType | _: TimestampType => true
    case _ => false
  }

  def isRowTime(dataType: InternalType): Boolean =
    dataType == DataTypes.ROWTIME_INDICATOR

  def isProcTime(dataType: InternalType): Boolean =
    dataType == DataTypes.PROCTIME_INDICATOR

  def isTimeInterval(dataType: InternalType): Boolean = dataType match {
    case DataTypes.INTERVAL_MILLIS | DataTypes.INTERVAL_MONTHS => true
    case _ => false
  }

  def isString(dataType: InternalType): Boolean = dataType == DataTypes.STRING

  def isBinary(dataType: InternalType): Boolean = dataType == DataTypes.BYTE_ARRAY

  def isBoolean(dataType: InternalType): Boolean = dataType == DataTypes.BOOLEAN

  def isDecimal(dataType: InternalType): Boolean = dataType.isInstanceOf[DecimalType]

  def isInteger(dataType: InternalType): Boolean = dataType == DataTypes.INT

  def isIntegerFamily(dataType: InternalType): Boolean =
    dataType == DataTypes.INT ||
        dataType == DataTypes.BYTE ||
        dataType == DataTypes.LONG ||
        dataType == DataTypes.SHORT

  /**
    * Types that can be easily converted into a string without ambiguity.
    */
  def isSimpleStringRepresentation(dataType: InternalType): Boolean =
    isNumeric(dataType) || isString(dataType) || isTemporal(dataType) || isBoolean(dataType)

  def isLong(dataType: InternalType): Boolean = dataType == DataTypes.LONG

  def isIntervalMonths(dataType: InternalType): Boolean = dataType == DataTypes.INTERVAL_MONTHS

  def isIntervalMillis(dataType: InternalType): Boolean = dataType == DataTypes.INTERVAL_MILLIS

  def isArray(dataType: InternalType): Boolean = dataType.isInstanceOf[ArrayType]

  def isMap(dataType: InternalType): Boolean = dataType.isInstanceOf[MapType]

  def isList(dataType: InternalType): Boolean = dataType match {
    case gt: GenericType[_] => gt.getTypeInfo.isInstanceOf[ListTypeInfo[_]]
    case _ => false
  }

  def isIntegral(dataType: InternalType): Boolean = {
    dataType match {
      case DataTypes.BYTE
           | DataTypes.SHORT
           | DataTypes.INT
           | DataTypes.LONG => true
      case _ => false
    }
  }

  def isComparable(dataType: InternalType): Boolean =
    !dataType.isInstanceOf[GenericType[_]] &&
        !dataType.isInstanceOf[MapType] &&
        !dataType.isInstanceOf[RowType] &&
        !isArray(dataType)

  def assertNumericExpr(
      dataType: InternalType,
      caller: String)
    : ValidationResult = dataType match {
    case t if TypeCheckUtils.isNumeric(t) => ValidationSuccess
    case _ =>
      ValidationFailure(s"$caller requires numeric types, get $dataType here")
  }

  def assertIntegerFamilyExpr(
      dataType: InternalType,
      caller: String)
    : ValidationResult = dataType match {
    case t if TypeCheckUtils.isIntegral(t) => ValidationSuccess
    case _ =>
      ValidationFailure(s"$caller requires integer types but was '$dataType'.")
  }

  def assertOrderableExpr(dataType: InternalType, caller: String): ValidationResult = {
    if (TypeConverters.createExternalTypeInfoFromDataType(dataType).isSortKeyType) {
      ValidationSuccess
    } else {
      ValidationFailure(s"$caller requires orderable types, get $dataType here")
    }
  }

  /**
    * Checks whether a type implements own hashCode() and equals() methods for storing an instance
    * in Flink's state or performing a keyBy operation.
    *
    * @param name name of the operation.
    * @param t type information to be validated
    */
  def validateEqualsHashCode(name: String, t: TypeInformation[_]): Unit = t match {

    // make sure that a POJO class is a valid state type
    case pt: PojoTypeInfo[_] =>
      // we don't check the types recursively to give a chance of wrapping
      // proper hashCode/equals methods around an immutable type
      validateEqualsHashCode(name, pt.getClass)
    // BinaryRow direct hash in bytes, no need to check field types.
    case bt: BaseRowTypeInfo =>
    // recursively check composite types
    case ct: CompositeType[_] =>
      validateEqualsHashCode(name, t.getTypeClass)
      // we check recursively for entering Flink types such as tuples and rows
      for (i <- 0 until ct.getArity) {
        val subtype = ct.getTypeAt(i)
        validateEqualsHashCode(name, subtype)
      }
    // check other type information only based on the type class
    case _: TypeInformation[_] =>
      validateEqualsHashCode(name, t.getTypeClass)
  }

  /**
    * Checks whether a class implements own hashCode() and equals() methods for storing an instance
    * in Flink's state or performing a keyBy operation.
    *
    * @param name name of the operation
    * @param c class to be validated
    */
  def validateEqualsHashCode(name: String, c: Class[_]): Unit = {

    // skip primitives
    if (!c.isPrimitive) {
      // check the component type of arrays
      if (c.isArray) {
        validateEqualsHashCode(name, c.getComponentType)
      }
      // check type for methods
      else {
        if (c.getMethod("hashCode").getDeclaringClass eq classOf[Object]) {
          throw new ValidationException(
            s"Type '${c.getCanonicalName}' cannot be used in a $name operation because it " +
              s"does not implement a proper hashCode() method.")
        }
        if (c.getMethod("equals", classOf[Object]).getDeclaringClass eq classOf[Object]) {
          throw new ValidationException(
            s"Type '${c.getCanonicalName}' cannot be used in a $name operation because it " +
              s"does not implement a proper equals() method.")
        }
      }
    }
  }
}
