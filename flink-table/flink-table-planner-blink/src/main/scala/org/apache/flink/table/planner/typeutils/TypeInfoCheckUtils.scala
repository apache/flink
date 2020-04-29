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
package org.apache.flink.table.planner.typeutils

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo._
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils.{MapTypeInfo, ObjectArrayTypeInfo, PojoTypeInfo}
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.planner.validate._
import org.apache.flink.table.runtime.typeutils.{BigDecimalTypeInfo, DecimalDataTypeInfo, LegacyLocalDateTimeTypeInfo, LegacyTimestampTypeInfo}
import org.apache.flink.table.typeutils.TimeIntervalTypeInfo.{INTERVAL_MILLIS, INTERVAL_MONTHS}
import org.apache.flink.table.typeutils.{TimeIndicatorTypeInfo, TimeIntervalTypeInfo}

object TypeInfoCheckUtils {

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
    case BIG_DEC_TYPE_INFO | _: BigDecimalTypeInfo | _: DecimalDataTypeInfo => true
    case _ => false
  }

  def isTemporal(dataType: TypeInformation[_]): Boolean =
    isTimePoint(dataType) || isTimeInterval(dataType)

  def isTimePoint(dataType: TypeInformation[_]): Boolean =
    dataType.isInstanceOf[SqlTimeTypeInfo[_]] || dataType.isInstanceOf[LocalTimeTypeInfo[_]]

  def isTimeInterval(dataType: TypeInformation[_]): Boolean =
    dataType.isInstanceOf[TimeIntervalTypeInfo[_]]

  def isString(dataType: TypeInformation[_]): Boolean = dataType == STRING_TYPE_INFO

  def isBoolean(dataType: TypeInformation[_]): Boolean = dataType == BOOLEAN_TYPE_INFO

  def isDecimal(dataType: TypeInformation[_]): Boolean = dataType == BIG_DEC_TYPE_INFO

  def isInteger(dataType: TypeInformation[_]): Boolean = dataType == INT_TYPE_INFO

  def isIntegerFamily(dataType: TypeInformation[_]): Boolean =
    dataType.isInstanceOf[IntegerTypeInfo[_]]

  def isLong(dataType: TypeInformation[_]): Boolean = dataType == LONG_TYPE_INFO

  def isIntervalMonths(dataType: TypeInformation[_]): Boolean = dataType == INTERVAL_MONTHS

  def isIntervalMillis(dataType: TypeInformation[_]): Boolean = dataType == INTERVAL_MILLIS

  def isArray(dataType: TypeInformation[_]): Boolean = dataType match {
    case _: ObjectArrayTypeInfo[_, _] |
         _: BasicArrayTypeInfo[_, _] |
         _: PrimitiveArrayTypeInfo[_]  => true
    case _ => false
  }

  def isMap(dataType: TypeInformation[_]): Boolean =
    dataType.isInstanceOf[MapTypeInfo[_, _]]

  def isComparable(dataType: TypeInformation[_]): Boolean = dataType match {
    case _: LegacyLocalDateTimeTypeInfo |
         _: LegacyTimestampTypeInfo => true
    case _ =>
      classOf[Comparable[_]].isAssignableFrom(dataType.getTypeClass) && !isArray(dataType)
  }

  /**
    * Types that can be easily converted into a string without ambiguity.
    */
  def isSimpleStringRepresentation(dataType: TypeInformation[_]): Boolean =
    isNumeric(dataType) || isString(dataType) || isTemporal(dataType) || isBoolean(dataType)

  def assertNumericExpr(
      dataType: TypeInformation[_],
      caller: String)
  : ValidationResult = dataType match {
    case _: NumericTypeInfo[_] =>
      ValidationSuccess
    case BIG_DEC_TYPE_INFO | _: BigDecimalTypeInfo | _: DecimalDataTypeInfo =>
      ValidationSuccess
    case _ =>
      ValidationFailure(s"$caller requires numeric types, get $dataType here")
  }

  def assertIntegerFamilyExpr(
      dataType: TypeInformation[_],
      caller: String)
  : ValidationResult = dataType match {
    case _: IntegerTypeInfo[_] =>
      ValidationSuccess
    case _ =>
      ValidationFailure(s"$caller requires integer types but was '$dataType'.")
  }

  def assertOrderableExpr(dataType: TypeInformation[_], caller: String): ValidationResult = {
    if (dataType.isSortKeyType) {
      ValidationSuccess
    } else {
      ValidationFailure(s"$caller requires orderable types, get $dataType here")
    }
  }

  /**
    * Checks whether a type implements own hashCode() and equals() methods for storing an instance
    * in Flink's state or performing a keyBy operation.
    *
    * @param name name of the operation
    * @param t type information to be validated
    */
  def validateEqualsHashCode(name: String, t: TypeInformation[_]): Unit = t match {

    // make sure that a POJO class is a valid state type
    case pt: PojoTypeInfo[_] =>
      // we don't check the types recursively to give a chance of wrapping
      // proper hashCode/equals methods around an immutable type
      validateEqualsHashCode(name, pt.getClass)
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

  /**
    * Checks if a class is a Java primitive wrapper.
    */
  def isPrimitiveWrapper(clazz: Class[_]): Boolean = {
    clazz == classOf[java.lang.Boolean] ||
        clazz == classOf[java.lang.Byte] ||
        clazz == classOf[java.lang.Character] ||
        clazz == classOf[java.lang.Short] ||
        clazz == classOf[java.lang.Integer] ||
        clazz == classOf[java.lang.Long] ||
        clazz == classOf[java.lang.Double] ||
        clazz == classOf[java.lang.Float]
  }

  /**
    * Checks if one class can be assigned to a variable of another class.
    *
    * Adopted from o.a.commons.lang.ClassUtils#isAssignable(java.lang.Class[], java.lang.Class[])
    * but without null checks.
    */
  def isAssignable(classArray: Array[Class[_]], toClassArray: Array[Class[_]]): Boolean = {
    if (classArray.length != toClassArray.length) {
      return false
    }
    var i = 0
    while (i < classArray.length) {
      if (!isAssignable(classArray(i), toClassArray(i))) {
        return false
      }
      i += 1
    }
    true
  }

  /**
    * Checks if one class can be assigned to a variable of another class.
    *
    * Adopted from o.a.commons.lang.ClassUtils#isAssignable(java.lang.Class, java.lang.Class) but
    * without null checks.
    */
  def isAssignable(cls: Class[_], toClass: Class[_]): Boolean = {
    if (cls.equals(toClass)) {
      return true
    }
    if (cls.isPrimitive) {
      if (!toClass.isPrimitive) {
        return false
      }
      if (java.lang.Integer.TYPE.equals(cls)) {
        return java.lang.Long.TYPE.equals(toClass) ||
            java.lang.Float.TYPE.equals(toClass) ||
            java.lang.Double.TYPE.equals(toClass)
      }
      if (java.lang.Long.TYPE.equals(cls)) {
        return java.lang.Float.TYPE.equals(toClass) ||
            java.lang.Double.TYPE.equals(toClass)
      }
      if (java.lang.Boolean.TYPE.equals(cls)) {
        return false
      }
      if (java.lang.Double.TYPE.equals(cls)) {
        return false
      }
      if (java.lang.Float.TYPE.equals(cls)) {
        return java.lang.Double.TYPE.equals(toClass)
      }
      if (java.lang.Character.TYPE.equals(cls)) {
        return java.lang.Integer.TYPE.equals(toClass) ||
            java.lang.Long.TYPE.equals(toClass) ||
            java.lang.Float.TYPE.equals(toClass) ||
            java.lang.Double.TYPE.equals(toClass)
      }
      if (java.lang.Short.TYPE.equals(cls)) {
        return java.lang.Integer.TYPE.equals(toClass) ||
            java.lang.Long.TYPE.equals(toClass) ||
            java.lang.Float.TYPE.equals(toClass) ||
            java.lang.Double.TYPE.equals(toClass)
      }
      if (java.lang.Byte.TYPE.equals(cls)) {
        return java.lang.Short.TYPE.equals(toClass) ||
            java.lang.Integer.TYPE.equals(toClass) ||
            java.lang.Long.TYPE.equals(toClass) ||
            java.lang.Float.TYPE.equals(toClass) ||
            java.lang.Double.TYPE.equals(toClass)
      }
      // should never get here
      return false
    }
    toClass.isAssignableFrom(cls)
  }
}
