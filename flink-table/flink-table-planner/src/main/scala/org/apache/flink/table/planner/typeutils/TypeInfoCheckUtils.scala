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
import org.apache.flink.api.java.typeutils.{MapTypeInfo, ObjectArrayTypeInfo}
import org.apache.flink.table.planner.validate._
import org.apache.flink.table.runtime.typeutils.{BigDecimalTypeInfo, DecimalDataTypeInfo}
import org.apache.flink.table.typeutils.TimeIntervalTypeInfo

object TypeInfoCheckUtils {

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

  def isArray(dataType: TypeInformation[_]): Boolean = dataType match {
    case _: ObjectArrayTypeInfo[_, _] |
         _: BasicArrayTypeInfo[_, _] |
         _: PrimitiveArrayTypeInfo[_]  => true
    case _ => false
  }

  def isMap(dataType: TypeInformation[_]): Boolean =
    dataType.isInstanceOf[MapTypeInfo[_, _]]

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

  def assertOrderableExpr(dataType: TypeInformation[_], caller: String): ValidationResult = {
    if (dataType.isSortKeyType) {
      ValidationSuccess
    } else {
      ValidationFailure(s"$caller requires orderable types, get $dataType here")
    }
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
