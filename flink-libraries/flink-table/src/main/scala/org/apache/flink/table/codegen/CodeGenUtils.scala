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

package org.apache.flink.table.codegen

import java.lang.reflect.{Field, Method}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.calcite.util.BuiltInMethod
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo._
import org.apache.flink.api.common.typeinfo.{FractionalTypeInfo, SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils.{PojoTypeInfo, RowTypeInfo, TupleTypeInfo, TypeExtractor}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.table.typeutils.{TimeIntervalTypeInfo, TypeCheckUtils}

object CodeGenUtils {

  private val nameCounter = new AtomicInteger

  def newName(name: String): String = {
    s"$name$$${nameCounter.getAndIncrement}"
  }

  // when casting we first need to unbox Primitives, for example,
  // float a = 1.0f;
  // byte b = (byte) a;
  // works, but for boxed types we need this:
  // Float a = 1.0f;
  // Byte b = (byte)(float) a;
  def primitiveTypeTermForTypeInfo(tpe: TypeInformation[_]): String = tpe match {
    case INT_TYPE_INFO => "int"
    case LONG_TYPE_INFO => "long"
    case SHORT_TYPE_INFO => "short"
    case BYTE_TYPE_INFO => "byte"
    case FLOAT_TYPE_INFO => "float"
    case DOUBLE_TYPE_INFO => "double"
    case BOOLEAN_TYPE_INFO => "boolean"
    case CHAR_TYPE_INFO => "char"

    // From PrimitiveArrayTypeInfo we would get class "int[]", scala reflections
    // does not seem to like this, so we manually give the correct type here.
    case INT_PRIMITIVE_ARRAY_TYPE_INFO => "int[]"
    case LONG_PRIMITIVE_ARRAY_TYPE_INFO => "long[]"
    case SHORT_PRIMITIVE_ARRAY_TYPE_INFO => "short[]"
    case BYTE_PRIMITIVE_ARRAY_TYPE_INFO => "byte[]"
    case FLOAT_PRIMITIVE_ARRAY_TYPE_INFO => "float[]"
    case DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO => "double[]"
    case BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO => "boolean[]"
    case CHAR_PRIMITIVE_ARRAY_TYPE_INFO => "char[]"

    // internal primitive representation of time points
    case SqlTimeTypeInfo.DATE => "int"
    case SqlTimeTypeInfo.TIME => "int"
    case SqlTimeTypeInfo.TIMESTAMP => "long"

    // internal primitive representation of time intervals
    case TimeIntervalTypeInfo.INTERVAL_MONTHS => "int"
    case TimeIntervalTypeInfo.INTERVAL_MILLIS => "long"

    case _ =>
      tpe.getTypeClass.getCanonicalName
  }

  def boxedTypeTermForTypeInfo(tpe: TypeInformation[_]): String = tpe match {
    // From PrimitiveArrayTypeInfo we would get class "int[]", scala reflections
    // does not seem to like this, so we manually give the correct type here.
    case INT_PRIMITIVE_ARRAY_TYPE_INFO => "int[]"
    case LONG_PRIMITIVE_ARRAY_TYPE_INFO => "long[]"
    case SHORT_PRIMITIVE_ARRAY_TYPE_INFO => "short[]"
    case BYTE_PRIMITIVE_ARRAY_TYPE_INFO => "byte[]"
    case FLOAT_PRIMITIVE_ARRAY_TYPE_INFO => "float[]"
    case DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO => "double[]"
    case BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO => "boolean[]"
    case CHAR_PRIMITIVE_ARRAY_TYPE_INFO => "char[]"

    case _ =>
      tpe.getTypeClass.getCanonicalName
  }

  def primitiveDefaultValue(tpe: TypeInformation[_]): String = tpe match {
    case INT_TYPE_INFO => "-1"
    case LONG_TYPE_INFO => "-1L"
    case SHORT_TYPE_INFO => "-1"
    case BYTE_TYPE_INFO => "-1"
    case FLOAT_TYPE_INFO => "-1.0f"
    case DOUBLE_TYPE_INFO => "-1.0d"
    case BOOLEAN_TYPE_INFO => "false"
    case STRING_TYPE_INFO => "\"\""
    case CHAR_TYPE_INFO => "'\\0'"
    case SqlTimeTypeInfo.DATE | SqlTimeTypeInfo.TIME => "-1"
    case SqlTimeTypeInfo.TIMESTAMP => "-1L"
    case TimeIntervalTypeInfo.INTERVAL_MONTHS => "-1"
    case TimeIntervalTypeInfo.INTERVAL_MILLIS => "-1L"

    case _ => "null"
  }

  def superPrimitive(typeInfo: TypeInformation[_]): String = typeInfo match {
    case _: FractionalTypeInfo[_] => "double"
    case _ => "long"
  }

  def qualifyMethod(method: Method): String =
    method.getDeclaringClass.getCanonicalName + "." + method.getName

  def qualifyEnum(enum: Enum[_]): String =
    enum.getClass.getCanonicalName + "." + enum.name()

  def internalToTimePointCode(resultType: TypeInformation[_], resultTerm: String) =
    resultType match {
      case SqlTimeTypeInfo.DATE =>
        s"${qualifyMethod(BuiltInMethod.INTERNAL_TO_DATE.method)}($resultTerm)"
      case SqlTimeTypeInfo.TIME =>
        s"${qualifyMethod(BuiltInMethod.INTERNAL_TO_TIME.method)}($resultTerm)"
      case SqlTimeTypeInfo.TIMESTAMP =>
        s"${qualifyMethod(BuiltInMethod.INTERNAL_TO_TIMESTAMP.method)}($resultTerm)"
    }

  def timePointToInternalCode(resultType: TypeInformation[_], resultTerm: String) =
    resultType match {
      case SqlTimeTypeInfo.DATE =>
        s"${qualifyMethod(BuiltInMethod.DATE_TO_INT.method)}($resultTerm)"
      case SqlTimeTypeInfo.TIME =>
        s"${qualifyMethod(BuiltInMethod.TIME_TO_INT.method)}($resultTerm)"
      case SqlTimeTypeInfo.TIMESTAMP =>
        s"${qualifyMethod(BuiltInMethod.TIMESTAMP_TO_LONG.method)}($resultTerm)"
    }

  def compareEnum(term: String, enum: Enum[_]): Boolean = term == qualifyEnum(enum)

  def getEnum(genExpr: GeneratedExpression): Enum[_] = {
    val split = genExpr.resultTerm.split('.')
    val value = split.last
    val clazz = genExpr.resultType.getTypeClass
    enumValueOf(clazz, value)
  }

  def enumValueOf[T <: Enum[T]](cls: Class[_], stringValue: String): Enum[_] =
    Enum.valueOf(cls.asInstanceOf[Class[T]], stringValue).asInstanceOf[Enum[_]]

  // ----------------------------------------------------------------------------------------------

  def requireNumeric(genExpr: GeneratedExpression) =
    if (!TypeCheckUtils.isNumeric(genExpr.resultType)) {
      throw new CodeGenException("Numeric expression type expected, but was " +
        s"'${genExpr.resultType}'.")
    }

  def requireComparable(genExpr: GeneratedExpression) =
    if (!TypeCheckUtils.isComparable(genExpr.resultType)) {
      throw new CodeGenException(s"Comparable type expected, but was '${genExpr.resultType}'.")
    }

  def requireString(genExpr: GeneratedExpression) =
    if (!TypeCheckUtils.isString(genExpr.resultType)) {
      throw new CodeGenException("String expression type expected.")
    }

  def requireBoolean(genExpr: GeneratedExpression) =
    if (!TypeCheckUtils.isBoolean(genExpr.resultType)) {
      throw new CodeGenException("Boolean expression type expected.")
    }

  def requireTemporal(genExpr: GeneratedExpression) =
    if (!TypeCheckUtils.isTemporal(genExpr.resultType)) {
      throw new CodeGenException("Temporal expression type expected.")
    }

  def requireTimeInterval(genExpr: GeneratedExpression) =
    if (!TypeCheckUtils.isTimeInterval(genExpr.resultType)) {
      throw new CodeGenException("Interval expression type expected.")
    }

  def requireArray(genExpr: GeneratedExpression) =
    if (!TypeCheckUtils.isArray(genExpr.resultType)) {
      throw new CodeGenException("Array expression type expected.")
    }

  def requireInteger(genExpr: GeneratedExpression) =
    if (!TypeCheckUtils.isInteger(genExpr.resultType)) {
      throw new CodeGenException("Integer expression type expected.")
    }

  // ----------------------------------------------------------------------------------------------

  def isReference(genExpr: GeneratedExpression): Boolean = isReference(genExpr.resultType)

  def isReference(typeInfo: TypeInformation[_]): Boolean = typeInfo match {
    case INT_TYPE_INFO
         | LONG_TYPE_INFO
         | SHORT_TYPE_INFO
         | BYTE_TYPE_INFO
         | FLOAT_TYPE_INFO
         | DOUBLE_TYPE_INFO
         | BOOLEAN_TYPE_INFO
         | CHAR_TYPE_INFO => false
    case _ => true
  }

  // ----------------------------------------------------------------------------------------------

  sealed abstract class FieldAccessor

  case class ObjectFieldAccessor(field: Field) extends FieldAccessor

  case class ObjectGenericFieldAccessor(name: String) extends FieldAccessor

  case class ObjectPrivateFieldAccessor(field: Field) extends FieldAccessor

  case class ObjectMethodAccessor(methodName: String) extends FieldAccessor

  case class ProductAccessor(i: Int) extends FieldAccessor

  def fieldAccessorFor(compType: CompositeType[_], index: Int): FieldAccessor = {
    compType match {
      case ri: RowTypeInfo =>
        ProductAccessor(index)

      case cc: CaseClassTypeInfo[_] =>
        ObjectMethodAccessor(cc.getFieldNames()(index))

      case javaTup: TupleTypeInfo[_] =>
        ObjectGenericFieldAccessor("f" + index)

      case pt: PojoTypeInfo[_] =>
        val fieldName = pt.getFieldNames()(index)
        getFieldAccessor(pt.getTypeClass, fieldName)

      case _ => throw new CodeGenException(s"Unsupported composite type: '${compType}'")
    }
  }

  def getFieldAccessor(clazz: Class[_], fieldName: String): FieldAccessor = {
    val field = TypeExtractor.getDeclaredField(clazz, fieldName)
    if (field.isAccessible) {
      ObjectFieldAccessor(field)
    }
    else {
      ObjectPrivateFieldAccessor(field)
    }
  }

  def isFieldPrimitive(field: Field): Boolean = field.getType.isPrimitive

  def reflectiveFieldReadAccess(fieldTerm: String, field: Field, objectTerm: String): String =
    field.getType match {
      case java.lang.Integer.TYPE => s"$fieldTerm.getInt($objectTerm)"
      case java.lang.Long.TYPE => s"$fieldTerm.getLong($objectTerm)"
      case java.lang.Short.TYPE => s"$fieldTerm.getShort($objectTerm)"
      case java.lang.Byte.TYPE => s"$fieldTerm.getByte($objectTerm)"
      case java.lang.Float.TYPE => s"$fieldTerm.getFloat($objectTerm)"
      case java.lang.Double.TYPE => s"$fieldTerm.getDouble($objectTerm)"
      case java.lang.Boolean.TYPE => s"$fieldTerm.getBoolean($objectTerm)"
      case java.lang.Character.TYPE => s"$fieldTerm.getChar($objectTerm)"
      case _ => s"(${field.getType.getCanonicalName}) $fieldTerm.get($objectTerm)"
    }

  def reflectiveFieldWriteAccess(
      fieldTerm: String,
      field: Field,
      objectTerm: String,
      valueTerm: String)
    : String =
    field.getType match {
      case java.lang.Integer.TYPE => s"$fieldTerm.setInt($objectTerm, $valueTerm)"
      case java.lang.Long.TYPE => s"$fieldTerm.setLong($objectTerm, $valueTerm)"
      case java.lang.Short.TYPE => s"$fieldTerm.setShort($objectTerm, $valueTerm)"
      case java.lang.Byte.TYPE => s"$fieldTerm.setByte($objectTerm, $valueTerm)"
      case java.lang.Float.TYPE => s"$fieldTerm.setFloat($objectTerm, $valueTerm)"
      case java.lang.Double.TYPE => s"$fieldTerm.setDouble($objectTerm, $valueTerm)"
      case java.lang.Boolean.TYPE => s"$fieldTerm.setBoolean($objectTerm, $valueTerm)"
      case java.lang.Character.TYPE => s"$fieldTerm.setChar($objectTerm, $valueTerm)"
      case _ => s"$fieldTerm.set($objectTerm, $valueTerm)"
    }

  def absFloorRound(value: Double): Long = {

    if (value >= 0) {
      java.lang.Math.round(java.lang.Math.floor((value)))
    } else {
      0 - java.lang.Math.round(java.lang.Math.floor((java.lang.Math.abs(value))))
    }

  }
}
