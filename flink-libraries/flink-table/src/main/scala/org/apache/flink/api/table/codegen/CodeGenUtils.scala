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

package org.apache.flink.api.table.codegen

import java.util.concurrent.atomic.AtomicInteger

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo._
import org.apache.flink.api.common.typeinfo.{NumericTypeInfo, TypeInformation}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils.{PojoTypeInfo, TupleTypeInfo}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.api.table.typeinfo.RowTypeInfo

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
    case LONG_TYPE_INFO => "-1"
    case SHORT_TYPE_INFO => "-1"
    case BYTE_TYPE_INFO => "-1"
    case FLOAT_TYPE_INFO => "-1.0f"
    case DOUBLE_TYPE_INFO => "-1.0d"
    case BOOLEAN_TYPE_INFO => "false"
    case STRING_TYPE_INFO => "\"<empty>\""
    case CHAR_TYPE_INFO => "'\\0'"
    case _ => "null"
  }

  def requireNumeric(genExpr: GeneratedExpression) = genExpr.resultType match {
    case nti: NumericTypeInfo[_] => // ok
    case _ => throw new CodeGenException("Numeric expression type expected.")
  }

  def requireString(genExpr: GeneratedExpression) = genExpr.resultType match {
    case STRING_TYPE_INFO => // ok
    case _ => throw new CodeGenException("String expression type expected.")
  }

  def requireBoolean(genExpr: GeneratedExpression) = genExpr.resultType match {
    case BOOLEAN_TYPE_INFO => // ok
    case _ => throw new CodeGenException("Boolean expression type expected.")
  }

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

  def isNumeric(genExpr: GeneratedExpression): Boolean = isNumeric(genExpr.resultType)

  def isNumeric(typeInfo: TypeInformation[_]): Boolean = typeInfo match {
    case nti: NumericTypeInfo[_] => true
    case _ => false
  }

  def isString(genExpr: GeneratedExpression): Boolean = isString(genExpr.resultType)

  def isString(typeInfo: TypeInformation[_]): Boolean = typeInfo match {
    case STRING_TYPE_INFO => true
    case _ => false
  }

  def isBoolean(genExpr: GeneratedExpression): Boolean = isBoolean(genExpr.resultType)

  def isBoolean(typeInfo: TypeInformation[_]): Boolean = typeInfo match {
    case BOOLEAN_TYPE_INFO => true
    case _ => false
  }

  // ----------------------------------------------------------------------------------------------

  sealed abstract class FieldAccessor

  case class ObjectFieldAccessor(fieldName: String) extends FieldAccessor

  case class ObjectMethodAccessor(methodName: String) extends FieldAccessor

  case class ProductAccessor(i: Int) extends FieldAccessor

  def fieldAccessorFor(compType: CompositeType[_], index: Int): FieldAccessor = {
    compType match {
      case ri: RowTypeInfo =>
        ProductAccessor(index)

      case cc: CaseClassTypeInfo[_] =>
        ObjectMethodAccessor(cc.getFieldNames()(index))

      case javaTup: TupleTypeInfo[_] =>
        ObjectFieldAccessor("f" + index)

      case pj: PojoTypeInfo[_] =>
        ObjectFieldAccessor(pj.getFieldNames()(index))

      case _ => throw new CodeGenException("Unsupported composite type.")
    }
  }
}
