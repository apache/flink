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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.`type`._
import org.apache.flink.table.dataformat._
import org.apache.flink.table.typeutils.TypeCheckUtils

import java.lang.reflect.Method
import java.lang.{Boolean => JBoolean, Byte => JByte, Character => JChar, Double => JDouble, Float => JFloat, Integer => JInt, Long => JLong, Short => JShort}
import java.util.concurrent.atomic.AtomicInteger

object CodeGenUtils {

  // ------------------------------- DEFAULT TERMS ------------------------------------------

  val DEFAULT_TIMEZONE_TERM = "timeZone"

  // -------------------------- CANONICAL CLASS NAMES ---------------------------------------

  val BINARY_ROW: String = className[BinaryRow]
  val BINARY_STRING: String = className[BinaryString]
  val BASE_ROW: String = className[BaseRow]
  val GENERIC_ROW: String = className[GenericRow]

  // ----------------------------------------------------------------------------------------

  private val nameCounter = new AtomicInteger

  def newName(name: String): String = {
    s"$name$$${nameCounter.getAndIncrement}"
  }

  def newNames(names: String*): Seq[String] = {
    require(names.toSet.size == names.length, "Duplicated names")
    val newId = nameCounter.getAndIncrement
    names.map(name => s"$name$$$newId")
  }

  /**
    * Retrieve the canonical name of a class type.
    */
  def className[T](implicit m: Manifest[T]): String = m.runtimeClass.getCanonicalName

  def needCopyForType(t: InternalType): Boolean = t match {
    case InternalTypes.STRING => true
    case _: ArrayType => true
    case _: MapType => true
    case _: RowType => true
    case _: GenericType[_] => true
    case _ => false
  }

  // when casting we first need to unbox Primitives, for example,
  // float a = 1.0f;
  // byte b = (byte) a;
  // works, but for boxed types we need this:
  // Float a = 1.0f;
  // Byte b = (byte)(float) a;
  def primitiveTypeTermForType(t: InternalType): String = t match {
    case InternalTypes.INT => "int"
    case InternalTypes.LONG => "long"
    case InternalTypes.SHORT => "short"
    case InternalTypes.BYTE => "byte"
    case InternalTypes.FLOAT => "float"
    case InternalTypes.DOUBLE => "double"
    case InternalTypes.BOOLEAN => "boolean"
    case InternalTypes.CHAR => "char"

    case InternalTypes.DATE => "int"
    case InternalTypes.TIME => "int"
    case InternalTypes.TIMESTAMP => "long"

    // TODO: support [INTERVAL_MONTHS] and [INTERVAL_MILLIS] in the future

    case _ => boxedTypeTermForType(t)
  }

  def boxedTypeTermForType(t: InternalType): String = t match {
    case InternalTypes.INT => className[JInt]
    case InternalTypes.LONG => className[JLong]
    case InternalTypes.SHORT => className[JShort]
    case InternalTypes.BYTE => className[JByte]
    case InternalTypes.FLOAT => className[JFloat]
    case InternalTypes.DOUBLE => className[JDouble]
    case InternalTypes.BOOLEAN => className[JBoolean]
    case InternalTypes.CHAR => className[JChar]

    case InternalTypes.DATE => boxedTypeTermForType(InternalTypes.INT)
    case InternalTypes.TIME => boxedTypeTermForType(InternalTypes.INT)
    case InternalTypes.TIMESTAMP => boxedTypeTermForType(InternalTypes.LONG)

    case InternalTypes.STRING => BINARY_STRING

    // TODO: Support it when we introduce [Decimal]
    case _: DecimalType => throw new UnsupportedOperationException
    // BINARY is also an ArrayType and uses BinaryArray internally too
    case _: ArrayType => className[BinaryArray]
    case _: MapType => className[BinaryMap]
    case _: RowType => className[BaseRow]

    case gt: GenericType[_] => gt.getTypeInfo.getTypeClass.getCanonicalName
  }

  /**
    * Gets the boxed type term from external type info.
    * We only use TypeInformation to store external type info.
    */
  def boxedTypeTermForExternalType(t: TypeInformation[_]): String = t.getTypeClass.getCanonicalName

  /**
    * Gets the default value for a primitive type, and null for generic types
    */
  def primitiveDefaultValue(t: InternalType): String = t match {
    case InternalTypes.INT | InternalTypes.BYTE | InternalTypes.SHORT => "-1"
    case InternalTypes.LONG => "-1L"
    case InternalTypes.FLOAT => "-1.0f"
    case InternalTypes.DOUBLE => "-1.0d"
    case InternalTypes.BOOLEAN => "false"
    case InternalTypes.STRING => s"$BINARY_STRING.EMPTY_UTF8"
    case InternalTypes.CHAR => "'\\0'"

    case InternalTypes.DATE | InternalTypes.TIME => "-1"
    case InternalTypes.TIMESTAMP => "-1L"

    case _ => "null"
  }

  // -------------------------- Method & Enum ---------------------------------------

  def qualifyMethod(method: Method): String =
    method.getDeclaringClass.getCanonicalName + "." + method.getName

  def qualifyEnum(enum: Enum[_]): String =
    enum.getClass.getCanonicalName + "." + enum.name()

  def compareEnum(term: String, enum: Enum[_]): Boolean = term == qualifyEnum(enum)

  def getEnum(genExpr: GeneratedExpression): Enum[_] = {
    val split = genExpr.resultTerm.split('.')
    val value = split.last
    val clazz = genExpr.resultType.asInstanceOf[GenericType[_]].getTypeClass
    enumValueOf(clazz, value)
  }

  def enumValueOf[T <: Enum[T]](cls: Class[_], stringValue: String): Enum[_] =
    Enum.valueOf(cls.asInstanceOf[Class[T]], stringValue).asInstanceOf[Enum[_]]

  // --------------------------- Require Check ---------------------------------------

  def requireNumeric(genExpr: GeneratedExpression): Unit =
    if (!TypeCheckUtils.isNumeric(genExpr.resultType)) {
      throw new CodeGenException("Numeric expression type expected, but was " +
        s"'${genExpr.resultType}'.")
    }

  def requireComparable(genExpr: GeneratedExpression): Unit =
    if (!TypeCheckUtils.isComparable(genExpr.resultType)) {
      throw new CodeGenException(s"Comparable type expected, but was '${genExpr.resultType}'.")
    }

  def requireString(genExpr: GeneratedExpression): Unit =
    if (!TypeCheckUtils.isString(genExpr.resultType)) {
      throw new CodeGenException("String expression type expected.")
    }

  def requireBoolean(genExpr: GeneratedExpression): Unit =
    if (!TypeCheckUtils.isBoolean(genExpr.resultType)) {
      throw new CodeGenException("Boolean expression type expected.")
    }

  def requireArray(genExpr: GeneratedExpression): Unit =
    if (!TypeCheckUtils.isArray(genExpr.resultType)) {
      throw new CodeGenException("Array expression type expected.")
    }

  def requireMap(genExpr: GeneratedExpression): Unit =
    if (!TypeCheckUtils.isMap(genExpr.resultType)) {
      throw new CodeGenException("Map expression type expected.")
    }

  def requireInteger(genExpr: GeneratedExpression): Unit =
    if (!TypeCheckUtils.isInteger(genExpr.resultType)) {
      throw new CodeGenException("Integer expression type expected.")
    }

  // --------------------------- Generate Utils ---------------------------------------

  def generateOutputRecordStatement(
      t: InternalType,
      clazz: Class[_],
      outRecordTerm: String,
      outRecordWriterTerm: Option[String] = None): String = {
    t match {
      case rt: RowType if clazz == classOf[BinaryRow] =>
        val writerTerm = outRecordWriterTerm.getOrElse(
          throw new CodeGenException("No writer is specified when writing BinaryRow record.")
        )
        val binaryRowWriter = className[BinaryRowWriter]
        val typeTerm = clazz.getCanonicalName
        s"""
           |final $typeTerm $outRecordTerm = new $typeTerm(${rt.getArity});
           |final $binaryRowWriter $writerTerm = new $binaryRowWriter($outRecordTerm);
           |""".stripMargin.trim
      case rt: RowType if classOf[ObjectArrayRow].isAssignableFrom(clazz) =>
        val typeTerm = clazz.getCanonicalName
        s"final $typeTerm $outRecordTerm = new $typeTerm(${rt.getArity});"
      // TODO: support [JoinedRow] in the future
      case _ =>
        val typeTerm = boxedTypeTermForType(t)
        s"final $typeTerm $outRecordTerm = new $typeTerm();"
    }
  }

}
