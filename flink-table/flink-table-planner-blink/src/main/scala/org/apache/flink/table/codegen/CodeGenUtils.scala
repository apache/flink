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

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.{AtomicType => AtomicTypeInfo}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.memory.MemorySegment
import org.apache.flink.table.`type`._
import org.apache.flink.table.calcite.FlinkPlannerImpl
import org.apache.flink.table.dataformat._
import org.apache.flink.table.typeutils.TypeCheckUtils

import java.lang.reflect.Method
import java.lang.{Boolean => JBoolean, Byte => JByte, Character => JChar, Double => JDouble, Float => JFloat, Integer => JInt, Long => JLong, Short => JShort}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

object CodeGenUtils {

  // ------------------------------- DEFAULT TERMS ------------------------------------------

  val DEFAULT_TIMEZONE_TERM = "timeZone"

  // -------------------------- CANONICAL CLASS NAMES ---------------------------------------

  val BINARY_ROW: String = className[BinaryRow]
  val BINARY_ARRAY: String = className[BinaryArray]
  val BINARY_GENERIC: String = className[BinaryGeneric[_]]
  val BINARY_STRING: String = className[BinaryString]
  val BASE_ROW: String = className[BaseRow]
  val GENERIC_ROW: String = className[GenericRow]
  val SEGMENT: String = className[MemorySegment]

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
    case InternalTypes.BINARY => "byte[]"

    case _: DecimalType => className[Decimal]
    // BINARY is also an ArrayType and uses BinaryArray internally too
    case _: ArrayType => className[BinaryArray]
    case _: MapType => className[BinaryMap]
    case _: RowType => className[BaseRow]

    case _: GenericType[_] => className[BinaryGeneric[_]]
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
      case _: RowType if clazz == classOf[JoinedRow] =>
        val typeTerm = clazz.getCanonicalName
        s"final $typeTerm $outRecordTerm = new $typeTerm();"
      case _ =>
        val typeTerm = boxedTypeTermForType(t)
        s"final $typeTerm $outRecordTerm = new $typeTerm();"
    }
  }

  def baseRowFieldReadAccess(
      ctx: CodeGeneratorContext, pos: Int, rowTerm: String, fieldType: InternalType) : String =
    baseRowFieldReadAccess(ctx, pos.toString, rowTerm, fieldType)

  def baseRowFieldReadAccess(
      ctx: CodeGeneratorContext, pos: String, rowTerm: String, fieldType: InternalType) : String =
    fieldType match {
      case InternalTypes.INT => s"$rowTerm.getInt($pos)"
      case InternalTypes.LONG => s"$rowTerm.getLong($pos)"
      case InternalTypes.SHORT => s"$rowTerm.getShort($pos)"
      case InternalTypes.BYTE => s"$rowTerm.getByte($pos)"
      case InternalTypes.FLOAT => s"$rowTerm.getFloat($pos)"
      case InternalTypes.DOUBLE => s"$rowTerm.getDouble($pos)"
      case InternalTypes.BOOLEAN => s"$rowTerm.getBoolean($pos)"
      case InternalTypes.STRING => s"$rowTerm.getString($pos)"
      case InternalTypes.BINARY => s"$rowTerm.getBinary($pos)"
      case dt: DecimalType => s"$rowTerm.getDecimal($pos, ${dt.precision()}, ${dt.scale()})"
      case InternalTypes.CHAR => s"$rowTerm.getChar($pos)"
      case _: TimestampType => s"$rowTerm.getLong($pos)"
      case _: DateType => s"$rowTerm.getInt($pos)"
      case InternalTypes.TIME => s"$rowTerm.getInt($pos)"
      case _: ArrayType => s"$rowTerm.getArray($pos)"
      case _: MapType  => s"$rowTerm.getMap($pos)"
      case rt: RowType => s"$rowTerm.getRow($pos, ${rt.getArity})"
      case _: GenericType[_] => s"$rowTerm.getGeneric($pos)"
    }

  /**
    * Generates code for comparing two field.
    */
  def genCompare(
      ctx: CodeGeneratorContext,
      t: InternalType,
      nullsIsLast: Boolean,
      c1: String,
      c2: String): String = t match {
    case InternalTypes.BOOLEAN => s"($c1 == $c2 ? 0 : ($c1 ? 1 : -1))"
    case _: PrimitiveType | _: DateType | _: TimeType | _: TimestampType =>
      s"($c1 > $c2 ? 1 : $c1 < $c2 ? -1 : 0)"
    case InternalTypes.BINARY =>
      val sortUtil = classOf[org.apache.flink.table.runtime.sort.SortUtil].getCanonicalName
      s"$sortUtil.compareBinary($c1, $c2)"
    case at: ArrayType =>
      val compareFunc = newName("compareArray")
      val compareCode = genArrayCompare(
        ctx,
        FlinkPlannerImpl.getNullDefaultOrder(true), at, "a", "b")
      val funcCode: String =
        s"""
          public int $compareFunc($BINARY_ARRAY a, $BINARY_ARRAY b) {
            $compareCode
            return 0;
          }
        """
      ctx.addReusableMember(funcCode)
      s"$compareFunc($c1, $c2)"
    case rowType: RowType =>
      val orders = rowType.getFieldTypes.map(_ => true)
      val comparisons = genRowCompare(
        ctx,
        rowType.getFieldTypes.indices.toArray,
        rowType.getFieldTypes,
        orders,
        FlinkPlannerImpl.getNullDefaultOrders(orders),
        "a",
        "b")
      val compareFunc = newName("compareRow")
      val funcCode: String =
        s"""
          public int $compareFunc($BASE_ROW a, $BASE_ROW b) {
            $comparisons
            return 0;
          }
        """
      ctx.addReusableMember(funcCode)
      s"$compareFunc($c1, $c2)"
    case gt: GenericType[_] =>
      val ser = ctx.addReusableObject(gt.getSerializer, "serializer")
      val comp = ctx.addReusableObject(
        gt.getTypeInfo.asInstanceOf[AtomicTypeInfo[_]].createComparator(true, new ExecutionConfig),
        "comparator")
      s"""
         |$comp.compare(
         |  $BINARY_GENERIC.getJavaObjectFromBinaryGeneric($c1, $ser),
         |  $BINARY_GENERIC.getJavaObjectFromBinaryGeneric($c2, $ser)
         |)
       """.stripMargin
    case other if other.isInstanceOf[AtomicType] => s"$c1.compareTo($c2)"
  }

  /**
    * Generates code for comparing array.
    */
  def genArrayCompare(
      ctx: CodeGeneratorContext, nullsIsLast: Boolean, t: ArrayType, a: String, b: String)
    : String = {
    val nullIsLastRet = if (nullsIsLast) 1 else -1
    val elementType = t.getElementType
    val fieldA = newName("fieldA")
    val isNullA = newName("isNullA")
    val lengthA = newName("lengthA")
    val fieldB = newName("fieldB")
    val isNullB = newName("isNullB")
    val lengthB = newName("lengthB")
    val minLength = newName("minLength")
    val i = newName("i")
    val comp = newName("comp")
    val typeTerm = primitiveTypeTermForType(elementType)
    s"""
        int $lengthA = a.numElements();
        int $lengthB = b.numElements();
        int $minLength = ($lengthA > $lengthB) ? $lengthB : $lengthA;
        for (int $i = 0; $i < $minLength; $i++) {
          boolean $isNullA = a.isNullAt($i);
          boolean $isNullB = b.isNullAt($i);
          if ($isNullA && $isNullB) {
            // Continue to compare the next element
          } else if ($isNullA) {
            return $nullIsLastRet;
          } else if ($isNullB) {
            return ${-nullIsLastRet};
          } else {
            $typeTerm $fieldA = ${baseRowFieldReadAccess(ctx, i, a, elementType)};
            $typeTerm $fieldB = ${baseRowFieldReadAccess(ctx, i, b, elementType)};
            int $comp = ${genCompare(ctx, elementType, nullsIsLast, fieldA, fieldB)};
            if ($comp != 0) {
              return $comp;
            }
          }
        }

        if ($lengthA < $lengthB) {
          return -1;
        } else if ($lengthA > $lengthB) {
          return 1;
        }
      """
  }

  /**
    * Generates code for comparing row keys.
    */
  def genRowCompare(
      ctx: CodeGeneratorContext,
      keys: Array[Int],
      keyTypes: Array[InternalType],
      orders: Array[Boolean],
      nullsIsLast: Array[Boolean],
      row1: String,
      row2: String): String = {

    val compares = new mutable.ArrayBuffer[String]

    for (i <- keys.indices) {
      val index = keys(i)

      val symbol = if (orders(i)) "" else "-"

      val nullIsLastRet = if (nullsIsLast(i)) 1 else -1

      val t = keyTypes(i)

      val typeTerm = primitiveTypeTermForType(t)
      val fieldA = newName("fieldA")
      val isNullA = newName("isNullA")
      val fieldB = newName("fieldB")
      val isNullB = newName("isNullB")
      val comp = newName("comp")

      val code =
        s"""
           |boolean $isNullA = $row1.isNullAt($index);
           |boolean $isNullB = $row2.isNullAt($index);
           |if ($isNullA && $isNullB) {
           |  // Continue to compare the next element
           |} else if ($isNullA) {
           |  return $nullIsLastRet;
           |} else if ($isNullB) {
           |  return ${-nullIsLastRet};
           |} else {
           |  $typeTerm $fieldA = ${baseRowFieldReadAccess(ctx, index, row1, t)};
           |  $typeTerm $fieldB = ${baseRowFieldReadAccess(ctx, index, row2, t)};
           |  int $comp = ${genCompare(ctx, t, nullsIsLast(i), fieldA, fieldB)};
           |  if ($comp != 0) {
           |    return $symbol$comp;
           |  }
           |}
         """.stripMargin
      compares += code
    }
    compares.mkString
  }

}
