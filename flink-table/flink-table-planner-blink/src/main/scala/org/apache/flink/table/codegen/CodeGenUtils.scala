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
import org.apache.flink.core.memory.MemorySegment
import org.apache.flink.table.`type`.InternalTypeUtils.getInternalClassForType
import org.apache.flink.table.`type`.TypeConverters.createInternalTypeFromTypeInfo
import org.apache.flink.table.`type`._
import org.apache.flink.table.dataformat.DataFormatConverters.IdentityConverter
import org.apache.flink.table.dataformat.{Decimal, _}
import org.apache.flink.table.dataformat.util.BinaryRowUtil.BYTE_ARRAY_BASE_OFFSET
import org.apache.flink.table.dataview.StateDataViewStore
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.generated.{AggsHandleFunction, HashFunction, NamespaceAggsHandleFunction}
import org.apache.flink.table.typeutils.TypeCheckUtils
import org.apache.flink.table.util.MurmurHashUtil
import org.apache.flink.types.Row

import java.lang.reflect.Method
import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Integer => JInt, Long => JLong, Short => JShort}
import java.util.concurrent.atomic.AtomicInteger

object CodeGenUtils {

  // ------------------------------- DEFAULT TERMS ------------------------------------------

  val DEFAULT_TIMEZONE_TERM = "timeZone"

  val DEFAULT_INPUT1_TERM = "in1"

  val DEFAULT_INPUT2_TERM = "in2"

  val DEFAULT_COLLECTOR_TERM = "c"

  val DEFAULT_OUT_RECORD_TERM = "out"

  val DEFAULT_OPERATOR_COLLECTOR_TERM = "output"

  val DEFAULT_OUT_RECORD_WRITER_TERM = "outWriter"

  val DEFAULT_CONTEXT_TERM = "ctx"

  // -------------------------- CANONICAL CLASS NAMES ---------------------------------------

  val BINARY_ROW: String = className[BinaryRow]

  val BINARY_ARRAY: String = className[BinaryArray]

  val BINARY_GENERIC: String = className[BinaryGeneric[_]]

  val BINARY_STRING: String = className[BinaryString]

  val BINARY_MAP: String = className[BinaryMap]

  val BASE_ROW: String = className[BaseRow]

  val JOINED_ROW: String = className[JoinedRow]

  val GENERIC_ROW: String = className[GenericRow]

  val DECIMAL: String = className[Decimal]

  val SEGMENT: String = className[MemorySegment]

  val AGGS_HANDLER_FUNCTION: String = className[AggsHandleFunction]

  val NAMESPACE_AGGS_HANDLER_FUNCTION: String = className[NamespaceAggsHandleFunction[_]]

  val STATE_DATA_VIEW_STORE: String = className[StateDataViewStore]

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

    case InternalTypes.DATE => "int"
    case InternalTypes.TIME => "int"
    case _: TimestampType => "long"

    case InternalTypes.INTERVAL_MONTHS => "int"
    case InternalTypes.INTERVAL_MILLIS => "long"

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

    case InternalTypes.DATE => boxedTypeTermForType(InternalTypes.INT)
    case InternalTypes.TIME => boxedTypeTermForType(InternalTypes.INT)
    case _: TimestampType => boxedTypeTermForType(InternalTypes.LONG)

    case InternalTypes.STRING => BINARY_STRING
    case InternalTypes.BINARY => "byte[]"

    case _: DecimalType => className[Decimal]
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

    case _: DateType | InternalTypes.TIME => "-1"
    case _: TimestampType => "-1L"

    case _ => "null"
  }

  /**
    * If it's internally compatible, don't need to DataStructure converter.
    * clazz != classOf[Row] => Row can only infer GenericType[Row].
    */
  def isInternalClass(clazz: Class[_], t: TypeInformation[_]): Boolean =
    clazz != classOf[Object] && clazz != classOf[Row] &&
      (classOf[BaseRow].isAssignableFrom(clazz) ||
          clazz == getInternalClassForType(createInternalTypeFromTypeInfo(t)))

  def hashCodeForType(ctx: CodeGeneratorContext, t: InternalType, term: String): String = t match {
    case InternalTypes.BYTE => s"${className[JByte]}.hashCode($term)"
    case InternalTypes.SHORT => s"${className[JShort]}.hashCode($term)"
    case InternalTypes.INT => s"${className[JInt]}.hashCode($term)"
    case InternalTypes.LONG => s"${className[JLong]}.hashCode($term)"
    case InternalTypes.FLOAT => s"${className[JFloat]}.hashCode($term)"
    case InternalTypes.DOUBLE => s"${className[JDouble]}.hashCode($term)"
    case InternalTypes.STRING => s"$term.hashCode()"
    case InternalTypes.BINARY => s"${className[MurmurHashUtil]}.hashUnsafeBytes(" +
      s"$term, $BYTE_ARRAY_BASE_OFFSET, $term.length)"
    case _: DecimalType => s"$term.hashCode()"
    case _: DateType => s"${className[JInt]}.hashCode($term)"
    case InternalTypes.TIME => s"${className[JInt]}.hashCode($term)"
    case _: TimestampType => s"${className[JLong]}.hashCode($term)"
    case _: ArrayType => throw new IllegalArgumentException(s"Not support type to hash: $t")
    case rowType: RowType =>
      val subCtx = CodeGeneratorContext(ctx.tableConfig)
      val genHash = HashCodeGenerator.generateRowHash(
        subCtx, rowType, "SubHashRow", (0 until rowType.getArity).toArray)
      ctx.addReusableInnerClass(genHash.getClassName, genHash.getCode)
      val refs = ctx.addReusableObject(subCtx.references.toArray, "subRefs")
      val hashFunc = newName("hashFunc")
      ctx.addReusableMember(s"${classOf[HashFunction].getCanonicalName} $hashFunc;")
      ctx.addReusableInitStatement(s"$hashFunc = new ${genHash.getClassName}($refs);")
      s"$hashFunc.hashCode($term)"
    case gt: GenericType[_] =>
      val serTerm = ctx.addReusableObject(gt.getSerializer, "serializer")
      s"$BINARY_GENERIC.getJavaObjectFromBinaryGeneric($term, $serTerm).hashCode()"
  }

  // ----------------------------------------------------------------------------------------------

  // Cast numeric type to another numeric type with larger range.
  // This function must be in sync with [[NumericOrDefaultReturnTypeInference]].
  def getNumericCastedResultTerm(expr: GeneratedExpression, targetType: InternalType): String = {
    (expr.resultType, targetType) match {
      case _ if expr.resultType == targetType => expr.resultTerm

      // byte -> other numeric types
      case (_: ByteType, _: ShortType) => s"(short) ${expr.resultTerm}"
      case (_: ByteType, _: IntType) => s"(int) ${expr.resultTerm}"
      case (_: ByteType, _: LongType) => s"(long) ${expr.resultTerm}"
      case (_: ByteType, dt: DecimalType) =>
        s"${classOf[Decimal].getCanonicalName}.castFrom(" +
          s"${expr.resultTerm}, ${dt.precision}, ${dt.scale})"
      case (_: ByteType, _: FloatType) => s"(float) ${expr.resultTerm}"
      case (_: ByteType, _: DoubleType) => s"(double) ${expr.resultTerm}"

      // short -> other numeric types
      case (_: ShortType, _: IntType) => s"(int) ${expr.resultTerm}"
      case (_: ShortType, _: LongType) => s"(long) ${expr.resultTerm}"
      case (_: ShortType, dt: DecimalType) =>
        s"${classOf[Decimal].getCanonicalName}.castFrom(" +
          s"${expr.resultTerm}, ${dt.precision}, ${dt.scale})"
      case (_: ShortType, _: FloatType) => s"(float) ${expr.resultTerm}"
      case (_: ShortType, _: DoubleType) => s"(double) ${expr.resultTerm}"

      // int -> other numeric types
      case (_: IntType, _: LongType) => s"(long) ${expr.resultTerm}"
      case (_: IntType, dt: DecimalType) =>
        s"${classOf[Decimal].getCanonicalName}.castFrom(" +
          s"${expr.resultTerm}, ${dt.precision}, ${dt.scale})"
      case (_: IntType, _: FloatType) => s"(float) ${expr.resultTerm}"
      case (_: IntType, _: DoubleType) => s"(double) ${expr.resultTerm}"

      // long -> other numeric types
      case (_: LongType, dt: DecimalType) =>
        s"${classOf[Decimal].getCanonicalName}.castFrom(" +
          s"${expr.resultTerm}, ${dt.precision}, ${dt.scale})"
      case (_: LongType, _: FloatType) => s"(float) ${expr.resultTerm}"
      case (_: LongType, _: DoubleType) => s"(double) ${expr.resultTerm}"

      // decimal -> other numeric types
      case (_: DecimalType, dt: DecimalType) =>
        s"${classOf[Decimal].getCanonicalName}.castToDecimal(" +
          s"${expr.resultTerm}, ${dt.precision}, ${dt.scale})"
      case (_: DecimalType, _: FloatType) =>
        s"${classOf[Decimal].getCanonicalName}.castToFloat(${expr.resultTerm})"
      case (_: DecimalType, _: DoubleType) =>
        s"${classOf[Decimal].getCanonicalName}.castToDouble(${expr.resultTerm})"

      // float -> other numeric types
      case (_: FloatType, _: DoubleType) => s"(double) ${expr.resultTerm}"

      case _ => null
    }
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

  def requireTemporal(genExpr: GeneratedExpression): Unit =
    if (!TypeCheckUtils.isTemporal(genExpr.resultType)) {
      throw new CodeGenException("Temporal expression type expected.")
    }

  def requireTimeInterval(genExpr: GeneratedExpression): Unit =
    if (!TypeCheckUtils.isTimeInterval(genExpr.resultType)) {
      throw new CodeGenException("Interval expression type expected.")
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

  // --------------------------------------------------------------------------------
  // DataFormat Operations
  // --------------------------------------------------------------------------------

  // -------------------------- BaseRow Read Access -------------------------------

  def baseRowFieldReadAccess(
      ctx: CodeGeneratorContext,
      index: Int,
      rowTerm: String,
      fieldType: InternalType) : String =
    baseRowFieldReadAccess(ctx, index.toString, rowTerm, fieldType)

  def baseRowFieldReadAccess(
      ctx: CodeGeneratorContext,
      indexTerm: String,
      rowTerm: String,
      fieldType: InternalType) : String =
    fieldType match {
      // primitive types
      case InternalTypes.BOOLEAN => s"$rowTerm.getBoolean($indexTerm)"
      case InternalTypes.BYTE => s"$rowTerm.getByte($indexTerm)"
      case InternalTypes.SHORT => s"$rowTerm.getShort($indexTerm)"
      case InternalTypes.INT => s"$rowTerm.getInt($indexTerm)"
      case InternalTypes.LONG => s"$rowTerm.getLong($indexTerm)"
      case InternalTypes.FLOAT => s"$rowTerm.getFloat($indexTerm)"
      case InternalTypes.DOUBLE => s"$rowTerm.getDouble($indexTerm)"
      case InternalTypes.STRING => s"$rowTerm.getString($indexTerm)"
      case InternalTypes.BINARY => s"$rowTerm.getBinary($indexTerm)"
      case dt: DecimalType => s"$rowTerm.getDecimal($indexTerm, ${dt.precision()}, ${dt.scale()})"

      // temporal types
      case _: DateType => s"$rowTerm.getInt($indexTerm)"
      case InternalTypes.TIME => s"$rowTerm.getInt($indexTerm)"
      case _: TimestampType => s"$rowTerm.getLong($indexTerm)"

      // complex types
      case _: ArrayType => s"$rowTerm.getArray($indexTerm)"
      case _: MapType  => s"$rowTerm.getMap($indexTerm)"
      case rt: RowType => s"$rowTerm.getRow($indexTerm, ${rt.getArity})"

      case _: GenericType[_] => s"$rowTerm.getGeneric($indexTerm)"
    }

  // -------------------------- BaseRow Set Field -------------------------------

  def baseRowSetField(
    ctx: CodeGeneratorContext,
    rowClass: Class[_ <: BaseRow],
    rowTerm: String,
    indexTerm: String,
    fieldExpr: GeneratedExpression,
    binaryRowWriterTerm: Option[String]): String = {

    val fieldType = fieldExpr.resultType
    val fieldTerm = fieldExpr.resultTerm

    if (rowClass == classOf[BinaryRow]) {
      binaryRowWriterTerm match {
        case Some(writer) =>
          // use writer to set field
          val writeField = binaryWriterWriteField(ctx, indexTerm, fieldTerm, writer, fieldType)
          if (ctx.nullCheck) {
            s"""
               |${fieldExpr.code}
               |if (${fieldExpr.nullTerm}) {
               |  ${binaryWriterWriteNull(indexTerm, writer, fieldType)};
               |} else {
               |  $writeField;
               |}
             """.stripMargin
          } else {
            s"""
               |${fieldExpr.code}
               |$writeField;
             """.stripMargin
          }

        case None =>
          // directly set field to BinaryRow, this depends on all the fields are fixed length
          val writeField = binaryRowFieldSetAccess(indexTerm, rowTerm, fieldType, fieldTerm)
          if (ctx.nullCheck) {
            s"""
               |${fieldExpr.code}
               |if (${fieldExpr.nullTerm}) {
               |  ${binaryRowSetNull(indexTerm, rowTerm, fieldType)};
               |} else {
               |  $writeField;
               |}
             """.stripMargin
          } else {
            s"""
               |${fieldExpr.code}
               |$writeField;
             """.stripMargin
          }
      }
    } else if (rowClass == classOf[GenericRow] || rowClass == classOf[BoxedWrapperRow]) {
      val writeField = if (rowClass == classOf[GenericRow]) {
        s"$rowTerm.setField($indexTerm, $fieldTerm);"
      } else {
        boxedWrapperRowFieldSetAccess(rowTerm, indexTerm, fieldTerm, fieldType)
      }
      if (ctx.nullCheck) {
        s"""
           |${fieldExpr.code}
           |if (${fieldExpr.nullTerm}) {
           |  $rowTerm.setNullAt($indexTerm);
           |} else {
           |  $writeField;
           |}
          """.stripMargin
      } else {
        s"""
           |${fieldExpr.code}
           |$writeField;
         """.stripMargin
      }
    } else {
      throw new UnsupportedOperationException("Not support set field for " + rowClass)
    }
  }

  // -------------------------- BinaryRow Set Field -------------------------------

  def binaryRowSetNull(index: Int, rowTerm: String, t: InternalType): String =
    binaryRowSetNull(index.toString, rowTerm, t)

  def binaryRowSetNull(indexTerm: String, rowTerm: String, t: InternalType): String = t match {
    case d: DecimalType if !Decimal.isCompact(d.precision()) =>
      s"$rowTerm.setDecimal($indexTerm, null, ${d.precision()})"
    case _ => s"$rowTerm.setNullAt($indexTerm)"
  }

  def binaryRowFieldSetAccess(
      index: Int,
      binaryRowTerm: String,
      fieldType: InternalType,
      fieldValTerm: String): String =
    binaryRowFieldSetAccess(index.toString, binaryRowTerm, fieldType, fieldValTerm)

  def binaryRowFieldSetAccess(
      index: String,
      binaryRowTerm: String,
      fieldType: InternalType,
      fieldValTerm: String): String =
    fieldType match {
      case InternalTypes.INT => s"$binaryRowTerm.setInt($index, $fieldValTerm)"
      case InternalTypes.LONG => s"$binaryRowTerm.setLong($index, $fieldValTerm)"
      case InternalTypes.SHORT => s"$binaryRowTerm.setShort($index, $fieldValTerm)"
      case InternalTypes.BYTE => s"$binaryRowTerm.setByte($index, $fieldValTerm)"
      case InternalTypes.FLOAT => s"$binaryRowTerm.setFloat($index, $fieldValTerm)"
      case InternalTypes.DOUBLE => s"$binaryRowTerm.setDouble($index, $fieldValTerm)"
      case InternalTypes.BOOLEAN => s"$binaryRowTerm.setBoolean($index, $fieldValTerm)"
      case _: DateType =>  s"$binaryRowTerm.setInt($index, $fieldValTerm)"
      case InternalTypes.TIME =>  s"$binaryRowTerm.setInt($index, $fieldValTerm)"
      case _: TimestampType =>  s"$binaryRowTerm.setLong($index, $fieldValTerm)"
      case d: DecimalType =>
        s"$binaryRowTerm.setDecimal($index, $fieldValTerm, ${d.precision()})"
      case _ =>
        throw new CodeGenException("Fail to find binary row field setter method of InternalType "
          + fieldType + ".")
    }

  // -------------------------- BoxedWrapperRow Set Field -------------------------------

  def boxedWrapperRowFieldSetAccess(
      rowTerm: String,
      indexTerm: String,
      fieldTerm: String,
      fieldType: InternalType): String =
    fieldType match {
      case InternalTypes.INT => s"$rowTerm.setInt($indexTerm, $fieldTerm)"
      case InternalTypes.LONG => s"$rowTerm.setLong($indexTerm, $fieldTerm)"
      case InternalTypes.SHORT => s"$rowTerm.setShort($indexTerm, $fieldTerm)"
      case InternalTypes.BYTE => s"$rowTerm.setByte($indexTerm, $fieldTerm)"
      case InternalTypes.FLOAT => s"$rowTerm.setFloat($indexTerm, $fieldTerm)"
      case InternalTypes.DOUBLE => s"$rowTerm.setDouble($indexTerm, $fieldTerm)"
      case InternalTypes.BOOLEAN => s"$rowTerm.setBoolean($indexTerm, $fieldTerm)"
      case _: DateType =>  s"$rowTerm.setInt($indexTerm, $fieldTerm)"
      case InternalTypes.TIME =>  s"$rowTerm.setInt($indexTerm, $fieldTerm)"
      case _: TimestampType =>  s"$rowTerm.setLong($indexTerm, $fieldTerm)"
      case _ => s"$rowTerm.setNonPrimitiveValue($indexTerm, $fieldTerm)"
    }

  // -------------------------- BinaryArray Set Access -------------------------------

  def binaryArraySetNull(
      index: Int,
      arrayTerm: String,
      elementType: InternalType): String = elementType match {
    case InternalTypes.BOOLEAN => s"$arrayTerm.setNullBoolean($index)"
    case InternalTypes.BYTE => s"$arrayTerm.setNullByte($index)"
    case InternalTypes.SHORT => s"$arrayTerm.setNullShort($index)"
    case InternalTypes.INT => s"$arrayTerm.setNullInt($index)"
    case InternalTypes.LONG => s"$arrayTerm.setNullLong($index)"
    case InternalTypes.FLOAT => s"$arrayTerm.setNullFloat($index)"
    case InternalTypes.DOUBLE => s"$arrayTerm.setNullDouble($index)"
    case InternalTypes.TIME => s"$arrayTerm.setNullInt($index)"
    case _: DateType => s"$arrayTerm.setNullInt($index)"
    case _: TimestampType => s"$arrayTerm.setNullLong($index)"
    case _ => s"$arrayTerm.setNullLong($index)"
  }

  // -------------------------- BinaryWriter Write -------------------------------

  def binaryWriterWriteNull(index: Int, writerTerm: String, t: InternalType): String =
    binaryWriterWriteNull(index.toString, writerTerm, t)

  def binaryWriterWriteNull(
      indexTerm: String,
      writerTerm: String,
      t: InternalType): String = t match {
    case d: DecimalType if !Decimal.isCompact(d.precision()) =>
      s"$writerTerm.writeDecimal($indexTerm, null, ${d.precision()})"
    case _ => s"$writerTerm.setNullAt($indexTerm)"
  }

  def binaryWriterWriteField(
      ctx: CodeGeneratorContext,
      index: Int,
      fieldValTerm: String,
      writerTerm: String,
      fieldType: InternalType): String =
    binaryWriterWriteField(ctx, index.toString, fieldValTerm, writerTerm, fieldType)

  def binaryWriterWriteField(
      ctx: CodeGeneratorContext,
      indexTerm: String,
      fieldValTerm: String,
      writerTerm: String,
      fieldType: InternalType): String =
    fieldType match {
      case InternalTypes.INT => s"$writerTerm.writeInt($indexTerm, $fieldValTerm)"
      case InternalTypes.LONG => s"$writerTerm.writeLong($indexTerm, $fieldValTerm)"
      case InternalTypes.SHORT => s"$writerTerm.writeShort($indexTerm, $fieldValTerm)"
      case InternalTypes.BYTE => s"$writerTerm.writeByte($indexTerm, $fieldValTerm)"
      case InternalTypes.FLOAT => s"$writerTerm.writeFloat($indexTerm, $fieldValTerm)"
      case InternalTypes.DOUBLE => s"$writerTerm.writeDouble($indexTerm, $fieldValTerm)"
      case InternalTypes.BOOLEAN => s"$writerTerm.writeBoolean($indexTerm, $fieldValTerm)"
      case InternalTypes.BINARY => s"$writerTerm.writeBinary($indexTerm, $fieldValTerm)"
      case InternalTypes.STRING => s"$writerTerm.writeString($indexTerm, $fieldValTerm)"
      case d: DecimalType =>
        s"$writerTerm.writeDecimal($indexTerm, $fieldValTerm, ${d.precision()})"
      case _: DateType => s"$writerTerm.writeInt($indexTerm, $fieldValTerm)"
      case InternalTypes.TIME => s"$writerTerm.writeInt($indexTerm, $fieldValTerm)"
      case _: TimestampType => s"$writerTerm.writeLong($indexTerm, $fieldValTerm)"

      // complex types
      case _: ArrayType => s"$writerTerm.writeArray($indexTerm, $fieldValTerm)"
      case _: MapType => s"$writerTerm.writeMap($indexTerm, $fieldValTerm)"
      case _: RowType =>
        val serializerTerm = ctx.addReusableTypeSerializer(fieldType)
        s"$writerTerm.writeRow($indexTerm, $fieldValTerm, $serializerTerm)"

      case _: GenericType[_] => s"$writerTerm.writeGeneric($indexTerm, $fieldValTerm)"
    }

  private def isConverterIdentity(t: TypeInformation[_]): Boolean = {
    DataFormatConverters.getConverterForTypeInfo(t).isInstanceOf[IdentityConverter[_]]
  }

  def genToInternal(ctx: CodeGeneratorContext, t: TypeInformation[_], term: String): String =
    genToInternal(ctx, t)(term)

  def genToInternal(ctx: CodeGeneratorContext, t: TypeInformation[_]): String => String = {
    val iTerm = boxedTypeTermForType(createInternalTypeFromTypeInfo(t))
    if (isConverterIdentity(t)) {
      term => s"($iTerm) $term"
    } else {
      val eTerm = boxedTypeTermForExternalType(t)
      val converter = ctx.addReusableObject(
        DataFormatConverters.getConverterForTypeInfo(t),
        "converter")
      term => s"($iTerm) $converter.toInternal(($eTerm) $term)"
    }
  }

  def genToInternalIfNeeded(
      ctx: CodeGeneratorContext,
      t: TypeInformation[_],
      clazz: Class[_],
      term: String): String = {
    if (isInternalClass(clazz, t)) {
      s"(${boxedTypeTermForType(createInternalTypeFromTypeInfo(t))}) $term"
    } else {
      genToInternal(ctx, t, term)
    }
  }

  def genToExternal(ctx: CodeGeneratorContext, t: TypeInformation[_], term: String): String = {
    val iTerm = boxedTypeTermForType(createInternalTypeFromTypeInfo(t))
    if (isConverterIdentity(t)) {
      s"($iTerm) $term"
    } else {
      val eTerm = boxedTypeTermForExternalType(t)
      val converter = ctx.addReusableObject(
        DataFormatConverters.getConverterForTypeInfo(t),
        "converter")
      s"($eTerm) $converter.toExternal(($iTerm) $term)"
    }
  }

  def genToExternalIfNeeded(
      ctx: CodeGeneratorContext,
      t: TypeInformation[_],
      clazz: Class[_],
      term: String): String = {
    if (isInternalClass(clazz, t)) {
      s"(${boxedTypeTermForType(createInternalTypeFromTypeInfo(t))}) $term"
    } else {
      genToExternal(ctx, t, term)
    }
  }

  def udfFieldName(udf: UserDefinedFunction): String = s"function_${udf.functionIdentifier}"

  def genLogInfo(logTerm: String, format: String, argTerm: String): String =
    s"""$logTerm.info("$format", $argTerm);"""
}
