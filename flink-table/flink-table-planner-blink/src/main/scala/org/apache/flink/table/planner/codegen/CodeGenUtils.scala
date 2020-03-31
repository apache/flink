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

package org.apache.flink.table.planner.codegen

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.core.memory.MemorySegment
import org.apache.flink.table.dataformat.DataFormatConverters.IdentityConverter
import org.apache.flink.table.dataformat.util.BinaryRowUtil.BYTE_ARRAY_BASE_OFFSET
import org.apache.flink.table.dataformat.{BinaryStringUtil, Decimal, _}
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.runtime.dataview.StateDataViewStore
import org.apache.flink.table.runtime.generated.{AggsHandleFunction, HashFunction, NamespaceAggsHandleFunction, TableAggsHandleFunction}
import org.apache.flink.table.runtime.types.ClassLogicalTypeConverter.getInternalClassForType
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType
import org.apache.flink.table.runtime.types.PlannerTypeUtils.isInteroperable
import org.apache.flink.table.runtime.typeutils.TypeCheckUtils
import org.apache.flink.table.runtime.util.MurmurHashUtil
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.table.types.logical._
import org.apache.flink.types.Row
import java.lang.reflect.Method
import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Integer => JInt, Long => JLong, Short => JShort}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.flink.table.planner.codegen.GenerateUtils.{generateInputFieldUnboxing, generateNonNullField}

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

  val BASE_ARRAY: String = className[BaseArray]

  val BINARY_GENERIC: String = className[BinaryGeneric[_]]

  val BINARY_STRING: String = className[BinaryString]

  val BINARY_MAP: String = className[BinaryMap]

  val BASE_MAP: String = className[BaseMap]

  val BASE_ROW: String = className[BaseRow]

  val JOINED_ROW: String = className[JoinedRow]

  val GENERIC_ROW: String = className[GenericRow]

  val DECIMAL_TERM: String = className[Decimal]

  val SEGMENT: String = className[MemorySegment]

  val AGGS_HANDLER_FUNCTION: String = className[AggsHandleFunction]

  val TABLE_AGGS_HANDLER_FUNCTION: String = className[TableAggsHandleFunction]

  val NAMESPACE_AGGS_HANDLER_FUNCTION: String = className[NamespaceAggsHandleFunction[_]]

  val STATE_DATA_VIEW_STORE: String = className[StateDataViewStore]

  val STRING_UTIL: String = className[BinaryStringUtil]

  val SQL_TIMESTAMP: String = className[SqlTimestamp]

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
  def className[T](implicit m: Manifest[T]): String = {
    val name = m.runtimeClass.getCanonicalName
    if (name == null) {
      throw new CodeGenException(
        s"Class '${m.runtimeClass.getName}' does not have a canonical name. " +
          s"Make sure it is statically accessible.")
    }
    name
  }

  /**
   * Returns a term for representing the given class in Java code.
   */
  def typeTerm(clazz: Class[_]): String = {
    val name = clazz.getCanonicalName
    if (name == null) {
      throw new CodeGenException(
        s"Class '${clazz.getName}' does not have a canonical name. " +
          s"Make sure it is statically accessible.")
    }
    name
  }

  // when casting we first need to unbox Primitives, for example,
  // float a = 1.0f;
  // byte b = (byte) a;
  // works, but for boxed types we need this:
  // Float a = 1.0f;
  // Byte b = (byte)(float) a;
  def primitiveTypeTermForType(t: LogicalType): String = t.getTypeRoot match {
    case INTEGER => "int"
    case BIGINT => "long"
    case SMALLINT => "short"
    case TINYINT => "byte"
    case FLOAT => "float"
    case DOUBLE => "double"
    case BOOLEAN => "boolean"

    case DATE => "int"
    case TIME_WITHOUT_TIME_ZONE => "int"
    case INTERVAL_YEAR_MONTH => "int"
    case INTERVAL_DAY_TIME => "long"

    case _ => boxedTypeTermForType(t)
  }

  def boxedTypeTermForType(t: LogicalType): String = t.getTypeRoot match {
    case INTEGER => className[JInt]
    case BIGINT => className[JLong]
    case SMALLINT => className[JShort]
    case TINYINT => className[JByte]
    case FLOAT => className[JFloat]
    case DOUBLE => className[JDouble]
    case BOOLEAN => className[JBoolean]

    case DATE => className[JInt]
    case TIME_WITHOUT_TIME_ZONE => className[JInt]
    case INTERVAL_YEAR_MONTH => className[JInt]
    case INTERVAL_DAY_TIME => className[JLong]

    case VARCHAR | CHAR => BINARY_STRING
    case VARBINARY | BINARY => "byte[]"

    case DECIMAL => className[Decimal]
    case ARRAY => className[BaseArray]
    case MULTISET | MAP => className[BaseMap]
    case ROW => className[BaseRow]
    case TIMESTAMP_WITHOUT_TIME_ZONE | TIMESTAMP_WITH_LOCAL_TIME_ZONE => className[SqlTimestamp]

    case RAW => className[BinaryGeneric[_]]
  }

  /**
    * Gets the default value for a primitive type, and null for generic types
    */
  def primitiveDefaultValue(t: LogicalType): String = t.getTypeRoot match {
    case INTEGER | TINYINT | SMALLINT => "-1"
    case BIGINT => "-1L"
    case FLOAT => "-1.0f"
    case DOUBLE => "-1.0d"
    case BOOLEAN => "false"
    case VARCHAR | CHAR => s"$BINARY_STRING.EMPTY_UTF8"

    case DATE | TIME_WITHOUT_TIME_ZONE => "-1"
    case INTERVAL_YEAR_MONTH => "-1"
    case INTERVAL_DAY_TIME => "-1L"

    case _ => "null"
  }

  /**
    * If it's internally compatible, don't need to DataStructure converter.
    * clazz != classOf[Row] => Row can only infer GenericType[Row].
    */
  def isInternalClass(t: DataType): Boolean = {
    val clazz = t.getConversionClass
    clazz != classOf[Object] && clazz != classOf[Row] &&
        (classOf[BaseRow].isAssignableFrom(clazz) ||
            clazz == getInternalClassForType(fromDataTypeToLogicalType(t)))
  }

  def hashCodeForType(
      ctx: CodeGeneratorContext, t: LogicalType, term: String): String = t.getTypeRoot match {
    case BOOLEAN => s"${className[JBoolean]}.hashCode($term)"
    case TINYINT => s"${className[JByte]}.hashCode($term)"
    case SMALLINT => s"${className[JShort]}.hashCode($term)"
    case INTEGER => s"${className[JInt]}.hashCode($term)"
    case BIGINT => s"${className[JLong]}.hashCode($term)"
    case FLOAT => s"${className[JFloat]}.hashCode($term)"
    case DOUBLE => s"${className[JDouble]}.hashCode($term)"
    case VARCHAR | CHAR => s"$term.hashCode()"
    case VARBINARY | BINARY => s"${className[MurmurHashUtil]}.hashUnsafeBytes(" +
      s"$term, $BYTE_ARRAY_BASE_OFFSET, $term.length)"
    case DECIMAL => s"$term.hashCode()"
    case DATE => s"${className[JInt]}.hashCode($term)"
    case TIME_WITHOUT_TIME_ZONE => s"${className[JInt]}.hashCode($term)"
    case TIMESTAMP_WITHOUT_TIME_ZONE | TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
      s"$term.hashCode()"
    case INTERVAL_YEAR_MONTH => s"${className[JInt]}.hashCode($term)"
    case INTERVAL_DAY_TIME => s"${className[JLong]}.hashCode($term)"
    case ARRAY => throw new IllegalArgumentException(s"Not support type to hash: $t")
    case ROW =>
      val rowType = t.asInstanceOf[RowType]
      val subCtx = CodeGeneratorContext(ctx.tableConfig)
      val genHash = HashCodeGenerator.generateRowHash(
        subCtx, rowType, "SubHashRow", (0 until rowType.getFieldCount).toArray)
      ctx.addReusableInnerClass(genHash.getClassName, genHash.getCode)
      val refs = ctx.addReusableObject(subCtx.references.toArray, "subRefs")
      val hashFunc = newName("hashFunc")
      ctx.addReusableMember(s"${classOf[HashFunction].getCanonicalName} $hashFunc;")
      ctx.addReusableInitStatement(s"$hashFunc = new ${genHash.getClassName}($refs);")
      s"$hashFunc.hashCode($term)"
    case RAW =>
      val gt = t.asInstanceOf[TypeInformationRawType[_]]
      val serTerm = ctx.addReusableObject(
        gt.getTypeInformation.createSerializer(new ExecutionConfig), "serializer")
      s"$BINARY_GENERIC.getJavaObjectFromBinaryGeneric($term, $serTerm).hashCode()"
  }

  // ----------------------------------------------------------------------------------------------

  // Cast numeric type to another numeric type with larger range.
  // This function must be in sync with [[NumericOrDefaultReturnTypeInference]].
  def getNumericCastedResultTerm(expr: GeneratedExpression, targetType: LogicalType): String = {
    (expr.resultType.getTypeRoot, targetType.getTypeRoot) match {
      case _ if isInteroperable(expr.resultType, targetType) => expr.resultTerm

      // byte -> other numeric types
      case (TINYINT, SMALLINT) => s"(short) ${expr.resultTerm}"
      case (TINYINT, INTEGER) => s"(int) ${expr.resultTerm}"
      case (TINYINT, BIGINT) => s"(long) ${expr.resultTerm}"
      case (TINYINT, DECIMAL) =>
        val dt = targetType.asInstanceOf[DecimalType]
        s"${classOf[Decimal].getCanonicalName}.castFrom(" +
          s"${expr.resultTerm}, ${dt.getPrecision}, ${dt.getScale})"
      case (TINYINT, FLOAT) => s"(float) ${expr.resultTerm}"
      case (TINYINT, DOUBLE) => s"(double) ${expr.resultTerm}"

      // short -> other numeric types
      case (SMALLINT, INTEGER) => s"(int) ${expr.resultTerm}"
      case (SMALLINT, BIGINT) => s"(long) ${expr.resultTerm}"
      case (SMALLINT, DECIMAL) =>
        val dt = targetType.asInstanceOf[DecimalType]
        s"${classOf[Decimal].getCanonicalName}.castFrom(" +
          s"${expr.resultTerm}, ${dt.getPrecision}, ${dt.getScale})"
      case (SMALLINT, FLOAT) => s"(float) ${expr.resultTerm}"
      case (SMALLINT, DOUBLE) => s"(double) ${expr.resultTerm}"

      // int -> other numeric types
      case (INTEGER, BIGINT) => s"(long) ${expr.resultTerm}"
      case (INTEGER, DECIMAL) =>
        val dt = targetType.asInstanceOf[DecimalType]
        s"${classOf[Decimal].getCanonicalName}.castFrom(" +
          s"${expr.resultTerm}, ${dt.getPrecision}, ${dt.getScale})"
      case (INTEGER, FLOAT) => s"(float) ${expr.resultTerm}"
      case (INTEGER, DOUBLE) => s"(double) ${expr.resultTerm}"

      // long -> other numeric types
      case (BIGINT, DECIMAL) =>
        val dt = targetType.asInstanceOf[DecimalType]
        s"${classOf[Decimal].getCanonicalName}.castFrom(" +
          s"${expr.resultTerm}, ${dt.getPrecision}, ${dt.getScale})"
      case (BIGINT, FLOAT) => s"(float) ${expr.resultTerm}"
      case (BIGINT, DOUBLE) => s"(double) ${expr.resultTerm}"

      // decimal -> other numeric types
      case (DECIMAL, DECIMAL) =>
        val dt = targetType.asInstanceOf[DecimalType]
        s"${classOf[Decimal].getCanonicalName}.castToDecimal(" +
          s"${expr.resultTerm}, ${dt.getPrecision}, ${dt.getScale})"
      case (DECIMAL, FLOAT) =>
        s"${classOf[Decimal].getCanonicalName}.castToFloat(${expr.resultTerm})"
      case (DECIMAL, DOUBLE) =>
        s"${classOf[Decimal].getCanonicalName}.castToDouble(${expr.resultTerm})"

      // float -> other numeric types
      case (FLOAT, DOUBLE) => s"(double) ${expr.resultTerm}"

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
    val clazz = genExpr.resultType.asInstanceOf[TypeInformationRawType[_]]
        .getTypeInformation.getTypeClass
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

  def requireCharacterString(genExpr: GeneratedExpression): Unit =
    if (!TypeCheckUtils.isCharacterString(genExpr.resultType)) {
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
      fieldType: LogicalType) : String =
    baseRowFieldReadAccess(ctx, index.toString, rowTerm, fieldType)

  def baseRowFieldReadAccess(
      ctx: CodeGeneratorContext,
      indexTerm: String,
      rowTerm: String,
      t: LogicalType) : String =
    t.getTypeRoot match {
      // primitive types
      case BOOLEAN => s"$rowTerm.getBoolean($indexTerm)"
      case TINYINT => s"$rowTerm.getByte($indexTerm)"
      case SMALLINT => s"$rowTerm.getShort($indexTerm)"
      case INTEGER => s"$rowTerm.getInt($indexTerm)"
      case BIGINT => s"$rowTerm.getLong($indexTerm)"
      case FLOAT => s"$rowTerm.getFloat($indexTerm)"
      case DOUBLE => s"$rowTerm.getDouble($indexTerm)"
      case VARCHAR | CHAR => s"$rowTerm.getString($indexTerm)"
      case VARBINARY | BINARY => s"$rowTerm.getBinary($indexTerm)"
      case DECIMAL =>
        val dt = t.asInstanceOf[DecimalType]
        s"$rowTerm.getDecimal($indexTerm, ${dt.getPrecision}, ${dt.getScale})"

      // temporal types
      case DATE => s"$rowTerm.getInt($indexTerm)"
      case TIME_WITHOUT_TIME_ZONE => s"$rowTerm.getInt($indexTerm)"
      case TIMESTAMP_WITHOUT_TIME_ZONE =>
        val dt = t.asInstanceOf[TimestampType]
        s"$rowTerm.getTimestamp($indexTerm, ${dt.getPrecision})"
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
        val dt = t.asInstanceOf[LocalZonedTimestampType]
        s"$rowTerm.getTimestamp($indexTerm, ${dt.getPrecision})"
      case INTERVAL_YEAR_MONTH => s"$rowTerm.getInt($indexTerm)"
      case INTERVAL_DAY_TIME => s"$rowTerm.getLong($indexTerm)"

      // complex types
      case ARRAY => s"$rowTerm.getArray($indexTerm)"
      case MULTISET | MAP  => s"$rowTerm.getMap($indexTerm)"
      case ROW => s"$rowTerm.getRow($indexTerm, ${t.asInstanceOf[RowType].getFieldCount})"

      case RAW => s"$rowTerm.getGeneric($indexTerm)"
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
        s"$rowTerm.setField($indexTerm, $fieldTerm)"
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

  def binaryRowSetNull(index: Int, rowTerm: String, t: LogicalType): String =
    binaryRowSetNull(index.toString, rowTerm, t)

  def binaryRowSetNull(indexTerm: String, rowTerm: String, t: LogicalType): String = t match {
    case d: DecimalType if !Decimal.isCompact(d.getPrecision) =>
      s"$rowTerm.setDecimal($indexTerm, null, ${d.getPrecision})"
    case d: TimestampType if !SqlTimestamp.isCompact(d.getPrecision) =>
      s"$rowTerm.setTimestamp($indexTerm, null, ${d.getPrecision})"
    case d: LocalZonedTimestampType if !SqlTimestamp.isCompact(d.getPrecision) =>
      s"$rowTerm.setTimestamp($indexTerm, null, ${d.getPrecision})"
    case _ => s"$rowTerm.setNullAt($indexTerm)"
  }

  def binaryRowFieldSetAccess(
      index: Int,
      binaryRowTerm: String,
      fieldType: LogicalType,
      fieldValTerm: String): String =
    binaryRowFieldSetAccess(index.toString, binaryRowTerm, fieldType, fieldValTerm)

  def binaryRowFieldSetAccess(
      index: String,
      binaryRowTerm: String,
      t: LogicalType,
      fieldValTerm: String): String =
    t.getTypeRoot match {
      case INTEGER => s"$binaryRowTerm.setInt($index, $fieldValTerm)"
      case BIGINT => s"$binaryRowTerm.setLong($index, $fieldValTerm)"
      case SMALLINT => s"$binaryRowTerm.setShort($index, $fieldValTerm)"
      case TINYINT => s"$binaryRowTerm.setByte($index, $fieldValTerm)"
      case FLOAT => s"$binaryRowTerm.setFloat($index, $fieldValTerm)"
      case DOUBLE => s"$binaryRowTerm.setDouble($index, $fieldValTerm)"
      case BOOLEAN => s"$binaryRowTerm.setBoolean($index, $fieldValTerm)"
      case DATE =>  s"$binaryRowTerm.setInt($index, $fieldValTerm)"
      case TIME_WITHOUT_TIME_ZONE =>  s"$binaryRowTerm.setInt($index, $fieldValTerm)"
      case TIMESTAMP_WITHOUT_TIME_ZONE =>
        val dt = t.asInstanceOf[TimestampType]
        s"$binaryRowTerm.setTimestamp($index, $fieldValTerm, ${dt.getPrecision})"
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
        val dt = t.asInstanceOf[LocalZonedTimestampType]
        s"$binaryRowTerm.setTimestamp($index, $fieldValTerm, ${dt.getPrecision})"
      case INTERVAL_YEAR_MONTH =>  s"$binaryRowTerm.setInt($index, $fieldValTerm)"
      case INTERVAL_DAY_TIME =>  s"$binaryRowTerm.setLong($index, $fieldValTerm)"
      case DECIMAL =>
        val dt = t.asInstanceOf[DecimalType]
        s"$binaryRowTerm.setDecimal($index, $fieldValTerm, ${dt.getPrecision})"
      case _ =>
        throw new CodeGenException("Fail to find binary row field setter method of LogicalType "
          + t + ".")
    }

  // -------------------------- BoxedWrapperRow Set Field -------------------------------

  def boxedWrapperRowFieldSetAccess(
      rowTerm: String,
      indexTerm: String,
      fieldTerm: String,
      t: LogicalType): String =
    t.getTypeRoot match {
      case INTEGER => s"$rowTerm.setInt($indexTerm, $fieldTerm)"
      case BIGINT => s"$rowTerm.setLong($indexTerm, $fieldTerm)"
      case SMALLINT => s"$rowTerm.setShort($indexTerm, $fieldTerm)"
      case TINYINT => s"$rowTerm.setByte($indexTerm, $fieldTerm)"
      case FLOAT => s"$rowTerm.setFloat($indexTerm, $fieldTerm)"
      case DOUBLE => s"$rowTerm.setDouble($indexTerm, $fieldTerm)"
      case BOOLEAN => s"$rowTerm.setBoolean($indexTerm, $fieldTerm)"
      case DATE =>  s"$rowTerm.setInt($indexTerm, $fieldTerm)"
      case TIME_WITHOUT_TIME_ZONE =>  s"$rowTerm.setInt($indexTerm, $fieldTerm)"
      case INTERVAL_YEAR_MONTH => s"$rowTerm.setInt($indexTerm, $fieldTerm)"
      case INTERVAL_DAY_TIME => s"$rowTerm.setLong($indexTerm, $fieldTerm)"
      case _ => s"$rowTerm.setNonPrimitiveValue($indexTerm, $fieldTerm)"
    }

  // -------------------------- BinaryArray Set Access -------------------------------

  def binaryArraySetNull(
      index: Int,
      arrayTerm: String,
      t: LogicalType): String = t.getTypeRoot match {
    case BOOLEAN => s"$arrayTerm.setNullBoolean($index)"
    case TINYINT => s"$arrayTerm.setNullByte($index)"
    case SMALLINT => s"$arrayTerm.setNullShort($index)"
    case INTEGER => s"$arrayTerm.setNullInt($index)"
    case FLOAT => s"$arrayTerm.setNullFloat($index)"
    case DOUBLE => s"$arrayTerm.setNullDouble($index)"
    case TIME_WITHOUT_TIME_ZONE => s"$arrayTerm.setNullInt($index)"
    case DATE => s"$arrayTerm.setNullInt($index)"
    case INTERVAL_YEAR_MONTH => s"$arrayTerm.setNullInt($index)"
    case _ => s"$arrayTerm.setNullLong($index)"
  }

  // -------------------------- BinaryWriter Write -------------------------------

  def binaryWriterWriteNull(index: Int, writerTerm: String, t: LogicalType): String =
    binaryWriterWriteNull(index.toString, writerTerm, t)

  def binaryWriterWriteNull(
      indexTerm: String,
      writerTerm: String,
      t: LogicalType): String = t match {
    case d: DecimalType if !Decimal.isCompact(d.getPrecision) =>
      s"$writerTerm.writeDecimal($indexTerm, null, ${d.getPrecision})"
    case d: TimestampType if !SqlTimestamp.isCompact(d.getPrecision) =>
      s"$writerTerm.writeTimestamp($indexTerm, null, ${d.getPrecision})"
    case d: LocalZonedTimestampType if !SqlTimestamp.isCompact(d.getPrecision) =>
      s"$writerTerm.writeTimestamp($indexTerm, null, ${d.getPrecision})"
    case _ => s"$writerTerm.setNullAt($indexTerm)"
  }

  def binaryWriterWriteField(
      ctx: CodeGeneratorContext,
      index: Int,
      fieldValTerm: String,
      writerTerm: String,
      fieldType: LogicalType): String =
    binaryWriterWriteField(ctx, index.toString, fieldValTerm, writerTerm, fieldType)

  def binaryWriterWriteField(
      ctx: CodeGeneratorContext,
      indexTerm: String,
      fieldValTerm: String,
      writerTerm: String,
      t: LogicalType): String =
    t.getTypeRoot match {
      case INTEGER => s"$writerTerm.writeInt($indexTerm, $fieldValTerm)"
      case BIGINT => s"$writerTerm.writeLong($indexTerm, $fieldValTerm)"
      case SMALLINT => s"$writerTerm.writeShort($indexTerm, $fieldValTerm)"
      case TINYINT => s"$writerTerm.writeByte($indexTerm, $fieldValTerm)"
      case FLOAT => s"$writerTerm.writeFloat($indexTerm, $fieldValTerm)"
      case DOUBLE => s"$writerTerm.writeDouble($indexTerm, $fieldValTerm)"
      case BOOLEAN => s"$writerTerm.writeBoolean($indexTerm, $fieldValTerm)"
      case VARBINARY | BINARY => s"$writerTerm.writeBinary($indexTerm, $fieldValTerm)"
      case VARCHAR | CHAR => s"$writerTerm.writeString($indexTerm, $fieldValTerm)"
      case DECIMAL =>
        val dt = t.asInstanceOf[DecimalType]
        s"$writerTerm.writeDecimal($indexTerm, $fieldValTerm, ${dt.getPrecision})"
      case DATE => s"$writerTerm.writeInt($indexTerm, $fieldValTerm)"
      case TIME_WITHOUT_TIME_ZONE => s"$writerTerm.writeInt($indexTerm, $fieldValTerm)"
      case TIMESTAMP_WITHOUT_TIME_ZONE =>
        val dt = t.asInstanceOf[TimestampType]
        s"$writerTerm.writeTimestamp($indexTerm, $fieldValTerm, ${dt.getPrecision})"
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
        val dt = t.asInstanceOf[LocalZonedTimestampType]
        s"$writerTerm.writeTimestamp($indexTerm, $fieldValTerm, ${dt.getPrecision})"
      case INTERVAL_YEAR_MONTH => s"$writerTerm.writeInt($indexTerm, $fieldValTerm)"
      case INTERVAL_DAY_TIME => s"$writerTerm.writeLong($indexTerm, $fieldValTerm)"

      // complex types
      case ARRAY =>
        val ser = ctx.addReusableTypeSerializer(t)
        s"$writerTerm.writeArray($indexTerm, $fieldValTerm, $ser)"
      case MULTISET | MAP =>
        val ser = ctx.addReusableTypeSerializer(t)
        s"$writerTerm.writeMap($indexTerm, $fieldValTerm, $ser)"
      case ROW =>
        val ser = ctx.addReusableTypeSerializer(t)
        s"$writerTerm.writeRow($indexTerm, $fieldValTerm, $ser)"
      case RAW =>
        val ser = ctx.addReusableTypeSerializer(t)
        s"$writerTerm.writeGeneric($indexTerm, $fieldValTerm, $ser)"
    }

  private def isConverterIdentity(t: DataType): Boolean = {
    DataFormatConverters.getConverterForDataType(t).isInstanceOf[IdentityConverter[_]]
  }

  def genToInternal(ctx: CodeGeneratorContext, t: DataType, term: String): String =
    genToInternal(ctx, t)(term)

  /**
   * Generates code for converting the given external source data type to the internal data format.
   *
   * Use this function for converting at the edges of the API where primitive types CAN NOT occur
   * and NO NULL CHECKING is required as it might have been done by surrounding layers.
   */
  def genToInternal(ctx: CodeGeneratorContext, t: DataType): String => String = {
    if (isConverterIdentity(t)) {
      term => s"$term"
    } else {
      val iTerm = boxedTypeTermForType(fromDataTypeToLogicalType(t))
      val eTerm = typeTerm(t.getConversionClass)
      val converter = ctx.addReusableObject(
        DataFormatConverters.getConverterForDataType(t),
        "converter")
      term => s"($iTerm) $converter.toInternal(($eTerm) $term)"
    }
  }

  /**
   * Generates code for converting the given external source data type to the internal data format.
   *
   * Use this function for converting at the edges of the API where PRIMITIVE TYPES can occur or
   * the RESULT CAN BE NULL.
   */
  def genToInternalIfNeeded(
      ctx: CodeGeneratorContext,
      sourceDataType: DataType,
      externalTerm: String)
    : GeneratedExpression = {
    val sourceType = sourceDataType.getLogicalType
    val sourceClass = sourceDataType.getConversionClass
    // convert external source type to internal format
    val internalResultTerm = if (isInternalClass(sourceDataType)) {
      s"$externalTerm"
    } else {
      genToInternal(ctx, sourceDataType, externalTerm)
    }
    // extract null term from result term
    if (sourceClass.isPrimitive) {
      generateNonNullField(sourceType, internalResultTerm)
    } else {
      generateInputFieldUnboxing(ctx, sourceType, externalTerm, internalResultTerm)
    }
  }

  def genToExternal(
      ctx: CodeGeneratorContext,
      targetType: DataType,
      internalTerm: String): String = {
    if (isConverterIdentity(targetType)) {
      s"$internalTerm"
    } else {
      val iTerm = boxedTypeTermForType(fromDataTypeToLogicalType(targetType))
      val eTerm = typeTerm(targetType.getConversionClass)
      val converter = ctx.addReusableObject(
        DataFormatConverters.getConverterForDataType(targetType),
        "converter")
      s"($eTerm) $converter.toExternal(($iTerm) $internalTerm)"
    }
  }

  /**
   * Generates code for converting the internal data format to the given external target data type.
   *
   * Use this function for converting at the edges of the API.
   */
  def genToExternalIfNeeded(
      ctx: CodeGeneratorContext,
      targetDataType: DataType,
      internalExpr: GeneratedExpression)
    : String = {
    val targetType = fromDataTypeToLogicalType(targetDataType)
    // convert internal format to target type
    val externalResultTerm = if (isInternalClass(targetDataType)) {
      s"(${boxedTypeTermForType(targetType)}) ${internalExpr.resultTerm}"
    } else {
      genToExternal(ctx, targetDataType, internalExpr.resultTerm)
    }
    // merge null term into the result term
    if (targetDataType.getConversionClass.isPrimitive) {
      externalResultTerm
    } else {
      s"${internalExpr.nullTerm} ? null : ($externalResultTerm)"
    }
  }

  def udfFieldName(udf: UserDefinedFunction): String = s"function_${udf.functionIdentifier}"

  def genLogInfo(logTerm: String, format: String, argTerm: String): String =
    s"""$logTerm.info("$format", $argTerm);"""
}
