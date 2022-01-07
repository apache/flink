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

import java.lang.reflect.Method
import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Integer => JInt, Long => JLong, Object => JObject, Short => JShort}
import java.util.concurrent.atomic.AtomicLong

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.core.memory.MemorySegment
import org.apache.flink.table.data._
import org.apache.flink.table.data.binary.BinaryRowDataUtil.BYTE_ARRAY_BASE_OFFSET
import org.apache.flink.table.data.binary._
import org.apache.flink.table.data.util.DataFormatConverters
import org.apache.flink.table.data.util.DataFormatConverters.IdentityConverter
import org.apache.flink.table.data.utils.JoinedRowData
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.planner.codegen.GenerateUtils.{generateInputFieldUnboxing, generateNonNullField}
import org.apache.flink.table.runtime.dataview.StateDataViewStore
import org.apache.flink.table.runtime.functions.NumericUtils
import org.apache.flink.table.runtime.generated.{AggsHandleFunction, HashFunction, NamespaceAggsHandleFunction, TableAggsHandleFunction}
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType
import org.apache.flink.table.runtime.types.PlannerTypeUtils.isInteroperable
import org.apache.flink.table.runtime.typeutils.TypeCheckUtils
import org.apache.flink.table.runtime.util.{MurmurHashUtil, TimeWindowUtil}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.table.types.logical._
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks.{getFieldCount, getPrecision, getScale, hasRoot}
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils.toInternalConversionClass
import org.apache.flink.table.types.utils.DataTypeUtils.isInternal
import org.apache.flink.types.{Row, RowKind}

import java.lang.reflect.Method
import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Integer => JInt, Long => JLong, Object => JObject, Short => JShort}
import java.util.concurrent.atomic.AtomicLong

import scala.annotation.tailrec

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

  val BINARY_ROW: String = className[BinaryRowData]

  val ARRAY_DATA: String = className[ArrayData]

  val BINARY_ARRAY: String = className[BinaryArrayData]

  val BINARY_RAW_VALUE: String = className[BinaryRawValueData[_]]

  val BINARY_STRING: String = className[BinaryStringData]

  val MAP_DATA: String = className[MapData]

  val BINARY_MAP: String = className[BinaryMapData]

  val GENERIC_MAP: String = className[GenericMapData]

  val ROW_DATA: String = className[RowData]

  val JOINED_ROW: String = className[JoinedRowData]

  val GENERIC_ROW: String = className[GenericRowData]

  val ROW_KIND: String = className[RowKind]

  val NUMERIC_UTIL: String = className[NumericUtils]

  val DECIMAL_UTIL: String = className[DecimalDataUtils]

  val SEGMENT: String = className[MemorySegment]

  val AGGS_HANDLER_FUNCTION: String = className[AggsHandleFunction]

  val TABLE_AGGS_HANDLER_FUNCTION: String = className[TableAggsHandleFunction]

  val NAMESPACE_AGGS_HANDLER_FUNCTION: String = className[NamespaceAggsHandleFunction[_]]

  val STATE_DATA_VIEW_STORE: String = className[StateDataViewStore]

  val BINARY_STRING_UTIL: String = className[BinaryStringDataUtil]

  val TIME_WINDOW_UTIL: String = className[TimeWindowUtil]

  val TIMESTAMP_DATA: String = className[TimestampData]

  val RUNTIME_CONTEXT: String = className[RuntimeContext]

  // ----------------------------------------------------------------------------------------

  private val nameCounter = new AtomicLong

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
    if (clazz == classOf[StringData]) {
      // we should always use BinaryStringData in code generation instead of StringData
      // to allow accessing more useful methods easily
      return BINARY_STRING
    } else if (clazz == classOf[RawValueData[_]]) {
      // we should always use BinaryRawValueData in code generation instead of RawValueData
      // to allow accessing more useful methods easily
      return BINARY_RAW_VALUE
    }
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
  @tailrec
  def primitiveTypeTermForType(t: LogicalType): String = t.getTypeRoot match {
    // ordered by type root definition
    case BOOLEAN => "boolean"
    case TINYINT => "byte"
    case SMALLINT => "short"
    case INTEGER | DATE | TIME_WITHOUT_TIME_ZONE | INTERVAL_YEAR_MONTH => "int"
    case BIGINT | INTERVAL_DAY_TIME => "long"
    case FLOAT => "float"
    case DOUBLE => "double"
    case DISTINCT_TYPE => primitiveTypeTermForType(t.asInstanceOf[DistinctType].getSourceType)
    case _ => boxedTypeTermForType(t)
  }

  @tailrec
  def boxedTypeTermForType(t: LogicalType): String = t.getTypeRoot match {
    // ordered by type root definition
    case CHAR | VARCHAR => BINARY_STRING
    case BOOLEAN => className[JBoolean]
    case BINARY | VARBINARY => "byte[]"
    case DECIMAL => className[DecimalData]
    case TINYINT => className[JByte]
    case SMALLINT => className[JShort]
    case INTEGER | DATE | TIME_WITHOUT_TIME_ZONE | INTERVAL_YEAR_MONTH => className[JInt]
    case BIGINT | INTERVAL_DAY_TIME => className[JLong]
    case FLOAT => className[JFloat]
    case DOUBLE => className[JDouble]
    case TIMESTAMP_WITHOUT_TIME_ZONE | TIMESTAMP_WITH_LOCAL_TIME_ZONE => className[TimestampData]
    case TIMESTAMP_WITH_TIME_ZONE =>
      throw new UnsupportedOperationException("Unsupported type: " + t)
    case ARRAY => className[ArrayData]
    case MULTISET | MAP => className[MapData]
    case ROW | STRUCTURED_TYPE => className[RowData]
    case TIMESTAMP_WITHOUT_TIME_ZONE | TIMESTAMP_WITH_LOCAL_TIME_ZONE => className[TimestampData]
    case DISTINCT_TYPE => boxedTypeTermForType(t.asInstanceOf[DistinctType].getSourceType)
    case NULL => className[JObject] // special case for untyped null literals
    case RAW => className[BinaryRawValueData[_]]
    case SYMBOL | UNRESOLVED =>
      throw new IllegalArgumentException("Illegal type: " + t)
  }

  /**
    * Gets the default value for a primitive type, and null for generic types
    */
  @tailrec
  def primitiveDefaultValue(t: LogicalType): String = t.getTypeRoot match {
    // ordered by type root definition
    case CHAR | VARCHAR => s"$BINARY_STRING.EMPTY_UTF8"
    case BOOLEAN => "false"
    case TINYINT | SMALLINT | INTEGER | DATE | TIME_WITHOUT_TIME_ZONE | INTERVAL_YEAR_MONTH => "-1"
    case BIGINT | INTERVAL_DAY_TIME => "-1L"
    case FLOAT => "-1.0f"
    case DOUBLE => "-1.0d"

    case DISTINCT_TYPE => primitiveDefaultValue(t.asInstanceOf[DistinctType].getSourceType)

    case _ => "null"
  }

  @tailrec
  def hashCodeForType(
      ctx: CodeGeneratorContext,
      t: LogicalType,
      term: String)
    : String = t.getTypeRoot match {
    // ordered by type root definition
    case VARCHAR | CHAR =>
      s"$term.hashCode()"
    case BOOLEAN =>
      s"${className[JBoolean]}.hashCode($term)"
    case BINARY | VARBINARY =>
      s"${className[MurmurHashUtil]}.hashUnsafeBytes($term, $BYTE_ARRAY_BASE_OFFSET, $term.length)"
    case DECIMAL =>
      s"$term.hashCode()"
    case TINYINT =>
      s"${className[JByte]}.hashCode($term)"
    case SMALLINT =>
      s"${className[JShort]}.hashCode($term)"
    case INTEGER | DATE | TIME_WITHOUT_TIME_ZONE | INTERVAL_YEAR_MONTH =>
      s"${className[JInt]}.hashCode($term)"
    case BIGINT | INTERVAL_DAY_TIME => s"${className[JLong]}.hashCode($term)"
    case FLOAT => s"${className[JFloat]}.hashCode($term)"
    case DOUBLE => s"${className[JDouble]}.hashCode($term)"
    case TIMESTAMP_WITHOUT_TIME_ZONE | TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
      s"$term.hashCode()"
    case TIMESTAMP_WITH_TIME_ZONE | ARRAY | MULTISET | MAP =>
      throw new UnsupportedOperationException(
        s"Unsupported type($t) to generate hash code," +
            s" the type($t) is not supported as a GROUP_BY/PARTITION_BY/JOIN_EQUAL/UNION field.")
    case INTERVAL_DAY_TIME => s"${className[JLong]}.hashCode($term)"
    case ROW | STRUCTURED_TYPE =>
      val fieldCount = getFieldCount(t)
      val subCtx = CodeGeneratorContext(ctx.tableConfig)
      val genHash = HashCodeGenerator.generateRowHash(
        subCtx, t, "SubHashRow", (0 until fieldCount).toArray)
      ctx.addReusableInnerClass(genHash.getClassName, genHash.getCode)
      val refs = ctx.addReusableObject(subCtx.references.toArray, "subRefs")
      val hashFunc = newName("hashFunc")
      ctx.addReusableMember(s"${classOf[HashFunction].getCanonicalName} $hashFunc;")
      ctx.addReusableInitStatement(s"$hashFunc = new ${genHash.getClassName}($refs);")
      s"$hashFunc.hashCode($term)"
    case DISTINCT_TYPE =>
      hashCodeForType(ctx, t.asInstanceOf[DistinctType].getSourceType, term)
    case RAW =>
      val serializer = t match {
        case rt: RawType[_] =>
          rt.getTypeSerializer
        case tirt: TypeInformationRawType[_] =>
          tirt.getTypeInformation.createSerializer(new ExecutionConfig)
      }
      val serTerm = ctx.addReusableObject(serializer, "serializer")
      s"$BINARY_RAW_VALUE.getJavaObjectFromRawValueData($term, $serTerm).hashCode()"
    case NULL | SYMBOL | UNRESOLVED =>
      throw new IllegalArgumentException("Illegal type: " + t)
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
        s"$DECIMAL_UTIL.castFrom(" +
          s"${expr.resultTerm}, ${dt.getPrecision}, ${dt.getScale})"
      case (TINYINT, FLOAT) => s"(float) ${expr.resultTerm}"
      case (TINYINT, DOUBLE) => s"(double) ${expr.resultTerm}"

      // short -> other numeric types
      case (SMALLINT, INTEGER) => s"(int) ${expr.resultTerm}"
      case (SMALLINT, BIGINT) => s"(long) ${expr.resultTerm}"
      case (SMALLINT, DECIMAL) =>
        val dt = targetType.asInstanceOf[DecimalType]
        s"$DECIMAL_UTIL.castFrom(" +
          s"${expr.resultTerm}, ${dt.getPrecision}, ${dt.getScale})"
      case (SMALLINT, FLOAT) => s"(float) ${expr.resultTerm}"
      case (SMALLINT, DOUBLE) => s"(double) ${expr.resultTerm}"

      // int -> other numeric types
      case (INTEGER, BIGINT) => s"(long) ${expr.resultTerm}"
      case (INTEGER, DECIMAL) =>
        val dt = targetType.asInstanceOf[DecimalType]
        s"$DECIMAL_UTIL.castFrom(" +
          s"${expr.resultTerm}, ${dt.getPrecision}, ${dt.getScale})"
      case (INTEGER, FLOAT) => s"(float) ${expr.resultTerm}"
      case (INTEGER, DOUBLE) => s"(double) ${expr.resultTerm}"

      // long -> other numeric types
      case (BIGINT, DECIMAL) =>
        val dt = targetType.asInstanceOf[DecimalType]
        s"$DECIMAL_UTIL.castFrom(" +
          s"${expr.resultTerm}, ${dt.getPrecision}, ${dt.getScale})"
      case (BIGINT, FLOAT) => s"(float) ${expr.resultTerm}"
      case (BIGINT, DOUBLE) => s"(double) ${expr.resultTerm}"

      // decimal -> other numeric types
      case (DECIMAL, DECIMAL) =>
        val dt = targetType.asInstanceOf[DecimalType]
        s"$DECIMAL_UTIL.castToDecimal(" +
          s"${expr.resultTerm}, ${dt.getPrecision}, ${dt.getScale})"
      case (DECIMAL, FLOAT) =>
        s"$DECIMAL_UTIL.castToFloat(${expr.resultTerm})"
      case (DECIMAL, DOUBLE) =>
        s"$DECIMAL_UTIL.castToDouble(${expr.resultTerm})"

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

  def udfFieldName(udf: UserDefinedFunction): String = s"function_${udf.functionIdentifier}"

  def genLogInfo(logTerm: String, format: String, argTerm: String): String =
    s"""$logTerm.info("$format", $argTerm);"""

  // --------------------------------------------------------------------------------
  // DataFormat Operations
  // --------------------------------------------------------------------------------

  // -------------------------- RowData Read Access -------------------------------

  def rowFieldReadAccess(
      ctx: CodeGeneratorContext,
      index: Int,
      rowTerm: String,
      fieldType: LogicalType) : String =
    rowFieldReadAccess(ctx, index.toString, rowTerm, fieldType)

  @tailrec
  def rowFieldReadAccess(
      ctx: CodeGeneratorContext,
      indexTerm: String,
      rowTerm: String,
      t: LogicalType)
    : String = t.getTypeRoot match {
      // ordered by type root definition
      case CHAR | VARCHAR =>
        s"(($BINARY_STRING) $rowTerm.getString($indexTerm))"
      case BOOLEAN =>
        s"$rowTerm.getBoolean($indexTerm)"
      case BINARY | VARBINARY =>
        s"$rowTerm.getBinary($indexTerm)"
      case DECIMAL =>
        s"$rowTerm.getDecimal($indexTerm, ${getPrecision(t)}, ${getScale(t)})"
      case TINYINT =>
        s"$rowTerm.getByte($indexTerm)"
      case SMALLINT =>
        s"$rowTerm.getShort($indexTerm)"
      case INTEGER | DATE | TIME_WITHOUT_TIME_ZONE | INTERVAL_YEAR_MONTH =>
        s"$rowTerm.getInt($indexTerm)"
      case BIGINT | INTERVAL_DAY_TIME =>
        s"$rowTerm.getLong($indexTerm)"
      case FLOAT =>
        s"$rowTerm.getFloat($indexTerm)"
      case DOUBLE =>
        s"$rowTerm.getDouble($indexTerm)"
      case TIMESTAMP_WITHOUT_TIME_ZONE | TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
        s"$rowTerm.getTimestamp($indexTerm, ${getPrecision(t)})"
      case TIMESTAMP_WITH_TIME_ZONE =>
        throw new UnsupportedOperationException("Unsupported type: " + t)
      case ARRAY =>
        s"$rowTerm.getArray($indexTerm)"
      case MULTISET | MAP  =>
        s"$rowTerm.getMap($indexTerm)"
      case ROW | STRUCTURED_TYPE =>
        s"$rowTerm.getRow($indexTerm, ${getFieldCount(t)})"
      case DISTINCT_TYPE =>
        rowFieldReadAccess(ctx, indexTerm, rowTerm, t.asInstanceOf[DistinctType].getSourceType)
      case RAW =>
        s"(($BINARY_RAW_VALUE) $rowTerm.getRawValue($indexTerm))"
      case NULL | SYMBOL | UNRESOLVED =>
        throw new IllegalArgumentException("Illegal type: " + t)
    }

  // -------------------------- RowData Set Field -------------------------------

  def rowSetField(
      ctx: CodeGeneratorContext,
      rowClass: Class[_ <: RowData],
      rowTerm: String,
      indexTerm: String,
      fieldExpr: GeneratedExpression,
      binaryRowWriterTerm: Option[String]): String = {

    val fieldType = fieldExpr.resultType
    val fieldTerm = fieldExpr.resultTerm

    if (rowClass == classOf[BinaryRowData]) {
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
          // directly set field to BinaryRowData, this depends on all the fields are fixed length
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
    } else if (rowClass == classOf[GenericRowData] || rowClass == classOf[BoxedWrapperRowData]) {
      val writeField = if (rowClass == classOf[GenericRowData]) {
        s"$rowTerm.setField($indexTerm, $fieldTerm)"
      } else {
        boxedWrapperRowFieldSetAccess(rowTerm, indexTerm, fieldTerm, fieldType)
      }
      val setNullField = if (rowClass == classOf[GenericRowData]) {
        s"$rowTerm.setField($indexTerm, null)"
      } else {
        s"$rowTerm.setNullAt($indexTerm)"
      }

      if (ctx.nullCheck) {
        s"""
           |${fieldExpr.code}
           |if (${fieldExpr.nullTerm}) {
           |  $setNullField;
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

  // -------------------------- BinaryRowData Set Field -------------------------------

  def binaryRowSetNull(index: Int, rowTerm: String, t: LogicalType): String =
    binaryRowSetNull(index.toString, rowTerm, t)

  @tailrec
  def binaryRowSetNull(
      indexTerm: String,
      rowTerm: String,
      t: LogicalType)
    : String = t.getTypeRoot match {
    // ordered by type root definition
    case DECIMAL if !DecimalData.isCompact(getPrecision(t)) =>
      s"$rowTerm.setDecimal($indexTerm, null, ${getPrecision(t)})"
    case TIMESTAMP_WITHOUT_TIME_ZONE | TIMESTAMP_WITH_LOCAL_TIME_ZONE
        if !TimestampData.isCompact(getPrecision(t)) =>
      s"$rowTerm.setTimestamp($indexTerm, null, ${getPrecision(t)})"
    case DISTINCT_TYPE =>
      binaryRowSetNull(indexTerm, rowTerm, t.asInstanceOf[DistinctType].getSourceType)
    case _ =>
      s"$rowTerm.setNullAt($indexTerm)"
  }

  def binaryRowFieldSetAccess(
      index: Int,
      binaryRowTerm: String,
      fieldType: LogicalType,
      fieldValTerm: String): String =
    binaryRowFieldSetAccess(index.toString, binaryRowTerm, fieldType, fieldValTerm)

  @tailrec
  def binaryRowFieldSetAccess(
      index: String,
      binaryRowTerm: String,
      t: LogicalType,
      fieldValTerm: String)
    : String = t.getTypeRoot match {
    // ordered by type root definition
    case BOOLEAN =>
      s"$binaryRowTerm.setBoolean($index, $fieldValTerm)"
    case DECIMAL =>
      s"$binaryRowTerm.setDecimal($index, $fieldValTerm, ${getPrecision(t)})"
    case TINYINT =>
      s"$binaryRowTerm.setByte($index, $fieldValTerm)"
    case SMALLINT =>
      s"$binaryRowTerm.setShort($index, $fieldValTerm)"
    case INTEGER | DATE | TIME_WITHOUT_TIME_ZONE | INTERVAL_YEAR_MONTH =>
      s"$binaryRowTerm.setInt($index, $fieldValTerm)"
    case BIGINT | INTERVAL_DAY_TIME =>
      s"$binaryRowTerm.setLong($index, $fieldValTerm)"
    case FLOAT =>
      s"$binaryRowTerm.setFloat($index, $fieldValTerm)"
    case DOUBLE =>
      s"$binaryRowTerm.setDouble($index, $fieldValTerm)"
    case TIMESTAMP_WITHOUT_TIME_ZONE | TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
      s"$binaryRowTerm.setTimestamp($index, $fieldValTerm, ${getPrecision(t)})"
    case DISTINCT_TYPE =>
      binaryRowFieldSetAccess(
        index,
        binaryRowTerm,
        t.asInstanceOf[DistinctType].getSourceType,
        fieldValTerm)
    case _ =>
      throw new CodeGenException(
        "Fail to find binary row field setter method of LogicalType " + t + ".")
  }

  // -------------------------- BoxedWrapperRowData Set Field -------------------------------

  @tailrec
  def boxedWrapperRowFieldSetAccess(
      rowTerm: String,
      indexTerm: String,
      fieldTerm: String,
      t: LogicalType)
    : String = t.getTypeRoot match {
    // ordered by type root definition
    case BOOLEAN =>
      s"$rowTerm.setBoolean($indexTerm, $fieldTerm)"
    case TINYINT =>
      s"$rowTerm.setByte($indexTerm, $fieldTerm)"
    case SMALLINT =>
      s"$rowTerm.setShort($indexTerm, $fieldTerm)"
    case INTEGER | DATE | TIME_WITHOUT_TIME_ZONE | INTERVAL_YEAR_MONTH =>
      s"$rowTerm.setInt($indexTerm, $fieldTerm)"
    case BIGINT | INTERVAL_DAY_TIME =>
      s"$rowTerm.setLong($indexTerm, $fieldTerm)"
    case FLOAT =>
      s"$rowTerm.setFloat($indexTerm, $fieldTerm)"
    case DOUBLE =>
      s"$rowTerm.setDouble($indexTerm, $fieldTerm)"
    case DISTINCT_TYPE =>
      boxedWrapperRowFieldSetAccess(
        rowTerm,
        indexTerm,
        fieldTerm,
        t.asInstanceOf[DistinctType].getSourceType)
    case _ =>
      s"$rowTerm.setNonPrimitiveValue($indexTerm, $fieldTerm)"
  }

  // -------------------------- BinaryArray Set Access -------------------------------

  @tailrec
  def binaryArraySetNull(
      index: Int,
      arrayTerm: String,
      t: LogicalType)
    : String = t.getTypeRoot match {
    // ordered by type root definition
    case BOOLEAN =>
      s"$arrayTerm.setNullBoolean($index)"
    case TINYINT =>
      s"$arrayTerm.setNullByte($index)"
    case SMALLINT =>
      s"$arrayTerm.setNullShort($index)"
    case INTEGER | DATE | TIME_WITHOUT_TIME_ZONE | INTERVAL_YEAR_MONTH =>
      s"$arrayTerm.setNullInt($index)"
    case FLOAT =>
      s"$arrayTerm.setNullFloat($index)"
    case DOUBLE =>
      s"$arrayTerm.setNullDouble($index)"
    case DISTINCT_TYPE =>
      binaryArraySetNull(index, arrayTerm, t)
    case _ =>
      s"$arrayTerm.setNullLong($index)"
  }

  // -------------------------- BinaryWriter Write -------------------------------

  def binaryWriterWriteNull(index: Int, writerTerm: String, t: LogicalType): String =
    binaryWriterWriteNull(index.toString, writerTerm, t)

  @tailrec
  def binaryWriterWriteNull(
      indexTerm: String,
      writerTerm: String,
      t: LogicalType)
    : String = t.getTypeRoot match {
    // ordered by type root definition
    case DECIMAL if !DecimalData.isCompact(getPrecision(t)) =>
      s"$writerTerm.writeDecimal($indexTerm, null, ${getPrecision(t)})"
    case TIMESTAMP_WITHOUT_TIME_ZONE | TIMESTAMP_WITH_LOCAL_TIME_ZONE
        if !TimestampData.isCompact(getPrecision(t)) =>
      s"$writerTerm.writeTimestamp($indexTerm, null, ${getPrecision(t)})"
    case DISTINCT_TYPE =>
      binaryWriterWriteNull(indexTerm, writerTerm, t.asInstanceOf[DistinctType].getSourceType)
    case _ =>
      s"$writerTerm.setNullAt($indexTerm)"
  }

  def binaryWriterWriteField(
      ctx: CodeGeneratorContext,
      index: Int,
      fieldValTerm: String,
      writerTerm: String,
      fieldType: LogicalType): String =
    binaryWriterWriteField(ctx, index.toString, fieldValTerm, writerTerm, fieldType)

  @tailrec
  def binaryWriterWriteField(
      ctx: CodeGeneratorContext,
      indexTerm: String,
      fieldValTerm: String,
      writerTerm: String,
      t: LogicalType)
    : String = t.getTypeRoot match {
    // ordered by type root definition
    case CHAR | VARCHAR =>
      s"$writerTerm.writeString($indexTerm, $fieldValTerm)"
    case BOOLEAN =>
      s"$writerTerm.writeBoolean($indexTerm, $fieldValTerm)"
    case BINARY | VARBINARY =>
      s"$writerTerm.writeBinary($indexTerm, $fieldValTerm)"
    case DECIMAL =>
      s"$writerTerm.writeDecimal($indexTerm, $fieldValTerm, ${getPrecision(t)})"
    case TINYINT =>
      s"$writerTerm.writeByte($indexTerm, $fieldValTerm)"
    case SMALLINT =>
      s"$writerTerm.writeShort($indexTerm, $fieldValTerm)"
    case INTEGER | DATE | TIME_WITHOUT_TIME_ZONE | INTERVAL_YEAR_MONTH =>
      s"$writerTerm.writeInt($indexTerm, $fieldValTerm)"
    case BIGINT | INTERVAL_DAY_TIME =>
      s"$writerTerm.writeLong($indexTerm, $fieldValTerm)"
    case FLOAT =>
      s"$writerTerm.writeFloat($indexTerm, $fieldValTerm)"
    case DOUBLE =>
      s"$writerTerm.writeDouble($indexTerm, $fieldValTerm)"
    case TIMESTAMP_WITHOUT_TIME_ZONE | TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
      s"$writerTerm.writeTimestamp($indexTerm, $fieldValTerm, ${getPrecision(t)})"
    case TIMESTAMP_WITH_TIME_ZONE =>
      throw new UnsupportedOperationException("Unsupported type: " + t)
    case ARRAY =>
      val ser = ctx.addReusableTypeSerializer(t)
      s"$writerTerm.writeArray($indexTerm, $fieldValTerm, $ser)"
    case MULTISET | MAP =>
      val ser = ctx.addReusableTypeSerializer(t)
      s"$writerTerm.writeMap($indexTerm, $fieldValTerm, $ser)"
    case ROW | STRUCTURED_TYPE =>
      val ser = ctx.addReusableTypeSerializer(t)
      s"$writerTerm.writeRow($indexTerm, $fieldValTerm, $ser)"
    case DISTINCT_TYPE =>
      binaryWriterWriteField(
        ctx,
        indexTerm,
        fieldValTerm,
        writerTerm,
        t.asInstanceOf[DistinctType].getSourceType)
    case RAW =>
      val ser = ctx.addReusableTypeSerializer(t)
      s"$writerTerm.writeRawValue($indexTerm, $fieldValTerm, $ser)"
    case NULL | SYMBOL | UNRESOLVED =>
      throw new IllegalArgumentException("Illegal type: " + t);
  }

  // -------------------------- Data Structure Conversion  -------------------------------

  /**
   * Generates code for converting the given term of external data type to an internal data
   * structure.
   *
   * Use this function for converting at the edges of the API where primitive types CAN NOT occur
   * and NO NULL CHECKING is required as it might have been done by surrounding layers.
   */
  def genToInternalConverter(
      ctx: CodeGeneratorContext,
      sourceDataType: DataType)
    : String => String = {

    // fallback to old stack if at least one legacy type is present
    if (LogicalTypeChecks.hasLegacyTypes(sourceDataType.getLogicalType)) {
      return genToInternalConverterWithLegacy(ctx, sourceDataType)
    }

    if (isInternal(sourceDataType)) {
      externalTerm => s"$externalTerm"
    } else {
      val internalTypeTerm = boxedTypeTermForType(sourceDataType.getLogicalType)
      val externalTypeTerm = typeTerm(sourceDataType.getConversionClass)
      val converterTerm = ctx.addReusableConverter(sourceDataType)
      externalTerm =>
        s"($internalTypeTerm) $converterTerm.toInternalOrNull(($externalTypeTerm) $externalTerm)"
    }
  }

  /**
   * Generates code for converting the given term of external data type to an internal data
   * structure.
   *
   * Use this function for converting at the edges of the API where primitive types CAN NOT occur
   * and NO NULL CHECKING is required as it might have been done by surrounding layers.
   */
  def genToInternalConverter(
      ctx: CodeGeneratorContext,
      sourceDataType: DataType,
      externalTerm: String)
    : String = {
    genToInternalConverter(ctx, sourceDataType)(externalTerm)
  }

  /**
   * Generates code for converting the given term of external data type to an internal data
   * structure.
   *
   * Use this function for converting at the edges of the API where PRIMITIVE TYPES can occur or
   * the RESULT CAN BE NULL.
   */
  def genToInternalConverterAll(
      ctx: CodeGeneratorContext,
      sourceDataType: DataType,
      externalTerm: String)
    : GeneratedExpression = {

    // fallback to old stack if at least one legacy type is present
    if (LogicalTypeChecks.hasLegacyTypes(sourceDataType.getLogicalType)) {
      return genToInternalConverterAllWithLegacy(ctx, sourceDataType, externalTerm)
    }

    val sourceType = sourceDataType.getLogicalType
    val sourceClass = sourceDataType.getConversionClass
    // convert external source type to internal structure
    val internalResultTerm = if (isInternal(sourceDataType)) {
      s"$externalTerm"
    } else {
      genToInternalConverter(ctx, sourceDataType)(externalTerm)
    }
    // extract null term from result term
    if (sourceClass.isPrimitive) {
      generateNonNullField(sourceType, internalResultTerm)
    } else {
      generateInputFieldUnboxing(ctx, sourceType, externalTerm, internalResultTerm)
    }
  }

  /**
   * Generates code for converting the given term of internal data structure to the given
   * external target data type.
   *
   * Use this function for converting at the edges of the API where primitive types CAN NOT occur
   * and NO NULL CHECKING is required as it might have been done by surrounding layers.
   */
  def genToExternalConverter(
      ctx: CodeGeneratorContext,
      targetDataType: DataType,
      internalTerm: String)
    : String = {

    // fallback to old stack if at least one legacy type is present
    if (LogicalTypeChecks.hasLegacyTypes(targetDataType.getLogicalType)) {
      return genToExternalConverterWithLegacy(ctx, targetDataType, internalTerm)
    }

    if (isInternal(targetDataType)) {
      s"$internalTerm"
    } else {
      val internalTypeTerm = boxedTypeTermForType(targetDataType.getLogicalType)
      val externalTypeTerm = typeTerm(targetDataType.getConversionClass)
      val converterTerm = ctx.addReusableConverter(targetDataType)
      s"($externalTypeTerm) $converterTerm.toExternal(($internalTypeTerm) $internalTerm)"
    }
  }

  /**
   * Generates code for converting the given expression of internal data structure to the given
   * external target data type.
   *
   * Use this function for converting at the edges of the API where PRIMITIVE TYPES can occur or
   * the RESULT CAN BE NULL.
   */
  def genToExternalConverterAll(
      ctx: CodeGeneratorContext,
      targetDataType: DataType,
      internalExpr: GeneratedExpression)
    : String = {

    // fallback to old stack if at least one legacy type is present
    if (LogicalTypeChecks.hasLegacyTypes(targetDataType.getLogicalType)) {
      return genToExternalConverterAllWithLegacy(ctx, targetDataType, internalExpr)
    }

    val targetType = targetDataType.getLogicalType
    val targetTypeTerm = boxedTypeTermForType(targetType)

    // untyped null literal
    if (hasRoot(internalExpr.resultType, NULL)) {
      return s"($targetTypeTerm) null"
    }

    // convert internal structure to target type
    val externalResultTerm = if (isInternal(targetDataType)) {
      s"($targetTypeTerm) ${internalExpr.resultTerm}"
    } else {
      genToExternalConverter(ctx, targetDataType, internalExpr.resultTerm)
    }
    // merge null term into the result term
    if (targetDataType.getConversionClass.isPrimitive) {
      externalResultTerm
    } else {
      s"${internalExpr.nullTerm} ? null : ($externalResultTerm)"
    }
  }

  /**
    * If it's internally compatible, don't need to DataStructure converter.
    * clazz != classOf[Row] => Row can only infer GenericType[Row].
    */
  @deprecated
  def isInternalClass(t: DataType): Boolean = {
    val clazz = t.getConversionClass
    clazz != classOf[Object] && clazz != classOf[Row] &&
        (classOf[RowData].isAssignableFrom(clazz) ||
            clazz == toInternalConversionClass(fromDataTypeToLogicalType(t)))
  }

  @deprecated
  private def isConverterIdentity(t: DataType): Boolean = {
    DataFormatConverters.getConverterForDataType(t).isInstanceOf[IdentityConverter[_]]
  }

  /**
   * Generates code for converting the given external source data type to the internal data format.
   *
   * Use this function for converting at the edges of the API where primitive types CAN NOT occur
   * and NO NULL CHECKING is required as it might have been done by surrounding layers.
   *
   * @deprecated This uses the legacy [[DataFormatConverters]] including legacy types.
   */
  @deprecated
  private def genToInternalConverterWithLegacy(
      ctx: CodeGeneratorContext,
      t: DataType)
    : String => String = {
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
   *
   * @deprecated This uses the legacy [[DataFormatConverters]] including legacy types.
   */
  @deprecated
  private def genToInternalConverterAllWithLegacy(
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
      genToInternalConverterWithLegacy(ctx, sourceDataType)(externalTerm)
    }
    // extract null term from result term
    if (sourceClass.isPrimitive) {
      generateNonNullField(sourceType, internalResultTerm)
    } else {
      generateInputFieldUnboxing(ctx, sourceType, externalTerm, internalResultTerm)
    }
  }

  /**
   * @deprecated This uses the legacy [[DataFormatConverters]] including legacy types.
   */
  @deprecated
  def genToExternalConverterWithLegacy( // still public due to FLINK-18701
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
   * @deprecated This uses the legacy [[DataFormatConverters]] including legacy types.
   */
  @deprecated
  private def genToExternalConverterAllWithLegacy(
      ctx: CodeGeneratorContext,
      targetDataType: DataType,
      internalExpr: GeneratedExpression)
    : String = {
    val targetType = fromDataTypeToLogicalType(targetDataType)
    val targetTypeTerm = boxedTypeTermForType(targetType)

    // untyped null literal
    if (hasRoot(internalExpr.resultType, NULL)) {
      return s"($targetTypeTerm) null"
    }

    // convert internal format to target type
    val externalResultTerm = if (isInternalClass(targetDataType)) {
      s"($targetTypeTerm) ${internalExpr.resultTerm}"
    } else {
      genToExternalConverterWithLegacy(ctx, targetDataType, internalExpr.resultTerm)
    }
    // merge null term into the result term
    if (targetDataType.getConversionClass.isPrimitive) {
      externalResultTerm
    } else {
      s"${internalExpr.nullTerm} ? null : ($externalResultTerm)"
    }
  }
}
