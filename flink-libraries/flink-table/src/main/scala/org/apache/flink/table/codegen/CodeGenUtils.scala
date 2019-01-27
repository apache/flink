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

import java.lang.reflect.Method
import java.lang.{Boolean => JBoolean, Byte => JByte, Character => JChar, Double => JDouble, Float => JFloat, Integer => JInt, Long => JLong, Short => JShort}
import java.math.{BigDecimal => JBigDecimal}
import java.sql.{Date, Time, Timestamp}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.calcite.avatica.util.ByteString
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.`type`.SqlTypeName.{ROW => _, _}
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.util.BuiltInMethod
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.flink.api.common.InvalidProgramException
import org.apache.flink.api.common.functions.{FlatJoinFunction, FlatMapFunction, MapFunction}
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.api.types._
import org.apache.flink.table.codegen.CodeGeneratorContext.{BASE_ROW_UTIL, BINARY_STRING}
import org.apache.flink.table.codegen.GeneratedExpression.NEVER_NULL
import org.apache.flink.table.codegen.calls.ScalarOperators._
import org.apache.flink.table.codegen.calls.{BinaryStringCallGen, BuiltInMethods, CurrentTimePointCallGen, FunctionGenerator}
import org.apache.flink.table.dataformat._
import org.apache.flink.table.errorcode.TableErrors
import org.apache.flink.table.functions.sql.ScalarSqlFunctions
import org.apache.flink.table.functions.sql.internal.{SqlRuntimeFilterBuilderFunction, SqlRuntimeFilterFunction, SqlThrowExceptionFunction}
import org.apache.flink.table.typeutils.TypeCheckUtils.{isNumeric, isTemporal, isTimeInterval}
import org.apache.flink.table.typeutils._
import org.apache.flink.table.util.Logging.CODE_LOG
import org.apache.flink.types.Row
import org.apache.flink.util.StringUtils

import org.codehaus.commons.compiler.{CompileException, ICookable}
import org.codehaus.janino.SimpleCompiler

import scala.collection.mutable.ListBuffer

object CodeGenUtils {

  private val nameCounter = new AtomicInteger

  def newName(name: String): String = {
    s"$name$$${nameCounter.getAndIncrement}"
  }

  def newNames(names: Seq[String]): Seq[String] = {
    require(names.toSet.size == names.length, "Duplicated names")
    val newId = nameCounter.getAndIncrement
    names.map(name => s"$name$$$newId")
  }

  /**
    * Retrieve the canonical name of a class type.
    */
  def className[T](implicit m: Manifest[T]): String = m.runtimeClass.getCanonicalName

  def needCopyForType(t: InternalType): Boolean = t match {
    case DataTypes.STRING => true
    case _: ArrayType => true
    case _: MapType => true
    case _: RowType => true
    case _: GenericType[_] => true
    case _ => false
  }

  def needCloneRefForType(t: InternalType): Boolean = t match {
    case DataTypes.STRING => true
    case _ => false
  }

  def needCloneRefForDataType(t: DataType): Boolean =
    TypeConverters.createExternalTypeInfoFromDataType(t) match {
      case BinaryStringTypeInfo.INSTANCE => true
      case _ => false
  }

  // when casting we first need to unbox Primitives, for example,
  // float a = 1.0f;
  // byte b = (byte) a;
  // works, but for boxed types we need this:
  // Float a = 1.0f;
  // Byte b = (byte)(float) a;
  def primitiveTypeTermForType(t: InternalType): String = t match {
    case DataTypes.INT => "int"
    case DataTypes.LONG => "long"
    case DataTypes.SHORT => "short"
    case DataTypes.BYTE => "byte"
    case DataTypes.FLOAT => "float"
    case DataTypes.DOUBLE => "double"
    case DataTypes.BOOLEAN => "boolean"
    case DataTypes.CHAR => "char"

    case _: DateType => "int"
    case DataTypes.TIME => "int"
    case _: TimestampType => "long"

    case DataTypes.INTERVAL_MONTHS => "int"
    case DataTypes.INTERVAL_MILLIS => "long"

    case _ => boxedTypeTermForType(t)
  }

  def isInternalPrimitive(tpe: InternalType): Boolean = {
    // now, only temporal type use primitive for representation
    isTemporal(tpe)
  }

  def externalBoxedTermForType(t: DataType): String = t match {
    case DataTypes.STRING => classOf[String].getCanonicalName
    case _: DecimalType => classOf[JBigDecimal].getCanonicalName
    case at: ArrayType if at.isPrimitive =>
      s"${primitiveTypeTermForType(at.getElementInternalType)}[]"
    case at: ArrayType => s"${externalBoxedTermForType(at.getElementType)}[]"
    case bt: RowType => classOf[Row].getCanonicalName
    case _: MapType => classOf[java.util.Map[_, _]].getCanonicalName
    case _: TimestampType if t != DataTypes.INTERVAL_MILLIS => classOf[Timestamp].getCanonicalName
    case _: DateType if t != DataTypes.INTERVAL_MONTHS => classOf[Date].getCanonicalName
    case DataTypes.TIME => classOf[Time].getCanonicalName
    case it: InternalType => boxedTypeTermForType(it)
    case wt: TypeInfoWrappedDataType => wt.getTypeInfo match {
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
      case _ => wt.getTypeInfo.getTypeClass.getCanonicalName
    }
  }

  def boxedTypeTermForType(t: InternalType): String = t match {
    case DataTypes.INT => classOf[JInt].getCanonicalName
    case DataTypes.LONG => classOf[JLong].getCanonicalName
    case DataTypes.SHORT => classOf[JShort].getCanonicalName
    case DataTypes.BYTE => classOf[JByte].getCanonicalName
    case DataTypes.FLOAT => classOf[JFloat].getCanonicalName
    case DataTypes.DOUBLE => classOf[JDouble].getCanonicalName
    case DataTypes.BOOLEAN => classOf[JBoolean].getCanonicalName
    case DataTypes.CHAR => classOf[JChar].getCanonicalName

    case _: DateType => boxedTypeTermForType(DataTypes.INT)
    case DataTypes.TIME => boxedTypeTermForType(DataTypes.INT)
    case _: TimestampType => boxedTypeTermForType(DataTypes.LONG)

    case DataTypes.STRING => BINARY_STRING
    case DataTypes.BYTE_ARRAY => "byte[]"
    case _: DecimalType => classOf[Decimal].getCanonicalName
    case _: ArrayType => classOf[BaseArray].getCanonicalName
    case _: MapType => classOf[BaseMap].getCanonicalName
    case _: RowType => classOf[BaseRow].getCanonicalName

    case gt: GenericType[_] => gt.getTypeInfo.getTypeClass.getCanonicalName
  }

  def primitiveDefaultValue(t: InternalType): String = t match {
    case DataTypes.INT | DataTypes.BYTE | DataTypes.SHORT => "-1"
    case DataTypes.LONG => "-1L"
    case DataTypes.FLOAT => "-1.0f"
    case DataTypes.DOUBLE => "-1.0d"
    case DataTypes.BOOLEAN => "false"
    case DataTypes.STRING => s"$BINARY_STRING.EMPTY_UTF8"
    case DataTypes.CHAR => "'\\0'"

    case _: DateType | DataTypes.TIME => "-1"
    case _: TimestampType => "-1L"

    case _ => "null"
  }

  /**
    * If it's internally compatible, don't need to DataStructure converter.
    * clazz != classOf[Row] => Row can only infer GenericType[Row].
    */
  def isInternalClass(clazz: Class[_], t: DataType): Boolean =
    clazz != classOf[Object] && clazz != classOf[Row] &&
      (classOf[BaseRow].isAssignableFrom(clazz) ||
        clazz == TypeConverters.createInternalTypeInfoFromDataType(t).getTypeClass)

  def qualifyMethod(method: Method): String =
    method.getDeclaringClass.getCanonicalName + "." + method.getName

  def qualifyEnum(enum: Enum[_]): String =
    enum.getClass.getCanonicalName + "." + enum.name()

  def internalToStringCode(t: InternalType,
                           resultTerm: String,
                           zoneTerm: String): String =
    t match {
      case DataTypes.DATE =>
        s"${qualifyMethod(BuiltInMethod.UNIX_DATE_TO_STRING.method)}($resultTerm)"
      case DataTypes.TIME =>
        s"${qualifyMethod(BuiltInMethods.UNIX_TIME_TO_STRING)}($resultTerm)"
      case _: TimestampType =>
        s"""${qualifyMethod(BuiltInMethods.TIMESTAMP_TO_STRING)}($resultTerm, 3, $zoneTerm)"""
    }

  def compareEnum(term: String, enum: Enum[_]): Boolean = term == qualifyEnum(enum)

  def getEnum(genExpr: GeneratedExpression): Enum[_] = {
    val split = genExpr.resultTerm.split('.')
    val value = split.last
    enumValueOf(genExpr.resultType.asInstanceOf[GenericType[_]].getTypeInfo.getTypeClass, value)
  }

  def enumValueOf[T <: Enum[T]](cls: Class[_], stringValue: String): Enum[_] =
    Enum.valueOf(cls.asInstanceOf[Class[T]], stringValue).asInstanceOf[Enum[_]]

  // ----------------------------------------------------------------------------------------------

  def requireNumeric(genExpr: GeneratedExpression, operatorName: String): Unit =
    if (!TypeCheckUtils.isNumeric(genExpr.resultType)) {
      throw new CodeGenException(
        TableErrors.INST.sqlCodeGenOperatorParamError(
          "Numeric expression type expected, but was " + s"'${genExpr.resultType}'.",
          operatorName))
    }

  def requireComparable(genExpr: GeneratedExpression, operatorName: String): Unit =
    if (!TypeCheckUtils.isComparable(genExpr.resultType)) {
      throw new CodeGenException(
        TableErrors.INST.sqlCodeGenOperatorParamError(
          s"Comparable type expected, but was '${genExpr.resultType}'.",
          operatorName))
    }

  def requireString(genExpr: GeneratedExpression, operatorName: String): Unit =
    if (!TypeCheckUtils.isString(genExpr.resultType)) {
      throw new CodeGenException(
        TableErrors.INST.sqlCodeGenOperatorParamError(
          "String expression type expected.",
          operatorName))
    }

  def requireBoolean(genExpr: GeneratedExpression, operatorName: String): Unit =
    if (!TypeCheckUtils.isBoolean(genExpr.resultType)) {
      throw new CodeGenException(
        TableErrors.INST.sqlCodeGenOperatorParamError(
          "Boolean expression type expected.",
          operatorName))
    }

  def requireTemporal(genExpr: GeneratedExpression, operatorName: String): Unit =
    if (!TypeCheckUtils.isTemporal(genExpr.resultType)) {
      throw new CodeGenException(
        TableErrors.INST.sqlCodeGenOperatorParamError(
          "Temporal expression type expected.",
          operatorName))
    }

  def requireTimeInterval(genExpr: GeneratedExpression, operatorName: String): Unit =
    if (!TypeCheckUtils.isTimeInterval(genExpr.resultType)) {
      throw new CodeGenException(
        TableErrors.INST.sqlCodeGenOperatorParamError(
          "Interval expression type expected.",
          operatorName))
    }

  def requireArray(genExpr: GeneratedExpression, operatorName: String): Unit =
    if (!TypeCheckUtils.isArray(genExpr.resultType)) {
      throw new CodeGenException(
        TableErrors.INST.sqlCodeGenOperatorParamError(
          "Array expression type expected.",
          operatorName))
    }

  def requireMap(genExpr: GeneratedExpression, operatorName: String): Unit =
    if (!TypeCheckUtils.isMap(genExpr.resultType)) {
      throw new CodeGenException(
        TableErrors.INST.sqlCodeGenOperatorParamError(
          "Array expression type expected.",
          operatorName))
    }

  def requireInteger(genExpr: GeneratedExpression, operatorName: String): Unit =
    if (!TypeCheckUtils.isInteger(genExpr.resultType)) {
      throw new CodeGenException(
        TableErrors.INST.sqlCodeGenOperatorParamError(
          "Integer expression type expected.",
          operatorName))
    }

  def requireList(genExpr: GeneratedExpression, operatorName: String): Unit =
    if (!TypeCheckUtils.isList(genExpr.resultType)) {
      throw new CodeGenException(
        TableErrors.INST.sqlCodeGenOperatorParamError(
          "List expression type expected.",
          operatorName))
    }

  def generateNullLiteral(
      resultType: InternalType,
      nullCheck: Boolean): GeneratedExpression = {
    val defaultValue = primitiveDefaultValue(resultType)
    val resultTypeTerm = primitiveTypeTermForType(resultType)
    if (nullCheck) {
      GeneratedExpression(
        s"(($resultTypeTerm)$defaultValue)",
        "true",
        "",
        resultType,
        literal = true)
    } else {
      throw new CodeGenException("Null literals are not allowed if nullCheck is disabled.")
    }
  }

  def generateNonNullLiteral(
      literalType: InternalType,
      literalCode: String,
      literalValue: Any,
      nullCheck: Boolean): GeneratedExpression = {
    val resultTypeTerm = primitiveTypeTermForType(literalType)
    GeneratedExpression(
      s"(($resultTypeTerm)$literalCode)",
      "false",
      "",
      literalType,
      literal = true,
      literalValue = literalValue)
  }

  def generateLiteral(
      ctx: CodeGeneratorContext,
      literalRelDataType: RelDataType,
      literalInternalType: InternalType,
      literalValue: Any,
      nullCheck: Boolean): GeneratedExpression = {
    if (literalValue == null) {
      return generateNullLiteral(literalInternalType, nullCheck)
    }
    // non-null values
    literalRelDataType.getSqlTypeName match {

      case BOOLEAN =>
        generateNonNullLiteral(literalInternalType, literalValue.toString, literalValue, nullCheck)

      case TINYINT =>
        val decimal = BigDecimal(literalValue.asInstanceOf[JBigDecimal])
        generateNonNullLiteral(
          literalInternalType,
          decimal.byteValue().toString,
          decimal.byteValue(), nullCheck)

      case SMALLINT =>
        val decimal = BigDecimal(literalValue.asInstanceOf[JBigDecimal])
        generateNonNullLiteral(
          literalInternalType,
          decimal.shortValue().toString,
          decimal.shortValue(), nullCheck)

      case INTEGER =>
        val decimal = BigDecimal(literalValue.asInstanceOf[JBigDecimal])
        generateNonNullLiteral(
          literalInternalType,
          decimal.intValue().toString,
          decimal.intValue(), nullCheck)

      case BIGINT =>
        val decimal = BigDecimal(literalValue.asInstanceOf[JBigDecimal])
        generateNonNullLiteral(
          literalInternalType,
          decimal.longValue().toString + "L",
          decimal.longValue(), nullCheck)

      case FLOAT =>
        val floatValue = literalValue.asInstanceOf[JBigDecimal].floatValue()
        floatValue match {
          case Float.NaN => generateNonNullLiteral(
            literalInternalType, "java.lang.Float.NaN", Float.NaN, nullCheck)
          case Float.NegativeInfinity =>
            generateNonNullLiteral(
              literalInternalType,
              "java.lang.Float.NEGATIVE_INFINITY",
              Float.NegativeInfinity, nullCheck)
          case Float.PositiveInfinity => generateNonNullLiteral(
            literalInternalType,
            "java.lang.Float.POSITIVE_INFINITY",
            Float.PositiveInfinity, nullCheck)
          case _ => generateNonNullLiteral(
            literalInternalType,
            floatValue.toString + "f",
            floatValue,
            nullCheck)
        }

      case DOUBLE =>
        val doubleValue = literalValue.asInstanceOf[JBigDecimal].doubleValue()
        doubleValue match {
          case Double.NaN => generateNonNullLiteral(
            literalInternalType, "java.lang.Double.NaN", Double.NaN, nullCheck)
          case Double.NegativeInfinity =>
            generateNonNullLiteral(
              literalInternalType,
              "java.lang.Double.NEGATIVE_INFINITY",
              Double.NegativeInfinity, nullCheck)
          case Double.PositiveInfinity =>
            generateNonNullLiteral(
              literalInternalType,
              "java.lang.Double.POSITIVE_INFINITY",
              Double.PositiveInfinity, nullCheck)
          case _ => generateNonNullLiteral(
            literalInternalType, doubleValue.toString + "d", doubleValue, nullCheck)
        }
      case DECIMAL =>
        val precision = literalRelDataType.getPrecision
        val scale = literalRelDataType.getScale
        val fieldTerm = newName("decimal")
        val fieldDecimal =
          s"""
             |${classOf[Decimal].getCanonicalName} $fieldTerm =
             |    ${Decimal.Ref.castFrom}("${literalValue.toString}", $precision, $scale);
             |""".stripMargin
        ctx.addReusableMember(fieldDecimal)
        generateNonNullLiteral(
          literalInternalType,
          fieldTerm,
          Decimal.fromBigDecimal(literalValue.asInstanceOf[JBigDecimal], precision, scale),
          nullCheck)

      case VARCHAR | CHAR =>
        val escapedValue = StringEscapeUtils.ESCAPE_JAVA.translate(literalValue.toString)
        val field = ctx.addReusableStringConstants(escapedValue)
        generateNonNullLiteral(
          literalInternalType,
          field,
          BinaryString.fromString(escapedValue),
          nullCheck)
      case VARBINARY | BINARY =>
        val bytesVal = literalValue.asInstanceOf[ByteString].getBytes
        val fieldTerm = ctx.addReusableObject(bytesVal, "binary",
                                              bytesVal.getClass.getCanonicalName)
        generateNonNullLiteral(
          literalInternalType,
          fieldTerm,
          BinaryString.fromBytes(bytesVal),
          nullCheck)
      case SYMBOL =>
        generateSymbol(literalValue.asInstanceOf[Enum[_]])

      case DATE =>
        generateNonNullLiteral(literalInternalType, literalValue.toString, literalValue, nullCheck)

      case TIME =>
        generateNonNullLiteral(literalInternalType, literalValue.toString, literalValue, nullCheck)

      case TIMESTAMP =>
        // Hack
        // Currently, in RexLiteral/SqlLiteral(Calcite), TimestampString has no time zone.
        // TimeString, DateString TimestampString are treated as UTC time/(unix time)
        // when they are converted/formatted/validated
        // Here, we adjust millis before Calcite solve TimeZone perfectly
        val millis = literalValue.asInstanceOf[Long]
        val adjustedValue = millis - ctx.getTableConfig.getTimeZone.getOffset(millis)
        generateNonNullLiteral(
          literalInternalType, adjustedValue.toString + "L", literalValue, nullCheck)
      case typeName if YEAR_INTERVAL_TYPES.contains(typeName) =>
        val decimal = BigDecimal(literalValue.asInstanceOf[JBigDecimal])
        if (decimal.isValidInt) {
          generateNonNullLiteral(
            literalInternalType,
            decimal.intValue().toString,
            decimal.intValue(), nullCheck)
        } else {
          throw new CodeGenException(
            s"Decimal '$decimal' can not be converted to interval of months.")
        }

      case typeName if DAY_INTERVAL_TYPES.contains(typeName) =>
        val decimal = BigDecimal(literalValue.asInstanceOf[JBigDecimal])
        if (decimal.isValidLong) {
          generateNonNullLiteral(
            literalInternalType,
            decimal.longValue().toString + "L",
            decimal.longValue(), nullCheck)
        } else {
          throw new CodeGenException(
            s"Decimal '$decimal' can not be converted to interval of milliseconds.")
        }

      case t@_ =>
        throw new CodeGenException(s"Type not supported: $t")
    }
  }

  def generateNonNullField(
      t: InternalType,
      code: String,
      nullCheck: Boolean): GeneratedExpression = {
    GeneratedExpression(s"((${primitiveTypeTermForType(t)}) $code)", "false", "", t)
  }

  def generateSymbol(enum: Enum[_]): GeneratedExpression =
    GeneratedExpression(qualifyEnum(enum), "false", "", new GenericType(enum.getDeclaringClass))

  def generateProctimeTimestamp(
    contextTerm: String,
    ctx: CodeGeneratorContext): GeneratedExpression = {
    val resultTerm = ctx.newReusableField("result", "long")
    val resultCode =
      s"""
         |$resultTerm = $contextTerm.timerService().currentProcessingTime();
         |""".stripMargin.trim
    GeneratedExpression(resultTerm, NEVER_NULL, resultCode, DataTypes.TIMESTAMP)
  }

  def generateCurrentTimestamp(
      ctx: CodeGeneratorContext): GeneratedExpression = {
    new CurrentTimePointCallGen(false).generate(ctx, Seq(), DataTypes.TIMESTAMP, false)
  }

  def generateRowtimeAccess(
      contextTerm: String,
      ctx: CodeGeneratorContext): GeneratedExpression = {
    val Seq(resultTerm, nullTerm) = ctx.newReusableFields(
      Seq("result", "isNull"),
      Seq("Long", "boolean"))

    val accessCode =
      s"""
         |$resultTerm = $contextTerm.timestamp();
         |if ($resultTerm == null) {
         |  throw new RuntimeException("Rowtime timestamp is null. Please make sure that a " +
         |    "proper TimestampAssigner is defined and the stream environment uses the EventTime " +
         |    "time characteristic.");
         |}
         |$nullTerm = false;
       """.stripMargin.trim

    GeneratedExpression(resultTerm, nullTerm, accessCode, DataTypes.ROWTIME_INDICATOR)
  }

  def generateInputAccess(
      ctx: CodeGeneratorContext,
      inputType: InternalType,
      inputTerm: String,
      index: Int,
      nullableInput: Boolean,
      nullCheck: Boolean,
      fieldCopy: Boolean = false): GeneratedExpression = {
    // if input has been used before, we can reuse the code that
    // has already been generated
    val inputExpr = ctx.getReusableInputUnboxingExprs(inputTerm, index) match {
      // input access and unboxing has already been generated
      case Some(expr) => expr

      // generate input access and unboxing if necessary
      case None =>
        val expr = if (nullableInput) {
          generateNullableInputFieldAccess(ctx, inputType, inputTerm, index, nullCheck, fieldCopy)
        } else {
          generateFieldAccess(ctx, inputType, inputTerm, index, nullCheck, fieldCopy)
        }

        ctx.addReusableInputUnboxingExprs(inputTerm, index, expr)
        expr
    }
    // hide the generated code as it will be executed only once
    GeneratedExpression(inputExpr.resultTerm, inputExpr.nullTerm, "", inputExpr.resultType)
  }

  /**
    * Generates field access code expression. The different between this method and
    * [[generateFieldAccess(ctx, inputType, inputTerm, index, nullCheck)]] is that this method
    * accepts an additional `fieldCopy` parameter. When copyResult is set to true, the returned
    * result will be copied.
    *
    * NOTE: Please set `fieldCopy` to true when the result will be buffered.
    */
  def generateFieldAccess(
      ctx: CodeGeneratorContext,
      inputType: InternalType,
      inputTerm: String,
      index: Int,
      nullCheck: Boolean,
      fieldCopy: Boolean): GeneratedExpression = {
    val expr = generateFieldAccess(ctx, inputType, inputTerm, index, nullCheck)
    if (fieldCopy) {
      expr.copyResultIfNeeded(ctx, fieldCopy)
    } else {
      expr
    }
  }

  def generateFieldAccess(
      ctx: CodeGeneratorContext,
      inputType: InternalType,
      inputTerm: String,
      index: Int,
      nullCheck: Boolean): GeneratedExpression =
    inputType match {
      case ct: RowType =>
        val fieldType = ct.getFieldTypes()(index).toInternalType
        val resultTypeTerm = primitiveTypeTermForType(fieldType)
        val defaultValue = primitiveDefaultValue(fieldType)
        val readCode = baseRowFieldReadAccess(ctx, index.toString, inputTerm, fieldType)
        val Seq(fieldTerm, nullTerm) = ctx.newReusableFields(
          Seq("field", "isNull"),
          Seq(resultTypeTerm, "boolean"))
        val inputCode = if (nullCheck) {
          s"""
             |$nullTerm = $inputTerm.isNullAt($index);
             |$fieldTerm = $defaultValue;
             |if (!$nullTerm) {
             |  $fieldTerm = $readCode;
             |}
           """.stripMargin.trim
        } else {
          s"""
             |$nullTerm = false;
             |$fieldTerm = $readCode;
           """.stripMargin
        }
        GeneratedExpression(fieldTerm, nullTerm, inputCode, fieldType)

      case _ =>
        val fieldTypeTerm = boxedTypeTermForType(inputType)
        val inputCode = s"($fieldTypeTerm) $inputTerm"
        generateInputFieldUnboxing(inputType, inputCode, nullCheck, ctx)
    }

  def generateNullableInputFieldAccess(
      ctx: CodeGeneratorContext,
      inputType: InternalType,
      inputTerm: String,
      index: Int,
      nullCheck: Boolean,
      fieldCopy: Boolean = false): GeneratedExpression = {

    val fieldType = inputType match {
      case ct: RowType => ct.getFieldTypes()(index).toInternalType
      case _ => inputType
    }
    val resultTypeTerm = primitiveTypeTermForType(fieldType)
    val defaultValue = primitiveDefaultValue(fieldType)

    val Seq(resultTerm, nullTerm) = ctx.newReusableFields(
      Seq("result", "isNull"),
      Seq(resultTypeTerm, "boolean"))
    val fieldAccessExpr = generateFieldAccess(
      ctx, inputType, inputTerm, index, nullCheck, fieldCopy)

    val inputCheckCode =
      s"""
         |$resultTerm = $defaultValue;
         |$nullTerm = true;
         |if ($inputTerm != null) {
         |  ${fieldAccessExpr.code}
         |  $resultTerm = ${fieldAccessExpr.resultTerm};
         |  $nullTerm = ${fieldAccessExpr.nullTerm};
         |}
         |""".stripMargin.trim

    GeneratedExpression(resultTerm, nullTerm, inputCheckCode, fieldType)
  }

  /**
   * Converts the external boxed format to an internal mostly primitive field representation.
   * Wrapper types can autoboxed to their corresponding primitive type (Integer -> int).
   *
   * @param fieldType type of field
   * @param fieldTerm expression term of field to be unboxed
   * @param nullCheck whether to check null
   * @return internal unboxed field representation
   */
  def generateInputFieldUnboxing(
      fieldType: InternalType,
      fieldTerm: String,
      nullCheck: Boolean,
      ctx: CodeGeneratorContext): GeneratedExpression = {

    val resultTypeTerm = primitiveTypeTermForType(fieldType)
    val defaultValue = primitiveDefaultValue(fieldType)

    val Seq(resultTerm, nullTerm) = ctx.newReusableFields(
      Seq("result", "isNull"),
      Seq(resultTypeTerm, "boolean"))

    val wrappedCode = if (nullCheck) {
      s"""
         |$nullTerm = $fieldTerm == null;
         |$resultTerm = $defaultValue;
         |if (!$nullTerm) {
         |  $resultTerm = $fieldTerm;
         |}
         |""".stripMargin.trim
    } else {
      s"""
         |$resultTerm = $fieldTerm;
         |""".stripMargin.trim
    }

    GeneratedExpression(resultTerm, nullTerm, wrappedCode, fieldType)
  }

  def generateCallExpression(
      ctx: CodeGeneratorContext,
      operator: SqlOperator,
      operands: Seq[GeneratedExpression],
      resultType: InternalType,
      nullCheck: Boolean): GeneratedExpression = {
    operator match {
      // arithmetic
      case PLUS if isNumeric(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumeric(left, operator.getName)
        requireNumeric(right, operator.getName)
        generateArithmeticOperator(ctx, "+", nullCheck, resultType, left, right)

      case PLUS | DATETIME_PLUS if isTemporal(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireTemporal(left, operator.getName)
        requireTemporal(right, operator.getName)
        generateTemporalPlusMinus(ctx, plus = true, nullCheck, resultType, left, right)

      case MINUS if isNumeric(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumeric(left, operator.getName)
        requireNumeric(right, operator.getName)
        generateArithmeticOperator(ctx, "-", nullCheck, resultType, left, right)

      case MINUS | MINUS_DATE if isTemporal(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireTemporal(left, operator.getName)
        requireTemporal(right, operator.getName)
        generateTemporalPlusMinus(ctx, plus = false, nullCheck, resultType, left, right)

      case MULTIPLY if isNumeric(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumeric(left, operator.getName)
        requireNumeric(right, operator.getName)
        generateArithmeticOperator(ctx, "*", nullCheck, resultType, left, right)

      case MULTIPLY if isTimeInterval(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireTimeInterval(left, operator.getName)
        requireNumeric(right, operator.getName)
        generateArithmeticOperator(ctx, "*", nullCheck, resultType, left, right)

      case ScalarSqlFunctions.DIVIDE | DIVIDE_INTEGER if isNumeric(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumeric(left, operator.getName)
        requireNumeric(right, operator.getName)
        generateArithmeticOperator(ctx, "/", nullCheck, resultType, left, right)

      case MOD if isNumeric(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumeric(left, operator.getName)
        requireNumeric(right, operator.getName)
        generateArithmeticOperator(ctx, "%", nullCheck, resultType, left, right)

      case UNARY_MINUS if isNumeric(resultType) =>
        val operand = operands.head
        requireNumeric(operand, operator.getName)
        generateUnaryArithmeticOperator(ctx, "-", nullCheck, resultType, operand)

      case UNARY_MINUS if isTimeInterval(resultType) =>
        val operand = operands.head
        requireTimeInterval(operand, operator.getName)
        generateUnaryIntervalPlusMinus(ctx, plus = false, nullCheck, operand)

      case UNARY_PLUS if isNumeric(resultType) =>
        val operand = operands.head
        requireNumeric(operand, operator.getName)
        generateUnaryArithmeticOperator(ctx, "+", nullCheck, resultType, operand)

      case UNARY_PLUS if isTimeInterval(resultType) =>
        val operand = operands.head
        requireTimeInterval(operand, operator.getName)
        generateUnaryIntervalPlusMinus(ctx, plus = true, nullCheck, operand)

      // comparison
      case EQUALS =>
        val left = operands.head
        val right = operands(1)
        generateEquals(ctx, nullCheck, left, right)

      case NOT_EQUALS =>
        val left = operands.head
        val right = operands(1)
        generateNotEquals(ctx, nullCheck, left, right)

      case GREATER_THAN =>
        val left = operands.head
        val right = operands(1)
        requireComparable(left, operator.getName)
        requireComparable(right, operator.getName)
        generateComparison(ctx, ">", nullCheck, left, right)

      case GREATER_THAN_OR_EQUAL =>
        val left = operands.head
        val right = operands(1)
        requireComparable(left, operator.getName)
        requireComparable(right, operator.getName)
        generateComparison(ctx, ">=", nullCheck, left, right)

      case LESS_THAN =>
        val left = operands.head
        val right = operands(1)
        requireComparable(left, operator.getName)
        requireComparable(right, operator.getName)
        generateComparison(ctx, "<", nullCheck, left, right)

      case LESS_THAN_OR_EQUAL =>
        val left = operands.head
        val right = operands(1)
        requireComparable(left, operator.getName)
        requireComparable(right, operator.getName)
        generateComparison(ctx, "<=", nullCheck, left, right)

      case IS_NULL =>
        val operand = operands.head
        generateIsNull(nullCheck, operand)

      case IS_NOT_NULL =>
        val operand = operands.head
        generateIsNotNull(nullCheck, operand)

      // logic
      case AND =>
        operands.reduceLeft { (left: GeneratedExpression, right: GeneratedExpression) =>
          requireBoolean(left, operator.getName)
          requireBoolean(right, operator.getName)
          generateAnd(nullCheck, left, right)
        }

      case OR =>
        operands.reduceLeft { (left: GeneratedExpression, right: GeneratedExpression) =>
          requireBoolean(left, operator.getName)
          requireBoolean(right, operator.getName)
          generateOr(nullCheck, left, right)
        }

      case NOT =>
        val operand = operands.head
        requireBoolean(operand, operator.getName)
        generateNot(ctx, nullCheck, operand)

      case CASE =>
        generateIfElse(ctx, nullCheck, operands, resultType)

      case IS_TRUE =>
        val operand = operands.head
        requireBoolean(operand, operator.getName)
        generateIsTrue(operand)

      case IS_NOT_TRUE =>
        val operand = operands.head
        requireBoolean(operand, operator.getName)
        generateIsNotTrue(operand)

      case IS_FALSE =>
        val operand = operands.head
        requireBoolean(operand, operator.getName)
        generateIsFalse(operand)

      case IS_NOT_FALSE =>
        val operand = operands.head
        requireBoolean(operand, operator.getName)
        generateIsNotFalse(operand)

      case IN =>
        val left = operands.head
        val right = operands.tail
        generateIn(ctx, left, right, nullCheck)

      case NOT_IN =>
        val left = operands.head
        val right = operands.tail
        generateNot(ctx, nullCheck, generateIn(ctx, left, right, nullCheck))

      // casting
      case CAST =>
        val operand = operands.head
        generateCast(ctx, nullCheck, operand, resultType)

      // Reinterpret
      case REINTERPRET =>
        val operand = operands.head
        generateReinterpret(ctx, nullCheck, operand, resultType)

      // as / renaming
      case AS =>
        operands.head

      // rows
      case ROW =>
        generateRow(ctx, resultType, operands, nullCheck)

      // arrays
      case ARRAY_VALUE_CONSTRUCTOR =>
        generateArray(ctx, resultType, operands, nullCheck)

      // maps
      case MAP_VALUE_CONSTRUCTOR =>
        generateMap(ctx, resultType, operands, nullCheck)

      case ITEM =>
        operands.head.resultType match {
          case t: InternalType if TypeCheckUtils.isArray(t) =>
            val array = operands.head
            val index = operands(1)
            requireInteger(index, operator.getName)
            generateArrayElementAt(ctx, array, index, nullCheck)

          case t: InternalType if TypeCheckUtils.isMap(t) =>
            val key = operands(1)
            generateMapGet(ctx, operands.head, key, nullCheck)

          case _ => throw new CodeGenException("Expect an array or a map.")
        }

      case CARDINALITY =>
        operands.head.resultType match {
          case t: InternalType if TypeCheckUtils.isArray(t) =>
            val array = operands.head
            generateArrayCardinality(ctx, nullCheck, array)

          case t: InternalType if TypeCheckUtils.isMap(t) =>
            val map = operands.head
            generateMapCardinality(ctx, nullCheck, map)

          case _ => throw new CodeGenException("Expect an array or a map.")
        }

      case ELEMENT =>
        val array = operands.head
        requireArray(array, operator.getName)
        generateArrayElement(ctx, array, nullCheck)

      case DOT =>
        generateDOT(ctx, operands, nullCheck)

      case func: SqlRuntimeFilterFunction => generateRuntimeFilter(ctx, operands, func)

      case func: SqlRuntimeFilterBuilderFunction =>
        generateRuntimeFilterBuilder(ctx, operands, func)

      case _: SqlThrowExceptionFunction =>
        val nullValue = generateNullLiteral(resultType, nullCheck)
        val code =
          s"""
             |${nullValue.code}
             |org.apache.flink.util.ExceptionUtils.rethrow(
             |  new RuntimeException(${operands.head.resultTerm}.toString()));
             |""".stripMargin
        GeneratedExpression(nullValue.resultTerm, nullValue.nullTerm, code, resultType)

      case ScalarSqlFunctions.PROCTIME =>
        // attribute is proctime indicator.
        // We use a null literal and generate a timestamp when we need it.
        generateNullLiteral(DataTypes.PROCTIME_INDICATOR, nullCheck)

      // advanced scalar functions
      case sqlOperator: SqlOperator =>
        BinaryStringCallGen.generateCallExpression(ctx, operator, operands, resultType).getOrElse{
          FunctionGenerator.getCallGenerator(
            sqlOperator,
            operands.map(expr => expr.resultType),
            resultType).getOrElse(
            throw new CodeGenException(TableErrors.INST.sqlCodeGenUnsupportedScalaFunc(
              s"$sqlOperator(${operands.map(_.resultType).mkString(",")})")))
            .generate(ctx, operands, resultType, nullCheck)
        }

      // unknown or invalid
      case call@_ =>
        throw new CodeGenException(
          TableErrors.INST.sqlCodeGenUnsupportedCall(
            s"$call${operands.map(_.resultType).mkString(",")}"))
    }
  }

  // ----------------------------------------------------------------------------------------------

  def isReference(genExpr: GeneratedExpression): Boolean = isReference(genExpr.resultType)

  def isReference(t: InternalType): Boolean = t match {
    case DataTypes.INT
         | DataTypes.LONG
         | DataTypes.SHORT
         | DataTypes.BYTE
         | DataTypes.FLOAT
         | DataTypes.DOUBLE
         | DataTypes.BOOLEAN
         | DataTypes.CHAR => false
    case _ => true
  }

  def baseRowFieldReadAccess(
      ctx: CodeGeneratorContext,
      pos: Int,
      rowTerm: String,
      fieldType: InternalType) : String =
    baseRowFieldReadAccess(ctx, pos.toString, rowTerm, fieldType)

  def baseRowFieldReadAccess(
      ctx: CodeGeneratorContext,
      pos: String,
      rowTerm: String,
      fieldType: InternalType) : String =
    fieldType match {
      case DataTypes.INT => s"$rowTerm.getInt($pos)"
      case DataTypes.LONG => s"$rowTerm.getLong($pos)"
      case DataTypes.SHORT => s"$rowTerm.getShort($pos)"
      case DataTypes.BYTE => s"$rowTerm.getByte($pos)"
      case DataTypes.FLOAT => s"$rowTerm.getFloat($pos)"
      case DataTypes.DOUBLE => s"$rowTerm.getDouble($pos)"
      case DataTypes.BOOLEAN => s"$rowTerm.getBoolean($pos)"
      case DataTypes.STRING =>
        val reuse = newName("reuseBString")
        ctx.addReusableMember(s"$BINARY_STRING $reuse = new $BINARY_STRING();")
        s"$rowTerm.getBinaryString($pos, $reuse)"
      case dt: DecimalType => s"$rowTerm.getDecimal($pos, ${dt.precision()}, ${dt.scale()})"
      case DataTypes.CHAR => s"$rowTerm.getChar($pos)"
      case _: TimestampType => s"$rowTerm.getLong($pos)"
      case _: DateType => s"$rowTerm.getInt($pos)"
      case DataTypes.TIME => s"$rowTerm.getInt($pos)"
      case DataTypes.BYTE_ARRAY => s"$rowTerm.getByteArray($pos)"
      case _: ArrayType => s"$rowTerm.getBaseArray($pos)"
      case _: MapType  => s"$rowTerm.getBaseMap($pos)"
      case rt: RowType =>
        s"$rowTerm.getBaseRow($pos, ${rt.getArity})"

      case gt: GenericType[_] =>
        s"""
           |(${gt.getTypeClass.getCanonicalName})
           |  $rowTerm.getGeneric($pos, ${ctx.addReusableTypeSerializer(fieldType)})
         """.stripMargin.trim
    }

  def binaryWriterWriteNull(pos: Int, writerTerm: String, t: InternalType): String = t match {
    case d: DecimalType if !Decimal.isCompact(d.precision()) =>
      s"$writerTerm.writeDecimal($pos, null, ${d.precision()}, ${d.scale()})"
    case _ => s"$writerTerm.setNullAt($pos)"
  }

  def binaryRowSetNull(pos: Int, rowTerm: String, t: InternalType): String = t match {
    case d: DecimalType if !Decimal.isCompact(d.precision()) =>
      s"$rowTerm.setDecimal($pos, null, ${d.precision()}, ${d.scale()})"
    case _ => s"$rowTerm.setNullAt($pos)"
  }

  def binaryRowFieldSetAccess(
      pos: Int,
      binaryRowTerm: String,
      fieldType: InternalType,
      fieldValTerm: String)
    : String =
    fieldType match {
      case DataTypes.INT => s"$binaryRowTerm.setInt($pos, $fieldValTerm)"
      case DataTypes.LONG => s"$binaryRowTerm.setLong($pos, $fieldValTerm)"
      case DataTypes.SHORT => s"$binaryRowTerm.setShort($pos, $fieldValTerm)"
      case DataTypes.BYTE => s"$binaryRowTerm.setByte($pos, $fieldValTerm)"
      case DataTypes.FLOAT => s"$binaryRowTerm.setFloat($pos, $fieldValTerm)"
      case DataTypes.DOUBLE => s"$binaryRowTerm.setDouble($pos, $fieldValTerm)"
      case DataTypes.BOOLEAN => s"$binaryRowTerm.setBoolean($pos, $fieldValTerm)"
      case DataTypes.CHAR =>  s"$binaryRowTerm.setChar($pos, $fieldValTerm)"
      case _: DateType =>  s"$binaryRowTerm.setInt($pos, $fieldValTerm)"
      case DataTypes.TIME =>  s"$binaryRowTerm.setInt($pos, $fieldValTerm)"
      case _: TimestampType =>  s"$binaryRowTerm.setLong($pos, $fieldValTerm)"
      case d: DecimalType =>
        s"$binaryRowTerm.setDecimal($pos, $fieldValTerm, ${d.precision()}, ${d.scale()})"
      case _ =>
        throw new CodeGenException("Fail to find binary row field setter method of InternalType "
            + fieldType + ".")
    }

  def binaryWriterWriteField(
      ctx: CodeGeneratorContext,
      pos: Int,
      fieldValTerm: String,
      writerTerm: String,
      fieldType: InternalType): String =
    fieldType match {
      case DataTypes.INT => s"$writerTerm.writeInt($pos, $fieldValTerm)"
      case DataTypes.LONG => s"$writerTerm.writeLong($pos, $fieldValTerm)"
      case DataTypes.SHORT => s"$writerTerm.writeShort($pos, $fieldValTerm)"
      case DataTypes.BYTE => s"$writerTerm.writeByte($pos, $fieldValTerm)"
      case DataTypes.FLOAT => s"$writerTerm.writeFloat($pos, $fieldValTerm)"
      case DataTypes.DOUBLE => s"$writerTerm.writeDouble($pos, $fieldValTerm)"
      case DataTypes.BOOLEAN => s"$writerTerm.writeBoolean($pos, $fieldValTerm)"
      case DataTypes.STRING => s"$writerTerm.writeBinaryString($pos, $fieldValTerm)"
      case d: DecimalType =>
        s"$writerTerm.writeDecimal($pos, $fieldValTerm, ${d.precision()}, ${d.scale()})"
      case DataTypes.CHAR => s"$writerTerm.writeChar($pos, $fieldValTerm)"
      case _: DateType => s"$writerTerm.writeInt($pos, $fieldValTerm)"
      case DataTypes.TIME => s"$writerTerm.writeInt($pos, $fieldValTerm)"
      case _: TimestampType => s"$writerTerm.writeLong($pos, $fieldValTerm)"
      case DataTypes.BYTE_ARRAY => s"$writerTerm.writeByteArray($pos, $fieldValTerm)"
      case _: ArrayType =>
        s"$BASE_ROW_UTIL.writeBaseArray($writerTerm, $pos, $fieldValTerm, " +
          s"(${classOf[BaseArraySerializer].getCanonicalName}) " +
            s"${ctx.addReusableTypeSerializer(fieldType)})"

      case _: MapType =>
        s"$BASE_ROW_UTIL.writeBaseMap($writerTerm, $pos, $fieldValTerm, " +
          s"(${classOf[BaseMapSerializer].getCanonicalName}) " +
          s"${ctx.addReusableTypeSerializer(fieldType)})"

      case _: RowType =>
        s"$BASE_ROW_UTIL.writeBaseRow($writerTerm, $pos, $fieldValTerm, " +
          s"(${classOf[BaseRowSerializer[_]].getCanonicalName}) " +
          s"${ctx.addReusableTypeSerializer(fieldType)})"

      case _: GenericType[_] => s"$writerTerm.writeGeneric($pos, $fieldValTerm, " +
        s"${ctx.addReusableTypeSerializer(fieldType)})"
    }

  def baseArraySetNull(
      pos: Int,
      term: String,
      t: InternalType): String = t match {
    case DataTypes.BOOLEAN => s"$term.setNullBoolean($pos)"
    case DataTypes.BYTE => s"$term.setNullByte($pos)"
    case DataTypes.CHAR => s"$term.setNullChar($pos)"
    case DataTypes.SHORT => s"$term.setNullShort($pos)"
    case DataTypes.INT => s"$term.setNullInt($pos)"
    case DataTypes.LONG => s"$term.setNullLong($pos)"
    case DataTypes.FLOAT => s"$term.setNullFloat($pos)"
    case DataTypes.DOUBLE => s"$term.setNullDouble($pos)"
    case DataTypes.TIME => s"$term.setNullInt($pos)"
    case _: DateType => s"$term.setNullInt($pos)"
    case _: TimestampType => s"$term.setNullLong($pos)"
    case _ => s"$term.setNullLong($pos)"
  }

  def boxedWrapperRowFieldUpdateAccess(
      pos: Int,
      fieldValTerm: String,
      rowTerm: String,
      fieldType: InternalType): String =
    fieldType match {
      case DataTypes.INT => s"$rowTerm.setInt($pos, $fieldValTerm)"
      case DataTypes.LONG => s"$rowTerm.setLong($pos, $fieldValTerm)"
      case DataTypes.SHORT => s"$rowTerm.setShort($pos, $fieldValTerm)"
      case DataTypes.BYTE => s"$rowTerm.setByte($pos, $fieldValTerm)"
      case DataTypes.FLOAT => s"$rowTerm.setFloat($pos, $fieldValTerm)"
      case DataTypes.DOUBLE => s"$rowTerm.setDouble($pos, $fieldValTerm)"
      case DataTypes.BOOLEAN => s"$rowTerm.setBoolean($pos, $fieldValTerm)"
      case DataTypes.CHAR =>  s"$rowTerm.setChar($pos, $fieldValTerm)"
      case _: DateType =>  s"$rowTerm.setInt($pos, $fieldValTerm)"
      case DataTypes.TIME =>  s"$rowTerm.setInt($pos, $fieldValTerm)"
      case _: TimestampType =>  s"$rowTerm.setLong($pos, $fieldValTerm)"
      case _ => s"$rowTerm.setNonPrimitiveValue($pos, $fieldValTerm)"
    }

  // ----------------------------------------------------------------------------------------------

  @throws(classOf[CompileException])
  def compile[T](cl: ClassLoader, name: String, code: String): Class[T] = {
    CODE_LOG.debug(s"Compiling: $name \n\n Code:\n$code")
    require(cl != null, "Classloader must not be null.")
    val compiler = new SimpleCompiler()
    compiler.setParentClassLoader(cl)
    try {
      compiler.cook(code)
    } catch {
      case t: Throwable =>
        println(CodeFormatter.format(code))
        throw new InvalidProgramException("Table program cannot be compiled. " +
            "This is a bug. Please file an issue.", t)
    }
    compiler.getClassLoader.loadClass(name).asInstanceOf[Class[T]]
  }

  /**
    * enable code generate debug for janino
    * like "gcc -g"
    */
  def enableCodeGenerateDebug(): Unit = {
    System.setProperty(ICookable.SYSTEM_PROPERTY_SOURCE_DEBUGGING_ENABLE, "true")
  }

  def disableCodeGenerateDebug(): Unit = {
    System.setProperty(ICookable.SYSTEM_PROPERTY_SOURCE_DEBUGGING_ENABLE, "false")
  }

  def setCodeGenerateTmpDir(path: String): Unit = {
    if (!StringUtils.isNullOrWhitespaceOnly(path)) {
      System.setProperty(ICookable.SYSTEM_PROPERTY_SOURCE_DEBUGGING_DIR, path)
    } else {
      throw new RuntimeException("code generate tmp dir can't be empty")
    }
  }

  // ----------------------------------------------------------------------------------------------

  def genLogInfo(logTerm: String, format: String, argTerm: String): String =
    s"""$logTerm.info("$format", $argTerm);"""

  /**
    *
    * @param codeBuffer sequence of code need to be split
    * @param limitLength maxLength of split code
    * @param subFunctionName name of function which code will be split into
    * @param subFunctionModifier modifier of function which code will be split into
    * @param defineParams  params for function definition
    * @param callingParams params for function call
    * @return
    */
  def generateSplitFunctionCalls(
    codeBuffer: Seq[String],
    limitLength: Int,
    subFunctionName: String,
    subFunctionModifier: String,
    fieldStatementLength: Int,
    defineParams: String = "",
    callingParams: String = ""): GeneratedSplittableExpression = {

    val bodies = new ListBuffer[String]()
    val rest = codeBuffer.foldLeft("")((acc, code) => {
      if (acc.length + code.length <= limitLength) {
        if (acc.length > 0) {
          acc + "\n" + code
        } else {
          code
        }
      } else {
        if (acc.length > 0) {
          bodies += acc
        }
        code
      }
    })
    bodies += rest

    val defines = bodies.indices
      .map(index => s"$subFunctionModifier ${subFunctionName}_$index($defineParams)")

    val callings = bodies.indices
      .map(index => s"${subFunctionName}_$index($callingParams);")

    val isSplit = (defines.length > 1) ||
      (defines.length == 1 && codeBuffer.map(_.length).sum + fieldStatementLength > limitLength)

    GeneratedSplittableExpression(defines, bodies, callings, isSplit)
  }

  def getDefineParamsByFunctionClass(clazz: Class[_]): String = {
    if (clazz == classOf[FlatMapFunction[_, _]]) {
      s"Object _in1, " +
        s"org.apache.flink.util.Collector ${CodeGeneratorContext.DEFAULT_COLLECTOR_TERM}"
    } else if (clazz == classOf[MapFunction[_, _]]) {
      s"Object _in1"
    } else if (clazz == classOf[FlatJoinFunction[_, _, _]]) {
      s"Object _in1, Object _in2, " +
        s"org.apache.flink.util.Collector ${CodeGeneratorContext.DEFAULT_COLLECTOR_TERM}"
    } else if (clazz == classOf[ProcessFunction[_, _]]) {
      "Object _in1, org.apache.flink.streaming.api.functions.ProcessFunction.Context " +
        s"${CodeGeneratorContext.DEFAULT_CONTEXT_TERM}, org.apache.flink.util.Collector " +
        s"${CodeGeneratorContext.DEFAULT_COLLECTOR_TERM}"
    } else {
      ""
    }
  }

  def getCallingParamsByFunctionClass(clazz: Class[_]): String = {
    if (clazz == classOf[FlatMapFunction[_, _]]) {
      s"${CodeGeneratorContext.DEFAULT_INPUT1_TERM}, " +
        s"${CodeGeneratorContext.DEFAULT_COLLECTOR_TERM}"
    } else if (clazz == classOf[MapFunction[_, _]]) {
      s"${CodeGeneratorContext.DEFAULT_INPUT1_TERM}"
    } else if (clazz == classOf[FlatJoinFunction[_, _, _]]) {
      s"${CodeGeneratorContext.DEFAULT_INPUT1_TERM}, " +
        s"${CodeGeneratorContext.DEFAULT_INPUT2_TERM}, " +
        s"${CodeGeneratorContext.DEFAULT_COLLECTOR_TERM}"
    } else if (clazz == classOf[ProcessFunction[_, _]]) {
      s"${CodeGeneratorContext.DEFAULT_INPUT1_TERM}, " +
        s"${CodeGeneratorContext.DEFAULT_CONTEXT_TERM}, " +
        s"${CodeGeneratorContext.DEFAULT_COLLECTOR_TERM}"
    } else {
      ""
    }
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
}
