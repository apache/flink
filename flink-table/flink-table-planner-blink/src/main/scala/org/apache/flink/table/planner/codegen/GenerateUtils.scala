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

import java.math.{BigDecimal => JBigDecimal}
import java.time.ZoneOffset

import org.apache.calcite.avatica.util.ByteString
import org.apache.calcite.util.TimestampString
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.{AtomicType => AtomicTypeInfo}
import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.apache.flink.table.data._
import org.apache.flink.table.data.binary.BinaryRowData
import org.apache.flink.table.data.utils.JoinedRowData
import org.apache.flink.table.data.writer.BinaryRowWriter
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.codegen.GeneratedExpression.{ALWAYS_NULL, NEVER_NULL, NO_CODE}
import org.apache.flink.table.planner.codegen.calls.CurrentTimePointCallGen
import org.apache.flink.table.planner.plan.utils.SortUtil
import org.apache.flink.table.runtime.typeutils.TypeCheckUtils.{isCharacterString, isReference, isTemporal}
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.table.types.logical._
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks.{getFieldCount, getFieldTypes}
import org.apache.flink.table.util.TimestampStringUtils.toLocalDateTime

import scala.annotation.tailrec
import scala.collection.mutable

/**
  * Utilities to generate code for general purpose.
  */
object GenerateUtils {

  // ----------------------------------------------------------------------------------------
  // basic call generate utils
  // ----------------------------------------------------------------------------------------

  /**
    * Generates a call with a single result statement.
    */
  def generateCallIfArgsNotNull(
      ctx: CodeGeneratorContext,
      returnType: LogicalType,
      operands: Seq[GeneratedExpression],
      resultNullable: Boolean = false)
      (call: Seq[String] => String): GeneratedExpression = {
    generateCallWithStmtIfArgsNotNull(ctx, returnType, operands, resultNullable) {
      args => ("", call(args))
    }
  }

  /**
    * Generates a call with auxiliary statements and result expression.
    */
  def generateCallWithStmtIfArgsNotNull(
      ctx: CodeGeneratorContext,
      returnType: LogicalType,
      operands: Seq[GeneratedExpression],
      resultNullable: Boolean = false)
      (call: Seq[String] => (String, String)): GeneratedExpression = {
    val resultTypeTerm = if (resultNullable) {
      boxedTypeTermForType(returnType)
    } else {
      primitiveTypeTermForType(returnType)
    }
    val nullTerm = ctx.addReusableLocalVariable("boolean", "isNull")
    val resultTerm = ctx.addReusableLocalVariable(resultTypeTerm, "result")
    val defaultValue = primitiveDefaultValue(returnType)
    val isResultNullable = resultNullable || (isReference(returnType) && !isTemporal(returnType))
    val nullTermCode = if (ctx.nullCheck && isResultNullable) {
      s"$nullTerm = ($resultTerm == null);"
    } else {
      ""
    }

    val (stmt, result) = call(operands.map(_.resultTerm))

    val resultCode = if (ctx.nullCheck && operands.nonEmpty) {
      s"""
         |${operands.map(_.code).mkString("\n")}
         |$nullTerm = ${operands.map(_.nullTerm).mkString(" || ")};
         |$resultTerm = $defaultValue;
         |if (!$nullTerm) {
         |  $stmt
         |  $resultTerm = $result;
         |  $nullTermCode
         |}
         |""".stripMargin
    } else if (ctx.nullCheck && operands.isEmpty) {
      s"""
         |${operands.map(_.code).mkString("\n")}
         |$nullTerm = false;
         |$stmt
         |$resultTerm = $result;
         |$nullTermCode
         |""".stripMargin
    } else {
      s"""
         |$nullTerm = false;
         |${operands.map(_.code).mkString("\n")}
         |$stmt
         |$resultTerm = $result;
         |""".stripMargin
    }

    GeneratedExpression(resultTerm, nullTerm, resultCode, returnType)
  }

  /**
    * Generates a string result call with a single result statement.
    * This will convert the String result to BinaryStringData.
    */
  def generateStringResultCallIfArgsNotNull(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression])
      (call: Seq[String] => String): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, new VarCharType(VarCharType.MAX_LENGTH), operands) {
      args => s"$BINARY_STRING.fromString(${call(args)})"
    }
  }


  /**
    * Generates a string result call with auxiliary statements and result expression.
    * This will convert the String result to BinaryStringData.
    */
  def generateStringResultCallWithStmtIfArgsNotNull(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression])
      (call: Seq[String] => (String, String)): GeneratedExpression = {
    generateCallWithStmtIfArgsNotNull(ctx, new VarCharType(VarCharType.MAX_LENGTH), operands) {
      args =>
        val (stmt, result) = call(args)
        (stmt, s"$BINARY_STRING.fromString($result)")
    }
  }

  /**
    * Generates a call with the nullable args.
    */
  def generateCallIfArgsNullable(
      ctx: CodeGeneratorContext,
      returnType: LogicalType,
      operands: Seq[GeneratedExpression],
      resultNullable: Boolean = false)
      (call: Seq[String] => String): GeneratedExpression = {
    val resultTypeTerm = if (resultNullable) {
      boxedTypeTermForType(returnType)
    } else {
      primitiveTypeTermForType(returnType)
    }
    val defaultValue = primitiveDefaultValue(returnType)
    val nullTerm = ctx.addReusableLocalVariable("boolean", "isNull")
    val resultTerm = ctx.addReusableLocalVariable(resultTypeTerm, "result")
    val isResultNullable = resultNullable || (isReference(returnType) && !isTemporal(returnType))
    val nullTermCode = if (ctx.nullCheck && isResultNullable) {
      s"$nullTerm = ($resultTerm == null);"
    } else {
      s"$nullTerm = false;"
    }

    // TODO: should we also consider other types?
    val parameters = operands.map(x =>
      if (isCharacterString(x.resultType)){
        "( " + x.nullTerm + " ) ? null : (" + x.resultTerm + ")"
      } else {
        x.resultTerm
      })

    val resultCode = if (ctx.nullCheck) {
      s"""
         |${operands.map(_.code).mkString("\n")}
         |$resultTerm = ${call(parameters)};
         |$nullTermCode
         |if ($nullTerm) {
         |  $resultTerm = $defaultValue;
         |}
       """.stripMargin
    } else {
      s"""
         |${operands.map(_.code).mkString("\n")}
         |$resultTerm = ${call(parameters)};
         |$nullTermCode
       """.stripMargin
    }


    GeneratedExpression(resultTerm, nullTerm, resultCode, returnType)
  }

  // --------------------------- General Generate Utils ----------------------------------

  /**
    * Generates a record declaration statement, and add it to reusable member. The record
    * can be any type of RowData or other types.
    *
    * @param t  the record type
    * @param clazz  the specified class of the type (only used when RowType)
    * @param recordTerm the record term to be declared
    * @param recordWriterTerm the record writer term (only used when BinaryRowData type)
    * @param ctx the code generator context
    * @return the record initialization statement
   */
  @tailrec
  def generateRecordStatement(
      t: LogicalType,
      clazz: Class[_],
      recordTerm: String,
      recordWriterTerm: Option[String] = None,
      ctx: CodeGeneratorContext)
    : String = t.getTypeRoot match {
    // ordered by type root definition
    case ROW | STRUCTURED_TYPE if clazz == classOf[BinaryRowData] =>
      val writerTerm = recordWriterTerm.getOrElse(
        throw new CodeGenException("No writer is specified when writing BinaryRowData record.")
      )
      val binaryRowWriter = className[BinaryRowWriter]
      val typeTerm = clazz.getCanonicalName
      ctx.addReusableMember(s"$typeTerm $recordTerm = new $typeTerm(${getFieldCount(t)});")
      ctx.addReusableMember(
        s"$binaryRowWriter $writerTerm = new $binaryRowWriter($recordTerm);")
      s"""
         |$recordTerm = new $typeTerm(${getFieldCount(t)});
         |$writerTerm = new $binaryRowWriter($recordTerm);
         |""".stripMargin.trim
    case ROW | STRUCTURED_TYPE if clazz == classOf[GenericRowData] ||
        clazz == classOf[BoxedWrapperRowData] =>
      val typeTerm = clazz.getCanonicalName
      ctx.addReusableMember(s"$typeTerm $recordTerm = new $typeTerm(${getFieldCount(t)});")
      s"$recordTerm = new $typeTerm(${getFieldCount(t)});"
    case ROW | STRUCTURED_TYPE if clazz == classOf[JoinedRowData] =>
      val typeTerm = clazz.getCanonicalName
      ctx.addReusableMember(s"$typeTerm $recordTerm = new $typeTerm();")
      s"$recordTerm = new $typeTerm();"
    case DISTINCT_TYPE =>
      generateRecordStatement(
        t.asInstanceOf[DistinctType].getSourceType,
        clazz,
        recordTerm,
        recordWriterTerm,
        ctx)
    case _ =>
      val typeTerm = boxedTypeTermForType(t)
      ctx.addReusableMember(s"$typeTerm $recordTerm = new $typeTerm();")
      s"$recordTerm = new $typeTerm();"
  }

  def generateNullLiteral(
      resultType: LogicalType,
      nullCheck: Boolean): GeneratedExpression = {
    if (!nullCheck) {
      throw new CodeGenException("Null literals are not allowed if nullCheck is disabled.")
    }
    val defaultValue = primitiveDefaultValue(resultType)
    val resultTypeTerm = primitiveTypeTermForType(resultType)
    GeneratedExpression(
      s"(($resultTypeTerm) $defaultValue)",
      ALWAYS_NULL,
      NO_CODE,
      resultType,
      literalValue = Some(null))
  }

  def generateNonNullLiteral(
      literalType: LogicalType,
      literalCode: String,
      literalValue: Any): GeneratedExpression = {
    val resultTypeTerm = primitiveTypeTermForType(literalType)
    GeneratedExpression(
      s"(($resultTypeTerm) $literalCode)",
      NEVER_NULL,
      NO_CODE,
      literalType,
      literalValue = Some(literalValue))
  }

  @tailrec
  def generateLiteral(
      ctx: CodeGeneratorContext,
      literalType: LogicalType,
      literalValue: Any): GeneratedExpression = {
    if (literalValue == null) {
      return generateNullLiteral(literalType, ctx.nullCheck)
    }
    // non-null values
    literalType.getTypeRoot match {
      // ordered by type root definition
      case CHAR | VARCHAR =>
        val escapedValue = StringEscapeUtils.ESCAPE_JAVA.translate(literalValue.toString)
        val field = ctx.addReusableStringConstants(escapedValue)
        generateNonNullLiteral(literalType, field, StringData.fromString(escapedValue))

      case BOOLEAN =>
        generateNonNullLiteral(literalType, literalValue.toString, literalValue)

      case BINARY | VARBINARY =>
        val bytesVal = literalValue.asInstanceOf[ByteString].getBytes
        val fieldTerm = ctx.addReusableObject(
          bytesVal, "binary", bytesVal.getClass.getCanonicalName)
        generateNonNullLiteral(literalType, fieldTerm, bytesVal)

      case DECIMAL =>
        val dt = literalType.asInstanceOf[DecimalType]
        val precision = dt.getPrecision
        val scale = dt.getScale
        val fieldTerm = newName("decimal")
        val decimalClass = className[DecimalData]
        val fieldDecimal =
          s"""
             |$decimalClass $fieldTerm =
             |    $DECIMAL_UTIL.castFrom("${literalValue.toString}", $precision, $scale);
             |""".stripMargin
        ctx.addReusableMember(fieldDecimal)
        val value = DecimalData.fromBigDecimal(
          literalValue.asInstanceOf[JBigDecimal], precision, scale)
        if (value == null) {
          generateNullLiteral(literalType, ctx.nullCheck)
        } else {
          generateNonNullLiteral(literalType, fieldTerm, value)
        }

      case TINYINT =>
        val decimal = BigDecimal(literalValue.asInstanceOf[JBigDecimal])
        generateNonNullLiteral(literalType, decimal.byteValue().toString, decimal.byteValue())

      case SMALLINT =>
        val decimal = BigDecimal(literalValue.asInstanceOf[JBigDecimal])
        generateNonNullLiteral(literalType, decimal.shortValue().toString, decimal.shortValue())

      case INTEGER =>
        val decimal = BigDecimal(literalValue.asInstanceOf[JBigDecimal])
        generateNonNullLiteral(literalType, decimal.intValue().toString, decimal.intValue())

      case BIGINT =>
        val decimal = BigDecimal(literalValue.asInstanceOf[JBigDecimal])
        generateNonNullLiteral(
          literalType, decimal.longValue().toString + "L", decimal.longValue())

      case FLOAT =>
        val floatValue = literalValue.asInstanceOf[JBigDecimal].floatValue()
        floatValue match {
          case Float.NegativeInfinity =>
            generateNonNullLiteral(
              literalType,
              "java.lang.Float.NEGATIVE_INFINITY",
              Float.NegativeInfinity)
          case Float.PositiveInfinity => generateNonNullLiteral(
            literalType,
            "java.lang.Float.POSITIVE_INFINITY",
            Float.PositiveInfinity)
          case _ => generateNonNullLiteral(
            literalType, floatValue.toString + "f", floatValue)
        }

      case DOUBLE =>
        val doubleValue = literalValue.asInstanceOf[JBigDecimal].doubleValue()
        doubleValue match {
          case Double.NegativeInfinity =>
            generateNonNullLiteral(
              literalType,
              "java.lang.Double.NEGATIVE_INFINITY",
              Double.NegativeInfinity)
          case Double.PositiveInfinity =>
            generateNonNullLiteral(
              literalType,
              "java.lang.Double.POSITIVE_INFINITY",
              Double.PositiveInfinity)
          case _ => generateNonNullLiteral(
            literalType, doubleValue.toString + "d", doubleValue)
        }

      case DATE =>
        generateNonNullLiteral(literalType, literalValue.toString, literalValue)

      case TIME_WITHOUT_TIME_ZONE =>
        generateNonNullLiteral(literalType, literalValue.toString, literalValue)

      case TIMESTAMP_WITHOUT_TIME_ZONE =>
        val fieldTerm = newName("timestamp")
        val ldt = toLocalDateTime(literalValue.asInstanceOf[TimestampString])
        val ts = TimestampData.fromLocalDateTime(ldt)
        val fieldTimestamp =
          s"""
             |$TIMESTAMP_DATA $fieldTerm =
             |  $TIMESTAMP_DATA.fromEpochMillis(${ts.getMillisecond}L, ${ts.getNanoOfMillisecond});
           """.stripMargin
        ctx.addReusableMember(fieldTimestamp)
        generateNonNullLiteral(literalType, fieldTerm, ts)

      case TIMESTAMP_WITH_TIME_ZONE =>
        throw new UnsupportedOperationException("Unsupported type: " + literalType)

      case TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
        val fieldTerm = newName("timestampWithLocalZone")
        val ins =
          toLocalDateTime(literalValue.asInstanceOf[TimestampString])
            .atOffset(ZoneOffset.UTC)
            .toInstant
        val ts = TimestampData.fromInstant(ins)
        val fieldTimestampWithLocalZone =
          s"""
             |$TIMESTAMP_DATA $fieldTerm =
             |  $TIMESTAMP_DATA.fromEpochMillis(${ts.getMillisecond}L, ${ts.getNanoOfMillisecond});
           """.stripMargin
        ctx.addReusableMember(fieldTimestampWithLocalZone)
        generateNonNullLiteral(literalType, fieldTerm, literalValue)

      case INTERVAL_YEAR_MONTH =>
        val decimal = BigDecimal(literalValue.asInstanceOf[JBigDecimal])
        if (decimal.isValidInt) {
          generateNonNullLiteral(literalType, decimal.intValue().toString, decimal.intValue())
        } else {
          throw new CodeGenException(
            s"Decimal '$decimal' can not be converted to interval of months.")
        }

      case INTERVAL_DAY_TIME =>
        val decimal = BigDecimal(literalValue.asInstanceOf[JBigDecimal])
        if (decimal.isValidLong) {
          generateNonNullLiteral(
            literalType,
            decimal.longValue().toString + "L",
            decimal.longValue())
        } else {
          throw new CodeGenException(
            s"Decimal '$decimal' can not be converted to interval of milliseconds.")
        }

      case DISTINCT_TYPE =>
        generateLiteral(ctx, literalType.asInstanceOf[DistinctType].getSourceType, literalValue)

      // Symbol type for special flags e.g. TRIM's BOTH, LEADING, TRAILING
      case RAW if literalType.asInstanceOf[TypeInformationRawType[_]]
          .getTypeInformation.getTypeClass.isAssignableFrom(classOf[Enum[_]]) =>
        generateSymbol(literalValue.asInstanceOf[Enum[_]])

      case SYMBOL =>
        throw new UnsupportedOperationException() // TODO support symbol?

      case ARRAY | MULTISET | MAP | ROW | STRUCTURED_TYPE | NULL | UNRESOLVED =>
        throw new CodeGenException(s"Type not supported: $literalType")
    }
  }

  def generateSymbol(enum: Enum[_]): GeneratedExpression = {
    GeneratedExpression(
      qualifyEnum(enum),
      NEVER_NULL,
      NO_CODE,
      new TypeInformationRawType[AnyRef](new GenericTypeInfo[AnyRef](
        enum.getDeclaringClass.asInstanceOf[Class[AnyRef]])),
      literalValue = Some(enum))
  }

  /**
    * Generates access to a non-null field that does not require unboxing logic.
    *
    * @param fieldType type of field
    * @param fieldTerm expression term of field (already unboxed)
    * @return internal unboxed field representation
    */
  private[flink] def generateNonNullField(
      fieldType: LogicalType,
      fieldTerm: String)
    : GeneratedExpression = {
    val resultTypeTerm = primitiveTypeTermForType(fieldType)
    GeneratedExpression(s"(($resultTypeTerm) $fieldTerm)", NEVER_NULL, NO_CODE, fieldType)
  }

  def generateProctimeTimestamp(
      ctx: CodeGeneratorContext,
      contextTerm: String): GeneratedExpression = {
    val resultType = new TimestampType(3)
    val resultTypeTerm = primitiveTypeTermForType(resultType)
    val resultTerm = ctx.addReusableLocalVariable(resultTypeTerm, "result")
    val resultCode =
      s"""
         |$resultTerm = $TIMESTAMP_DATA.fromEpochMillis(
         |  $contextTerm.timerService().currentProcessingTime());
         |""".stripMargin.trim
    // the proctime has been materialized, so it's TIMESTAMP now, not PROCTIME_INDICATOR
    GeneratedExpression(resultTerm, NEVER_NULL, resultCode, resultType)
  }

  def generateCurrentTimestamp(
      ctx: CodeGeneratorContext): GeneratedExpression = {
    new CurrentTimePointCallGen(false).generate(ctx, Seq(), new TimestampType(3))
  }

  def generateRowtimeAccess(
      ctx: CodeGeneratorContext,
      contextTerm: String): GeneratedExpression = {
    val resultType = new TimestampType(true, TimestampKind.ROWTIME, 3)
    val resultTypeTerm = primitiveTypeTermForType(resultType)
    val Seq(resultTerm, nullTerm) = ctx.addReusableLocalVariables(
      (resultTypeTerm, "result"),
      ("boolean", "isNull"))

    val accessCode =
      s"""
         |$resultTerm = $TIMESTAMP_DATA.fromEpochMillis($contextTerm.timestamp());
         |if ($resultTerm == null) {
         |  throw new RuntimeException("Rowtime timestamp is null. Please make sure that a " +
         |    "proper TimestampAssigner is defined and the stream environment uses the EventTime " +
         |    "time characteristic.");
         |}
         |$nullTerm = false;
       """.stripMargin.trim

    GeneratedExpression(
      resultTerm,
      nullTerm,
      accessCode,
      resultType)
  }

  /**
    * Generates access to a field of the input.
    * @param ctx  code generator context which maintains various code statements.
    * @param inputType  input type
    * @param inputTerm  input term
    * @param index  the field index to access
    * @param nullableInput  whether the input is nullable
    * @param deepCopy whether to copy the accessed field (usually needed when buffered)
    */
  def generateInputAccess(
      ctx: CodeGeneratorContext,
      inputType: LogicalType,
      inputTerm: String,
      index: Int,
      nullableInput: Boolean,
      deepCopy: Boolean = false): GeneratedExpression = {
    // if input has been used before, we can reuse the code that
    // has already been generated
    val inputExpr = ctx.getReusableInputUnboxingExprs(inputTerm, index) match {
      // input access and unboxing has already been generated
      case Some(expr) => expr

      // generate input access and unboxing if necessary
      case None =>
        val expr = if (nullableInput) {
          generateNullableInputFieldAccess(ctx, inputType, inputTerm, index, deepCopy)
        } else {
          generateFieldAccess(ctx, inputType, inputTerm, index, deepCopy)
        }

        ctx.addReusableInputUnboxingExprs(inputTerm, index, expr)
        expr
    }
    // hide the generated code as it will be executed only once
    GeneratedExpression(inputExpr.resultTerm, inputExpr.nullTerm, "", inputExpr.resultType)
  }

  def generateNullableInputFieldAccess(
    ctx: CodeGeneratorContext,
    inputType: LogicalType,
    inputTerm: String,
    index: Int,
    deepCopy: Boolean = false): GeneratedExpression = {

    @tailrec
    def getFieldType(t: LogicalType, pos: Int): LogicalType = t.getTypeRoot match {
      // ordered by type root definition
      case ROW | STRUCTURED_TYPE => t.getChildren.get(pos)
      case DISTINCT_TYPE => getFieldType(t.asInstanceOf[DistinctType].getSourceType, pos)
      case _ => t
    }

    val fieldType = getFieldType(inputType, index)
    val resultTypeTerm = primitiveTypeTermForType(fieldType)
    val defaultValue = primitiveDefaultValue(fieldType)
    val Seq(resultTerm, nullTerm) = ctx.addReusableLocalVariables(
      (resultTypeTerm, "result"),
      ("boolean", "isNull"))

    val fieldAccessExpr = generateFieldAccess(
      ctx, inputType, inputTerm, index, deepCopy)

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
    * @param ctx code generator context which maintains various code statements.
    * @param inputType type of field
    * @param inputTerm expression term of field to be unboxed
    * @param inputUnboxingTerm unboxing/conversion term
    * @return internal unboxed field representation
    */
  def generateInputFieldUnboxing(
      ctx: CodeGeneratorContext,
      inputType: LogicalType,
      inputTerm: String,
      inputUnboxingTerm: String)
    : GeneratedExpression = {

    val resultTypeTerm = primitiveTypeTermForType(inputType)
    val defaultValue = primitiveDefaultValue(inputType)

    val Seq(resultTerm, nullTerm) = ctx.addReusableLocalVariables(
      (resultTypeTerm, "result"),
      ("boolean", "isNull"))

    val wrappedCode = if (ctx.nullCheck) {
      s"""
         |$nullTerm = $inputTerm == null;
         |$resultTerm = $defaultValue;
         |if (!$nullTerm) {
         |  $resultTerm = $inputUnboxingTerm;
         |}
         |""".stripMargin.trim
    } else {
      s"""
         |$resultTerm = $inputUnboxingTerm;
         |""".stripMargin.trim
    }

    GeneratedExpression(resultTerm, nullTerm, wrappedCode, inputType)
  }

  /**
    * Generates field access code expression. The different between this method and
    * [[generateFieldAccess(ctx, inputType, inputTerm, index)]] is that this method
    * accepts an additional `deepCopy` parameter. When deepCopy is set to true, the returned
    * result will be copied.
    *
    * NOTE: Please set `deepCopy` to true when the result will be buffered.
    */
  def generateFieldAccess(
    ctx: CodeGeneratorContext,
    inputType: LogicalType,
    inputTerm: String,
    index: Int,
    deepCopy: Boolean): GeneratedExpression = {
    val expr = generateFieldAccess(ctx, inputType, inputTerm, index)
    if (deepCopy) {
      expr.deepCopy(ctx)
    } else {
      expr
    }
  }

  @tailrec
  def generateFieldAccess(
      ctx: CodeGeneratorContext,
      inputType: LogicalType,
      inputTerm: String,
      index: Int)
    : GeneratedExpression = inputType.getTypeRoot match {
      // ordered by type root definition
      case ROW | STRUCTURED_TYPE =>
        val fieldType = getFieldTypes(inputType).get(index)
        val resultTypeTerm = primitiveTypeTermForType(fieldType)
        val defaultValue = primitiveDefaultValue(fieldType)
        val readCode = rowFieldReadAccess(ctx, index.toString, inputTerm, fieldType)
        val Seq(fieldTerm, nullTerm) = ctx.addReusableLocalVariables(
          (resultTypeTerm, "field"),
          ("boolean", "isNull"))

        val inputCode = if (ctx.nullCheck) {
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

      case DISTINCT_TYPE =>
        generateFieldAccess(
          ctx,
          inputType.asInstanceOf[DistinctType].getSourceType,
          inputTerm,
          index)

      case _ =>
        val fieldTypeTerm = boxedTypeTermForType(inputType)
        val inputCode = s"($fieldTypeTerm) $inputTerm"
        generateInputFieldUnboxing(ctx, inputType, inputCode, inputCode)
    }

  /**
    * Generates code for comparing two fields.
    */
  @tailrec
  def generateCompare(
      ctx: CodeGeneratorContext,
      t: LogicalType,
      nullsIsLast: Boolean,
      leftTerm: String,
      rightTerm: String)
    : String = t.getTypeRoot match {
    // ordered by type root definition
    case CHAR | VARCHAR | DECIMAL | TIMESTAMP_WITHOUT_TIME_ZONE | TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
      s"$leftTerm.compareTo($rightTerm)"
    case BOOLEAN =>
      s"($leftTerm == $rightTerm ? 0 : ($leftTerm ? 1 : -1))"
    case BINARY | VARBINARY =>
      val sortUtil = classOf[org.apache.flink.table.runtime.operators.sort.SortUtil]
        .getCanonicalName
      s"$sortUtil.compareBinary($leftTerm, $rightTerm)"
    case TINYINT | SMALLINT | INTEGER | BIGINT | FLOAT | DOUBLE | DATE | TIME_WITHOUT_TIME_ZONE |
         INTERVAL_YEAR_MONTH | INTERVAL_DAY_TIME =>
      s"($leftTerm > $rightTerm ? 1 : $leftTerm < $rightTerm ? -1 : 0)"
    case TIMESTAMP_WITH_TIME_ZONE | MULTISET | MAP =>
      throw new UnsupportedOperationException() // TODO support MULTISET and MAP?
    case ARRAY =>
      val at = t.asInstanceOf[ArrayType]
      val compareFunc = newName("compareArray")
      val compareCode = generateArrayCompare(
        ctx,
        SortUtil.getNullDefaultOrder(true), at, "a", "b")
      val funcCode: String =
        s"""
          public int $compareFunc($ARRAY_DATA a, $ARRAY_DATA b) {
            $compareCode
            return 0;
          }
        """
      ctx.addReusableMember(funcCode)
      s"$compareFunc($leftTerm, $rightTerm)"
    case ROW | STRUCTURED_TYPE =>
      val fieldCount = getFieldCount(t)
      val orders = (0 until fieldCount).map(_ => true).toArray
      val comparisons = generateRowCompare(
        ctx,
        (0 until fieldCount).toArray,
        getFieldTypes(t).toArray(Array[LogicalType]()),
        orders,
        SortUtil.getNullDefaultOrders(orders),
        "a",
        "b")
      val compareFunc = newName("compareRow")
      val funcCode: String =
        s"""
          public int $compareFunc($ROW_DATA a, $ROW_DATA b) {
            $comparisons
            return 0;
          }
        """
      ctx.addReusableMember(funcCode)
      s"$compareFunc($leftTerm, $rightTerm)"
    case DISTINCT_TYPE =>
      generateCompare(
        ctx,
        t.asInstanceOf[DistinctType].getSourceType,
        nullsIsLast,
        leftTerm,
        rightTerm)
    case RAW =>
      t match {
        case rawType: RawType[_] =>
          val clazz = rawType.getOriginatingClass
          if (!classOf[Comparable[_]].isAssignableFrom(clazz)) {
            throw new CodeGenException(
              s"Raw type class '$clazz' must implement ${className[Comparable[_]]} to be used " +
                s"in a comparision of two '${rawType.asSummaryString()}' types.")
          }
          val serializer = rawType.getTypeSerializer
          val serializerTerm = ctx.addReusableObject(serializer, "serializer")
          s"((${className[Comparable[_]]}) $leftTerm.toObject($serializerTerm))" +
            s".compareTo($rightTerm.toObject($serializerTerm))"

        case rawType: TypeInformationRawType[_] =>
          val serializer = rawType.getTypeInformation.createSerializer(new ExecutionConfig)
          val ser = ctx.addReusableObject(serializer, "serializer")
          val comp = ctx.addReusableObject(
            rawType.getTypeInformation.asInstanceOf[AtomicTypeInfo[_]]
              .createComparator(true, new ExecutionConfig),
          "comparator")
          s"$comp.compare($leftTerm.toObject($ser), $rightTerm.toObject($ser))"
      }
    case NULL | SYMBOL | UNRESOLVED =>
      throw new IllegalArgumentException("Illegal type: " + t)
  }

  /**
    * Generates code for comparing array.
    */
  def generateArrayCompare(
    ctx: CodeGeneratorContext,
    nullsIsLast: Boolean,
    arrayType: ArrayType,
    leftTerm: String,
    rightTerm: String)
  : String = {
    val nullIsLastRet = if (nullsIsLast) 1 else -1
    val elementType = arrayType.getElementType
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
        int $lengthA = a.size();
        int $lengthB = b.size();
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
            $typeTerm $fieldA = ${rowFieldReadAccess(ctx, i, leftTerm, elementType)};
            $typeTerm $fieldB = ${rowFieldReadAccess(ctx, i, rightTerm, elementType)};
            int $comp = ${generateCompare(ctx, elementType, nullsIsLast, fieldA, fieldB)};
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
  def generateRowCompare(
    ctx: CodeGeneratorContext,
    keys: Array[Int],
    keyTypes: Array[LogicalType],
    orders: Array[Boolean],
    nullsIsLast: Array[Boolean],
    leftTerm: String,
    rightTerm: String): String = {

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
           |boolean $isNullA = $leftTerm.isNullAt($index);
           |boolean $isNullB = $rightTerm.isNullAt($index);
           |if ($isNullA && $isNullB) {
           |  // Continue to compare the next element
           |} else if ($isNullA) {
           |  return $nullIsLastRet;
           |} else if ($isNullB) {
           |  return ${-nullIsLastRet};
           |} else {
           |  $typeTerm $fieldA = ${rowFieldReadAccess(ctx, index, leftTerm, t)};
           |  $typeTerm $fieldB = ${rowFieldReadAccess(ctx, index, rightTerm, t)};
           |  int $comp = ${generateCompare(ctx, t, nullsIsLast(i), fieldA, fieldB)};
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
