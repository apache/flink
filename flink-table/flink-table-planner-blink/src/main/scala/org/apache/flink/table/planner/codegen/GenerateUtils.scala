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
import org.apache.flink.api.common.typeinfo.{AtomicType => AtomicTypeInfo}
import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.apache.flink.table.dataformat._
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.codegen.GeneratedExpression.{ALWAYS_NULL, NEVER_NULL, NO_CODE}
import org.apache.flink.table.planner.codegen.calls.CurrentTimePointCallGen
import org.apache.flink.table.planner.plan.utils.SortUtil
import org.apache.flink.table.runtime.types.PlannerTypeUtils
import org.apache.flink.table.runtime.typeutils.TypeCheckUtils.{isCharacterString, isReference, isTemporal}
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.table.types.logical._
import org.apache.calcite.avatica.util.ByteString
import org.apache.calcite.util.TimestampString
import org.apache.commons.lang3.StringEscapeUtils
import java.math.{BigDecimal => JBigDecimal}

import org.apache.flink.table.util.TimestampStringUtils.toLocalDateTime

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
    * This will convert the String result to BinaryString.
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
    * This will convert the String result to BinaryString.
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
    * Generates a record declaration statement. The record can be any type of BaseRow or
    * other types.
    * @param t  the record type
    * @param clazz  the specified class of the type (only used when RowType)
    * @param recordTerm the record term to be declared
    * @param recordWriterTerm the record writer term (only used when BinaryRow type)
    * @return the record declaration statement
    */
  def generateRecordStatement(
      t: LogicalType,
      clazz: Class[_],
      recordTerm: String,
      recordWriterTerm: Option[String] = None): String = {
    t match {
      case rt: RowType if clazz == classOf[BinaryRow] =>
        val writerTerm = recordWriterTerm.getOrElse(
          throw new CodeGenException("No writer is specified when writing BinaryRow record.")
        )
        val binaryRowWriter = className[BinaryRowWriter]
        val typeTerm = clazz.getCanonicalName
        s"""
           |final $typeTerm $recordTerm = new $typeTerm(${rt.getFieldCount});
           |final $binaryRowWriter $writerTerm = new $binaryRowWriter($recordTerm);
           |""".stripMargin.trim
      case rt: RowType if classOf[ObjectArrayRow].isAssignableFrom(clazz) =>
        val typeTerm = clazz.getCanonicalName
        s"final $typeTerm $recordTerm = new $typeTerm(${rt.getFieldCount});"
      case _: RowType if clazz == classOf[JoinedRow] =>
        val typeTerm = clazz.getCanonicalName
        s"final $typeTerm $recordTerm = new $typeTerm();"
      case _ =>
        val typeTerm = boxedTypeTermForType(t)
        s"final $typeTerm $recordTerm = new $typeTerm();"
    }
  }

  def generateNullLiteral(
      resultType: LogicalType,
      nullCheck: Boolean): GeneratedExpression = {
    val defaultValue = primitiveDefaultValue(resultType)
    val resultTypeTerm = primitiveTypeTermForType(resultType)
    if (nullCheck) {
      GeneratedExpression(
        s"(($resultTypeTerm) $defaultValue)",
        ALWAYS_NULL,
        NO_CODE,
        resultType,
        literalValue = Some(null))  // the literal is null
    } else {
      throw new CodeGenException("Null literals are not allowed if nullCheck is disabled.")
    }
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

  def generateLiteral(
      ctx: CodeGeneratorContext,
      literalType: LogicalType,
      literalValue: Any): GeneratedExpression = {
    if (literalValue == null) {
      return generateNullLiteral(literalType, ctx.nullCheck)
    }
    // non-null values
    literalType.getTypeRoot match {

      case BOOLEAN =>
        generateNonNullLiteral(literalType, literalValue.toString, literalValue)

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
      case DECIMAL =>
        val dt = literalType.asInstanceOf[DecimalType]
        val precision = dt.getPrecision
        val scale = dt.getScale
        val fieldTerm = newName("decimal")
        val decimalClass = className[Decimal]
        val fieldDecimal =
          s"""
             |$decimalClass $fieldTerm =
             |    $decimalClass.castFrom("${literalValue.toString}", $precision, $scale);
             |""".stripMargin
        ctx.addReusableMember(fieldDecimal)
        val value = Decimal.fromBigDecimal(
          literalValue.asInstanceOf[JBigDecimal], precision, scale)
        generateNonNullLiteral(literalType, fieldTerm, value)

      case VARCHAR | CHAR =>
        val escapedValue = StringEscapeUtils.ESCAPE_JAVA.translate(literalValue.toString)
        val field = ctx.addReusableStringConstants(escapedValue)
        generateNonNullLiteral(literalType, field, BinaryString.fromString(escapedValue))

      case VARBINARY | BINARY =>
        val bytesVal = literalValue.asInstanceOf[ByteString].getBytes
        val fieldTerm = ctx.addReusableObject(
          bytesVal, "binary", bytesVal.getClass.getCanonicalName)
        generateNonNullLiteral(literalType, fieldTerm, bytesVal)

      case DATE =>
        generateNonNullLiteral(literalType, literalValue.toString, literalValue)

      case TIME_WITHOUT_TIME_ZONE =>
        generateNonNullLiteral(literalType, literalValue.toString, literalValue)

      case TIMESTAMP_WITHOUT_TIME_ZONE =>
        val fieldTerm = newName("timestamp")
        val ldt = toLocalDateTime(literalValue.asInstanceOf[TimestampString])
        val ts = SqlTimestamp.fromLocalDateTime(ldt)
        val fieldTimestamp =
          s"""
             |$SQL_TIMESTAMP $fieldTerm =
             |  $SQL_TIMESTAMP.fromEpochMillis(${ts.getMillisecond}L, ${ts.getNanoOfMillisecond});
           """.stripMargin
        ctx.addReusableMember(fieldTimestamp)
        generateNonNullLiteral(literalType, fieldTerm, ts)

      case TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
        val fieldTerm = newName("timestampWithLocalZone")
        val ins =
          toLocalDateTime(literalValue.asInstanceOf[TimestampString])
          .atZone(ctx.tableConfig.getLocalTimeZone).toInstant
        val ts = SqlTimestamp.fromInstant(ins)
        val fieldTimestampWithLocalZone =
          s"""
             |$SQL_TIMESTAMP $fieldTerm =
             |  $SQL_TIMESTAMP.fromEpochMillis(${ts.getMillisecond}L, ${ts.getNanoOfMillisecond});
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

      // Symbol type for special flags e.g. TRIM's BOTH, LEADING, TRAILING
      case RAW if literalType.asInstanceOf[TypeInformationRawType[_]]
          .getTypeInformation.getTypeClass.isAssignableFrom(classOf[Enum[_]]) =>
        generateSymbol(literalValue.asInstanceOf[Enum[_]])

      case t@_ =>
        throw new CodeGenException(s"Type not supported: $t")
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
         |$resultTerm = $SQL_TIMESTAMP.fromEpochMillis(
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
         |$resultTerm = $SQL_TIMESTAMP.fromEpochMillis($contextTerm.timestamp());
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

    val fieldType = inputType match {
      case ct: RowType => ct.getTypeAt(index)
      case _ => inputType
    }
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

  def generateFieldAccess(
      ctx: CodeGeneratorContext,
      inputType: LogicalType,
      inputTerm: String,
      index: Int): GeneratedExpression =
    inputType match {
      case ct: RowType =>
        val fieldType = ct.getTypeAt(index)
        val resultTypeTerm = primitiveTypeTermForType(fieldType)
        val defaultValue = primitiveDefaultValue(fieldType)
        val readCode = baseRowFieldReadAccess(ctx, index.toString, inputTerm, fieldType)
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

      case _ =>
        val fieldTypeTerm = boxedTypeTermForType(inputType)
        val inputCode = s"($fieldTypeTerm) $inputTerm"
        generateInputFieldUnboxing(ctx, inputType, inputCode, inputCode)
    }

  /**
    * Generates code for comparing two field.
    */
  def generateCompare(
      ctx: CodeGeneratorContext,
      t: LogicalType,
      nullsIsLast: Boolean,
      leftTerm: String,
      rightTerm: String): String = t.getTypeRoot match {
    case BOOLEAN => s"($leftTerm == $rightTerm ? 0 : ($leftTerm ? 1 : -1))"
    case DATE | TIME_WITHOUT_TIME_ZONE =>
      s"($leftTerm > $rightTerm ? 1 : $leftTerm < $rightTerm ? -1 : 0)"
    case _ if PlannerTypeUtils.isPrimitive(t) =>
      s"($leftTerm > $rightTerm ? 1 : $leftTerm < $rightTerm ? -1 : 0)"
    case VARBINARY | BINARY =>
      val sortUtil = classOf[org.apache.flink.table.runtime.operators.sort.SortUtil]
        .getCanonicalName
      s"$sortUtil.compareBinary($leftTerm, $rightTerm)"
    case ARRAY =>
      val at = t.asInstanceOf[ArrayType]
      val compareFunc = newName("compareArray")
      val compareCode = generateArrayCompare(
        ctx,
        SortUtil.getNullDefaultOrder(true), at, "a", "b")
      val funcCode: String =
        s"""
          public int $compareFunc($BASE_ARRAY a, $BASE_ARRAY b) {
            $compareCode
            return 0;
          }
        """
      ctx.addReusableMember(funcCode)
      s"$compareFunc($leftTerm, $rightTerm)"
    case ROW =>
      val rowType = t.asInstanceOf[RowType]
      val orders = (0 until rowType.getFieldCount).map(_ => true).toArray
      val comparisons = generateRowCompare(
        ctx,
        (0 until rowType.getFieldCount).toArray,
        rowType.getChildren.toArray(Array[LogicalType]()),
        orders,
        SortUtil.getNullDefaultOrders(orders),
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
      s"$compareFunc($leftTerm, $rightTerm)"
    case RAW =>
      val rawType = t.asInstanceOf[TypeInformationRawType[_]]
      val ser = ctx.addReusableObject(
        rawType.getTypeInformation.createSerializer(new ExecutionConfig), "serializer")
      val comp = ctx.addReusableObject(
        rawType.getTypeInformation.asInstanceOf[AtomicTypeInfo[_]]
            .createComparator(true, new ExecutionConfig),
        "comparator")
      s"""
         |$comp.compare(
         |  $BINARY_GENERIC.getJavaObjectFromBinaryGeneric($leftTerm, $ser),
         |  $BINARY_GENERIC.getJavaObjectFromBinaryGeneric($rightTerm, $ser)
         |)
       """.stripMargin
    case other => s"$leftTerm.compareTo($rightTerm)"
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
            $typeTerm $fieldA = ${baseRowFieldReadAccess(ctx, i, leftTerm, elementType)};
            $typeTerm $fieldB = ${baseRowFieldReadAccess(ctx, i, rightTerm, elementType)};
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
           |  $typeTerm $fieldA = ${baseRowFieldReadAccess(ctx, index, leftTerm, t)};
           |  $typeTerm $fieldB = ${baseRowFieldReadAccess(ctx, index, rightTerm, t)};
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
