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

package org.apache.flink.table.codegen.calls

import org.apache.flink.table.`type`._
import org.apache.flink.table.codegen.CodeGenUtils.{binaryRowFieldSetAccess, binaryRowSetNull, binaryWriterWriteField, binaryWriterWriteNull, _}
import org.apache.flink.table.codegen.GenerateUtils._
import org.apache.flink.table.codegen.GeneratedExpression.{NEVER_NULL, NO_CODE, ALWAYS_NULL}
import org.apache.flink.table.codegen.{CodeGenException, CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.dataformat._
import org.apache.flink.table.typeutils.TypeCheckUtils._
import org.apache.flink.table.typeutils.{TypeCheckUtils, TypeCoercion}
import org.apache.flink.util.Preconditions.checkArgument

import org.apache.calcite.avatica.util.DateTimeUtils.MILLIS_PER_DAY
import org.apache.calcite.avatica.util.{DateTimeUtils, TimeUnitRange}
import org.apache.calcite.util.BuiltInMethod

import java.lang.{StringBuilder => JStringBuilder}
import java.nio.charset.StandardCharsets

/**
  * Utilities to generate SQL scalar operators, e.g. arithmetic operator,
  * compare operator, equal operator, etc.
  */
object ScalarOperatorGens {

  // ----------------------------------------------------------------------------------------
  // scalar operators generate utils
  // ----------------------------------------------------------------------------------------

  /**
    * Generates a binary arithmetic operator, e.g. + - * / %
    */
  def generateBinaryArithmeticOperator(
    ctx: CodeGeneratorContext,
    operator: String,
    resultType: InternalType,
    left: GeneratedExpression,
    right: GeneratedExpression)
  : GeneratedExpression = {

    resultType match {
      case dt: DecimalType =>
        return generateDecimalBinaryArithmeticOperator(ctx, operator, dt, left, right)
      case _ =>
    }

    val leftCasting = operator match {
      case "%" =>
        if (left.resultType == right.resultType) {
          numericCasting(left.resultType, resultType)
        } else {
          val castedType = if (isDecimal(left.resultType)) {
            InternalTypes.LONG
          } else {
            left.resultType
          }
          numericCasting(left.resultType, castedType)
        }
      case _ => numericCasting(left.resultType, resultType)
    }

    val rightCasting = numericCasting(right.resultType, resultType)
    val resultTypeTerm = primitiveTypeTermForType(resultType)

    generateOperatorIfNotNull(ctx, resultType, left, right) {
      (leftTerm, rightTerm) =>
        s"($resultTypeTerm) (${leftCasting(leftTerm)} $operator ${rightCasting(rightTerm)})"
    }
  }

  /**
    * Generates a binary arithmetic operator for Decimal, e.g. + - * / %
    */
  private def generateDecimalBinaryArithmeticOperator(
      ctx: CodeGeneratorContext,
      operator: String,
      resultType: DecimalType,
      left: GeneratedExpression,
      right: GeneratedExpression)
    : GeneratedExpression = {

    // do not cast a decimal operand to resultType, which may change its value.
    // use it as is during calculation.
    def castToDec(t: InternalType): String => String = t match {
      case _: DecimalType => (operandTerm: String) => s"$operandTerm"
      case _ => numericCasting(t, resultType)
    }
    val methods = Map(
      "+" -> "add",
      "-" -> "subtract",
      "*" -> "multiply",
      "/" -> "divide",
      "%" -> "mod")

    generateOperatorIfNotNull(ctx, resultType, left, right) {
      (leftTerm, rightTerm) => {
        val method = methods(operator)
        val leftCasted = castToDec(left.resultType)(leftTerm)
        val rightCasted = castToDec(right.resultType)(rightTerm)
        val precision = resultType.precision()
        val scale = resultType.scale()
        s"$DECIMAL.$method($leftCasted, $rightCasted, $precision, $scale)"
      }
    }
  }

  /**
    * Generates an unary arithmetic operator, e.g. -num
    */
  def generateUnaryArithmeticOperator(
      ctx: CodeGeneratorContext,
      operator: String,
      resultType: InternalType,
      operand: GeneratedExpression)
    : GeneratedExpression = {
    generateUnaryOperatorIfNotNull(ctx, resultType, operand) {
      operandTerm =>
        if (isDecimal(operand.resultType) && operator == "-") {
          s"$operandTerm.negate()"
        } else if (isDecimal(operand.resultType) && operator == "+") {
          s"$operandTerm"
        } else {
          s"$operator($operandTerm)"
        }
    }
  }


  def generateTemporalPlusMinus(
    ctx: CodeGeneratorContext,
    plus: Boolean,
    resultType: InternalType,
    left: GeneratedExpression,
    right: GeneratedExpression)
  : GeneratedExpression = {

    val op = if (plus) "+" else "-"

    (left.resultType, right.resultType) match {
      // arithmetic of time point and time interval
      case (InternalTypes.INTERVAL_MONTHS, InternalTypes.INTERVAL_MONTHS) |
           (InternalTypes.INTERVAL_MILLIS, InternalTypes.INTERVAL_MILLIS) =>
        generateBinaryArithmeticOperator(ctx, op, left.resultType, left, right)

      case (InternalTypes.DATE, InternalTypes.INTERVAL_MILLIS) =>
        generateOperatorIfNotNull(ctx, InternalTypes.DATE, left, right) {
          (l, r) => s"$l $op ((int) ($r / ${MILLIS_PER_DAY}L))"
        }

      case (InternalTypes.DATE, InternalTypes.INTERVAL_MONTHS) =>
        generateOperatorIfNotNull(ctx, InternalTypes.DATE, left, right) {
          (l, r) => s"${qualifyMethod(BuiltInMethod.ADD_MONTHS.method)}($l, $op($r))"
        }

      case (InternalTypes.TIME, InternalTypes.INTERVAL_MILLIS) =>
        generateOperatorIfNotNull(ctx, InternalTypes.TIME, left, right) {
          (l, r) => s"$l $op ((int) ($r))"
        }

      case (InternalTypes.TIMESTAMP, InternalTypes.INTERVAL_MILLIS) =>
        generateOperatorIfNotNull(ctx, InternalTypes.TIMESTAMP, left, right) {
          (l, r) => s"$l $op $r"
        }

      case (InternalTypes.TIMESTAMP, InternalTypes.INTERVAL_MONTHS) =>
        generateOperatorIfNotNull(ctx, InternalTypes.TIMESTAMP, left, right) {
          (l, r) => s"${qualifyMethod(BuiltInMethod.ADD_MONTHS.method)}($l, $op($r))"
        }

      // minus arithmetic of time points (i.e. for TIMESTAMPDIFF)
      case (InternalTypes.TIMESTAMP | InternalTypes.TIME | InternalTypes.DATE,
      InternalTypes.TIMESTAMP | InternalTypes.TIME | InternalTypes.DATE) if !plus =>
        resultType match {
          case InternalTypes.INTERVAL_MONTHS =>
            generateOperatorIfNotNull(ctx, resultType, left, right) {
              (ll, rr) => (left.resultType, right.resultType) match {
                case (InternalTypes.TIMESTAMP, InternalTypes.DATE) =>
                  s"${qualifyMethod(BuiltInMethod.SUBTRACT_MONTHS.method)}" +
                    s"($ll, $rr * ${MILLIS_PER_DAY}L)"
                case (InternalTypes.DATE, InternalTypes.TIMESTAMP) =>
                  s"${qualifyMethod(BuiltInMethod.SUBTRACT_MONTHS.method)}" +
                    s"($ll * ${MILLIS_PER_DAY}L, $rr)"
                case _ =>
                  s"${qualifyMethod(BuiltInMethod.SUBTRACT_MONTHS.method)}($ll, $rr)"
              }
            }

          case InternalTypes.INTERVAL_MILLIS =>
            generateOperatorIfNotNull(ctx, resultType, left, right) {
              (ll, rr) => (left.resultType, right.resultType) match {
                case (InternalTypes.TIMESTAMP, InternalTypes.TIMESTAMP) =>
                  s"$ll $op $rr"
                case (InternalTypes.DATE, InternalTypes.DATE) =>
                  s"($ll * ${MILLIS_PER_DAY}L) $op ($rr * ${MILLIS_PER_DAY}L)"
                case (InternalTypes.TIMESTAMP, InternalTypes.DATE) =>
                  s"$ll $op ($rr * ${MILLIS_PER_DAY}L)"
                case (InternalTypes.DATE, InternalTypes.TIMESTAMP) =>
                  s"($ll * ${MILLIS_PER_DAY}L) $op $rr"
              }
            }
        }

      case _ =>
        throw new CodeGenException("Unsupported temporal arithmetic.")
    }
  }

  def generateUnaryIntervalPlusMinus(
    ctx: CodeGeneratorContext,
    plus: Boolean,
    operand: GeneratedExpression)
  : GeneratedExpression = {
    val operator = if (plus) "+" else "-"
    generateUnaryArithmeticOperator(ctx, operator, operand.resultType, operand)
  }

  // ----------------------------------------------------------------------------------------
  // scalar expression generate utils
  // ----------------------------------------------------------------------------------------

  /**
    * Generates IN expression using a HashSet
    */
  def generateIn(
      ctx: CodeGeneratorContext,
      needle: GeneratedExpression,
      haystack: Seq[GeneratedExpression])
    : GeneratedExpression = {

    // add elements to hash set if they are constant
    if (haystack.forall(_.literal)) {

      // determine common numeric type
      val widerType = TypeCoercion.widerTypeOf(
        needle.resultType,
        haystack.head.resultType)

      // we need to normalize the values for the hash set
      val castNumeric = widerType match {
        case Some(t) => (value: GeneratedExpression) =>
          numericCasting(value.resultType, t)(value.resultTerm)
        case None => (value: GeneratedExpression) => value.resultTerm
      }

      val resultType = widerType match {
        case Some(t) => t
        case None => needle.resultType
      }

      val elements = haystack.map { element =>
        element.copy(
          castNumeric(element), // cast element to wider type
          element.nullTerm,
          element.code,
          resultType)
      }
      val setTerm = ctx.addReusableHashSet(elements, resultType)

      val castedNeedle = needle.copy(
        castNumeric(needle), // cast needle to wider type
        needle.nullTerm,
        needle.code,
        resultType)

      val Seq(resultTerm, nullTerm) = newNames("result", "isNull")
      val resultTypeTerm = primitiveTypeTermForType(InternalTypes.BOOLEAN)
      val defaultValue = primitiveDefaultValue(InternalTypes.BOOLEAN)

      val operatorCode = if (ctx.nullCheck) {
        s"""
           |${castedNeedle.code}
           |$resultTypeTerm $resultTerm = $defaultValue;
           |boolean $nullTerm = true;
           |if (!${castedNeedle.nullTerm}) {
           |  $resultTerm = $setTerm.contains(${castedNeedle.resultTerm});
           |  $nullTerm = !$resultTerm && $setTerm.containsNull();
           |}
           |""".stripMargin.trim
      }
      else {
        s"""
           |${castedNeedle.code}
           |$resultTypeTerm $resultTerm = $setTerm.contains(${castedNeedle.resultTerm});
           |""".stripMargin.trim
      }

      GeneratedExpression(resultTerm, nullTerm, operatorCode, InternalTypes.BOOLEAN)
    } else {
      // we use a chain of ORs for a set that contains non-constant elements
      haystack
        .map(generateEquals(ctx, needle, _))
        .reduce((left, right) =>
          generateOr(ctx, left, right)
        )
    }
  }

  def generateEquals(
      ctx: CodeGeneratorContext,
      left: GeneratedExpression,
      right: GeneratedExpression)
    : GeneratedExpression = {
    if (left.resultType == InternalTypes.STRING && right.resultType == InternalTypes.STRING) {
      generateOperatorIfNotNull(ctx, InternalTypes.BOOLEAN, left, right) {
        (leftTerm, rightTerm) => s"$leftTerm.equals($rightTerm)"
      }
    }
    // numeric types
    else if (isNumeric(left.resultType) && isNumeric(right.resultType)) {
      generateComparison(ctx, "==", left, right)
    }
    // temporal types
    else if (isTemporal(left.resultType) && left.resultType == right.resultType) {
      generateComparison(ctx, "==", left, right)
    }
    // array types
    else if (isArray(left.resultType) && left.resultType == right.resultType) {
      generateArrayComparison(ctx, left, right)
    }
    // map types
    else if (isMap(left.resultType) && left.resultType == right.resultType) {
      generateMapComparison(ctx, left, right)
    }
    // comparable types of same type
    else if (isComparable(left.resultType) && left.resultType == right.resultType) {
      generateComparison(ctx, "==", left, right)
    }
    // support date/time/timestamp equalTo string.
    // for performance, we cast literal string to literal time.
    else if (isTimePoint(left.resultType) && right.resultType == InternalTypes.STRING) {
      if (right.literal) {
        generateEquals(ctx, left, generateCastStringLiteralToDateTime(ctx, right, left.resultType))
      } else {
        generateEquals(ctx, left, generateCast(ctx, right, left.resultType))
      }
    }
    else if (isTimePoint(right.resultType) && left.resultType == InternalTypes.STRING) {
      if (left.literal) {
        generateEquals(
          ctx,
          generateCastStringLiteralToDateTime(ctx, left, right.resultType),
          right)
      } else {
        generateEquals(ctx, generateCast(ctx, left, right.resultType), right)
      }
    }
    // non comparable types
    else {
      generateOperatorIfNotNull(ctx, InternalTypes.BOOLEAN, left, right) {
        if (isReference(left)) {
          (leftTerm, rightTerm) => s"$leftTerm.equals($rightTerm)"
        }
        else if (isReference(right)) {
          (leftTerm, rightTerm) => s"$rightTerm.equals($leftTerm)"
        }
        else {
          throw new CodeGenException(s"Incomparable types: ${left.resultType} and " +
            s"${right.resultType}")
        }
      }
    }
  }

  def generateNotEquals(
      ctx: CodeGeneratorContext,
      left: GeneratedExpression,
      right: GeneratedExpression)
    : GeneratedExpression = {
    if (left.resultType == InternalTypes.STRING && right.resultType == InternalTypes.STRING) {
      generateOperatorIfNotNull(ctx, InternalTypes.BOOLEAN, left, right) {
        (leftTerm, rightTerm) => s"!$leftTerm.equals($rightTerm)"
      }
    }
    // numeric types
    else if (isNumeric(left.resultType) && isNumeric(right.resultType)) {
      generateComparison(ctx, "!=", left, right)
    }
    // temporal types
    else if (isTemporal(left.resultType) && left.resultType == right.resultType) {
      generateComparison(ctx, "!=", left, right)
    }
    // array types
    else if (isArray(left.resultType) && left.resultType == right.resultType) {
      val equalsExpr = generateEquals(ctx, left, right)
      GeneratedExpression(
        s"(!${equalsExpr.resultTerm})", equalsExpr.nullTerm, equalsExpr.code, InternalTypes.BOOLEAN)
    }
    // map types
    else if (isMap(left.resultType) && left.resultType == right.resultType) {
      val equalsExpr = generateEquals(ctx, left, right)
      GeneratedExpression(
        s"(!${equalsExpr.resultTerm})", equalsExpr.nullTerm, equalsExpr.code, InternalTypes.BOOLEAN)
    }
    // comparable types
    else if (isComparable(left.resultType) && left.resultType == right.resultType) {
      generateComparison(ctx, "!=", left, right)
    }
    // non-comparable types
    else {
      generateOperatorIfNotNull(ctx, InternalTypes.BOOLEAN, left, right) {
        if (isReference(left)) {
          (leftTerm, rightTerm) => s"!($leftTerm.equals($rightTerm))"
        }
        else if (isReference(right)) {
          (leftTerm, rightTerm) => s"!($rightTerm.equals($leftTerm))"
        }
        else {
          throw new CodeGenException(s"Incomparable types: ${left.resultType} and " +
            s"${right.resultType}")
        }
      }
    }
  }

  /**
    * Generates comparison code for numeric types and comparable types of same type.
    */
  def generateComparison(
      ctx: CodeGeneratorContext,
      operator: String,
      left: GeneratedExpression,
      right: GeneratedExpression)
    : GeneratedExpression = {
    generateOperatorIfNotNull(ctx, InternalTypes.BOOLEAN, left, right) {
      // either side is decimal
      if (isDecimal(left.resultType) || isDecimal(right.resultType)) {
        (leftTerm, rightTerm) => {
          s"${className[Decimal]}.compare($leftTerm, $rightTerm) $operator 0"
        }
      }
      // both sides are numeric
      else if (isNumeric(left.resultType) && isNumeric(right.resultType)) {
        (leftTerm, rightTerm) => s"$leftTerm $operator $rightTerm"
      }
      // both sides are temporal of same type
      else if (isTemporal(left.resultType) && left.resultType == right.resultType) {
        (leftTerm, rightTerm) => s"$leftTerm $operator $rightTerm"
      }
      // both sides are boolean
      else if (isBoolean(left.resultType) && left.resultType == right.resultType) {
        operator match {
          case "==" | "!=" => (leftTerm, rightTerm) => s"$leftTerm $operator $rightTerm"
          case ">" | "<" | "<=" | ">=" =>
            (leftTerm, rightTerm) =>
              s"java.lang.Boolean.compare($leftTerm, $rightTerm) $operator 0"
          case _ => throw new CodeGenException(s"Unsupported boolean comparison '$operator'.")
        }
      }
      // both sides are binary type
      else if (isBinary(left.resultType) && left.resultType == right.resultType) {
        (leftTerm, rightTerm) =>
          s"java.util.Arrays.equals($leftTerm, $rightTerm)"
      }
      // both sides are same comparable type
      else if (isComparable(left.resultType) && left.resultType == right.resultType) {
        (leftTerm, rightTerm) =>
          s"(($leftTerm == null) ? (($rightTerm == null) ? 0 : -1) : (($rightTerm == null) ? " +
            s"1 : ($leftTerm.compareTo($rightTerm)))) $operator 0"
      }
      else {
        throw new CodeGenException(s"Incomparable types: ${left.resultType} and " +
          s"${right.resultType}")
      }
    }
  }

  def generateIsNull(
      ctx: CodeGeneratorContext,
      operand: GeneratedExpression): GeneratedExpression = {
    if (ctx.nullCheck) {
      GeneratedExpression(operand.nullTerm, NEVER_NULL, operand.code, InternalTypes.BOOLEAN)
    }
    else if (!ctx.nullCheck && isReference(operand)) {
      val resultTerm = newName("isNull")
      val operatorCode =
        s"""
           |${operand.code}
           |boolean $resultTerm = ${operand.resultTerm} == null;
           |""".stripMargin
      GeneratedExpression(resultTerm, NEVER_NULL, operatorCode, InternalTypes.BOOLEAN)
    }
    else {
      GeneratedExpression("false", NEVER_NULL, operand.code, InternalTypes.BOOLEAN)
    }
  }

  def generateIsNotNull(
      ctx: CodeGeneratorContext,
      operand: GeneratedExpression): GeneratedExpression = {
    if (ctx.nullCheck) {
      val resultTerm = newName("result")
      val operatorCode =
        s"""
           |${operand.code}
           |boolean $resultTerm = !${operand.nullTerm};
           |""".stripMargin.trim
      GeneratedExpression(resultTerm, NEVER_NULL, operatorCode, InternalTypes.BOOLEAN)
    }
    else if (!ctx.nullCheck && isReference(operand)) {
      val resultTerm = newName("result")
      val operatorCode =
        s"""
           |${operand.code}
           |boolean $resultTerm = ${operand.resultTerm} != null;
           |""".stripMargin.trim
      GeneratedExpression(resultTerm, NEVER_NULL, operatorCode, InternalTypes.BOOLEAN)
    }
    else {
      GeneratedExpression("true", NEVER_NULL, operand.code, InternalTypes.BOOLEAN)
    }
  }

  def generateAnd(
      ctx: CodeGeneratorContext,
      left: GeneratedExpression,
      right: GeneratedExpression): GeneratedExpression = {
    val Seq(resultTerm, nullTerm) = newNames("result", "isNull")

    val operatorCode = if (ctx.nullCheck) {
      // Three-valued logic:
      // no Unknown -> Two-valued logic
      // True && Unknown -> Unknown
      // False && Unknown -> False
      // Unknown && True -> Unknown
      // Unknown && False -> False
      // Unknown && Unknown -> Unknown
      s"""
         |${left.code}
         |
         |boolean $resultTerm = false;
         |boolean $nullTerm = false;
         |if (!${left.nullTerm} && !${left.resultTerm}) {
         |  // left expr is false, skip right expr
         |} else {
         |  ${right.code}
         |
         |  if (!${left.nullTerm} && !${right.nullTerm}) {
         |    $resultTerm = ${left.resultTerm} && ${right.resultTerm};
         |    $nullTerm = false;
         |  }
         |  else if (!${left.nullTerm} && ${left.resultTerm} && ${right.nullTerm}) {
         |    $resultTerm = false;
         |    $nullTerm = true;
         |  }
         |  else if (!${left.nullTerm} && !${left.resultTerm} && ${right.nullTerm}) {
         |    $resultTerm = false;
         |    $nullTerm = false;
         |  }
         |  else if (${left.nullTerm} && !${right.nullTerm} && ${right.resultTerm}) {
         |    $resultTerm = false;
         |    $nullTerm = true;
         |  }
         |  else if (${left.nullTerm} && !${right.nullTerm} && !${right.resultTerm}) {
         |    $resultTerm = false;
         |    $nullTerm = false;
         |  }
         |  else {
         |    $resultTerm = false;
         |    $nullTerm = true;
         |  }
         |}
       """.stripMargin.trim
    }
    else {
      s"""
         |${left.code}
         |boolean $resultTerm = false;
         |if (${left.resultTerm}) {
         |  ${right.code}
         |  $resultTerm = ${right.resultTerm};
         |}
         |""".stripMargin.trim
    }

    GeneratedExpression(resultTerm, nullTerm, operatorCode, InternalTypes.BOOLEAN)
  }

  def generateOr(
      ctx: CodeGeneratorContext,
      left: GeneratedExpression,
      right: GeneratedExpression): GeneratedExpression = {
    val Seq(resultTerm, nullTerm) = newNames("result", "isNull")

    val operatorCode = if (ctx.nullCheck) {
      // Three-valued logic:
      // no Unknown -> Two-valued logic
      // True || Unknown -> True
      // False || Unknown -> Unknown
      // Unknown || True -> True
      // Unknown || False -> Unknown
      // Unknown || Unknown -> Unknown
      s"""
         |${left.code}
         |
        |boolean $resultTerm = true;
         |boolean $nullTerm = false;
         |if (!${left.nullTerm} && ${left.resultTerm}) {
         |  // left expr is true, skip right expr
         |} else {
         |  ${right.code}
         |
        |  if (!${left.nullTerm} && !${right.nullTerm}) {
         |    $resultTerm = ${left.resultTerm} || ${right.resultTerm};
         |    $nullTerm = false;
         |  }
         |  else if (!${left.nullTerm} && ${left.resultTerm} && ${right.nullTerm}) {
         |    $resultTerm = true;
         |    $nullTerm = false;
         |  }
         |  else if (!${left.nullTerm} && !${left.resultTerm} && ${right.nullTerm}) {
         |    $resultTerm = false;
         |    $nullTerm = true;
         |  }
         |  else if (${left.nullTerm} && !${right.nullTerm} && ${right.resultTerm}) {
         |    $resultTerm = true;
         |    $nullTerm = false;
         |  }
         |  else if (${left.nullTerm} && !${right.nullTerm} && !${right.resultTerm}) {
         |    $resultTerm = false;
         |    $nullTerm = true;
         |  }
         |  else {
         |    $resultTerm = false;
         |    $nullTerm = true;
         |  }
         |}
         |""".stripMargin.trim
    }
    else {
      s"""
         |${left.code}
         |boolean $resultTerm = true;
         |if (!${left.resultTerm}) {
         |  ${right.code}
         |  $resultTerm = ${right.resultTerm};
         |}
         |""".stripMargin.trim
    }

    GeneratedExpression(resultTerm, nullTerm, operatorCode, InternalTypes.BOOLEAN)
  }

  def generateNot(
      ctx: CodeGeneratorContext,
      operand: GeneratedExpression)
    : GeneratedExpression = {
    // Three-valued logic:
    // no Unknown -> Two-valued logic
    // Unknown -> Unknown
    generateUnaryOperatorIfNotNull(ctx, InternalTypes.BOOLEAN, operand) {
      operandTerm => s"!($operandTerm)"
    }
  }

  def generateIsTrue(operand: GeneratedExpression): GeneratedExpression = {
    GeneratedExpression(
      operand.resultTerm, // unknown is always false by default
      GeneratedExpression.NEVER_NULL,
      operand.code,
      InternalTypes.BOOLEAN)
  }

  def generateIsNotTrue(operand: GeneratedExpression): GeneratedExpression = {
    GeneratedExpression(
      s"(!${operand.resultTerm})", // unknown is always false by default
      GeneratedExpression.NEVER_NULL,
      operand.code,
      InternalTypes.BOOLEAN)
  }

  def generateIsFalse(operand: GeneratedExpression): GeneratedExpression = {
    GeneratedExpression(
      s"(!${operand.resultTerm} && !${operand.nullTerm})",
      GeneratedExpression.NEVER_NULL,
      operand.code,
      InternalTypes.BOOLEAN)
  }

  def generateIsNotFalse(operand: GeneratedExpression): GeneratedExpression = {
    GeneratedExpression(
      s"(${operand.resultTerm} || ${operand.nullTerm})",
      GeneratedExpression.NEVER_NULL,
      operand.code,
      InternalTypes.BOOLEAN)
  }

  def generateReinterpret(
      ctx: CodeGeneratorContext,
      operand: GeneratedExpression,
      targetType: InternalType)
    : GeneratedExpression = (operand.resultType, targetType) match {

    case (fromTp, toTp) if fromTp == toTp =>
      operand

    // internal reinterpretation of temporal types
    // Date -> Integer
    // Time -> Integer
    // Timestamp -> Long
    // Integer -> Date
    // Integer -> Time
    // Long -> Timestamp
    // Integer -> Interval Months
    // Long -> Interval Millis
    // Interval Months -> Integer
    // Interval Millis -> Long
    // Date -> Long
    // Time -> Long
    // Interval Months -> Long
    case (InternalTypes.DATE, InternalTypes.INT) |
         (InternalTypes.TIME, InternalTypes.INT) |
         (_: TimestampType, InternalTypes.LONG) |
         (InternalTypes.INT, InternalTypes.DATE) |
         (InternalTypes.INT, InternalTypes.TIME) |
         (InternalTypes.LONG, _: TimestampType) |
         (InternalTypes.INT, InternalTypes.INTERVAL_MONTHS) |
         (InternalTypes.LONG, InternalTypes.INTERVAL_MILLIS) |
         (InternalTypes.INTERVAL_MONTHS, InternalTypes.INT) |
         (InternalTypes.INTERVAL_MILLIS, InternalTypes.LONG) |
         (InternalTypes.DATE, InternalTypes.LONG) |
         (InternalTypes.TIME, InternalTypes.LONG) |
         (InternalTypes.INTERVAL_MONTHS, InternalTypes.LONG) =>
      internalExprCasting(operand, targetType)

    case (from, to) =>
      throw new CodeGenException(s"Unsupported reinterpret from '$from' to '$to'.")
  }

  def generateCast(
      ctx: CodeGeneratorContext,
      operand: GeneratedExpression,
      targetType: InternalType)
    : GeneratedExpression = (operand.resultType, targetType) match {

    // special case: cast from TimeIndicatorTypeInfo to SqlTimeTypeInfo
    case (InternalTypes.PROCTIME_INDICATOR, InternalTypes.TIMESTAMP) |
         (InternalTypes.ROWTIME_INDICATOR, InternalTypes.TIMESTAMP) |
         (InternalTypes.TIMESTAMP, InternalTypes.PROCTIME_INDICATOR) |
         (InternalTypes.TIMESTAMP, InternalTypes.ROWTIME_INDICATOR) =>
      operand.copy(resultType = InternalTypes.TIMESTAMP) // just replace the DataType

    // identity casting
    case (fromTp, toTp) if fromTp == toTp =>
      operand

    // Date/Time/Timestamp -> String
    case (left, InternalTypes.STRING) if TypeCheckUtils.isTimePoint(left) =>
      generateStringResultCallIfArgsNotNull(ctx, Seq(operand)) {
        operandTerm =>
          val zoneTerm = ctx.addReusableTimeZone()
          s"${internalToStringCode(left, operandTerm.head, zoneTerm)}"
      }

    // Interval Months -> String
    case (InternalTypes.INTERVAL_MONTHS, InternalTypes.STRING) =>
      val method = qualifyMethod(BuiltInMethod.INTERVAL_YEAR_MONTH_TO_STRING.method)
      val timeUnitRange = qualifyEnum(TimeUnitRange.YEAR_TO_MONTH)
      generateStringResultCallIfArgsNotNull(ctx, Seq(operand)) {
        terms => s"$method(${terms.head}, $timeUnitRange)"
      }

    // Interval Millis -> String
    case (InternalTypes.INTERVAL_MILLIS, InternalTypes.STRING) =>
      val method = qualifyMethod(BuiltInMethod.INTERVAL_DAY_TIME_TO_STRING.method)
      val timeUnitRange = qualifyEnum(TimeUnitRange.DAY_TO_SECOND)
      generateStringResultCallIfArgsNotNull(ctx, Seq(operand)) {
        terms => s"$method(${terms.head}, $timeUnitRange, 3)" // milli second precision
      }

    // Array -> String
    case (at: ArrayType, InternalTypes.STRING) =>
      generateCastArrayToString(ctx, operand, at)

    // Byte array -> String UTF-8
    case (InternalTypes.BINARY, InternalTypes.STRING) =>
      val charset = classOf[StandardCharsets].getCanonicalName
      generateStringResultCallIfArgsNotNull(ctx, Seq(operand)) {
        terms => s"(new String(${terms.head}, $charset.UTF_8))"
      }


    // Map -> String
    case (mt: MapType, InternalTypes.STRING) =>
      generateCastMapToString(ctx, operand, mt)

    // composite type -> String
    case (brt: RowType, InternalTypes.STRING) =>
      generateCastBaseRowToString(ctx, operand, brt)

    case (g: GenericType[_], InternalTypes.STRING) =>
      generateStringResultCallIfArgsNotNull(ctx, Seq(operand)) {
        terms =>
          val converter = DataFormatConverters.getConverterForTypeInfo(g.getTypeInfo)
          val converterTerm = ctx.addReusableObject(converter, "converter")
          s""" "" + $converterTerm.toExternal(${terms.head})"""
      }

    // * (not Date/Time/Timestamp) -> String
    // TODO: GenericType with Date/Time/Timestamp -> String would call toString implicitly
    case (_, InternalTypes.STRING) =>
      generateStringResultCallIfArgsNotNull(ctx, Seq(operand)) {
        terms => s""" "" + ${terms.head}"""
      }

    // String -> Boolean
    case (InternalTypes.STRING, InternalTypes.BOOLEAN) =>
      generateUnaryOperatorIfNotNull(
        ctx,
        targetType,
        operand,
        resultNullable = true) {
        operandTerm => s"$operandTerm.toBooleanSQL()"
      }

    // String -> NUMERIC TYPE (not Character)
    case (InternalTypes.STRING, _)
      if TypeCheckUtils.isNumeric(targetType) =>
      targetType match {
        case dt: DecimalType =>
          generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
            operandTerm => s"$operandTerm.toDecimal(${dt.precision}, ${dt.scale})"
          }
        case _ =>
          val methodName = targetType match {
            case InternalTypes.BYTE => "toByte"
            case InternalTypes.SHORT => "toShort"
            case InternalTypes.INT => "toInt"
            case InternalTypes.LONG => "toLong"
            case InternalTypes.DOUBLE => "toDouble"
            case InternalTypes.FLOAT => "toFloat"
            case _ => null
          }
          assert(methodName != null, "Unexpected data type.")
          generateUnaryOperatorIfNotNull(
            ctx,
            targetType,
            operand,
            resultNullable = true) {
            operandTerm => s"($operandTerm.trim().$methodName())"
          }
      }

    // String -> Date
    case (InternalTypes.STRING, InternalTypes.DATE) =>
      generateUnaryOperatorIfNotNull(
        ctx,
        targetType,
        operand,
        resultNullable = true) {
        operandTerm =>
          s"${qualifyMethod(BuiltInMethod.STRING_TO_DATE.method)}($operandTerm.toString())"
      }

    // String -> Time
    case (InternalTypes.STRING, InternalTypes.TIME) =>
      generateUnaryOperatorIfNotNull(
        ctx, 
        targetType,
        operand, 
        resultNullable = true) {
        operandTerm =>
          s"${qualifyMethod(BuiltInMethod.STRING_TO_TIME.method)}($operandTerm.toString())"
      }

    // String -> Timestamp
    case (InternalTypes.STRING, InternalTypes.TIMESTAMP) =>
      generateUnaryOperatorIfNotNull(
        ctx, 
        targetType,
        operand, 
        resultNullable = true) {
        operandTerm =>
          val zoneTerm = ctx.addReusableTimeZone()
          s"""${qualifyMethod(BuiltInMethods.STRING_TO_TIMESTAMP)}($operandTerm.toString(),
             | $zoneTerm)""".stripMargin
      }

    // String -> binary
    case (InternalTypes.STRING, InternalTypes.BINARY) =>
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
        operandTerm => s"$operandTerm.getBytes()"
      }

    // Note: SQL2003 $6.12 - casting is not allowed between boolean and numeric types.
    //       Calcite does not allow it either.

    // Boolean -> BigDecimal
    case (InternalTypes.BOOLEAN, dt: DecimalType) =>
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
        operandTerm => s"$DECIMAL.castFrom($operandTerm, ${dt.precision}, ${dt.scale})"
      }

    // Boolean -> NUMERIC TYPE
    case (InternalTypes.BOOLEAN, _) if TypeCheckUtils.isNumeric(targetType) =>
      val targetTypeTerm = primitiveTypeTermForType(targetType)
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
        operandTerm => s"($targetTypeTerm) ($operandTerm ? 1 : 0)"
      }

    // BigDecimal -> Boolean
    case (_: DecimalType, InternalTypes.BOOLEAN) =>
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
        operandTerm => s"$DECIMAL.castToBoolean($operandTerm)"
      }

    // BigDecimal -> Timestamp
    case (_: DecimalType, InternalTypes.TIMESTAMP) =>
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
        operandTerm => s"$DECIMAL.castToTimestamp($operandTerm)"
      }

    // NUMERIC TYPE -> Boolean
    case (left, InternalTypes.BOOLEAN) if isNumeric(left) =>
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
        operandTerm => s"$operandTerm != 0"
      }

    // between NUMERIC TYPE | Decimal
    case  (left, right) if isNumeric(left) && isNumeric(right) =>
      val operandCasting = numericCasting(operand.resultType, targetType)
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
        operandTerm => s"${operandCasting(operandTerm)}"
      }

    // Date -> Timestamp
    case (InternalTypes.DATE, InternalTypes.TIMESTAMP) =>
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
        operandTerm =>
          s"$operandTerm * ${classOf[DateTimeUtils].getCanonicalName}.MILLIS_PER_DAY"
      }

    // Timestamp -> Date
    case (InternalTypes.TIMESTAMP, InternalTypes.DATE) =>
      val targetTypeTerm = primitiveTypeTermForType(targetType)
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
        operandTerm =>
          s"($targetTypeTerm) ($operandTerm / " +
            s"${classOf[DateTimeUtils].getCanonicalName}.MILLIS_PER_DAY)"
      }

    // Time -> Timestamp
    case (InternalTypes.TIME, InternalTypes.TIMESTAMP) =>
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
        operandTerm => s"$operandTerm"
      }

    // Timestamp -> Time
    case (InternalTypes.TIMESTAMP, InternalTypes.TIME) =>
      val targetTypeTerm = primitiveTypeTermForType(targetType)
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
        operandTerm =>
          s"($targetTypeTerm) ($operandTerm % " +
            s"${classOf[DateTimeUtils].getCanonicalName}.MILLIS_PER_DAY)"
      }

    // Timestamp -> Decimal
    case  (InternalTypes.TIMESTAMP, dt: DecimalType) =>
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
        operandTerm => s"$DECIMAL.castFrom" +
          s"(((double) ($operandTerm / 1000.0)), ${dt.precision}, ${dt.scale})"
      }

    // Tinyint -> Timestamp
    // Smallint -> Timestamp
    // Int -> Timestamp
    // Bigint -> Timestamp
    case (InternalTypes.BYTE, InternalTypes.TIMESTAMP) |
         (InternalTypes.SHORT,InternalTypes.TIMESTAMP) |
         (InternalTypes.INT, InternalTypes.TIMESTAMP) |
         (InternalTypes.LONG, InternalTypes.TIMESTAMP) =>
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
        operandTerm => s"(((long) $operandTerm) * 1000)"
      }

    // Float -> Timestamp
    // Double -> Timestamp
    case (InternalTypes.FLOAT, InternalTypes.TIMESTAMP) |
         (InternalTypes.DOUBLE, InternalTypes.TIMESTAMP) =>
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
        operandTerm => s"((long) ($operandTerm * 1000))"
      }

    // Timestamp -> Tinyint
    case (InternalTypes.TIMESTAMP, InternalTypes.BYTE) =>
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
        operandTerm => s"((byte) ($operandTerm / 1000))"
      }

    // Timestamp -> Smallint
    case (InternalTypes.TIMESTAMP, InternalTypes.SHORT) =>
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
        operandTerm => s"((short) ($operandTerm / 1000))"
      }

    // Timestamp -> Int
    case (InternalTypes.TIMESTAMP, InternalTypes.INT) =>
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
        operandTerm => s"((int) ($operandTerm / 1000))"
      }

    // Timestamp -> BigInt
    case (InternalTypes.TIMESTAMP, InternalTypes.LONG) =>
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
        operandTerm => s"((long) ($operandTerm / 1000))"
      }

    // Timestamp -> Float
    case (InternalTypes.TIMESTAMP, InternalTypes.FLOAT) =>
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
        operandTerm => s"((float) ($operandTerm / 1000.0))"
      }

    // Timestamp -> Double
    case (InternalTypes.TIMESTAMP, InternalTypes.DOUBLE) =>
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
        operandTerm => s"((double) ($operandTerm / 1000.0))"
      }

    // internal temporal casting
    // Date -> Integer
    // Time -> Integer
    // Integer -> Date
    // Integer -> Time
    // Integer -> Interval Months
    // Long -> Interval Millis
    // Interval Months -> Integer
    // Interval Millis -> Long
    case (InternalTypes.DATE, InternalTypes.INT) |
         (InternalTypes.TIME, InternalTypes.INT) |
         (InternalTypes.INT, InternalTypes.DATE) |
         (InternalTypes.INT, InternalTypes.TIME) |
         (InternalTypes.INT, InternalTypes.INTERVAL_MONTHS) |
         (InternalTypes.LONG, InternalTypes.INTERVAL_MILLIS) |
         (InternalTypes.INTERVAL_MONTHS, InternalTypes.INT) |
         (InternalTypes.INTERVAL_MILLIS, InternalTypes.LONG) =>
      internalExprCasting(operand, targetType)

    // internal reinterpretation of temporal types
    // Date, Time, Interval Months -> Long
    case  (InternalTypes.DATE, InternalTypes.LONG)
          | (InternalTypes.TIME, InternalTypes.LONG)
          | (InternalTypes.INTERVAL_MONTHS, InternalTypes.LONG) =>
      internalExprCasting(operand, targetType)

    case (from, to) =>
      throw new CodeGenException(s"Unsupported cast from '$from' to '$to'.")
  }

  def generateIfElse(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      resultType: InternalType,
      i: Int = 0)
    : GeneratedExpression = {
    // else part
    if (i == operands.size - 1) {
      generateCast(ctx, operands(i), resultType)
    }
    else {
      // check that the condition is boolean
      // we do not check for null instead we use the default value
      // thus null is false
      requireBoolean(operands(i))
      val condition = operands(i)
      val trueAction = generateCast(ctx, operands(i + 1), resultType)
      val falseAction = generateIfElse(ctx, operands, resultType, i + 2)

      val Seq(resultTerm, nullTerm) = newNames("result", "isNull")
      val resultTypeTerm = primitiveTypeTermForType(resultType)

      val operatorCode = if (ctx.nullCheck) {
        s"""
           |${condition.code}
           |$resultTypeTerm $resultTerm;
           |boolean $nullTerm;
           |if (${condition.resultTerm}) {
           |  ${trueAction.code}
           |  $resultTerm = ${trueAction.resultTerm};
           |  $nullTerm = ${trueAction.nullTerm};
           |}
           |else {
           |  ${falseAction.code}
           |  $resultTerm = ${falseAction.resultTerm};
           |  $nullTerm = ${falseAction.nullTerm};
           |}
           |""".stripMargin.trim
      }
      else {
        s"""
           |${condition.code}
           |$resultTypeTerm $resultTerm;
           |if (${condition.resultTerm}) {
           |  ${trueAction.code}
           |  $resultTerm = ${trueAction.resultTerm};
           |}
           |else {
           |  ${falseAction.code}
           |  $resultTerm = ${falseAction.resultTerm};
           |}
           |""".stripMargin.trim
      }

      GeneratedExpression(resultTerm, nullTerm, operatorCode, resultType)
    }
  }

  def generateDot(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression]): GeneratedExpression = {

    // due to https://issues.apache.org/jira/browse/CALCITE-2162, expression such as
    // "array[1].a.b" won't work now.
    if (operands.size > 2) {
      throw new CodeGenException(
        "A DOT operator with more than 2 operands is not supported yet.")
    }

    checkArgument(operands(1).literal)
    checkArgument(operands(1).resultType == InternalTypes.STRING)
    checkArgument(operands.head.resultType.isInstanceOf[RowType])

    val fieldName = operands(1).literalValue.get.toString
    val fieldIdx = operands
      .head
      .resultType
      .asInstanceOf[RowType]
      .getFieldIndex(fieldName)

    val access = generateFieldAccess(
      ctx,
      operands.head.resultType,
      operands.head.resultTerm,
      fieldIdx)

    val Seq(resultTerm, nullTerm) = newNames("result", "isNull")
    val resultTypeTerm = primitiveTypeTermForType(access.resultType)
    val defaultValue = primitiveDefaultValue(access.resultType)

    val resultCode = if (ctx.nullCheck) {
      s"""
         |${operands.map(_.code).mkString("\n")}
         |$resultTypeTerm $resultTerm;
         |boolean $nullTerm;
         |if (${operands.map(_.nullTerm).mkString(" || ")}) {
         |  $resultTerm = $defaultValue;
         |  $nullTerm = true;
         |}
         |else {
         |  ${access.code}
         |  $resultTerm = ${access.resultTerm};
         |  $nullTerm = ${access.nullTerm};
         |}
         |""".stripMargin
    } else {
      s"""
         |${operands.map(_.code).mkString("\n")}
         |${access.code}
         |$resultTypeTerm $resultTerm = ${access.resultTerm};
         |""".stripMargin
    }


    GeneratedExpression(
      resultTerm,
      nullTerm,
      resultCode,
      access.resultType
    )
  }

  // ----------------------------------------------------------------------------------------
  // value construction and accessing generate utils
  // ----------------------------------------------------------------------------------------

  def generateRow(
      ctx: CodeGeneratorContext,
      resultType: InternalType,
      elements: Seq[GeneratedExpression]): GeneratedExpression = {
    checkArgument(resultType.isInstanceOf[RowType])
    val rowType = resultType.asInstanceOf[RowType]
    val fieldTypes = rowType.getFieldTypes
    val isLiteral = elements.forall(e => e.literal)
    val isPrimitive = fieldTypes.forall(f => f.isInstanceOf[PrimitiveType])

    if (isLiteral) {
      // generate literal row
      generateLiteralRow(ctx, rowType, elements)
    } else {
      if (isPrimitive) {
        // generate primitive row
        val mapped = elements.zipWithIndex.map { case (element, idx) =>
          if (element.literal) {
            element
          } else {
            val tpe = fieldTypes(idx)
            val resultTerm = primitiveDefaultValue(tpe)
            GeneratedExpression(resultTerm, ALWAYS_NULL, NO_CODE, tpe, Some(null))
          }
        }
        val row = generateLiteralRow(ctx, rowType, mapped)
        val code = elements.zipWithIndex.map { case (element, idx) =>
          val tpe = fieldTypes(idx)
          if (element.literal) {
            ""
          } else if(ctx.nullCheck) {
            s"""
               |${element.code}
               |if (${element.nullTerm}) {
               |  ${binaryRowSetNull(idx, row.resultTerm, tpe)};
               |} else {
               |  ${binaryRowFieldSetAccess(idx, row.resultTerm, tpe, element.resultTerm)};
               |}
           """.stripMargin
          } else {
            s"""
               |${element.code}
               |${binaryRowFieldSetAccess(idx, row.resultTerm, tpe, element.resultTerm)};
           """.stripMargin
          }
        }.mkString("\n")
        GeneratedExpression(row.resultTerm, NEVER_NULL, code, rowType)
      } else {
        // generate general row
        generateNonLiteralRow(ctx, rowType, elements)
      }
    }
  }

  private def generateLiteralRow(
      ctx: CodeGeneratorContext,
      rowType: RowType,
      elements: Seq[GeneratedExpression]): GeneratedExpression = {
    checkArgument(elements.forall(e => e.literal))
    val expr = generateNonLiteralRow(ctx, rowType, elements)
    ctx.addReusableInitStatement(expr.code)
    GeneratedExpression(expr.resultTerm, GeneratedExpression.NEVER_NULL, NO_CODE, rowType)
  }

  private def generateNonLiteralRow(
      ctx: CodeGeneratorContext,
      rowType: RowType,
      elements: Seq[GeneratedExpression]): GeneratedExpression = {

    val rowTerm = newName("row")
    val writerTerm = newName("writer")
    val writerCls = className[BinaryRowWriter]

    val writeCode = elements.zipWithIndex.map {
      case (element, idx) =>
        val tpe = rowType.getTypeAt(idx)
        if (ctx.nullCheck) {
          s"""
             |${element.code}
             |if (${element.nullTerm}) {
             |  ${binaryWriterWriteNull(idx, writerTerm, tpe)};
             |} else {
             |  ${binaryWriterWriteField(ctx, idx, element.resultTerm, writerTerm, tpe)};
             |}
           """.stripMargin
        } else {
          s"""
             |${element.code}
             |${binaryWriterWriteField(ctx, idx, element.resultTerm, writerTerm, tpe)};
           """.stripMargin
        }
    }.mkString("\n")

    val code =
      s"""
         |$writerTerm.reset();
         |$writeCode
         |$writerTerm.complete();
       """.stripMargin

    ctx.addReusableMember(s"$BINARY_ROW $rowTerm = new $BINARY_ROW(${rowType.getArity});")
    ctx.addReusableMember(s"$writerCls $writerTerm = new $writerCls($rowTerm);")
    GeneratedExpression(rowTerm, GeneratedExpression.NEVER_NULL, code, rowType)
  }

  def generateArray(
      ctx: CodeGeneratorContext,
      resultType: InternalType,
      elements: Seq[GeneratedExpression]): GeneratedExpression = {

    checkArgument(resultType.isInstanceOf[ArrayType])
    val arrayType = resultType.asInstanceOf[ArrayType]
    val elementType = arrayType.getElementType
    val isLiteral = elements.forall(e => e.literal)
    val isPrimitive = elementType.isInstanceOf[PrimitiveType]

    if (isLiteral) {
      // generate literal array
      generateLiteralArray(ctx, arrayType, elements)
    } else {
      if (isPrimitive) {
        // generate primitive array
        val mapped = elements.map { element =>
          if (element.literal) {
            element
          } else {
            val resultTerm = primitiveDefaultValue(elementType)
            GeneratedExpression(resultTerm, ALWAYS_NULL, NO_CODE, elementType, Some(null))
          }
        }
        val array = generateLiteralArray(ctx, arrayType, mapped)
        val code = generatePrimitiveArrayUpdateCode(ctx, array.resultTerm, elementType, elements)
        GeneratedExpression(array.resultTerm, GeneratedExpression.NEVER_NULL, code, arrayType)
      } else {
        // generate general array
        generateNonLiteralArray(ctx, arrayType, elements)
      }
    }
  }

  private def generatePrimitiveArrayUpdateCode(
      ctx: CodeGeneratorContext,
      arrayTerm: String,
      elementType: InternalType,
      elements: Seq[GeneratedExpression]): String = {
    elements.zipWithIndex.map { case (element, idx) =>
      if (element.literal) {
        ""
      } else if (ctx.nullCheck) {
        s"""
           |${element.code}
           |if (${element.nullTerm}) {
           |  ${binaryArraySetNull(idx, arrayTerm, elementType)};
           |} else {
           |  ${binaryRowFieldSetAccess(
          idx, arrayTerm, elementType, element.resultTerm)};
           |}
             """.stripMargin
      } else {
        s"""
           |${element.code}
           |${binaryRowFieldSetAccess(
          idx, arrayTerm, elementType, element.resultTerm)};
             """.stripMargin
      }
    }.mkString("\n")
  }

  private def generateLiteralArray(
      ctx: CodeGeneratorContext,
      arrayType: ArrayType,
      elements: Seq[GeneratedExpression]): GeneratedExpression = {
    checkArgument(elements.forall(e => e.literal))
    val expr = generateNonLiteralArray(ctx, arrayType, elements)
    ctx.addReusableInitStatement(expr.code)
    GeneratedExpression(expr.resultTerm, GeneratedExpression.NEVER_NULL, NO_CODE, arrayType)
  }

  private def generateNonLiteralArray(
      ctx: CodeGeneratorContext,
      arrayType: ArrayType,
      elements: Seq[GeneratedExpression]): GeneratedExpression = {

    val elementType = arrayType.getElementType
    val arrayTerm = newName("array")
    val writerTerm = newName("writer")
    val writerCls = className[BinaryArrayWriter]
    val elementSize = BinaryArray.calculateFixLengthPartSize(elementType)

    val writeCode = elements.zipWithIndex.map {
      case (element, idx) =>
        s"""
           |${element.code}
           |if (${element.nullTerm}) {
           |  ${binaryArraySetNull(idx, writerTerm, elementType)};
           |} else {
           |  ${binaryWriterWriteField(ctx, idx, element.resultTerm, writerTerm, elementType)};
           |}
          """.stripMargin
    }.mkString("\n")

    val code =
      s"""
         |$writerTerm.reset();
         |$writeCode
         |$writerTerm.complete();
         """.stripMargin

    val memberStmt =
      s"""
         |$BINARY_ARRAY $arrayTerm = new $BINARY_ARRAY();
         |$writerCls $writerTerm = new $writerCls($arrayTerm, ${elements.length}, $elementSize);
       """.stripMargin

    ctx.addReusableMember(memberStmt)
    GeneratedExpression(arrayTerm, GeneratedExpression.NEVER_NULL, code, arrayType)
  }

  def generateArrayElementAt(
      ctx: CodeGeneratorContext,
      array: GeneratedExpression,
      index: GeneratedExpression): GeneratedExpression = {
    val Seq(resultTerm, nullTerm) = newNames("result", "isNull")
    val componentInfo = array.resultType.asInstanceOf[ArrayType].getElementType
    val resultTypeTerm = primitiveTypeTermForType(componentInfo)
    val defaultTerm = primitiveDefaultValue(componentInfo)

    val idxStr = s"${index.resultTerm} - 1"
    val arrayIsNull = s"${array.resultTerm}.isNullAt($idxStr)"
    val arrayGet =
      baseRowFieldReadAccess(ctx, idxStr, array.resultTerm, componentInfo)

    val arrayAccessCode =
      s"""
         |${array.code}
         |${index.code}
         |boolean $nullTerm = ${array.nullTerm} || ${index.nullTerm} || $arrayIsNull;
         |$resultTypeTerm $resultTerm = $nullTerm ? $defaultTerm : $arrayGet;
         |""".stripMargin

    GeneratedExpression(resultTerm, nullTerm, arrayAccessCode, componentInfo)
  }

  def generateArrayElement(
      ctx: CodeGeneratorContext,
      array: GeneratedExpression): GeneratedExpression = {
    val Seq(resultTerm, nullTerm) = newNames("result", "isNull")
    val resultType = array.resultType.asInstanceOf[ArrayType].getElementType
    val resultTypeTerm = primitiveTypeTermForType(resultType)
    val defaultValue = primitiveDefaultValue(resultType)

    val arrayLengthCode = s"${array.nullTerm} ? 0 : ${array.resultTerm}.numElements()"

    val arrayGet = baseRowFieldReadAccess(ctx, 0, array.resultTerm, resultType)
    val arrayAccessCode =
      s"""
         |${array.code}
         |boolean $nullTerm;
         |$resultTypeTerm $resultTerm;
         |switch ($arrayLengthCode) {
         |  case 0:
         |    $nullTerm = true;
         |    $resultTerm = $defaultValue;
         |    break;
         |  case 1:
         |    $nullTerm = ${array.resultTerm}.isNullAt(0);
         |    $resultTerm = $nullTerm ? $defaultValue : $arrayGet;
         |    break;
         |  default:
         |    throw new RuntimeException("Array has more than one element.");
         |}
         |""".stripMargin

    GeneratedExpression(resultTerm, nullTerm, arrayAccessCode, resultType)
  }

  def generateArrayCardinality(
      ctx: CodeGeneratorContext,
      array: GeneratedExpression)
    : GeneratedExpression = {
    generateUnaryOperatorIfNotNull(ctx, InternalTypes.INT, array) {
      _ => s"${array.resultTerm}.numElements()"
    }
  }

  def generateMap(
      ctx: CodeGeneratorContext,
      resultType: InternalType,
      elements: Seq[GeneratedExpression]): GeneratedExpression = {

    checkArgument(resultType.isInstanceOf[MapType])
    val mapType = resultType.asInstanceOf[MapType]
    val mapTerm = newName("map")

    // prepare map key array
    val keyElements = elements.grouped(2).map { case Seq(key, _) => key }.toSeq
    val keyType = mapType.getKeyType
    val keyExpr = generateArray(ctx, InternalTypes.createArrayType(keyType), keyElements)
    val isKeyFixLength = keyType.isInstanceOf[PrimitiveType]

    // prepare map value array
    val valueElements = elements.grouped(2).map { case Seq(_, value) => value }.toSeq
    val valueType = mapType.getValueType
    val valueExpr = generateArray(ctx, InternalTypes.createArrayType(valueType), valueElements)
    val isValueFixLength = valueType.isInstanceOf[PrimitiveType]

    // construct binary map
    ctx.addReusableMember(s"$BINARY_MAP $mapTerm = null;")

    val code = if (isKeyFixLength && isValueFixLength) {
      // the key and value are fixed length, initialize and reuse the map in constructor
      val init = s"$mapTerm = $BINARY_MAP.valueOf(${keyExpr.resultTerm}, ${valueExpr.resultTerm});"
      ctx.addReusableInitStatement(init)
      // there are some non-literal primitive fields need to update
      val keyArrayTerm = newName("keyArray")
      val valueArrayTerm = newName("valueArray")
      val keyUpdate = generatePrimitiveArrayUpdateCode(
        ctx, keyArrayTerm, keyType, keyElements)
      val valueUpdate = generatePrimitiveArrayUpdateCode(
        ctx, valueArrayTerm, valueType, valueElements)
      s"""
         |$BINARY_ARRAY $keyArrayTerm = $mapTerm.keyArray();
         |$keyUpdate
         |$BINARY_ARRAY $valueArrayTerm = $mapTerm.valueArray();
         |$valueUpdate
       """.stripMargin
    } else {
      // the key or value is not fixed length, re-create the map on every update
      s"""
         |${keyExpr.code}
         |${valueExpr.code}
         |$mapTerm = $BINARY_MAP.valueOf(${keyExpr.resultTerm}, ${valueExpr.resultTerm});
       """.stripMargin
    }
    GeneratedExpression(mapTerm, NEVER_NULL, code, resultType)
  }

  def generateMapGet(
      ctx: CodeGeneratorContext,
      map: GeneratedExpression,
      key: GeneratedExpression): GeneratedExpression = {
    val Seq(resultTerm, nullTerm) = newNames("result", "isNull")
    val tmpKey = newName("key")
    val length = newName("length")
    val keys = newName("keys")
    val values = newName("values")
    val index = newName("index")
    val found = newName("found")

    val mapType = map.resultType.asInstanceOf[MapType]
    val keyType = mapType.getKeyType
    val valueType = mapType.getValueType

    // use primitive for key as key is not null
    val keyTypeTerm = primitiveTypeTermForType(keyType)
    val valueTypeTerm = primitiveTypeTermForType(valueType)
    val valueDefault = primitiveDefaultValue(valueType)

    val mapTerm = map.resultTerm

    val equal = generateEquals(ctx, key, GeneratedExpression(tmpKey, NEVER_NULL, NO_CODE, keyType))
    val code =
      s"""
         |final int $length = $mapTerm.numElements();
         |final $BINARY_ARRAY $keys = $mapTerm.keyArray();
         |final $BINARY_ARRAY $values = $mapTerm.valueArray();
         |
         |int $index = 0;
         |boolean $found = false;
         |if (${key.nullTerm}) {
         |  while ($index < $length && !$found) {
         |    if ($keys.isNullAt($index)) {
         |      $found = true;
         |    } else {
         |      $index++;
         |    }
         |  }
         |} else {
         |  while ($index < $length && !$found) {
         |    final $keyTypeTerm $tmpKey = ${baseRowFieldReadAccess(ctx, index, keys, keyType)};
         |    ${equal.code}
         |    if (${equal.resultTerm}) {
         |      $found = true;
         |    } else {
         |      $index++;
         |    }
         |  }
         |}
         |
         |if (!$found || $values.isNullAt($index)) {
         |  $nullTerm = true;
         |} else {
         |  $resultTerm = ${baseRowFieldReadAccess(ctx, index, values, valueType)};
         |}
        """.stripMargin

    val accessCode =
      s"""
         |${map.code}
         |${key.code}
         |boolean $nullTerm = (${map.nullTerm} || ${key.nullTerm});
         |$valueTypeTerm $resultTerm = $valueDefault;
         |if (!$nullTerm) {
         | $code
         |}
        """.stripMargin

    GeneratedExpression(resultTerm, nullTerm, accessCode, valueType)
  }

  def generateMapCardinality(
      ctx: CodeGeneratorContext,
      map: GeneratedExpression): GeneratedExpression = {
    generateUnaryOperatorIfNotNull(ctx, InternalTypes.INT, map) {
      _ => s"${map.resultTerm}.numElements()"
    }
  }

  // ----------------------------------------------------------------------------------------
  // private generate utils
  // ----------------------------------------------------------------------------------------

  private def generateCastStringLiteralToDateTime(
      ctx: CodeGeneratorContext,
      stringLiteral: GeneratedExpression,
      expectType: InternalType): GeneratedExpression = {
    checkArgument(stringLiteral.literal)
    val rightTerm = stringLiteral.resultTerm
    val typeTerm = primitiveTypeTermForType(expectType)
    val defaultTerm = primitiveDefaultValue(expectType)
    val term = newName("stringToTime")
    val zoneTerm = ctx.addReusableTimeZone()
    val code = stringToInternalCode(expectType, rightTerm, zoneTerm)
    val stmt = s"$typeTerm $term = ${stringLiteral.nullTerm} ? $defaultTerm : $code;"
    ctx.addReusableMember(stmt)
    stringLiteral.copy(resultType = expectType, resultTerm = term)
  }

  private def generateCastArrayToString(
      ctx: CodeGeneratorContext,
      operand: GeneratedExpression,
      at: ArrayType): GeneratedExpression =
    generateStringResultCallWithStmtIfArgsNotNull(ctx, Seq(operand)) {
      terms =>
        val builderCls = classOf[JStringBuilder].getCanonicalName
        val builderTerm = newName("builder")
        ctx.addReusableMember(s"""$builderCls $builderTerm = new $builderCls();""")

        val arrayTerm = terms.head

        val indexTerm = newName("i")
        val numTerm = newName("num")

        val elementType = at.getElementType
        val elementCls = primitiveTypeTermForType(elementType)
        val elementTerm = newName("element")
        val elementNullTerm = newName("isNull")
        val elementCode =
          s"""
             |$elementCls $elementTerm = ${primitiveDefaultValue(elementType)};
             |boolean $elementNullTerm = $arrayTerm.isNullAt($indexTerm);
             |if (!$elementNullTerm) {
             |  $elementTerm = ($elementCls) ${
            baseRowFieldReadAccess(ctx, indexTerm, arrayTerm, elementType)};
             |}
             """.stripMargin
        val elementExpr = GeneratedExpression(
          elementTerm, elementNullTerm, elementCode, elementType)
        val castExpr = generateCast(ctx, elementExpr, InternalTypes.STRING)

        val stmt =
          s"""
             |$builderTerm.setLength(0);
             |$builderTerm.append("[");
             |int $numTerm = $arrayTerm.numElements();
             |for (int $indexTerm = 0; $indexTerm < $numTerm; $indexTerm++) {
             |  if ($indexTerm != 0) {
             |    $builderTerm.append(", ");
             |  }
             |
             |  ${castExpr.code}
             |  if (${castExpr.nullTerm}) {
             |    $builderTerm.append("null");
             |  } else {
             |    $builderTerm.append(${castExpr.resultTerm});
             |  }
             |}
             |$builderTerm.append("]");
             """.stripMargin
        (stmt, s"$builderTerm.toString()")
    }

  private def generateCastMapToString(
      ctx: CodeGeneratorContext,
      operand: GeneratedExpression,
      mt: MapType): GeneratedExpression =
    generateStringResultCallWithStmtIfArgsNotNull(ctx, Seq(operand)) {
      terms =>
        val resultTerm = newName("toStringResult")

        val builderCls = classOf[JStringBuilder].getCanonicalName
        val builderTerm = newName("builder")
        ctx.addReusableMember(s"$builderCls $builderTerm = new $builderCls();")

        val binaryMapTerm = terms.head
        val arrayCls = classOf[BinaryArray].getCanonicalName
        val keyArrayTerm = newName("keyArray")
        val valueArrayTerm = newName("valueArray")

        val indexTerm = newName("i")
        val numTerm = newName("num")

        val keyType = mt.getKeyType
        val keyCls = primitiveTypeTermForType(keyType)
        val keyTerm = newName("key")
        val keyNullTerm = newName("isNull")
        val keyCode =
          s"""
             |$keyCls $keyTerm = ${primitiveDefaultValue(keyType)};
             |boolean $keyNullTerm = $keyArrayTerm.isNullAt($indexTerm);
             |if (!$keyNullTerm) {
             |  $keyTerm = ($keyCls) ${
            baseRowFieldReadAccess(ctx, indexTerm, keyArrayTerm, keyType)};
             |}
             """.stripMargin
        val keyExpr = GeneratedExpression(keyTerm, keyNullTerm, keyCode, keyType)
        val keyCastExpr = generateCast(ctx, keyExpr, InternalTypes.STRING)

        val valueType = mt.getValueType
        val valueCls = primitiveTypeTermForType(valueType)
        val valueTerm = newName("value")
        val valueNullTerm = newName("isNull")
        val valueCode =
          s"""
             |$valueCls $valueTerm = ${primitiveDefaultValue(valueType)};
             |boolean $valueNullTerm = $valueArrayTerm.isNullAt($indexTerm);
             |if (!$valueNullTerm) {
             |  $valueTerm = ($valueCls) ${
            baseRowFieldReadAccess(ctx, indexTerm, valueArrayTerm, valueType)};
             |}
             """.stripMargin
        val valueExpr = GeneratedExpression(valueTerm, valueNullTerm, valueCode, valueType)
        val valueCastExpr = generateCast(ctx, valueExpr, InternalTypes.STRING)

        val stmt =
          s"""
             |String $resultTerm;
             |$arrayCls $keyArrayTerm = $binaryMapTerm.keyArray();
             |$arrayCls $valueArrayTerm = $binaryMapTerm.valueArray();
             |
             |$builderTerm.setLength(0);
             |$builderTerm.append("{");
             |
             |int $numTerm = $binaryMapTerm.numElements();
             |for (int $indexTerm = 0; $indexTerm < $numTerm; $indexTerm++) {
             |  if ($indexTerm != 0) {
             |    $builderTerm.append(", ");
             |  }
             |
             |  ${keyCastExpr.code}
             |  if (${keyCastExpr.nullTerm}) {
             |    $builderTerm.append("null");
             |  } else {
             |    $builderTerm.append(${keyCastExpr.resultTerm});
             |  }
             |  $builderTerm.append("=");
             |
             |  ${valueCastExpr.code}
             |  if (${valueCastExpr.nullTerm}) {
             |    $builderTerm.append("null");
             |  } else {
             |    $builderTerm.append(${valueCastExpr.resultTerm});
             |  }
             |}
             |$builderTerm.append("}");
             |
             |$resultTerm = $builderTerm.toString();
             """.stripMargin
        (stmt, resultTerm)
    }

  private def generateCastBaseRowToString(
      ctx: CodeGeneratorContext,
      operand: GeneratedExpression,
      brt: RowType): GeneratedExpression =
    generateStringResultCallWithStmtIfArgsNotNull(ctx, Seq(operand)) {
      terms =>
        val builderCls = classOf[JStringBuilder].getCanonicalName
        val builderTerm = newName("builder")
        ctx.addReusableMember(s"""$builderCls $builderTerm = new $builderCls();""")

        val rowTerm = terms.head

        val appendCode = brt.getFieldTypes.zipWithIndex.map {
          case (elementType, idx) =>
            val elementCls = primitiveTypeTermForType(elementType)
            val elementTerm = newName("element")
            val elementExpr = GeneratedExpression(
              elementTerm, s"$rowTerm.isNullAt($idx)",
              s"$elementCls $elementTerm = ($elementCls) ${baseRowFieldReadAccess(
                ctx, idx, rowTerm, elementType)};", elementType)
            val castExpr = generateCast(ctx, elementExpr, InternalTypes.STRING)
            s"""
               |${if (idx != 0) s"""$builderTerm.append(",");""" else ""}
               |${castExpr.code}
               |if (${castExpr.nullTerm}) {
               |  $builderTerm.append("null");
               |} else {
               |  $builderTerm.append(${castExpr.resultTerm});
               |}
               """.stripMargin
        }.mkString("\n")

        val stmt =
          s"""
             |$builderTerm.setLength(0);
             |$builderTerm.append("(");
             |$appendCode
             |$builderTerm.append(")");
             """.stripMargin
        (stmt, s"$builderTerm.toString()")
    }

  private def generateArrayComparison(
      ctx: CodeGeneratorContext,
      left: GeneratedExpression,
      right: GeneratedExpression): GeneratedExpression =
    generateCallWithStmtIfArgsNotNull(ctx, InternalTypes.BOOLEAN, Seq(left, right)) {
      args =>
        val leftTerm = args.head
        val rightTerm = args(1)

        val resultTerm = newName("compareResult")
        val binaryArrayCls = classOf[BinaryArray].getCanonicalName

        val elementType = left.resultType.asInstanceOf[ArrayType].getElementType
        val elementCls = primitiveTypeTermForType(elementType)
        val elementDefault = primitiveDefaultValue(elementType)

        val leftElementTerm = newName("leftElement")
        val leftElementNullTerm = newName("leftElementIsNull")
        val leftElementExpr =
          GeneratedExpression(leftElementTerm, leftElementNullTerm, "", elementType)

        val rightElementTerm = newName("rightElement")
        val rightElementNullTerm = newName("rightElementIsNull")
        val rightElementExpr =
          GeneratedExpression(rightElementTerm, rightElementNullTerm, "", elementType)

        val indexTerm = newName("index")
        val elementEqualsExpr = generateEquals(ctx, leftElementExpr, rightElementExpr)

        val stmt =
          s"""
             |boolean $resultTerm;
             |if ($leftTerm instanceof $binaryArrayCls && $rightTerm instanceof $binaryArrayCls) {
             |  $resultTerm = $leftTerm.equals($rightTerm);
             |} else {
             |  if ($leftTerm.numElements() == $rightTerm.numElements()) {
             |    $resultTerm = true;
             |    for (int $indexTerm = 0; $indexTerm < $leftTerm.numElements(); $indexTerm++) {
             |      $elementCls $leftElementTerm = $elementDefault;
             |      boolean $leftElementNullTerm = $leftTerm.isNullAt($indexTerm);
             |      if (!$leftElementNullTerm) {
             |        $leftElementTerm =
             |          ${baseRowFieldReadAccess(ctx, indexTerm, leftTerm, elementType)};
             |      }
             |
             |      $elementCls $rightElementTerm = $elementDefault;
             |      boolean $rightElementNullTerm = $rightTerm.isNullAt($indexTerm);
             |      if (!$rightElementNullTerm) {
             |        $rightElementTerm =
             |          ${baseRowFieldReadAccess(ctx, indexTerm, rightTerm, elementType)};
             |      }
             |
             |      ${elementEqualsExpr.code}
             |      if (!${elementEqualsExpr.resultTerm}) {
             |        $resultTerm = false;
             |        break;
             |      }
             |    }
             |  } else {
             |    $resultTerm = false;
             |  }
             |}
             """.stripMargin
        (stmt, resultTerm)
    }

  private def generateMapComparison(
      ctx: CodeGeneratorContext,
      left: GeneratedExpression,
      right: GeneratedExpression): GeneratedExpression =
    generateCallWithStmtIfArgsNotNull(ctx, InternalTypes.BOOLEAN, Seq(left, right)) {
      args =>
        val leftTerm = args.head
        val rightTerm = args(1)
        val resultTerm = newName("compareResult")
        val stmt = s"boolean $resultTerm = $leftTerm.equals($rightTerm);"
        (stmt, resultTerm)
    }
  
  // ------------------------------------------------------------------------------------------

  private def generateUnaryOperatorIfNotNull(
      ctx: CodeGeneratorContext,
      returnType: InternalType,
      operand: GeneratedExpression,
      resultNullable: Boolean = false)
      (expr: String => String): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, returnType, Seq(operand), resultNullable) {
      args => expr(args.head)
    }
  }

  private def generateOperatorIfNotNull(
      ctx: CodeGeneratorContext,
      returnType: InternalType,
      left: GeneratedExpression,
      right: GeneratedExpression,
      resultNullable: Boolean = false)
      (expr: (String, String) => String)
    : GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, returnType, Seq(left, right), resultNullable) {
      args => expr(args.head, args(1))
    }
  }

  // ----------------------------------------------------------------------------------------------

  private def internalExprCasting(
      expr: GeneratedExpression,
      targetType: InternalType)
    : GeneratedExpression = {
    expr.copy(resultType = targetType)
  }

  private def numericCasting(
      operandType: InternalType,
      resultType: InternalType): String => String = {

    val resultTypeTerm = primitiveTypeTermForType(resultType)

    def decToPrimMethod(targetType: InternalType): String = targetType match {
      case InternalTypes.BYTE => "castToByte"
      case InternalTypes.SHORT => "castToShort"
      case InternalTypes.INT => "castToInt"
      case InternalTypes.LONG => "castToLong"
      case InternalTypes.FLOAT => "castToFloat"
      case InternalTypes.DOUBLE => "castToDouble"
      case InternalTypes.BOOLEAN => "castToBoolean"
      case _ => throw new CodeGenException(s"Unsupported decimal casting type: '$targetType'")
    }

    // no casting necessary
    if (operandType == resultType) {
      operandTerm => s"$operandTerm"
    }
    // decimal to decimal, may have different precision/scale
    else if (isDecimal(resultType) && isDecimal(operandType)) {
      val dt = resultType.asInstanceOf[DecimalType]
      operandTerm =>
        s"$DECIMAL.castToDecimal($operandTerm, ${dt.precision()}, ${dt.scale()})"
    }
    // non_decimal_numeric to decimal
    else if (isDecimal(resultType) && isNumeric(operandType)) {
      val dt = resultType.asInstanceOf[DecimalType]
      operandTerm =>
        s"$DECIMAL.castFrom($operandTerm, ${dt.precision()}, ${dt.scale()})"
    }
    // decimal to non_decimal_numeric
    else if (isNumeric(resultType) && isDecimal(operandType) ) {
      operandTerm =>
        s"$DECIMAL.${decToPrimMethod(resultType)}($operandTerm)"
    }
    // numeric to numeric
    // TODO: Create a wrapper layer that handles type conversion between numeric.
    else if (isNumeric(operandType) && isNumeric(resultType)) {
      val resultTypeValue = resultTypeTerm + "Value()"
      val boxedTypeTerm = boxedTypeTermForType(operandType)
      operandTerm =>
        s"(new $boxedTypeTerm($operandTerm)).$resultTypeValue"
    }
    // result type is time interval and operand type is integer
    else if (isTimeInterval(resultType) && isInteger(operandType)){
      operandTerm => s"(($resultTypeTerm) $operandTerm)"
    }
    else {
      throw new CodeGenException(s"Unsupported casting from $operandType to $resultType.")
    }
  }

  private def stringToInternalCode(
      targetType: InternalType,
      operandTerm: String,
      zoneTerm: String): String =
    targetType match {
      case InternalTypes.DATE =>
        s"${qualifyMethod(BuiltInMethod.STRING_TO_DATE.method)}($operandTerm.toString())"
      case InternalTypes.TIME =>
        s"${qualifyMethod(BuiltInMethod.STRING_TO_TIME.method)}($operandTerm.toString())"
      case InternalTypes.TIMESTAMP =>
        s"""${qualifyMethod(BuiltInMethods.STRING_TO_TIMESTAMP)}($operandTerm.toString(),
           | $zoneTerm)""".stripMargin
      case _ => throw new UnsupportedOperationException
    }

  private def internalToStringCode(
      fromType: InternalType,
      operandTerm: String,
      zoneTerm: String): String =
    fromType match {
      case InternalTypes.DATE =>
        s"${qualifyMethod(BuiltInMethod.UNIX_DATE_TO_STRING.method)}($operandTerm)"
      case InternalTypes.TIME =>
        s"${qualifyMethod(BuiltInMethods.UNIX_TIME_TO_STRING)}($operandTerm)"
      case _: TimestampType => // including rowtime indicator
        s"${qualifyMethod(BuiltInMethods.TIMESTAMP_TO_STRING)}($operandTerm, 3, $zoneTerm)"
    }

}
