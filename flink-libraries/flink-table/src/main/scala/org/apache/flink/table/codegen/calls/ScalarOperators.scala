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

import java.math.MathContext

import org.apache.calcite.avatica.util.DateTimeUtils.MILLIS_PER_DAY
import org.apache.calcite.avatica.util.{DateTimeUtils, TimeUnitRange}
import org.apache.calcite.util.BuiltInMethod
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo._
import org.apache.flink.api.java.typeutils.{MapTypeInfo, ObjectArrayTypeInfo, RowTypeInfo}
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.calls.CallGenerator.generateCallIfArgsNotNull
import org.apache.flink.table.codegen.{CodeGenException, CodeGenerator, GeneratedExpression}
import org.apache.flink.table.typeutils.TypeCheckUtils._
import org.apache.flink.table.typeutils.{TimeIndicatorTypeInfo, TimeIntervalTypeInfo, TypeCoercion}

object ScalarOperators {

  def generateStringConcatOperator(
      nullCheck: Boolean,
      left: GeneratedExpression,
      right: GeneratedExpression)
    : GeneratedExpression = {
    generateOperatorIfNotNull(nullCheck, STRING_TYPE_INFO, left, right) {
      (leftTerm, rightTerm) => s"$leftTerm + $rightTerm"
    }
  }

  def generateArithmeticOperator(
      operator: String,
      nullCheck: Boolean,
      resultType: TypeInformation[_],
      left: GeneratedExpression,
      right: GeneratedExpression,
      config: TableConfig): GeneratedExpression = {

    val leftCasting = operator match {
      case "%" =>
        if (left.resultType == right.resultType) {
          numericCasting(left.resultType, resultType)
        } else {
          val castedType = if (isDecimal(left.resultType)) {
            Types.LONG
          } else {
            left.resultType
          }
          numericCasting(left.resultType, castedType)
        }
      case _ => numericCasting(left.resultType, resultType)
    }
    val rightCasting = numericCasting(right.resultType, resultType)
    val resultTypeTerm = primitiveTypeTermForTypeInfo(resultType)

    generateOperatorIfNotNull(nullCheck, resultType, left, right) {
      (leftTerm, rightTerm) =>
        if (isDecimal(resultType)) {
          val decMethod = arithOpToDecMethod(operator)
          operator match {
            // include math context for decimal division
            case "/" =>
              val mathContext = mathContextToString(config.getDecimalContext)
              s"${leftCasting(leftTerm)}.$decMethod(${rightCasting(rightTerm)}, $mathContext)"
            case _ =>
              s"${leftCasting(leftTerm)}.$decMethod(${rightCasting(rightTerm)})"
          }
        } else {
          s"($resultTypeTerm) (${leftCasting(leftTerm)} $operator ${rightCasting(rightTerm)})"
        }
    }
  }

  def generateUnaryArithmeticOperator(
      operator: String,
      nullCheck: Boolean,
      resultType: TypeInformation[_],
      operand: GeneratedExpression)
    : GeneratedExpression = {
    generateUnaryOperatorIfNotNull(nullCheck, resultType, operand) {
      (operandTerm) =>
        if (isDecimal(operand.resultType) && operator == "-") {
          s"$operandTerm.negate()"
        } else if (isDecimal(operand.resultType) && operator == "+") {
          s"$operandTerm"
        } else {
          s"$operator($operandTerm)"
        }
    }
  }

  def generateIn(
      codeGenerator: CodeGenerator,
      needle: GeneratedExpression,
      haystack: Seq[GeneratedExpression])
    : GeneratedExpression = {

    // determine common numeric type
    val widerType = TypeCoercion.widerTypeOf(needle.resultType, haystack.head.resultType)

    // we need to normalize the values for the hash set
    // decimals are converted to a normalized string
    val castNumeric = widerType match {

      case Some(t) => (value: GeneratedExpression) =>
        val casted = numericCasting(value.resultType, t)(value.resultTerm)
        if (isDecimal(t)) {
          s"$casted.stripTrailingZeros().toEngineeringString()"
        } else {
          casted
        }

      case None => (value: GeneratedExpression) =>
        if (isDecimal(value.resultType)) {
          s"${value.resultTerm}.stripTrailingZeros().toEngineeringString()"
        } else {
          value.resultTerm
        }
    }

    // add elements to hash set if they are constant
    if (haystack.forall(_.literal)) {
      val elements = haystack.map { element =>
        element.copy(
            castNumeric(element), // cast element to wider type
            element.nullTerm,
            element.code,
            widerType.getOrElse(needle.resultType))
      }
      val setTerm = codeGenerator.addReusableSet(elements)

      val castedNeedle = needle.copy(
        castNumeric(needle), // cast needle to wider type
        needle.nullTerm,
        needle.code,
        widerType.getOrElse(needle.resultType))

      val resultTerm = newName("result")
      val nullTerm = newName("isNull")
      val resultTypeTerm = primitiveTypeTermForTypeInfo(BOOLEAN_TYPE_INFO)
      val defaultValue = primitiveDefaultValue(BOOLEAN_TYPE_INFO)

      val operatorCode = if (codeGenerator.nullCheck) {
        s"""
          |${castedNeedle.code}
          |$resultTypeTerm $resultTerm;
          |boolean $nullTerm;
          |if (!${castedNeedle.nullTerm}) {
          |  $resultTerm = $setTerm.contains(${castedNeedle.resultTerm});
          |  $nullTerm = !$resultTerm && $setTerm.contains(null);
          |}
          |else {
          |  $resultTerm = $defaultValue;
          |  $nullTerm = true;
          |}
          |""".stripMargin
      }
      else {
        s"""
          |${castedNeedle.code}
          |$resultTypeTerm $resultTerm = $setTerm.contains(${castedNeedle.resultTerm});
          |""".stripMargin
      }

      GeneratedExpression(resultTerm, nullTerm, operatorCode, BOOLEAN_TYPE_INFO)
    } else {
      // we use a chain of ORs for a set that contains non-constant elements
      haystack
        .map(generateEquals(codeGenerator.nullCheck, needle, _))
        .reduce( (left, right) =>
          generateOr(
            nullCheck = true,
            left,
            right
          )
        )
    }
  }  
  
  def generateEquals(
      nullCheck: Boolean,
      left: GeneratedExpression,
      right: GeneratedExpression)
    : GeneratedExpression = {
    // numeric types
    if (isNumeric(left.resultType) && isNumeric(right.resultType)) {
      generateComparison("==", nullCheck, left, right)
    }
    // temporal types
    else if (isTemporal(left.resultType) && left.resultType == right.resultType) {
      generateComparison("==", nullCheck, left, right)
    }
    // array types
    else if (isArray(left.resultType) &&
        left.resultType.getTypeClass == right.resultType.getTypeClass) {
      generateOperatorIfNotNull(nullCheck, BOOLEAN_TYPE_INFO, left, right) {
        (leftTerm, rightTerm) => s"java.util.Arrays.equals($leftTerm, $rightTerm)"
      }
    }
    // map types
    else if (isMap(left.resultType) &&
      left.resultType.getTypeClass == right.resultType.getTypeClass) {
      generateOperatorIfNotNull(nullCheck, BOOLEAN_TYPE_INFO, left, right) {
        (leftTerm, rightTerm) => s"$leftTerm.equals($rightTerm)"
      }
    }
    // comparable types of same type
    else if (isComparable(left.resultType) && left.resultType == right.resultType) {
      generateComparison("==", nullCheck, left, right)
    }
    // non comparable types
    else {
      generateOperatorIfNotNull(nullCheck, BOOLEAN_TYPE_INFO, left, right) {
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
      nullCheck: Boolean,
      left: GeneratedExpression,
      right: GeneratedExpression)
    : GeneratedExpression = {
    // numeric types
    if (isNumeric(left.resultType) && isNumeric(right.resultType)) {
      generateComparison("!=", nullCheck, left, right)
    }
    // temporal types
    else if (isTemporal(left.resultType) && left.resultType == right.resultType) {
      generateComparison("!=", nullCheck, left, right)
    }
    // array types
    else if (isArray(left.resultType) &&
        left.resultType.getTypeClass == right.resultType.getTypeClass) {
      generateOperatorIfNotNull(nullCheck, BOOLEAN_TYPE_INFO, left, right) {
        (leftTerm, rightTerm) => s"!java.util.Arrays.equals($leftTerm, $rightTerm)"
      }
    }
    // map types
    else if (isMap(left.resultType) &&
      left.resultType.getTypeClass == right.resultType.getTypeClass) {
      generateOperatorIfNotNull(nullCheck, BOOLEAN_TYPE_INFO, left, right) {
        (leftTerm, rightTerm) => s"!($leftTerm.equals($rightTerm))"
      }
    }
    // comparable types
    else if (isComparable(left.resultType) && left.resultType == right.resultType) {
      generateComparison("!=", nullCheck, left, right)
    }
    // non-comparable types
    else {
      generateOperatorIfNotNull(nullCheck, BOOLEAN_TYPE_INFO, left, right) {
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
      operator: String,
      nullCheck: Boolean,
      left: GeneratedExpression,
      right: GeneratedExpression)
    : GeneratedExpression = {
    generateOperatorIfNotNull(nullCheck, BOOLEAN_TYPE_INFO, left, right) {
      // left is decimal or both sides are decimal
      if (isDecimal(left.resultType) && isNumeric(right.resultType)) {
        (leftTerm, rightTerm) => {
          val operandCasting = numericCasting(right.resultType, left.resultType)
          s"$leftTerm.compareTo(${operandCasting(rightTerm)}) $operator 0"
        }
      }
      // right is decimal
      else if (isNumeric(left.resultType) && isDecimal(right.resultType)) {
        (leftTerm, rightTerm) => {
          val operandCasting = numericCasting(left.resultType, right.resultType)
          s"${operandCasting(leftTerm)}.compareTo($rightTerm) $operator 0"
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
          case _ => throw new CodeGenException(s"Unsupported boolean comparison '$operator'.")
        }
      }
      // both sides are same comparable type
      else if (isComparable(left.resultType) && left.resultType == right.resultType) {
        (leftTerm, rightTerm) => s"$leftTerm.compareTo($rightTerm) $operator 0"
      }
      else {
        throw new CodeGenException(s"Incomparable types: ${left.resultType} and " +
            s"${right.resultType}")
      }
    }
  }

  def generateIsNull(
      nullCheck: Boolean,
      operand: GeneratedExpression)
    : GeneratedExpression = {
    val resultTerm = newName("result")
    val nullTerm = newName("isNull")
    val operatorCode = if (nullCheck) {
      s"""
        |${operand.code}
        |boolean $resultTerm = ${operand.nullTerm};
        |boolean $nullTerm = false;
        |""".stripMargin
    }
    else if (!nullCheck && isReference(operand)) {
      s"""
        |${operand.code}
        |boolean $resultTerm = ${operand.resultTerm} == null;
        |boolean $nullTerm = false;
        |""".stripMargin
    }
    else {
      s"""
        |${operand.code}
        |boolean $resultTerm = false;
        |""".stripMargin
    }

    GeneratedExpression(resultTerm, nullTerm, operatorCode, BOOLEAN_TYPE_INFO)
  }

  def generateIsNotNull(
      nullCheck: Boolean,
      operand: GeneratedExpression)
    : GeneratedExpression = {
    val resultTerm = newName("result")
    val nullTerm = newName("isNull")
    val operatorCode = if (nullCheck) {
      s"""
        |${operand.code}
        |boolean $resultTerm = !${operand.nullTerm};
        |boolean $nullTerm = false;
        |""".stripMargin
    }
    else if (!nullCheck && isReference(operand)) {
      s"""
        |${operand.code}
        |boolean $resultTerm = ${operand.resultTerm} != null;
        |boolean $nullTerm = false;
        |""".stripMargin
    }
    else {
      s"""
        |${operand.code}
        |boolean $resultTerm = true;
        |""".stripMargin
    }

    GeneratedExpression(resultTerm, nullTerm, operatorCode, BOOLEAN_TYPE_INFO)
  }

  def generateAnd(
      nullCheck: Boolean,
      left: GeneratedExpression,
      right: GeneratedExpression)
    : GeneratedExpression = {
    val resultTerm = newName("result")
    val nullTerm = newName("isNull")

    val operatorCode = if (nullCheck) {
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
         |  // left expr is false, result is always false
         |  // skip right expr
         |} else {
         |  ${right.code}
         |
         |  if (${left.nullTerm}) {
         |    // left is null (unknown)
         |    if (${right.nullTerm} || ${right.resultTerm}) {
         |      $nullTerm = true;
         |    }
         |  } else {
         |    // left is true
         |    if (${right.nullTerm}) {
         |      $nullTerm = true;
         |    } else if (${right.resultTerm}) {
         |      $resultTerm = true;
         |    }
         |  }
         |}
       """.stripMargin
    }
    else {
      s"""
         |${left.code}
         |boolean $resultTerm = false;
         |if (${left.resultTerm}) {
         |  ${right.code}
         |  $resultTerm = ${right.resultTerm};
         |}
         |""".stripMargin
    }

    GeneratedExpression(resultTerm, nullTerm, operatorCode, BOOLEAN_TYPE_INFO)
  }

  def generateOr(
      nullCheck: Boolean,
      left: GeneratedExpression,
      right: GeneratedExpression)
    : GeneratedExpression = {
    val resultTerm = newName("result")
    val nullTerm = newName("isNull")

    val operatorCode = if (nullCheck) {
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
        |""".stripMargin
    }
    else {
      s"""
         |${left.code}
         |boolean $resultTerm = true;
         |if (!${left.resultTerm}) {
         |  ${right.code}
         |  $resultTerm = ${right.resultTerm};
         |}
        |""".stripMargin
    }

    GeneratedExpression(resultTerm, nullTerm, operatorCode, BOOLEAN_TYPE_INFO)
  }

  def generateNot(
      nullCheck: Boolean,
      operand: GeneratedExpression)
    : GeneratedExpression = {
    // Three-valued logic:
    // no Unknown -> Two-valued logic
    // Unknown -> Unknown
    generateUnaryOperatorIfNotNull(nullCheck, BOOLEAN_TYPE_INFO, operand) {
      (operandTerm) => s"!($operandTerm)"
    }
  }

  def generateIsTrue(operand: GeneratedExpression): GeneratedExpression = {
    GeneratedExpression(
      operand.resultTerm, // unknown is always false by default
      GeneratedExpression.NEVER_NULL,
      operand.code,
      BOOLEAN_TYPE_INFO)
  }

  def generateIsNotTrue(operand: GeneratedExpression): GeneratedExpression = {
    GeneratedExpression(
      s"(!${operand.resultTerm})", // unknown is always false by default
      GeneratedExpression.NEVER_NULL,
      operand.code,
      BOOLEAN_TYPE_INFO)
  }

  def generateIsFalse(operand: GeneratedExpression): GeneratedExpression = {
    GeneratedExpression(
      s"(!${operand.resultTerm} && !${operand.nullTerm})",
      GeneratedExpression.NEVER_NULL,
      operand.code,
      BOOLEAN_TYPE_INFO)
  }

  def generateIsNotFalse(operand: GeneratedExpression): GeneratedExpression = {
    GeneratedExpression(
      s"(${operand.resultTerm} || ${operand.nullTerm})",
      GeneratedExpression.NEVER_NULL,
      operand.code,
      BOOLEAN_TYPE_INFO)
  }

  def generateCast(
      nullCheck: Boolean,
      operand: GeneratedExpression,
      targetType: TypeInformation[_])
    : GeneratedExpression = (operand.resultType, targetType) match {

    // special case: cast from TimeIndicatorTypeInfo to SqlTimeTypeInfo
    case (ti: TimeIndicatorTypeInfo, SqlTimeTypeInfo.TIMESTAMP) =>
      operand.copy(resultType = SqlTimeTypeInfo.TIMESTAMP) // just replace the TypeInformation

    // identity casting
    case (fromTp, toTp) if fromTp == toTp =>
      operand

    // array identity casting
    // (e.g. for Integer[] that can be ObjectArrayTypeInfo or BasicArrayTypeInfo)
    case (fromTp, toTp) if isArray(fromTp) && fromTp.getTypeClass == toTp.getTypeClass =>
      operand

    // Date/Time/Timestamp -> String
    case (dtt: SqlTimeTypeInfo[_], STRING_TYPE_INFO) =>
      generateUnaryOperatorIfNotNull(nullCheck, targetType, operand) {
        (operandTerm) => s"${internalToTimePointCode(dtt, operandTerm)}.toString()"
      }

    // Interval Months -> String
    case (TimeIntervalTypeInfo.INTERVAL_MONTHS, STRING_TYPE_INFO) =>
      val method = qualifyMethod(BuiltInMethod.INTERVAL_YEAR_MONTH_TO_STRING.method)
      val timeUnitRange = qualifyEnum(TimeUnitRange.YEAR_TO_MONTH)
      generateUnaryOperatorIfNotNull(nullCheck, targetType, operand) {
        (operandTerm) => s"$method($operandTerm, $timeUnitRange)"
      }

    // Interval Millis -> String
    case (TimeIntervalTypeInfo.INTERVAL_MILLIS, STRING_TYPE_INFO) =>
      val method = qualifyMethod(BuiltInMethod.INTERVAL_DAY_TIME_TO_STRING.method)
      val timeUnitRange = qualifyEnum(TimeUnitRange.DAY_TO_SECOND)
      generateUnaryOperatorIfNotNull(nullCheck, targetType, operand) {
        (operandTerm) => s"$method($operandTerm, $timeUnitRange, 3)" // milli second precision
      }

    // Object array -> String
    case (_: ObjectArrayTypeInfo[_, _] | _: BasicArrayTypeInfo[_, _], STRING_TYPE_INFO) =>
      generateUnaryOperatorIfNotNull(nullCheck, targetType, operand) {
        (operandTerm) => s"java.util.Arrays.deepToString($operandTerm)"
      }

    // Primitive array -> String
    case (_: PrimitiveArrayTypeInfo[_], STRING_TYPE_INFO) =>
      generateUnaryOperatorIfNotNull(nullCheck, targetType, operand) {
        (operandTerm) => s"java.util.Arrays.toString($operandTerm)"
      }

    // * (not Date/Time/Timestamp) -> String
    case (_, STRING_TYPE_INFO) =>
      generateUnaryOperatorIfNotNull(nullCheck, targetType, operand) {
        (operandTerm) => s""" "" + $operandTerm"""
      }

    // * -> Character
    case (_, CHAR_TYPE_INFO) =>
      throw new CodeGenException("Character type not supported.")

    // String -> NUMERIC TYPE (not Character), Boolean
    case (STRING_TYPE_INFO, _: NumericTypeInfo[_])
        | (STRING_TYPE_INFO, BOOLEAN_TYPE_INFO) =>
      val wrapperClass = targetType.getTypeClass.getCanonicalName
      generateUnaryOperatorIfNotNull(nullCheck, targetType, operand) {
        (operandTerm) => s"$wrapperClass.valueOf($operandTerm)"
      }

    // String -> BigDecimal
    case (STRING_TYPE_INFO, BIG_DEC_TYPE_INFO) =>
      val wrapperClass = targetType.getTypeClass.getCanonicalName
      generateUnaryOperatorIfNotNull(nullCheck, targetType, operand) {
        (operandTerm) => s"new $wrapperClass($operandTerm)"
      }

    // String -> Date
    case (STRING_TYPE_INFO, SqlTimeTypeInfo.DATE) =>
      generateUnaryOperatorIfNotNull(nullCheck, targetType, operand) {
        (operandTerm) => s"${qualifyMethod(BuiltInMethod.STRING_TO_DATE.method)}($operandTerm)"
      }

    // String -> Time
    case (STRING_TYPE_INFO, SqlTimeTypeInfo.TIME) =>
      generateUnaryOperatorIfNotNull(nullCheck, targetType, operand) {
        (operandTerm) => s"${qualifyMethod(BuiltInMethod.STRING_TO_TIME.method)}($operandTerm)"
      }

    // String -> Timestamp
    case (STRING_TYPE_INFO, SqlTimeTypeInfo.TIMESTAMP) =>
      generateUnaryOperatorIfNotNull(nullCheck, targetType, operand) {
        (operandTerm) => s"${qualifyMethod(BuiltInMethod.STRING_TO_TIMESTAMP.method)}" +
          s"($operandTerm)"
      }

    // Boolean -> NUMERIC TYPE
    case (BOOLEAN_TYPE_INFO, nti: NumericTypeInfo[_]) =>
      val targetTypeTerm = primitiveTypeTermForTypeInfo(nti)
      generateUnaryOperatorIfNotNull(nullCheck, targetType, operand) {
        (operandTerm) => s"($targetTypeTerm) ($operandTerm ? 1 : 0)"
      }

    // Boolean -> BigDecimal
    case (BOOLEAN_TYPE_INFO, BIG_DEC_TYPE_INFO) =>
      generateUnaryOperatorIfNotNull(nullCheck, targetType, operand) {
        (operandTerm) => s"$operandTerm ? java.math.BigDecimal.ONE : java.math.BigDecimal.ZERO"
      }

    // NUMERIC TYPE -> Boolean
    case (_: NumericTypeInfo[_], BOOLEAN_TYPE_INFO) =>
      generateUnaryOperatorIfNotNull(nullCheck, targetType, operand) {
        (operandTerm) => s"$operandTerm != 0"
      }

    // BigDecimal -> Boolean
    case (BIG_DEC_TYPE_INFO, BOOLEAN_TYPE_INFO) =>
      generateUnaryOperatorIfNotNull(nullCheck, targetType, operand) {
        (operandTerm) => s"$operandTerm.compareTo(java.math.BigDecimal.ZERO) != 0"
      }

    // NUMERIC TYPE, BigDecimal -> NUMERIC TYPE, BigDecimal
    case (_: NumericTypeInfo[_], _: NumericTypeInfo[_])
        | (BIG_DEC_TYPE_INFO, _: NumericTypeInfo[_])
        | (_: NumericTypeInfo[_], BIG_DEC_TYPE_INFO) =>
      val operandCasting = numericCasting(operand.resultType, targetType)
      generateUnaryOperatorIfNotNull(nullCheck, targetType, operand) {
        (operandTerm) => s"${operandCasting(operandTerm)}"
      }

    // Date -> Timestamp
    case (SqlTimeTypeInfo.DATE, SqlTimeTypeInfo.TIMESTAMP) =>
      generateUnaryOperatorIfNotNull(nullCheck, targetType, operand) {
        (operandTerm) =>
          s"$operandTerm * ${classOf[DateTimeUtils].getCanonicalName}.MILLIS_PER_DAY"
      }

    // Timestamp -> Date
    case (SqlTimeTypeInfo.TIMESTAMP, SqlTimeTypeInfo.DATE) =>
      val targetTypeTerm = primitiveTypeTermForTypeInfo(targetType)
      generateUnaryOperatorIfNotNull(nullCheck, targetType, operand) {
        (operandTerm) =>
          s"($targetTypeTerm) ($operandTerm / " +
            s"${classOf[DateTimeUtils].getCanonicalName}.MILLIS_PER_DAY)"
      }

    // Time -> Timestamp
    case (SqlTimeTypeInfo.TIME, SqlTimeTypeInfo.TIMESTAMP) =>
      generateUnaryOperatorIfNotNull(nullCheck, targetType, operand) {
        (operandTerm) => s"$operandTerm"
      }

    // Timestamp -> Time
    case (SqlTimeTypeInfo.TIMESTAMP, SqlTimeTypeInfo.TIME) =>
      val targetTypeTerm = primitiveTypeTermForTypeInfo(targetType)
      generateUnaryOperatorIfNotNull(nullCheck, targetType, operand) {
        (operandTerm) =>
          s"($targetTypeTerm) ($operandTerm % " +
            s"${classOf[DateTimeUtils].getCanonicalName}.MILLIS_PER_DAY)"
      }

    // internal temporal casting
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
    case (SqlTimeTypeInfo.DATE, INT_TYPE_INFO) |
         (SqlTimeTypeInfo.TIME, INT_TYPE_INFO) |
         (SqlTimeTypeInfo.TIMESTAMP, LONG_TYPE_INFO) |
         (INT_TYPE_INFO, SqlTimeTypeInfo.DATE) |
         (INT_TYPE_INFO, SqlTimeTypeInfo.TIME) |
         (LONG_TYPE_INFO, SqlTimeTypeInfo.TIMESTAMP) |
         (INT_TYPE_INFO, TimeIntervalTypeInfo.INTERVAL_MONTHS) |
         (LONG_TYPE_INFO, TimeIntervalTypeInfo.INTERVAL_MILLIS) |
         (TimeIntervalTypeInfo.INTERVAL_MONTHS, INT_TYPE_INFO) |
         (TimeIntervalTypeInfo.INTERVAL_MILLIS, LONG_TYPE_INFO) =>
      internalExprCasting(operand, targetType)

    // internal reinterpretation of temporal types
    // Date, Time, Interval Months -> Long
    case  (SqlTimeTypeInfo.DATE, LONG_TYPE_INFO)
        | (SqlTimeTypeInfo.TIME, LONG_TYPE_INFO)
        | (TimeIntervalTypeInfo.INTERVAL_MONTHS, LONG_TYPE_INFO) =>
      internalExprCasting(operand, targetType)

    case (from, to) =>
      throw new CodeGenException(s"Unsupported cast from '$from' to '$to'.")
  }

  def generateIfElse(
      nullCheck: Boolean,
      operands: Seq[GeneratedExpression],
      resultType: TypeInformation[_],
      i: Int = 0)
    : GeneratedExpression = {
    // else part
    if (i == operands.size - 1) {
      generateCast(nullCheck, operands(i), resultType)
    }
    else {
      // check that the condition is boolean
      // we do not check for null instead we use the default value
      // thus null is false
      requireBoolean(operands(i))
      val condition = operands(i)
      val trueAction = generateCast(nullCheck, operands(i + 1), resultType)
      val falseAction = generateIfElse(nullCheck, operands, resultType, i + 2)

      val resultTerm = newName("result")
      val nullTerm = newName("isNull")
      val resultTypeTerm = primitiveTypeTermForTypeInfo(resultType)

      val operatorCode = if (nullCheck) {
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
          |""".stripMargin
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
          |""".stripMargin
      }

      GeneratedExpression(resultTerm, nullTerm, operatorCode, resultType)
    }
  }

  def generateTemporalPlusMinus(
      plus: Boolean,
      nullCheck: Boolean,
      resultType: TypeInformation[_],
      left: GeneratedExpression,
      right: GeneratedExpression,
      config: TableConfig)
    : GeneratedExpression = {

    val op = if (plus) "+" else "-"

    (left.resultType, right.resultType) match {
      // arithmetic of time point and time interval
      case (l: TimeIntervalTypeInfo[_], r: TimeIntervalTypeInfo[_]) if l == r =>
        generateArithmeticOperator(op, nullCheck, l, left, right, config)

      case (SqlTimeTypeInfo.DATE, TimeIntervalTypeInfo.INTERVAL_MILLIS) =>
        resultType match {
          case SqlTimeTypeInfo.DATE =>
            generateOperatorIfNotNull(nullCheck, SqlTimeTypeInfo.DATE, left, right) {
              (l, r) => s"$l $op ((int) ($r / ${MILLIS_PER_DAY}L))"
            }
          case SqlTimeTypeInfo.TIMESTAMP =>
            generateOperatorIfNotNull(nullCheck, SqlTimeTypeInfo.TIMESTAMP, left, right) {
              (l, r) => s"$l * ${MILLIS_PER_DAY}L $op $r"
            }
        }

      case (SqlTimeTypeInfo.DATE, TimeIntervalTypeInfo.INTERVAL_MONTHS) =>
        generateOperatorIfNotNull(nullCheck, SqlTimeTypeInfo.DATE, left, right) {
            (l, r) => s"${qualifyMethod(BuiltInMethod.ADD_MONTHS.method)}($l, $op($r))"
        }

      case (SqlTimeTypeInfo.TIME, TimeIntervalTypeInfo.INTERVAL_MILLIS) =>
        generateOperatorIfNotNull(nullCheck, SqlTimeTypeInfo.TIME, left, right) {
            (l, r) => s"$l $op ((int) ($r))"
        }

      case (SqlTimeTypeInfo.TIMESTAMP, TimeIntervalTypeInfo.INTERVAL_MILLIS) =>
        generateOperatorIfNotNull(nullCheck, SqlTimeTypeInfo.TIMESTAMP, left, right) {
          (l, r) => s"$l $op $r"
        }

      case (SqlTimeTypeInfo.TIMESTAMP, TimeIntervalTypeInfo.INTERVAL_MONTHS) =>
        generateOperatorIfNotNull(nullCheck, SqlTimeTypeInfo.TIMESTAMP, left, right) {
          (l, r) => s"${qualifyMethod(BuiltInMethod.ADD_MONTHS.method)}($l, $op($r))"
        }

      // minus arithmetic of time points (i.e. for TIMESTAMPDIFF)
      case (l: SqlTimeTypeInfo[_], r: SqlTimeTypeInfo[_]) if !plus =>
        resultType match {
          case TimeIntervalTypeInfo.INTERVAL_MONTHS =>
            generateOperatorIfNotNull(nullCheck, resultType, left, right) {
              (ll, rr) => (l, r) match {
                case (SqlTimeTypeInfo.TIMESTAMP, SqlTimeTypeInfo.DATE) =>
                  s"${qualifyMethod(BuiltInMethod.SUBTRACT_MONTHS.method)}" +
                    s"($ll, $rr * ${MILLIS_PER_DAY}L)"
                case (SqlTimeTypeInfo.DATE, SqlTimeTypeInfo.TIMESTAMP) =>
                  s"${qualifyMethod(BuiltInMethod.SUBTRACT_MONTHS.method)}" +
                    s"($ll * ${MILLIS_PER_DAY}L, $rr)"
                case _ =>
                  s"${qualifyMethod(BuiltInMethod.SUBTRACT_MONTHS.method)}($ll, $rr)"
               }
            }

          case TimeIntervalTypeInfo.INTERVAL_MILLIS =>
            generateOperatorIfNotNull(nullCheck, resultType, left, right) {
              (ll, rr) => (l, r) match {
                case (SqlTimeTypeInfo.TIMESTAMP, SqlTimeTypeInfo.TIMESTAMP) =>
                  s"$ll $op $rr"
                case (SqlTimeTypeInfo.DATE, SqlTimeTypeInfo.DATE) =>
                  s"($ll * ${MILLIS_PER_DAY}L) $op ($rr * ${MILLIS_PER_DAY}L)"
                case (SqlTimeTypeInfo.TIMESTAMP, SqlTimeTypeInfo.DATE) =>
                  s"$ll $op ($rr * ${MILLIS_PER_DAY}L)"
                case (SqlTimeTypeInfo.DATE, SqlTimeTypeInfo.TIMESTAMP) =>
                  s"($ll * ${MILLIS_PER_DAY}L) $op $rr"
              }
            }
        }

      case _ =>
        throw new CodeGenException("Unsupported temporal arithmetic.")
    }
  }

  def generateUnaryIntervalPlusMinus(
      plus: Boolean,
      nullCheck: Boolean,
      operand: GeneratedExpression)
    : GeneratedExpression = {
    val operator = if (plus) "+" else "-"
    generateUnaryArithmeticOperator(operator, nullCheck, operand.resultType, operand)
  }

  def generateRow(
      codeGenerator: CodeGenerator,
      resultType: TypeInformation[_],
      elements: Seq[GeneratedExpression])
  : GeneratedExpression = {
    val rowTerm = codeGenerator.addReusableRow(resultType.getArity)

    val boxedElements: Seq[GeneratedExpression] = resultType match {
      case ct: RowTypeInfo => // should always be RowTypeInfo
        if (resultType.getArity == elements.size) {
          elements.zipWithIndex.map {
            case (e, idx) => codeGenerator.generateNullableOutputBoxing(e,
              ct.getTypeAt(idx))
          }
        } else {
          throw new CodeGenException(s"Illegal row generation operation. " +
            s"Expected row arity ${resultType.getArity} but was ${elements.size}.")
        }
      case _ => throw new CodeGenException(s"Unsupported row generation operation. " +
        s"Expected RowTypeInfo but was $resultType.")
    }

    val code = boxedElements
      .zipWithIndex
      .map { case (element, idx) =>
        s"""
           |${element.code}
           |$rowTerm.setField($idx, ${element.resultTerm});
           |""".stripMargin
      }
      .mkString("\n")

    GeneratedExpression(rowTerm, GeneratedExpression.NEVER_NULL, code, resultType)
  }

  def generateArray(
      codeGenerator: CodeGenerator,
      resultType: TypeInformation[_],
      elements: Seq[GeneratedExpression])
    : GeneratedExpression = {
    val arrayTerm = codeGenerator.addReusableArray(resultType.getTypeClass, elements.size)

    val boxedElements: Seq[GeneratedExpression] = resultType match {
      // we box the elements to also represent null values
      case oati: ObjectArrayTypeInfo[_, _] =>
        elements.map { e =>
          codeGenerator.generateNullableOutputBoxing(e, oati.getComponentInfo)
        }
      // no boxing necessary
      case _: PrimitiveArrayTypeInfo[_] => elements
    }

    val code = boxedElements
      .zipWithIndex
      .map { case (element, idx) =>
        s"""
          |${element.code}
          |$arrayTerm[$idx] = ${element.resultTerm};
          |""".stripMargin
      }
      .mkString("\n")

    GeneratedExpression(arrayTerm, GeneratedExpression.NEVER_NULL, code, resultType)
  }

  def generateArrayElementAt(
      codeGenerator: CodeGenerator,
      array: GeneratedExpression,
      index: GeneratedExpression)
    : GeneratedExpression = {

    val resultTerm = newName("result")

    def unboxArrayElement(componentInfo: TypeInformation[_]): GeneratedExpression = {
      // get boxed array element
      val resultTypeTerm = boxedTypeTermForTypeInfo(componentInfo)

      val arrayAccessCode = if (codeGenerator.nullCheck) {
        s"""
          |${array.code}
          |${index.code}
          |$resultTypeTerm $resultTerm = (${array.nullTerm} || ${index.nullTerm}) ?
          |  null : ${array.resultTerm}[${index.resultTerm} - 1];
          |""".stripMargin
      } else {
        s"""
          |${array.code}
          |${index.code}
          |$resultTypeTerm $resultTerm = ${array.resultTerm}[${index.resultTerm} - 1];
          |""".stripMargin
      }

      // generate unbox code
      val unboxing = codeGenerator.generateInputFieldUnboxing(componentInfo, resultTerm)

      unboxing.copy(code =
        s"""
          |$arrayAccessCode
          |${unboxing.code}
          |""".stripMargin
      )
    }

    array.resultType match {

      // unbox object array types
      case oati: ObjectArrayTypeInfo[_, _] =>
        unboxArrayElement(oati.getComponentInfo)

      // unbox basic array types
      case bati: BasicArrayTypeInfo[_, _] =>
        unboxArrayElement(bati.getComponentInfo)

      // no unboxing necessary
      case pati: PrimitiveArrayTypeInfo[_] =>
        generateOperatorIfNotNull(codeGenerator.nullCheck, pati.getComponentType, array, index) {
          (leftTerm, rightTerm) => s"$leftTerm[$rightTerm - 1]"
        }
    }
  }

  def generateArrayElement(
      codeGenerator: CodeGenerator,
      array: GeneratedExpression)
    : GeneratedExpression = {

    val nullTerm = newName("isNull")
    val resultTerm = newName("result")
    val resultType = array.resultType match {
      case oati: ObjectArrayTypeInfo[_, _] => oati.getComponentInfo
      case bati: BasicArrayTypeInfo[_, _] => bati.getComponentInfo
      case pati: PrimitiveArrayTypeInfo[_] => pati.getComponentType
    }
    val resultTypeTerm = primitiveTypeTermForTypeInfo(resultType)
    val defaultValue = primitiveDefaultValue(resultType)

    val arrayLengthCode = if (codeGenerator.nullCheck) {
      s"${array.nullTerm} ? 0 : ${array.resultTerm}.length"
    } else {
      s"${array.resultTerm}.length"
    }

    def unboxArrayElement(componentInfo: TypeInformation[_]): String = {
      // generate unboxing code
      val unboxing = codeGenerator.generateInputFieldUnboxing(
        componentInfo,
        s"${array.resultTerm}[0]")

      s"""
        |${array.code}
        |${if (codeGenerator.nullCheck) s"boolean $nullTerm;" else "" }
        |$resultTypeTerm $resultTerm;
        |switch ($arrayLengthCode) {
        |  case 0:
        |    ${if (codeGenerator.nullCheck) s"$nullTerm = true;" else "" }
        |    $resultTerm = $defaultValue;
        |    break;
        |  case 1:
        |    ${unboxing.code}
        |    ${if (codeGenerator.nullCheck) s"$nullTerm = ${unboxing.nullTerm};" else "" }
        |    $resultTerm = ${unboxing.resultTerm};
        |    break;
        |  default:
        |    throw new RuntimeException("Array has more than one element.");
        |}
        |""".stripMargin
    }

    val arrayAccessCode = array.resultType match {
      case oati: ObjectArrayTypeInfo[_, _] =>
        unboxArrayElement(oati.getComponentInfo)

      case bati: BasicArrayTypeInfo[_, _] =>
        unboxArrayElement(bati.getComponentInfo)

      case pati: PrimitiveArrayTypeInfo[_] =>
        s"""
          |${array.code}
          |${if (codeGenerator.nullCheck) s"boolean $nullTerm;" else "" }
          |$resultTypeTerm $resultTerm;
          |switch ($arrayLengthCode) {
          |  case 0:
          |    ${if (codeGenerator.nullCheck) s"$nullTerm = true;" else "" }
          |    $resultTerm = $defaultValue;
          |    break;
          |  case 1:
          |    ${if (codeGenerator.nullCheck) s"$nullTerm = false;" else "" }
          |    $resultTerm = ${array.resultTerm}[0];
          |    break;
          |  default:
          |    throw new RuntimeException("Array has more than one element.");
          |}
          |""".stripMargin
    }

    GeneratedExpression(resultTerm, nullTerm, arrayAccessCode, resultType)
  }

  def generateArrayCardinality(
      nullCheck: Boolean,
      array: GeneratedExpression)
    : GeneratedExpression = {

    generateUnaryOperatorIfNotNull(nullCheck, INT_TYPE_INFO, array) {
      (operandTerm) => s"${array.resultTerm}.length"
    }
  }

  def generateConcat(
      nullCheck: Boolean,
      operands: Seq[GeneratedExpression])
    : GeneratedExpression = {

    generateCallIfArgsNotNull(nullCheck, STRING_TYPE_INFO, operands) {
      (terms) =>s"${qualifyMethod(BuiltInMethods.CONCAT)}(${terms.mkString(", ")})"
    }
  }

  def generateConcatWs(operands: Seq[GeneratedExpression]): GeneratedExpression = {

    val resultTerm = newName("result")
    val nullTerm = newName("isNull")
    val defaultValue = primitiveDefaultValue(Types.STRING)

    val tempTerms = operands.tail.map(_ => newName("temp"))

    val operatorCode =
      s"""
        |${operands.map(_.code).mkString("\n")}
        |
        |String $resultTerm;
        |boolean $nullTerm;
        |if (${operands.head.nullTerm}) {
        |  $nullTerm = true;
        |  $resultTerm = $defaultValue;
        |} else {
        |  ${operands.tail.zip(tempTerms).map {
                case (o: GeneratedExpression, t: String) =>
                  s"String $t;\n" +
                  s"  if (${o.nullTerm}) $t = null; else $t = ${o.resultTerm};"
              }.mkString("\n")
            }
        |  $nullTerm = false;
        |  $resultTerm = ${qualifyMethod(BuiltInMethods.CONCAT_WS)}
        |   (${operands.head.resultTerm}, ${tempTerms.mkString(", ")});
        |}
        |""".stripMargin

    GeneratedExpression(resultTerm, nullTerm, operatorCode, Types.STRING)
  }

  def generateMap(
      codeGenerator: CodeGenerator,
      resultType: TypeInformation[_],
      elements: Seq[GeneratedExpression])
    : GeneratedExpression = {

    val mapTerm = codeGenerator.addReusableMap()

    val boxedElements: Seq[GeneratedExpression] = resultType match {
      case mti: MapTypeInfo[_, _] =>
        elements.zipWithIndex.map { case (e, idx) =>
          codeGenerator.generateNullableOutputBoxing(e,
            if (idx % 2 == 0) mti.getKeyTypeInfo else mti.getValueTypeInfo)
        }
    }

    // clear the map when it is not guaranteed that keys are constant
    var clearMap: Boolean = false

    val code = boxedElements.grouped(2)
      .map { case Seq(key, value) =>
        // check if all keys are constant
        if (!key.literal) {
          clearMap = true
        }
        s"""
           |${key.code}
           |${value.code}
           |$mapTerm.put(${key.resultTerm}, ${value.resultTerm});
           |""".stripMargin
      }
      .mkString("\n")

    GeneratedExpression(
      mapTerm,
      GeneratedExpression.NEVER_NULL,
      (if (clearMap) s"$mapTerm.clear();\n" else "") + code,
      resultType)
  }

  def generateMapGet(
      codeGenerator: CodeGenerator,
      map: GeneratedExpression,
      key: GeneratedExpression)
    : GeneratedExpression = {

    val resultTerm = newName("result")
    val nullTerm = newName("isNull")
    val ty = map.resultType.asInstanceOf[MapTypeInfo[_,_]]
    val resultType = ty.getValueTypeInfo
    val resultTypeTerm = boxedTypeTermForTypeInfo(ty.getValueTypeInfo)
    val accessCode = if (codeGenerator.nullCheck) {
      s"""
         |${map.code}
         |${key.code}
         |$resultTypeTerm $resultTerm = (${map.nullTerm} || ${key.nullTerm}) ?
         |  null : ($resultTypeTerm) ${map.resultTerm}.get(${key.resultTerm});
         |boolean $nullTerm = $resultTerm == null;
         |""".stripMargin
    } else {
      s"""
         |${map.code}
         |${key.code}
         |$resultTypeTerm $resultTerm = ($resultTypeTerm)
         | ${map.resultTerm}.get(${key.resultTerm});
         |""".stripMargin
    }
    val unboxing = codeGenerator.generateInputFieldUnboxing(resultType, resultTerm)

    unboxing.copy(code =
      s"""
         |$accessCode
         |${unboxing.code}
         |""".stripMargin
    )
  }

  def generateMapCardinality(
      nullCheck: Boolean,
      map: GeneratedExpression)
    : GeneratedExpression = {
    generateUnaryOperatorIfNotNull(nullCheck, INT_TYPE_INFO, map) {
      (operandTerm) => s"$operandTerm.size()"
    }
  }

  // ----------------------------------------------------------------------------------------------

  private def generateUnaryOperatorIfNotNull(
      nullCheck: Boolean,
      resultType: TypeInformation[_],
      operand: GeneratedExpression)
      (expr: (String) => String)
    : GeneratedExpression = {
    val resultTerm = newName("result")
    val nullTerm = newName("isNull")
    val resultTypeTerm = primitiveTypeTermForTypeInfo(resultType)
    val defaultValue = primitiveDefaultValue(resultType)

    val operatorCode = if (nullCheck) {
      s"""
        |${operand.code}
        |$resultTypeTerm $resultTerm;
        |boolean $nullTerm;
        |if (!${operand.nullTerm}) {
        |  $resultTerm = ${expr(operand.resultTerm)};
        |  $nullTerm = false;
        |}
        |else {
        |  $resultTerm = $defaultValue;
        |  $nullTerm = true;
        |}
        |""".stripMargin
    }
    else {
      s"""
        |${operand.code}
        |$resultTypeTerm $resultTerm = ${expr(operand.resultTerm)};
        |""".stripMargin
    }

    GeneratedExpression(resultTerm, nullTerm, operatorCode, resultType)
  }

  private def generateOperatorIfNotNull(
      nullCheck: Boolean,
      resultType: TypeInformation[_],
      left: GeneratedExpression,
      right: GeneratedExpression)
      (expr: (String, String) => String)
    : GeneratedExpression = {
    val resultTerm = newName("result")
    val nullTerm = newName("isNull")
    val resultTypeTerm = primitiveTypeTermForTypeInfo(resultType)
    val defaultValue = primitiveDefaultValue(resultType)

    val resultCode = if (nullCheck) {
      s"""
        |${left.code}
        |${right.code}
        |boolean $nullTerm = ${left.nullTerm} || ${right.nullTerm};
        |$resultTypeTerm $resultTerm;
        |if ($nullTerm) {
        |  $resultTerm = $defaultValue;
        |}
        |else {
        |  $resultTerm = ${expr(left.resultTerm, right.resultTerm)};
        |}
        |""".stripMargin
    }
    else {
      s"""
        |${left.code}
        |${right.code}
        |$resultTypeTerm $resultTerm = ${expr(left.resultTerm, right.resultTerm)};
        |""".stripMargin
    }

    GeneratedExpression(resultTerm, nullTerm, resultCode, resultType)
  }

  private def internalExprCasting(
      expr: GeneratedExpression,
      typeInfo: TypeInformation[_])
    : GeneratedExpression = {
    GeneratedExpression(expr.resultTerm, expr.nullTerm, expr.code, typeInfo)
  }

  private def arithOpToDecMethod(operator: String): String = operator match {
    case "+" => "add"
    case "-" => "subtract"
    case "*" => "multiply"
    case "/" => "divide"
    case "%" => "remainder"
    case _ => throw new CodeGenException(s"Unsupported decimal arithmetic operator: '$operator'")
  }

  private def mathContextToString(mathContext: MathContext): String = mathContext match {
    case MathContext.DECIMAL32 => "java.math.MathContext.DECIMAL32"
    case MathContext.DECIMAL64 => "java.math.MathContext.DECIMAL64"
    case MathContext.DECIMAL128 => "java.math.MathContext.DECIMAL128"
    case MathContext.UNLIMITED => "java.math.MathContext.UNLIMITED"
    case _ => s"""new java.math.MathContext("$mathContext")"""
  }

  private def numericCasting(
      operandType: TypeInformation[_],
      resultType: TypeInformation[_])
    : (String) => String = {

    def decToPrimMethod(targetType: TypeInformation[_]): String = targetType match {
      case BYTE_TYPE_INFO => "byteValueExact"
      case SHORT_TYPE_INFO => "shortValueExact"
      case INT_TYPE_INFO => "intValueExact"
      case LONG_TYPE_INFO => "longValueExact"
      case FLOAT_TYPE_INFO => "floatValue"
      case DOUBLE_TYPE_INFO => "doubleValue"
      case _ => throw new CodeGenException(s"Unsupported decimal casting type: '$targetType'")
    }

    val resultTypeTerm = primitiveTypeTermForTypeInfo(resultType)
    // no casting necessary
    if (operandType == resultType) {
      (operandTerm) => s"$operandTerm"
    }
    // result type is decimal but numeric operand is not
    else if (isDecimal(resultType) && !isDecimal(operandType) && isNumeric(operandType)) {
      (operandTerm) =>
        s"java.math.BigDecimal.valueOf((${superPrimitive(operandType)}) $operandTerm)"
    }
    // numeric result type is not decimal but operand is
    else if (isNumeric(resultType) && !isDecimal(resultType) && isDecimal(operandType) ) {
      (operandTerm) => s"$operandTerm.${decToPrimMethod(resultType)}()"
    }
    // result type and operand type are numeric but not decimal
    else if (isNumeric(operandType) && isNumeric(resultType)
        && !isDecimal(operandType) && !isDecimal(resultType)) {
      (operandTerm) => s"(($resultTypeTerm) $operandTerm)"
    }
    // result type is time interval and operand type is integer
    else if (isTimeInterval(resultType) && isInteger(operandType)){
      (operandTerm) => s"(($resultTypeTerm) $operandTerm)"
    }
    else {
      throw new CodeGenException(s"Unsupported casting from $operandType to $resultType.")
    }
  }
}
