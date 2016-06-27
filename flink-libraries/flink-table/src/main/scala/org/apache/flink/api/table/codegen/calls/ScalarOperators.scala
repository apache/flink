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
package org.apache.flink.api.table.codegen.calls

import org.apache.calcite.avatica.util.DateTimeUtils
import org.apache.calcite.util.BuiltInMethod
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.{NumericTypeInfo, SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.api.table.codegen.CodeGenUtils._
import org.apache.flink.api.table.codegen.{CodeGenException, GeneratedExpression}
import org.apache.flink.api.table.typeutils.TypeCheckUtils._

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
      right: GeneratedExpression)
    : GeneratedExpression = {
    val leftCasting = numericCasting(left.resultType, resultType)
    val rightCasting = numericCasting(right.resultType, resultType)
    val resultTypeTerm = primitiveTypeTermForTypeInfo(resultType)

    generateOperatorIfNotNull(nullCheck, resultType, left, right) {
      (leftTerm, rightTerm) =>
        if (isDecimal(resultType)) {
          s"${leftCasting(leftTerm)}.${arithOpToDecMethod(operator)}(${rightCasting(rightTerm)})"
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
        } else if (isNumeric(operand.resultType)) {
          s"$operator($operandTerm)"
        }  else {
          throw new CodeGenException("Unsupported unary operator.")
        }
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
        |${right.code}
        |boolean $resultTerm;
        |boolean $nullTerm;
        |if (!${left.nullTerm} && !${right.nullTerm}) {
        |  $resultTerm = ${left.resultTerm} && ${right.resultTerm};
        |  $nullTerm = false;
        |}
        |else if (!${left.nullTerm} && ${left.resultTerm} && ${right.nullTerm}) {
        |  $resultTerm = false;
        |  $nullTerm = true;
        |}
        |else if (!${left.nullTerm} && !${left.resultTerm} && ${right.nullTerm}) {
        |  $resultTerm = false;
        |  $nullTerm = false;
        |}
        |else if (${left.nullTerm} && !${right.nullTerm} && ${right.resultTerm}) {
        |  $resultTerm = false;
        |  $nullTerm = true;
        |}
        |else if (${left.nullTerm} && !${right.nullTerm} && !${right.resultTerm}) {
        |  $resultTerm = false;
        |  $nullTerm = false;
        |}
        |else {
        |  $resultTerm = false;
        |  $nullTerm = true;
        |}
        |""".stripMargin
    }
    else {
      s"""
        |${left.code}
        |${right.code}
        |boolean $resultTerm = ${left.resultTerm} && ${right.resultTerm};
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
      // True && Unknown -> True
      // False && Unknown -> Unknown
      // Unknown && True -> True
      // Unknown && False -> Unknown
      // Unknown && Unknown -> Unknown
      s"""
        |${left.code}
        |${right.code}
        |boolean $resultTerm;
        |boolean $nullTerm;
        |if (!${left.nullTerm} && !${right.nullTerm}) {
        |  $resultTerm = ${left.resultTerm} || ${right.resultTerm};
        |  $nullTerm = false;
        |}
        |else if (!${left.nullTerm} && ${left.resultTerm} && ${right.nullTerm}) {
        |  $resultTerm = true;
        |  $nullTerm = false;
        |}
        |else if (!${left.nullTerm} && !${left.resultTerm} && ${right.nullTerm}) {
        |  $resultTerm = false;
        |  $nullTerm = true;
        |}
        |else if (${left.nullTerm} && !${right.nullTerm} && ${right.resultTerm}) {
        |  $resultTerm = true;
        |  $nullTerm = false;
        |}
        |else if (${left.nullTerm} && !${right.nullTerm} && !${right.resultTerm}) {
        |  $resultTerm = false;
        |  $nullTerm = true;
        |}
        |else {
        |  $resultTerm = false;
        |  $nullTerm = true;
        |}
        |""".stripMargin
    }
    else {
      s"""
        |${left.code}
        |${right.code}
        |boolean $resultTerm = ${left.resultTerm} || ${right.resultTerm};
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

  def generateCast(
      nullCheck: Boolean,
      operand: GeneratedExpression,
      targetType: TypeInformation[_])
    : GeneratedExpression = (operand.resultType, targetType) match {
    // identity casting
    case (fromTp, toTp) if fromTp == toTp =>
      operand

    // Date/Time/Timestamp -> String
    case (dtt: SqlTimeTypeInfo[_], STRING_TYPE_INFO) =>
      generateUnaryOperatorIfNotNull(nullCheck, targetType, operand) {
        (operandTerm) => s"""${internalToTemporalCode(dtt, operandTerm)}.toString()"""
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

    // Date -> Integer, Time -> Integer
    case (SqlTimeTypeInfo.DATE, INT_TYPE_INFO) | (SqlTimeTypeInfo.TIME, INT_TYPE_INFO) =>
      internalExprCasting(operand, INT_TYPE_INFO)

    // Timestamp -> Long
    case (SqlTimeTypeInfo.TIMESTAMP, LONG_TYPE_INFO) =>
      internalExprCasting(operand, LONG_TYPE_INFO)

    // Integer -> Date
    case (INT_TYPE_INFO, SqlTimeTypeInfo.DATE) =>
      internalExprCasting(operand, SqlTimeTypeInfo.DATE)

    // Integer -> Time
    case (INT_TYPE_INFO, SqlTimeTypeInfo.TIME) =>
      internalExprCasting(operand, SqlTimeTypeInfo.TIME)

    // Long -> Timestamp
    case (LONG_TYPE_INFO, SqlTimeTypeInfo.TIMESTAMP) =>
      internalExprCasting(operand, SqlTimeTypeInfo.TIMESTAMP)

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
    case _ => throw new CodeGenException("Unsupported decimal arithmetic operator.")
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
      case _ => throw new CodeGenException("Unsupported decimal casting type.")
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
    else {
      throw new CodeGenException(s"Unsupported casting from $operandType to $resultType.")
    }
  }
}
