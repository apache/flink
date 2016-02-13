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
package org.apache.flink.api.table.codegen

import org.apache.flink.api.common.typeinfo.BasicTypeInfo.BOOLEAN_TYPE_INFO
import org.apache.flink.api.common.typeinfo.{NumericTypeInfo, TypeInformation}
import org.apache.flink.api.table.codegen.CodeGenUtils._

object OperatorCodeGen {

  def generateArithmeticOperator(
      operator: String,
      nullCheck: Boolean,
      resultType: TypeInformation[_],
      left: GeneratedExpression,
      right: GeneratedExpression)
    : GeneratedExpression = {
    // String arithmetic // TODO rework
    if (isString(left)) {
      generateOperatorIfNotNull(nullCheck, resultType, left, right) {
      (leftTerm, rightTerm) => s"$leftTerm $operator $rightTerm"
      }
    }
    // Numeric arithmetic
    else if (isNumeric(left) && isNumeric(right)) {
      val leftType = left.resultType.asInstanceOf[NumericTypeInfo[_]]
      val rightType = right.resultType.asInstanceOf[NumericTypeInfo[_]]
      val resultTypeTerm = primitiveTypeTermForTypeInfo(resultType)

      generateOperatorIfNotNull(nullCheck, resultType, left, right) {
      (leftTerm, rightTerm) =>
        // no casting required
        if (leftType == resultType && rightType == resultType) {
          s"$leftTerm $operator $rightTerm"
        }
        // left needs casting
        else if (leftType != resultType && rightType == resultType) {
          s"(($resultTypeTerm) $leftTerm) $operator $rightTerm"
        }
        // right needs casting
        else if (leftType == resultType && rightType != resultType) {
          s"$leftTerm $operator (($resultTypeTerm) $rightTerm)"
        }
        // both sides need casting
        else {
          s"(($resultTypeTerm) $leftTerm) $operator (($resultTypeTerm) $rightTerm)"
        }
      }
    }
    else {
      throw new CodeGenException("Unsupported arithmetic operation.")
    }
  }

  def generateUnaryArithmeticOperator(
      operator: String,
      nullCheck: Boolean,
      resultType: TypeInformation[_],
      operand: GeneratedExpression)
    : GeneratedExpression = {
    generateUnaryOperatorIfNotNull(nullCheck, resultType, operand) {
      (operandTerm) => s"$operator($operandTerm)"
    }
  }

  def generateEquals(
      nullCheck: Boolean,
      left: GeneratedExpression,
      right: GeneratedExpression)
    : GeneratedExpression = {
    generateOperatorIfNotNull(nullCheck, BOOLEAN_TYPE_INFO, left, right) {
      if (isReference(left)) {
        (leftTerm, rightTerm) => s"$leftTerm.equals($rightTerm)"
      }
      else if (isReference(right)) {
        (leftTerm, rightTerm) => s"$rightTerm.equals($leftTerm)"
      }
      else {
        (leftTerm, rightTerm) => s"$leftTerm == $rightTerm"
      }
    }
  }

  def generateNotEquals(
      nullCheck: Boolean,
      left: GeneratedExpression,
      right: GeneratedExpression)
    : GeneratedExpression = {
    generateOperatorIfNotNull(nullCheck, BOOLEAN_TYPE_INFO, left, right) {
      if (isReference(left)) {
        (leftTerm, rightTerm) => s"!($leftTerm.equals($rightTerm))"
      }
      else if (isReference(right)) {
        (leftTerm, rightTerm) => s"!($rightTerm.equals($leftTerm))"
      }
      else {
        (leftTerm, rightTerm) => s"$leftTerm != $rightTerm"
      }
    }
  }

  def generateComparison(
      operator: String,
      nullCheck: Boolean,
      left: GeneratedExpression,
      right: GeneratedExpression)
    : GeneratedExpression = {
    generateOperatorIfNotNull(nullCheck, BOOLEAN_TYPE_INFO, left, right) {
      if (isString(left) && isString(right)) {
        (leftTerm, rightTerm) => s"$leftTerm.compareTo($rightTerm) $operator 0"
      }
      else if (isNumeric(left) && isNumeric(right)) {
        (leftTerm, rightTerm) => s"$leftTerm $operator $rightTerm"
      }
      else {
        throw new CodeGenException("Comparison is only supported for Strings and numeric types.")
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
    else if (!nullCheck && isReference(operand.resultType)) {
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
    else if (!nullCheck && isReference(operand.resultType)) {
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
        |boolean $resultTerm = ${left.resultTerm} && ${right.resultTerm};
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

}
