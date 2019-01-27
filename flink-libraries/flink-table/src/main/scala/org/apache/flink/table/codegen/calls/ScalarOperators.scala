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

import java.nio.charset.StandardCharsets
import java.util.concurrent.CompletableFuture

import org.apache.calcite.avatica.util.DateTimeUtils.MILLIS_PER_DAY
import org.apache.calcite.avatica.util.{DateTimeUtils, TimeUnitRange}
import org.apache.calcite.util.BuiltInMethod
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.api.types._
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.calls.CallGenerator._
import org.apache.flink.table.codegen._
import org.apache.flink.table.dataformat._
import org.apache.flink.table.dataformat.util.BinaryRowUtil
import org.apache.flink.table.dataformat.{BinaryArray, BinaryArrayWriter, BinaryMap, Decimal}
import org.apache.flink.table.functions.sql.internal.{SqlRuntimeFilterBuilderFunction, SqlRuntimeFilterFunction}
import org.apache.flink.table.runtime.conversion.DataStructureConverters.genToExternal
import org.apache.flink.table.runtime.util.{BloomFilter, BloomFilterAcc, RuntimeFilterUtils}
import org.apache.flink.table.typeutils.TypeCheckUtils._
import org.apache.flink.table.typeutils._
import org.apache.flink.util.SerializedValue

object ScalarOperators {

  def generateArithmeticOperator(
      ctx: CodeGeneratorContext,
      operator: String,
      nullCheck: Boolean,
      resultType: InternalType,
      left: GeneratedExpression,
      right: GeneratedExpression)
    : GeneratedExpression = {

    resultType match {
      case dt: DecimalType =>
        return generateDecimalOperator(ctx, operator, nullCheck, dt, left, right)
      case _ =>
    }

    val leftCasting = operator match {
      case "%" =>
        if (left.resultType == right.resultType) {
          numericCasting(left.resultType, resultType)
        } else {
          val castedType = if (isDecimal(left.resultType)) {
            DataTypes.LONG
          } else {
            left.resultType
          }
          numericCasting(left.resultType, castedType)
        }
      case _ => numericCasting(left.resultType, resultType)
    }

    val rightCasting = numericCasting(right.resultType, resultType)
    val resultTypeTerm = primitiveTypeTermForType(resultType)

    generateOperatorIfNotNull(ctx, nullCheck, resultType, left, right) {
      (leftTerm, rightTerm) =>
        s"($resultTypeTerm) (${leftCasting(leftTerm)} $operator ${rightCasting(rightTerm)})"
    }
  }

  private def generateDecimalOperator(
      ctx: CodeGeneratorContext,
      operator: String,
      nullCheck: Boolean,
      resultType: DecimalType,
      left: GeneratedExpression,
      right: GeneratedExpression)
    : GeneratedExpression = {

    // do not cast a decimal operand to resultType, which may change its value.
    // use it as is during calculation.
    def castToDec(t: InternalType) = t match {
      case _: DecimalType =>
        (operandTerm: String) => s"$operandTerm"
      case _ => numericCasting(t, resultType)
    }
    val leftCasting = castToDec(left.resultType)
    val rightCasting = castToDec(right.resultType)

    val method = Decimal.Ref.operator(operator)
    generateOperatorIfNotNull(ctx, nullCheck, resultType, left, right) {
      (leftTerm, rightTerm) =>
        s"$method(${leftCasting(leftTerm)}, ${rightCasting(rightTerm)}, " +
          s"${resultType.precision}, ${resultType.scale})"
    }
  }

  def generateUnaryArithmeticOperator(
      ctx: CodeGeneratorContext,
      operator: String,
      nullCheck: Boolean,
      resultType: InternalType,
      operand: GeneratedExpression)
    : GeneratedExpression = {
    generateUnaryOperatorIfNotNull(ctx, nullCheck, resultType, operand) {
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

  /**
    * Gen in by a hash set.
    */
  def generateIn(
      ctx: CodeGeneratorContext,
      needle: GeneratedExpression,
      haystack: Seq[GeneratedExpression],
      nullCheck: Boolean)
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
      val setTerm = ctx.addReusableSet(elements, resultType)

      val castedNeedle = needle.copy(
        castNumeric(needle), // cast needle to wider type
        needle.nullTerm,
        needle.code,
        resultType)

      val Seq(resultTerm, nullTerm) = newNames(Seq("result", "isNull"))
      val resultTypeTerm = primitiveTypeTermForType(DataTypes.BOOLEAN)
      val defaultValue = primitiveDefaultValue(DataTypes.BOOLEAN)

      val operatorCode = if (nullCheck) {
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

      GeneratedExpression(resultTerm, nullTerm, operatorCode, DataTypes.BOOLEAN)
    } else {
      // we use a chain of ORs for a set that contains non-constant elements
      haystack
        .map(generateEquals(ctx, nullCheck, needle, _))
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
      ctx: CodeGeneratorContext,
      nullCheck: Boolean,
      left: GeneratedExpression,
      right: GeneratedExpression)
    : GeneratedExpression = {
    if (left.resultType == DataTypes.STRING && right.resultType == DataTypes.STRING) {
      BinaryStringCallGen.generateStringEquals(ctx, left, right)
    }
    // numeric types
    else if (isNumeric(left.resultType) && isNumeric(right.resultType)) {
      generateComparison(ctx, "==", nullCheck, left, right)
    }
    // temporal types
    else if (isTemporal(left.resultType) && left.resultType == right.resultType) {
      generateComparison(ctx, "==", nullCheck, left, right)
    }
    // array types
    else if (isArray(left.resultType) && left.resultType == right.resultType) {
      generateArrayComparison(ctx, nullCheck, left, right)
    }
    // map types
    else if (isMap(left.resultType) && left.resultType == right.resultType) {
      generateMapComparison(ctx, nullCheck, left, right)
    }
    // comparable types of same type
    else if (isComparable(left.resultType) && left.resultType == right.resultType) {
      generateComparison(ctx, "==", nullCheck, left, right)
    }
    // support date/time/timestamp equalTo string.
    // for performance, we cast literal string to literal time.
    else if (isTimePoint(left.resultType) && right.resultType == DataTypes.STRING) {
      if (right.literal) {
        generateEquals(ctx, nullCheck, left, strLiteralCastToTime(ctx, right, left.resultType))
      } else {
        generateEquals(ctx, nullCheck, left, generateCast(ctx, nullCheck, right, left.resultType))
      }
    }
    else if (isTimePoint(right.resultType) && left.resultType == DataTypes.STRING) {
      if (left.literal) {
        generateEquals(ctx, nullCheck, strLiteralCastToTime(ctx, left, right.resultType), right)
      } else {
        generateEquals(ctx, nullCheck, generateCast(ctx, nullCheck, left, right.resultType), right)
      }
    }
    // non comparable types
    else {
      generateOperatorIfNotNull(ctx, nullCheck, DataTypes.BOOLEAN, left, right) {
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

  def strLiteralCastToTime(
      ctx: CodeGeneratorContext,
      expr: GeneratedExpression,
      expectType: InternalType): GeneratedExpression = {
    val rightTerm = expr.resultTerm
    val typeTerm = primitiveTypeTermForType(expectType)
    val defaultTerm = primitiveDefaultValue(expectType)
    val toTimeMethod = expectType match {
      case DataTypes.DATE => BuiltInMethod.STRING_TO_DATE.method
      case DataTypes.TIME => BuiltInMethod.STRING_TO_TIME.method
      case DataTypes.TIMESTAMP => BuiltInMethod.STRING_TO_TIMESTAMP.method
    }
    val term = newName("stringToTime")
    ctx.addReusableMember(s"$typeTerm $term = ${expr.nullTerm} ? " +
        s"$defaultTerm : ${qualifyMethod(toTimeMethod)}($rightTerm.toString());")
    expr.copy(resultType = expectType, resultTerm = term)
  }

  def generateNotEquals(
      ctx: CodeGeneratorContext,
      nullCheck: Boolean,
      left: GeneratedExpression,
      right: GeneratedExpression)
    : GeneratedExpression = {
    if (left.resultType == DataTypes.STRING && right.resultType == DataTypes.STRING) {
      BinaryStringCallGen.generateStringNotEquals(ctx, left, right)
    }
    // numeric types
    else if (isNumeric(left.resultType) && isNumeric(right.resultType)) {
      generateComparison(ctx, "!=", nullCheck, left, right)
    }
    // temporal types
    else if (isTemporal(left.resultType) && left.resultType == right.resultType) {
      generateComparison(ctx, "!=", nullCheck, left, right)
    }
    // array types
    else if (isArray(left.resultType) && left.resultType == right.resultType) {
      val equalsExpr = generateEquals(ctx, nullCheck, left, right)
      GeneratedExpression(
        s"(!${equalsExpr.resultTerm})", equalsExpr.nullTerm, equalsExpr.code, DataTypes.BOOLEAN)
    }
    // map types
    else if (isMap(left.resultType) && left.resultType == right.resultType) {
      val equalsExpr = generateEquals(ctx, nullCheck, left, right)
      GeneratedExpression(
        s"(!${equalsExpr.resultTerm})", equalsExpr.nullTerm, equalsExpr.code, DataTypes.BOOLEAN)
    }
    // comparable types
    else if (isComparable(left.resultType) && left.resultType == right.resultType) {
      generateComparison(ctx, "!=", nullCheck, left, right)
    }
    // non-comparable types
    else {
      generateOperatorIfNotNull(ctx, nullCheck, DataTypes.BOOLEAN, left, right) {
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
      nullCheck: Boolean,
      left: GeneratedExpression,
      right: GeneratedExpression)
    : GeneratedExpression = {
    generateOperatorIfNotNull(ctx, nullCheck, DataTypes.BOOLEAN, left, right) {
      // either side is decimal
      if (isDecimal(left.resultType) || isDecimal(right.resultType)) {
        (leftTerm, rightTerm) => {
          s"${Decimal.Ref.compare}($leftTerm, $rightTerm) $operator 0"
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
            (leftTerm, rightTerm) => s"java.lang.Boolean.compare($leftTerm, $rightTerm) $operator 0"
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

  def generateArrayComparison(
      ctx: CodeGeneratorContext,
      nullCheck: Boolean,
      left: GeneratedExpression,
      right: GeneratedExpression): GeneratedExpression =
    generateCallWithStmtIfArgsNotNull(ctx, nullCheck, DataTypes.BOOLEAN, Seq(left, right)) {
      args =>
        val leftTerm = args.head
        val rightTerm = args(1)

        val resultTerm = newName("compareResult")
        val binaryArrayCls = classOf[BinaryArray].getCanonicalName

        val elementType = left.resultType.asInstanceOf[ArrayType].getElementInternalType
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
        val elementEqualsExpr = generateEquals(ctx, nullCheck, leftElementExpr, rightElementExpr)

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

  def generateMapComparison(
      ctx: CodeGeneratorContext,
      nullCheck: Boolean,
      left: GeneratedExpression,
      right: GeneratedExpression): GeneratedExpression =
    generateCallWithStmtIfArgsNotNull(ctx, nullCheck, DataTypes.BOOLEAN, Seq(left, right)) {
      args =>
        val leftTerm = args.head
        val rightTerm = args(1)

        val resultTerm = newName("compareResult")
        val binaryMapCls = classOf[BinaryMap].getCanonicalName

        val mapType = left.resultType.asInstanceOf[MapType]
        val mapCls = classOf[java.util.Map[AnyRef, AnyRef]].getCanonicalName
        val keyCls = boxedTypeTermForType(mapType.getKeyInternalType)
        val valueCls = boxedTypeTermForType(mapType.getValueInternalType)

        val leftMapTerm = newName("leftMap")
        val leftKeyTerm = newName("leftKey")
        val leftValueTerm = newName("leftValue")
        val leftValueNullTerm = newName("leftValueIsNull")
        val leftValueExpr =
          GeneratedExpression(leftValueTerm, leftValueNullTerm, "", mapType.getValueInternalType)

        val rightMapTerm = newName("rightMap")
        val rightValueTerm = newName("rightValue")
        val rightValueNullTerm = newName("rightValueIsNull")
        val rightValueExpr =
          GeneratedExpression(rightValueTerm, rightValueNullTerm, "", mapType.getValueInternalType)

        val entryTerm = newName("entry")
        val entryCls = classOf[java.util.Map.Entry[AnyRef, AnyRef]].getCanonicalName
        val valueEqualsExpr = generateEquals(ctx, nullCheck, leftValueExpr, rightValueExpr)

        val internalTypeCls = classOf[InternalType].getCanonicalName
        val keyTypeTerm =
          ctx.addReusableObject(mapType.getKeyInternalType, "keyType", internalTypeCls)
        val valueTypeTerm =
          ctx.addReusableObject(mapType.getValueInternalType, "valueType", internalTypeCls)

        val stmt =
          s"""
             |boolean $resultTerm;
             |if ($leftTerm instanceof $binaryMapCls && $rightTerm instanceof $binaryMapCls) {
             |  $resultTerm = $leftTerm.equals($rightTerm);
             |} else {
             |  if ($leftTerm.numElements() == $rightTerm.numElements()) {
             |    $resultTerm = true;
             |    $mapCls $leftMapTerm = $leftTerm.toJavaMap($keyTypeTerm, $valueTypeTerm);
             |    $mapCls $rightMapTerm = $rightTerm.toJavaMap($keyTypeTerm, $valueTypeTerm);
             |
             |    for ($entryCls $entryTerm : $leftMapTerm.entrySet()) {
             |      $keyCls $leftKeyTerm = ($keyCls) $entryTerm.getKey();
             |      if ($rightMapTerm.containsKey($leftKeyTerm)) {
             |        $valueCls $leftValueTerm = ($valueCls) $entryTerm.getValue();
             |        $valueCls $rightValueTerm = ($valueCls) $rightMapTerm.get($leftKeyTerm);
             |        boolean $leftValueNullTerm = ($leftValueTerm == null);
             |        boolean $rightValueNullTerm = ($rightValueTerm == null);
             |
             |        ${valueEqualsExpr.code}
             |        if (!${valueEqualsExpr.resultTerm}) {
             |          $resultTerm = false;
             |          break;
             |        }
             |      } else {
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

  def generateIsNull(
      nullCheck: Boolean,
      operand: GeneratedExpression): GeneratedExpression = {
    if (nullCheck) {
      GeneratedExpression(operand.nullTerm, "false", operand.code, DataTypes.BOOLEAN)
    }
    else if (!nullCheck && isReference(operand)) {
      val resultTerm = newName("isNull")
      val operatorCode = s"""
         |${operand.code}
         |boolean $resultTerm = ${operand.resultTerm} == null;
         |""".stripMargin.trim
      GeneratedExpression(resultTerm, "false", operatorCode, DataTypes.BOOLEAN)
    }
    else {
      GeneratedExpression("false", "false", operand.code, DataTypes.BOOLEAN)
    }
  }

  def generateIsNotNull(
      nullCheck: Boolean,
      operand: GeneratedExpression): GeneratedExpression = {
    if (nullCheck) {
      val resultTerm = newName("result")
      val operatorCode =
        s"""
           |${operand.code}
           |boolean $resultTerm = !${operand.nullTerm};
           |""".stripMargin.trim
      GeneratedExpression(resultTerm, "false", operatorCode, DataTypes.BOOLEAN)
    }
    else if (!nullCheck && isReference(operand)) {
      val resultTerm = newName("result")
      val operatorCode =
        s"""
           |${operand.code}
           |boolean $resultTerm = ${operand.resultTerm} != null;
           |""".stripMargin.trim
      GeneratedExpression(resultTerm, "false", operatorCode, DataTypes.BOOLEAN)
    }
    else {
      GeneratedExpression("true", "false", operand.code, DataTypes.BOOLEAN)
    }
  }

  def generateAnd(
      nullCheck: Boolean,
      left: GeneratedExpression,
      right: GeneratedExpression): GeneratedExpression = {
    val Seq(resultTerm, nullTerm) = newNames(Seq("result", "isNull"))

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

    GeneratedExpression(resultTerm, nullTerm, operatorCode, DataTypes.BOOLEAN)
  }

  def generateOr(
      nullCheck: Boolean,
      left: GeneratedExpression,
      right: GeneratedExpression): GeneratedExpression = {
    val Seq(resultTerm, nullTerm) = newNames(Seq("result", "isNull"))

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

    GeneratedExpression(resultTerm, nullTerm, operatorCode, DataTypes.BOOLEAN)
  }

  def generateNot(
      ctx: CodeGeneratorContext,
      nullCheck: Boolean,
      operand: GeneratedExpression)
    : GeneratedExpression = {
    // Three-valued logic:
    // no Unknown -> Two-valued logic
    // Unknown -> Unknown
    generateUnaryOperatorIfNotNull(ctx, nullCheck, DataTypes.BOOLEAN, operand) {
      operandTerm => s"!($operandTerm)"
    }
  }

  def generateIsTrue(operand: GeneratedExpression): GeneratedExpression = {
    GeneratedExpression(
      operand.resultTerm, // unknown is always false by default
      GeneratedExpression.NEVER_NULL,
      operand.code,
      DataTypes.BOOLEAN)
  }

  def generateIsNotTrue(operand: GeneratedExpression): GeneratedExpression = {
    GeneratedExpression(
      s"(!${operand.resultTerm})", // unknown is always false by default
      GeneratedExpression.NEVER_NULL,
      operand.code,
      DataTypes.BOOLEAN)
  }

  def generateIsFalse(operand: GeneratedExpression): GeneratedExpression = {
    GeneratedExpression(
      s"(!${operand.resultTerm} && !${operand.nullTerm})",
      GeneratedExpression.NEVER_NULL,
      operand.code,
      DataTypes.BOOLEAN)
  }

  def generateIsNotFalse(operand: GeneratedExpression): GeneratedExpression = {
    GeneratedExpression(
      s"(${operand.resultTerm} || ${operand.nullTerm})",
      GeneratedExpression.NEVER_NULL,
      operand.code,
      DataTypes.BOOLEAN)
  }

  def generateReinterpret(
                    ctx: CodeGeneratorContext,
                    nullCheck: Boolean,
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
    case (DataTypes.DATE, DataTypes.INT) |
         (DataTypes.TIME, DataTypes.INT) |
         (_: TimestampType, DataTypes.LONG) |
         (DataTypes.INT, DataTypes.DATE) |
         (DataTypes.INT, DataTypes.TIME) |
         (DataTypes.LONG, _: TimestampType) |
         (DataTypes.INT, DataTypes.INTERVAL_MONTHS) |
         (DataTypes.LONG, DataTypes.INTERVAL_MILLIS) |
         (DataTypes.INTERVAL_MONTHS, DataTypes.INT) |
         (DataTypes.INTERVAL_MILLIS, DataTypes.LONG) |
         (DataTypes.DATE, DataTypes.LONG) |
         (DataTypes.TIME, DataTypes.LONG) |
         (DataTypes.INTERVAL_MONTHS, DataTypes.LONG) =>
      internalExprCasting(operand, targetType)

    case (from, to) =>
      throw new CodeGenException(s"Unsupported reinterpret from '$from' to '$to'.")
  }

  def generateCast(
      ctx: CodeGeneratorContext,
      nullCheck: Boolean,
      operand: GeneratedExpression,
      targetType: InternalType)
    : GeneratedExpression = (operand.resultType, targetType) match {

    // special case: cast from TimeIndicatorTypeInfo to SqlTimeTypeInfo
    case (DataTypes.PROCTIME_INDICATOR, DataTypes.TIMESTAMP) |
         (DataTypes.ROWTIME_INDICATOR, DataTypes.TIMESTAMP) |
         (DataTypes.TIMESTAMP, DataTypes.PROCTIME_INDICATOR) |
         (DataTypes.TIMESTAMP, DataTypes.ROWTIME_INDICATOR) =>
      operand.copy(resultType = DataTypes.TIMESTAMP) // just replace the DataType

    // identity casting
    case (fromTp, toTp) if fromTp == toTp =>
      operand

    // Date/Time/Timestamp -> String
    case (left, DataTypes.STRING) if TypeCheckUtils.isTimePoint(left) =>
      generateReturnStringCallIfArgsNotNull(ctx, Seq(operand)) {
        operandTerm =>
          val zoneTerm = ctx.addReusableTimeZone()
          s"${internalToStringCode(left, operandTerm.head, zoneTerm)}"
      }

    // Interval Months -> String
    case (DataTypes.INTERVAL_MONTHS, DataTypes.STRING) =>
      val method = qualifyMethod(BuiltInMethod.INTERVAL_YEAR_MONTH_TO_STRING.method)
      val timeUnitRange = qualifyEnum(TimeUnitRange.YEAR_TO_MONTH)
      generateReturnStringCallIfArgsNotNull(ctx, Seq(operand)) {
        terms => s"$method(${terms.head}, $timeUnitRange)"
      }

    // Interval Millis -> String
    case (DataTypes.INTERVAL_MILLIS, DataTypes.STRING) =>
      val method = qualifyMethod(BuiltInMethod.INTERVAL_DAY_TIME_TO_STRING.method)
      val timeUnitRange = qualifyEnum(TimeUnitRange.DAY_TO_SECOND)
      generateReturnStringCallIfArgsNotNull(ctx, Seq(operand)) {
        terms => s"$method(${terms.head}, $timeUnitRange, 3)" // milli second precision
      }

    // Array -> String
    case (at: ArrayType, DataTypes.STRING) =>
      generateCastArrayToString(ctx, nullCheck, operand, at)

    // Byte array -> String UTF-8
    case (DataTypes.BYTE_ARRAY, DataTypes.STRING) =>
      val charset = classOf[StandardCharsets].getCanonicalName
      generateReturnStringCallIfArgsNotNull(ctx, Seq(operand)) {
        terms =>
          s"(new String(${genToExternal(ctx, operand.resultType,
            terms.head)
          }, $charset.UTF_8))"
      }


    // Map -> String
    case (mt: MapType, DataTypes.STRING) =>
      generateCastMapToString(ctx, nullCheck, operand, mt)

    // composite type -> String
    case (brt: RowType, DataTypes.STRING) =>
      generateBaseRowToString(ctx, nullCheck, operand, brt)

    // * (not Date/Time/Timestamp) -> String
    // TODO: GenericType with Date/Time/Timestamp -> String would call toString implicitly
    case (_, DataTypes.STRING) =>
      generateReturnStringCallIfArgsNotNull(ctx, Seq(operand)) {
        terms => s""" "" + ${terms.head}"""
      }

    // * -> Character
    case (_, DataTypes.CHAR) =>
      throw new CodeGenException("Character type not supported.")

    // String -> Boolean
    case (DataTypes.STRING, DataTypes.BOOLEAN) =>
      generateUnaryOperatorIfNotNull(
        ctx,
        nullCheck,
        targetType,
        operand,
        primitiveNullable = true) {
        operandTerm => s"$operandTerm.toBooleanSQL()"
      }

    // String -> NUMERIC TYPE (not Character)
    case (DataTypes.STRING, _)
      if TypeCheckUtils.isNumeric(targetType) =>
      targetType match {
        case dt: DecimalType =>
          generateUnaryOperatorIfNotNull(ctx, nullCheck, targetType, operand) {
            operandTerm => s"$operandTerm.toDecimal(${dt.precision}, ${dt.scale})"
          }
        case _ =>
          val methodName = targetType match {
            case DataTypes.BYTE => "toByte"
            case DataTypes.SHORT => "toShort"
            case DataTypes.INT => "toInt"
            case DataTypes.LONG => "toLong"
            case DataTypes.DOUBLE => "toDouble"
            case DataTypes.FLOAT => "toFloat"
            case _ => null
          }
          assert(methodName != null, "Unexpected data type.")
          generateUnaryOperatorIfNotNull(
            ctx,
            nullCheck,
            targetType,
            operand,
            primitiveNullable = true) {
            operandTerm => s"($operandTerm.trim().$methodName())"
          }
      }

    // String -> Date
    case (DataTypes.STRING, DataTypes.DATE) =>
      generateUnaryOperatorIfNotNull(ctx, nullCheck, targetType,
        operand, primitiveNullable = true) {
        operandTerm =>
          s"${qualifyMethod(BuiltInMethod.STRING_TO_DATE.method)}($operandTerm.toString())"
      }

    // String -> Time
    case (DataTypes.STRING, DataTypes.TIME) =>
      generateUnaryOperatorIfNotNull(ctx, nullCheck, targetType,
        operand, primitiveNullable = true) {
        operandTerm =>
          s"${qualifyMethod(BuiltInMethod.STRING_TO_TIME.method)}($operandTerm.toString())"
      }

    // String -> Timestamp
    case (DataTypes.STRING, DataTypes.TIMESTAMP) =>
      generateUnaryOperatorIfNotNull(ctx, nullCheck, targetType,
        operand, primitiveNullable = true) {
        operandTerm =>
          val zoneTerm = ctx.addReusableTimeZone()
          s"""${qualifyMethod(BuiltInMethods.STRING_TO_TIMESTAMP)}($operandTerm.toString(),
             | $zoneTerm)""".stripMargin
      }

    // String -> binary
    case (DataTypes.STRING, DataTypes.BYTE_ARRAY) =>
      generateUnaryOperatorIfNotNull(ctx, nullCheck, targetType, operand) {
        operandTerm => s"$operandTerm.getBytes()"
      }

    // Note: SQL2003 $6.12 - casting is not allowed between boolean and numeric types.
    //       Calcite does not allow it either.

    // Boolean -> BigDecimal
    case (DataTypes.BOOLEAN, dt: DecimalType) =>
      generateUnaryOperatorIfNotNull(ctx, nullCheck, targetType, operand) {
        operandTerm => s"${Decimal.Ref.castFrom}($operandTerm, ${dt.precision}, ${dt.scale})"
      }

    // Boolean -> NUMERIC TYPE
    case (DataTypes.BOOLEAN, _) if TypeCheckUtils.isNumeric(targetType) =>
      val targetTypeTerm = primitiveTypeTermForType(targetType)
      generateUnaryOperatorIfNotNull(ctx, nullCheck, targetType, operand) {
        operandTerm => s"($targetTypeTerm) ($operandTerm ? 1 : 0)"
      }

    // BigDecimal -> Boolean
    case (_: DecimalType, DataTypes.BOOLEAN) =>
      generateUnaryOperatorIfNotNull(ctx, nullCheck, targetType, operand) {
        operandTerm => s"${Decimal.Ref.castTo(targetType)}($operandTerm)"
      }

    // BigDecimal -> Timestamp
    case (_: DecimalType, DataTypes.TIMESTAMP) =>
      generateUnaryOperatorIfNotNull(ctx, nullCheck, targetType, operand) {
        operandTerm => s"${Decimal.Ref.castTo(targetType)}($operandTerm)"
      }

    // NUMERIC TYPE -> Boolean
    case (left, DataTypes.BOOLEAN) if isNumeric(left) =>
      generateUnaryOperatorIfNotNull(ctx, nullCheck, targetType, operand) {
        operandTerm => s"$operandTerm != 0"
      }

    // between NUMERIC TYPE | Decimal
    case  (left, right) if isNumeric(left) && isNumeric(right) =>
      val operandCasting = numericCasting(operand.resultType, targetType)
      generateUnaryOperatorIfNotNull(ctx, nullCheck, targetType, operand) {
        operandTerm => s"${operandCasting(operandTerm)}"
      }

    // Date -> Timestamp
    case (DataTypes.DATE, DataTypes.TIMESTAMP) =>
      generateUnaryOperatorIfNotNull(ctx, nullCheck, targetType, operand) {
        operandTerm =>
          s"$operandTerm * ${classOf[DateTimeUtils].getCanonicalName}.MILLIS_PER_DAY"
      }

    // Timestamp -> Date
    case (DataTypes.TIMESTAMP, DataTypes.DATE) =>
      val targetTypeTerm = primitiveTypeTermForType(targetType)
      generateUnaryOperatorIfNotNull(ctx, nullCheck, targetType, operand) {
        operandTerm =>
          s"($targetTypeTerm) ($operandTerm / " +
            s"${classOf[DateTimeUtils].getCanonicalName}.MILLIS_PER_DAY)"
      }

    // Time -> Timestamp
    case (DataTypes.TIME, DataTypes.TIMESTAMP) =>
      generateUnaryOperatorIfNotNull(ctx, nullCheck, targetType, operand) {
        operandTerm => s"$operandTerm"
      }

    // Timestamp -> Time
    case (DataTypes.TIMESTAMP, DataTypes.TIME) =>
      val targetTypeTerm = primitiveTypeTermForType(targetType)
      generateUnaryOperatorIfNotNull(ctx, nullCheck, targetType, operand) {
        operandTerm =>
          s"($targetTypeTerm) ($operandTerm % " +
            s"${classOf[DateTimeUtils].getCanonicalName}.MILLIS_PER_DAY)"
      }

    // Timestamp -> Decimal
    case  (DataTypes.TIMESTAMP, dt: DecimalType) =>
      generateUnaryOperatorIfNotNull(ctx, nullCheck, targetType, operand) {
        operandTerm => s"${Decimal.Ref.castFrom}" +
          s"(((double) ($operandTerm / 1000.0)), ${dt.precision}, ${dt.scale})"
      }

    // Tinyint -> Timestamp
    // Smallint -> Timestamp
    // Int -> Timestamp
    // Bigint -> Timestamp
    case (DataTypes.BYTE, DataTypes.TIMESTAMP) |
         (DataTypes.SHORT,DataTypes.TIMESTAMP) |
         (DataTypes.INT, DataTypes.TIMESTAMP) |
         (DataTypes.LONG, DataTypes.TIMESTAMP) =>
      generateUnaryOperatorIfNotNull(ctx, nullCheck, targetType, operand) {
        operandTerm => s"(((long) $operandTerm) * 1000)"
      }

    // Float -> Timestamp
    // Double -> Timestamp
    case (DataTypes.FLOAT, DataTypes.TIMESTAMP) |
         (DataTypes.DOUBLE, DataTypes.TIMESTAMP) =>
      generateUnaryOperatorIfNotNull(ctx, nullCheck, targetType, operand) {
        operandTerm => s"((long) ($operandTerm * 1000))"
      }

    // Timestamp -> Tinyint
    case (DataTypes.TIMESTAMP, DataTypes.BYTE) =>
      generateUnaryOperatorIfNotNull(ctx, nullCheck, targetType, operand) {
        operandTerm => s"((byte) ($operandTerm / 1000))"
      }

    // Timestamp -> Smallint
    case (DataTypes.TIMESTAMP, DataTypes.SHORT) =>
      generateUnaryOperatorIfNotNull(ctx, nullCheck, targetType, operand) {
        operandTerm => s"((short) ($operandTerm / 1000))"
      }

    // Timestamp -> Int
    case (DataTypes.TIMESTAMP, DataTypes.INT) =>
      generateUnaryOperatorIfNotNull(ctx, nullCheck, targetType, operand) {
        operandTerm => s"((int) ($operandTerm / 1000))"
      }

    // Timestamp -> BigInt
    case (DataTypes.TIMESTAMP, DataTypes.LONG) =>
      generateUnaryOperatorIfNotNull(ctx, nullCheck, targetType, operand) {
        operandTerm => s"((long) ($operandTerm / 1000))"
      }

    // Timestamp -> Float
    case (DataTypes.TIMESTAMP, DataTypes.FLOAT) =>
      generateUnaryOperatorIfNotNull(ctx, nullCheck, targetType, operand) {
        operandTerm => s"((float) ($operandTerm / 1000.0))"
      }

    // Timestamp -> Double
    case (DataTypes.TIMESTAMP, DataTypes.DOUBLE) =>
      generateUnaryOperatorIfNotNull(ctx, nullCheck, targetType, operand) {
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
    case (DataTypes.DATE, DataTypes.INT) |
         (DataTypes.TIME, DataTypes.INT) |
         (DataTypes.INT, DataTypes.DATE) |
         (DataTypes.INT, DataTypes.TIME) |
         (DataTypes.INT, DataTypes.INTERVAL_MONTHS) |
         (DataTypes.LONG, DataTypes.INTERVAL_MILLIS) |
         (DataTypes.INTERVAL_MONTHS, DataTypes.INT) |
         (DataTypes.INTERVAL_MILLIS, DataTypes.LONG) =>
      internalExprCasting(operand, targetType)

    // internal reinterpretation of temporal types
    // Date, Time, Interval Months -> Long
    case  (DataTypes.DATE, DataTypes.LONG)
        | (DataTypes.TIME, DataTypes.LONG)
        | (DataTypes.INTERVAL_MONTHS, DataTypes.LONG) =>
      internalExprCasting(operand, targetType)

    case (from, to) =>
      throw new CodeGenException(s"Unsupported cast from '$from' to '$to'.")
  }

  def generateCastArrayToString(
      ctx: CodeGeneratorContext,
      nullCheck: Boolean,
      operand: GeneratedExpression,
      at: ArrayType): GeneratedExpression =
    generateReturnStringCallWithStmtIfArgsNotNull(ctx, Seq(operand)) {
      terms =>
        val builderCls = classOf[StringBuilder].getCanonicalName
        val builderTerm = newName("builder")
        ctx.addReusableMember(s"""$builderCls $builderTerm = new $builderCls();""")

        val arrayTerm = terms.head

        val indexTerm = newName("i")
        val numTerm = newName("num")

        val elementType = at.getElementInternalType
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
        val castExpr = generateCast(ctx, nullCheck, elementExpr, DataTypes.STRING)

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

  def generateCastMapToString(
      ctx: CodeGeneratorContext,
      nullCheck: Boolean,
      operand: GeneratedExpression,
      mt: MapType): GeneratedExpression =
    generateReturnStringCallWithStmtIfArgsNotNull(ctx, Seq(operand)) {
      terms =>
        val resultTerm = newName("toStringResult")

        val builderCls = classOf[StringBuilder].getCanonicalName
        val builderTerm = newName("builder")
        ctx.addReusableMember(s"$builderCls $builderTerm = new $builderCls();")

        val mapTerm = terms.head
        val genericMapCls = classOf[GenericMap].getCanonicalName
        val genericMapTerm = newName("genericMap")
        val binaryMapCls = classOf[BinaryMap].getCanonicalName
        val binaryMapTerm = newName("binaryMap")
        val arrayCls = classOf[BinaryArray].getCanonicalName
        val keyArrayTerm = newName("keyArray")
        val valueArrayTerm = newName("valueArray")

        val indexTerm = newName("i")
        val numTerm = newName("num")

        val keyType = mt.getKeyInternalType
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
        val keyCastExpr = generateCast(ctx, nullCheck, keyExpr, DataTypes.STRING)

        val valueType = mt.getValueInternalType
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
        val valueCastExpr = generateCast(ctx, nullCheck, valueExpr, DataTypes.STRING)

        val stmt =
          s"""
             |String $resultTerm;
             |if ($mapTerm instanceof $binaryMapCls) {
             |  $binaryMapCls $binaryMapTerm = ($binaryMapCls) $mapTerm;
             |
             |  $arrayCls $keyArrayTerm = $binaryMapTerm.keyArray();
             |  $arrayCls $valueArrayTerm = $binaryMapTerm.valueArray();
             |
             |  $builderTerm.setLength(0);
             |  $builderTerm.append("{");
             |
             |  int $numTerm = $binaryMapTerm.numElements();
             |  for (int $indexTerm = 0; $indexTerm < $numTerm; $indexTerm++) {
             |    if ($indexTerm != 0) {
             |      $builderTerm.append(", ");
             |    }
             |
             |    ${keyCastExpr.code}
             |    if (${keyCastExpr.nullTerm}) {
             |      $builderTerm.append("null");
             |    } else {
             |      $builderTerm.append(${keyCastExpr.resultTerm});
             |    }
             |    $builderTerm.append("=");
             |
             |    ${valueCastExpr.code}
             |    if (${valueCastExpr.nullTerm}) {
             |      $builderTerm.append("null");
             |    } else {
             |      $builderTerm.append(${valueCastExpr.resultTerm});
             |    }
             |  }
             |  $builderTerm.append("}");
             |
             |  $resultTerm = $builderTerm.toString();
             |} else {
             |  $genericMapCls $genericMapTerm = ($genericMapCls) $mapTerm;
             |  $resultTerm = $genericMapTerm.toString();
             |}
             """.stripMargin
        (stmt, resultTerm)
    }

  def generateBaseRowToString(
      ctx: CodeGeneratorContext,
      nullCheck: Boolean,
      operand: GeneratedExpression,
      brt: RowType): GeneratedExpression =
    generateReturnStringCallWithStmtIfArgsNotNull(ctx, Seq(operand)) {
      terms =>
        val builderCls = classOf[StringBuilder].getCanonicalName
        val builderTerm = newName("builder")
        ctx.addReusableMember(s"""$builderCls $builderTerm = new $builderCls();""")

        val rowTerm = terms.head

        val appendCode = brt.getFieldTypes.map(_.toInternalType).zipWithIndex.map {
          case (elementType, idx) =>
            val elementCls = primitiveTypeTermForType(elementType)
            val elementTerm = newName("element")
            val elementExpr = GeneratedExpression(
              elementTerm, s"$rowTerm.isNullAt($idx)",
              s"$elementCls $elementTerm = ($elementCls) ${baseRowFieldReadAccess(
                ctx, idx, rowTerm, elementType)};", elementType)
            val castExpr = generateCast(ctx, nullCheck, elementExpr, DataTypes.STRING)
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
             |$appendCode
             """.stripMargin
        (stmt, s"$builderTerm.toString()")
    }

  def generateIfElse(
      ctx: CodeGeneratorContext,
      nullCheck: Boolean,
      operands: Seq[GeneratedExpression],
      resultType: InternalType,
      i: Int = 0)
    : GeneratedExpression = {
    // else part
    if (i == operands.size - 1) {
      generateCast(ctx, nullCheck, operands(i), resultType)
    }
    else {
      // check that the condition is boolean
      // we do not check for null instead we use the default value
      // thus null is false
      requireBoolean(operands(i), "CASE")
      val condition = operands(i)
      val trueAction = generateCast(ctx, nullCheck, operands(i + 1), resultType)
      val falseAction = generateIfElse(ctx, nullCheck, operands, resultType, i + 2)

      val Seq(resultTerm, nullTerm) = newNames(Seq("result", "isNull"))
      val resultTypeTerm = primitiveTypeTermForType(resultType)

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

  def generateTemporalPlusMinus(
      ctx: CodeGeneratorContext,
      plus: Boolean,
      nullCheck: Boolean,
      resultType: InternalType,
      left: GeneratedExpression,
      right: GeneratedExpression)
    : GeneratedExpression = {

    val op = if (plus) "+" else "-"

    (left.resultType, right.resultType) match {
      // arithmetic of time point and time interval
      case (DataTypes.INTERVAL_MONTHS, DataTypes.INTERVAL_MONTHS) |
           (DataTypes.INTERVAL_MILLIS, DataTypes.INTERVAL_MILLIS) =>
        generateArithmeticOperator(ctx, op, nullCheck, left.resultType, left, right)

      case (DataTypes.DATE, DataTypes.INTERVAL_MILLIS) =>
        generateOperatorIfNotNull(ctx, nullCheck, DataTypes.DATE, left, right) {
            (l, r) => s"$l $op ((int) ($r / ${MILLIS_PER_DAY}L))"
        }

      case (DataTypes.DATE, DataTypes.INTERVAL_MONTHS) =>
        generateOperatorIfNotNull(ctx, nullCheck, DataTypes.DATE, left, right) {
            (l, r) => s"${qualifyMethod(BuiltInMethod.ADD_MONTHS.method)}($l, $op($r))"
        }

      case (DataTypes.TIME, DataTypes.INTERVAL_MILLIS) =>
        generateOperatorIfNotNull(ctx, nullCheck, DataTypes.TIME, left, right) {
            (l, r) => s"$l $op ((int) ($r))"
        }

      case (DataTypes.TIMESTAMP, DataTypes.INTERVAL_MILLIS) =>
        generateOperatorIfNotNull(ctx, nullCheck, DataTypes.TIMESTAMP, left, right) {
          (l, r) => s"$l $op $r"
        }

      case (DataTypes.TIMESTAMP, DataTypes.INTERVAL_MONTHS) =>
        generateOperatorIfNotNull(ctx, nullCheck, DataTypes.TIMESTAMP, left, right) {
          (l, r) => s"${qualifyMethod(BuiltInMethod.ADD_MONTHS.method)}($l, $op($r))"
        }

      // minus arithmetic of time points (i.e. for TIMESTAMPDIFF)
      case (DataTypes.TIMESTAMP | DataTypes.TIME | DataTypes.DATE, DataTypes.TIMESTAMP |
            DataTypes.TIME | DataTypes.DATE) if !plus =>
        resultType match {
          case DataTypes.INTERVAL_MONTHS =>
            generateOperatorIfNotNull(ctx, nullCheck, resultType, left, right) {
              (ll, rr) => (left.resultType, right.resultType) match {
                case (DataTypes.TIMESTAMP, DataTypes.DATE) =>
                  s"${qualifyMethod(BuiltInMethod.SUBTRACT_MONTHS.method)}" +
                      s"($ll, $rr * ${MILLIS_PER_DAY}L)"
                case (DataTypes.DATE, DataTypes.TIMESTAMP) =>
                  s"${qualifyMethod(BuiltInMethod.SUBTRACT_MONTHS.method)}" +
                      s"($ll * ${MILLIS_PER_DAY}L, $rr)"
                case _ =>
                  s"${qualifyMethod(BuiltInMethod.SUBTRACT_MONTHS.method)}($ll, $rr)"
              }
            }

          case DataTypes.INTERVAL_MILLIS =>
            generateOperatorIfNotNull(ctx, nullCheck, resultType, left, right) {
              (ll, rr) => (left.resultType, right.resultType) match {
                case (DataTypes.TIMESTAMP, DataTypes.TIMESTAMP) =>
                  s"$ll $op $rr"
                case (DataTypes.DATE, DataTypes.DATE) =>
                  s"($ll * ${MILLIS_PER_DAY}L) $op ($rr * ${MILLIS_PER_DAY}L)"
                case (DataTypes.TIMESTAMP, DataTypes.DATE) =>
                  s"$ll $op ($rr * ${MILLIS_PER_DAY}L)"
                case (DataTypes.DATE, DataTypes.TIMESTAMP) =>
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
      nullCheck: Boolean,
      operand: GeneratedExpression)
    : GeneratedExpression = {
    val operator = if (plus) "+" else "-"
    generateUnaryArithmeticOperator(ctx, operator, nullCheck, operand.resultType, operand)
  }

  private[flink] def makeReusableRow(
      ctx: CodeGeneratorContext,
      rowType: RowType,
      elements: Seq[GeneratedExpression],
      nullCheck: Boolean,
      initRow: Boolean): GeneratedExpression = {

    val rowTerm = newName("row")
    val rowCls = classOf[BinaryRow].getCanonicalName

    val writerTerm = newName("writer")
    val writerCls = classOf[BinaryRowWriter].getCanonicalName

    val writeCode = elements.zipWithIndex.map {
      case (element, idx) =>
        val tpe = rowType.getInternalTypeAt(idx)
        if (nullCheck) {
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

    ctx.addReusableMember(s"$rowCls $rowTerm = new $rowCls(${rowType.getArity});",
      if (initRow) code else "")
    ctx.addReusableMember(s"$writerCls $writerTerm = new $writerCls($rowTerm);")
    GeneratedExpression(rowTerm, GeneratedExpression.NEVER_NULL, code, rowType)
  }

  def generateRow(
      ctx: CodeGeneratorContext,
      resultType: InternalType,
      elements: Seq[GeneratedExpression],
      nullCheck: Boolean): GeneratedExpression = {
    val rowType = resultType.asInstanceOf[RowType]
    val fieldTypes = rowType.getFieldTypes

    def getLiteralRow: GeneratedExpression =
      makeReusableRow(ctx, rowType, elements, nullCheck, initRow = true)

    def getPrimitiveRow: GeneratedExpression = {
      val mapped = elements.zipWithIndex.map { case (element, idx) =>
        if (element.literal) {
          element
        } else {
          val tpe = fieldTypes(idx).toInternalType
          val resultTerm = primitiveDefaultValue(tpe)
          val nullTerm = if (resultTerm == "null") "true" else "false"
          GeneratedExpression(resultTerm, nullTerm, "", tpe)
        }
      }
      makeReusableRow(ctx, rowType, mapped, nullCheck, initRow = true)
    }

    def getNonPrimitiveRow: GeneratedExpression =
      makeReusableRow(ctx, rowType, elements, nullCheck, initRow = false)

    (elements.forall(element => element.resultType.isInstanceOf[PrimitiveType]),
      elements.forall(element => element.literal)) match {
      case (_, true) =>
        GeneratedExpression(
          getLiteralRow.resultTerm, GeneratedExpression.NEVER_NULL, "", rowType)
      case (true, false) =>
        val row = getPrimitiveRow
        val updateCode = elements.zipWithIndex.map { case (element, idx) =>
          val tpe = fieldTypes(idx).toInternalType
          if (element.literal) {
            ""
          } else if(nullCheck) {
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
        GeneratedExpression(
          row.resultTerm, GeneratedExpression.NEVER_NULL, updateCode, rowType)
      case (false, false) =>
        getNonPrimitiveRow
    }
  }

  private[flink] def makeReusableArray(
      ctx: CodeGeneratorContext,
      resultType: ArrayType,
      elements: Seq[GeneratedExpression],
      nullCheck: Boolean,
      initArray: Boolean): GeneratedExpression = {
    val elementType = resultType.getElementInternalType
    val arrayTerm = newName("array")
    val writerTerm = newName("writer")
    val arrayCls = classOf[BinaryArray].getCanonicalName
    val writerCls = classOf[BinaryArrayWriter].getCanonicalName
    val elementSize = BinaryArray.calculateElementSize(elementType)

    val writeCode = elements
      .zipWithIndex
      .map { case (element, idx) =>
        s"""
           |${element.code}
           |if (${element.nullTerm}) {
           |  ${baseArraySetNull(idx, writerTerm, elementType)};
           |} else {
           |  ${binaryWriterWriteField(
          ctx, idx, element.resultTerm, writerTerm, elementType)};
           |}
             """.stripMargin.trim
      }.mkString("\n")

    val code =
      s"""
         |$writerTerm.reset();
         |$writeCode
         |$writerTerm.complete();
         """.stripMargin

    ctx.addReusableMember(s"$arrayCls $arrayTerm = new $arrayCls();",
      if (initArray) code else "")
    ctx.addReusableMember(
      s"$writerCls $writerTerm = new $writerCls($arrayTerm, ${elements.length}, $elementSize);")
    GeneratedExpression(arrayTerm, GeneratedExpression.NEVER_NULL, code, resultType)
  }

  def generateArray(
      ctx: CodeGeneratorContext,
      resultType: InternalType,
      elements: Seq[GeneratedExpression],
      nullCheck: Boolean): GeneratedExpression = {
    val arrayType = resultType.asInstanceOf[ArrayType]
    val elementType = arrayType.getElementInternalType

    def getLiteralArray: GeneratedExpression =
      makeReusableArray(ctx, arrayType, elements, nullCheck, initArray = true)

    def getPrimitiveArray: GeneratedExpression = {
      val mapped = elements.map { element =>
        if (element.literal) {
          element
        } else {
          val resultTerm = primitiveDefaultValue(elementType)
          val nullTerm = if (resultTerm == "null") "true" else "false"
          GeneratedExpression(resultTerm, nullTerm, "", elementType)
        }
      }
      makeReusableArray(ctx, arrayType, mapped, nullCheck, initArray = true)
    }

    def getNonPrimitiveArray: GeneratedExpression =
      makeReusableArray(ctx, arrayType, elements, nullCheck, initArray = false)

    (elements.forall(element => element.resultType.isInstanceOf[PrimitiveType]),
      elements.forall(element => element.literal)) match {
      case (_, true) =>
        GeneratedExpression(
          getLiteralArray.resultTerm, GeneratedExpression.NEVER_NULL, "", arrayType)
      case (true, false) =>
        val array = getPrimitiveArray
        val updateCode = elements.zipWithIndex.map { case (element, idx) =>
          if (element.literal) {
            ""
          } else if(nullCheck) {
            s"""
               |${element.code}
               |if (${element.nullTerm}) {
               |  ${baseArraySetNull(idx, array.resultTerm, elementType)};
               |} else {
               |  ${binaryRowFieldSetAccess(
              idx, array.resultTerm, elementType, element.resultTerm)};
               |}
           """.stripMargin
          } else {
            s"""
               |${element.code}
               |${binaryRowFieldSetAccess(
              idx, array.resultTerm, elementType, element.resultTerm)};
           """.stripMargin
          }
        }.mkString("\n")
        GeneratedExpression(
          array.resultTerm, GeneratedExpression.NEVER_NULL, updateCode, arrayType)
      case (false, false) =>
        getNonPrimitiveArray
    }
  }

  def generateArrayElementAt(
      ctx: CodeGeneratorContext,
      array: GeneratedExpression,
      index: GeneratedExpression,
      nullCheck: Boolean): GeneratedExpression = {
    val Seq(resultTerm, nullTerm) = newNames(Seq("result", "isNull"))
    val componentInfo = array.resultType.asInstanceOf[ArrayType].getElementInternalType
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
         |""".stripMargin.trim

    GeneratedExpression(resultTerm, nullTerm, arrayAccessCode, componentInfo)
  }

  def generateArrayElement(
      ctx: CodeGeneratorContext,
      array: GeneratedExpression,
      nullCheck: Boolean): GeneratedExpression = {
    val Seq(resultTerm, nullTerm) = newNames(Seq("result", "isNull"))
    val resultType = array.resultType.asInstanceOf[ArrayType].getElementInternalType
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
         |""".stripMargin.trim

    GeneratedExpression(resultTerm, nullTerm, arrayAccessCode, resultType)
  }

  def generateArrayCardinality(
      ctx: CodeGeneratorContext,
      nullCheck: Boolean,
      array: GeneratedExpression)
    : GeneratedExpression = {

    generateUnaryOperatorIfNotNull(ctx, nullCheck, DataTypes.INT, array) {
      _ => s"${array.resultTerm}.numElements()"
    }
  }

  def generateMap(
      ctx: CodeGeneratorContext,
      resultType: InternalType,
      elements: Seq[GeneratedExpression],
      nullCheck: Boolean): GeneratedExpression = {

    def getLiteralMapArray(
        tpe: InternalType,
        elements: Seq[GeneratedExpression]): GeneratedExpression =
      makeReusableArray(ctx, DataTypes.createArrayType(tpe), elements, nullCheck, initArray = true)

    def getPrimitiveMapArray(
        tpe: InternalType,
        elements: Seq[GeneratedExpression]): GeneratedExpression = {
      val mapped = elements.map { item =>
        if (item.literal) {
          item
        } else {
          val resultTerm = primitiveDefaultValue(tpe)
          val nullTerm = if (resultTerm == "null") "true" else "false"
          GeneratedExpression(resultTerm, nullTerm, "", tpe)
        }
      }
      makeReusableArray(ctx, DataTypes.createArrayType(tpe), mapped, nullCheck, initArray = true)
    }

    def getNonPrimitiveMapArray(
        tpe: InternalType,
        elements: Seq[GeneratedExpression]): GeneratedExpression =
      makeReusableArray(ctx, DataTypes.createArrayType(tpe), elements, nullCheck, initArray = false)

    def getPrimitiveMapArrayUpdateCode(
        arrayTerm: String,
        tpe: InternalType,
        elements: Seq[GeneratedExpression]): String = {
      elements.zipWithIndex.map {
        case (key, i) => if (key.literal) {
          ""
        } else if (nullCheck) {
          s"""
             |${key.code}
             |if (${key.nullTerm}) {
             |  ${CodeGenUtils.baseArraySetNull(i, arrayTerm, tpe)};
             |} else {
             |  $arrayTerm.setNotNullAt($i);
             |  ${binaryRowFieldSetAccess(i, arrayTerm, tpe, key.resultTerm)};
             |}
             |""".stripMargin
        } else {
          s"""
             |${key.code}
             |${binaryRowFieldSetAccess(i, arrayTerm, tpe, key.resultTerm)};
             |""".stripMargin
        }
      }.mkString("\n")
    }

    val mapType = resultType.asInstanceOf[MapType]
    val mapTerm = newName("map")
    val binaryMapTerm = newName("binaryMap")

    // Prepare map key array
    val keyElements = elements.grouped(2).map { case Seq(key, _) => key }.toSeq
    val keyType = mapType.getKeyInternalType
    val (keyArr, keyUpdate, keyNeedsRefill) =
      (keyType, keyElements.forall(_.literal)) match {
        case (_, true) =>
          (getLiteralMapArray(keyType, keyElements), "", false)
        case (_ :PrimitiveType, false) =>
          val arr = getPrimitiveMapArray(keyType, keyElements)
          val update = getPrimitiveMapArrayUpdateCode(arr.resultTerm, keyType, keyElements)
          (arr, update, false)
        case (_, false) =>
          val arr = getNonPrimitiveMapArray(keyType, keyElements)
          val update = arr.code
          (arr, update, true)
      }

    // Prepare map value array
    val valueElements = elements.grouped(2).map { case Seq(_, value) => value }.toSeq
    val valueType = mapType.getValueInternalType
    val (valueArr, valueUpdate, valueNeedsRefill) =
      (valueType, valueElements.forall(_.literal)) match {
        case (_, true) =>
          (getLiteralMapArray(valueType, valueElements), "", false)
        case (_ :PrimitiveType, false) =>
          val arr = getPrimitiveMapArray(valueType, valueElements)
          val update = getPrimitiveMapArrayUpdateCode(arr.resultTerm, valueType, valueElements)
          (arr, update, false)
        case (_, false) =>
          val arr = getNonPrimitiveMapArray(valueType, valueElements)
          val update = arr.code
          (arr, update, true)
      }

    // Construct binary map
    val mapCls = classOf[BaseMap].getCanonicalName
    val binaryMapCls = classOf[BinaryMap].getCanonicalName
    val initMap = if (!keyNeedsRefill && !valueNeedsRefill) {
        s"""
           |$binaryMapCls $binaryMapTerm =
           |  $binaryMapCls.valueOf(${keyArr.resultTerm}, ${valueArr.resultTerm});
           |${keyArr.resultTerm} = $binaryMapTerm.keyArray();
           |${valueArr.resultTerm} = $binaryMapTerm.valueArray();
           |$mapTerm = $binaryMapTerm;
           |""".stripMargin
      } else {
        ""
      }
    ctx.addReusableMember(s"$mapCls $mapTerm = null;", initMap)

    // Update binary map
    val code = if (keyNeedsRefill || valueNeedsRefill) {
      s"""
         |$keyUpdate
         |$valueUpdate
         |$mapTerm = $binaryMapCls.valueOf(${keyArr.resultTerm}, ${valueArr.resultTerm});
         |""".stripMargin
    } else {
      s"""
         |$keyUpdate
         |$valueUpdate
         |""".stripMargin
    }

    GeneratedExpression(mapTerm, GeneratedExpression.NEVER_NULL, code, resultType)
  }

  def generateMapGet(
      ctx: CodeGeneratorContext,
      map: GeneratedExpression,
      key: GeneratedExpression,
      nullCheck: Boolean): GeneratedExpression = {
    val Seq(resultTerm, nullTerm) = newNames(Seq("result", "isNull"))
    val tmpKey = newName("key")
    val tmpValue = newName("value")
    val length = newName("length")
    val keys = newName("keys")
    val values = newName("values")
    val index = newName("index")
    val found = newName("found")

    val ty = map.resultType.asInstanceOf[MapType]
    val keyType = ty.getKeyInternalType
    val valueType = ty.getValueInternalType

    val keyTypeTerm = primitiveTypeTermForType(keyType)
    val valueTypeTerm = primitiveTypeTermForType(valueType)
    val boxedValueTypeTerm = boxedTypeTermForType(valueType)
    val valueDefault = primitiveDefaultValue(valueType)

    val mapTerm = map.resultTerm
    val binaryMapTypeTerm = classOf[BinaryMap].getCanonicalName
    val binaryMapTerm = newName("binaryMap")
    val genericMapTypeTerm = classOf[GenericMap].getCanonicalName
    val genericMapTerm = newName("genericMap")

    val arrayTypeTerm = classOf[BinaryArray].getCanonicalName

    val equal = generateEquals(
      ctx, nullCheck, key, GeneratedExpression(tmpKey, "false", "", keyType))
    val code =
      s"""
         |if ($mapTerm instanceof $binaryMapTypeTerm) {
         |  $binaryMapTypeTerm $binaryMapTerm = ($binaryMapTypeTerm) $mapTerm;
         |
         |  final int $length = $binaryMapTerm.numElements();
         |  final $arrayTypeTerm $keys = $binaryMapTerm.keyArray();
         |  final $arrayTypeTerm $values = $binaryMapTerm.valueArray();
         |
         |  int $index = 0;
         |  boolean $found = false;
         |  while ($index < $length && !$found) {
         |    final $keyTypeTerm $tmpKey = ${baseRowFieldReadAccess(ctx, index, keys, keyType)};
         |    ${equal.code}
         |    if (${equal.resultTerm}) {
         |      $found = true;
         |    } else {
         |      $index++;
         |    }
         |  }
         |
         |  if (!$found || $values.isNullAt($index)) {
         |    $nullTerm = true;
         |  } else {
         |    $resultTerm = ${baseRowFieldReadAccess(ctx, index, values, valueType)};
         |  }
         |} else {
         |  $genericMapTypeTerm $genericMapTerm = ($genericMapTypeTerm) $mapTerm;
         |  $boxedValueTypeTerm $tmpValue =
         |    ($boxedValueTypeTerm) $genericMapTerm.get(($keyTypeTerm) ${key.resultTerm});
         |  if ($tmpValue == null) {
         |    $nullTerm = true;
         |  } else {
         |    $resultTerm = $tmpValue;
         |  }
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
        """.stripMargin.trim

    GeneratedExpression(resultTerm, nullTerm, accessCode, valueType)
  }

  def generateMapCardinality(
      ctx: CodeGeneratorContext,
      nullCheck: Boolean,
      map: GeneratedExpression): GeneratedExpression = {
    generateUnaryOperatorIfNotNull(ctx, nullCheck, DataTypes.INT, map) {
      _ => s"${map.resultTerm}.numElements()"
    }
  }

  // ----------------------------------------------------------------------------------------------

  private def internalExprCasting(
      expr: GeneratedExpression,
      typeInfo: InternalType)
    : GeneratedExpression = {
    GeneratedExpression(expr.resultTerm, expr.nullTerm, expr.code, typeInfo)
  }

  def numericCasting(
      operandType: InternalType,
      resultType: InternalType)
    : String => String = {

    val resultTypeTerm = primitiveTypeTermForType(resultType)
    val resultTypeValue = resultTypeTerm + "Value()"
    val wrapperClass = boxedTypeTermForType(operandType)

    // no casting necessary
    if (operandType == resultType) {
      operandTerm => s"$operandTerm"
    }
    // decimal to decimal, may have different precision/scale
    else if (isDecimal(resultType) && isDecimal(operandType)) {
      val dt = resultType.asInstanceOf[DecimalType]
      operandTerm =>
        s"${Decimal.Ref.castTo(classOf[Decimal])}($operandTerm, " +
          s"${dt.precision()}, ${dt.scale()})"
    }
    // numeric to decimal
    else if (isDecimal(resultType) && isNumeric(operandType)) {
      val dt = resultType.asInstanceOf[DecimalType]
      operandTerm =>
        s"${Decimal.Ref.castFrom}($operandTerm, " +
          s"${dt.precision()}, ${dt.scale()})"
    }
    // decimal to numeric
    else if (isNumeric(resultType) && isDecimal(operandType) ) {
      operandTerm =>
        s"${Decimal.Ref.castTo(resultType)}($operandTerm)"
    }
    // numeric to numeric
    // TODO: Create a wrapper layer that handles type conversion between numeric.
    else if (isNumeric(operandType) && isNumeric(resultType)) {
      operandTerm => s"(new $wrapperClass($operandTerm)).$resultTypeValue"
    }
    // result type is time interval and operand type is integer
    else if (isTimeInterval(resultType) && isInteger(operandType)){
      operandTerm => s"(($resultTypeTerm) $operandTerm)"
    }
    else {
      throw new CodeGenException(s"Unsupported casting from $operandType to $resultType.")
    }
  }

  def generateRuntimeFilter(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      func: SqlRuntimeFilterFunction): GeneratedExpression = {
    val rfFuture = newName("rfFuture")
    val futureField = classOf[CompletableFuture[_]].getCanonicalName
    ctx.addReusableMember(s"private transient $futureField $rfFuture;")
    val rfUtils = classOf[RuntimeFilterUtils].getCanonicalName
    ctx.addReusableOpenStatement(s"$rfFuture = $rfUtils.asyncGetBroadcastBloomFilter(" +
        s"getRuntimeContext(), ${"\""}${func.getBroadcastId}${"\""});")

    val bfField = classOf[BloomFilter].getCanonicalName
    val bf = newName("bf")
    val rfResult = newName("rfResult")
    val waitRf = ctx.getTableConfig.getConf.getBoolean(
      TableConfigOptions.SQL_EXEC_RUNTIME_FILTER_WAIT)
    val (hashCode, hash) = runtimeFilterHash(operands.head)
    var verify =
      s"""
         |$bfField $bf = ($bfField) $rfFuture.get();
         |if ($bf != null) {
         |  $hashCode
         |  $rfResult = $bf.testHash($hash);
         |}
       """.stripMargin
    if (!waitRf) {
      verify =
          s"""
             |if ($rfFuture.isDone()) {
             |  $verify
             |}
             """.stripMargin
    }
    val code =
      s"""
         |${operands.head.code}
         |boolean $rfResult = true;
         |$verify
       """.stripMargin
    GeneratedExpression(rfResult, "false", code, DataTypes.BOOLEAN)
  }

  def generateRuntimeFilterBuilder(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      func: SqlRuntimeFilterBuilderFunction): GeneratedExpression = {
    val bf = newName("bfField")
    val bfField = classOf[BloomFilter].getCanonicalName
    ctx.addReusableMember(s"private transient $bfField $bf;")
    ctx.addReusableOpenStatement(s"$bf = new $bfField(" +
        s"$bfField.suitableMaxNumEntries(${func.ndv.longValue()})," +
        s"${func.minFpp(ctx.tableConfig)});")
    val accTypeField = classOf[BloomFilterAcc].getCanonicalName
    val quotaBid = "\"" + func.broadcastId + "\""

    val accField = newName("acc")
    val serializedValue = classOf[SerializedValue[_]].getCanonicalName
    ctx.addReusableMember(s"$accTypeField $accField = new $accTypeField();")
    ctx.addReusableOpenStatement(s"getRuntimeContext().addPreAggregatedAccumulator(" +
        s"$quotaBid, $accField);")
    // must endInput because close will be invoke in failover.
    ctx.addReusableEndInputStatement(
      s"""
         |$accField.add($serializedValue.fromBytes($bfField.toBytes($bf)));
         |getRuntimeContext().commitPreAggregatedAccumulator($quotaBid);
       """.stripMargin)

    val (hashCode, hash) = runtimeFilterHash(operands.head)
    val code =
      s"""
         |${operands.head.code}
         |$hashCode
         |$bf.addHash($hash);
       """.stripMargin
    GeneratedExpression("true", "false", code, DataTypes.BOOLEAN)
  }

  def runtimeFilterHash(expr: GeneratedExpression): (String, String) = {
    val bf = classOf[BloomFilter].getCanonicalName
    val brUtil = classOf[BinaryRowUtil].getCanonicalName
    val hash = newName("rfHashCode")
    val term = expr.resultTerm
    val goHash = expr.resultType match {
      case t if TypeCheckUtils.isNumeric(t) || TypeCheckUtils.isTemporal(t) =>
        s"$bf.getLongHash($term)"
      case DataTypes.STRING => s"$term.hash64()"
      case DataTypes.BYTE_ARRAY => s"$brUtil.hashByteArray64($term)"
      case _: DecimalType => s"$brUtil.hashDecimal64($term)"
      case _ => HashCodeGenerator.hashExpr(expr)
    }
    (s"""
       |long $hash = 0;
       |if (!${expr.nullTerm}) {
       |  $hash = $goHash;
       |}
       |
     """.stripMargin, hash)
  }

  def generateDOT(ctx: CodeGeneratorContext,
                  operands: Seq[GeneratedExpression],
                  nullCheck: Boolean): GeneratedExpression = {

    // Due to https://issues.apache.org/jira/browse/CALCITE-2162, expression such as
    // "array[1].a.b" won't work now.
    if (operands.size > 2) {
      throw new CodeGenException(
        "A DOT operator with more than 2 operands is not supported yet.")
    }

    val fieldName = operands(1).literalValue.toString
    val fieldIdx = operands
        .head
        .resultType
        .asInstanceOf[RowType]
        .getFieldIndex(fieldName)
    val access = generateFieldAccess(
      ctx,
      operands.head.resultType,
      operands.head.resultTerm,
      fieldIdx,
      nullCheck)

    val Seq(resultTerm, nullTerm) = newNames(Seq("result", "isNull"))
    val resultTypeTerm = primitiveTypeTermForType(access.resultType)
    val defaultValue = primitiveDefaultValue(access.resultType)

    val resultCode = if (nullCheck) {
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
}
