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

import java.lang.reflect.{Field, Method}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.util.BuiltInMethod
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo._
import org.apache.flink.api.common.typeinfo._
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils._
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.table.codegen.GeneratedExpression.NEVER_NULL
import org.apache.flink.table.codegen.calls.ScalarOperators._
import org.apache.flink.table.codegen.calls.{CurrentTimePointCallGen, FunctionGenerator}
import org.apache.flink.table.functions.sql.{ScalarSqlFunctions, StreamRecordTimestampSqlFunction}
import org.apache.flink.table.typeutils.TypeCheckUtils.{isNumeric, isTemporal, isTimeInterval, isTimePoint}
import org.apache.flink.table.typeutils.{TimeIndicatorTypeInfo, TimeIntervalTypeInfo, TypeCheckUtils}

object CodeGenUtils {

  private val nameCounter = new AtomicInteger

  def newName(name: String): String = {
    s"$name$$${nameCounter.getAndIncrement}"
  }

  // when casting we first need to unbox Primitives, for example,
  // float a = 1.0f;
  // byte b = (byte) a;
  // works, but for boxed types we need this:
  // Float a = 1.0f;
  // Byte b = (byte)(float) a;
  def primitiveTypeTermForTypeInfo(tpe: TypeInformation[_]): String = tpe match {
    case INT_TYPE_INFO => "int"
    case LONG_TYPE_INFO => "long"
    case SHORT_TYPE_INFO => "short"
    case BYTE_TYPE_INFO => "byte"
    case FLOAT_TYPE_INFO => "float"
    case DOUBLE_TYPE_INFO => "double"
    case BOOLEAN_TYPE_INFO => "boolean"
    case CHAR_TYPE_INFO => "char"

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

    // internal primitive representation of time points
    case SqlTimeTypeInfo.DATE => "int"
    case SqlTimeTypeInfo.TIME => "int"
    case SqlTimeTypeInfo.TIMESTAMP => "long"

    // internal primitive representation of time intervals
    case TimeIntervalTypeInfo.INTERVAL_MONTHS => "int"
    case TimeIntervalTypeInfo.INTERVAL_MILLIS => "long"

    case _ =>
      tpe.getTypeClass.getCanonicalName
  }

  def boxedTypeTermForTypeInfo(tpe: TypeInformation[_]): String = tpe match {
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

    // time indicators are represented as Long even if they seem to be Timestamp
    case _: TimeIndicatorTypeInfo => "java.lang.Long"

    case _ =>
      tpe.getTypeClass.getCanonicalName
  }

  def primitiveDefaultValue(tpe: TypeInformation[_]): String = tpe match {
    case INT_TYPE_INFO => "-1"
    case LONG_TYPE_INFO => "-1L"
    case SHORT_TYPE_INFO => "-1"
    case BYTE_TYPE_INFO => "-1"
    case FLOAT_TYPE_INFO => "-1.0f"
    case DOUBLE_TYPE_INFO => "-1.0d"
    case BOOLEAN_TYPE_INFO => "false"
    case STRING_TYPE_INFO => "\"\""
    case CHAR_TYPE_INFO => "'\\0'"
    case SqlTimeTypeInfo.DATE | SqlTimeTypeInfo.TIME => "-1"
    case SqlTimeTypeInfo.TIMESTAMP => "-1L"
    case TimeIntervalTypeInfo.INTERVAL_MONTHS => "-1"
    case TimeIntervalTypeInfo.INTERVAL_MILLIS => "-1L"

    case _ => "null"
  }

  def superPrimitive(typeInfo: TypeInformation[_]): String = typeInfo match {
    case _: FractionalTypeInfo[_] => "double"
    case _ => "long"
  }

  def qualifyMethod(method: Method): String =
    method.getDeclaringClass.getCanonicalName + "." + method.getName

  def qualifyEnum(enum: Enum[_]): String =
    enum.getClass.getCanonicalName + "." + enum.name()

  def internalToTimePointCode(resultType: TypeInformation[_], resultTerm: String): String =
    resultType match {
      case _: TimeIndicatorTypeInfo =>
        resultTerm // time indicators are not modified
      case SqlTimeTypeInfo.DATE =>
        s"${qualifyMethod(BuiltInMethod.INTERNAL_TO_DATE.method)}($resultTerm)"
      case SqlTimeTypeInfo.TIME =>
        s"${qualifyMethod(BuiltInMethod.INTERNAL_TO_TIME.method)}($resultTerm)"
      case SqlTimeTypeInfo.TIMESTAMP =>
        s"${qualifyMethod(BuiltInMethod.INTERNAL_TO_TIMESTAMP.method)}($resultTerm)"
    }

  def timePointToInternalCode(resultType: TypeInformation[_], resultTerm: String): String =
    resultType match {
      case SqlTimeTypeInfo.DATE =>
        s"${qualifyMethod(BuiltInMethod.DATE_TO_INT.method)}($resultTerm)"
      case SqlTimeTypeInfo.TIME =>
        s"${qualifyMethod(BuiltInMethod.TIME_TO_INT.method)}($resultTerm)"
      case SqlTimeTypeInfo.TIMESTAMP =>
        s"${qualifyMethod(BuiltInMethod.TIMESTAMP_TO_LONG.method)}($resultTerm)"
    }

  def compareEnum(term: String, enum: Enum[_]): Boolean = term == qualifyEnum(enum)

  def getEnum(genExpr: GeneratedExpression): Enum[_] = {
    val split = genExpr.resultTerm.split('.')
    val value = split.last
    val clazz = genExpr.resultType.getTypeClass
    enumValueOf(clazz, value)
  }

  def enumValueOf[T <: Enum[T]](cls: Class[_], stringValue: String): Enum[_] =
    Enum.valueOf(cls.asInstanceOf[Class[T]], stringValue).asInstanceOf[Enum[_]]

  // ----------------------------------------------------------------------------------------------

  def requireNumeric(genExpr: GeneratedExpression): Unit =
    if (!TypeCheckUtils.isNumeric(genExpr.resultType)) {
      throw new CodeGenException("Numeric expression type expected, but was " +
        s"'${genExpr.resultType}'.")
    }

  def requireComparable(genExpr: GeneratedExpression): Unit =
    if (!TypeCheckUtils.isComparable(genExpr.resultType)) {
      throw new CodeGenException(s"Comparable type expected, but was '${genExpr.resultType}'.")
    }

  def requireString(genExpr: GeneratedExpression): Unit =
    if (!TypeCheckUtils.isString(genExpr.resultType)) {
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

  def requireInteger(genExpr: GeneratedExpression): Unit =
    if (!TypeCheckUtils.isInteger(genExpr.resultType)) {
      throw new CodeGenException("Integer expression type expected.")
    }

  def generateNullLiteral(
      resultType: TypeInformation[_],
      nullCheck: Boolean): GeneratedExpression = {
    val resultTerm = newName("result")
    val nullTerm = newName("isNull")
    val resultTypeTerm = primitiveTypeTermForTypeInfo(resultType)
    val defaultValue = primitiveDefaultValue(resultType)

    if (nullCheck) {
      val wrappedCode =
        s"""
           |$resultTypeTerm $resultTerm = $defaultValue;
           |boolean $nullTerm = true;
           |""".stripMargin
      GeneratedExpression(resultTerm, nullTerm, wrappedCode, resultType, literal = true)
    } else {
      throw new CodeGenException("Null literals are not allowed if nullCheck is disabled.")
    }
  }

  def generateNonNullLiteral(
      literalType: TypeInformation[_],
      literalCode: String,
      nullCheck: Boolean): GeneratedExpression = {
    val resultTerm = newName("result")
    val nullTerm = newName("isNull")
    val resultTypeTerm = primitiveTypeTermForTypeInfo(literalType)
    val resultCode = if (nullCheck) {
      s"""
         |$resultTypeTerm $resultTerm = $literalCode;
         |boolean $nullTerm = false;
         |""".stripMargin
    } else {
      s"""
         |$resultTypeTerm $resultTerm = $literalCode;
         |""".stripMargin
    }

    GeneratedExpression(resultTerm, nullTerm, resultCode, literalType, literal = true)
  }

  def generateSymbol(enum: Enum[_]): GeneratedExpression =
    GeneratedExpression(qualifyEnum(enum), "false", "", new GenericTypeInfo(enum.getDeclaringClass))

  def generateProctimeTimestamp(contextTerm: String): GeneratedExpression = {
    val resultTerm = newName("result")
    val resultCode =
      s"""
         |long $resultTerm = $contextTerm.timerService().currentProcessingTime();
         |""".stripMargin
    GeneratedExpression(resultTerm, NEVER_NULL, resultCode, SqlTimeTypeInfo.TIMESTAMP)
  }

  def generateStreamRecordRowtimeAccess(contextTerm: String): GeneratedExpression = {
    val resultTerm = newName("result")
    val nullTerm = newName("isNull")

    val accessCode =
      s"""
         |Long $resultTerm = $contextTerm.timestamp();
         |if ($resultTerm == null) {
         |  throw new RuntimeException("Rowtime timestamp is null. Please make sure that a " +
         |    "proper TimestampAssigner is defined and the stream environment uses the EventTime " +
         |    "time characteristic.");
         |}
         |boolean $nullTerm = false;
       """.stripMargin

    GeneratedExpression(resultTerm, nullTerm, accessCode, Types.LONG)
  }

  def generateCurrentTimestamp(
      ctx: CodeGeneratorContext,
      nullCheck: Boolean): GeneratedExpression = {
    new CurrentTimePointCallGen(Types.SQL_TIMESTAMP, false).generate(ctx, Seq(), nullCheck)
  }

  def generateInputAccess(
      ctx: CodeGeneratorContext,
      inputType: TypeInformation[_ <: Any],
      inputTerm: String,
      index: Int,
      nullableInput: Boolean,
      nullCheck: Boolean): GeneratedExpression = {
    // if input has been used before, we can reuse the code that
    // has already been generated
    val inputExpr = ctx.getReusableInputUnboxingExprs(inputTerm, index) match {
      // input access and unboxing has already been generated
      case Some(expr) => expr

      // generate input access and unboxing if necessary
      case None =>
        val expr = if (nullableInput) {
          generateNullableInputFieldAccess(ctx, inputType, inputTerm, index, nullCheck)
        } else {
          generateFieldAccess(ctx, inputType, inputTerm, index, nullCheck)
        }

        ctx.addReusableInputUnboxingExprs(inputTerm, index, expr)
        expr
    }
    // hide the generated code as it will be executed only once
    GeneratedExpression(inputExpr.resultTerm, inputExpr.nullTerm, "", inputExpr.resultType)
  }

  def generateFieldAccess(
      ctx: CodeGeneratorContext,
      inputType: TypeInformation[_],
      inputTerm: String,
      index: Int,
      nullCheck: Boolean): GeneratedExpression = {
    inputType match {
      case ct: CompositeType[_] =>
        val accessor = fieldAccessorFor(ct, index)
        val fieldType: TypeInformation[Any] = ct.getTypeAt(index)
        val fieldTypeTerm = boxedTypeTermForTypeInfo(fieldType)

        accessor match {
          case ObjectFieldAccessor(field) =>
            // primitive
            if (isFieldPrimitive(field)) {
              generateNonNullLiteral(fieldType, s"$inputTerm.${field.getName}", nullCheck)
            }
            // Object
            else {
              generateInputFieldUnboxing(
                fieldType,
                s"($fieldTypeTerm) $inputTerm.${field.getName}",
                nullCheck)
            }

          case ObjectGenericFieldAccessor(fieldName) =>
            // Object
            val inputCode = s"($fieldTypeTerm) $inputTerm.$fieldName"
            generateInputFieldUnboxing(fieldType, inputCode, nullCheck)

          case ObjectMethodAccessor(methodName) =>
            // Object
            val inputCode = s"($fieldTypeTerm) $inputTerm.$methodName()"
            generateInputFieldUnboxing(fieldType, inputCode, nullCheck)

          case ProductAccessor(i) =>
            // Object
            val inputCode = s"($fieldTypeTerm) $inputTerm.getField($i)"
            generateInputFieldUnboxing(fieldType, inputCode, nullCheck)

          case ObjectPrivateFieldAccessor(field) =>
            val fieldTerm = ctx.addReusablePrivateFieldAccess(ct.getTypeClass, field.getName)
            val reflectiveAccessCode = reflectiveFieldReadAccess(fieldTerm, field, inputTerm)
            // primitive
            if (isFieldPrimitive(field)) {
              generateNonNullLiteral(fieldType, reflectiveAccessCode, nullCheck)
            }
            // Object
            else {
              generateInputFieldUnboxing(fieldType, reflectiveAccessCode, nullCheck)
            }
        }

      case at: AtomicType[_] =>
        val fieldTypeTerm = boxedTypeTermForTypeInfo(at)
        val inputCode = s"($fieldTypeTerm) $inputTerm"
        generateInputFieldUnboxing(at, inputCode, nullCheck)

      case _ =>
        throw new CodeGenException("Unsupported type for input field access.")
    }
  }

  def generateNullableInputFieldAccess(
      ctx: CodeGeneratorContext,
      inputType: TypeInformation[_ <: Any],
      inputTerm: String,
      index: Int,
      nullCheck: Boolean): GeneratedExpression = {
    val resultTerm = newName("result")
    val nullTerm = newName("isNull")

    val fieldType = inputType match {
      case ct: CompositeType[_] => ct.getTypeAt(index)
      case at: AtomicType[_] => at
      case _ => throw new CodeGenException("Unsupported type for input field access.")
    }
    val resultTypeTerm = primitiveTypeTermForTypeInfo(fieldType)
    val defaultValue = primitiveDefaultValue(fieldType)
    val fieldAccessExpr = generateFieldAccess(ctx, inputType, inputTerm, index, nullCheck)

    val inputCheckCode =
      s"""
         |$resultTypeTerm $resultTerm;
         |boolean $nullTerm;
         |if ($inputTerm == null) {
         |  $resultTerm = $defaultValue;
         |  $nullTerm = true;
         |}
         |else {
         |  ${fieldAccessExpr.code}
         |  $resultTerm = ${fieldAccessExpr.resultTerm};
         |  $nullTerm = ${fieldAccessExpr.nullTerm};
         |}
         |""".stripMargin

    GeneratedExpression(resultTerm, nullTerm, inputCheckCode, fieldType)
  }

  /**
   * Converts the external boxed format to an internal mostly primitive field representation.
   * Wrapper types can autoboxed to their corresponding primitive type (Integer -> int). External
   * objects are converted to their internal representation (Timestamp -> internal timestamp
   * in long).
   *
   * @param fieldType type of field
   * @param fieldTerm expression term of field to be unboxed
   * @param nullCheck whether to check null
   * @return internal unboxed field representation
   */
  def generateInputFieldUnboxing(
      fieldType: TypeInformation[_],
      fieldTerm: String,
      nullCheck: Boolean): GeneratedExpression = {
    val resultTerm = newName("result")
    val nullTerm = newName("isNull")
    val resultTypeTerm = primitiveTypeTermForTypeInfo(fieldType)
    val defaultValue = primitiveDefaultValue(fieldType)

    // explicit unboxing
    val unboxedFieldCode = if (isTimePoint(fieldType)) {
      timePointToInternalCode(fieldType, fieldTerm)
    } else {
      fieldTerm
    }

    val wrappedCode = if (nullCheck && !isReference(fieldType)) {
      // assumes that fieldType is a boxed primitive.
      s"""
         |boolean $nullTerm = $fieldTerm == null;
         |$resultTypeTerm $resultTerm;
         |if ($nullTerm) {
         |  $resultTerm = $defaultValue;
         |}
         |else {
         |  $resultTerm = $fieldTerm;
         |}
         |""".stripMargin
    } else if (nullCheck) {
      s"""
         |boolean $nullTerm = $fieldTerm == null;
         |$resultTypeTerm $resultTerm;
         |if ($nullTerm) {
         |  $resultTerm = $defaultValue;
         |}
         |else {
         |  $resultTerm = $unboxedFieldCode;
         |}
         |""".stripMargin
    } else {
      s"""
         |$resultTypeTerm $resultTerm = $unboxedFieldCode;
         |""".stripMargin
    }

    GeneratedExpression(resultTerm, nullTerm, wrappedCode, fieldType)
  }

  /**
   * Converts the internal mostly primitive field representation to an external boxed format.
   * Primitive types can autoboxed to their corresponding object type (int -> Integer). Internal
   * representations are converted to their external objects (internal timestamp
   * in long -> Timestamp).
   *
   * @param expr expression to be boxed
   * @param nullCheck whether to check null
   * @return external boxed field representation
   */
  def generateOutputFieldBoxing(
      expr: GeneratedExpression,
      nullCheck: Boolean): GeneratedExpression = {
    expr.resultType match {
      // convert internal date/time/timestamp to java.sql.* objects
      case SqlTimeTypeInfo.DATE | SqlTimeTypeInfo.TIME | SqlTimeTypeInfo.TIMESTAMP =>
        val resultTerm = newName("result")
        val resultTypeTerm = boxedTypeTermForTypeInfo(expr.resultType)
        val convMethod = internalToTimePointCode(expr.resultType, expr.resultTerm)

        val resultCode = if (nullCheck) {
          s"""
             |${expr.code}
             |$resultTypeTerm $resultTerm;
             |if (${expr.nullTerm}) {
             |  $resultTerm = null;
             |}
             |else {
             |  $resultTerm = $convMethod;
             |}
             |""".stripMargin
        } else {
          s"""
             |${expr.code}
             |$resultTypeTerm $resultTerm = $convMethod;
             |""".stripMargin
        }

        GeneratedExpression(resultTerm, expr.nullTerm, resultCode, expr.resultType)

      // other types are autoboxed or need no boxing
      case _ => expr
    }
  }

  def generateCallExpression(
      ctx: CodeGeneratorContext,
      operator: SqlOperator,
      operands: Seq[GeneratedExpression],
      resultType: TypeInformation[_],
      nullCheck: Boolean,
      contextTerm: String): GeneratedExpression = {
    operator match {
      // arithmetic
      case PLUS if isNumeric(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumeric(left)
        requireNumeric(right)
        generateArithmeticOperator("+", nullCheck, resultType, left, right)

      case PLUS | DATETIME_PLUS if isTemporal(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireTemporal(left)
        requireTemporal(right)
        generateTemporalPlusMinus(plus = true, nullCheck, left, right)

      case MINUS if isNumeric(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumeric(left)
        requireNumeric(right)
        generateArithmeticOperator("-", nullCheck, resultType, left, right)

      case MINUS | MINUS_DATE if isTemporal(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireTemporal(left)
        requireTemporal(right)
        generateTemporalPlusMinus(plus = false, nullCheck, left, right)

      case MULTIPLY if isNumeric(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumeric(left)
        requireNumeric(right)
        generateArithmeticOperator("*", nullCheck, resultType, left, right)

      case MULTIPLY if isTimeInterval(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireTimeInterval(left)
        requireNumeric(right)
        generateArithmeticOperator("*", nullCheck, resultType, left, right)

      case DIVIDE | DIVIDE_INTEGER if isNumeric(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumeric(left)
        requireNumeric(right)
        generateArithmeticOperator("/", nullCheck, resultType, left, right)

      case MOD if isNumeric(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumeric(left)
        requireNumeric(right)
        generateArithmeticOperator("%", nullCheck, resultType, left, right)

      case UNARY_MINUS if isNumeric(resultType) =>
        val operand = operands.head
        requireNumeric(operand)
        generateUnaryArithmeticOperator("-", nullCheck, resultType, operand)

      case UNARY_MINUS if isTimeInterval(resultType) =>
        val operand = operands.head
        requireTimeInterval(operand)
        generateUnaryIntervalPlusMinus(plus = false, nullCheck, operand)

      case UNARY_PLUS if isNumeric(resultType) =>
        val operand = operands.head
        requireNumeric(operand)
        generateUnaryArithmeticOperator("+", nullCheck, resultType, operand)

      case UNARY_PLUS if isTimeInterval(resultType) =>
        val operand = operands.head
        requireTimeInterval(operand)
        generateUnaryIntervalPlusMinus(plus = true, nullCheck, operand)

      // comparison
      case EQUALS =>
        val left = operands.head
        val right = operands(1)
        generateEquals(nullCheck, left, right)

      case NOT_EQUALS =>
        val left = operands.head
        val right = operands(1)
        generateNotEquals(nullCheck, left, right)

      case GREATER_THAN =>
        val left = operands.head
        val right = operands(1)
        requireComparable(left)
        requireComparable(right)
        generateComparison(">", nullCheck, left, right)

      case GREATER_THAN_OR_EQUAL =>
        val left = operands.head
        val right = operands(1)
        requireComparable(left)
        requireComparable(right)
        generateComparison(">=", nullCheck, left, right)

      case LESS_THAN =>
        val left = operands.head
        val right = operands(1)
        requireComparable(left)
        requireComparable(right)
        generateComparison("<", nullCheck, left, right)

      case LESS_THAN_OR_EQUAL =>
        val left = operands.head
        val right = operands(1)
        requireComparable(left)
        requireComparable(right)
        generateComparison("<=", nullCheck, left, right)

      case IS_NULL =>
        val operand = operands.head
        generateIsNull(nullCheck, operand)

      case IS_NOT_NULL =>
        val operand = operands.head
        generateIsNotNull(nullCheck, operand)

      // logic
      case AND =>
        operands.reduceLeft { (left: GeneratedExpression, right: GeneratedExpression) =>
          requireBoolean(left)
          requireBoolean(right)
          generateAnd(nullCheck, left, right)
        }

      case OR =>
        operands.reduceLeft { (left: GeneratedExpression, right: GeneratedExpression) =>
          requireBoolean(left)
          requireBoolean(right)
          generateOr(nullCheck, left, right)
        }

      case NOT =>
        val operand = operands.head
        requireBoolean(operand)
        generateNot(nullCheck, operand)

      case CASE =>
        generateIfElse(nullCheck, operands, resultType)

      case IS_TRUE =>
        val operand = operands.head
        requireBoolean(operand)
        generateIsTrue(operand)

      case IS_NOT_TRUE =>
        val operand = operands.head
        requireBoolean(operand)
        generateIsNotTrue(operand)

      case IS_FALSE =>
        val operand = operands.head
        requireBoolean(operand)
        generateIsFalse(operand)

      case IS_NOT_FALSE =>
        val operand = operands.head
        requireBoolean(operand)
        generateIsNotFalse(operand)

      case IN =>
        val left = operands.head
        val right = operands.tail
        generateIn(ctx, left, right, nullCheck)

      // casting
      case CAST | REINTERPRET =>
        val operand = operands.head
        generateCast(nullCheck, operand, resultType)

      // as / renaming
      case AS =>
        operands.head

      // string arithmetic
      case CONCAT =>
        val left = operands.head
        val right = operands(1)
        requireString(left)
        generateArithmeticOperator("+", nullCheck, resultType, left, right)

      // arrays
      case ARRAY_VALUE_CONSTRUCTOR =>
        generateArray(ctx, resultType, operands, nullCheck)

      case ITEM =>
        operands.head.resultType match {
          case _: ObjectArrayTypeInfo[_, _] |
               _: BasicArrayTypeInfo[_, _] |
               _: PrimitiveArrayTypeInfo[_] =>
            val array = operands.head
            val index = operands(1)
            requireInteger(index)
            generateArrayElementAt(array, index, nullCheck)

          case _: MapTypeInfo[_, _] =>
            val key = operands(1)
            generateMapGet(operands.head, key, nullCheck)

          case _ => throw new CodeGenException("Expect an array or a map.")
        }

      case CARDINALITY =>
        val array = operands.head
        requireArray(array)
        generateArrayCardinality(nullCheck, array)

      case ELEMENT =>
        val array = operands.head
        requireArray(array)
        generateArrayElement(array, nullCheck)

      case ScalarSqlFunctions.CONCAT =>
        generateConcat(nullCheck = true, operands)

      case ScalarSqlFunctions.CONCAT_WS =>
        generateConcatWs(operands)

      case StreamRecordTimestampSqlFunction =>
        generateStreamRecordRowtimeAccess(contextTerm)

      // advanced scalar functions
      case sqlOperator: SqlOperator =>
        val callGen = FunctionGenerator.getCallGenerator(
          sqlOperator,
          operands.map(_.resultType),
          resultType)
        callGen.getOrElse(
          throw new CodeGenException(s"Unsupported call: $sqlOperator \n" +
              s"If you think this function should be supported, " +
              s"you can create an issue and start a discussion for it."))
            .generate(ctx, operands, nullCheck)

      // unknown or invalid
      case call@_ =>
        throw new CodeGenException(s"Unsupported call: $call")
    }
  }

  // ----------------------------------------------------------------------------------------------

  def isReference(genExpr: GeneratedExpression): Boolean = isReference(genExpr.resultType)

  def isReference(typeInfo: TypeInformation[_]): Boolean = typeInfo match {
    case INT_TYPE_INFO
         | LONG_TYPE_INFO
         | SHORT_TYPE_INFO
         | BYTE_TYPE_INFO
         | FLOAT_TYPE_INFO
         | DOUBLE_TYPE_INFO
         | BOOLEAN_TYPE_INFO
         | CHAR_TYPE_INFO => false
    case _ => true
  }

  // ----------------------------------------------------------------------------------------------

  sealed abstract class FieldAccessor

  case class ObjectFieldAccessor(field: Field) extends FieldAccessor

  case class ObjectGenericFieldAccessor(name: String) extends FieldAccessor

  case class ObjectPrivateFieldAccessor(field: Field) extends FieldAccessor

  case class ObjectMethodAccessor(methodName: String) extends FieldAccessor

  case class ProductAccessor(i: Int) extends FieldAccessor

  def fieldAccessorFor(compType: CompositeType[_], index: Int): FieldAccessor = {
    compType match {
      case ri: RowTypeInfo =>
        ProductAccessor(index)

      case cc: CaseClassTypeInfo[_] =>
        ObjectMethodAccessor(cc.getFieldNames()(index))

      case javaTup: TupleTypeInfo[_] =>
        ObjectGenericFieldAccessor("f" + index)

      case pt: PojoTypeInfo[_] =>
        val fieldName = pt.getFieldNames()(index)
        getFieldAccessor(pt.getTypeClass, fieldName)

      case _ => throw new CodeGenException(s"Unsupported composite type: '$compType'")
    }
  }

  def getFieldAccessor(clazz: Class[_], fieldName: String): FieldAccessor = {
    val field = TypeExtractor.getDeclaredField(clazz, fieldName)
    if (field.isAccessible) {
      ObjectFieldAccessor(field)
    }
    else {
      ObjectPrivateFieldAccessor(field)
    }
  }

  def isFieldPrimitive(field: Field): Boolean = field.getType.isPrimitive

  def reflectiveFieldReadAccess(fieldTerm: String, field: Field, objectTerm: String): String =
    field.getType match {
      case java.lang.Integer.TYPE => s"$fieldTerm.getInt($objectTerm)"
      case java.lang.Long.TYPE => s"$fieldTerm.getLong($objectTerm)"
      case java.lang.Short.TYPE => s"$fieldTerm.getShort($objectTerm)"
      case java.lang.Byte.TYPE => s"$fieldTerm.getByte($objectTerm)"
      case java.lang.Float.TYPE => s"$fieldTerm.getFloat($objectTerm)"
      case java.lang.Double.TYPE => s"$fieldTerm.getDouble($objectTerm)"
      case java.lang.Boolean.TYPE => s"$fieldTerm.getBoolean($objectTerm)"
      case java.lang.Character.TYPE => s"$fieldTerm.getChar($objectTerm)"
      case _ => s"(${field.getType.getCanonicalName}) $fieldTerm.get($objectTerm)"
    }

  def reflectiveFieldWriteAccess(
      fieldTerm: String,
      field: Field,
      objectTerm: String,
      valueTerm: String)
    : String =
    field.getType match {
      case java.lang.Integer.TYPE => s"$fieldTerm.setInt($objectTerm, $valueTerm)"
      case java.lang.Long.TYPE => s"$fieldTerm.setLong($objectTerm, $valueTerm)"
      case java.lang.Short.TYPE => s"$fieldTerm.setShort($objectTerm, $valueTerm)"
      case java.lang.Byte.TYPE => s"$fieldTerm.setByte($objectTerm, $valueTerm)"
      case java.lang.Float.TYPE => s"$fieldTerm.setFloat($objectTerm, $valueTerm)"
      case java.lang.Double.TYPE => s"$fieldTerm.setDouble($objectTerm, $valueTerm)"
      case java.lang.Boolean.TYPE => s"$fieldTerm.setBoolean($objectTerm, $valueTerm)"
      case java.lang.Character.TYPE => s"$fieldTerm.setChar($objectTerm, $valueTerm)"
      case _ => s"$fieldTerm.set($objectTerm, $valueTerm)"
    }
}
