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

import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.`type`.{ReturnTypes, SqlTypeName}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.`type`._
import org.apache.flink.table.api.TableException
import org.apache.flink.table.calcite.{FlinkTypeFactory, RexAggLocalVariable, RexDistinctKeyVariable}
import org.apache.flink.table.codegen.CodeGenUtils.{requireTemporal, requireTimeInterval, _}
import org.apache.flink.table.codegen.GenerateUtils._
import org.apache.flink.table.codegen.GeneratedExpression.{NEVER_NULL, NO_CODE}
import org.apache.flink.table.codegen.calls.ScalarOperatorGens._
import org.apache.flink.table.dataformat._
import org.apache.flink.table.functions.sql.FlinkSqlOperatorTable._
import org.apache.flink.table.typeutils.TypeCheckUtils.{isNumeric, isTemporal, isTimeInterval}
import org.apache.flink.table.typeutils.{TimeIndicatorTypeInfo, TypeCheckUtils}

import scala.collection.JavaConversions._

/**
  * This code generator is mainly responsible for generating codes for a given calcite [[RexNode]].
  * It can also generate type conversion codes for the result converter.
  */
class ExprCodeGenerator(ctx: CodeGeneratorContext, nullableInput: Boolean)
  extends RexVisitor[GeneratedExpression] {

  // check if nullCheck is enabled when inputs can be null
  if (nullableInput && !ctx.nullCheck) {
    throw new CodeGenException("Null check must be enabled if entire rows can be null.")
  }

  /**
    * term of the [[ProcessFunction]]'s context, can be changed when needed
    */
  var contextTerm = "ctx"

  /**
    * information of the first input
    */
  var input1Type: InternalType = _
  var input1Term: String = _
  var input1FieldMapping: Option[Array[Int]] = None

  /**
    * information of the optional second input
    */
  var input2Type: Option[InternalType] = None
  var input2Term: Option[String] = None
  var input2FieldMapping: Option[Array[Int]] = None

  /**
    * Bind the input information, should be called before generating expression.
    */
  def bindInput(
      inputType: InternalType,
      inputTerm: String = DEFAULT_INPUT1_TERM,
      inputFieldMapping: Option[Array[Int]] = None): ExprCodeGenerator = {
    input1Type = inputType
    input1Term = inputTerm
    input1FieldMapping = inputFieldMapping
    this
  }

  /**
    * In some cases, the expression will have two inputs (e.g. join condition and udtf). We should
    * bind second input information before use.
    */
  def bindSecondInput(
      inputType: InternalType,
      inputTerm: String = DEFAULT_INPUT2_TERM,
      inputFieldMapping: Option[Array[Int]] = None): ExprCodeGenerator = {
    input2Type = Some(inputType)
    input2Term = Some(inputTerm)
    input2FieldMapping = inputFieldMapping
    this
  }

  private lazy val input1Mapping: Array[Int] = input1FieldMapping match {
    case Some(mapping) => mapping
    case _ => fieldIndices(input1Type)
  }

  private lazy val input2Mapping: Array[Int] = input2FieldMapping match {
    case Some(mapping) => mapping
    case _ => input2Type match {
      case Some(input) => fieldIndices(input)
      case _ => Array[Int]()
    }
  }
  
  private def fieldIndices(t: InternalType): Array[Int] = t match {
    case rt: RowType => (0 until rt.getArity).toArray
    case _ => Array(0)
  }
 
  /**
    * Generates an expression from a RexNode. If objects or variables can be reused, they will be
    * added to reusable code sections internally.
    *
    * @param rex Calcite row expression
    * @return instance of GeneratedExpression
    */
  def generateExpression(rex: RexNode): GeneratedExpression = {
    rex.accept(this)
  }

  /**
    * Generates an expression that converts the first input (and second input) into the given type.
    * If two inputs are converted, the second input is appended. If objects or variables can
    * be reused, they will be added to reusable code sections internally. The evaluation result
    * will be stored in the variable outRecordTerm.
    *
    * @param returnType conversion target type. Inputs and output must have the same arity.
    * @param outRecordTerm the result term
    * @param outRecordWriterTerm the result writer term
    * @param reusedOutRow If objects or variables can be reused, they will be added to reusable
    * code sections internally.
    * @return instance of GeneratedExpression
    */
  def generateConverterResultExpression(
      returnType: RowType,
      returnTypeClazz: Class[_ <: BaseRow],
      outRecordTerm: String = DEFAULT_OUT_RECORD_TERM,
      outRecordWriterTerm: String = DEFAULT_OUT_RECORD_WRITER_TERM,
      reusedOutRow: Boolean = true,
      fieldCopy: Boolean = false,
      rowtimeExpression: Option[RexNode] = None)
    : GeneratedExpression = {
    val input1AccessExprs = input1Mapping.map {
      case TimeIndicatorTypeInfo.ROWTIME_STREAM_MARKER |
           TimeIndicatorTypeInfo.ROWTIME_BATCH_MARKER if rowtimeExpression.isDefined =>
        // generate rowtime attribute from expression
        generateExpression(rowtimeExpression.get)
      case TimeIndicatorTypeInfo.ROWTIME_STREAM_MARKER |
           TimeIndicatorTypeInfo.ROWTIME_BATCH_MARKER =>
        throw new TableException("Rowtime extraction expression missing. Please report a bug.")
      case TimeIndicatorTypeInfo.PROCTIME_STREAM_MARKER =>
        // attribute is proctime indicator.
        // we use a null literal and generate a timestamp when we need it.
        generateNullLiteral(InternalTypes.PROCTIME_INDICATOR, ctx.nullCheck)
      case TimeIndicatorTypeInfo.PROCTIME_BATCH_MARKER =>
        // attribute is proctime field in a batch query.
        // it is initialized with the current time.
        generateCurrentTimestamp(ctx)
      case idx =>
        // get type of result field
        generateInputAccess(
          ctx,
          input1Type,
          input1Term,
          idx,
          nullableInput,
          fieldCopy)
    }

    val input2AccessExprs = input2Type match {
      case Some(ti) =>
        input2Mapping.map(idx => generateInputAccess(
          ctx,
          ti,
          input2Term.get,
          idx,
          nullableInput,
          ctx.nullCheck)
        ).toSeq
      case None => Seq() // add nothing
    }

    generateResultExpression(
      input1AccessExprs ++ input2AccessExprs,
      returnType,
      returnTypeClazz,
      outRow = outRecordTerm,
      outRowWriter = Some(outRecordWriterTerm),
      reusedOutRow = reusedOutRow)
  }

  /**
    * Generates an expression from a sequence of other expressions. The evaluation result
    * may be stored in the variable outRecordTerm.
    *
    * @param fieldExprs field expressions to be converted
    * @param returnType conversion target type. Type must have the same arity than fieldExprs.
    * @param outRow the result term
    * @param outRowWriter the result writer term for BinaryRow.
    * @param reusedOutRow If objects or variables can be reused, they will be added to reusable
    *                     code sections internally.
    * @param outRowAlreadyExists Don't need addReusableRecord if out row already exists.
    * @return instance of GeneratedExpression
    */
  def generateResultExpression(
      fieldExprs: Seq[GeneratedExpression],
      returnType: RowType,
      returnTypeClazz: Class[_ <: BaseRow],
      outRow: String = DEFAULT_OUT_RECORD_TERM,
      outRowWriter: Option[String] = Some(DEFAULT_OUT_RECORD_WRITER_TERM),
      reusedOutRow: Boolean = true,
      outRowAlreadyExists: Boolean = false): GeneratedExpression = {
    val fieldExprIdxToOutputRowPosMap = fieldExprs.indices.map(i => i -> i).toMap
    generateResultExpression(fieldExprs, fieldExprIdxToOutputRowPosMap, returnType,
      returnTypeClazz, outRow, outRowWriter, reusedOutRow, outRowAlreadyExists)
  }

  /**
    * Generates an expression from a sequence of other expressions. The evaluation result
    * may be stored in the variable outRecordTerm.
    *
    * @param fieldExprs field expressions to be converted
    * @param fieldExprIdxToOutputRowPosMap Mapping index of fieldExpr in `fieldExprs`
    *                                      to position of output row.
    * @param returnType conversion target type. Type must have the same arity than fieldExprs.
    * @param outRow the result term
    * @param outRowWriter the result writer term for BinaryRow.
    * @param reusedOutRow If objects or variables can be reused, they will be added to reusable
    *                     code sections internally.
    * @param outRowAlreadyExists Don't need addReusableRecord if out row already exists.
    * @return instance of GeneratedExpression
    */
  def generateResultExpression(
    fieldExprs: Seq[GeneratedExpression],
    fieldExprIdxToOutputRowPosMap: Map[Int, Int],
    returnType: RowType,
    returnTypeClazz: Class[_ <: BaseRow],
    outRow: String,
    outRowWriter: Option[String],
    reusedOutRow: Boolean,
    outRowAlreadyExists: Boolean)
  : GeneratedExpression = {
    // initial type check
    if (returnType.getArity != fieldExprs.length) {
      throw new CodeGenException(
        s"Arity [${returnType.getArity}] of result type [$returnType] does not match " +
          s"number [${fieldExprs.length}] of expressions [$fieldExprs].")
    }
    if (fieldExprIdxToOutputRowPosMap.size != fieldExprs.length) {
      throw new CodeGenException(
        s"Size [${returnType.getArity}] of fieldExprIdxToOutputRowPosMap does not match " +
          s"number [${fieldExprs.length}] of expressions [$fieldExprs].")
    }
    // type check
    fieldExprs.zipWithIndex foreach {
      // timestamp type(Include TimeIndicator) and generic type can compatible with each other.
      case (fieldExpr, i)
        if fieldExpr.resultType.isInstanceOf[GenericType[_]] ||
          fieldExpr.resultType.isInstanceOf[TimestampType] =>
        if (returnType.getTypeAt(i).getClass != fieldExpr.resultType.getClass
          && !returnType.getTypeAt(i).isInstanceOf[GenericType[_]]) {
          throw new CodeGenException(
            s"Incompatible types of expression and result type, Expression[$fieldExpr] type is " +
              s"[${fieldExpr.resultType}], result type is [${returnType.getTypeAt(i)}]")
        }
      case (fieldExpr, i) if fieldExpr.resultType != returnType.getTypeAt(i) =>
        throw new CodeGenException(
          s"Incompatible types of expression and result type. Expression[$fieldExpr] type is " +
            s"[${fieldExpr.resultType}], result type is [${returnType.getTypeAt(i)}]")
      case _ => // ok
    }

    val setFieldsCode = fieldExprs.zipWithIndex.map { case (fieldExpr, index) =>
      val pos = fieldExprIdxToOutputRowPosMap.getOrElse(index,
        throw new CodeGenException(s"Illegal field expr index: $index"))
      baseRowSetField(ctx, returnTypeClazz, outRow, pos.toString, fieldExpr, outRowWriter)
    }.mkString("\n")

    val outRowInitCode = if (!outRowAlreadyExists) {
      val initCode = generateRecordStatement(returnType, returnTypeClazz, outRow, outRowWriter)
      if (reusedOutRow) {
        ctx.addReusableMember(initCode)
        NO_CODE
      } else {
        initCode
      }
    } else {
      NO_CODE
    }

    val code = if (returnTypeClazz == classOf[BinaryRow] && outRowWriter.isDefined) {
      val writer = outRowWriter.get
      val resetWriter = if (ctx.nullCheck) s"$writer.reset();" else s"$writer.resetCursor();"
      val completeWriter: String = s"$writer.complete();"
      s"""
         |$outRowInitCode
         |$resetWriter
         |$setFieldsCode
         |$completeWriter
        """.stripMargin
    } else {
      s"""
         |$outRowInitCode
         |$setFieldsCode
        """.stripMargin
    }
    GeneratedExpression(outRow, NEVER_NULL, code, returnType)
  }

  override def visitInputRef(inputRef: RexInputRef): GeneratedExpression = {
    // if inputRef index is within size of input1 we work with input1, input2 otherwise
    val input = if (inputRef.getIndex < InternalTypeUtils.getArity(input1Type)) {
      (input1Type, input1Term)
    } else {
      (input2Type.getOrElse(throw new CodeGenException("Invalid input access.")),
        input2Term.getOrElse(throw new CodeGenException("Invalid input access.")))
    }

    val index = if (input._2 == input1Term) {
      inputRef.getIndex
    } else {
      inputRef.getIndex - InternalTypeUtils.getArity(input1Type)
    }

    generateInputAccess(ctx, input._1, input._2, index, nullableInput, ctx.nullCheck)
  }

  override def visitTableInputRef(rexTableInputRef: RexTableInputRef): GeneratedExpression =
    visitInputRef(rexTableInputRef)

  override def visitFieldAccess(rexFieldAccess: RexFieldAccess): GeneratedExpression = {
    val refExpr = rexFieldAccess.getReferenceExpr.accept(this)
    val index = rexFieldAccess.getField.getIndex
    val fieldAccessExpr = generateFieldAccess(
      ctx,
      refExpr.resultType,
      refExpr.resultTerm,
      index)

    val resultTypeTerm = primitiveTypeTermForType(fieldAccessExpr.resultType)
    val defaultValue = primitiveDefaultValue(fieldAccessExpr.resultType)
    val Seq(resultTerm, nullTerm) = ctx.addReusableLocalVariables(
      (resultTypeTerm, "result"),
      ("boolean", "isNull"))

    val resultCode = if (ctx.nullCheck) {
      s"""
         |${refExpr.code}
         |if (${refExpr.nullTerm}) {
         |  $resultTerm = $defaultValue;
         |  $nullTerm = true;
         |}
         |else {
         |  ${fieldAccessExpr.code}
         |  $resultTerm = ${fieldAccessExpr.resultTerm};
         |  $nullTerm = ${fieldAccessExpr.nullTerm};
         |}
         |""".stripMargin
    } else {
      s"""
         |${refExpr.code}
         |${fieldAccessExpr.code}
         |$resultTerm = ${fieldAccessExpr.resultTerm};
         |""".stripMargin
    }

    GeneratedExpression(resultTerm, nullTerm, resultCode, fieldAccessExpr.resultType)
  }

  override def visitLiteral(literal: RexLiteral): GeneratedExpression = {
    val resultType = FlinkTypeFactory.toInternalType(literal.getType)
    val value = literal.getValue3
    generateLiteral(ctx, resultType, value)
  }

  override def visitCorrelVariable(correlVariable: RexCorrelVariable): GeneratedExpression = {
    GeneratedExpression(input1Term, NEVER_NULL, NO_CODE, input1Type)
  }

  override def visitLocalRef(localRef: RexLocalRef): GeneratedExpression = localRef match {
    case local: RexAggLocalVariable =>
      GeneratedExpression(local.fieldTerm, local.nullTerm, NO_CODE, local.internalType)
    case value: RexDistinctKeyVariable =>
      val inputExpr = ctx.getReusableInputUnboxingExprs(input1Term, 0) match {
        case Some(expr) => expr
        case None =>
          val pType = primitiveTypeTermForType(value.internalType)
          val defaultValue = primitiveDefaultValue(value.internalType)
          val resultTerm = newName("field")
          val nullTerm = newName("isNull")
          val code =
            s"""
               |$pType $resultTerm = $defaultValue;
               |boolean $nullTerm = true;
               |if ($input1Term != null) {
               |  $nullTerm = false;
               |  $resultTerm = ($pType) $input1Term;
               |}
            """.stripMargin
          val expr = GeneratedExpression(resultTerm, nullTerm, code, value.internalType)
          ctx.addReusableInputUnboxingExprs(input1Term, 0, expr)
          expr
      }
      // hide the generated code as it will be executed only once
      GeneratedExpression(inputExpr.resultTerm, inputExpr.nullTerm, NO_CODE, inputExpr.resultType)
    case _ => throw new CodeGenException("Local variables are not supported yet.")
  }

  override def visitRangeRef(rangeRef: RexRangeRef): GeneratedExpression =
    throw new CodeGenException("Range references are not supported yet.")

  override def visitDynamicParam(dynamicParam: RexDynamicParam): GeneratedExpression =
    throw new CodeGenException("Dynamic parameter references are not supported yet.")

  override def visitCall(call: RexCall): GeneratedExpression = {

    val resultType = FlinkTypeFactory.toInternalType(call.getType)

    // convert operands and help giving untyped NULL literals a type
    val operands = call.getOperands.zipWithIndex.map {

      // this helps e.g. for AS(null)
      // we might need to extend this logic in case some rules do not create typed NULLs
      case (operandLiteral: RexLiteral, 0) if
      operandLiteral.getType.getSqlTypeName == SqlTypeName.NULL &&
        call.getOperator.getReturnTypeInference == ReturnTypes.ARG0 =>
        generateNullLiteral(resultType, ctx.nullCheck)

      case (o@_, _) => o.accept(this)
    }

    generateCallExpression(ctx, call.getOperator, operands, resultType)
  }

  override def visitOver(over: RexOver): GeneratedExpression =
    throw new CodeGenException("Aggregate functions over windows are not supported yet.")

  override def visitSubQuery(subQuery: RexSubQuery): GeneratedExpression =
    throw new CodeGenException("Subqueries are not supported yet.")

  override def visitPatternFieldRef(fieldRef: RexPatternFieldRef): GeneratedExpression =
    throw new CodeGenException("Pattern field references are not supported yet.")

  // ----------------------------------------------------------------------------------------

  private def generateCallExpression(
      ctx: CodeGeneratorContext,
      operator: SqlOperator,
      operands: Seq[GeneratedExpression],
      resultType: InternalType): GeneratedExpression = {
    operator match {
      // arithmetic
      case PLUS if isNumeric(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumeric(left)
        requireNumeric(right)
        generateBinaryArithmeticOperator(ctx, "+", resultType, left, right)

      case PLUS | DATETIME_PLUS if isTemporal(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireTemporal(left)
        requireTemporal(right)
        generateTemporalPlusMinus(ctx, plus = true, resultType, left, right)

      case MINUS if isNumeric(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumeric(left)
        requireNumeric(right)
        generateBinaryArithmeticOperator(ctx, "-", resultType, left, right)

      case MINUS | MINUS_DATE if isTemporal(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireTemporal(left)
        requireTemporal(right)
        generateTemporalPlusMinus(ctx, plus = false, resultType, left, right)

      case MULTIPLY if isNumeric(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumeric(left)
        requireNumeric(right)
        generateBinaryArithmeticOperator(ctx, "*", resultType, left, right)

      case MULTIPLY if isTimeInterval(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireTimeInterval(left)
        requireNumeric(right)
        generateBinaryArithmeticOperator(ctx, "*", resultType, left, right)

      case DIVIDE | DIVIDE_INTEGER if isNumeric(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumeric(left)
        requireNumeric(right)
        generateBinaryArithmeticOperator(ctx, "/", resultType, left, right)

      case MOD if isNumeric(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumeric(left)
        requireNumeric(right)
        generateBinaryArithmeticOperator(ctx, "%", resultType, left, right)

      case UNARY_MINUS if isNumeric(resultType) =>
        val operand = operands.head
        requireNumeric(operand)
        generateUnaryArithmeticOperator(ctx, "-", resultType, operand)

      case UNARY_MINUS if isTimeInterval(resultType) =>
        val operand = operands.head
        requireTimeInterval(operand)
        generateUnaryIntervalPlusMinus(ctx, plus = false, operand)

      case UNARY_PLUS if isNumeric(resultType) =>
        val operand = operands.head
        requireNumeric(operand)
        generateUnaryArithmeticOperator(ctx, "+", resultType, operand)

      case UNARY_PLUS if isTimeInterval(resultType) =>
        val operand = operands.head
        requireTimeInterval(operand)
        generateUnaryIntervalPlusMinus(ctx, plus = true, operand)

      // comparison
      case EQUALS =>
        val left = operands.head
        val right = operands(1)
        generateEquals(ctx, left, right)

      case NOT_EQUALS =>
        val left = operands.head
        val right = operands(1)
        generateNotEquals(ctx, left, right)

      case GREATER_THAN =>
        val left = operands.head
        val right = operands(1)
        requireComparable(left)
        requireComparable(right)
        generateComparison(ctx, ">", left, right)

      case GREATER_THAN_OR_EQUAL =>
        val left = operands.head
        val right = operands(1)
        requireComparable(left)
        requireComparable(right)
        generateComparison(ctx, ">=", left, right)

      case LESS_THAN =>
        val left = operands.head
        val right = operands(1)
        requireComparable(left)
        requireComparable(right)
        generateComparison(ctx, "<", left, right)

      case LESS_THAN_OR_EQUAL =>
        val left = operands.head
        val right = operands(1)
        requireComparable(left)
        requireComparable(right)
        generateComparison(ctx, "<=", left, right)

      case IS_NULL =>
        val operand = operands.head
        generateIsNull(ctx, operand)

      case IS_NOT_NULL =>
        val operand = operands.head
        generateIsNotNull(ctx, operand)

      // logic
      case AND =>
        operands.reduceLeft { (left: GeneratedExpression, right: GeneratedExpression) =>
          requireBoolean(left)
          requireBoolean(right)
          generateAnd(ctx, left, right)
        }

      case OR =>
        operands.reduceLeft { (left: GeneratedExpression, right: GeneratedExpression) =>
          requireBoolean(left)
          requireBoolean(right)
          generateOr(ctx, left, right)
        }

      case NOT =>
        val operand = operands.head
        requireBoolean(operand)
        generateNot(ctx, operand)

      case CASE =>
        generateIfElse(ctx, operands, resultType)

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
        generateIn(ctx, left, right)

      case NOT_IN =>
        val left = operands.head
        val right = operands.tail
        generateNot(ctx, generateIn(ctx, left, right))

      // casting
      case CAST =>
        val operand = operands.head
        generateCast(ctx, operand, resultType)

      // Reinterpret
      case REINTERPRET =>
        val operand = operands.head
        generateReinterpret(ctx, operand, resultType)

      // as / renaming
      case AS =>
        operands.head

      // rows
      case ROW =>
        generateRow(ctx, resultType, operands)

      // arrays
      case ARRAY_VALUE_CONSTRUCTOR =>
        generateArray(ctx, resultType, operands)

      // maps
      case MAP_VALUE_CONSTRUCTOR =>
        generateMap(ctx, resultType, operands)

      case ITEM =>
        operands.head.resultType match {
          case t: InternalType if TypeCheckUtils.isArray(t) =>
            val array = operands.head
            val index = operands(1)
            requireInteger(index)
            generateArrayElementAt(ctx, array, index)

          case t: InternalType if TypeCheckUtils.isMap(t) =>
            val key = operands(1)
            generateMapGet(ctx, operands.head, key)

          case _ => throw new CodeGenException("Expect an array or a map.")
        }

      case CARDINALITY =>
        operands.head.resultType match {
          case t: InternalType if TypeCheckUtils.isArray(t) =>
            val array = operands.head
            generateArrayCardinality(ctx, array)

          case t: InternalType if TypeCheckUtils.isMap(t) =>
            val map = operands.head
            generateMapCardinality(ctx, map)

          case _ => throw new CodeGenException("Expect an array or a map.")
        }

      case ELEMENT =>
        val array = operands.head
        requireArray(array)
        generateArrayElement(ctx, array)

      case DOT =>
        generateDot(ctx, operands)

      case PROCTIME =>
        // attribute is proctime indicator.
        // We use a null literal and generate a timestamp when we need it.
        generateNullLiteral(InternalTypes.PROCTIME_INDICATOR, ctx.nullCheck)

      case PROCTIME_MATERIALIZE =>
        generateProctimeTimestamp(ctx, contextTerm)

      case STREAMRECORD_TIMESTAMP =>
        generateRowtimeAccess(ctx, contextTerm)

      // advanced scalar functions
      case sqlOperator: SqlOperator =>
        val explainCall = s"$sqlOperator(${operands.map(_.resultType).mkString(", ")})"
        // TODO: support BinaryStringCallGen and  FunctionGenerator
        throw new CodeGenException(s"Unsupported call: $explainCall \n" +
          s"If you think this function should be supported, " +
          s"you can create an issue and start a discussion for it.")

      // unknown or invalid
      case call@_ =>
        val explainCall = s"$call(${operands.map(_.resultType).mkString(", ")})"
        throw new CodeGenException(s"Unsupported call: $explainCall")
    }
  }
}
