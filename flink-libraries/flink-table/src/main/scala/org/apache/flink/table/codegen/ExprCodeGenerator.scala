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
import org.apache.calcite.sql.`type`.{ReturnTypes, SqlTypeName}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.types._
import org.apache.flink.table.calcite.{FlinkTypeFactory, RexAggBufferVariable, RexAggLocalVariable, RexDistinctKeyVariable}
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.GeneratedExpression.{NEVER_NULL, NO_CODE}
import org.apache.flink.table.dataformat._
import org.apache.flink.table.functions.sql.{ProctimeSqlFunction, StreamRecordTimestampSqlFunction}
import org.apache.flink.table.typeutils.TypeUtils
import org.apache.flink.util.Preconditions.checkArgument

import scala.collection.JavaConversions._

/**
 * This code generator is mainly responsible for generating codes for a given calcite [[RexNode]].
 * It can also generate type conversion codes for the result converter.
 */
class ExprCodeGenerator(ctx: CodeGeneratorContext, nullableInput: Boolean, nullCheck: Boolean)
  extends RexVisitor[GeneratedExpression] {

  // check if nullCheck is enabled when inputs can be null
  if (nullableInput && !nullCheck) {
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
      inputTerm: String = CodeGeneratorContext.DEFAULT_INPUT1_TERM,
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
      inputTerm: String = CodeGeneratorContext.DEFAULT_INPUT2_TERM,
      inputFieldMapping: Option[Array[Int]] = None): ExprCodeGenerator = {
    input2Type = Some(inputType)
    input2Term = Some(inputTerm)
    input2FieldMapping = inputFieldMapping
    this
  }

  protected lazy val input1Mapping: Array[Int] = input1FieldMapping match {
    case Some(mapping) => mapping
    case _ => (0 until TypeUtils.getArity(input1Type)).toArray
  }

  protected lazy val input2Mapping: Array[Int] = input2FieldMapping match {
    case Some(mapping) => mapping
    case _ => input2Type match {
      case Some(input) => (0 until TypeUtils.getArity(input)).toArray
      case _ => Array[Int]()
    }
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
      outRecordTerm: String = CodeGeneratorContext.DEFAULT_OUT_RECORD_TERM,
      outRecordWriterTerm: String = CodeGeneratorContext.DEFAULT_OUT_RECORD_WRITER_TERM,
      reusedOutRow: Boolean = true,
      fieldCopy: Boolean = false,
      rowtimeExpression: Option[RexNode] = None)
    : GeneratedExpression = {
    val input1AccessExprs = input1Mapping.map {
      case DataTypes.ROWTIME_STREAM_MARKER |
           DataTypes.ROWTIME_BATCH_MARKER if rowtimeExpression.isDefined =>
        // generate rowtime attribute from expression
        generateExpression(rowtimeExpression.get)
      case DataTypes.ROWTIME_STREAM_MARKER |
           DataTypes.ROWTIME_BATCH_MARKER =>
        throw new TableException("Rowtime extraction expression missing. Please report a bug.")
      case DataTypes.PROCTIME_STREAM_MARKER =>
        // attribute is proctime indicator.
        // we use a null literal and generate a timestamp when we need it.
        generateNullLiteral(DataTypes.PROCTIME_INDICATOR, nullCheck)
      case DataTypes.PROCTIME_BATCH_MARKER =>
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
          nullCheck,
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
          nullCheck)
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
      outRow: String = CodeGeneratorContext.DEFAULT_OUT_RECORD_TERM,
      outRowWriter: Option[String] = Some(CodeGeneratorContext.DEFAULT_OUT_RECORD_WRITER_TERM),
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
        if (returnType.getInternalTypeAt(i).getClass != fieldExpr.resultType.getClass
          && !returnType.getInternalTypeAt(i).isInstanceOf[GenericType[_]]) {
          throw new CodeGenException(
            s"Incompatible types of expression and result type, Expression[$fieldExpr] type is " +
                s"[${fieldExpr.resultType}], result type is [${returnType.getInternalTypeAt(i)}]")
        }
      case (fieldExpr, i) if fieldExpr.resultType != returnType.getInternalTypeAt(i) =>
        throw new CodeGenException(
          s"Incompatible types of expression and result type. Expression[$fieldExpr] type is " +
              s"[${fieldExpr.resultType}], result type is [${returnType.getInternalTypeAt(i)}]")
      case _ => // ok
    }

    def getOutputRowPos(fieldExprIdx: Int): Int =
      fieldExprIdxToOutputRowPosMap.getOrElse(fieldExprIdx,
        throw new CodeGenException(s"Illegal field expr index: $fieldExprIdx"))

    def objectArrayRowWrite(
        genUpdate: (Int, GeneratedExpression) => String): GeneratedExpression = {
      val initReturnRecord = if (outRowAlreadyExists) {
        ""
      } else {
        ctx.addOutputRecord(returnType, returnTypeClazz, outRow, reused = reusedOutRow)
      }
      val resultBuffer: Seq[String] = fieldExprs.zipWithIndex map {
        case (fieldExpr, i) =>
          val idx = getOutputRowPos(i)
          if (nullCheck) {
            s"""
               |${fieldExpr.code}
               |if (${fieldExpr.nullTerm}) {
               |  $outRow.setNullAt($idx);
               |} else {
               |  ${genUpdate(idx, fieldExpr)};
               |}
               |""".stripMargin
          }
          else {
            s"""
               |${fieldExpr.code}
               |${genUpdate(idx, fieldExpr)};
               |""".stripMargin
          }
      }

      val statement =
        s"""
           |$initReturnRecord
           |${resultBuffer.mkString("")}
         """.stripMargin.trim

      GeneratedExpression(outRow, "false", statement, returnType,
        codeBuffer = resultBuffer, preceding = s"$initReturnRecord")
    }

    returnTypeClazz match {
      case cls if cls == classOf[BinaryRow] =>
        outRowWriter match {
          case Some(writer) => // binary row writer.
            val initReturnRecord = if (outRowAlreadyExists) {
              ""
            } else {
              ctx.addOutputRecord(returnType, returnTypeClazz, outRow, outRowWriter, reusedOutRow)
            }
            val resetWriter = if (nullCheck) s"$writer.reset();" else s"$writer.resetCursor();"
            val completeWriter: String = s"$writer.complete();"
            val resultBuffer = fieldExprs.zipWithIndex map {
              case (fieldExpr, i) =>
                val t = returnType.getInternalTypeAt(i)
                val idx = getOutputRowPos(i)
                val writeCode = binaryWriterWriteField(ctx, idx, fieldExpr.resultTerm, writer, t)
                if (nullCheck) {
                  s"""
                     |${fieldExpr.code}
                     |if (${fieldExpr.nullTerm}) {
                     |  ${binaryWriterWriteNull(idx, writer, t)};
                     |} else {
                     |  $writeCode;
                     |}
                     |""".stripMargin.trim
                } else {
                  s"""
                     |${fieldExpr.code}
                     |$writeCode;
                     |""".stripMargin.trim
                }
            }

            val statement =
              s"""
                 |$initReturnRecord
                 |$resetWriter
                 |${resultBuffer.mkString("\n")}
                 |$completeWriter
                 |""".stripMargin.trim
            GeneratedExpression(outRow, "false", statement, returnType,
              codeBuffer = resultBuffer,
              preceding = s"$initReturnRecord\n$resetWriter",
              flowing = s"$completeWriter")

          case None => // update to binary row (setXXX).
            checkArgument(outRowAlreadyExists)
            val resultBuffer = fieldExprs.zipWithIndex map {
              case (fieldExpr, i) =>
                val t = returnType.getInternalTypeAt(i)
                val idx = getOutputRowPos(i)
                val writeCode = binaryRowFieldSetAccess(idx, outRow, t, fieldExpr.resultTerm)
                if (nullCheck) {
                  s"""
                     |${fieldExpr.code}
                     |if (${fieldExpr.nullTerm}) {
                     |  ${binaryRowSetNull(idx, outRow, t)};
                     |} else {
                     |  $writeCode;
                     |}
                     |""".stripMargin.trim
                } else {
                  s"""
                     |${fieldExpr.code}
                     |$writeCode;
                     |""".stripMargin.trim
                }
            }
            GeneratedExpression(
              outRow, "false", resultBuffer.mkString(""), returnType)
        }

      case cls if cls == classOf[GenericRow] =>
        objectArrayRowWrite((i: Int, expr: GeneratedExpression) =>
        s"$outRow.update($i, ${expr.resultTerm})")

      case cls if cls == classOf[BoxedWrapperRow] =>
        objectArrayRowWrite((i: Int, expr: GeneratedExpression) =>
          boxedWrapperRowFieldUpdateAccess(i, expr.resultTerm, outRow, expr.resultType))
    }
  }

  override def visitInputRef(inputRef: RexInputRef): GeneratedExpression = {
    // if inputRef index is within size of input1 we work with input1, input2 otherwise
    val input = if (inputRef.getIndex < TypeUtils.getArity(input1Type)) {
      (input1Type, input1Term)
    } else {
      (input2Type.getOrElse(throw new CodeGenException("Invalid input access.")),
        input2Term.getOrElse(throw new CodeGenException("Invalid input access.")))
    }

    val index = if (input._2 == input1Term) {
      inputRef.getIndex
    } else {
      inputRef.getIndex - TypeUtils.getArity(input1Type)
    }

    generateInputAccess(ctx, input._1, input._2, index, nullableInput, nullCheck)
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
      index,
      nullCheck)

    val resultTypeTerm = primitiveTypeTermForType(fieldAccessExpr.resultType)
    val defaultValue = primitiveDefaultValue(fieldAccessExpr.resultType)
    val Seq(resultTerm, nullTerm) = ctx.newReusableFields(
      Seq("result", "isNull"),
      Seq(resultTypeTerm, "boolean"))

    val resultCode = if (nullCheck) {
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
    generateLiteral(ctx, literal.getType, resultType, value, nullCheck)
  }

  override def visitCorrelVariable(correlVariable: RexCorrelVariable): GeneratedExpression = {
    GeneratedExpression(input1Term, NEVER_NULL, NO_CODE, input1Type)
  }

  override def visitLocalRef(localRef: RexLocalRef): GeneratedExpression = localRef match {
    case localVar: RexAggBufferVariable =>
      val resultTerm = localVar.getName
      val nullTerm = resultTerm + "IsNull"
      val pType = primitiveTypeTermForType(localVar.internalType)
      ctx.addReusableMember(s"$pType $resultTerm;")
      ctx.addReusableMember(s"boolean $nullTerm;")
      GeneratedExpression(resultTerm, nullTerm, "", localVar.internalType)
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

    // special case: time materialization
    if (call.getOperator == ProctimeSqlFunction) {
      return generateProctimeTimestamp(contextTerm, ctx)
    }

    if (call.getOperator == StreamRecordTimestampSqlFunction) {
      return generateRowtimeAccess(contextTerm, ctx)
    }

    val resultType = FlinkTypeFactory.toInternalType(call.getType)

    // convert operands and help giving untyped NULL literals a type
    val operands = call.getOperands.zipWithIndex.map {

      // this helps e.g. for AS(null)
      // we might need to extend this logic in case some rules do not create typed NULLs
      case (operandLiteral: RexLiteral, 0) if
          operandLiteral.getType.getSqlTypeName == SqlTypeName.NULL &&
          call.getOperator.getReturnTypeInference == ReturnTypes.ARG0 =>
        generateNullLiteral(resultType, nullCheck)

      case (o@_, idx) => o.accept(this)
    }

    generateCallExpression(
      ctx, call.getOperator, operands, resultType, nullCheck)
  }

  override def visitOver(over: RexOver): GeneratedExpression =
    throw new CodeGenException("Aggregate functions over windows are not supported yet.")

  override def visitSubQuery(subQuery: RexSubQuery): GeneratedExpression =
    throw new CodeGenException("Subqueries are not supported yet.")

  override def visitPatternFieldRef(fieldRef: RexPatternFieldRef): GeneratedExpression =
    throw new CodeGenException("Pattern field references are not supported yet.")
}
