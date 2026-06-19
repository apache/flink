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

import org.apache.flink.table.data.RowData
import org.apache.flink.table.data.binary.BinaryRowData
import org.apache.flink.table.data.writer.BinaryRowWriter
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.codegen.GeneratedExpression.{NEVER_NULL, NO_CODE}
import org.apache.flink.table.planner.codegen.GenerateUtils.{generateLiteral, generateRecordStatement}
import org.apache.flink.table.planner.codegen.calls.ScalarOperatorGens
import org.apache.flink.table.planner.functions.aggfunctions._
import org.apache.flink.table.planner.plan.utils.{AggregateInfo, RexLiteralUtil}
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil.{Constant, FunctionParam}
import org.apache.flink.table.runtime.generated.{GeneratedProjection, Projection}
import org.apache.flink.table.types.logical.{BigIntType, LogicalType, RowType}

import scala.collection.mutable.ArrayBuffer

/**
 * CodeGenerator for projection, Take out some fields of [[RowData]] to generate a new [[RowData]].
 */
object ProjectionCodeGenerator {

  val EMPTY_INPUT_MAPPING_VALUE: Int = -1

  def generateProjectionExpression(
      ctx: CodeGeneratorContext,
      inType: RowType,
      outType: RowType,
      inputMapping: Array[Int],
      outClass: Class[_ <: RowData] = classOf[BinaryRowData],
      inputTerm: String = DEFAULT_INPUT1_TERM,
      outRecordTerm: String = DEFAULT_OUT_RECORD_TERM,
      outRecordWriterTerm: String = DEFAULT_OUT_RECORD_WRITER_TERM,
      reusedOutRecord: Boolean = true,
      constantExpressions: Array[GeneratedExpression] = null): GeneratedExpression = {
    val exprGenerator = new ExprCodeGenerator(ctx, false)
      .bindInput(inType, inputTerm = inputTerm, inputFieldMapping = Option(inputMapping))
    val accessExprs = {
      inputMapping.indices.map(
        idx =>
          if (inputMapping(idx) == EMPTY_INPUT_MAPPING_VALUE) {
            constantExpressions(idx)
          } else {
            GenerateUtils.generateFieldAccess(ctx, inType, inputTerm, inputMapping(idx))
          })
    }
    val expression = exprGenerator.generateResultExpression(
      accessExprs,
      outType,
      outClass,
      outRow = outRecordTerm,
      outRowWriter = Option(outRecordWriterTerm),
      reusedOutRow = reusedOutRecord)

    val outRowInitCode = {
      val initCode =
        generateRecordStatement(outType, outClass, outRecordTerm, Some(outRecordWriterTerm), ctx)
      if (reusedOutRecord) {
        NO_CODE
      } else {
        initCode
      }
    }

    val code =
      s"""
         |$outRowInitCode
         |${expression.code}
        """.stripMargin
    GeneratedExpression(outRecordTerm, NEVER_NULL, code, outType)
  }

  /**
   * CodeGenerator for projection.
   * @param reusedOutRecord
   *   If objects or variables can be reused, they will be added a reusable output record to the
   *   member area of the generated class. If not they will be as temp variables.
   * @return
   */
  def generateProjection(
      ctx: CodeGeneratorContext,
      name: String,
      inType: RowType,
      outType: RowType,
      inputMapping: Array[Int],
      outClass: Class[_ <: RowData] = classOf[BinaryRowData],
      inputTerm: String = DEFAULT_INPUT1_TERM,
      outRecordTerm: String = DEFAULT_OUT_RECORD_TERM,
      outRecordWriterTerm: String = DEFAULT_OUT_RECORD_WRITER_TERM,
      reusedOutRecord: Boolean = true,
      constantExpressions: Array[GeneratedExpression] = null): GeneratedProjection = {
    val className = newName(ctx, name)
    val baseClass = classOf[Projection[_, _]]

    val expression = generateProjectionExpression(
      ctx,
      inType,
      outType,
      inputMapping,
      outClass,
      inputTerm,
      outRecordTerm,
      outRecordWriterTerm,
      reusedOutRecord,
      constantExpressions)

    val code =
      s"""
         |public class $className implements ${baseClass.getCanonicalName}<$ROW_DATA, ${outClass.getCanonicalName}> {
         |
         |  ${ctx.reuseMemberCode()}
         |
         |  public $className(Object[] references) throws Exception {
         |    ${ctx.reuseInitCode()}
         |  }
         |
         |  @Override
         |  public ${outClass.getCanonicalName} apply($ROW_DATA $inputTerm) {
         |    ${ctx.reuseLocalVariableCode()}
         |    ${expression.code}
         |    return ${expression.resultTerm};
         |  }
         |}
        """.stripMargin

    new GeneratedProjection(className, code, ctx.references.toArray, ctx.tableConfig)
  }

  /**
   * If adaptive local hash aggregation takes effect, local hash aggregation will be suppressed. In
   * order to ensure that the data structure transmitted downstream with doing local hash
   * aggregation is consistent with the data format transmitted downstream without doing local hash
   * aggregation, we need to do projection for grouping function value.
   *
   * <p> For example, for sql statement "select a, avg(b), count(c) from T group by a", if local
   * hash aggregation suppressed and a row (1, 5, "a") comes to local hash aggregation, we will pass
   * (1, 5, 1, 1) to downstream.
   */
  def genAdaptiveLocalHashAggValueProjectionCode(
      ctx: CodeGeneratorContext,
      inputType: RowType,
      outClass: Class[_ <: RowData] = classOf[BinaryRowData],
      inputTerm: String = DEFAULT_INPUT1_TERM,
      aggInfos: Array[AggregateInfo],
      auxGrouping: Array[Int],
      outRecordTerm: String = DEFAULT_OUT_RECORD_TERM,
      outRecordWriterTerm: String = DEFAULT_OUT_RECORD_WRITER_TERM): String = {
    val fieldExprs =
      genAdaptiveLocalHashAggValueProjectionExpr(ctx, inputType, inputTerm, aggInfos, auxGrouping)
    val binaryRowWriter = CodeGenUtils.className[BinaryRowWriter]
    val typeTerm = outClass.getCanonicalName
    ctx.addReusableMember(s"private $typeTerm $outRecordTerm= new $typeTerm(${fieldExprs.size});")
    ctx.addReusableMember(
      s"private $binaryRowWriter $outRecordWriterTerm = new $binaryRowWriter($outRecordTerm);")

    val fieldExprIdxToOutputRowPosMap = fieldExprs.indices.map(i => i -> i).toMap
    val setFieldsCode = fieldExprs.zipWithIndex
      .map {
        case (fieldExpr, index) =>
          val pos = fieldExprIdxToOutputRowPosMap.getOrElse(
            index,
            throw new CodeGenException(s"Illegal field expr index: $index"))
          rowSetField(
            ctx,
            classOf[BinaryRowData],
            outRecordTerm,
            pos.toString,
            fieldExpr,
            Option(outRecordWriterTerm))
      }
      .mkString("\n")

    val writer = outRecordWriterTerm
    val resetWriter = s"$writer.reset();"
    val completeWriter: String = s"$writer.complete();"
    s"""
       |$resetWriter
       |$setFieldsCode
       |$completeWriter
        """.stripMargin
  }

  def genAdaptiveLocalHashAggValueProjectionExpr(
      ctx: CodeGeneratorContext,
      inputType: RowType,
      inputTerm: String = DEFAULT_INPUT1_TERM,
      aggInfos: Array[AggregateInfo],
      auxGrouping: Array[Int]): Seq[GeneratedExpression] = {
    val fieldExprs: ArrayBuffer[GeneratedExpression] = ArrayBuffer()
    // The auxGrouping also need to consider which is aggBuffer field
    auxGrouping.foreach(i => fieldExprs += reuseFieldExprForAggFunc(ctx, inputType, inputTerm, i))

    aggInfos.map {
      aggInfo =>
        aggInfo.function match {
          case sumAggFunction: SumAggFunction =>
            fieldExprs += genValueProjectionForSumAggFunc(
              ctx,
              inputType,
              inputTerm,
              sumAggFunction.getResultType.getLogicalType,
              aggInfo.agg.getArgList.get(0))
          case _: MaxAggFunction | _: MinAggFunction =>
            fieldExprs += reuseFieldExprForAggFunc(
              ctx,
              inputType,
              inputTerm,
              aggInfo.agg.getArgList.get(0))
          case avgAggFunction: AvgAggFunction =>
            fieldExprs += genValueProjectionForSumAggFunc(
              ctx,
              inputType,
              inputTerm,
              avgAggFunction.getSumType.getLogicalType,
              aggInfo.agg.getArgList.get(0))
            fieldExprs += genValueProjectionForCountAggFunc(
              ctx,
              inputTerm,
              aggInfo.agg.getArgList.get(0))
          case _: CountAggFunction =>
            fieldExprs += genValueProjectionForCountAggFunc(
              ctx,
              inputTerm,
              aggInfo.agg.getArgList.get(0))
          case _: Count1AggFunction =>
            fieldExprs += genValueProjectionForCount1AggFunc(ctx)
        }
    }
    fieldExprs
  }

  /**
   * Do projection for grouping function 'sum(col)' if adaptive local hash aggregation takes effect.
   * For 'count(col)', we will try to convert the projected value type to sum agg function target
   * type if col is not null and convert it to default value type if col is null.
   */
  def genValueProjectionForSumAggFunc(
      ctx: CodeGeneratorContext,
      inputType: LogicalType,
      inputTerm: String,
      targetType: LogicalType,
      index: Int): GeneratedExpression = {
    val fieldExpr = reuseFieldExprForAggFunc(ctx, inputType, inputTerm, index)
    // Convert the projected value type to sum agg func target type.
    ScalarOperatorGens.generateCast(ctx, fieldExpr, targetType, nullOnFailure = true)
  }

  /** Get reuse field expr if it has been evaluated before for adaptive local hash aggregation. */
  def reuseFieldExprForAggFunc(
      ctx: CodeGeneratorContext,
      inputType: LogicalType,
      inputTerm: String,
      index: Int): GeneratedExpression = {
    // Reuse the field access code if it has been evaluated before
    ctx.getReusableInputUnboxingExprs(inputTerm, index) match {
      case None => GenerateUtils.generateFieldAccess(ctx, inputType, inputTerm, index)
      case Some(expr) =>
        GeneratedExpression(expr.resultTerm, expr.nullTerm, NO_CODE, expr.resultType)
    }
  }

  /**
   * Do projection for grouping function 'count(col)' if adaptive local hash aggregation takes
   * effect. 'count(col)' will be convert to 1L if col is not null and convert to 0L if col is null.
   */
  def genValueProjectionForCountAggFunc(
      ctx: CodeGeneratorContext,
      inputTerm: String,
      index: Int): GeneratedExpression = {
    val fieldNullCode = ctx.getReusableInputUnboxingExprs(inputTerm, index) match {
      case None => s"$inputTerm.isNullAt($index)"
      case Some(expr) => expr.nullTerm
    }

    val Seq(fieldTerm, nullTerm) =
      ctx.addReusableLocalVariables(("long", "field"), ("boolean", "isNull"))
    val inputCode =
      s"""
         |$fieldTerm = 0L;
         |if (!$fieldNullCode) {
         |  $fieldTerm = 1L;
         |}
           """.stripMargin.trim

    GeneratedExpression(fieldTerm, nullTerm, inputCode, new BigIntType())
  }

  /**
   * Do projection for grouping function 'count(*)' or 'count(1)' if adaptive local hash agg takes
   * effect. 'count(*) or count(1)' will be convert to 1L and transmitted to downstream.
   */
  def genValueProjectionForCount1AggFunc(ctx: CodeGeneratorContext): GeneratedExpression = {
    val Seq(fieldTerm, nullTerm) =
      ctx.addReusableLocalVariables(("long", "field"), ("boolean", "isNull"))
    val inputCode =
      s"""
         |$fieldTerm = 1L;
         |""".stripMargin.trim
    GeneratedExpression(fieldTerm, nullTerm, inputCode, new BigIntType())
  }

  /** For java invoke. */
  def generateProjection(
      ctx: CodeGeneratorContext,
      name: String,
      inputType: RowType,
      outputType: RowType,
      inputMapping: Array[Int]): GeneratedProjection =
    generateProjection(
      ctx,
      name,
      inputType,
      outputType,
      inputMapping,
      inputTerm = DEFAULT_INPUT1_TERM)

  /** For java invoke. */
  def generateProjection(
      ctx: CodeGeneratorContext,
      name: String,
      inputType: RowType,
      outputType: RowType,
      inputMapping: Array[Int],
      outClass: Class[_ <: RowData]): GeneratedProjection =
    generateProjection(
      ctx,
      name,
      inputType,
      outputType,
      inputMapping,
      outClass = outClass,
      inputTerm = DEFAULT_INPUT1_TERM)

  /**
   * Create projection for lookup keys from left table.
   *
   * @param orderedLookupKeys
   *   lookup keys in the schema order.
   * @param ctx
   *   the context of code generator.
   * @param name
   *   the name of projection.
   * @param inputType
   *   the row type of input.
   * @param outputType
   *   the row type of output.
   * @param inputMapping
   *   the index array. Each value of the array represents the index in the input row that maps to
   *   the corresponding position in the output row. If the value is equal to -1, it indicates that
   *   the corresponding output should use the value from the constant lookup keys.
   * @param outClass
   *   the class of output.
   * @return
   *   the GeneratedProjection
   */
  def generateProjectionForLookupKeysFromLeftTable(
      orderedLookupKeys: Array[FunctionParam],
      ctx: CodeGeneratorContext,
      name: String,
      inputType: RowType,
      outputType: RowType,
      inputMapping: Array[Int],
      outClass: Class[_ <: RowData]): GeneratedProjection = {
    val constantExpressions: Array[GeneratedExpression] =
      new Array[GeneratedExpression](orderedLookupKeys.length)
    for (i <- orderedLookupKeys.indices) {
      orderedLookupKeys(i) match {
        case constantLookupKey: Constant =>
          val res = RexLiteralUtil.toFlinkInternalValue(constantLookupKey.literal)
          constantExpressions(i) = generateLiteral(ctx, res.f0, res.f1)
        case _ =>
          constantExpressions(i) = null
      }
    }
    generateProjection(
      ctx,
      name,
      inputType,
      outputType,
      inputMapping,
      outClass = outClass,
      inputTerm = DEFAULT_INPUT1_TERM,
      constantExpressions = constantExpressions)
  }
}
