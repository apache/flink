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
import org.apache.flink.table.planner.codegen.GenerateUtils.generateRecordStatement
import org.apache.flink.table.planner.codegen.calls.ScalarOperatorGens
import org.apache.flink.table.planner.functions.aggfunctions._
import org.apache.flink.table.planner.plan.utils.AggregateInfo
import org.apache.flink.table.runtime.generated.{GeneratedProjection, Projection}
import org.apache.flink.table.types.logical.{BigIntType, LogicalType, RowType}
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getFieldTypes

import scala.collection.mutable.ArrayBuffer

/**
 * CodeGenerator for projection, Take out some fields of [[RowData]] to generate a new [[RowData]].
 */
object ProjectionCodeGenerator {

  def generateProjectionExpression(
      ctx: CodeGeneratorContext,
      inType: RowType,
      outType: RowType,
      inputMapping: Array[Int],
      outClass: Class[_ <: RowData] = classOf[BinaryRowData],
      inputTerm: String = DEFAULT_INPUT1_TERM,
      outRecordTerm: String = DEFAULT_OUT_RECORD_TERM,
      outRecordWriterTerm: String = DEFAULT_OUT_RECORD_WRITER_TERM,
      reusedOutRecord: Boolean = true): GeneratedExpression = {
    val exprGenerator = new ExprCodeGenerator(ctx, false)
      .bindInput(inType, inputTerm = inputTerm, inputFieldMapping = Option(inputMapping))
    val accessExprs =
      inputMapping.map(idx => GenerateUtils.generateFieldAccess(ctx, inType, inputTerm, idx))
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
      reusedOutRecord: Boolean = true): GeneratedProjection = {
    val className = newName(name)
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
      reusedOutRecord)

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
   * If adaptive local hash agg takes effect, local hash agg is suppressed. In order to ensure that
   * the data structure transmitted downstream with doing local hash agg is consistent with the data
   * format transmitted downstream without doing local hash agg, we need to do projection for
   * grouping function value.
   *
   * <p> For example, for sql statement "select a, avg(b), count(c) from T group by a", if local
   * hash agg suppressed and a row (1, 5, "a") comes to local hash agg, we will pass (1, 5, 1, 1) to
   * downstream.
   */
  def generateAdaptiveLocalHashAggValueProjectionCode(
      ctx: CodeGeneratorContext,
      inputType: RowType,
      outClass: Class[_ <: RowData] = classOf[BinaryRowData],
      inputTerm: String = DEFAULT_INPUT1_TERM,
      aggInfos: Array[AggregateInfo],
      outRecordTerm: String = DEFAULT_OUT_RECORD_TERM,
      outRecordWriterTerm: String = DEFAULT_OUT_RECORD_WRITER_TERM): String = {
    val fieldExprs: ArrayBuffer[GeneratedExpression] = ArrayBuffer()
    aggInfos.map {
      aggInfo =>
        aggInfo.function match {
          case sumAggFunction: SumAggFunction =>
            fieldExprs += generateValueProjectionForSumAggFunc(
              ctx,
              inputType,
              inputTerm,
              sumAggFunction.getResultType.getLogicalType,
              aggInfo.agg.getArgList.get(0))
          case _: MaxAggFunction | _: MinAggFunction =>
            fieldExprs += GenerateUtils.generateFieldAccess(
              ctx,
              inputType,
              inputTerm,
              aggInfo.agg.getArgList.get(0))
          case avgAggFunction: AvgAggFunction =>
            fieldExprs += generateValueProjectionForSumAggFunc(
              ctx,
              inputType,
              inputTerm,
              avgAggFunction.getSumType.getLogicalType,
              aggInfo.agg.getArgList.get(0))
            fieldExprs += generateValueProjectionForCountAggFunc(
              ctx,
              inputTerm,
              aggInfo.agg.getArgList.get(0))
          case _: CountAggFunction =>
            fieldExprs += generateValueProjectionForCountAggFunc(
              ctx,
              inputTerm,
              aggInfo.agg.getArgList.get(0))
          case _: Count1AggFunction =>
            fieldExprs += generateValueProjectionForCount1AggFunc(ctx)
        }
    }

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

  /**
   * Do projection for grouping function 'sum(col)' if adaptive local hash agg takes effect. For
   * 'count(col)', we will try to convert the projected value type to sum agg function target type
   * if col is not null and convert it to default value type if col is null.
   */
  def generateValueProjectionForSumAggFunc(
      ctx: CodeGeneratorContext,
      inputType: LogicalType,
      inputTerm: String,
      targetType: LogicalType,
      index: Int): GeneratedExpression = {
    val fieldType = getFieldTypes(inputType).get(index)
    val resultTypeTerm = primitiveTypeTermForType(fieldType)
    val defaultValue = primitiveDefaultValue(fieldType)
    val readCode = rowFieldReadAccess(index.toString, inputTerm, fieldType)
    val Seq(fieldTerm, nullTerm) =
      ctx.addReusableLocalVariables((resultTypeTerm, "field"), ("boolean", "isNull"))

    val inputCode =
      s"""
         |$nullTerm = $inputTerm.isNullAt($index);
         |$fieldTerm = $defaultValue;
         |if (!$nullTerm) {
         |  $fieldTerm = $readCode;
         |}
           """.stripMargin.trim

    val expression = GeneratedExpression(fieldTerm, nullTerm, inputCode, fieldType)
    // Convert the projected value type to sum agg func target type.
    ScalarOperatorGens.generateCast(ctx, expression, targetType, true)
  }

  /**
   * Do projection for grouping function 'count(col)' if adaptive local hash agg takes effect.
   * 'count(col)' will be convert to 1L if col is not null and convert to 0L if col is null.
   */
  def generateValueProjectionForCountAggFunc(
      ctx: CodeGeneratorContext,
      inputTerm: String,
      index: Int): GeneratedExpression = {
    val Seq(fieldTerm, nullTerm) =
      ctx.addReusableLocalVariables(("long", "field"), ("boolean", "isNull"))

    val inputCode =
      s"""
         |$fieldTerm = 0L;
         |if (!$inputTerm.isNullAt($index)) {
         |  $fieldTerm = 1L;
         |}
           """.stripMargin.trim

    GeneratedExpression(fieldTerm, nullTerm, inputCode, new BigIntType())
  }

  /**
   * Do projection for grouping function 'count(*)' or 'count(1)' if adaptive local hash agg takes
   * effect. 'count(*) or count(1)' will be convert to 1L and transmitted to downstream.
   */
  def generateValueProjectionForCount1AggFunc(ctx: CodeGeneratorContext): GeneratedExpression = {
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
}
