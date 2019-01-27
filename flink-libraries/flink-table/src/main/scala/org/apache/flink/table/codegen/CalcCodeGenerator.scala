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

import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlKind
import org.apache.flink.api.common.functions.Function
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.types.{DataTypes, RowType, TypeConverters}
import org.apache.flink.table.api.{TableConfig, TableConfigOptions, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.CodeGenUtils.{boxedTypeTermForType, newNames}
import org.apache.flink.table.codegen.operator.OperatorCodeGenerator
import org.apache.flink.table.dataformat.{BaseRow, BoxedWrapperRow}
import org.apache.flink.table.runtime.OneInputSubstituteStreamOperator
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object CalcCodeGenerator {

  private[flink] def generateCalcOperator(
    ctx: CodeGeneratorContext,
    cluster: RelOptCluster,
    inputDataType: RelDataType,
    inputTransform: StreamTransformation[BaseRow],
    outRowType: RelDataType,
    config: TableConfig,
    calcProgram: RexProgram,
    condition: Option[RexNode],
    retainHeader: Boolean = false,
    ruleDescription: String
  ): OneInputSubstituteStreamOperator[BaseRow, BaseRow] = {
    val inputType = TypeConverters.createInternalTypeFromTypeInfo(
      inputTransform.getOutputType).asInstanceOf[RowType]
    // filter out time attributes
    val inputTerm = CodeGeneratorContext.DEFAULT_INPUT1_TERM
    val outputType = FlinkTypeFactory.toInternalRowType(outRowType)
    val (processCode, codeSplit, filterCodeSplit) = generateProcessCode(
      ctx,
      inputType,
      outputType,
      classOf[BoxedWrapperRow],
      outputType.getFieldNames,
      config,
      calcProgram,
      condition,
      retainHeader = retainHeader)

    val genOperatorExpression =
      OperatorCodeGenerator.generateOneInputStreamOperator[BaseRow, BaseRow](
        ctx,
        ruleDescription,
        processCode,
        "",
        inputType,
        config,
        inputTerm = inputTerm,
        splitFunc = codeSplit,
        filterSplitFunc = filterCodeSplit,
        lazyInputUnboxingCode = true)

    new OneInputSubstituteStreamOperator[BaseRow, BaseRow](
      genOperatorExpression.name,
      genOperatorExpression.code,
      references = ctx.references)
  }

  private[flink] def generateFunction[T <: Function](
      inputType: RowType,
      ruleDescription: String,
      returnType: RowType,
      outRowClass: Class[_ <: BaseRow],
      calcProjection: RexProgram,
      calcCondition: Option[RexNode],
      config: TableConfig,
      functionClass: Class[T]): GeneratedFunction[T, BaseRow] = {
    val ctx = CodeGeneratorContext(config)
    val inputTerm = CodeGeneratorContext.DEFAULT_INPUT1_TERM
    val collectorTerm = CodeGeneratorContext.DEFAULT_OPERATOR_COLLECTOR_TERM
    val (processCode, codeSplit, filterCodeSplit) = generateProcessCode(
      ctx,
      inputType,
      returnType,
      outRowClass,
      returnType.getFieldNames,
      config,
      calcProjection,
      calcCondition,
      collectorTerm = collectorTerm,
      outputDirectly = true
    )
    FunctionCodeGenerator.generateFunction(
      ctx,
      ruleDescription,
      functionClass,
      processCode,
      returnType,
      inputType,
      config,
      input1Term = inputTerm,
      collectorTerm = collectorTerm,
      codeSplit = codeSplit,
      filterCodeSplit = filterCodeSplit
    )
  }

  private[flink] def generateProcessCode(
      ctx: CodeGeneratorContext,
      inputType: RowType,
      outRowType: RowType,
      outRowClass: Class[_ <: BaseRow],
      resultFieldNames: Seq[String],
      config: TableConfig,
      calcProgram: RexProgram,
      condition: Option[RexNode],
      inputTerm: String = CodeGeneratorContext.DEFAULT_INPUT1_TERM,
      collectorTerm: String = CodeGeneratorContext.DEFAULT_OPERATOR_COLLECTOR_TERM,
      retainHeader: Boolean = false,
      outputDirectly: Boolean = false
  ): (String, GeneratedSplittableExpression, GeneratedSplittableExpression) = {

    val projection = calcProgram.getProjectList.map(calcProgram.expandLocalRef)
    val exprGenerator = new ExprCodeGenerator(ctx, false, config.getNullCheck)
      .bindInput(inputType, inputTerm = inputTerm)

    val onlyFilter = projection.lengthCompare(inputType.getArity) == 0 &&
      projection.zipWithIndex.forall { case (rexNode, index) =>
        rexNode.isInstanceOf[RexInputRef] && rexNode.asInstanceOf[RexInputRef].getIndex == index
      }
    var splitFunc: GeneratedSplittableExpression =
      GeneratedSplittableExpression.UNSPLIT_EXPRESSION
    var filterSplitFunc: GeneratedSplittableExpression =
      GeneratedSplittableExpression.UNSPLIT_EXPRESSION

    def produceOutputCode(resultTerm: String) = if (outputDirectly) {
      s"$collectorTerm.collect($resultTerm);"
    } else {
      s"${OperatorCodeGenerator.generatorCollect(resultTerm)}"
    }

    def produceProjectionCode = {
      val projectionExprs = projection.map(exprGenerator.generateExpression)
      val projectionExpression = exprGenerator.generateResultExpression(
        projectionExprs,
        outRowType,
        outRowClass)

      val inputTypeTerm = boxedTypeTermForType(inputType)

      splitFunc = CodeGenUtils.generateSplitFunctionCalls(
        projectionExpression.codeBuffer,
        config.getConf.getInteger(TableConfigOptions.SQL_CODEGEN_LENGTH_MAX),
        "calcProjectApply",
        "private final void",
        ctx.reuseFieldCode().length,
        defineParams = s"$inputTypeTerm $inputTerm",
        callingParams = inputTerm)

      val projectionExpressionCode = if (splitFunc.isSplit) {
        s"""
           |${projectionExpression.preceding}
           |${splitFunc.callings.mkString("\n")}
           |${projectionExpression.flowing}
       """.stripMargin
      } else {
        projectionExpression.code
      }
      val header = if (retainHeader) {
        s"${projectionExpression.resultTerm}.setHeader($inputTerm.getHeader());"
      } else {
        ""
      }

      s"""
         |$header
         |$projectionExpressionCode
         |${produceOutputCode(projectionExpression.resultTerm)}
         |""".stripMargin
    }

    def produceFilterCode: GeneratedExpression = {
      condition.get match {
        case call: RexCall if call.getKind == SqlKind.OR && call.operands.length > 15 =>
          val subConditions = call.getOperands.asScala.map(exprGenerator.generateExpression)
          filterSplitFunc = CodeGenUtils.generateSplitFunctionCalls(
            subConditions.map(sf => sf.code + "\n" + s"return ${sf.resultTerm};"),
            1, //split every sub condition to single method
            "filterApply",
            "private final boolean",
            0)
          val Seq(conditionResultTerm, nullTerm) = newNames(Seq("result", "isNull"))
          val callCode =
            s"""
               |boolean $nullTerm = false;
               |boolean $conditionResultTerm =
               (${filterSplitFunc.callings.map{
              c => c.substring(0, c.length - 1)
            }.mkString(" || ")});
             """.stripMargin
          GeneratedExpression(conditionResultTerm, nullTerm, callCode, DataTypes.BOOLEAN)
        case call: RexCall if call.getKind == SqlKind.AND
          && call.operands.forall(_.isA(SqlKind.OR)) =>
          val andPostions = call.operands.map(_.asInstanceOf[RexCall].operands.size())
          val subOps = call.operands.flatMap(_.asInstanceOf[RexCall].operands.asScala)
          if (subOps.length > 15) {
            val subConditions = subOps.map(exprGenerator.generateExpression)
            filterSplitFunc = CodeGenUtils.generateSplitFunctionCalls(
              subConditions.map(sf => sf.code + "\n" + s"return ${sf.resultTerm};"),
              1, //split every sub condition to single method
              "filterApply",
              "private final boolean",
              0)
            val Seq(conditionResultTerm, nullTerm) = newNames(Seq("result", "isNull"))
            var index = 0
            var subCalls = filterSplitFunc.callings.map(c => c.substring(0, c.length - 1))
            val resBuffer = new ListBuffer[Seq[String]]()
            while (subCalls.nonEmpty) {
              val len = andPostions(index)
              val subRes = subCalls.take(len)
              subCalls = subCalls.drop(len)
              index += 1
              resBuffer.add(subRes)
            }

            val callCode =
              s"""
                 |boolean $nullTerm = false;
                 |boolean $conditionResultTerm =
               (${resBuffer.map(_.mkString("(", " || ", ")")).mkString(" && ")});
             """.stripMargin

            GeneratedExpression(conditionResultTerm, nullTerm, callCode, DataTypes.BOOLEAN)
          } else {
            exprGenerator.generateExpression(condition.get)
          }
        case _ => exprGenerator.generateExpression(condition.get)
      }
    }

    val processCode = if (condition.isEmpty && onlyFilter) {
      throw new TableException("This calc has no useful projection and no filter. " +
        "It should be removed by CalcRemoveRule.")
    } else if (condition.isEmpty) { // only projection
    val projectionCode = produceProjectionCode
      s"""
         |${ctx.reuseInputUnboxingCode()}
         |$projectionCode
         |""".stripMargin
    } else {
      val filterCondition = produceFilterCode
      // only filter
      if (onlyFilter) {
        s"""
           |${ctx.reuseInputUnboxingCode()}
           |${filterCondition.code}
           |if (${filterCondition.resultTerm}) {
           |  ${produceOutputCode(inputTerm)}
           |}
           |""".stripMargin
      } else { // both filter and projection
      val filterInputCode = ctx.reuseInputUnboxingCode()
        val filterInputSet = Set(ctx.reusableInputUnboxingExprs.keySet.toSeq: _*)

        // if any filter conditions, projection code will enter an new scope
        val projectionCode = produceProjectionCode

        val projectionInputCode = ctx.reusableInputUnboxingExprs
          .filter((entry) => !filterInputSet.contains(entry._1))
          .values.map(_.code).mkString("\n")
        s"""
           |$filterInputCode
           |${filterCondition.code}
           |if (${filterCondition.resultTerm}) {
           |  $projectionInputCode
           |  $projectionCode
           |}
           |""".stripMargin
      }
    }
    (processCode, splitFunc, filterSplitFunc)
  }
}
