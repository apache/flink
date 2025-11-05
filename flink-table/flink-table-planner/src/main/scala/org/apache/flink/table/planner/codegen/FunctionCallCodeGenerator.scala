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

import org.apache.flink.api.common.functions.{FlatMapFunction, Function, OpenContext}
import org.apache.flink.configuration.ReadableConfig
import org.apache.flink.streaming.api.functions.async.AsyncFunction
import org.apache.flink.table.catalog.DataTypeFactory
import org.apache.flink.table.data.{GenericRowData, RowData}
import org.apache.flink.table.data.utils.JoinedRowData
import org.apache.flink.table.functions.{AsyncTableFunction, TableFunction, UserDefinedFunction, UserDefinedFunctionHelper}
import org.apache.flink.table.planner.codegen.CodeGenUtils.{boxedTypeTermForType, className, newName, DEFAULT_COLLECTOR_TERM, DEFAULT_INPUT1_TERM, DEFAULT_INPUT2_TERM}
import org.apache.flink.table.planner.codegen.GenerateUtils.{generateInputAccess, generateLiteral}
import org.apache.flink.table.planner.delegation.PlannerBase
import org.apache.flink.table.planner.functions.inference.FunctionCallContext
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil.{Constant, FieldRef, FunctionParam}
import org.apache.flink.table.planner.plan.utils.RexLiteralUtil
import org.apache.flink.table.runtime.collector.ListenableCollector
import org.apache.flink.table.runtime.collector.ListenableCollector.CollectListener
import org.apache.flink.table.runtime.generated.{GeneratedCollector, GeneratedFunction}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.{LogicalType, RowType}
import org.apache.flink.util.Collector

import org.apache.calcite.rex.RexNode

import java.util

import scala.collection.JavaConverters._

object FunctionCallCodeGenerator {

  case class GeneratedTableFunctionWithDataType[F <: Function](
      tableFunc: GeneratedFunction[F],
      dataType: DataType)

  /** Generates a sync function ([[TableFunction]]) call. */
  def generateSyncFunctionCall(
      tableConfig: ReadableConfig,
      classLoader: ClassLoader,
      dataTypeFactory: DataTypeFactory,
      inputType: LogicalType,
      functionOutputType: LogicalType,
      collectorOutputType: LogicalType,
      parameters: util.List[FunctionParam],
      syncFunctionDefinition: TableFunction[_],
      inferCall: (
          CodeGeneratorContext,
          FunctionCallContext,
          UserDefinedFunction,
          Seq[GeneratedExpression]) => (GeneratedExpression, DataType),
      functionName: String,
      generateClassName: String,
      fieldCopy: Boolean): GeneratedTableFunctionWithDataType[FlatMapFunction[RowData, RowData]] = {

    val bodyCode: GeneratedExpression => String = call => {
      val resultCollectorTerm = call.resultTerm
      s"""
         |$resultCollectorTerm.setCollector($DEFAULT_COLLECTOR_TERM);
         |${call.code}
         |""".stripMargin
    }

    generateFunctionCall(
      classOf[FlatMapFunction[RowData, RowData]],
      tableConfig,
      classLoader,
      dataTypeFactory,
      inputType,
      functionOutputType,
      collectorOutputType,
      parameters,
      syncFunctionDefinition,
      inferCall,
      functionName,
      generateClassName,
      fieldCopy,
      bodyCode
    )
  }

  /** Generates an async function ([[AsyncTableFunction]]) call. */
  def generateAsyncFunctionCall(
      tableConfig: ReadableConfig,
      classLoader: ClassLoader,
      dataTypeFactory: DataTypeFactory,
      inputType: LogicalType,
      functionOutputType: LogicalType,
      collectorOutputType: LogicalType,
      parameters: util.List[FunctionParam],
      asyncFunctionDefinition: AsyncTableFunction[_],
      generateCallWithDataType: (
          CodeGeneratorContext,
          FunctionCallContext,
          UserDefinedFunction,
          Seq[GeneratedExpression]) => (GeneratedExpression, DataType),
      functionName: String,
      generateClassName: String
  ): GeneratedTableFunctionWithDataType[AsyncFunction[RowData, AnyRef]] = {
    generateFunctionCall(
      classOf[AsyncFunction[RowData, AnyRef]],
      tableConfig,
      classLoader,
      dataTypeFactory,
      inputType,
      functionOutputType,
      collectorOutputType,
      parameters,
      asyncFunctionDefinition,
      generateCallWithDataType,
      functionName,
      generateClassName,
      fieldCopy = true,
      _.code
    )
  }

  private def generateFunctionCall[F <: Function](
      generatedClass: Class[F],
      tableConfig: ReadableConfig,
      classLoader: ClassLoader,
      dataTypeFactory: DataTypeFactory,
      inputType: LogicalType,
      functionOutputType: LogicalType,
      collectorOutputType: LogicalType,
      parameters: util.List[FunctionParam],
      functionDefinition: UserDefinedFunction,
      generateCallWithDataType: (
          CodeGeneratorContext,
          FunctionCallContext,
          UserDefinedFunction,
          Seq[GeneratedExpression]) => (GeneratedExpression, DataType),
      functionName: String,
      generateClassName: String,
      fieldCopy: Boolean,
      bodyCode: GeneratedExpression => String): GeneratedTableFunctionWithDataType[F] = {

    val callContext =
      new FunctionCallContext(
        dataTypeFactory,
        functionDefinition,
        inputType,
        parameters,
        functionOutputType)

    // create the final UDF for runtime
    val udf = UserDefinedFunctionHelper.createSpecializedFunction(
      functionName,
      functionDefinition,
      callContext,
      classOf[PlannerBase].getClassLoader,
      tableConfig,
      // no need to support expression evaluation at this point
      null
    )

    val ctx = new CodeGeneratorContext(tableConfig, classLoader)
    val operands = prepareOperands(ctx, inputType, parameters, fieldCopy)

    val callWithDataType: (GeneratedExpression, DataType) =
      generateCallWithDataType(ctx, callContext, udf, operands)

    val function = FunctionCodeGenerator.generateFunction(
      ctx,
      generateClassName,
      generatedClass,
      bodyCode(callWithDataType._1),
      collectorOutputType,
      inputType)

    GeneratedTableFunctionWithDataType(function, callWithDataType._2)
  }

  private def prepareOperands(
      ctx: CodeGeneratorContext,
      inputType: LogicalType,
      parameters: util.List[FunctionParam],
      fieldCopy: Boolean): Seq[GeneratedExpression] = {

    parameters.asScala
      .map {
        case constantKey: Constant =>
          val res = RexLiteralUtil.toFlinkInternalValue(constantKey.literal)
          generateLiteral(ctx, res.f0, res.f1)
        case fieldKey: FieldRef =>
          generateInputAccess(
            ctx,
            inputType,
            DEFAULT_INPUT1_TERM,
            fieldKey.index,
            nullableInput = false,
            fieldCopy)
        case _ =>
          throw new CodeGenException("Invalid parameters.")
      }
  }

  /**
   * Generates collector for join ([[Collector]])
   *
   * Differs from CommonCorrelate.generateCollector which has no real condition because of
   * FLINK-7865, here we should deal with outer join type when real conditions filtered result.
   */
  def generateCollector(
      ctx: CodeGeneratorContext,
      inputRowType: RowType,
      rightRowType: RowType,
      resultRowType: RowType,
      condition: Option[RexNode],
      pojoFieldMapping: Option[Array[Int]],
      retainHeader: Boolean = true): GeneratedCollector[ListenableCollector[RowData]] = {

    val inputTerm = DEFAULT_INPUT1_TERM
    val rightInputTerm = DEFAULT_INPUT2_TERM

    val exprGenerator = new ExprCodeGenerator(ctx, nullableInput = false)
      .bindInput(rightRowType, inputTerm = rightInputTerm, inputFieldMapping = pojoFieldMapping)

    val rightResultExpr =
      exprGenerator.generateConverterResultExpression(rightRowType, classOf[GenericRowData])

    val joinedRowTerm = CodeGenUtils.newName(ctx, "joinedRow")
    ctx.addReusableOutputRecord(resultRowType, classOf[JoinedRowData], joinedRowTerm)

    val header = if (retainHeader) {
      s"$joinedRowTerm.setRowKind($inputTerm.getRowKind());"
    } else {
      ""
    }

    val body =
      s"""
         |${rightResultExpr.code}
         |$joinedRowTerm.replace($inputTerm, ${rightResultExpr.resultTerm});
         |$header
         |outputResult($joinedRowTerm);
      """.stripMargin

    val collectorCode = if (condition.isEmpty) {
      body
    } else {

      val filterGenerator = new ExprCodeGenerator(ctx, nullableInput = false)
        .bindInput(inputRowType, inputTerm)
        .bindSecondInput(rightRowType, rightInputTerm, pojoFieldMapping)
      val filterCondition = filterGenerator.generateExpression(condition.get)

      s"""
         |${filterCondition.code}
         |if (${filterCondition.resultTerm}) {
         |  $body
         |}
         |""".stripMargin
    }

    generateTableFunctionCollectorForJoinTable(
      ctx,
      "JoinTableFuncCollector",
      collectorCode,
      inputRowType,
      rightRowType,
      inputTerm = inputTerm,
      collectedTerm = rightInputTerm)
  }

  /**
   * The only differences against CollectorCodeGenerator.generateTableFunctionCollector is
   * "super.collect" call is binding with collect join row in "body" code
   */
  private def generateTableFunctionCollectorForJoinTable(
      ctx: CodeGeneratorContext,
      name: String,
      bodyCode: String,
      inputType: RowType,
      collectedType: RowType,
      inputTerm: String = DEFAULT_INPUT1_TERM,
      collectedTerm: String = DEFAULT_INPUT2_TERM)
      : GeneratedCollector[ListenableCollector[RowData]] = {

    val funcName = newName(ctx, name)
    val input1TypeClass = boxedTypeTermForType(inputType)
    val input2TypeClass = boxedTypeTermForType(collectedType)

    val funcCode =
      s"""
      public class $funcName extends ${classOf[ListenableCollector[_]].getCanonicalName} {

        ${ctx.reuseMemberCode()}

        public $funcName(Object[] references) throws Exception {
          ${ctx.reuseInitCode()}
        }

        @Override
        public void open(${className[OpenContext]} openContext) throws Exception {
          ${ctx.reuseOpenCode()}
        }

        @Override
        public void collect(Object record) throws Exception {
          $input1TypeClass $inputTerm = ($input1TypeClass) getInput();
          $input2TypeClass $collectedTerm = ($input2TypeClass) record;

          // callback only when collectListener exists, equivalent to:
          // getCollectListener().ifPresent(
          //   listener -> ((CollectListener) listener).onCollect(record));
          // TODO we should update code splitter's grammar file to accept lambda expressions.

          if (getCollectListener().isPresent()) {
             ((${classOf[CollectListener[_]].getCanonicalName}) getCollectListener().get())
             .onCollect(record);
          }

          ${ctx.reuseLocalVariableCode()}
          ${ctx.reuseInputUnboxingCode()}
          ${ctx.reusePerRecordCode()}
          $bodyCode
        }

        @Override
        public void close() throws Exception {
          ${ctx.reuseCloseCode()}
        }
      }
    """.stripMargin

    new GeneratedCollector(funcName, funcCode, ctx.references.toArray, ctx.tableConfig)
  }
}
