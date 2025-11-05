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

import org.apache.flink.api.common.functions.{FlatMapFunction, OpenContext}
import org.apache.flink.configuration.ReadableConfig
import org.apache.flink.streaming.api.functions.async.AsyncFunction
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.catalog.DataTypeFactory
import org.apache.flink.table.connector.source.{LookupTableSource, ScanTableSource}
import org.apache.flink.table.data.{GenericRowData, RowData}
import org.apache.flink.table.functions._
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.codegen.FunctionCallCodeGenerator.GeneratedTableFunctionWithDataType
import org.apache.flink.table.planner.codegen.Indenter.toISC
import org.apache.flink.table.planner.codegen.calls.BridgingFunctionGenUtil
import org.apache.flink.table.planner.codegen.calls.BridgingFunctionGenUtil.verifyFunctionAwareImplementation
import org.apache.flink.table.planner.functions.inference.FunctionCallContext
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil.FunctionParam
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toScala
import org.apache.flink.table.runtime.collector.{ListenableCollector, TableFunctionResultFuture}
import org.apache.flink.table.runtime.generated.{GeneratedCollector, GeneratedFunction, GeneratedResultFuture}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.extraction.ExtractionUtils.extractSimpleGeneric
import org.apache.flink.table.types.inference.{TypeInference, TypeStrategies, TypeTransformations}
import org.apache.flink.table.types.logical.{LogicalType, RowType}
import org.apache.flink.table.types.utils.DataTypeUtils.transform
import org.apache.flink.types.Row

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

import java.util

import scala.collection.JavaConverters._

object LookupJoinCodeGenerator {

  private val ARRAY_LIST = className[util.ArrayList[_]]

  /** Generates a lookup function ([[TableFunction]]) */
  def generateSyncLookupFunction(
      tableConfig: ReadableConfig,
      classLoader: ClassLoader,
      dataTypeFactory: DataTypeFactory,
      inputType: LogicalType,
      tableSourceType: LogicalType,
      returnType: LogicalType,
      lookupKeys: util.List[FunctionParam],
      syncLookupFunction: TableFunction[_],
      functionName: String,
      fieldCopy: Boolean): GeneratedFunction[FlatMapFunction[RowData, RowData]] = {

    FunctionCallCodeGenerator
      .generateSyncFunctionCall(
        tableConfig,
        classLoader,
        dataTypeFactory,
        inputType,
        tableSourceType,
        returnType,
        lookupKeys,
        syncLookupFunction,
        generateCallWithDataType(
          dataTypeFactory,
          functionName,
          tableSourceType,
          classOf[TableFunction[_]]),
        functionName,
        "LookupFunction",
        fieldCopy
      )
      .tableFunc
  }

  /** Generates a async lookup function ([[AsyncTableFunction]]) */
  def generateAsyncLookupFunction(
      tableConfig: ReadableConfig,
      classLoader: ClassLoader,
      dataTypeFactory: DataTypeFactory,
      inputType: LogicalType,
      tableSourceType: LogicalType,
      returnType: LogicalType,
      lookupKeys: util.List[FunctionParam],
      asyncLookupFunction: AsyncTableFunction[_],
      functionName: String): GeneratedTableFunctionWithDataType[AsyncFunction[RowData, AnyRef]] = {
    FunctionCallCodeGenerator.generateAsyncFunctionCall(
      tableConfig,
      classLoader,
      dataTypeFactory,
      inputType,
      tableSourceType,
      returnType,
      lookupKeys,
      asyncLookupFunction,
      generateCallWithDataType(
        dataTypeFactory,
        functionName,
        tableSourceType,
        classOf[AsyncTableFunction[_]]),
      functionName,
      "AsyncLookupFunction"
    )
  }

  private def generateCallWithDataType(
      dataTypeFactory: DataTypeFactory,
      functionName: String,
      tableSourceType: LogicalType,
      baseClass: Class[_]
  ) = (
      ctx: CodeGeneratorContext,
      callContext: FunctionCallContext,
      udf: UserDefinedFunction,
      operands: Seq[GeneratedExpression]) => {
    def inferCallWithDataType(
        ctx: CodeGeneratorContext,
        callContext: FunctionCallContext,
        udf: UserDefinedFunction,
        operands: Seq[GeneratedExpression],
        legacy: Boolean,
        e: Exception = null): (GeneratedExpression, DataType) = {
      val inference = createLookupTypeInference(
        dataTypeFactory,
        callContext,
        baseClass,
        udf,
        functionName,
        legacy,
        e)

      // TODO: filter all records when there is any nulls on the join key, because
      //  "IS NOT DISTINCT FROM" is not supported yet.
      val callWithDataType = BridgingFunctionGenUtil.generateFunctionAwareCallWithDataType(
        ctx,
        operands,
        tableSourceType,
        inference,
        callContext,
        udf,
        functionName,
        skipIfArgsNull = true
      )
      callWithDataType
    }

    try {
      // user provided type inference has precedence
      // this ensures that all functions work in the same way
      inferCallWithDataType(ctx, callContext, udf, operands, legacy = false)
    } catch {
      case e: Exception =>
        inferCallWithDataType(ctx, callContext, udf, operands, legacy = true, e)
    }
  }

  /**
   * The [[LogicalType]] for inputs and output are known in a [[LookupTableSource]]. Thus, the
   * function's type inference is actually only necessary for enriching the conversion class.
   *
   * For [[ScanTableSource]], we always assume internal data structures. For [[LookupTableSource]],
   * we try regular inference and fallback to internal/default external data structures.
   */
  private def createLookupTypeInference(
      dataTypeFactory: DataTypeFactory,
      callContext: FunctionCallContext,
      baseClass: Class[_],
      udf: UserDefinedFunction,
      functionName: String,
      legacy: Boolean,
      e: Exception): TypeInference = {

    if (!legacy) {
      // user provided type inference has precedence
      // this ensures that all functions work in the same way
      udf.getTypeInference(dataTypeFactory)
    } else {
      // for convenience, we assume internal or default external data structures
      // of expected logical types
      val defaultArgDataTypes = callContext.getArgumentDataTypes.asScala
      val defaultOutputDataType = callContext.getOutputDataType.get()

      val outputClass =
        if (udf.isInstanceOf[LookupFunction] || udf.isInstanceOf[AsyncLookupFunction]) {
          Some(classOf[RowData])
        } else {
          toScala(extractSimpleGeneric(baseClass, udf.getClass, 0))
        }
      val (argDataTypes, outputDataType) = outputClass match {
        case Some(c) if c == classOf[Row] =>
          (defaultArgDataTypes, defaultOutputDataType)
        case Some(c) if c == classOf[RowData] =>
          val internalArgDataTypes = defaultArgDataTypes
            .map(dt => transform(dt, TypeTransformations.TO_INTERNAL_CLASS))
          val internalOutputDataType =
            transform(defaultOutputDataType, TypeTransformations.TO_INTERNAL_CLASS)
          (internalArgDataTypes, internalOutputDataType)
        case _ =>
          throw new ValidationException(
            s"Could not determine a type inference for lookup function '$functionName'. " +
              s"Lookup functions support regular type inference. However, for convenience, the " +
              s"output class can simply be a ${classOf[Row].getSimpleName} or " +
              s"${classOf[RowData].getSimpleName} class in which case the input and output " +
              s"types are derived from the table's schema with default conversion.",
            e)
      }

      verifyFunctionAwareImplementation(argDataTypes, outputDataType, udf, functionName)

      TypeInference
        .newBuilder()
        .typedArguments(argDataTypes.asJava)
        .outputTypeStrategy(TypeStrategies.explicit(outputDataType))
        .build()
    }
  }

  def generateCollector(
      ctx: CodeGeneratorContext,
      inputRowType: RowType,
      rightRowType: RowType,
      resultRowType: RowType,
      condition: Option[RexNode],
      pojoFieldMapping: Option[Array[Int]],
      retainHeader: Boolean = true): GeneratedCollector[ListenableCollector[RowData]] = {
    FunctionCallCodeGenerator.generateCollector(
      ctx,
      inputRowType,
      rightRowType,
      resultRowType,
      condition,
      pojoFieldMapping,
      retainHeader)
  }

  /**
   * Generates a [[TableFunctionResultFuture]] that can be passed to Java compiler.
   *
   * @param tableConfig
   *   The TableConfig
   * @param name
   *   Class name of the table function collector. Must not be unique but has to be a valid Java
   *   class identifier.
   * @param leftInputType
   *   The type information of the element being collected
   * @param collectedType
   *   The type information of the element collected by the collector
   * @param condition
   *   The filter condition before collect elements
   * @return
   *   instance of GeneratedCollector
   */
  def generateTableAsyncCollector(
      tableConfig: ReadableConfig,
      classLoader: ClassLoader,
      name: String,
      leftInputType: RowType,
      collectedType: RowType,
      condition: Option[RexNode]): GeneratedResultFuture[TableFunctionResultFuture[RowData]] = {

    val ctx = new CodeGeneratorContext(tableConfig, classLoader)
    val funcName = newName(ctx, name)
    val input1TypeClass = boxedTypeTermForType(leftInputType)
    val input2TypeClass = boxedTypeTermForType(collectedType)
    val input1Term = DEFAULT_INPUT1_TERM
    val input2Term = DEFAULT_INPUT2_TERM
    val outTerm = "resultCollection"

    val body = if (condition.isEmpty) {
      "getResultFuture().complete(records);"
    } else {
      val filterGenerator = new ExprCodeGenerator(ctx, nullableInput = false)
        .bindInput(leftInputType, input1Term)
        .bindSecondInput(collectedType, input2Term)
      val filterCondition = filterGenerator.generateExpression(condition.get)

      s"""
         |if (records == null || records.size() == 0) {
         |  getResultFuture().complete(java.util.Collections.emptyList());
         |  return;
         |}
         |try {
         |  $input1TypeClass $input1Term = ($input1TypeClass) getInput();
         |  $ARRAY_LIST $outTerm = new $ARRAY_LIST();
         |  for (Object record : records) {
         |    $input2TypeClass $input2Term = ($input2TypeClass) record;
         |    ${ctx.reuseLocalVariableCode()}
         |    ${ctx.reuseInputUnboxingCode()}
         |    ${ctx.reusePerRecordCode()}
         |    ${filterCondition.code}
         |    if (${filterCondition.resultTerm}) {
         |      $outTerm.add(record);
         |    }
         |  }
         |  getResultFuture().complete($outTerm);
         |} catch (Exception e) {
         |  getResultFuture().completeExceptionally(e);
         |}
         |""".stripMargin
    }

    val funcCode =
      j"""
      public class $funcName extends ${classOf[TableFunctionResultFuture[_]].getCanonicalName} {

        ${ctx.reuseMemberCode()}

        public $funcName(Object[] references) throws Exception {
          ${ctx.reuseInitCode()}
        }

        @Override
        public void open(${className[OpenContext]} openContext) throws Exception {
          ${ctx.reuseOpenCode()}
        }

        @Override
        public void complete(java.util.Collection records) throws Exception {
          $body
        }

        public void close() throws Exception {
          ${ctx.reuseCloseCode()}
        }
      }
    """.stripMargin

    new GeneratedResultFuture(funcName, funcCode, ctx.references.toArray, ctx.tableConfig)
  }

  /**
   * Generates calculate flatmap function for temporal join which is used to projection/filter the
   * dimension table results
   */
  def generateCalcMapFunction(
      tableConfig: ReadableConfig,
      classLoader: ClassLoader,
      projection: Seq[RexNode],
      condition: RexNode,
      outputType: RelDataType,
      tableSourceRowType: RowType): GeneratedFunction[FlatMapFunction[RowData, RowData]] = {
    generateCalcMapFunction(
      tableConfig,
      classLoader,
      projection,
      condition,
      FlinkTypeFactory.toLogicalRowType(outputType),
      tableSourceRowType
    )
  }

  /**
   * Generates calculate flatmap function for temporal join which is used to projection/filter the
   * dimension table results
   */
  def generateCalcMapFunction(
      tableConfig: ReadableConfig,
      classLoader: ClassLoader,
      projection: Seq[RexNode],
      condition: RexNode,
      outputType: RowType,
      tableSourceRowType: RowType): GeneratedFunction[FlatMapFunction[RowData, RowData]] = {
    CalcCodeGenerator.generateFunction(
      tableSourceRowType,
      "TableCalcMapFunction",
      outputType,
      classOf[GenericRowData],
      projection,
      Option(condition),
      tableConfig,
      classLoader
    )
  }
}
