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

import org.apache.flink.api.common.functions.{FlatMapFunction, Function}
import org.apache.flink.configuration.{Configuration, ReadableConfig}
import org.apache.flink.streaming.api.functions.async.AsyncFunction
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.catalog.DataTypeFactory
import org.apache.flink.table.connector.source.{LookupTableSource, ScanTableSource}
import org.apache.flink.table.data.{GenericRowData, RowData}
import org.apache.flink.table.data.utils.JoinedRowData
import org.apache.flink.table.functions.{AsyncLookupFunction, AsyncTableFunction, LookupFunction, TableFunction, UserDefinedFunction, UserDefinedFunctionHelper}
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.codegen.GenerateUtils._
import org.apache.flink.table.planner.codegen.Indenter.toISC
import org.apache.flink.table.planner.codegen.calls.BridgingFunctionGenUtil
import org.apache.flink.table.planner.codegen.calls.BridgingFunctionGenUtil.verifyFunctionAwareImplementation
import org.apache.flink.table.planner.delegation.PlannerBase
import org.apache.flink.table.planner.functions.inference.LookupCallContext
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil.{ConstantLookupKey, FieldRefLookupKey, LookupKey}
import org.apache.flink.table.planner.plan.utils.RexLiteralUtil
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toScala
import org.apache.flink.table.runtime.collector.{ListenableCollector, TableFunctionResultFuture}
import org.apache.flink.table.runtime.collector.ListenableCollector.CollectListener
import org.apache.flink.table.runtime.generated.{GeneratedCollector, GeneratedFilterCondition, GeneratedFunction, GeneratedResultFuture}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.extraction.ExtractionUtils.extractSimpleGeneric
import org.apache.flink.table.types.inference.{TypeInference, TypeStrategies, TypeTransformations}
import org.apache.flink.table.types.logical.{LogicalType, RowType}
import org.apache.flink.table.types.utils.DataTypeUtils.transform
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

import java.util

import scala.collection.JavaConverters._

object LookupJoinCodeGenerator {

  case class GeneratedTableFunctionWithDataType[F <: Function](
      tableFunc: GeneratedFunction[F],
      dataType: DataType)

  private val ARRAY_LIST = className[util.ArrayList[_]]

  /** Generates a lookup function ([[TableFunction]]) */
  def generateSyncLookupFunction(
      tableConfig: ReadableConfig,
      classLoader: ClassLoader,
      dataTypeFactory: DataTypeFactory,
      inputType: LogicalType,
      tableSourceType: LogicalType,
      returnType: LogicalType,
      lookupKeys: util.Map[Integer, LookupKey],
      lookupKeyOrder: Array[Int],
      syncLookupFunction: TableFunction[_],
      functionName: String,
      fieldCopy: Boolean): GeneratedFunction[FlatMapFunction[RowData, RowData]] = {

    val bodyCode: GeneratedExpression => String = call => {
      val resultCollectorTerm = call.resultTerm
      s"""
         |$resultCollectorTerm.setCollector($DEFAULT_COLLECTOR_TERM);
         |${call.code}
         |""".stripMargin
    }

    generateLookupFunction(
      classOf[FlatMapFunction[RowData, RowData]],
      tableConfig,
      classLoader,
      dataTypeFactory,
      inputType,
      tableSourceType,
      returnType,
      lookupKeys,
      lookupKeyOrder,
      classOf[TableFunction[_]],
      syncLookupFunction,
      functionName,
      fieldCopy,
      bodyCode
    ).tableFunc
  }

  /** Generates a async lookup function ([[AsyncTableFunction]]) */
  def generateAsyncLookupFunction(
      tableConfig: ReadableConfig,
      classLoader: ClassLoader,
      dataTypeFactory: DataTypeFactory,
      inputType: LogicalType,
      tableSourceType: LogicalType,
      returnType: LogicalType,
      lookupKeys: util.Map[Integer, LookupKey],
      lookupKeyOrder: Array[Int],
      asyncLookupFunction: AsyncTableFunction[_],
      functionName: String): GeneratedTableFunctionWithDataType[AsyncFunction[RowData, AnyRef]] = {

    generateLookupFunction(
      classOf[AsyncFunction[RowData, AnyRef]],
      tableConfig,
      classLoader,
      dataTypeFactory,
      inputType,
      tableSourceType,
      returnType,
      lookupKeys,
      lookupKeyOrder,
      classOf[AsyncTableFunction[_]],
      asyncLookupFunction,
      functionName,
      fieldCopy = true, // always copy input field because of async buffer
      _.code
    )
  }

  private def generateLookupFunction[F <: Function](
      generatedClass: Class[F],
      tableConfig: ReadableConfig,
      classLoader: ClassLoader,
      dataTypeFactory: DataTypeFactory,
      inputType: LogicalType,
      tableSourceType: LogicalType,
      returnType: LogicalType,
      lookupKeys: util.Map[Integer, LookupKey],
      lookupKeyOrder: Array[Int],
      lookupFunctionBase: Class[_],
      lookupFunction: UserDefinedFunction,
      functionName: String,
      fieldCopy: Boolean,
      bodyCode: GeneratedExpression => String): GeneratedTableFunctionWithDataType[F] = {

    val callContext = new LookupCallContext(
      dataTypeFactory,
      lookupFunction,
      inputType,
      lookupKeys,
      lookupKeyOrder,
      tableSourceType)

    // create the final UDF for runtime
    val udf = UserDefinedFunctionHelper.createSpecializedFunction(
      functionName,
      lookupFunction,
      callContext,
      classOf[PlannerBase].getClassLoader,
      tableConfig,
      // no need to support expression evaluation at this point
      null)

    val inference =
      createLookupTypeInference(dataTypeFactory, callContext, lookupFunctionBase, udf, functionName)

    val ctx = new CodeGeneratorContext(tableConfig, classLoader)
    val operands = prepareOperands(ctx, inputType, lookupKeys, lookupKeyOrder, fieldCopy)
    val callWithDataType = BridgingFunctionGenUtil.generateFunctionAwareCallWithDataType(
      ctx,
      operands,
      tableSourceType,
      inference,
      callContext,
      udf,
      functionName,
      // TODO: filter all records when there is any nulls on the join key, because
      //  "IS NOT DISTINCT FROM" is not supported yet.
      skipIfArgsNull = true
    )

    val function = FunctionCodeGenerator.generateFunction(
      ctx,
      "LookupFunction",
      generatedClass,
      bodyCode(callWithDataType._1),
      returnType,
      inputType)

    GeneratedTableFunctionWithDataType(function, callWithDataType._2)
  }

  private def prepareOperands(
      ctx: CodeGeneratorContext,
      inputType: LogicalType,
      lookupKeys: util.Map[Integer, LookupKey],
      lookupKeyOrder: Array[Int],
      fieldCopy: Boolean): Seq[GeneratedExpression] = {

    lookupKeyOrder
      .map(Integer.valueOf)
      .map(lookupKeys.get)
      .map {
        case constantKey: ConstantLookupKey =>
          val res = RexLiteralUtil.toFlinkInternalValue(constantKey.literal)
          generateLiteral(ctx, res.f0, res.f1)
        case fieldKey: FieldRefLookupKey =>
          generateInputAccess(
            ctx,
            inputType,
            DEFAULT_INPUT1_TERM,
            fieldKey.index,
            nullableInput = false,
            fieldCopy)
        case _ =>
          throw new CodeGenException("Invalid lookup key.")
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
      callContext: LookupCallContext,
      baseClass: Class[_],
      udf: UserDefinedFunction,
      functionName: String): TypeInference = {

    try {
      // user provided type inference has precedence
      // this ensures that all functions work in the same way
      udf.getTypeInference(dataTypeFactory)
    } catch {
      case e: Exception =>
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

  /**
   * Generates collector for temporal join ([[Collector]])
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

    val joinedRowTerm = CodeGenUtils.newName("joinedRow")
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

    val funcName = newName(name)
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
        public void open(${className[Configuration]} parameters) throws Exception {
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
             ((${classOf[CollectListener[_]].getCanonicalName}) getCollectListener().get()).onCollect(record);
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

    val funcName = newName(name)
    val input1TypeClass = boxedTypeTermForType(leftInputType)
    val input2TypeClass = boxedTypeTermForType(collectedType)
    val input1Term = DEFAULT_INPUT1_TERM
    val input2Term = DEFAULT_INPUT2_TERM
    val outTerm = "resultCollection"

    val ctx = new CodeGeneratorContext(tableConfig, classLoader)

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
        public void open(${className[Configuration]} parameters) throws Exception {
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
    CalcCodeGenerator.generateFunction(
      tableSourceRowType,
      "TableCalcMapFunction",
      FlinkTypeFactory.toLogicalRowType(outputType),
      classOf[GenericRowData],
      projection,
      Option(condition),
      tableConfig,
      classLoader
    )
  }

  /**
   * Generates pre-filter condition for lookup join which can be applied before access the dimension
   * table.
   */
  def generatePreFilterCondition(
      tableConfig: ReadableConfig,
      classLoader: ClassLoader,
      preFilterCondition: RexNode,
      leftType: LogicalType): GeneratedFilterCondition = {
    val ctx = new CodeGeneratorContext(tableConfig, classLoader)
    // should consider null fields
    val exprGenerator =
      new ExprCodeGenerator(ctx, false).bindInput(leftType, CodeGenUtils.DEFAULT_INPUT_TERM)

    val bodyCode = if (preFilterCondition == null) {
      "return true;"
    } else {
      val condition = exprGenerator.generateExpression(preFilterCondition)
      s"""
         |${condition.code}
         |return ${condition.resultTerm};
         |""".stripMargin
    }

    FunctionCodeGenerator.generateFilterCondition(ctx, "PreFilterCondition", bodyCode)
  }
}
