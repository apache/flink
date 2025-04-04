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

import org.apache.flink.api.common.functions.OpenContext
import org.apache.flink.api.common.state.{ListState, MapState}
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.api.dataview.{DataView, ListView, MapView}
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.data.RowData
import org.apache.flink.table.data.conversion.RowRowConverter
import org.apache.flink.table.functions.{FunctionDefinition, ProcessTableFunction, UserDefinedFunction, UserDefinedFunctionHelper}
import org.apache.flink.table.functions.UserDefinedFunctionHelper.{validateClassForRuntime, PROCESS_TABLE_EVAL, PROCESS_TABLE_ON_TIMER}
import org.apache.flink.table.planner.calcite.{FlinkTypeFactory, RexTableArgCall}
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.codegen.GeneratedExpression.{NEVER_NULL, NO_CODE}
import org.apache.flink.table.planner.codegen.Indenter.toISC
import org.apache.flink.table.planner.codegen.calls.BridgingFunctionGenUtil
import org.apache.flink.table.planner.codegen.calls.BridgingFunctionGenUtil.{verifyFunctionAwareOutputType, DefaultExpressionEvaluatorFactory}
import org.apache.flink.table.planner.delegation.PlannerBase
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction
import org.apache.flink.table.planner.functions.inference.OperatorBindingCallContext
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toScala
import org.apache.flink.table.runtime.dataview.DataViewUtils
import org.apache.flink.table.runtime.dataview.StateListView.KeyedStateListView
import org.apache.flink.table.runtime.dataview.StateMapView.KeyedStateMapViewWithKeysNotNull
import org.apache.flink.table.runtime.generated.{GeneratedProcessTableRunner, ProcessTableRunner}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.extraction.ExtractionUtils
import org.apache.flink.table.types.inference.{StaticArgument, StaticArgumentTrait, SystemTypeInference, TypeInferenceUtil}
import org.apache.flink.table.types.inference.TypeInferenceUtil.StateInfo
import org.apache.flink.table.types.logical.LogicalType
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks
import org.apache.flink.types.Row

import org.apache.calcite.rex.{RexCall, RexCallBinding, RexNode, RexUtil}
import org.apache.calcite.sql.SqlKind

import java.util
import java.util.Collections

import scala.collection.JavaConverters._

/** Code generator for [[ProcessTableRunner]]. */
object ProcessTableRunnerGenerator {

  case class GeneratedRunnerResult(
      runner: GeneratedProcessTableRunner,
      stateInfos: util.LinkedHashMap[String, StateInfo])

  def generate(
      ctx: CodeGeneratorContext,
      invocation: RexCall,
      inputTimeColumns: java.util.List[Integer],
      inputChangelogModes: java.util.List[ChangelogMode]): GeneratedRunnerResult = {
    val function: BridgingSqlFunction = invocation.getOperator.asInstanceOf[BridgingSqlFunction]
    val definition: FunctionDefinition = function.getDefinition
    val dataTypeFactory = function.getDataTypeFactory
    val typeFactory = function.getTypeFactory
    val rexFactory = function.getRexFactory
    val functionName = function.getName

    val call = removeSystemTypeInference(
      invocation,
      function.getTypeInference.getStaticArguments.get().asScala,
      typeFactory)
    val returnType = FlinkTypeFactory.toLogicalType(call.getType)

    // For specialized functions, this call context is able to provide the final input changelog modes.
    // Thus, functions can reconfigure themselves for the exact use case.
    // Including updating their state layout.
    val callContext = new OperatorBindingCallContext(
      dataTypeFactory,
      definition,
      RexCallBinding.create(typeFactory, call, Collections.emptyList()),
      call.getType,
      inputTimeColumns,
      inputChangelogModes)

    // Create the final UDF for runtime
    val udf = UserDefinedFunctionHelper.createSpecializedFunction(
      functionName,
      definition,
      callContext,
      classOf[PlannerBase].getClassLoader,
      ctx.tableConfig,
      new DefaultExpressionEvaluatorFactory(ctx.tableConfig, ctx.classLoader, rexFactory)
    )
    val functionTerm = ctx.addReusableFunction(udf)
    val inference = udf.getTypeInference(dataTypeFactory)
    val castCallContext = TypeInferenceUtil.castArguments(inference, callContext, null)

    // Enrich argument types with conversion class
    val enrichedArgumentDataTypes = toScala(castCallContext.getArgumentDataTypes)

    // Enrich output types with conversion class
    val enrichedOutputDataType =
      TypeInferenceUtil.inferOutputType(castCallContext, inference.getOutputTypeStrategy)
    verifyFunctionAwareOutputType(returnType, enrichedOutputDataType, udf)

    // Derive state type with conversion class.
    // This happens after specialization such that the state entries can be highly
    // specialized to the call.
    val stateInfos =
      TypeInferenceUtil.inferStateInfos(castCallContext, inference.getStateTypeStrategies)
    val stateDataTypes = stateInfos.asScala.values.map(_.getDataType).toSeq
    stateDataTypes.foreach(ExtractionUtils.checkStateDataType)

    val stateHandlesTerm = "stateHandles"
    val valueStateToFunctionTerm = "valueStateToFunction"
    val stateClearedTerm = "stateCleared"
    val valueStateFromFunctionTerm = "valueStateFromFunction"
    val stateEntries = stateInfos.asScala.values.zipWithIndex.toSeq
    val externalStateOperands =
      generateStateToFunction(ctx, stateEntries, stateHandlesTerm, valueStateToFunctionTerm)
    val stateFromFunctionCode = generateStateFromFunction(
      ctx,
      stateEntries,
      externalStateOperands,
      stateClearedTerm,
      valueStateFromFunctionTerm)

    // Generate result collector
    val resultCollectorTerm =
      BridgingFunctionGenUtil.generateResultCollector(ctx, enrichedOutputDataType, returnType)
    val setCollectorCode = s"$functionTerm.setCollector($resultCollectorTerm);"
    ctx.addReusableOpenStatement(setCollectorCode)

    // Generate call to eval()
    val evalCallCode = generateEvalCode(
      ctx,
      call,
      enrichedArgumentDataTypes,
      externalStateOperands,
      stateDataTypes,
      udf,
      functionName,
      functionTerm,
      resultCollectorTerm,
      stateFromFunctionCode)

    // Generate call to onTimer()
    val onTimerCallCode = generateOnTimerCode(
      externalStateOperands,
      stateDataTypes,
      udf,
      functionName,
      functionTerm,
      resultCollectorTerm,
      stateFromFunctionCode)

    // Generate runner
    val name = newName(ctx, "ProcessTableRunner")
    val code =
      j"""
      public final class $name
          extends ${className[ProcessTableRunner]} {

        ${ctx.reuseMemberCode()}

        public $name(Object[] references) throws Exception {
          ${ctx.reuseInitCode()}
        }

        @Override
        public void open(${className[OpenContext]} openContext) throws Exception {
          ${ctx.reuseOpenCode()}
        }

        @Override
        public void callEval() throws Exception {
          ${ctx.reuseLocalVariableCode()}
          $evalCallCode
          $stateFromFunctionCode
        }

        @Override
        public void callOnTimer() throws Exception {
          ${ctx.reuseLocalVariableCode()}
          $onTimerCallCode
        }

        @Override
        public void close() throws Exception {
          ${ctx.reuseCloseCode()}
        }

        ${ctx.reuseInnerClassDefinitionCode()}
      }
    """.stripMargin

    val generatedRunner =
      new GeneratedProcessTableRunner(name, code, ctx.references.toArray, ctx.tableConfig)

    GeneratedRunnerResult(generatedRunner, stateInfos)
  }

  private def removeSystemTypeInference(
      invocation: RexCall,
      staticArgs: Seq[StaticArgument],
      typeFactory: FlinkTypeFactory): RexCall = {
    val operands = invocation.getOperands.asScala
    // Remove system arguments
    val newOperands = operands
      .dropRight(SystemTypeInference.PROCESS_TABLE_FUNCTION_SYSTEM_ARGS.size())
    // Remove system output columns
    val prefixOutputSystemFields = newOperands.zipWithIndex.map {
      case (tableArgCall: RexTableArgCall, pos) =>
        val staticArg = staticArgs(pos)
        if (staticArg.is(StaticArgumentTrait.PASS_COLUMNS_THROUGH)) {
          tableArgCall.getType.getFieldCount
        } else {
          tableArgCall.getPartitionKeys.length
        }
      case _ => 0
    }.sum
    val onTimeArg = operands(
      operands.size - 1 - SystemTypeInference.PROCESS_TABLE_FUNCTION_ARG_ON_TIME_OFFSET)
    val suffixOutputSystemFields =
      if (onTimeArg.getKind == SqlKind.DEFAULT || RexUtil.isNullLiteral(onTimeArg, true)) {
        0
      } else {
        1
      }
    val projectedFields = invocation.getType.getFieldList.asScala
      .drop(prefixOutputSystemFields)
      .dropRight(suffixOutputSystemFields)
    val newReturnType = typeFactory.createStructType(projectedFields.asJava)
    invocation.clone(newReturnType, newOperands.asJava)
  }

  private def generateStateToFunction(
      ctx: CodeGeneratorContext,
      stateEntries: Seq[(StateInfo, Int)],
      stateHandlesTerm: String,
      valueStateToFunctionTerm: String): Seq[GeneratedExpression] = {
    stateEntries.map {
      case (stateInfo, pos) =>
        val stateDataType = stateInfo.getDataType
        val stateType = stateDataType.getLogicalType
        val externalStateTypeTerm = typeTerm(stateDataType.getConversionClass)
        val externalStateTerm = newName(ctx, "externalState")

        val externalStateCode = if (DataViewUtils.isDataView(stateType, classOf[DataView])) {
          generateDataViewStateToFunction(
            ctx,
            stateHandlesTerm,
            pos,
            stateType,
            externalStateTypeTerm,
            externalStateTerm)
          NO_CODE
        } else {
          DataViewUtils.checkForInvalidDataViews(stateType)
          generateValueStateToFunction(
            ctx,
            valueStateToFunctionTerm,
            pos,
            externalStateTypeTerm,
            externalStateTerm,
            stateDataType)
        }

        GeneratedExpression(s"$externalStateTerm", NEVER_NULL, externalStateCode, stateType)
    }
  }

  private def generateDataViewStateToFunction(
      ctx: CodeGeneratorContext,
      stateHandlesTerm: String,
      pos: Int,
      stateType: LogicalType,
      externalStateTypeTerm: String,
      externalStateTerm: String): Unit = {
    ctx.addReusableMember(s"private $externalStateTypeTerm $externalStateTerm;")

    val (constructor, stateHandleTypeTerm) =
      if (DataViewUtils.isDataView(stateType, classOf[ListView[_]])) {
        (className[KeyedStateListView[_, _]], className[ListState[_]])
      } else if (DataViewUtils.isDataView(stateType, classOf[MapView[_, _]])) {
        (className[KeyedStateMapViewWithKeysNotNull[_, _, _]], className[MapState[_, _]])
      }

    val openCode =
      s"""
         |$externalStateTerm = new $constructor(($stateHandleTypeTerm) $stateHandlesTerm[$pos]);
         """.stripMargin
    ctx.addReusableOpenStatement(openCode)
  }

  private def generateValueStateToFunction(
      ctx: CodeGeneratorContext,
      valueStateToFunctionTerm: String,
      pos: Int,
      externalStateTypeTerm: String,
      externalStateTerm: String,
      stateDataType: DataType): String = {
    val stateEntryTerm = s"$valueStateToFunctionTerm[$pos]"
    val converterCode = genToExternalConverter(ctx, stateDataType, stateEntryTerm)

    val constructorCode = stateDataType.getConversionClass match {
      case rowType if rowType == classOf[Row] =>
        // This allows us to retrieve the converter term that has been generated
        // in genToExternalConverter(). The converter is able to create named positions
        // for row fields.
        val converterTerm = ctx.addReusableConverter(stateDataType)
        s"((${className[RowRowConverter]}) $converterTerm).createEmptyRow()"
      case rowType if rowType == classOf[RowData] =>
        val fieldCount = LogicalTypeChecks.getFieldCount(stateDataType.getLogicalType)
        s"new $GENERIC_ROW($fieldCount)"
      case structuredType @ _ => s"new ${className(structuredType)}()"
    }

    s"""
       |final $externalStateTypeTerm $externalStateTerm;
       |if ($stateEntryTerm == null) {
       |  $externalStateTerm = $constructorCode;
       |} else {
       |  $externalStateTerm = $converterCode;
       |}
       |""".stripMargin
  }

  private def generateStateFromFunction(
      ctx: CodeGeneratorContext,
      stateEntries: Seq[(StateInfo, Int)],
      externalStateOperands: Seq[GeneratedExpression],
      stateClearedTerm: String,
      stateFromFunctionTerm: String): String = {
    stateEntries
      .map {
        case (stateInfo, pos) =>
          val stateDataType = stateInfo.getDataType
          val stateType = stateDataType.getLogicalType

          if (DataViewUtils.isDataView(stateType, classOf[DataView])) {
            NO_CODE
          } else {
            val stateEntryTerm = s"$stateFromFunctionTerm[$pos]"
            val externalStateOperandTerm = externalStateOperands(pos).resultTerm
            s"$stateEntryTerm = $stateClearedTerm[$pos] ? null : " +
              s"${genToInternalConverter(ctx, stateDataType)(externalStateOperandTerm)};"
          }
      }
      .filter(c => c != NO_CODE)
      .mkString("\n")
  }

  private def generateEvalOperands(
      ctx: CodeGeneratorContext,
      call: RexCall,
      inputIndexTerm: String,
      inputRowTerm: String): Seq[GeneratedExpression] = {
    val generator = new ExprCodeGenerator(ctx, false)
    call.getOperands.asScala
      .map {
        case tableArgCall: RexTableArgCall =>
          val inputIndex = tableArgCall.getInputIndex
          val tableType = FlinkTypeFactory.toLogicalType(call.getType).copy(true)
          GeneratedExpression(
            s"$inputRowTerm",
            s"$inputIndexTerm != $inputIndex",
            NO_CODE,
            tableType)
        case rexNode: RexNode =>
          generator.generateExpression(rexNode)
      }
  }

  private def verifyMethodImplementation(
      methodName: String,
      contextClass: Class[_],
      parameterDataTypes: Seq[DataType],
      udf: UserDefinedFunction,
      functionName: String,
      optional: Boolean): Option[Boolean] = {
    if (optional && ExtractionUtils.collectMethods(udf.getClass, methodName).isEmpty) {
      return None
    }
    val argumentClasses = parameterDataTypes.map(_.getConversionClass)
    try {
      // Try with context first
      val fullSignature = contextClass +: argumentClasses
      validateClassForRuntime(
        udf.getClass,
        methodName,
        fullSignature.toArray,
        classOf[Unit],
        functionName)
      Some(true)
    } catch {
      case _: ValidationException =>
        // Try without context also to not force people to use a context
        val minimalSignature = argumentClasses
        validateClassForRuntime(
          udf.getClass,
          methodName,
          minimalSignature.toArray,
          classOf[Unit],
          functionName)
        Some(false)
    }
  }

  private def generateEvalCode(
      ctx: CodeGeneratorContext,
      call: RexCall,
      enrichedArgumentDataTypes: Seq[DataType],
      externalStateOperands: Seq[GeneratedExpression],
      stateDataTypes: Seq[DataType],
      udf: UserDefinedFunction,
      functionName: String,
      functionTerm: String,
      resultCollectorTerm: String,
      stateFromFunctionCode: String): String = {
    val inputIndexTerm = "inputIndex"
    val inputRowTerm = "inputRow"
    val collectorTerm = "evalCollector"
    val callOperands = generateEvalOperands(ctx, call, inputIndexTerm, inputRowTerm)
    val externalCallOperands =
      BridgingFunctionGenUtil.prepareExternalOperands(ctx, callOperands, enrichedArgumentDataTypes)
    val allExternalOperands = externalStateOperands ++ externalCallOperands
    val allDataTypes = stateDataTypes ++ enrichedArgumentDataTypes

    // Find and verify suitable runtime method
    val withContext = verifyMethodImplementation(
      PROCESS_TABLE_EVAL,
      classOf[ProcessTableFunction.Context],
      allDataTypes,
      udf,
      functionName,
      optional = false)
    val contextOperand = if (withContext.get) {
      Seq("runnerContext")
    } else {
      Seq()
    }

    val methodOperands = contextOperand ++ allExternalOperands.map(_.resultTerm)

    val functionCallCode =
      s"""
         |$resultCollectorTerm.setCollector($collectorTerm);
         |${allExternalOperands.map(_.code).mkString("\n")}
         |$functionTerm.eval(${methodOperands.mkString(", ")});
         |$stateFromFunctionCode
         |""".stripMargin

    functionCallCode
  }

  private def generateOnTimerCode(
      externalStateOperands: Seq[GeneratedExpression],
      stateDataTypes: Seq[DataType],
      udf: UserDefinedFunction,
      functionName: String,
      functionTerm: String,
      resultCollectorTerm: String,
      stateFromFunctionCode: String): String = {
    val collectorTerm = "onTimerCollector"

    // Find and verify suitable runtime method
    val withContext = verifyMethodImplementation(
      PROCESS_TABLE_ON_TIMER,
      classOf[ProcessTableFunction.OnTimerContext],
      stateDataTypes,
      udf,
      functionName,
      optional = true)
    if (withContext.isEmpty) {
      return ""
    }
    val contextOperand = if (withContext.get) {
      Seq("runnerOnTimerContext")
    } else {
      Seq()
    }

    val methodOperands = contextOperand ++ externalStateOperands.map(_.resultTerm)

    val functionCallCode =
      s"""
         |$resultCollectorTerm.setCollector($collectorTerm);
         |${externalStateOperands.map(_.code).mkString("\n")}
         |$functionTerm.onTimer(${methodOperands.mkString(", ")});
         |$stateFromFunctionCode
         |""".stripMargin

    functionCallCode
  }
}
