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
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.data.RowData
import org.apache.flink.table.data.conversion.RowRowConverter
import org.apache.flink.table.functions.{FunctionDefinition, ProcessTableFunction, UserDefinedFunction, UserDefinedFunctionHelper}
import org.apache.flink.table.functions.UserDefinedFunctionHelper.{validateClassForRuntime, PROCESS_TABLE_EVAL}
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
import org.apache.flink.table.runtime.generated.{GeneratedProcessTableRunner, ProcessTableRunner}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.extraction.ExtractionUtils
import org.apache.flink.table.types.inference.{StaticArgument, StaticArgumentTrait, SystemTypeInference, TypeInferenceUtil}
import org.apache.flink.table.types.inference.TypeInferenceUtil.StateInfo
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks
import org.apache.flink.types.Row

import org.apache.calcite.rex.{RexCall, RexCallBinding, RexNode}

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
    val inference = udf.getTypeInference(dataTypeFactory)

    // Enrich argument types with conversion class
    val adaptedCallContext = TypeInferenceUtil.adaptArguments(inference, callContext, null)
    val enrichedArgumentDataTypes = toScala(adaptedCallContext.getArgumentDataTypes)

    // Enrich output types with conversion class
    val enrichedOutputDataType =
      TypeInferenceUtil.inferOutputType(adaptedCallContext, inference.getOutputTypeStrategy)
    verifyFunctionAwareOutputType(returnType, enrichedOutputDataType, udf)

    // Derive state type with conversion class.
    // This happens after specialization such that the state entries can be highly
    // specialized to the call.
    val stateInfos =
      TypeInferenceUtil.inferStateInfos(adaptedCallContext, inference.getStateTypeStrategies)
    val stateDataTypes = stateInfos.asScala.values.map(_.getDataType).toSeq
    stateDataTypes.foreach(ExtractionUtils.checkStateDataType)

    // Generate operands (state + args)
    val stateToFunctionTerm = "stateToFunction"
    val stateClearedTerm = "stateCleared"
    val stateFromFunctionTerm = "stateFromFunction"
    val externalStateOperands = generateStateToFunction(ctx, stateDataTypes, stateToFunctionTerm)
    val stateFromFunctionCode = generateStateFromFunction(
      ctx,
      stateDataTypes,
      externalStateOperands,
      stateClearedTerm,
      stateFromFunctionTerm)
    val tablesTerm = "tables"
    val callOperands = generateCallOperands(ctx, call, tablesTerm)
    val externalCallOperands =
      BridgingFunctionGenUtil.prepareExternalOperands(ctx, callOperands, enrichedArgumentDataTypes)
    val allExternalOperands = externalStateOperands ++ externalCallOperands
    val allDataTypes = stateDataTypes ++ enrichedArgumentDataTypes

    // Find and verify suitable runtime method

    val withContext = verifyEvalImplementation(allDataTypes, udf, functionName)
    val contextTerm = if (withContext) {
      Some("runnerContext")
    } else {
      None
    }

    // Generate function call with operands (table/scalar arguments and state)
    val functionTerm = ctx.addReusableFunction(udf)
    val functionCall = BridgingFunctionGenUtil.generateTableFunctionCall(
      ctx,
      functionTerm,
      allExternalOperands,
      enrichedOutputDataType,
      returnType,
      skipIfArgsNull = false,
      contextTerm)

    // Generate setting result collector
    val resultCollectorTerm = functionCall.resultTerm
    val setCollectors =
      s"""
         |$resultCollectorTerm.setCollector(runnerCollector);
         |""".stripMargin
    ctx.addReusableOpenStatement(setCollectors)

    // Generate runner
    val name = newName(ctx, "ProcessTableRunner")
    val code =
      j"""
      public final class $name
          extends ${className[ProcessTableRunner]} {

        private transient $ROW_DATA[] $tablesTerm;

        ${ctx.reuseMemberCode()}

        public $name(Object[] references) throws Exception {
          ${ctx.reuseInitCode()}
        }

        @Override
        public void open(${className[OpenContext]} openContext) throws Exception {
          ${ctx.reuseOpenCode()}
          $tablesTerm = new $ROW_DATA[${inputChangelogModes.size()}];
        }

        @Override
        public void processElement(
            $ROW_DATA[] $stateToFunctionTerm,
            boolean[] $stateClearedTerm,
            $ROW_DATA[] $stateFromFunctionTerm,
            int inputIndex,
            $ROW_DATA row) throws Exception {
          ${className[util.Arrays]}.fill($tablesTerm, null);
          $tablesTerm[inputIndex] = row;
          ${ctx.reusePerRecordCode()}
          ${ctx.reuseLocalVariableCode()}
          ${ctx.reuseInputUnboxingCode()}
          ${functionCall.code}
          ${stateFromFunctionCode.mkString("\n")}
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
    // Remove system arguments
    val newOperands = invocation.getOperands.asScala
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
    val projectedFields = invocation.getType.getFieldList.asScala.drop(prefixOutputSystemFields)
    val newReturnType = typeFactory.createStructType(projectedFields.asJava)
    invocation.clone(newReturnType, newOperands.asJava)
  }

  private def generateStateToFunction(
      ctx: CodeGeneratorContext,
      stateDataTypes: Seq[DataType],
      stateToFunctionTerm: String): Seq[GeneratedExpression] = {
    stateDataTypes.zipWithIndex
      .map {
        case (stateDataType, pos) =>
          val stateEntryTerm = s"$stateToFunctionTerm[$pos]"
          val externalStateTypeTerm = typeTerm(stateDataType.getConversionClass)
          val externalStateTerm = newName(ctx, "externalState")

          val converterCode = genToExternalConverter(ctx, stateDataType, stateEntryTerm)

          val constructorCode = stateDataType.getConversionClass match {
            case rowType if rowType == classOf[Row] =>
              // This allows us to retrieve the converter term that has been generated
              // in genToExternalConverter(). The converter is able to created named positions
              // for row fields.
              val converterTerm = ctx.addReusableConverter(stateDataType)
              s"((${className[RowRowConverter]}) $converterTerm).createEmptyRow()"
            case rowType if rowType == classOf[RowData] =>
              val fieldCount = LogicalTypeChecks.getFieldCount(stateDataType.getLogicalType)
              s"new $GENERIC_ROW($fieldCount)"
            case structuredType @ _ => s"new ${className(structuredType)}()"
          }

          val externalStateCode =
            s"""
               |final $externalStateTypeTerm $externalStateTerm;
               |if ($stateEntryTerm == null) {
               |  $externalStateTerm = $constructorCode;
               |} else {
               |  $externalStateTerm = $converterCode;
               |}
               |""".stripMargin

          GeneratedExpression(
            s"$externalStateTerm",
            NEVER_NULL,
            externalStateCode,
            stateDataType.getLogicalType)
      }
  }

  private def generateStateFromFunction(
      ctx: CodeGeneratorContext,
      stateDataTypes: Seq[DataType],
      externalStateOperands: Seq[GeneratedExpression],
      stateClearedTerm: String,
      stateFromFunctionTerm: String): Seq[String] = {
    stateDataTypes.zipWithIndex
      .map {
        case (stateDataType, pos) =>
          val stateEntryTerm = s"$stateFromFunctionTerm[$pos]"
          val externalStateOperandTerm = externalStateOperands(pos).resultTerm
          s"$stateEntryTerm = $stateClearedTerm[$pos] ? null : " +
            s"${genToInternalConverter(ctx, stateDataType)(externalStateOperandTerm)};"
      }
  }

  private def generateCallOperands(
      ctx: CodeGeneratorContext,
      call: RexCall,
      tablesTerm: String): Seq[GeneratedExpression] = {
    val generator = new ExprCodeGenerator(ctx, false)
    call.getOperands.asScala
      .map {
        case tableArgCall: RexTableArgCall =>
          val inputIndex = tableArgCall.getInputIndex
          val tableType = FlinkTypeFactory.toLogicalType(call.getType).copy(true)
          GeneratedExpression(
            s"$tablesTerm[$inputIndex]",
            s"$tablesTerm[$inputIndex] == null",
            NO_CODE,
            tableType)
        case rexNode: RexNode =>
          generator.generateExpression(rexNode)
      }
  }

  private def verifyEvalImplementation(
      argumentDataTypes: Seq[DataType],
      udf: UserDefinedFunction,
      functionName: String): Boolean = {
    val contextClass = classOf[ProcessTableFunction.Context]
    val argumentClasses = argumentDataTypes.map(_.getConversionClass)
    try {
      // Try with context first
      val fullSignature = contextClass +: argumentClasses
      validateClassForRuntime(
        udf.getClass,
        PROCESS_TABLE_EVAL,
        fullSignature.toArray,
        classOf[Unit],
        functionName)
      true
    } catch {
      case _: ValidationException =>
        // Try without context also to not force people to use a context
        val minimalSignature = argumentClasses
        validateClassForRuntime(
          udf.getClass,
          PROCESS_TABLE_EVAL,
          minimalSignature.toArray,
          classOf[Unit],
          functionName)
        false
    }
  }
}
