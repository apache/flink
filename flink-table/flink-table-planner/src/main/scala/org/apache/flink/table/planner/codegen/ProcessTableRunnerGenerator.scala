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
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.functions.{FunctionDefinition, ProcessTableFunction, UserDefinedFunction, UserDefinedFunctionHelper}
import org.apache.flink.table.functions.UserDefinedFunctionHelper.{validateClassForRuntime, PROCESS_TABLE_EVAL}
import org.apache.flink.table.planner.calcite.{FlinkTypeFactory, RexTableArgCall}
import org.apache.flink.table.planner.codegen.CodeGenUtils.{className, newName, ROW_DATA}
import org.apache.flink.table.planner.codegen.GeneratedExpression.NO_CODE
import org.apache.flink.table.planner.codegen.Indenter.toISC
import org.apache.flink.table.planner.codegen.calls.BridgingFunctionGenUtil
import org.apache.flink.table.planner.codegen.calls.BridgingFunctionGenUtil.{verifyFunctionAwareOutputType, DefaultExpressionEvaluatorFactory}
import org.apache.flink.table.planner.delegation.PlannerBase
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction
import org.apache.flink.table.planner.functions.inference.OperatorBindingCallContext
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toScala
import org.apache.flink.table.runtime.generated.{GeneratedProcessTableRunner, ProcessTableRunner}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.inference.{StaticArgument, StaticArgumentTrait, SystemTypeInference, TypeInferenceUtil}

import org.apache.calcite.rex.{RexCall, RexCallBinding, RexNode}

import java.util
import java.util.Collections

import scala.collection.JavaConverters._

/** Code generator for [[ProcessTableRunner]]. */
object ProcessTableRunnerGenerator {

  def generate(
      ctx: CodeGeneratorContext,
      invocation: RexCall,
      inputChangelogModes: java.util.List[ChangelogMode]): GeneratedProcessTableRunner = {
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

    // Derive state type with conversion class
    val stateStrategies = inference.getStateTypeStrategies
    if (!stateStrategies.isEmpty) {
      throw new TableException("State is not supported for PTFs yet.")
    }

    // Enrich output types with conversion class
    val enrichedOutputDataType =
      TypeInferenceUtil.inferOutputType(adaptedCallContext, inference.getOutputTypeStrategy)
    verifyFunctionAwareOutputType(returnType, enrichedOutputDataType, udf)

    // Find and verify suitable runtime method
    val withContext = verifyEvalImplementation(enrichedArgumentDataTypes, udf, functionName)

    val tablesTerm = "tables"
    val contextTerm = if (withContext) {
      Some("runnerContext")
    } else {
      None
    }

    // Generate function call with operands (table/scalar arguments and state)
    val operands = generateOperands(ctx, call, tablesTerm)
    val functionCall = BridgingFunctionGenUtil.generateFunctionAwareCall(
      ctx,
      operands,
      enrichedArgumentDataTypes,
      enrichedOutputDataType,
      returnType,
      udf,
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
          extends ${classOf[ProcessTableRunner].getCanonicalName} {

        private transient $ROW_DATA[] $tablesTerm;

        ${ctx.reuseMemberCode()}

        public $name(Object[] references) throws Exception {
          ${ctx.reuseInitCode()}
        }

        @Override
        public void open(${classOf[OpenContext].getCanonicalName} openContext) throws Exception {
          ${ctx.reuseOpenCode()}
          $tablesTerm = new $ROW_DATA[${inputChangelogModes.size()}];
        }

        @Override
        public void processElement(int inputIndex, $ROW_DATA row) throws Exception {
          ${className[util.Arrays]}.fill($tablesTerm, null);
          $tablesTerm[inputIndex] = row;
          ${ctx.reusePerRecordCode()}
          ${ctx.reuseLocalVariableCode()}
          ${ctx.reuseInputUnboxingCode()}
          ${functionCall.code}
        }

        @Override
        public void close() throws Exception {
          ${ctx.reuseCloseCode()}
        }

        ${ctx.reuseInnerClassDefinitionCode()}
      }
    """.stripMargin

    new GeneratedProcessTableRunner(name, code, ctx.references.toArray, ctx.tableConfig)
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

  private def generateOperands(
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
