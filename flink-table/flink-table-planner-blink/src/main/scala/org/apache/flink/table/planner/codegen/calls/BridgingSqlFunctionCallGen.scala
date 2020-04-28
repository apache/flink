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

package org.apache.flink.table.planner.codegen.calls

import java.lang.reflect.Method
import java.util.Collections

import org.apache.calcite.rex.{RexCall, RexCallBinding}
import org.apache.flink.table.dataformat.GenericRow
import org.apache.flink.table.functions.UserDefinedFunctionHelper.{SCALAR_EVAL, TABLE_EVAL}
import org.apache.flink.table.functions.{FunctionKind, ScalarFunction, TableFunction, UserDefinedFunction}
import org.apache.flink.table.planner.codegen.CodeGenUtils.{genToExternalIfNeeded, genToInternalIfNeeded, newName, typeTerm}
import org.apache.flink.table.planner.codegen.GeneratedExpression.NEVER_NULL
import org.apache.flink.table.planner.codegen._
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction
import org.apache.flink.table.planner.functions.inference.OperatorBindingCallContext
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toScala
import org.apache.flink.table.runtime.collector.WrappingCollector
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.extraction.utils.ExtractionUtils
import org.apache.flink.table.types.extraction.utils.ExtractionUtils.{createMethodSignatureString, isAssignable, isMethodInvokable, primitiveToWrapper}
import org.apache.flink.table.types.inference.TypeInferenceUtil
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsAvoidingCast
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks.{hasRoot, isCompositeType}
import org.apache.flink.table.types.logical.{LogicalType, LogicalTypeRoot, RowType}
import org.apache.flink.util.Preconditions

/**
 * Generates a call to a user-defined [[ScalarFunction]] or [[TableFunction]].
 *
 * Table functions are a special case because they are using a collector. Thus, the result of this
 * generator will be a reference to a [[WrappingCollector]]. Furthermore, atomic types are wrapped
 * into a row by the collector.
 */
class BridgingSqlFunctionCallGen(call: RexCall) extends CallGenerator {

  private val function: BridgingSqlFunction = call.getOperator.asInstanceOf[BridgingSqlFunction]
  private val udf: UserDefinedFunction = function.getDefinition.asInstanceOf[UserDefinedFunction]

  override def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType)
    : GeneratedExpression = {

    val inference = function.getTypeInference

    // we could have implemented a dedicated code generation context but the closer we are to
    // Calcite the more consistent is the type inference during the data type enrichment
    val callContext = new OperatorBindingCallContext(
      function.getDataTypeFactory,
      udf,
      RexCallBinding.create(
        function.getTypeFactory,
        call,
        Collections.emptyList()))

    // enrich argument types with conversion class
    val adaptedCallContext = TypeInferenceUtil.adaptArguments(
      inference,
      callContext,
      null)
    val enrichedArgumentDataTypes = toScala(adaptedCallContext.getArgumentDataTypes)
    verifyArgumentTypes(operands.map(_.resultType), enrichedArgumentDataTypes)

    // enrich output types with conversion class
    val enrichedOutputDataType = TypeInferenceUtil.inferOutputType(
      adaptedCallContext,
      inference.getOutputTypeStrategy)
    verifyFunctionAwareOutputType(returnType, enrichedOutputDataType)

    // find runtime method and generate call
    verifyFunctionAwareImplementation(enrichedArgumentDataTypes, enrichedOutputDataType)
    generateFunctionAwareCall(
      ctx,
      operands,
      enrichedArgumentDataTypes,
      enrichedOutputDataType,
      returnType)
  }

  private def generateFunctionAwareCall(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      argumentDataTypes: Seq[DataType],
      outputDataType: DataType,
      returnType: LogicalType)
    : GeneratedExpression = {

    val functionTerm = ctx.addReusableFunction(udf)

    // operand conversion
    val externalOperands = prepareExternalOperands(ctx, operands, argumentDataTypes)

    if (function.getDefinition.getKind == FunctionKind.TABLE) {
      Preconditions.checkState(
        hasRoot(returnType, LogicalTypeRoot.ROW),
        "Logical output type of function call should be a ROW type.",
        Seq(): _*)
      generateTableFunctionCall(
        ctx,
        functionTerm,
        externalOperands,
        outputDataType,
        returnType.asInstanceOf[RowType])
    } else {
      generateScalarFunctionCall(ctx, functionTerm, externalOperands, outputDataType)
    }
  }

  private def generateTableFunctionCall(
      ctx: CodeGeneratorContext,
      functionTerm: String,
      externalOperands: Seq[GeneratedExpression],
      functionOutputDataType: DataType,
      outputType: RowType)
    : GeneratedExpression = {
    val resultCollectorTerm = generateResultCollector(ctx, functionOutputDataType, outputType)

    val setCollectorCode = s"""
      |$functionTerm.setCollector($resultCollectorTerm);
      |""".stripMargin
    ctx.addReusableOpenStatement(setCollectorCode)

    // function call
    val functionCallCode =
      s"""
        |${externalOperands.map(_.code).mkString("\n")}
        |$functionTerm.eval(${externalOperands.map(_.resultTerm).mkString(", ")});
        |""".stripMargin

    // has no result
    GeneratedExpression(
      resultCollectorTerm,
      NEVER_NULL,
      functionCallCode,
      outputType)
  }

  /**
   * Generates a collector that converts the output of a table function (possibly as an atomic type)
   * into an internal row type. Returns a collector term for referencing the collector.
   */
  def generateResultCollector(
      ctx: CodeGeneratorContext,
      outputDataType: DataType,
      returnType: RowType)
    : String = {
    val outputType = outputDataType.getLogicalType

    val collectorCtx = CodeGeneratorContext(ctx.tableConfig)
    val externalResultTerm = newName("externalResult")

    // code for wrapping atomic types
    val collectorCode = if (!isCompositeType(outputType)) {
      val resultGenerator = new ExprCodeGenerator(collectorCtx, outputType.isNullable)
        .bindInput(outputType, externalResultTerm)
      val wrappedResult = resultGenerator.generateConverterResultExpression(
        returnType,
        classOf[GenericRow])
      s"""
       |${wrappedResult.code}
       |outputResult(${wrappedResult.resultTerm});
       |""".stripMargin
    } else {
      s"""
        |if ($externalResultTerm != null) {
        |  outputResult($externalResultTerm);
        |}
        |""".stripMargin
    }

    // collector for converting to internal types then wrapping atomic types
    val resultCollector = CollectorCodeGenerator.generateWrappingCollector(
      collectorCtx,
      "TableFunctionResultConverterCollector",
      outputType,
      externalResultTerm,
      // nullability is handled by the expression code generator if necessary
      CodeGenUtils.genToInternal(ctx, outputDataType),
      collectorCode)
    val resultCollectorTerm = newName("resultConverterCollector")
    CollectorCodeGenerator.addToContext(ctx, resultCollectorTerm, resultCollector)

    resultCollectorTerm
  }

  private def generateScalarFunctionCall(
      ctx: CodeGeneratorContext,
      functionTerm: String,
      externalOperands: Seq[GeneratedExpression],
      outputDataType: DataType)
    : GeneratedExpression = {

    // result conversion
    val externalResultClass = outputDataType.getConversionClass
    val externalResultTypeTerm = typeTerm(externalResultClass)
    // Janino does not fully support the JVM spec:
    // boolean b = (boolean) f(); where f returns Object
    // This is not supported and we need to box manually.
    val externalResultClassBoxed = primitiveToWrapper(externalResultClass)
    val externalResultCasting = if (externalResultClass == externalResultClassBoxed) {
      s"($externalResultTypeTerm)"
    } else {
      s"($externalResultTypeTerm) (${typeTerm(externalResultClassBoxed)})"
    }
    val externalResultTerm = ctx.addReusableLocalVariable(externalResultTypeTerm, "externalResult")
    val internalExpr = genToInternalIfNeeded(ctx, outputDataType, externalResultTerm)

    // function call
    internalExpr.copy(code =
      s"""
        |${externalOperands.map(_.code).mkString("\n")}
        |$externalResultTerm = $externalResultCasting $functionTerm
        |  .$SCALAR_EVAL(${externalOperands.map(_.resultTerm).mkString(", ")});
        |${internalExpr.code}
        |""".stripMargin)
  }

  private def prepareExternalOperands(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      argumentDataTypes: Seq[DataType])
    : Seq[GeneratedExpression] = {
    operands
      .zip(argumentDataTypes)
      .map { case (operand, dataType) =>
        operand.copy(resultTerm = genToExternalIfNeeded(ctx, dataType, operand))
      }
  }

  private def verifyArgumentTypes(
      operandTypes: Seq[LogicalType],
      enrichedDataTypes: Seq[DataType])
    : Unit = {
    val enrichedTypes = enrichedDataTypes.map(_.getLogicalType)
    operandTypes.zip(enrichedTypes).foreach { case (operandType, enrichedType) =>
      // check that the logical type has not changed during the enrichment
      if (!supportsAvoidingCast(operandType, enrichedType)) {
        throw new CodeGenException(
          s"Mismatch of function's argument data type '$enrichedType' and actual " +
            s"argument type '$operandType'.")
      }
    }
    // the data type class can only partially verify the conversion class,
    // now is the time for the final check
    enrichedDataTypes.foreach(dataType => {
      if (!dataType.getLogicalType.supportsOutputConversion(dataType.getConversionClass)) {
        throw new CodeGenException(
          s"Data type '$dataType' does not support an output conversion " +
            s"to class '${dataType.getConversionClass}'.")
      }
    })
  }

  private def verifyFunctionAwareOutputType(
      returnType: LogicalType,
      enrichedDataType: DataType)
    : Unit = {
    val enrichedType = enrichedDataType.getLogicalType
    // logically table functions wrap atomic types into ROW, however, the physical function might
    // return an atomic type
    if (function.getDefinition.getKind == FunctionKind.TABLE && !isCompositeType(enrichedType)) {
      Preconditions.checkState(
        hasRoot(returnType, LogicalTypeRoot.ROW) && returnType.getChildren.size() == 1,
        "Logical output type of function call should be a ROW wrapping an atomic type.",
        Seq(): _*)
      val atomicOutputType = returnType.asInstanceOf[RowType].getChildren.get(0)
      verifyOutputType(atomicOutputType, enrichedDataType)
    } else {
      verifyOutputType(returnType, enrichedDataType)
    }
  }

  private def verifyOutputType(
      returnType: LogicalType,
      enrichedDataType: DataType)
    : Unit = {
    val enrichedType = enrichedDataType.getLogicalType
    // check that the logical type has not changed during the enrichment
    if (!supportsAvoidingCast(enrichedType, returnType)) {
      throw new CodeGenException(
        s"Mismatch of expected output data type '$returnType' and function's " +
          s"output type '$enrichedType'.")
    }
    // the data type class can only partially verify the conversion class,
    // now is the time for the final check
    if (!enrichedType.supportsInputConversion(enrichedDataType.getConversionClass)) {
      throw new CodeGenException(
        s"Data type '$enrichedDataType' does not support an input conversion " +
          s"to class '${enrichedDataType.getConversionClass}'.")
    }
  }

  private def verifyFunctionAwareImplementation(
      argumentDataTypes: Seq[DataType],
      outputDataType: DataType)
    : Unit = {
    if (function.getDefinition.getKind == FunctionKind.TABLE) {
      verifyImplementation(TABLE_EVAL, argumentDataTypes, None)
    } else if (function.getDefinition.getKind == FunctionKind.SCALAR) {
      verifyImplementation(SCALAR_EVAL, argumentDataTypes, Some(outputDataType))
    } else {
      throw new CodeGenException(
        s"Unsupported function kind '${function.getDefinition.getKind}' for function '$function'.")
    }
  }

  private def verifyImplementation(
      methodName: String,
      argumentDataTypes: Seq[DataType],
      outputDataType: Option[DataType])
    : Unit = {
    val methods = toScala(ExtractionUtils.collectMethods(udf.getClass, methodName))
    val argumentClasses = argumentDataTypes.map(_.getConversionClass).toArray
    val outputClass = outputDataType.map(_.getConversionClass).getOrElse(classOf[Unit])
    // verifies regular JVM calling semantics
    def methodMatches(method: Method): Boolean = {
      isMethodInvokable(method, argumentClasses: _*) &&
        isAssignable(outputClass, method.getReturnType, true)
    }
    if (!methods.exists(methodMatches)) {
      throw new CodeGenException(
        s"Could not find an implementation method in class '${typeTerm(udf.getClass)}' for " +
          s"function '$function' that matches the following signature: \n" +
          s"${createMethodSignatureString(methodName, argumentClasses, outputClass)}")
    }
  }
}
