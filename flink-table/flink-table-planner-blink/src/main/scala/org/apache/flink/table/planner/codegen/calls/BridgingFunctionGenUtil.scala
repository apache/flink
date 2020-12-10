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

import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.data.GenericRowData
import org.apache.flink.table.functions.UserDefinedFunctionHelper.{ASYNC_TABLE_EVAL, SCALAR_EVAL, TABLE_EVAL, validateClassForRuntime}
import org.apache.flink.table.functions._
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.codegen.GeneratedExpression.{NEVER_NULL, NO_CODE}
import org.apache.flink.table.planner.codegen._
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toScala
import org.apache.flink.table.runtime.collector.WrappingCollector
import org.apache.flink.table.runtime.operators.join.lookup.DelegatingResultFuture
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.extraction.ExtractionUtils.primitiveToWrapper
import org.apache.flink.table.types.inference.{CallContext, TypeInference, TypeInferenceUtil}
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsAvoidingCast
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks.{hasRoot, isCompositeType}
import org.apache.flink.table.types.logical.{LogicalType, LogicalTypeRoot, RowType}
import org.apache.flink.table.types.utils.DataTypeUtils.{validateInputDataType, validateOutputDataType}
import org.apache.flink.util.Preconditions

import java.util.concurrent.CompletableFuture

/**
 * Helps in generating a call to a user-defined [[ScalarFunction]], [[TableFunction]],
 * or [[AsyncTableFunction]].
 *
 * Table functions are a special case because they are using a collector. Thus, the result of the
 * generation will be a reference to a [[WrappingCollector]]. Furthermore, atomic types are wrapped
 * into a row by the collector.
 *
 * Async table functions don't support atomic types.
 */
object BridgingFunctionGenUtil {

  def generateFunctionAwareCall(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType,
      inference: TypeInference,
      callContext: CallContext,
      udf: UserDefinedFunction,
      functionName: String,
      skipIfArgsNull: Boolean)
    : GeneratedExpression = {

    val (call, _) = generateFunctionAwareCallWithDataType(
      ctx,
      operands,
      returnType,
      inference,
      callContext,
      udf,
      functionName,
      skipIfArgsNull)

    call
  }

  def generateFunctionAwareCallWithDataType(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType,
      inference: TypeInference,
      callContext: CallContext,
      udf: UserDefinedFunction,
      functionName: String,
      skipIfArgsNull: Boolean)
    : (GeneratedExpression, DataType) = {

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
    verifyFunctionAwareOutputType(returnType, enrichedOutputDataType, udf)

    // find runtime method and generate call
    verifyFunctionAwareImplementation(
      enrichedArgumentDataTypes,
      enrichedOutputDataType,
      udf,
      functionName)
    val call = generateFunctionAwareCall(
      ctx,
      operands,
      enrichedArgumentDataTypes,
      enrichedOutputDataType,
      returnType,
      udf,
      skipIfArgsNull)
    (call, enrichedOutputDataType)
  }

  private def generateFunctionAwareCall(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      argumentDataTypes: Seq[DataType],
      outputDataType: DataType,
      returnType: LogicalType,
      udf: UserDefinedFunction,
      skipIfArgsNull: Boolean)
    : GeneratedExpression = {

    val functionTerm = ctx.addReusableFunction(udf)

    // operand conversion
    val externalOperands = prepareExternalOperands(ctx, operands, argumentDataTypes)

    if (udf.getKind == FunctionKind.TABLE) {
      generateTableFunctionCall(
        ctx,
        functionTerm,
        externalOperands,
        outputDataType,
        returnType,
        skipIfArgsNull)
    } else if (udf.getKind == FunctionKind.ASYNC_TABLE) {
      generateAsyncTableFunctionCall(
        ctx,
        functionTerm,
        externalOperands,
        returnType)
    } else {
      generateScalarFunctionCall(ctx, functionTerm, externalOperands, outputDataType)
    }
  }

  private def generateTableFunctionCall(
      ctx: CodeGeneratorContext,
      functionTerm: String,
      externalOperands: Seq[GeneratedExpression],
      functionOutputDataType: DataType,
      outputType: LogicalType,
      skipIfArgsNull: Boolean)
    : GeneratedExpression = {
    val resultCollectorTerm = generateResultCollector(ctx, functionOutputDataType, outputType)

    val setCollectorCode = s"""
      |$functionTerm.setCollector($resultCollectorTerm);
      |""".stripMargin
    ctx.addReusableOpenStatement(setCollectorCode)

    val functionCallCode = if (skipIfArgsNull) {
      s"""
        |${externalOperands.map(_.code).mkString("\n")}
        |if (${externalOperands.map(_.nullTerm).mkString(" || ")}) {
        |  // skip
        |} else {
        |  $functionTerm.eval(${externalOperands.map(_.resultTerm).mkString(", ")});
        |}
        |""".stripMargin
    } else {
      s"""
        |${externalOperands.map(_.code).mkString("\n")}
        |$functionTerm.eval(${externalOperands.map(_.resultTerm).mkString(", ")});
        |""".stripMargin
    }

    // has no result
    GeneratedExpression(
      resultCollectorTerm,
      NEVER_NULL,
      functionCallCode,
      outputType)
  }

  private def generateAsyncTableFunctionCall(
      ctx: CodeGeneratorContext,
      functionTerm: String,
      externalOperands: Seq[GeneratedExpression],
      outputType: LogicalType)
    : GeneratedExpression = {

    val DELEGATE = className[DelegatingResultFuture[_]]

    val functionCallCode =
      s"""
        |${externalOperands.map(_.code).mkString("\n")}
        |if (${externalOperands.map(_.nullTerm).mkString(" || ")}) {
        |  $DEFAULT_COLLECTOR_TERM.complete(java.util.Collections.emptyList());
        |} else {
        |  $DELEGATE delegates = new $DELEGATE($DEFAULT_COLLECTOR_TERM);
        |  $functionTerm.eval(
        |    delegates.getCompletableFuture(),
        |    ${externalOperands.map(_.resultTerm).mkString(", ")});
        |}
        |""".stripMargin

    // has no result
    GeneratedExpression(
      NO_CODE,
      NEVER_NULL,
      functionCallCode,
      outputType)
  }

  /**
   * Generates a collector that converts the output of a table function (possibly as an atomic type)
   * into an internal row type. Returns a collector term for referencing the collector.
   */
  private def generateResultCollector(
      ctx: CodeGeneratorContext,
      outputDataType: DataType,
      returnType: LogicalType)
    : String = {
    val outputType = outputDataType.getLogicalType

    val collectorCtx = CodeGeneratorContext(ctx.tableConfig)
    val externalResultTerm = newName("externalResult")

    // code for wrapping atomic types
    val collectorCode = if (!isCompositeType(outputType)) {
      val resultGenerator = new ExprCodeGenerator(collectorCtx, outputType.isNullable)
        .bindInput(outputType, externalResultTerm)
      val wrappedResult = resultGenerator.generateConverterResultExpression(
        returnType.asInstanceOf[RowType],
        classOf[GenericRowData])
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
      genToInternalConverter(ctx, outputDataType),
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
    val internalExpr = genToInternalConverterAll(ctx, outputDataType, externalResultTerm)

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
        operand.copy(resultTerm = genToExternalConverterAll(ctx, dataType, operand))
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
    enrichedDataTypes.foreach(validateOutputDataType)
  }

  private def verifyFunctionAwareOutputType(
      returnType: LogicalType,
      enrichedDataType: DataType,
      udf: UserDefinedFunction)
    : Unit = {
    val enrichedType = enrichedDataType.getLogicalType
    if (udf.getKind == FunctionKind.TABLE && !isCompositeType(enrichedType)) {
      // logically table functions wrap atomic types into ROW, however, the physical function might
      // return an atomic type
      Preconditions.checkState(
        hasRoot(returnType, LogicalTypeRoot.ROW) && returnType.getChildren.size() == 1,
        "Logical output type of function call should be a ROW wrapping an atomic type.",
        Seq(): _*)
      val atomicOutputType = returnType.asInstanceOf[RowType].getChildren.get(0)
      verifyOutputType(atomicOutputType, enrichedDataType, udf)
    } else if (udf.getKind == FunctionKind.ASYNC_TABLE && !isCompositeType(enrichedType)) {
      throw new CodeGenException(
        "Async table functions must not emit an atomic type. " +
            "Only a composite type such as the row type are supported.")
    }
    else if (udf.getKind == FunctionKind.TABLE || udf.getKind == FunctionKind.ASYNC_TABLE) {
      // null values are skipped therefore, the result top level row will always be not null
      verifyOutputType(returnType.copy(true), enrichedDataType, udf)
    } else {
      verifyOutputType(returnType, enrichedDataType, udf)
    }
  }

  private def verifyOutputType(
      returnType: LogicalType,
      enrichedDataType: DataType,
      udf: UserDefinedFunction)
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
    validateInputDataType(enrichedDataType)
  }

  def verifyFunctionAwareImplementation(
      argumentDataTypes: Seq[DataType],
      outputDataType: DataType,
      udf: UserDefinedFunction,
      functionName: String)
    : Unit = {
    if (udf.getKind == FunctionKind.TABLE) {
      verifyImplementation(TABLE_EVAL, argumentDataTypes, None, udf, functionName)
    } else if (udf.getKind == FunctionKind.ASYNC_TABLE) {
      verifyImplementation(
        ASYNC_TABLE_EVAL,
        DataTypes.NULL.bridgedTo(classOf[CompletableFuture[_]]) +: argumentDataTypes,
        None,
        udf,
        functionName)
    } else if (udf.getKind == FunctionKind.SCALAR) {
      verifyImplementation(SCALAR_EVAL, argumentDataTypes, Some(outputDataType), udf, functionName)
    } else {
      throw new CodeGenException(
        s"Unsupported function kind '${udf.getKind}' for function '$functionName'.")
    }
  }

  private def verifyImplementation(
      methodName: String,
      argumentDataTypes: Seq[DataType],
      outputDataType: Option[DataType],
      udf: UserDefinedFunction,
      functionName: String)
    : Unit = {
    val argumentClasses = argumentDataTypes.map(_.getConversionClass).toArray
    val outputClass = outputDataType.map(_.getConversionClass).getOrElse(classOf[Unit])
    validateClassForRuntime(
      udf.getClass,
      methodName,
      argumentClasses,
      outputClass,
      functionName)
  }
}
