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
import org.apache.flink.table.functions.UserDefinedFunctionHelper.SCALAR_EVAL
import org.apache.flink.table.functions.{ScalarFunction, TableFunction, UserDefinedFunction}
import org.apache.flink.table.planner.codegen.CodeGenUtils.{genToExternalIfNeeded, genToInternalIfNeeded, typeTerm}
import org.apache.flink.table.planner.codegen.{CodeGenException, CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction
import org.apache.flink.table.planner.functions.inference.OperatorBindingCallContext
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toScala
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.extraction.utils.ExtractionUtils
import org.apache.flink.table.types.extraction.utils.ExtractionUtils.{createMethodSignatureString, isAssignable, isMethodInvokable, primitiveToWrapper}
import org.apache.flink.table.types.inference.TypeInferenceUtil
import org.apache.flink.table.types.logical.LogicalType

/**
 * Generates a call to a user-defined [[ScalarFunction]] or [[TableFunction]] (future work).
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
    verifyOutputType(returnType, enrichedOutputDataType)

    // find runtime method and generate call
    verifyImplementation(enrichedArgumentDataTypes, enrichedOutputDataType)
    generateFunctionCall(ctx, operands, enrichedArgumentDataTypes, enrichedOutputDataType)
  }

  private def generateFunctionCall(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      argumentDataTypes: Seq[DataType],
      outputDataType: DataType)
    : GeneratedExpression = {

    val functionTerm = ctx.addReusableFunction(udf)

    // operand conversion
    val externalOperands = prepareExternalOperands(ctx, operands, argumentDataTypes)
    val externalOperandTerms = externalOperands.map(_.resultTerm).mkString(", ")

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
        |  .$SCALAR_EVAL($externalOperandTerms);
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
      // a nullability mismatch is acceptable if the enriched type can handle it
      if (operandType != enrichedType && operandType.copy(true) != enrichedType) {
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

  private def verifyOutputType(
      outputType: LogicalType,
      enrichedDataType: DataType)
    : Unit = {
    val enrichedType = enrichedDataType.getLogicalType
    // check that the logical type has not changed during the enrichment
    // a nullability mismatch is acceptable if the output type can handle it
    if (outputType != enrichedType && outputType != enrichedType.copy(true)) {
      throw new CodeGenException(
        s"Mismatch of expected output data type '$outputType' and function's " +
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

  private def verifyImplementation(
      argumentDataTypes: Seq[DataType],
      outputDataType: DataType)
    : Unit = {
    val methods = toScala(ExtractionUtils.collectMethods(udf.getClass, SCALAR_EVAL))
    val argumentClasses = argumentDataTypes.map(_.getConversionClass).toArray
    val outputClass = outputDataType.getConversionClass
    // verifies regular JVM calling semantics
    def methodMatches(method: Method): Boolean = {
      isMethodInvokable(method, argumentClasses: _*) &&
        isAssignable(outputClass, method.getReturnType, true)
    }
    if (!methods.exists(methodMatches)) {
      throw new CodeGenException(
        s"Could not find an implementation method that matches the following signature: " +
          s"${createMethodSignatureString(SCALAR_EVAL, argumentClasses, outputClass)}")
    }
  }
}
