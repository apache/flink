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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.apache.flink.table.dataformat.DataFormatConverters
import org.apache.flink.table.dataformat.DataFormatConverters.getConverterForDataType
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.codegen.calls.ScalarFunctionCallGen.prepareFunctionArgs
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, GenerateUtils, GeneratedExpression}
import org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils
import org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromLogicalTypeToDataType
import org.apache.flink.table.types.extraction.utils.ExtractionUtils
import org.apache.flink.table.types.logical.LogicalType
import org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType

/**
  * Generates a call to user-defined [[ScalarFunction]].
  *
  * @param scalarFunction user-defined [[ScalarFunction]] that might be overloaded
  */
class ScalarFunctionCallGen(scalarFunction: ScalarFunction) extends CallGenerator {

  override def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    val operandTypes = operands.map(_.resultType).toArray
    val arguments = operands.map {
      case expr if expr.literal =>
        getConverterForDataType(fromLogicalTypeToDataType(expr.resultType))
            .asInstanceOf[DataFormatConverters.DataFormatConverter[Any, Any]]
            .toExternal(expr.literalValue.get)
            .asInstanceOf[AnyRef]
      case _ => null
    }.toArray
    // determine function method and result class
    val resultClass = getResultTypeClassOfScalarFunction(scalarFunction, operandTypes)

    // convert parameters for function (output boxing)
    val parameters = prepareUDFArgs(ctx, operands, scalarFunction)

    // generate function call
    val functionReference = ctx.addReusableFunction(scalarFunction)
    val resultTypeTerm = if (resultClass.isPrimitive) {
      primitiveTypeTermForType(returnType)
    } else {
      boxedTypeTermForType(returnType)
    }
    val resultTerm = ctx.addReusableLocalVariable(resultTypeTerm, "result")
    val evalResult = s"$functionReference.eval(${parameters.map(_.resultTerm).mkString(", ")})"
    val resultExternalType = UserDefinedFunctionUtils.getResultTypeOfScalarFunction(
      scalarFunction, arguments, operandTypes)
    val setResult = {
      if (resultClass.isPrimitive && isInternalClass(resultExternalType)) {
        s"$resultTerm = $evalResult;"
      } else {
        val javaTerm = newName("javaResult")
        // it maybe a Internal class, so use resultClass is most safety.
        val boxedResultClass = ExtractionUtils.boxPrimitive(resultClass).asInstanceOf[Class[_]]
        val javaTypeTerm = boxedResultClass.getCanonicalName
        val resultExternalTypeWithResultClass =
          if (LogicalTypeDataTypeConverter.fromDataTypeToLogicalType(resultExternalType)
            .supportsOutputConversion(boxedResultClass)) {
            // resultClass of HiveScalarFunction is Object, which cannot be a valid
            // conversion class
            resultExternalType.bridgedTo(boxedResultClass)
          } else {
            resultExternalType
          }
        val internal = genToInternalIfNeeded(
          ctx, resultExternalTypeWithResultClass, javaTerm)
        s"""
            |$javaTypeTerm $javaTerm = ($javaTypeTerm) $evalResult;
            |$resultTerm = $javaTerm == null ? null : ($internal);
            """.stripMargin
      }
    }

    val functionCallCode =
      s"""
        |${parameters.map(_.code).mkString("\n")}
        |$setResult
        |""".stripMargin

    // convert result of function to internal representation (input unboxing)
    val resultUnboxing = if (resultClass.isPrimitive) {
      GenerateUtils.generateNonNullField(returnType, resultTerm)
    } else {
      GenerateUtils.generateInputFieldUnboxing(ctx, returnType, resultTerm)
    }
    resultUnboxing.copy(code =
      s"""
        |$functionCallCode
        |${resultUnboxing.code}
        |""".stripMargin
    )
  }

  def prepareUDFArgs(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      func: ScalarFunction): Array[GeneratedExpression] = {
    // get the expanded parameter types
    var paramClasses = getEvalMethodSignature(func, operands.map(_.resultType).toArray)
    prepareFunctionArgs(ctx, operands, paramClasses, func.getParameterTypes(paramClasses))
  }

}

object ScalarFunctionCallGen {

  def prepareFunctionArgs(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      parameterClasses: Array[Class[_]],
      parameterTypes: Array[TypeInformation[_]]): Array[GeneratedExpression] = {

    val signatureTypes = parameterTypes.zipWithIndex.map {
      case (t: GenericTypeInfo[_], i) =>
        // we don't trust GenericType, like Row and BaseRow and LocalTime
        val returnType = fromLogicalTypeToDataType(operands(i).resultType)
        if (operands(i).resultType.supportsOutputConversion(t.getTypeClass)) {
          returnType.bridgedTo(t.getTypeClass)
        } else {
          returnType
        }
      case (t, _) => fromLegacyInfoToDataType(t)
    }

    parameterClasses.zipWithIndex.zip(operands).map { case ((paramClass, i), operandExpr) =>
      if (paramClass.isPrimitive && isInternalClass(signatureTypes(i))) {
        operandExpr
      } else {
        val boxedParamClass = ExtractionUtils.boxPrimitive(paramClass).asInstanceOf[Class[_]]
        val signatureType =
          if (signatureTypes(i)
              .getLogicalType
              .supportsOutputConversion(boxedParamClass)) {
            signatureTypes(i).bridgedTo(boxedParamClass)
          } else {
            signatureTypes(i)
          }
        val externalResultTerm = genToExternalIfNeeded(
          ctx, signatureType, operandExpr.resultTerm)
        val exprOrNull = s"${operandExpr.nullTerm} ? null : ($externalResultTerm)"
        operandExpr.copy(resultTerm = exprOrNull)
      }
    }
  }

}
