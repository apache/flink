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

package org.apache.flink.table.codegen.calls

import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.apache.flink.table.codegen.CodeGenUtils.genToExternalIfNeeded
import org.apache.flink.table.codegen.GeneratedExpression.NEVER_NULL
import org.apache.flink.table.codegen.{CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils.getEvalMethodSignature
import org.apache.flink.table.types.LogicalTypeDataTypeConverter.fromLogicalTypeToDataType
import org.apache.flink.table.types.logical.LogicalType
import org.apache.flink.table.types.utils.TypeConversions

/**
  * Generates a call to user-defined [[TableFunction]].
  *
  * @param tableFunction user-defined [[TableFunction]] that might be overloaded
  */
class TableFunctionCallGen(tableFunction: TableFunction[_]) extends CallGenerator {

  override def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    // convert parameters for function (output boxing)
    val parameters = prepareUDFArgs(ctx, operands, tableFunction)

    // generate function call
    val functionReference = ctx.addReusableFunction(tableFunction)
    val functionCallCode =
      s"""
        |${parameters.map(_.code).mkString("\n")}
        |$functionReference.eval(${parameters.map(_.resultTerm).mkString(", ")});
        |""".stripMargin

    // has no result
    GeneratedExpression(
      functionReference,
      NEVER_NULL,
      functionCallCode,
      returnType)
  }

  def prepareUDFArgs(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      func: TableFunction[_]): Array[GeneratedExpression] = {

    // get the expanded parameter types
    var paramClasses = getEvalMethodSignature(func, operands.map(_.resultType).toArray)

    val signatureTypes = func
        .getParameterTypes(paramClasses)
        .zipWithIndex
        .map {
          case (t, i) =>
            // we don't trust GenericType.
            if (t.isInstanceOf[GenericTypeInfo[_]]) {
              fromLogicalTypeToDataType(operands(i).resultType)
            } else {
              TypeConversions.fromLegacyInfoToDataType(t)
            }
        }

    paramClasses.zipWithIndex.zip(operands).map { case ((paramClass, i), operandExpr) =>
      if (paramClass.isPrimitive) {
        operandExpr
      } else {
        val externalResultTerm = genToExternalIfNeeded(
          ctx, signatureTypes(i), paramClass, operandExpr.resultTerm)
        val exprOrNull = s"${operandExpr.nullTerm} ? null : ($externalResultTerm)"
        operandExpr.copy(resultTerm = exprOrNull)
      }
    }
  }
}
