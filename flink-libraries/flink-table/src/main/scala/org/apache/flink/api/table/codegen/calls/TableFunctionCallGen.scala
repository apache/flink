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

package org.apache.flink.api.table.codegen.calls

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.table.codegen.CodeGenUtils._
import org.apache.flink.api.table.codegen.GeneratedExpression.NEVER_NULL
import org.apache.flink.api.table.codegen.{CodeGenException, CodeGenerator, GeneratedExpression}
import org.apache.flink.api.table.functions.TableFunction
import org.apache.flink.api.table.functions.utils.UserDefinedFunctionUtils._

/**
  * Generates a call to user-defined [[TableFunction]].
  *
  * @param tableFunction user-defined [[TableFunction]] that might be overloaded
  * @param signature actual signature with which the function is called
  * @param returnType actual return type required by the surrounding
  */
class TableFunctionCallGen(
    tableFunction: TableFunction[_],
    signature: Seq[TypeInformation[_]],
    returnType: TypeInformation[_])
  extends CallGenerator {

  override def generate(
      codeGenerator: CodeGenerator,
      operands: Seq[GeneratedExpression])
    : GeneratedExpression = {
    // determine function signature
    val matchingSignature = getSignature(tableFunction, signature)
      .getOrElse(throw new CodeGenException("No matching signature found."))

    // convert parameters for function (output boxing)
    val parameters = matchingSignature
        .zip(operands)
        .map { case (paramClass, operandExpr) =>
          if (paramClass.isPrimitive) {
            operandExpr
          } else {
            val boxedTypeTerm = boxedTypeTermForTypeInfo(operandExpr.resultType)
            val boxedExpr = codeGenerator.generateOutputFieldBoxing(operandExpr)
            val exprOrNull: String = if (codeGenerator.nullCheck) {
              s"${boxedExpr.nullTerm} ? null : ($boxedTypeTerm) ${boxedExpr.resultTerm}"
            } else {
              boxedExpr.resultTerm
            }
            boxedExpr.copy(resultTerm = exprOrNull)
          }
        }

    // generate function call
    val functionReference = codeGenerator.addReusableFunction(tableFunction)
    val functionCallCode =
      s"""
        |${parameters.map(_.code).mkString("\n")}
        |$functionReference.clear();
        |$functionReference.eval(${parameters.map(_.resultTerm).mkString(", ")});
        |""".stripMargin

    // has no result
    GeneratedExpression(
      functionReference,
      NEVER_NULL,
      functionCallCode,
      returnType)
  }
}
