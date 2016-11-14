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
import org.apache.flink.api.table.codegen.{CodeGenException, CodeGenerator, GeneratedExpression}
import org.apache.flink.api.table.functions.ScalarFunction
import org.apache.flink.api.table.functions.utils.UserDefinedFunctionUtils._

/**
  * Generates a call to user-defined [[ScalarFunction]].
  *
  * @param scalarFunction user-defined [[ScalarFunction]] that might be overloaded
  * @param signature actual signature with which the function is called
  * @param returnType actual return type required by the surrounding
  */
class ScalarFunctionCallGen(
    scalarFunction: ScalarFunction,
    signature: Seq[TypeInformation[_]],
    returnType: TypeInformation[_])
  extends CallGenerator {

  override def generate(
      codeGenerator: CodeGenerator,
      operands: Seq[GeneratedExpression])
    : GeneratedExpression = {
    // determine function signature and result class
    val matchingSignature = getSignature(scalarFunction, signature)
      .getOrElse(throw new CodeGenException("No matching signature found."))
    val resultClass = getResultTypeClass(scalarFunction, matchingSignature)

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
    val functionReference = codeGenerator.addReusableFunction(scalarFunction)
    val resultTypeTerm = if (resultClass.isPrimitive) {
      primitiveTypeTermForTypeInfo(returnType)
    } else {
      boxedTypeTermForTypeInfo(returnType)
    }
    val resultTerm = newName("result")
    val functionCallCode =
      s"""
        |${parameters.map(_.code).mkString("\n")}
        |$resultTypeTerm $resultTerm = $functionReference.eval(
        |  ${parameters.map(_.resultTerm).mkString(", ")});
        |""".stripMargin

    // convert result of function to internal representation (input unboxing)
    val resultUnboxing = if (resultClass.isPrimitive) {
      codeGenerator.generateNonNullLiteral(returnType, resultTerm)
    } else {
      codeGenerator.generateInputFieldUnboxing(returnType, resultTerm)
    }
    resultUnboxing.copy(code =
      s"""
        |$functionCallCode
        |${resultUnboxing.code}
        |""".stripMargin
    )
  }
}
