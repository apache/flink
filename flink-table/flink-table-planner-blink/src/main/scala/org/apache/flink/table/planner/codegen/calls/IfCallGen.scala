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

import org.apache.flink.table.planner.codegen.CodeGenUtils.{primitiveDefaultValue, primitiveTypeTermForType}
import org.apache.flink.table.planner.codegen.{CodeGenUtils, CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.types.logical.LogicalType

/**
  * Generates IF function call.
  */
class IfCallGen() extends CallGenerator {

  override def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType)
    : GeneratedExpression = {

    // Inferred return type is ARG1. Must be the same as ARG2.
    // This is a temporary solution which introduce type cast in codegen.
    // Not elegant, but can allow IF function to handle different numeric type arguments.
    val castedResultTerm1 = CodeGenUtils.getNumericCastedResultTerm(operands(1), returnType)
    val castedResultTerm2 = CodeGenUtils.getNumericCastedResultTerm(operands(2), returnType)
    if (castedResultTerm1 == null || castedResultTerm2 == null) {
      throw new Exception(String.format("Unsupported operand types: IF(boolean, %s, %s)",
        operands(1).resultType, operands(2).resultType))
    }

    val resultTypeTerm = primitiveTypeTermForType(returnType)
    val resultDefault = primitiveDefaultValue(returnType)
    val Seq(resultTerm, nullTerm) = ctx.addReusableLocalVariables(
      (resultTypeTerm, "result"),
      ("boolean", "isNull"))

    val resultCode =
      s"""
         |${operands.head.code}
         |$resultTerm = $resultDefault;
         |if (${operands.head.resultTerm}) {
         |  ${operands(1).code}
         |  if (!${operands(1).nullTerm}) {
         |    $resultTerm = $castedResultTerm1;
         |  }
         |  $nullTerm = ${operands(1).nullTerm};
         |} else {
         |  ${operands(2).code}
         |  if (!${operands(2).nullTerm}) {
         |    $resultTerm = $castedResultTerm2;
         |  }
         |  $nullTerm = ${operands(2).nullTerm};
         |}
       """.stripMargin

    GeneratedExpression(resultTerm, nullTerm, resultCode, returnType)
  }
}
