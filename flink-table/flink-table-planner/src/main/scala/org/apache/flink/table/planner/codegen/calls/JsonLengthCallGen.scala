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

import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, CodeGenUtils, GeneratedExpression}
import org.apache.flink.table.planner.codegen.CodeGenUtils.qualifyMethod
import org.apache.flink.table.planner.codegen.GenerateUtils.generateCallWithStmtIfArgsNotNull
import org.apache.flink.table.runtime.functions.SqlJsonUtils
import org.apache.flink.table.types.logical.LogicalType

/**
 * [[CallGenerator]] for
 * [[org.apache.flink.table.functions.BuiltInFunctionDefinitions.JSON_LENGTH]].
 *
 * Like [[JsonValueCallGen]] and [[JsonQueryCallGen]], when multiple JSON function calls share the
 * same input expression the parsed JSON context is reused via a shared member variable, so a given
 * JSON string is parsed at most once per row regardless of how many JSON functions read it.
 */
class JsonLengthCallGen extends CallGenerator {
  override def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {

    generateCallWithStmtIfArgsNotNull(ctx, returnType, operands, resultNullable = true) {
      argTerms =>
        val inputTerm = s"${argTerms.head}.toString()"

        val (varName, parseCode) =
          ctx.getReusableInputUnboxingExprs(inputTerm, Int.MinValue) match {
            case Some(expr) => (expr.resultTerm, "")
            case None =>
              val newVarName = CodeGenUtils.newName(ctx, "jsonParsed")
              val typeName = classOf[SqlJsonUtils.JsonValueContext].getName
              ctx.addReusableMember(s"$typeName $newVarName;")
              ctx.addReusableInputUnboxingExprs(
                inputTerm,
                Int.MinValue,
                GeneratedExpression(newVarName, "false", "", null))
              val assign =
                s"$newVarName = ${qualifyMethod(BuiltInMethods.JSON_PARSE)}($inputTerm);"
              (newVarName, assign)
          }

        val (method, terms) =
          if (operands.length > 1) {
            (BuiltInMethods.JSON_LENGTH_PATH, Seq(varName, s"${argTerms(1)}.toString()"))
          } else {
            (BuiltInMethods.JSON_LENGTH, Seq(varName))
          }

        (parseCode, s"${qualifyMethod(method)}(${terms.mkString(", ")})")
    }
  }
}
