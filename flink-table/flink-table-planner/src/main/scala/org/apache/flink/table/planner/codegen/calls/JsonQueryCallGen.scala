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

import org.apache.flink.table.api.{JsonQueryOnEmptyOrError, JsonQueryWrapper, JsonValueOnEmptyOrError}
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, CodeGenException, CodeGenUtils, GeneratedExpression}
import org.apache.flink.table.planner.codegen.CodeGenUtils.{qualifyEnum, qualifyMethod, BINARY_STRING, GENERIC_ARRAY}
import org.apache.flink.table.planner.codegen.GenerateUtils.generateCallWithStmtIfArgsNotNull
import org.apache.flink.table.runtime.functions.SqlJsonUtils
import org.apache.flink.table.runtime.functions.SqlJsonUtils.JsonQueryReturnType
import org.apache.flink.table.types.logical.{ArrayType, LogicalType, LogicalTypeRoot}

import org.apache.calcite.sql.SqlJsonEmptyOrError

/**
 * [[CallGenerator]] for [[BuiltInMethods.JSON_QUERY]].
 *
 * We cannot use [[MethodCallGen]] for a few different reasons. First, the return type of the
 * built-in Calcite function is [[Object]] and needs to be cast based on the inferred return type
 * instead as users can change this using the RETURNING keyword.
 *
 * When multiple JSON function calls share the same input expression, the parsed JSON context is
 * reused via a shared member variable. For example, a query like:
 * {{{
 * SELECT JSON_VALUE(json_data, '$.type'), JSON_QUERY(json_data, '$.address') FROM t
 * }}}
 * generates code similar to:
 * {{{
 * // member variable (declared once)
 * SqlJsonUtils.JsonValueContext jsonParsed$0;
 *
 * // in processElement (parse emitted only by the first function)
 * jsonParsed$0 = SqlJsonUtils.jsonParse(field$0.toString());
 * Object rawResult$1 = SqlJsonUtils.jsonValue(jsonParsed$0, "$.type", ...);
 * // second call reuses jsonParsed$123 without re-parsing
 * Object rawResult$2 = SqlJsonUtils.jsonQuery(jsonParsed$0, "$.address", ...);
 * }}}
 */
class JsonQueryCallGen extends CallGenerator {
  override def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {

    generateCallWithStmtIfArgsNotNull(ctx, returnType, operands, resultNullable = true) {
      argTerms =>
        {
          val emptyBehavior = operands(3).literalValue.get.asInstanceOf[JsonQueryOnEmptyOrError]
          val errorBehavior = operands(4).literalValue.get.asInstanceOf[JsonQueryOnEmptyOrError]
          val wrapperBehavior = operands(2).literalValue.get.asInstanceOf[JsonQueryWrapper]
          val jsonQueryReturnType = if (returnType.getTypeRoot == LogicalTypeRoot.ARRAY) {
            JsonQueryReturnType.ARRAY
          } else {
            JsonQueryReturnType.STRING
          }
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

          val terms = Seq(
            varName,
            s"${argTerms(1)}.toString()",
            qualifyEnum(jsonQueryReturnType),
            qualifyEnum(wrapperBehavior),
            qualifyEnum(emptyBehavior),
            qualifyEnum(errorBehavior)
          )

          val rawResultTerm = CodeGenUtils.newName(ctx, "rawResult")
          val call = s"""
                        |$parseCode
                        |Object $rawResultTerm =
                        |    ${qualifyMethod(BuiltInMethods.JSON_QUERY_PARSED)}(${terms
                         .mkString(", ")});
           """.stripMargin

          val convertedResult = returnType.getTypeRoot match {
            case LogicalTypeRoot.VARCHAR =>
              s"$BINARY_STRING.fromString(java.lang.String.valueOf($rawResultTerm))"
            case LogicalTypeRoot.ARRAY =>
              val elementType = returnType.asInstanceOf[ArrayType].getElementType
              if (elementType.getTypeRoot == LogicalTypeRoot.VARCHAR) {
                s"($GENERIC_ARRAY) $rawResultTerm"
              } else {
                throw new CodeGenException(
                  s"Unsupported array element type '$elementType' for RETURNING ARRAY in JSON_QUERY().")
              }
            case _ =>
              throw new CodeGenException(
                s"Unsupported type '$returnType' "
                  + "for RETURNING in JSON_VALUE().")
          }

          val result = s"($rawResultTerm == null) ? null : ($convertedResult)"
          (call, result)
        }
    }
  }
}
