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
import org.apache.flink.table.runtime.functions.SqlJsonUtils

/**
 * Shares the parsed JSON input between the JSON function calls of a single generated expression,
 * see [[JsonValueCallGen]] and [[JsonQueryCallGen]].
 *
 * The input is parsed at most once per record, by whichever call runs first, and the result is held
 * in a member variable that the following calls on the same input read instead of parsing again.
 */
object JsonParseReuse {

  /**
   * Returns the expression holding the parsed input. The caller generates its own call with the
   * original operands and reads the parsed input from the returned expression.
   *
   * The parse is lazy: the result term is a call to a member method that parses on its first
   * invocation for the record. `generateCallWithStmtIfArgsNotNull` places a call under a guard that
   * requires *all* of its arguments to be non-null, so no single call can be made the owner of the
   * parse - in
   * {{{
   * SELECT JSON_VALUE(v, CAST(NULL AS STRING)), JSON_QUERY(v, '$.a')
   * }}}
   * the NULL path makes the first call short-circuit while the second one still needs the parse.
   * Conversely, when no call runs, no parse happens at all.
   */
  def parseSharedInput(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val input = operands.head
    val inputTerm = s"${input.resultTerm}.toString()"

    ctx.getReusableInputUnboxingExprs(inputTerm, Int.MinValue) match {
      case Some(expr) => expr
      case None =>
        val varName = CodeGenUtils.newName(ctx, "jsonParsed")
        val lastInputName = CodeGenUtils.newName(ctx, "jsonParsedInput")
        val methodName = CodeGenUtils.newName(ctx, "parseJson")
        val typeName = classOf[SqlJsonUtils.JsonValueContext].getName
        val inputType = CodeGenUtils.boxedTypeTermForType(input.resultType)
        ctx.addReusableMember(s"$typeName $varName;")
        ctx.addReusableMember(s"$inputType $lastInputName;")

        // keyed on the immutable input so it re-parses on change; no per-record reset needed
        ctx.addReusableMember(
          s"""
             |private $typeName $methodName($inputType in) {
             |  if (in != $lastInputName) {
             |    $lastInputName = in;
             |    $varName = ${qualifyMethod(BuiltInMethods.JSON_PARSE)}(in.toString());
             |  }
             |  return $varName;
             |}
             |""".stripMargin)

        val parsed =
          GeneratedExpression(s"$methodName(${input.resultTerm})", "false", "", null)
        ctx.addReusableInputUnboxingExprs(inputTerm, Int.MinValue, parsed)

        parsed
    }
  }
}
