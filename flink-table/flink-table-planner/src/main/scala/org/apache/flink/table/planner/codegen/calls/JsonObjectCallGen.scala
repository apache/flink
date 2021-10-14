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
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.codegen.JsonGenerateUtils.{createNodeTerm, getOnNullBehavior}
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.runtime.functions.SqlJsonUtils
import org.apache.flink.table.types.logical.LogicalType

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.{NullNode, ObjectNode}

import org.apache.calcite.rex.RexCall
import org.apache.calcite.sql.SqlJsonConstructorNullClause.{ABSENT_ON_NULL, NULL_ON_NULL}

/**
 * [[CallGenerator]] for `JSON_OBJECT`.
 *
 * `JSON_OBJECT` returns a character string. However, this creates an issue when nesting calls to
 * this function with the intention of creating a nested JSON structure. Instead of a nested JSON
 * object, a JSON string would be inserted, i.e.
 * `JSON_OBJECT(KEY 'K' VALUE JSON_OBJECT(KEY 'A' VALUE 'B'))` would result in
 * `{"K":"{\"A\":\"B\"}"}` instead of the intended `{"K":{"A":"B"}}`. We remedy this by treating
 * nested calls to this function differently and inserting the value as a raw node instead of as a
 * string node.
 */
class JsonObjectCallGen(call: RexCall) extends CallGenerator {
  private def jsonUtils = className[SqlJsonUtils]

  override def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {

    val nodeTerm = newName("node")
    ctx.addReusableMember(s"${className[ObjectNode]} $nodeTerm = $jsonUtils.createObjectNode();")

    val nullNodeTerm = newName("nullNode")
    ctx.addReusableMember(s"${className[NullNode]} $nullNodeTerm = $nodeTerm.nullNode();")

    val onNull = getOnNullBehavior(operands.head)
    val populateNodeCode = operands.zipWithIndex.drop(1).grouped(2).map {
      case Seq((keyExpr, _), (valueExpr, valueIdx)) =>
        val valueTerm = createNodeTerm(ctx, nodeTerm, valueExpr, call.operands.get(valueIdx))

        onNull match {
          case NULL_ON_NULL =>
            s"""
               |if (${valueExpr.nullTerm}) {
               |    $nodeTerm.set(${keyExpr.resultTerm}.toString(), $nullNodeTerm);
               |} else {
               |    $nodeTerm.set(${keyExpr.resultTerm}.toString(), $valueTerm);
               |}
               |""".stripMargin
          case ABSENT_ON_NULL =>
            s"""
               |if (!${valueExpr.nullTerm}) {
               |    $nodeTerm.set(${keyExpr.resultTerm}.toString(), $valueTerm);
               |}
               |""".stripMargin
        }
    }.mkString

    val resultTerm = newName("result")
    val resultTermType = primitiveTypeTermForType(returnType)
    val resultCode = s"""
       |${operands.map(_.code).mkString}
       |
       |$nodeTerm.removeAll();
       |$populateNodeCode
       |
       |$resultTermType $resultTerm =
       |    $BINARY_STRING.fromString($jsonUtils.serializeJson($nodeTerm));
       |""".stripMargin

    GeneratedExpression(resultTerm, "false", resultCode, returnType)
  }
}
