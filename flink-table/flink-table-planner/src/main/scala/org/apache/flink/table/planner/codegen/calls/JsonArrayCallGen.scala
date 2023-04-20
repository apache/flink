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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.{ArrayNode, NullNode}
import org.apache.flink.table.api.JsonOnNull
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.planner.codegen.CodeGenUtils.{className, newName, primitiveTypeTermForType, BINARY_STRING}
import org.apache.flink.table.planner.codegen.JsonGenerateUtils.{createNodeTerm, getOnNullBehavior}
import org.apache.flink.table.runtime.functions.SqlJsonUtils
import org.apache.flink.table.types.logical.LogicalType

import org.apache.calcite.rex.RexCall

/** [[CallGenerator]] for `JSON_ARRAY`. */
class JsonArrayCallGen(call: RexCall) extends CallGenerator {
  private def jsonUtils = className[SqlJsonUtils]

  override def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {

    val nodeTerm = newName("node")
    ctx.addReusableMember(s"${className[ArrayNode]} $nodeTerm = $jsonUtils.createArrayNode();")

    val nullNodeTerm = newName("nullNode")
    ctx.addReusableMember(s"${className[NullNode]} $nullNodeTerm = $nodeTerm.nullNode();")

    val onNull = getOnNullBehavior(operands.head)
    val populateNodeCode = operands.zipWithIndex
      .drop(1)
      .map {
        case (elementExpr, elementIdx) =>
          val elementTerm = createNodeTerm(ctx, elementExpr, call.operands.get(elementIdx))

          onNull match {
            case JsonOnNull.NULL =>
              s"""
                 |if (${elementExpr.nullTerm}) {
                 |    $nodeTerm.add($nullNodeTerm);
                 |} else {
                 |    $nodeTerm.add($elementTerm);
                 |}
                 |""".stripMargin
            case JsonOnNull.ABSENT =>
              s"""
                 |if (!${elementExpr.nullTerm}) {
                 |    $nodeTerm.add($elementTerm);
                 |}
                 |""".stripMargin
          }
      }
      .mkString

    val resultTerm = newName("result")
    val resultTermType = primitiveTypeTermForType(returnType)
    val resultCode =
      s"""
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
