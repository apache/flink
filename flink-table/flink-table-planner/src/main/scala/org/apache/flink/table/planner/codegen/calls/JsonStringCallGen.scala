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

import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.planner.codegen.CodeGenUtils.{className, newName, primitiveTypeTermForType, BINARY_STRING}
import org.apache.flink.table.planner.codegen.JsonGenerateUtils.createNodeTerm
import org.apache.flink.table.runtime.functions.SqlJsonUtils
import org.apache.flink.table.types.logical.LogicalType

import org.apache.calcite.rex.RexCall

/** [[CallGenerator]] for `JSON_STRING`. */
class JsonStringCallGen(call: RexCall) extends CallGenerator {
  private def jsonUtils = className[SqlJsonUtils]

  override def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {

    val valueTerm = createNodeTerm(ctx, operands.head, call.operands.get(0))

    val resultTerm = newName(ctx, "result")
    val resultTermType = primitiveTypeTermForType(returnType)
    val resultCode = s"""
                        |${operands.map(_.code).mkString}
                        |
                        |$resultTermType $resultTerm = null;
                        |if (!${operands.head.nullTerm}) {
                        |    $resultTerm =
                        |        $BINARY_STRING.fromString($jsonUtils.serializeJson($valueTerm));
                        |}
                        |""".stripMargin

    GeneratedExpression(resultTerm, operands.head.nullTerm, resultCode, returnType)
  }
}
