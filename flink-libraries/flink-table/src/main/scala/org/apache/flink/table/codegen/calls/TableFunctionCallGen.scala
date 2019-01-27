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

import org.apache.flink.table.api.functions.TableFunction
import org.apache.flink.table.api.types.InternalType
import org.apache.flink.table.codegen.GeneratedExpression.NEVER_NULL
import org.apache.flink.table.codegen.calls.ScalarFunctionCallGen.prepareUDFArgs
import org.apache.flink.table.codegen.{CodeGeneratorContext, GeneratedExpression}

/**
  * Generates a call to user-defined [[TableFunction]].
  *
  * @param tableFunction user-defined [[TableFunction]] that might be overloaded
  */
class TableFunctionCallGen(tableFunction: TableFunction[_]) extends CallGenerator {

  override def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: InternalType,
      nullCheck: Boolean): GeneratedExpression = {
    // convert parameters for function (output boxing)
    val parameters = prepareUDFArgs(ctx, nullCheck, operands, tableFunction)

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
}
