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

import org.apache.flink.table.planner.codegen.CodeGenUtils.{newNames, primitiveTypeTermForType}
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.runtime.typeutils.TypeCheckUtils.isBinaryString
import org.apache.flink.table.types.logical.LogicalType

import java.nio.charset.StandardCharsets

/**
  * Generates PRINT function call.
  */
class PrintCallGen extends CallGenerator {

  override def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    val Seq(resultTerm, nullTerm) = newNames("result", "isNull")
    val resultTypeTerm = primitiveTypeTermForType(returnType)

    // add logger, prefer name without number suffix to make sure only one definition
    // exists in each operator in case of multiple print(s)
    val logTerm = "logger$"
    ctx.addReusableLogger(logTerm, "_Print$_")

    val charsets = classOf[StandardCharsets].getCanonicalName
    val outputCode = if (isBinaryString(returnType)) {
      s"new String($resultTerm, $charsets.UTF_8)"
    } else {
      s"String.valueOf(${operands(1).resultTerm})"
    }

    val msgCode =
      s"""
         |(${operands.head.nullTerm} ? "null" : ${operands.head.resultTerm}.toString()) +
         |(${operands(1).nullTerm} ? "null" : $outputCode)
       """.stripMargin

    val resultCode =
      s"""
         |${operands(1).code};
         |$resultTypeTerm $resultTerm = ${operands(1).resultTerm};
         |boolean $nullTerm = ${operands(1).nullTerm};
         |org.slf4j.MDC.put("fromUser", "TRUE");
         |$logTerm.error($msgCode);
         |System.out.println($msgCode);
       """.stripMargin

    GeneratedExpression(resultTerm, nullTerm, resultCode, returnType)
  }
}
