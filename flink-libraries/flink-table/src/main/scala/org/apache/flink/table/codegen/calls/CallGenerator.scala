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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.{CodeGenerator, GeneratedExpression}

trait CallGenerator {

  def generate(
      codeGenerator: CodeGenerator,
      operands: Seq[GeneratedExpression])
    : GeneratedExpression

}

object CallGenerator {

  def generateCallIfArgsNotNull(
      nullCheck: Boolean,
      returnType: TypeInformation[_],
      operands: Seq[GeneratedExpression])
      (call: (Seq[String]) => String)
    : GeneratedExpression = {
    val resultTerm = newName("result")
    val nullTerm = newName("isNull")
    val resultTypeTerm = primitiveTypeTermForTypeInfo(returnType)
    val defaultValue = primitiveDefaultValue(returnType)

    val resultCode = if (nullCheck && operands.nonEmpty) {
      s"""
        |${operands.map(_.code).mkString("\n")}
        |boolean $nullTerm = ${operands.map(_.nullTerm).mkString(" || ")};
        |$resultTypeTerm $resultTerm;
        |if ($nullTerm) {
        |  $resultTerm = $defaultValue;
        |}
        |else {
        |  $resultTerm = ${call(operands.map(_.resultTerm))};
        |}
        |""".stripMargin
    } else if (nullCheck && operands.isEmpty) {
      s"""
        |${operands.map(_.code).mkString("\n")}
        |boolean $nullTerm = false;
        |$resultTypeTerm $resultTerm = ${call(operands.map(_.resultTerm))};
        |""".stripMargin
    } else{
      s"""
        |boolean $nullTerm = false;
        |${operands.map(_.code).mkString("\n")}
        |$resultTypeTerm $resultTerm = ${call(operands.map(_.resultTerm))};
        |""".stripMargin
    }

    GeneratedExpression(resultTerm, nullTerm, resultCode, returnType)
  }
}
