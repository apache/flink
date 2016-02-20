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

package org.apache.flink.api.table.codegen.calls

import java.lang.reflect.Method

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.table.codegen.CodeGenUtils._
import org.apache.flink.api.table.codegen.{CodeGenerator, GeneratedExpression}

class MethodCallGenerator(returnType: TypeInformation[_], method: Method) extends CallGenerator {

  override def generate(
      codeGenerator: CodeGenerator,
      operands: Seq[GeneratedExpression])
    : GeneratedExpression = {
    val resultTerm = newName("result")
    val nullTerm = newName("isNull")
    val resultTypeTerm = primitiveTypeTermForTypeInfo(returnType)
    val defaultValue = primitiveDefaultValue(returnType)

    val resultCode = if (codeGenerator.nullCheck) {
      s"""
        |${operands.map(_.code).mkString("\n")}
        |boolean $nullTerm = ${operands.map(_.nullTerm).mkString(" || ")};
        |$resultTypeTerm $resultTerm;
        |if ($nullTerm) {
        |  $resultTerm = $defaultValue;
        |}
        |else {
        |  $resultTerm = ${method.getDeclaringClass.getCanonicalName}.${method.getName}(
        |    ${operands.map(_.resultTerm).mkString(", ")});
        |}
        |""".stripMargin
    }
    else {
      s"""
        |${operands.map(_.code).mkString("\n")}
        |$resultTypeTerm $resultTerm = ${method.getDeclaringClass.getCanonicalName}.
        |  ${method.getName}(${operands.map(_.resultTerm).mkString(", ")});
        |""".stripMargin
    }

    GeneratedExpression(resultTerm, nullTerm, resultCode, returnType)
  }
}
