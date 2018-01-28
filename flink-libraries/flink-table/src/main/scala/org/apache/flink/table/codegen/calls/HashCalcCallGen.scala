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

import org.apache.commons.codec.Charsets
import org.apache.commons.codec.binary.Hex
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.table.codegen.CodeGenUtils.newName
import org.apache.flink.table.codegen.calls.CallGenerator.generateCallWithStmtIfArgsNotNull
import org.apache.flink.table.codegen.{CodeGenerator, GeneratedExpression}

class HashCalcCallGen(algName: String) extends CallGenerator {

  override def generate(
      codeGenerator: CodeGenerator,
      operands: Seq[GeneratedExpression])
    : GeneratedExpression = {

    val algorithm = newName("algorithm")
    val nullTerm = newName("isNull")

    val md = operands.size match {
      case 1 => {
        val code =
          s"""
             |String $algorithm = "$algName";
             """.stripMargin
        val expression = GeneratedExpression(algorithm, nullTerm, code,
          BasicTypeInfo.STRING_TYPE_INFO)
        codeGenerator.addReusableMessageDigest(expression)
      }
      case 2 => {
        val code =
          s"""
             |${operands(1).code}
             |boolean $nullTerm = ${operands(1).nullTerm};
             |String $algorithm;
             |if ($nullTerm) {
             |  $algorithm = "";
             |}
             |else {
             |  $algorithm = "SHA-" + ${operands(1).resultTerm};
             |}
      """.stripMargin
        val expression = GeneratedExpression(algorithm, nullTerm, code,
          BasicTypeInfo.STRING_TYPE_INFO)
        codeGenerator.addReusableMessageDigest(expression)
      }
    }

    generateCallWithStmtIfArgsNotNull(codeGenerator.nullCheck, STRING_TYPE_INFO, operands) {
      (terms) =>
        val auxiliaryStmt =
          s"$md.update(${terms.head}.getBytes(${classOf[Charsets].getCanonicalName}.UTF_8));"
        val result = s"${classOf[Hex].getCanonicalName}.encodeHexString($md.digest())"
        (Some(auxiliaryStmt), result)
    }
  }
}


