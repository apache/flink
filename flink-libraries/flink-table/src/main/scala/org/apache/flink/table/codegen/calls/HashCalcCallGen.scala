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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.table.codegen.CodeGenUtils.{newName, primitiveTypeTermForTypeInfo}
import org.apache.flink.table.codegen.{CodeGenerator, GeneratedExpression}

class HashCalcCallGen(algName: String) extends CallGenerator {

  override def generate(codeGenerator: CodeGenerator,
                        operands: Seq[GeneratedExpression])
  : GeneratedExpression = {

    val resultTypeTerm = primitiveTypeTermForTypeInfo(STRING_TYPE_INFO)
    val resultTerm = newName("result")
    val nullTerm = newName("isNull")
  val innerCalc =
    s"""
       |  try {
       |    java.security.MessageDigest md =
       |          java.security.MessageDigest.getInstance("$algName");
       |    md.update(${operands(0).resultTerm}.getBytes());
       |    $resultTerm =
       |          org.apache.commons.codec.binary.Hex.encodeHexString(md.digest());
       |    $nullTerm = false;
       |  } catch (java.security.NoSuchAlgorithmException e) {
       |    $nullTerm = true;
       |    $resultTerm = null;
       |  }
     """.stripMargin

    val code = if (codeGenerator.nullCheck) {
               s"""
               |${operands.map(_.code).mkString("\n")}
               |boolean $nullTerm = ${operands.map(_.nullTerm).mkString(" || ")};
               |$resultTypeTerm $resultTerm = null;
               |if (${codeGenerator.nullCheck} && $nullTerm) {
               |  $nullTerm = true;
               |  $resultTerm = null;
               |}
               |else {
               |  $innerCalc
               |}
         """.stripMargin;
    }
    else {
      s"""
         |${operands.map(_.code).mkString("\n")}
         |$resultTypeTerm $resultTerm = null;
         |$innerCalc
       """.stripMargin
    }

    GeneratedExpression(resultTerm, nullTerm, code, STRING_TYPE_INFO)

    }
}
