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

import java.nio.charset.StandardCharsets

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.table.codegen.CodeGenUtils.newName
import org.apache.flink.table.codegen.calls.CallGenerator.generateCallWithStmtIfArgsNotNull
import org.apache.flink.table.codegen.{CodeGenerator, GeneratedExpression}
import org.apache.flink.table.utils.EncodingUtils

class HashCalcCallGen(algName: String) extends CallGenerator {

  override def generate(
      codeGenerator: CodeGenerator,
      operands: Seq[GeneratedExpression])
    : GeneratedExpression = {

    val (initStmt, md) = operands.size match {

      // for function calls of MD5, SHA1, SHA224, SHA256, SHA384, SHA512
      case 1 =>
        (None, codeGenerator.addReusableMessageDigest(algName))

      // for function calls of SHA2 with constant bit length
      case 2 if operands(1).literal =>
        (None, codeGenerator.addReusableSha2MessageDigest(operands(1)))

      // for function calls of SHA2 with variable bit length
      case 2 =>
        val messageDigest = newName("messageDigest")
        val bitLen = operands(1).resultTerm
        val init =
          s"""
            |final java.security.MessageDigest $messageDigest;
            |if ($bitLen == 224 || $bitLen == 256 || $bitLen == 384 || $bitLen == 512) {
            |  try {
            |    $messageDigest = java.security.MessageDigest.getInstance("SHA-" + $bitLen);
            |  } catch (java.security.NoSuchAlgorithmException e) {
            |    throw new RuntimeException(
            |      "Algorithm for 'SHA-" + $bitLen + "' is not available.", e);
            |  }
            |} else {
            |  throw new RuntimeException("Unsupported algorithm.");
            |}
            |""".stripMargin
        (Some(init), messageDigest)
    }

    generateCallWithStmtIfArgsNotNull(codeGenerator.nullCheck, STRING_TYPE_INFO, operands) {
      (terms) =>
        val auxiliaryStmt =
          s"""
            |${initStmt.getOrElse("")}
            |$md.update(${terms.head}
            |  .getBytes(${classOf[StandardCharsets].getCanonicalName}.UTF_8));
            |""".stripMargin
        val result = s"${classOf[EncodingUtils].getCanonicalName}.hex($md.digest())"
        (Some(auxiliaryStmt), result)
    }
  }
}
