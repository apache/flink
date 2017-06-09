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

import org.apache.calcite.sql.fun.SqlTrimFunction.Flag.{BOTH, LEADING, TRAILING}
import org.apache.calcite.util.BuiltInMethod
import org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.calls.CallGenerator._
import org.apache.flink.table.codegen.{CodeGenerator, GeneratedExpression}

/**
  * Generates a TRIM function call.
  *
  * First operand: trim mode (see [[org.apache.calcite.sql.fun.SqlTrimFunction.Flag]])
  * Second operand: String to be removed
  * Third operand: String to be trimmed
  */
class TrimCallGen extends CallGenerator {

  override def generate(
      codeGenerator: CodeGenerator,
      operands: Seq[GeneratedExpression])
    : GeneratedExpression = {
    generateCallIfArgsNotNull(codeGenerator.nullCheck, STRING_TYPE_INFO, operands) {
      (terms) =>
        val leading = compareEnum(terms.head, BOTH) || compareEnum(terms.head, LEADING)
        val trailing = compareEnum(terms.head, BOTH) || compareEnum(terms.head, TRAILING)
        s"""
          |${qualifyMethod(BuiltInMethod.TRIM.method)}(
          |  $leading, $trailing, ${terms(1)}, ${terms(2)})
          |""".stripMargin
    }
  }

}
