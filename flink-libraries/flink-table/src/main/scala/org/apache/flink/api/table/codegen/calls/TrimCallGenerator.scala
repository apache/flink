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

import org.apache.calcite.sql.fun.SqlTrimFunction.Flag.{BOTH, LEADING, TRAILING}
import org.apache.calcite.util.BuiltInMethod
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.table.codegen.calls.CallGenerator._
import org.apache.flink.api.table.codegen.{CodeGenerator, GeneratedExpression}

/**
  * Generates a TRIM function call. The first operand determines the type of trimming
  * (see [[org.apache.calcite.sql.fun.SqlTrimFunction.Flag]]). Second operand determines
  * the String to be removed. Third operand is the String to be trimmed.
  */
class TrimCallGenerator(returnType: TypeInformation[_]) extends CallGenerator {

  override def generate(
      codeGenerator: CodeGenerator,
      operands: Seq[GeneratedExpression])
    : GeneratedExpression = {
    val method = BuiltInMethod.TRIM.method
    generateCallIfArgsNotNull(codeGenerator.nullCheck, returnType, operands) {
      (operandResultTerms) =>
        s"""
          |${method.getDeclaringClass.getCanonicalName}.${method.getName}(
          |  ${operandResultTerms.head} == ${BOTH.ordinal()} ||
          |    ${operandResultTerms.head} == ${LEADING.ordinal()},
          |  ${operandResultTerms.head} == ${BOTH.ordinal()} ||
          |    ${operandResultTerms.head} == ${TRAILING.ordinal()},
          |  ${operandResultTerms(1)},
          |  ${operandResultTerms(2)})
          |""".stripMargin
    }
  }

}
