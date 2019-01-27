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

import org.apache.flink.table.api.types.InternalType
import org.apache.flink.table.codegen.calls.ScalarOperators.generateNot
import org.apache.flink.table.codegen.{CodeGeneratorContext, GeneratedExpression}

/**
  * Inverts the boolean value of a CallGenerator result.
  */
class NotCallGen(callGenerator: CallGenerator) extends CallGenerator {

  override def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: InternalType,
      nullCheck: Boolean): GeneratedExpression = {
    val expr = callGenerator.generate(ctx, operands, returnType, nullCheck)
    generateNot(ctx, nullCheck, expr)
  }

}
