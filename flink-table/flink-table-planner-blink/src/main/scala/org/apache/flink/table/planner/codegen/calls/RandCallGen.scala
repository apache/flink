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

import org.apache.flink.table.planner.codegen.GenerateUtils.generateCallIfArgsNotNull
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.types.logical.{DoubleType, IntType, LogicalType}

/**
  * Generates a random function call.
  * Supports: RAND([seed]) and RAND_INTEGER([seed, ] bound)
  */
class RandCallGen(isRandInteger: Boolean, hasSeed: Boolean) extends CallGenerator {

  override def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    val randField = if (hasSeed) {
      if (operands.head.literal) {
        ctx.addReusableRandom(Some(operands.head))
      } else {
        s"(new java.util.Random(${operands.head.resultTerm}))"
      }
    } else {
      ctx.addReusableRandom(None)
    }

    if (isRandInteger) {
      generateCallIfArgsNotNull(ctx, new IntType(), operands) { terms =>
        s"$randField.nextInt(${terms.last})"
      }
    } else {
      generateCallIfArgsNotNull(ctx, new DoubleType(), operands) { _ =>
        s"$randField.nextDouble()"
      }
    }
  }

}
