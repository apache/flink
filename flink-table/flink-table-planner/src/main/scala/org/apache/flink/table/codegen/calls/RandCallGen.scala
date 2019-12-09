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
import org.apache.flink.table.codegen.calls.CallGenerator.generateCallIfArgsNotNull
import org.apache.flink.table.codegen.{CodeGenerator, GeneratedExpression}

/**
  * Generates a random function call.
  * Supports: RAND([seed]) and RAND_INTEGER([seed, ] bound)
  */
class RandCallGen(isRandInteger: Boolean, hasSeed: Boolean) extends CallGenerator {

  override def generate(
      codeGenerator: CodeGenerator,
      operands: Seq[GeneratedExpression])
    : GeneratedExpression = {

    val randField = if (hasSeed) {
      if (operands.head.literal) {
        codeGenerator.addReusableRandom(Some(operands.head))
      } else {
        s"(new java.util.Random(${operands.head.resultTerm}))"
      }
    } else {
      codeGenerator.addReusableRandom(None)
    }

    if (isRandInteger) {
      generateCallIfArgsNotNull(codeGenerator.nullCheck, INT_TYPE_INFO, operands) { terms =>
        s"$randField.nextInt(${terms.last})"
      }
    } else {
      generateCallIfArgsNotNull(codeGenerator.nullCheck, DOUBLE_TYPE_INFO, operands) { _ =>
        s"$randField.nextDouble()"
      }
    }
  }

}
