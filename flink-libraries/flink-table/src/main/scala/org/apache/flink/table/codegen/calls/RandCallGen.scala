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

import org.apache.calcite.util.BuiltInMethod
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.calls.CallGenerator.generateCallIfArgsNotNull
import org.apache.flink.table.codegen.{CodeGenerator, GeneratedExpression}

/**
  * Generates a rand/rand_integer function call.
  * Supports: RAND([seed]) and  RAND_INTEGER([seed, ] bound)
  */
class RandCallGen(method: BuiltInMethod) extends CallGenerator {

  override def generate(
    codeGenerator: CodeGenerator,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {

    method match {
      case BuiltInMethod.RAND =>
        assert(operands.isEmpty)
        val randField = codeGenerator.addReusableRandom(null)
        generateCallIfArgsNotNull(codeGenerator.nullCheck, DOUBLE_TYPE_INFO, operands) {
          _ => s"""$randField.nextDouble()""".stripMargin
        }
      case BuiltInMethod.RAND_SEED =>
        assert(operands.size == 1)
        val (randField, newOperands) = if (operands.head.code.isEmpty) {
          val (randField, initRandomExpr) = genInitRandomExpression(operands.head)
          (randField, Seq(initRandomExpr))
        } else {
          val randField = codeGenerator.addReusableRandom(operands.head)
          (randField, Seq.empty)
        }
        generateCallIfArgsNotNull(codeGenerator.nullCheck, DOUBLE_TYPE_INFO, newOperands) {
          _ => s"""$randField.nextDouble()""".stripMargin
        }
      case BuiltInMethod.RAND_INTEGER =>
        assert(operands.size == 1)
        val randField = codeGenerator.addReusableRandom(null)
        generateCallIfArgsNotNull(codeGenerator.nullCheck, INT_TYPE_INFO, operands) {
          (terms) => s"""$randField.nextInt(${terms.head})""".stripMargin
        }
      case BuiltInMethod.RAND_INTEGER_SEED =>
        assert(operands.size == 2)
        val (randField, newOperands) = if (operands.head.code.isEmpty) {
          val (randField, initRandomExpr) = genInitRandomExpression(operands.head)
          (randField, Seq(initRandomExpr, operands(1)))
        } else {
          val randField = codeGenerator.addReusableRandom(operands.head)
          (randField, Seq(operands(1)))
        }
        generateCallIfArgsNotNull(codeGenerator.nullCheck, INT_TYPE_INFO, newOperands) {
          (terms) => s"""$randField.nextInt(${terms.last})""".stripMargin
        }
    }
  }

  private def genInitRandomExpression(
    seedExpr: GeneratedExpression): (String, GeneratedExpression) = {
    val randField = newName("random")
    val initRandomCode =
      s"""
         |java.util.Random $randField;
         |if(!${seedExpr.nullTerm}) {
         |  $randField = new java.util.Random(${seedExpr.resultTerm});
         |}
         |else {
         |  $randField = new java.util.Random();
         |}
         |""".stripMargin
    val initRandomExpr = GeneratedExpression(
      randField,
      GeneratedExpression.NEVER_NULL,
      initRandomCode,
      TypeInformation.of(classOf[java.util.Random]))
    (randField, initRandomExpr)
  }

}
