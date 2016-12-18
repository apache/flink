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

import java.lang.reflect.Method

import org.apache.calcite.avatica.util.TimeUnitRange
import org.apache.calcite.avatica.util.TimeUnitRange.{MONTH, YEAR}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo.{BIG_DEC_TYPE_INFO, DOUBLE_TYPE_INFO, FLOAT_TYPE_INFO}
import org.apache.flink.table.codegen.CodeGenUtils.{getEnum, primitiveTypeTermForTypeInfo, qualifyMethod}
import org.apache.flink.table.codegen.calls.CallGenerator.generateCallIfArgsNotNull
import org.apache.flink.table.codegen.{CodeGenerator, GeneratedExpression}

/**
  * Generates floor/ceil function calls.
  */
class FloorCeilCallGen(
    arithmeticMethod: Method,
    temporalMethod: Option[Method] = None)
  extends MultiTypeMethodCallGen(arithmeticMethod) {

  override def generate(
      codeGenerator: CodeGenerator,
      operands: Seq[GeneratedExpression])
    : GeneratedExpression = operands.size match {
    // arithmetic
    case 1 =>
      operands.head.resultType match {
        case FLOAT_TYPE_INFO | DOUBLE_TYPE_INFO | BIG_DEC_TYPE_INFO =>
          super.generate(codeGenerator, operands)
        case _ =>
          operands.head // no floor/ceil necessary
      }

    // temporal
    case 2 =>
      val operand = operands.head
      val unit = getEnum(operands(1)).asInstanceOf[TimeUnitRange]
      val internalType = primitiveTypeTermForTypeInfo(operand.resultType)

      generateCallIfArgsNotNull(codeGenerator.nullCheck, operand.resultType, operands) {
        (terms) =>
          unit match {
            case YEAR | MONTH =>
              s"""
                |($internalType) ${qualifyMethod(temporalMethod.get)}(${terms(1)}, ${terms.head})
                |""".stripMargin
            case _ =>
              s"""
                |${qualifyMethod(arithmeticMethod)}(
                |  ($internalType) ${terms.head},
                |  ($internalType) ${unit.startUnit.multiplier.intValue()})
                |""".stripMargin
          }
      }
  }
}
