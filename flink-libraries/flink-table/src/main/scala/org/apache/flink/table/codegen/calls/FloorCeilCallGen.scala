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
import org.apache.calcite.avatica.util.TimeUnitRange._
import org.apache.flink.table.api.types.{DataTypes, DecimalType, InternalType}
import org.apache.flink.table.codegen.CodeGenUtils.{getEnum, primitiveTypeTermForType, qualifyMethod}
import org.apache.flink.table.codegen.calls.CallGenerator.generateCallIfArgsNotNull
import org.apache.flink.table.codegen.{CodeGeneratorContext, GeneratedExpression}

/**
  * Generates floor/ceil function calls.
  */
class FloorCeilCallGen(
    arithmeticMethod: Method,
    temporalMethod: Option[Method] = None)
  extends MethodCallGen(arithmeticMethod) {

  override def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: InternalType,
      nullCheck: Boolean): GeneratedExpression = operands.size match {
    // arithmetic
    case 1 =>
      operands.head.resultType match {
        case DataTypes.FLOAT | DataTypes.DOUBLE =>
          super.generate(ctx, operands, returnType, nullCheck)
        case dt: DecimalType =>
          generateCallIfArgsNotNull(ctx, nullCheck, returnType, operands) {
            (operandResultTerms) =>
              s"${qualifyMethod(arithmeticMethod)}(${operandResultTerms.mkString(", ")})"
          }
        case _ =>
          operands.head // no floor/ceil necessary
      }

    // temporal
    case 2 =>
      val operand = operands.head
      val unit = getEnum(operands(1)).asInstanceOf[TimeUnitRange]
      val internalType = primitiveTypeTermForType(operand.resultType)

      generateCallIfArgsNotNull(ctx, nullCheck, operand.resultType, operands) {
        (terms) =>
          unit match {
            // for Timestamp with timezone info
            case YEAR | QUARTER | MONTH | DAY | HOUR
              if (FunctionGenerator.isFunctionWithTimeZone(
                temporalMethod.getOrElse(arithmeticMethod))) =>
              val timeZone = ctx.addReusableTimeZone()
              s"""
                 |($internalType) ${qualifyMethod(temporalMethod.get)}(${terms(1)},
                 |                                                     ${terms.head},
                 |                                                     $timeZone)
                 |""".stripMargin

            // for Unix Date / Unix Time
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
