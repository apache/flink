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

import org.apache.flink.table.planner.codegen.CodeGenUtils.{getEnum, primitiveTypeTermForType, qualifyMethod, SQL_TIMESTAMP}
import org.apache.flink.table.planner.codegen.GenerateUtils.generateCallIfArgsNotNull
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.types.logical.{LogicalType, LogicalTypeRoot}

import org.apache.calcite.avatica.util.TimeUnitRange
import org.apache.calcite.avatica.util.TimeUnitRange._

import java.lang.reflect.Method
import java.util.TimeZone

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
      returnType: LogicalType): GeneratedExpression = operands.size match {
    // arithmetic
    case 1 =>
      operands.head.resultType.getTypeRoot match {
        case LogicalTypeRoot.FLOAT | LogicalTypeRoot.DOUBLE =>
          super.generate(ctx, operands, returnType)
        case LogicalTypeRoot.DECIMAL =>
          generateCallIfArgsNotNull(ctx, returnType, operands) {
            operandResultTerms =>
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
      val method = temporalMethod.getOrElse(arithmeticMethod)

      generateCallIfArgsNotNull(ctx, operand.resultType, operands) {
        terms =>
          unit match {
            // for Timestamp with timezone info
            case YEAR | QUARTER | MONTH | DAY | HOUR
              if terms.length + 1 == method.getParameterCount &&
                method.getParameterTypes()(terms.length) == classOf[TimeZone] =>
              val timeZone = ctx.addReusableTimeZone()
              val longTerm = s"${terms.head}.getMillisecond()"
              s"""
                 |$SQL_TIMESTAMP.fromEpochMillis(
                 |  ${qualifyMethod(temporalMethod.get)}(${terms(1)},
                 |  $longTerm,
                 |  $timeZone))
                 |""".stripMargin

            // for Unix Date / Unix Time
            case YEAR | MONTH =>
              operand.resultType.getTypeRoot match {
                case LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE =>
                  val longTerm = s"${terms.head}.getMillisecond()"
                  s"""
                     |$SQL_TIMESTAMP.fromEpochMillis(
                     |  ${qualifyMethod(temporalMethod.get)}(${terms(1)}, $longTerm))
                   """.stripMargin
                case _ =>
                  s"""
                     |($internalType) ${qualifyMethod(temporalMethod.get)}(
                     |  ${terms(1)}, ${terms.head})
                     |""".stripMargin
              }
            case _ =>
              operand.resultType.getTypeRoot match {
                case LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE =>
                  val longTerm = s"${terms.head}.getMillisecond()"
                  s"""
                     |$SQL_TIMESTAMP.fromEpochMillis(${qualifyMethod(arithmeticMethod)}(
                     |  $longTerm,
                     |  (long) ${unit.startUnit.multiplier.intValue()}))
                   """.stripMargin
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
}
