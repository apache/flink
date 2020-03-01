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

import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.codegen.GenerateUtils.generateCallIfArgsNotNull
import org.apache.flink.table.planner.codegen.{CodeGenException, CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.table.types.logical.{IntType, LogicalType}

import org.apache.calcite.avatica.util.DateTimeUtils.MILLIS_PER_DAY
import org.apache.calcite.avatica.util.TimeUnit
import org.apache.calcite.util.BuiltInMethod

class TimestampDiffCallGen extends CallGenerator {

  override def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType)
  : GeneratedExpression = {

    val unit = getEnum(operands.head).asInstanceOf[TimeUnit]
    unit match {
      case TimeUnit.YEAR |
           TimeUnit.MONTH |
           TimeUnit.QUARTER =>
        (operands(1).resultType.getTypeRoot, operands(2).resultType.getTypeRoot) match {
          case (TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITHOUT_TIME_ZONE) =>
            generateCallIfArgsNotNull(ctx, new IntType(), operands) {
              terms =>
                val leftTerm = s"${terms(1)}.getMillisecond()"
                val rightTerm = s"${terms(2)}.getMillisecond()"
                s"""
                   |${qualifyMethod(BuiltInMethod.SUBTRACT_MONTHS.method)}(
                   |  $leftTerm, $rightTerm) / ${unit.multiplier.intValue()}
                 """.stripMargin
            }
          case (TIMESTAMP_WITHOUT_TIME_ZONE, DATE) =>
            generateCallIfArgsNotNull(ctx, new IntType(), operands) {
              terms =>
                val leftTerm = s"${terms(1)}.getMillisecond()"
                s"""
                   |${qualifyMethod(BuiltInMethod.SUBTRACT_MONTHS.method)}($leftTerm,
                   |  ${terms(2)} * ${MILLIS_PER_DAY}L) / ${unit.multiplier.intValue()}
                   |""".stripMargin
            }

          case (DATE, TIMESTAMP_WITHOUT_TIME_ZONE) =>
            generateCallIfArgsNotNull(ctx, new IntType(), operands) {
              terms =>
                val rightTerm = s"${terms(2)}.getMillisecond()"
                s"""
                   |${qualifyMethod(BuiltInMethod.SUBTRACT_MONTHS.method)}(
                   |${terms(1)} * ${MILLIS_PER_DAY}L, $rightTerm) / ${unit.multiplier.intValue()}
                   |""".stripMargin
            }

          case _ =>
            generateCallIfArgsNotNull(ctx, new IntType(), operands) {
              terms =>
                s"""
                   |${qualifyMethod(BuiltInMethod.SUBTRACT_MONTHS.method)}(${terms(1)},
                   |  ${terms(2)}) / ${unit.multiplier.intValue()}
                   |""".stripMargin
            }
        }

      case TimeUnit.WEEK |
           TimeUnit.DAY |
           TimeUnit.HOUR |
           TimeUnit.MINUTE |
           TimeUnit.SECOND =>
        (operands(1).resultType.getTypeRoot, operands(2).resultType.getTypeRoot) match {
          case (TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITHOUT_TIME_ZONE) =>
            generateCallIfArgsNotNull(ctx, new IntType(), operands) {
              terms =>
                val leftTerm = s"${terms(1)}.getMillisecond()"
                val rightTerm = s"${terms(2)}.getMillisecond()"
                s"""
                   |(int)(($leftTerm - $rightTerm) / ${unit.multiplier.intValue()})
                   |""".stripMargin
            }

          case (TIMESTAMP_WITHOUT_TIME_ZONE, DATE) =>
            generateCallIfArgsNotNull(ctx, new IntType(), operands) {
              terms =>
                val leftTerm = s"${terms(1)}.getMillisecond()"
                s"""
                   |(int)(($leftTerm -
                   |  ${terms(2)} * ${MILLIS_PER_DAY}L) / ${unit.multiplier.intValue()})
                   |""".stripMargin
            }

          case (DATE, TIMESTAMP_WITHOUT_TIME_ZONE) =>
            generateCallIfArgsNotNull(ctx, new IntType(), operands) {
              terms =>
                val rightTerm = s"${terms(2)}.getMillisecond()"
                s"""
                   |(int)((${terms(1)} * ${MILLIS_PER_DAY}L -
                   |  $rightTerm) / ${unit.multiplier.intValue()})
                   |""".stripMargin
            }

          case (DATE, DATE) =>
            generateCallIfArgsNotNull(ctx, new IntType(), operands) {
              terms =>
                s"""
                   |(int)((${terms(1)} * ${MILLIS_PER_DAY}L -
                   |  ${terms(2)} * ${MILLIS_PER_DAY}L) / ${unit.multiplier.intValue()})
                   |""".stripMargin
            }
        }

      case _ =>
        throw new CodeGenException(
          "Unit '" + unit + "' can not be applied to the timestamp difference function.")
    }
  }
}
