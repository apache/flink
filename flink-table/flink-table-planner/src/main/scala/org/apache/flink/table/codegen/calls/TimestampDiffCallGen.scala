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

import org.apache.calcite.avatica.util.DateTimeUtils.MILLIS_PER_DAY
import org.apache.calcite.avatica.util.TimeUnit
import org.apache.calcite.util.BuiltInMethod
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.calls.CallGenerator.generateCallIfArgsNotNull
import org.apache.flink.table.codegen.{CodeGenException, CodeGenerator, GeneratedExpression}

class TimestampDiffCallGen extends CallGenerator {

  override def generate(
      codeGenerator: CodeGenerator,
      operands: Seq[GeneratedExpression])
    : GeneratedExpression = {

    val unit = getEnum(operands.head).asInstanceOf[TimeUnit]
    unit match {
      case TimeUnit.YEAR |
           TimeUnit.MONTH |
           TimeUnit.QUARTER =>
        (operands(1).resultType, operands(2).resultType) match {
          case (SqlTimeTypeInfo.TIMESTAMP, SqlTimeTypeInfo.DATE) =>
            generateCallIfArgsNotNull(codeGenerator.nullCheck, INT_TYPE_INFO, operands) {
              (terms) =>
                s"""
                  |${qualifyMethod(BuiltInMethod.SUBTRACT_MONTHS.method)}(${terms(1)},
                  |  ${terms(2)} * ${MILLIS_PER_DAY}L) / ${unit.multiplier.intValue()}
                  |""".stripMargin
            }

          case (SqlTimeTypeInfo.DATE, SqlTimeTypeInfo.TIMESTAMP) =>
            generateCallIfArgsNotNull(codeGenerator.nullCheck, INT_TYPE_INFO, operands) {
              (terms) =>
                s"""
                  |${qualifyMethod(BuiltInMethod.SUBTRACT_MONTHS.method)}(
                  |${terms(1)} * ${MILLIS_PER_DAY}L, ${terms(2)}) / ${unit.multiplier.intValue()}
                  |""".stripMargin
            }

          case _ =>
            generateCallIfArgsNotNull(codeGenerator.nullCheck, INT_TYPE_INFO, operands) {
              (terms) =>
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
        (operands(1).resultType, operands(2).resultType) match {
          case (SqlTimeTypeInfo.TIMESTAMP, SqlTimeTypeInfo.TIMESTAMP) =>
            generateCallIfArgsNotNull(codeGenerator.nullCheck, INT_TYPE_INFO, operands) {
              (terms) =>
                s"""
                  |(int)((${terms(1)} - ${terms(2)}) / ${unit.multiplier.intValue()})
                  |""".stripMargin
            }

          case (SqlTimeTypeInfo.TIMESTAMP, SqlTimeTypeInfo.DATE) =>
            generateCallIfArgsNotNull(codeGenerator.nullCheck, INT_TYPE_INFO, operands) {
              (terms) =>
                s"""
                  |(int)((${terms(1)} -
                  |  ${terms(2)} * ${MILLIS_PER_DAY}L) / ${unit.multiplier.intValue()})
                  |""".stripMargin
            }

          case (SqlTimeTypeInfo.DATE, SqlTimeTypeInfo.TIMESTAMP) =>
            generateCallIfArgsNotNull(codeGenerator.nullCheck, INT_TYPE_INFO, operands) {
              (terms) =>
                s"""
                  |(int)((${terms(1)} * ${MILLIS_PER_DAY}L -
                  |  ${terms(2)}) / ${unit.multiplier.intValue()})
                  |""".stripMargin
            }

          case (SqlTimeTypeInfo.DATE, SqlTimeTypeInfo.DATE) =>
            generateCallIfArgsNotNull(codeGenerator.nullCheck, INT_TYPE_INFO, operands) {
              (terms) =>
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
