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

import org.apache.calcite.avatica.util.{TimeUnit, TimeUnitRange}
import org.apache.flink.api.common.typeinfo.{SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.calls.CallGenerator.generateCallIfArgsNotNull
import org.apache.flink.table.codegen.{CodeGenException, CodeGenerator, GeneratedExpression}

class ExtractCallGen(returnType: TypeInformation[_], method: Method)
  extends MethodCallGen(returnType, method) {

  override def generate(codeGenerator: CodeGenerator, operands: Seq[GeneratedExpression])
  : GeneratedExpression = {
    val unit = getEnum(operands.head).asInstanceOf[TimeUnitRange].startUnit
    val tpe = operands(1).resultType
    unit match {
      case TimeUnit.YEAR |
           TimeUnit.MONTH |
           TimeUnit.DAY |
           TimeUnit.QUARTER |
           TimeUnit.DOY |
           TimeUnit.WEEK |
           TimeUnit.CENTURY |
           TimeUnit.MILLENNIUM =>
        tpe match {
          case SqlTimeTypeInfo.TIMESTAMP =>
            return generateCallIfArgsNotNull(codeGenerator.nullCheck, returnType, operands) {
              (terms) =>
                s"""
                   |${qualifyMethod(method)}(${terms.head},
                   |    ${terms(1)} / ${TimeUnit.DAY.multiplier.intValue()})
                   |""".stripMargin
            }

          case SqlTimeTypeInfo.DATE =>
            return super.generate(codeGenerator, operands)

          case _ => // do nothing
        }

      case _ => // do nothing
    }
    generateCallIfArgsNotNull(codeGenerator.nullCheck, returnType, operands) {
      (terms) => {
        val factor = getFactor(unit)
        unit match {
          case TimeUnit.QUARTER =>
            s"""
               |((${terms(1)} % $factor) - 1) / ${unit.multiplier.intValue()} + 1
               |""".stripMargin
          case _ =>
            if (factor == 1) {
              s"""
                 |${terms(1)} / ${unit.multiplier.intValue()}
                 |""".stripMargin
            } else {
              s"""
                 |(${terms(1)} % $factor) / ${unit.multiplier.intValue()}
                 |""".stripMargin
            }
        }
      }
    }
  }

  private def getFactor(unit: TimeUnit): Long = {
    unit match {
      case TimeUnit.DAY =>
        1L
      case TimeUnit.HOUR =>
        TimeUnit.DAY.multiplier.longValue()
      case TimeUnit.MINUTE =>
        TimeUnit.HOUR.multiplier.longValue()
      case TimeUnit.SECOND =>
        TimeUnit.MINUTE.multiplier.longValue()
      case TimeUnit.MONTH =>
        TimeUnit.YEAR.multiplier.longValue()
      case TimeUnit.QUARTER =>
        TimeUnit.YEAR.multiplier.longValue()
      case TimeUnit.YEAR |
           TimeUnit.CENTURY |
           TimeUnit.MILLENNIUM => 1L
      case _ =>
        throw new CodeGenException(s"Unit '$unit' is not supported.")
    }
  }
}
