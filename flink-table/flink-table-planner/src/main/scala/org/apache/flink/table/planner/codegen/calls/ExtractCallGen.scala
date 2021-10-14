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

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.codegen.GenerateUtils.generateCallIfArgsNotNull
import org.apache.flink.table.planner.codegen.{CodeGenException, CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.types.logical.{LogicalType, LogicalTypeRoot}

import org.apache.calcite.avatica.util.{TimeUnit, TimeUnitRange}

import java.lang.reflect.Method

class ExtractCallGen(method: Method)
  extends MethodCallGen(method) {

  override def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    val unit = getEnum(operands.head).asInstanceOf[TimeUnitRange].startUnit
    val tpe = operands(1).resultType
    unit match {
      case TimeUnit.YEAR |
           TimeUnit.MONTH |
           TimeUnit.DAY |
           TimeUnit.QUARTER |
           TimeUnit.DOY |
           TimeUnit.DOW |
           TimeUnit.WEEK |
           TimeUnit.CENTURY |
           TimeUnit.MILLENNIUM =>
        tpe.getTypeRoot match {
          case LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE =>
            return generateCallIfArgsNotNull(ctx, returnType, operands) {
              (terms) =>
                s"""
                   |${qualifyMethod(method)}(${terms.head},
                   |    ${terms(1)}.getMillisecond() / ${TimeUnit.DAY.multiplier.intValue()})
                   |""".stripMargin
            }

          case LogicalTypeRoot.DATE =>
            return super.generate(ctx, operands, returnType)

          case LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE =>
            throw new ValidationException("unit " + unit + " can not be applied to time variable")

          case _ => // do nothing
        }

      case _ => // do nothing
    }
    generateCallIfArgsNotNull(ctx, returnType, operands) {
      (terms) => {
        val factor = getFactor(unit)
        val longTerm = tpe.getTypeRoot match {
          case LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE => s"${terms(1)}.getMillisecond()"
          case _ => s"${terms(1)}"
        }
        unit match {
          case TimeUnit.QUARTER =>
            s"""
               |(($longTerm % $factor) - 1) / ${unit.multiplier.intValue()} + 1
               |""".stripMargin
          case TimeUnit.MICROSECOND =>
            tpe.getTypeRoot match {
              case LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE =>
                val nanoOfMilliTerm = s"${terms(1)}.getNanoOfMillisecond()"
                s"""
                   |($longTerm % $factor) * 1000 + $nanoOfMilliTerm / 1000
                 """.stripMargin
              case _ =>
                throw new ValidationException(
                  "unit " + unit + " can not be applied to " + tpe.toString + " variable")
            }
          case TimeUnit.NANOSECOND =>
            tpe.getTypeRoot match {
              case LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE =>
                val nanoOfMilliTerm = s"${terms(1)}.getNanoOfMillisecond()"
                s"""
                   |($longTerm % $factor) * 1000000 + $nanoOfMilliTerm
                 """.stripMargin
              case _ =>
                throw new ValidationException(
                  "unit " + unit + " can not be applied to " + tpe.toString + " variable")
            }
          case _ =>
            if (factor == 1) {
              s"""
                 |$longTerm / ${unit.multiplier.intValue()}
                 |""".stripMargin
            } else {
              s"""
                 |($longTerm % $factor) / ${unit.multiplier.intValue()}
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
      case TimeUnit.MILLISECOND | TimeUnit.MICROSECOND | TimeUnit.NANOSECOND =>
        TimeUnit.SECOND.multiplier.longValue()
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
