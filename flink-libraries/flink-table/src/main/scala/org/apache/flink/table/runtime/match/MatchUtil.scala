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

package org.apache.flink.table.runtime.`match`

import java.util

import org.apache.calcite.rel.RelCollation
import org.apache.calcite.rex.RexNode
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.cep.{PatternFlatSelectFunction, PatternSelectFunction}
import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.codegen.MatchCodeGenerator
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row

/**
  * An util class to generate match functions.
  */
object MatchUtil {

  private[flink] def generateIterativeCondition(
    config: TableConfig,
    inputType: RowSchema,
    patternName: String,
    patternNames: Seq[String],
    patternDefinition: RexNode,
    inputTypeInfo: TypeInformation[_]): IterativeCondition[Row] = {

    val generator = new MatchCodeGenerator(
      config, false, inputTypeInfo, patternNames, true, Some(patternName))
    val condition = generator.generateExpression(patternDefinition)
    val body =
      s"""
        |${condition.code}
        |return ${condition.resultTerm};
        |""".stripMargin

    val genCondition = generator.generateIterativeCondition("MatchRecognizeCondition", body)
    new IterativeConditionRunner(genCondition.name, genCondition.code)
  }

  private[flink] def generatePatternSelectFunction(
    config: TableConfig,
    returnType: RowSchema,
    patternNames: Seq[String],
    partitionKeys: util.List[RexNode],
    measures: util.Map[String, RexNode],
    inputTypeInfo: TypeInformation[_]): PatternSelectFunction[Row, CRow] = {

    val generator = new MatchCodeGenerator(config, false, inputTypeInfo, patternNames, false)

    val resultExpression = generator.generateSelectOutputExpression(
      partitionKeys,
      measures,
      returnType)
    val body =
      s"""
        |${resultExpression.code}
        |return ${resultExpression.resultTerm};
        |""".stripMargin

    generator.addReusableStatements()
    val genFunction = generator.generatePatternSelectFunction(
      "MatchRecognizePatternSelectFunction",
      body)
    new PatternSelectFunctionRunner(genFunction.name, genFunction.code)
  }

  private[flink] def generatePatternFlatSelectFunction(
    config: TableConfig,
    returnType: RowSchema,
    patternNames: Seq[String],
    partitionKeys: util.List[RexNode],
    orderKeys: RelCollation,
    measures: util.Map[String, RexNode],
    inputTypeInfo: TypeInformation[_]): PatternFlatSelectFunction[Row, CRow] = {

    val generator = new MatchCodeGenerator(config, false, inputTypeInfo, patternNames, false)

    val resultExpression = generator.generateFlatSelectOutputExpression(
      partitionKeys,
      orderKeys,
      measures,
      returnType)
    val body =
      s"""
        |${resultExpression.code}
        |""".stripMargin

    generator.addReusableStatements()
    val genFunction = generator.generatePatternFlatSelectFunction(
      "MatchRecognizePatternFlatSelectFunction",
      body)
    new PatternFlatSelectFunctionRunner(genFunction.name, genFunction.code)
  }
}
