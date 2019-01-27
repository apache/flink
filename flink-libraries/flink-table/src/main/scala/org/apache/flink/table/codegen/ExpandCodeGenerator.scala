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

package org.apache.flink.table.codegen

import org.apache.flink.table.api.{TableConfig, TableConfigOptions}
import org.apache.flink.table.api.types.InternalType
import org.apache.flink.table.codegen.CodeGenUtils.boxedTypeTermForType
import org.apache.flink.table.codegen.operator.OperatorCodeGenerator
import org.apache.flink.table.codegen.operator.OperatorCodeGenerator.{ELEMENT, STREAM_RECORD}
import org.apache.flink.table.dataformat.{BaseRow, GenericRow}
import org.apache.flink.table.runtime.OneInputSubstituteStreamOperator
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import org.apache.calcite.rex.RexNode

import scala.collection.JavaConversions._
import scala.collection.mutable

object ExpandCodeGenerator {

  def generateExpandOperator(
      ctx: CodeGeneratorContext,
      inputType: InternalType,
      outputType: BaseRowTypeInfo,
      config: TableConfig,
      projects: java.util.List[java.util.List[RexNode]],
      ruleDescription: String,
      retainHeader: Boolean = false): OneInputSubstituteStreamOperator[BaseRow, BaseRow] = {
    val inputTerm = CodeGeneratorContext.DEFAULT_INPUT1_TERM
    val inputTypeTerm = boxedTypeTermForType(inputType)

    val exprGenerator = new ExprCodeGenerator(ctx, false, config.getNullCheck)
      .bindInput(inputType, inputTerm = inputTerm)

    val processCodes = mutable.ListBuffer[String]()
    projects.foreach { project =>
      val projectionExprs = project.map(exprGenerator.generateExpression)
      val projectionResultExpr = exprGenerator.generateResultExpression(
        projectionExprs, outputType.toInternalType, classOf[GenericRow])
      val header = if (retainHeader) {
        s"${projectionResultExpr.resultTerm}.setHeader($inputTerm.getHeader());"
      } else {
        ""
      }

      processCodes += header
      processCodes ++= projectionResultExpr.codeBuffer
      processCodes += OperatorCodeGenerator.generatorCollect(projectionResultExpr.resultTerm)
    }

    val splitFunc = CodeGenUtils.generateSplitFunctionCalls(
      processCodes,
      config.getConf.getInteger(TableConfigOptions.SQL_CODEGEN_LENGTH_MAX),
      "applyExpand",
      "private final void",
      ctx.reuseFieldCode().length,
      defineParams = s"$inputTypeTerm $inputTerm, $STREAM_RECORD $ELEMENT",
      callingParams = s"$inputTerm, $ELEMENT"
    )

    val processCode = if (splitFunc.isSplit) {
      splitFunc.callings.mkString("\n")
    } else {
      processCodes.mkString("\n")
    }

    val endInputCode = ""

    val genOperatorExpression =
      OperatorCodeGenerator.generateOneInputStreamOperator[BaseRow, BaseRow](
        ctx,
        ruleDescription,
        processCode,
        endInputCode,
        inputType,
        config,
        inputTerm = inputTerm,
        splitFunc = splitFunc,
        lazyInputUnboxingCode = false)

    new OneInputSubstituteStreamOperator[BaseRow, BaseRow](
      genOperatorExpression.name,
      genOperatorExpression.code)
  }

}
