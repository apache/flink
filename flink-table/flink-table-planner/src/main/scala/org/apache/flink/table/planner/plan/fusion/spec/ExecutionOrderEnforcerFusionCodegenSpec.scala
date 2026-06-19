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
package org.apache.flink.table.planner.plan.fusion.spec

import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.planner.plan.fusion.{OpFusionCodegenSpecBase, OpFusionContext}

import java.util

/** The operator fusion codegen spec for ExecutionOrderEnforcer. */
class ExecutionOrderEnforcerFusionCodegenSpec(opCodegenCtx: CodeGeneratorContext)
  extends OpFusionCodegenSpecBase(opCodegenCtx) {
  private lazy val sourceInputId = 2

  private var dynamicFilteringInputContext: OpFusionContext = _
  private var sourceInputContext: OpFusionContext = _

  override def setup(opFusionContext: OpFusionContext): Unit = {
    super.setup(opFusionContext)
    val inputContexts = fusionContext.getInputFusionContexts
    assert(inputContexts.size == 2)
    dynamicFilteringInputContext = inputContexts.get(0)
    sourceInputContext = inputContexts.get(1)
  }

  override def variablePrefix(): String = "orderEnforcer"

  override def doProcessProduce(codegenCtx: CodeGeneratorContext): Unit = {
    dynamicFilteringInputContext.processProduce(codegenCtx)
    sourceInputContext.processProduce(codegenCtx)
  }

  override def doEndInputProduce(codegenCtx: CodeGeneratorContext): Unit = {
    dynamicFilteringInputContext.endInputProduce(codegenCtx)
    sourceInputContext.endInputProduce(codegenCtx)
  }

  override def doProcessConsume(
      inputId: Int,
      inputVars: util.List[GeneratedExpression],
      row: GeneratedExpression): String = {
    if (inputId == sourceInputId) {
      s"""
         |// call downstream to consume the row
         |${row.code}
         |${fusionContext.processConsume(null, row.resultTerm)}
         |""".stripMargin
    } else {
      ""
    }
  }

  override def doEndInputConsume(inputId: Int): String = {
    if (inputId == sourceInputId) {
      s"""
         |// call downstream endInput method
         |${fusionContext.endInputConsume()}
         |""".stripMargin
    } else {
      ""
    }
  }
}
