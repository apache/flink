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
import org.apache.flink.table.planner.codegen.OperatorCodeGenerator.generateCollect
import org.apache.flink.table.planner.plan.fusion.OpFusionCodegenSpecBase

import java.util

/** The operator fusion codegen spec for virtual root operator. */
class OutputFusionCodegenSpec(operatorCtx: CodeGeneratorContext)
  extends OpFusionCodegenSpecBase(operatorCtx) {

  override def variablePrefix: String = "output"

  override protected def doProcessProduce(codegenCtx: CodeGeneratorContext): Unit =
    fusionContext.getInputFusionContexts.get(0).processProduce(codegenCtx)

  override def doProcessConsume(
      inputId: Int,
      inputVars: util.List[GeneratedExpression],
      row: GeneratedExpression): String = {
    s"""
       |${row.code}
       |${generateCollect(row.resultTerm)}
       |""".stripMargin.trim
  }

  override protected def doEndInputProduce(fusionCtx: CodeGeneratorContext): Unit =
    fusionContext.getInputFusionContexts.get(0).endInputProduce(fusionCtx)

  override def doEndInputConsume(inputId: Int): String = ""
}
