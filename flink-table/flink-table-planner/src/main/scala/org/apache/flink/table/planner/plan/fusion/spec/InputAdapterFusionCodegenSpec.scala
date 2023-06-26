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

import org.apache.flink.streaming.api.operators.AbstractInput
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.planner.codegen.CodeGenUtils.{boxedTypeTermForType, className, DEFAULT_INPUT_TERM}
import org.apache.flink.table.planner.codegen.OperatorCodeGenerator.{ELEMENT, STREAM_RECORD}
import org.apache.flink.table.planner.plan.fusion.OpFusionCodegenSpecBase

import java.util

/** The operator fusion codegen spec for input operator. */
class InputAdapterFusionCodegenSpec(opCodegenCtx: CodeGeneratorContext, inputId: Int)
  extends OpFusionCodegenSpecBase(opCodegenCtx) {

  override def variablePrefix: String = "input"

  override protected def doProcessProduce(codegenCtx: CodeGeneratorContext): Unit = {
    val inputTypeTerm = boxedTypeTermForType(fusionContext.getOutputType)
    val inputTerm = DEFAULT_INPUT_TERM + inputId

    val processTerm = "processInput" + inputId
    codegenCtx.addReusableMember(
      s"""
         |public void $processTerm($inputTypeTerm $inputTerm) throws Exception {
         |  ${fusionContext.processConsume(null, inputTerm)}
         |}
         |""".stripMargin
    )
    codegenCtx.addReusableFusionCodegenProcessStatement(
      inputId,
      s"""
         |new ${className[AbstractInput[_, _]]}(this, $inputId) {
         |  @Override
         |  public void processElement($STREAM_RECORD $ELEMENT) throws Exception {
         |    $inputTypeTerm $inputTerm = ($inputTypeTerm) $ELEMENT.getValue();
         |    $processTerm($inputTerm);
         |  }
         |}
         |""".stripMargin
    )
  }

  override def doProcessConsume(
      inputId: Int,
      inputVars: util.List[GeneratedExpression],
      row: GeneratedExpression): String = {
    throw new UnsupportedOperationException
  }

  override protected def doEndInputProduce(codegenCtx: CodeGeneratorContext): Unit = {
    val endInputTerm = "endInput" + inputId
    codegenCtx.addReusableMember(
      s"""
         |public void $endInputTerm() throws Exception {
         |  ${fusionContext.endInputConsume()}
         |}
         |""".stripMargin
    )
    codegenCtx.addReusableFusionCodegenEndInputStatement(inputId, s"$endInputTerm();")
  }

  override def doEndInputConsume(inputId: Int): String = {
    throw new UnsupportedOperationException
  }
}
