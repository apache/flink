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
package org.apache.flink.table.planner.plan.fusion

import org.apache.flink.table.data.{BoxedWrapperRowData, RowData}
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, ExprCodeGenerator}

import java.util.Collections

/** The base class for physical operators that support fusion codegen. */
abstract class OpFusionCodegenSpecBase(opCodegenCtx: CodeGeneratorContext)
  extends OpFusionCodegenSpec {

  protected var fusionContext: OpFusionContext = _

  private lazy val exprCodeGenerator = new ExprCodeGenerator(opCodegenCtx, false)

  override def setup(opFusionContext: OpFusionContext): Unit = {
    fusionContext = opFusionContext
  }

  override def getCodeGeneratorContext(): CodeGeneratorContext = opCodegenCtx

  override def getExprCodeGenerator: ExprCodeGenerator = exprCodeGenerator

  override def usedInputColumns(inputId: Int): java.util.Set[Integer] = Collections.emptySet()

  override def getInputRowDataClass(inputId: Int): Class[_ <: RowData] = {
    // default use BoxedWrapperRowData
    classOf[BoxedWrapperRowData]
  }

}
