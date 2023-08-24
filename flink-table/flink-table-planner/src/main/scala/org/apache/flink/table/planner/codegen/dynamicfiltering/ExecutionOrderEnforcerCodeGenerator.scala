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
package org.apache.flink.table.planner.codegen.dynamicfiltering

import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, OperatorCodeGenerator}
import org.apache.flink.table.planner.codegen.CodeGenUtils.{DEFAULT_INPUT1_TERM, DEFAULT_INPUT2_TERM}
import org.apache.flink.table.planner.codegen.OperatorCodeGenerator.generateCollect
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExecutionOrderEnforcer
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory
import org.apache.flink.table.types.logical.RowType

/**
 * Operator code generator for ExecutionOrderEnforcer operator. Input1 is the dependent upstream,
 * input2 is the source. see [[BatchExecExecutionOrderEnforcer]] for details.
 */
object ExecutionOrderEnforcerCodeGenerator {
  def gen(
      ctx: CodeGeneratorContext,
      input1Type: RowType,
      input2Type: RowType): CodeGenOperatorFactory[RowData] = {

    new CodeGenOperatorFactory[RowData](
      OperatorCodeGenerator.generateTwoInputStreamOperator(
        ctx,
        "ExecutionOrderEnforcerOperator",
        "",
        s"""
           |${generateCollect(s"$DEFAULT_INPUT2_TERM")}
           |""".stripMargin,
        input1Type,
        input2Type,
        DEFAULT_INPUT1_TERM,
        DEFAULT_INPUT2_TERM,
        None,
        // we cannot pass None or use default here, because this operator must implement BoundedMultiInput
        Some(""),
        Some("")
      ))
  }

}
