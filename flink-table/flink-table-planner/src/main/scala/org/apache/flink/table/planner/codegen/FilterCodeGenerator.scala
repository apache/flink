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
package org.apache.flink.table.planner.codegen

import org.apache.flink.configuration.ReadableConfig
import org.apache.flink.table.runtime.generated.GeneratedFilterCondition
import org.apache.flink.table.types.logical.LogicalType

import org.apache.calcite.rex.RexNode

/**
 * A code generator for generating [[org.apache.flink.table.runtime.generated.FilterCondition]] that
 * can be used in an operator code.
 */
object FilterCodeGenerator {

  /** Generates filter condition runner. */
  def generateFilterCondition(
      tableConfig: ReadableConfig,
      classLoader: ClassLoader,
      filterCondition: RexNode,
      inputType: LogicalType): GeneratedFilterCondition = {
    val ctx = new CodeGeneratorContext(tableConfig, classLoader)
    // should consider null fields
    val exprGenerator =
      new ExprCodeGenerator(ctx, false).bindInput(inputType, CodeGenUtils.DEFAULT_INPUT_TERM)

    val bodyCode = if (filterCondition == null) {
      "return true;"
    } else {
      val condition = exprGenerator.generateExpression(filterCondition)
      s"""
         |${condition.code}
         |return ${condition.resultTerm};
         |""".stripMargin
    }

    FunctionCodeGenerator.generateFilterCondition(ctx, "PreFilterCondition", bodyCode)
  }
}
