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
package org.apache.flink.table.plan.nodes.physical.batch

import org.apache.flink.table.`type`.RowType
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.codegen.{CodeGeneratorContext, ExprCodeGenerator, FunctionCodeGenerator}
import org.apache.flink.table.generated.GeneratedJoinCondition
import org.apache.flink.table.plan.nodes.common.CommonPhysicalJoin

import org.apache.calcite.rel.core.Join

/**
  * Batch physical RelNode for [[Join]]
  */
trait BatchExecJoinBase extends CommonPhysicalJoin with BatchPhysicalRel {

  private[flink] def generateCondition(
      config: TableConfig,
      leftType: RowType,
      rightType: RowType): GeneratedJoinCondition = {
    val ctx = CodeGeneratorContext(config)
    val exprGenerator = new ExprCodeGenerator(ctx, false)
        .bindInput(leftType)
        .bindSecondInput(rightType)

    val body = if (joinInfo.isEqui) {
      // only equality condition
      "return true;"
    } else {
      val nonEquiPredicates = joinInfo.getRemaining(getCluster.getRexBuilder)
      val condition = exprGenerator.generateExpression(nonEquiPredicates)
      s"""
         |${condition.code}
         |return ${condition.resultTerm};
         |""".stripMargin
    }

    FunctionCodeGenerator.generateJoinCondition(
      ctx,
      "JoinConditionFunction",
      body)
  }
}
