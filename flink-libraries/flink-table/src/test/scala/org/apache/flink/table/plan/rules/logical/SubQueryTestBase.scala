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
package org.apache.flink.table.plan.rules.logical

import org.apache.flink.table.calcite.CalciteConfig
import org.apache.flink.table.plan.optimize._
import org.apache.flink.table.plan.rules.FlinkBatchExecRuleSets
import org.apache.flink.table.util._

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.sql2rel.SqlToRelConverter

class SubQueryTestBase(fieldsNullable: Boolean) extends TableTestBase {
  val util: BatchTableTestUtil = nullableBatchTestUtil(fieldsNullable)

  def buildPrograms(): FlinkChainedPrograms[BatchOptimizeContext] = {
    val programs = new FlinkChainedPrograms[BatchOptimizeContext]()

    programs.addLast(
      "semi_join",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkBatchExecRuleSets.SEMI_JOIN_RULES)
        .build()
    )

    // convert sub-queries before query decorrelation
    programs.addLast(
      "subquery",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkBatchExecRuleSets.TABLE_SUBQUERY_RULES)
        .build())

    // decorrelate
    programs.addLast("decorrelate", new FlinkDecorrelateProgram)
    programs
  }

  val builder = CalciteConfig.createBuilder(util.tableEnv.getConfig.getCalciteConfig)
  builder.replaceSqlToRelConverterConfig(
    SqlToRelConverter.configBuilder()
      .withTrimUnusedFields(false)
      .withConvertTableAccess(false)
      .withExpand(false)
      .withInSubQueryThreshold(3)
      .build())
    .replaceBatchPrograms(buildPrograms())

  util.tableEnv.config.setCalciteConfig(builder.build())
}
