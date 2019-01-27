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
package org.apache.flink.table.runtime.batch.sql

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.Types
import org.apache.flink.table.calcite.CalciteConfig
import org.apache.flink.table.plan.optimize._
import org.apache.flink.table.plan.rules.logical.FlinkAggregateJoinTransposeRule
import org.apache.flink.table.runtime.batch.sql.BatchTestBase.row

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.rel.rules._
import org.apache.calcite.tools.RuleSets
import org.junit.{Before, Test}

class DistinctAggOnJoinITCase extends BatchTestBase {

  val t1Name = "t1"
  val t1Types = new RowTypeInfo(Types.STRING, Types.INT)
  val t1FieldNames = "a, b"
  val t1Nullables = Seq(false, false)
  lazy val t1Data = Seq(row("A", 1), row("A", 2), row("B", 2), row("C", 3))

  val t2Name = "t2"
  val t2Types = new RowTypeInfo(Types.INT)
  val t2FieldNames = "c"
  val t2Nullables = Seq(false)
  lazy val t2Data = Seq(row(1), row(1), row(2))

  @Before
  def before(): Unit = {
    registerCollection(t1Name, t1Data, t1Types, t1FieldNames, t1Nullables)
    registerCollection(t2Name, t2Data, t2Types, t2FieldNames, t2Nullables)
    val batchPrograms = FlinkBatchPrograms.buildPrograms(tEnv.getConfig.getConf)
    val logicalProgram = batchPrograms.get(FlinkBatchPrograms.LOGICAL).get
    assert(logicalProgram.isInstanceOf[FlinkRuleSetProgram[_]])
    logicalProgram.asInstanceOf[FlinkRuleSetProgram[_]].remove(
      RuleSets.ofList(FlinkAggregateJoinTransposeRule.EXTENDED))
    batchPrograms.addBefore(
      FlinkBatchPrograms.LOGICAL,
      "aggregateTranspose",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(RuleSets.ofList(
          AggregateProjectMergeRule.INSTANCE,
          FlinkAggregateJoinTransposeRule.EXTENDED
        )).build()
    )
    val calciteConfig = CalciteConfig.createBuilder(tEnv.getConfig.getCalciteConfig)
      .replaceBatchPrograms(batchPrograms).build()
    tEnv.getConfig.setCalciteConfig(calciteConfig)
  }

  @Test
  def testDistinctAgg(): Unit = {
    checkResult("select count (distinct a) from t1, t2 where t1.b = t2.c", Seq(row(2)))
  }

}
