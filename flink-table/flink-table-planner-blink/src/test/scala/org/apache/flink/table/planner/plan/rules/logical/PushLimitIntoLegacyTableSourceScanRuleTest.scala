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
package org.apache.flink.table.planner.plan.rules.logical

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.rel.rules.SortProjectTransposeRule
import org.apache.calcite.tools.RuleSets
import org.apache.flink.table.api.SqlParserException
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalLegacyTableSourceScan, FlinkLogicalSort}
import org.apache.flink.table.planner.plan.optimize.program.{FlinkBatchProgram, FlinkHepRuleSetProgramBuilder, HEP_RULES_EXECUTION_TYPE}
import org.apache.flink.table.planner.utils.{TableConfigUtils, TableTestBase}
import org.junit.{Before, Test}

/**
 * Test for [[PushLimitIntoLegacyTableSourceScanRule]].
 */
class PushLimitIntoLegacyTableSourceScanRuleTest extends TableTestBase {
  protected val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    util.buildBatchProgram(FlinkBatchProgram.DEFAULT_REWRITE)
    val calciteConfig = TableConfigUtils.getCalciteConfig(util.tableEnv.getConfig)
    calciteConfig.getBatchProgram.get.addLast(
      "rules",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(RuleSets.ofList(PushLimitIntoLegacyTableSourceScanRule.INSTANCE,
          SortProjectTransposeRule.INSTANCE,
          // converts calcite rel(RelNode) to flink rel(FlinkRelNode)
          FlinkLogicalSort.BATCH_CONVERTER,
          FlinkLogicalLegacyTableSourceScan.CONVERTER))
        .build()
    )

    val ddl =
      s"""
         |CREATE TABLE LimitTable (
         |  a int,
         |  b bigint,
         |  c string
         |) WITH (
         |  'connector.type' = 'TestLimitableTableSource',
         |  'is-bounded' = 'true'
         |)
       """.stripMargin
    util.tableEnv.executeSql(ddl)
  }

  @Test(expected = classOf[SqlParserException])
  def testLimitWithNegativeOffset(): Unit = {
    util.verifyPlan("SELECT a, c FROM LimitTable LIMIT 10 OFFSET -1")
  }

  @Test(expected = classOf[SqlParserException])
  def testNegativeLimitWithoutOffset(): Unit = {
    util.verifyPlan("SELECT a, c FROM LimitTable LIMIT -1")
  }

  @Test(expected = classOf[SqlParserException])
  def testMysqlLimit(): Unit = {
    util.verifyPlan("SELECT a, c FROM LimitTable LIMIT 1, 10")
  }

  @Test
  def testCanPushdownLimitWithoutOffset(): Unit = {
    util.verifyPlan("SELECT a, c FROM LimitTable LIMIT 5")
  }

  @Test
  def testCanPushdownLimitWithOffset(): Unit = {
    util.verifyPlan("SELECT a, c FROM LimitTable LIMIT 10 OFFSET 1")
  }

  @Test
  def testCanPushdownFetchWithOffset(): Unit = {
    util.verifyPlan("SELECT a, c FROM LimitTable OFFSET 10 ROWS FETCH NEXT 10 ROWS ONLY")
  }

  @Test
  def testCanPushdownFetchWithoutOffset(): Unit = {
    util.verifyPlan("SELECT a, c FROM LimitTable FETCH FIRST 10 ROWS ONLY")
  }

  @Test
  def testCannotPushDownWithoutLimit(): Unit = {
    util.verifyPlan("SELECT a, c FROM LimitTable OFFSET 10")
  }

  @Test
  def testCannotPushDownWithoutFetch(): Unit = {
    util.verifyPlan("SELECT a, c FROM LimitTable OFFSET 10 ROWS")
  }

  @Test
  def testCannotPushDownWithOrderBy(): Unit = {
    val sqlQuery = "SELECT a, c FROM LimitTable ORDER BY c LIMIT 10"
    util.verifyPlan(sqlQuery)
  }
}
