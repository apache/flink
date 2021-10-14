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

package org.apache.flink.table.planner.plan.rules.physical.stream

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.tools.RuleSets
import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.optimize.program.FlinkStreamProgram.PHYSICAL
import org.apache.flink.table.planner.plan.optimize.program.{FlinkHepRuleSetProgramBuilder, FlinkVolcanoProgramBuilder, HEP_RULES_EXECUTION_TYPE}
import org.apache.flink.table.planner.plan.rules.logical.FlinkCalcMergeRule
import org.apache.flink.table.planner.plan.rules.physical.FlinkExpandConversionRule
import org.apache.flink.table.planner.utils.{StreamTableTestUtil, TableTestBase}
import org.junit.{Before, Test}

/**
 * Tests for [[ExpandWindowTableFunctionTransposeRule]].
 */
class ExpandWindowTableFunctionTransposeRuleTest extends TableTestBase {
  private val util: StreamTableTestUtil = streamTestUtil()

  @Before
  def setup(): Unit = {
    util.buildStreamProgram(PHYSICAL)
    val chainedProgram = util.getStreamProgram()
    // add the only needed converter rules
    chainedProgram.addLast(
      PHYSICAL,
      FlinkVolcanoProgramBuilder.newBuilder
        .add(
          RuleSets.ofList(
            FlinkExpandConversionRule.STREAM_INSTANCE,
            StreamPhysicalWindowTableFunctionRule.INSTANCE,
            StreamPhysicalWindowAggregateRule.INSTANCE,
            StreamPhysicalCalcRule.INSTANCE,
            StreamPhysicalExpandRule.INSTANCE,
            StreamPhysicalTableSourceScanRule.INSTANCE,
            StreamPhysicalWatermarkAssignerRule.INSTANCE))
        .setRequiredOutputTraits(Array(FlinkConventions.STREAM_PHYSICAL))
        .build())
    // add test rule
    chainedProgram.addLast(
      "test_rule",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(RuleSets.ofList(
          FlinkCalcMergeRule.STREAM_PHYSICAL_INSTANCE,
          ExpandWindowTableFunctionTransposeRule.INSTANCE))
        .build())

    util.replaceStreamProgram(chainedProgram)

    util.tableEnv.executeSql(
      s"""
         |CREATE TABLE MyTable (
         |  a INT,
         |  b BIGINT,
         |  c STRING NOT NULL,
         |  d DECIMAL(10, 3),
         |  e BIGINT,
         |  rowtime TIMESTAMP(3),
         |  proctime as PROCTIME(),
         |  WATERMARK FOR rowtime AS rowtime - INTERVAL '1' SECOND
         |) with (
         |  'connector' = 'values'
         |)
         |""".stripMargin)

    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, true)
  }

  @Test
  def testTumble_DistinctSplitEnabled(): Unit = {
    val sql =
      """
        |SELECT
        |   a,
        |   window_start,
        |   window_end,
        |   count(*),
        |   sum(d),
        |   max(d) filter (where b > 1000),
        |   count(distinct c) AS uv
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |GROUP BY a, window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testHop_DistinctSplitEnabled(): Unit = {
    val sql =
      """
        |SELECT
        |   a,
        |   window_start,
        |   window_end,
        |   count(*),
        |   sum(d),
        |   max(d) filter (where b > 1000),
        |   count(distinct c) AS uv
        |FROM TABLE(
        |   HOP(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
        |GROUP BY a, window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testCumulate_DistinctSplitEnabled(): Unit = {
    val sql =
      """
        |SELECT
        |   a,
        |   window_start,
        |   window_end,
        |   count(*),
        |   sum(d),
        |   max(d) filter (where b > 1000),
        |   count(distinct c) AS uv
        |FROM TABLE(
        |  CUMULATE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |GROUP BY a, window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }
}
