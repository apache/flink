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

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.plan.optimize.program.{BatchOptimizeContext, FlinkChainedProgram, FlinkHepRuleSetProgram, FlinkHepRuleSetProgramBuilder, HEP_RULES_EXECUTION_TYPE, StreamOptimizeContext}
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.WeightedAvgWithMerge
import org.apache.flink.table.planner.utils.{BatchTableTestUtil, StreamTableTestUtil, TableTestBase, TableTestUtil}

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.tools.RuleSets

import java.lang.{Boolean => JBoolean}
import java.sql.Timestamp
import java.util.{Collection => JCollection}

import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}

import scala.collection.JavaConversions._

/**
 * Test for [[ProjectWindowTableFunctionTransposeRule]].
 */
@RunWith(classOf[Parameterized])
class ProjectWindowTableFunctionTransposeRuleTest(isStreamingMode: Boolean) extends TableTestBase {
  private var util: TableTestUtil = _

  @Before
  def setup(): Unit = {
    val program = FlinkHepRuleSetProgramBuilder.newBuilder
      .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
      .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
      .add(RuleSets.ofList(ProjectWindowTableFunctionTransposeRule.INSTANCE))
      .build()
    val programName = "rules"
    if (isStreamingMode) {
      val programs = new FlinkChainedProgram[StreamOptimizeContext]()
      programs.addLast(
        programName,
        program.asInstanceOf[FlinkHepRuleSetProgram[StreamOptimizeContext]])
      util = streamTestUtil()
      util.asInstanceOf[StreamTableTestUtil].replaceStreamProgram(programs)
      util.tableEnv.executeSql(
        s"""
           |CREATE TABLE MyTable (
           |  a INT,
           |  b BIGINT,
           |  c STRING NOT NULL,
           |  d DECIMAL(10, 3),
           |  e BIGINT,
           |  rowtime TIMESTAMP(3),
           |  WATERMARK FOR rowtime AS rowtime - INTERVAL '1' SECOND
           |) with (
           |  'connector' = 'values'
           |)
           |""".stripMargin)
    } else {
      val programs = new FlinkChainedProgram[BatchOptimizeContext]()
      programs.addLast(
        programName,
        program.asInstanceOf[FlinkHepRuleSetProgram[BatchOptimizeContext]])
      util = batchTestUtil()
      util.asInstanceOf[BatchTableTestUtil].replaceBatchProgram(programs)
      util.addDataStream[(Int, Long, String, BigDecimal, Long, Timestamp)](
        "MyTable", 'a, 'b, 'c, 'd, 'e, 'rowtime)
    }
  }

  @Test
  def testPruneUnusedColumn(): Unit = {
    val sql =
      """
        |SELECT
        |   window_start,
        |   window_end,
        |   count(*),
        |   sum(d),
        |   max(d) filter (where b > 1000),
        |   count(distinct c) AS uv
        |FROM TABLE(
        |  CUMULATE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |GROUP BY window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testNoColumnToPrune(): Unit = {
    util.addTemporarySystemFunction("weightedAvg", classOf[WeightedAvgWithMerge])

    val sql =
      """
        |SELECT
        |   a,
        |   window_start,
        |   window_end,
        |   count(*),
        |   sum(d),
        |   max(d) filter (where b > 1000),
        |   weightedAvg(b, e) AS wAvg,
        |   count(distinct c) AS uv
        |FROM TABLE(
        |  CUMULATE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |GROUP BY a, window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }
}

object ProjectWindowTableFunctionTransposeRuleTest {
  @Parameterized.Parameters(name = "isStreamingMode={0}")
  def parameters(): JCollection[Array[Object]] = {
    Seq[Array[AnyRef]](Array(JBoolean.TRUE), Array(JBoolean.FALSE))
  }
}
