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

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.tools.RuleSets

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.plan.optimize.program.{FlinkChainedProgram, FlinkHepRuleSetProgramBuilder, HEP_RULES_EXECUTION_TYPE, StreamOptimizeContext}
import org.apache.flink.table.planner.utils.{StreamTableTestUtil, TableTestBase, TableTestUtil}
import org.junit.{Before, Test}

/**
 * Test for [[LogicalCorrelateToJoinFromTemporalTableRule]].
 */
class LogicalCorrelateToJoinFromTemporalTableRuleTest extends TableTestBase {

  protected val util: StreamTableTestUtil = streamTestUtil()

  @Before
  def setup(): Unit = {
    util.addTable(
      """
        |CREATE TABLE T1 (
        | id STRING,
        | mount INT,
        | proctime as PROCTIME(),
        | rowtime TIMESTAMP(3),
        | WATERMARK FOR rowtime AS rowtime
        |) WITH (
        | 'connector' = 'COLLECTION',
        | 'is-bounded' = 'false'
        |)
      """.stripMargin)

    //lookup table, CollectionTableSource implements LookupableTableSource interface
    util.addTable(
      """
        |CREATE TABLE T2 (
        | id STRING,
        | rate INT,
        | PRIMARY KEY(id) NOT ENFORCED
        |) WITH (
        | 'connector' = 'COLLECTION',
        | 'is-bounded' = 'false'
        |)
      """.stripMargin)

    // non-lookup table
    util.addTableSource[(String, Int)]("T3", 'id, 'rate, 'rowtime.rowtime())
  }

  @Test
  def testLookupJoinWithFilter(): Unit = {
    setUpCurrentRule(LogicalCorrelateToJoinFromTemporalTableRule.LOOKUP_JOIN_WITH_FILTER)
    util.verifyRelPlan("SELECT * FROM T1 JOIN T2 FOR SYSTEM_TIME AS OF T1.proctime AS dimTable " +
      "ON T1.id = dimTable.id AND dimTable.rate > 10")
  }

  @Test
  def testLeftLookupJoinOnTrue(): Unit = {
    // lookup join also does not support ON TRUE condition in runtime
    setUpCurrentRule(LogicalCorrelateToJoinFromTemporalTableRule.LOOKUP_JOIN_WITHOUT_FILTER)
    util.verifyRelPlan("SELECT * FROM T1 LEFT JOIN T2 FOR SYSTEM_TIME AS OF " +
      "T1.proctime AS dimTable ON TRUE")
  }

  @Test
  def testProcTimeTemporalJoinWithFilter(): Unit = {
    setUpCurrentRule(LogicalCorrelateToJoinFromTemporalTableRule.WITH_FILTER)
    util.verifyRelPlan("SELECT * FROM T1 JOIN T3 FOR SYSTEM_TIME AS OF T1.proctime AS dimTable " +
      "ON T1.id = dimTable.id AND dimTable.rate > 10")
  }

  @Test
  def testRowTimeTemporalJoinWithFilter(): Unit = {
    setUpCurrentRule(LogicalCorrelateToJoinFromTemporalTableRule.WITH_FILTER)
    util.verifyRelPlan("SELECT * FROM T1 JOIN T3 FOR SYSTEM_TIME AS OF T1.rowtime AS dimTable " +
      "ON T1.id = dimTable.id AND dimTable.rate > 10")
  }

  @Test
  def testRowTimeLeftTemporalJoinWithFilter(): Unit = {
    setUpCurrentRule(LogicalCorrelateToJoinFromTemporalTableRule.WITH_FILTER)
    util.verifyRelPlan(
      "SELECT * FROM T1 LEFT JOIN T3 FOR SYSTEM_TIME AS OF T1.rowtime AS dimTable " +
      "ON T1.id = dimTable.id AND dimTable.rate > 10")
  }

  @Test
  def testLookupJoinOnTrue(): Unit = {
    // lookup join also does not support ON TRUE condition in runtime
    setUpCurrentRule(LogicalCorrelateToJoinFromTemporalTableRule.LOOKUP_JOIN_WITHOUT_FILTER)
    util.verifyRelPlan("SELECT * FROM T1 JOIN T2 FOR SYSTEM_TIME AS OF " +
      "T1.proctime AS dimTable ON TRUE")
  }

  @Test
  def testProcTimeTemporalJoinOnTrue(): Unit = {
    setUpCurrentRule(LogicalCorrelateToJoinFromTemporalTableRule.WITHOUT_FILTER)
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Currently the join key in " +
      "Temporal Table Join can not be empty.")
    util.verifyRelPlan("SELECT * FROM T1 JOIN T3 FOR SYSTEM_TIME AS OF T1.rowtime AS dimTable " +
      "ON TRUE")
  }

  @Test
  def testRowTimeTemporalJoinOnTrue(): Unit = {
    setUpCurrentRule(LogicalCorrelateToJoinFromTemporalTableRule.WITHOUT_FILTER)
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Currently the join key in " +
      "Temporal Table Join can not be empty.")
    util.verifyRelPlan("SELECT * FROM T1 JOIN T3 FOR SYSTEM_TIME AS OF T1.proctime AS dimTable " +
      "ON TRUE")
  }

  @Test
  def testRightTemporalJoin(): Unit = {
    setUpCurrentRule(LogicalCorrelateToJoinFromTemporalTableRule.WITH_FILTER)
    expectedException.expect(classOf[AssertionError])
    expectedException.expectMessage("Correlate has invalid join type RIGHT")
    util.verifyRelPlan(
      "SELECT * FROM T1 RIGHT JOIN T3 FOR SYSTEM_TIME AS OF T1.rowtime AS dimTable " +
      "ON T1.id = dimTable.id AND dimTable.rate > 10")
  }

  @Test
  def testFullTemporalJoin(): Unit = {
    setUpCurrentRule(LogicalCorrelateToJoinFromTemporalTableRule.WITH_FILTER)
    expectedException.expect(classOf[AssertionError])
    expectedException.expectMessage("Correlate has invalid join type FULL")
    util.verifyRelPlan(
      "SELECT * FROM T1 FULL JOIN T3 FOR SYSTEM_TIME AS OF T1.rowtime AS dimTable " +
      "ON T1.id = dimTable.id AND dimTable.rate > 10")
  }

  def setUpCurrentRule(rule: RelOptRule): Unit = {
    val programs = new FlinkChainedProgram[StreamOptimizeContext]()
    programs.addLast(
      "rules",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(RuleSets.ofList(rule))
        .build()
    )
    util.replaceStreamProgram(programs)
  }
}
