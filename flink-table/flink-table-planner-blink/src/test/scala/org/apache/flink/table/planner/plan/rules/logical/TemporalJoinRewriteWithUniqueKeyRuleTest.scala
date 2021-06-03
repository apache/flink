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

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.planner.plan.optimize.program.FlinkStreamProgram.LOGICAL_REWRITE
import org.apache.flink.table.planner.plan.optimize.program._
import org.apache.flink.table.planner.utils.{StreamTableTestUtil, TableTestBase}

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.tools.RuleSets
import org.junit.{Before, Test}

/**
 * Test for [[TemporalJoinRewriteWithUniqueKeyRule]].
 */
class TemporalJoinRewriteWithUniqueKeyRuleTest extends TableTestBase {
  protected val util: StreamTableTestUtil = streamTestUtil()

  @Before
  def setup(): Unit = {
    util.buildStreamProgram(LOGICAL_REWRITE)
    val chainedProgram = util.getStreamProgram()
    // add test rule
    chainedProgram.addLast(
      "test_rules",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(RuleSets.ofList(
          FlinkLogicalRankRule.INSTANCE,
          CalcRankTransposeRule.INSTANCE,
          RankNumberColumnRemoveRule.INSTANCE,
          CalcSnapshotTransposeRule.INSTANCE,
          TemporalJoinRewriteWithUniqueKeyRule.INSTANCE))
        .build())

    util.replaceStreamProgram(chainedProgram)

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
        | rowtime TIMESTAMP(3),
        | WATERMARK FOR rowtime AS rowtime,
        | PRIMARY KEY(id) NOT ENFORCED
        |) WITH (
        | 'connector' = 'COLLECTION',
        | 'is-bounded' = 'false'
        |)
      """.stripMargin)

    util.addTable(
      """
        |CREATE TABLE T3 (
        | id STRING,
        | rate INT,
        | rowtime TIMESTAMP(3),
        | WATERMARK FOR rowtime AS rowtime
        |) WITH (
        | 'connector' = 'COLLECTION',
        | 'is-bounded' = 'false'
        |)
      """.stripMargin)

    util.addTable(
      " CREATE VIEW DeduplicatedView as SELECT id, rate, rowtime FROM " +
        "  (SELECT *, " +
        "          ROW_NUMBER() OVER (PARTITION BY id ORDER BY rowtime DESC) AS rowNum " +
        "   FROM T3 " +
        "  ) T " +
        "  WHERE rowNum = 1")
  }

  @Test
  def testPrimaryKeyInTemporalJoin(): Unit = {
    util.verifyRelPlan("SELECT * FROM T1 JOIN T2 FOR SYSTEM_TIME AS OF T1.rowtime AS T " +
      "ON T1.id = T.id")
  }

  @Test
  def testInferredPrimaryKeyInTemporalJoin(): Unit = {
    util.verifyRelPlan("SELECT * FROM T1 JOIN DeduplicatedView FOR SYSTEM_TIME AS OF " +
      "T1.rowtime AS T ON T1.id = T.id")
  }

  @Test
  def testPrimaryKeyInTemporalJoinOnTrue(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Currently the join key in " +
      "Temporal Table Join can not be empty.")
    util.verifyRelPlan("SELECT * FROM T1 JOIN T2 FOR SYSTEM_TIME AS OF T1.rowtime AS T " +
      "ON TRUE")
  }

  @Test
  def testInvalidPrimaryKeyInTemporalJoin(): Unit = {
    util.addTable(
      """
        |CREATE TABLE noPkTable (
        | id STRING,
        | rate INT,
        | rowtime TIMESTAMP(3),
        | WATERMARK FOR rowtime AS rowtime
        |) WITH (
        | 'connector' = 'COLLECTION',
        | 'is-bounded' = 'false'
        |)
      """.stripMargin)

    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Temporal Table Join requires primary key in versioned table," +
      " but no primary key can be found. The physical plan is:\nFlinkLogicalJoin(" +
      "condition=[AND(=($0, $4), __INITIAL_TEMPORAL_JOIN_CONDITION($3, $6," +
      " __TEMPORAL_JOIN_LEFT_KEY($0), __TEMPORAL_JOIN_RIGHT_KEY($4)))], joinType=[left])")
    util.verifyRelPlan("SELECT * FROM T1 LEFT JOIN noPkTable FOR SYSTEM_TIME AS OF " +
      "T1.rowtime AS T ON T1.id = T.id")
  }

  @Test
  def testInvalidInferredPrimaryKeyInTemporalJoin(): Unit = {
    util.addTable(
      " CREATE VIEW noPkView as SELECT id, rate, rowtime FROM " +
        "  (SELECT *, " +
        "          ROW_NUMBER() OVER (PARTITION BY id ORDER BY rowtime DESC) AS rowNum " +
        "   FROM T3 " +
        "  ) T " +
        "  WHERE rowNum = 2")

    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Temporal Table Join requires primary key in versioned table," +
      " but no primary key can be found. The physical plan is:\n" +
      "FlinkLogicalJoin(condition=[AND(=($0, $4), __INITIAL_TEMPORAL_JOIN_CONDITION(" +
      "$3, $6, __TEMPORAL_JOIN_LEFT_KEY($0), __TEMPORAL_JOIN_RIGHT_KEY($4)))], joinType=[inner])")
    util.verifyRelPlan("SELECT * FROM T1 JOIN noPkView FOR SYSTEM_TIME AS OF " +
      "T1.rowtime AS T ON T1.id = T.id")
  }

  @Test
  def testInferredPrimaryKeyInTemporalJoinOnTrue(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Currently the join key in " +
      "Temporal Table Join can not be empty.")
    util.verifyRelPlan("SELECT * FROM T1 JOIN DeduplicatedView FOR SYSTEM_TIME AS OF " +
      "T1.rowtime AS T ON TRUE")
  }
}
