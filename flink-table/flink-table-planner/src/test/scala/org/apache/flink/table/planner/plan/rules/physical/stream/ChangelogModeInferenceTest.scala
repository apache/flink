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

import org.apache.flink.api.common.time.Time
import org.apache.flink.table.api.{ExplainDetail, _}
import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.planner.plan.optimize.RelNodeBlockPlanBuilder
import org.apache.flink.table.planner.plan.optimize.program.FlinkChangelogModeInferenceProgram
import org.apache.flink.table.planner.utils.{AggregatePhaseStrategy, TableTestBase}

import org.junit.{Before, Test}

/**
 * Tests for [[FlinkChangelogModeInferenceProgram]].
 */
class ChangelogModeInferenceTest extends TableTestBase {

  private val util = streamTestUtil()

  @Before
  def before(): Unit = {
    util.addTable(
      """
        |CREATE TABLE MyTable (
        | word STRING,
        | number INT
        |) WITH (
        | 'connector' = 'COLLECTION',
        | 'is-bounded' = 'false'
        |)
      """.stripMargin)

    util.addTable(
      """
        |CREATE TABLE Orders (
        | amount INT,
        | currency STRING,
        | rowtime TIMESTAMP(3),
        | proctime AS PROCTIME(),
        | WATERMARK FOR rowtime AS rowtime
        |) WITH (
        | 'connector' = 'COLLECTION',
        | 'is-bounded' = 'false'
        |)
      """.stripMargin)
    util.addTable(
      """
        |CREATE TABLE ratesHistory (
        | currency STRING,
        | rate INT,
        | rowtime TIMESTAMP(3),
        | WATERMARK FOR rowtime AS rowtime
        |) WITH (
        |  'connector' = 'COLLECTION',
        |  'is-bounded' = 'false',
        |  'changelog-mode' = 'I'
        |)
      """.stripMargin)
    util.addTable(
      " CREATE VIEW DeduplicatedView AS SELECT currency, rate, rowtime FROM " +
        "  (SELECT *, " +
        "          ROW_NUMBER() OVER (PARTITION BY currency ORDER BY rowtime DESC) AS rowNum " +
        "   FROM ratesHistory" +
        "  ) T " +
        "  WHERE rowNum = 1")

    util.addTable(
      """
        |CREATE TABLE ratesChangelogStream (
        | currency STRING,
        | rate INT,
        | rowtime TIMESTAMP(3),
        | WATERMARK FOR rowtime as rowtime,
        | PRIMARY KEY(currency) NOT ENFORCED
        |) WITH (
        |  'connector' = 'values',
        |  'changelog-mode' = 'I,UA,UB,D'
        |)
      """.stripMargin)
  }

  @Test
  def testSelect(): Unit = {
    util.verifyRelPlan("SELECT word, number FROM MyTable", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testOneLevelGroupBy(): Unit = {
    // one level unbounded groupBy
    util.verifyRelPlan(
      "SELECT COUNT(number) FROM MyTable GROUP BY word", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testTwoLevelGroupByLocalGlobalOff(): Unit = {
    // two level unbounded groupBy
    val sql =
      """
        |SELECT cnt, COUNT(cnt) AS frequency FROM (
        |  SELECT word, COUNT(number) as cnt FROM MyTable GROUP BY word
        |) GROUP BY cnt
      """.stripMargin
    util.verifyRelPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testTwoLevelGroupByLocalGlobalOn(): Unit = {
      util.enableMiniBatch()
      util.tableEnv.getConfig.setIdleStateRetentionTime(Time.hours(1), Time.hours(2))
      util.tableEnv.getConfig.getConfiguration.setString(
        OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY,
        AggregatePhaseStrategy.TWO_PHASE.toString)
    // two level unbounded groupBy
    val sql =
      """
        |SELECT cnt, COUNT(cnt) AS frequency FROM (
        |  SELECT word, COUNT(number) as cnt FROM MyTable GROUP BY word
        |) GROUP BY cnt
      """.stripMargin
    util.verifyRelPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testTemporalJoinWithDeduplicateView(): Unit = {
    val sql =
      """
        |SELECT * FROM Orders AS o
        | JOIN DeduplicatedView FOR SYSTEM_TIME AS OF o.rowtime AS r
        | ON o.currency = r.currency
      """.stripMargin
    util.verifyRelPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testTemporalJoinWithChangelog(): Unit = {
    val sql =
      """
        |SELECT * FROM Orders AS o
        | JOIN ratesChangelogStream FOR SYSTEM_TIME AS OF o.rowtime AS r
        | ON o.currency = r.currency
      """.stripMargin
    util.verifyRelPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testGroupByWithUnion(): Unit = {
    util.addTable(
      """
        |CREATE TABLE MyTable2 (
        | word STRING,
        | cnt INT
        |) WITH (
        | 'connector' = 'COLLECTION',
        | 'is-bounded' = 'false'
        |)
      """.stripMargin)

    val sql =
      """
        |SELECT cnt, COUNT(cnt) AS frequency FROM (
        |   SELECT word, COUNT(number) AS cnt FROM MyTable GROUP BY word
        |   UNION ALL
        |   SELECT word, cnt FROM MyTable2
        |) GROUP BY cnt
      """.stripMargin
    util.verifyRelPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testPropagateUpdateKindAmongRelNodeBlocks(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      RelNodeBlockPlanBuilder.TABLE_OPTIMIZER_REUSE_OPTIMIZE_BLOCK_WITH_DIGEST_ENABLED,
      true)
    util.addTable(
      """
        |create table sink1 (
        |  a INT,
        |  b VARCHAR
        |) with (
        |  'connector' = 'values',
        |  'sink-insert-only' = 'false'
        |)
        |""".stripMargin)
    util.addTable(
      """
        |create table sink2 (
        |  a INT,
        |  b VARCHAR,
        |  primary key (b) not enforced
        |) with (
        |  'connector' = 'values',
        |  'sink-insert-only' = 'false'
        |)
        |""".stripMargin)

    util.tableEnv.executeSql(
      """
        |CREATE VIEW v1 AS
        |SELECT
        |  SUM(number) AS number, word
        |FROM MyTable
        |GROUP BY word
        |""".stripMargin)

    util.tableEnv.executeSql(
      """
        |CREATE VIEW v2 AS
        |SELECT number + 1 AS number, word FROM v1
        |UNION ALL
        |SELECT number - 1 AS number, word FROM v1
        |""".stripMargin)

    val statementSet = util.tableEnv.createStatementSet()
    // sink1 requires UB
    statementSet.addInsertSql(
      """
        |INSERT INTO sink1 SELECT number, word FROM v2 WHERE word > 'a'
        |""".stripMargin)
    // sink2 requires UB
    statementSet.addInsertSql(
      """
        |INSERT INTO sink1 SELECT number, word FROM v2 WHERE word < 'a'
        |""".stripMargin)
    // sink3 does not require UB
    statementSet.addInsertSql(
      """
        |INSERT INTO sink2 SELECT * FROM v1
        |""".stripMargin)
    util.verifyRelPlan(statementSet, ExplainDetail.CHANGELOG_MODE)
  }
}
