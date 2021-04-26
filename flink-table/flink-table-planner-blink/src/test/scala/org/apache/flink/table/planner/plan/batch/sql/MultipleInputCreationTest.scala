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

package org.apache.flink.table.planner.plan.batch.sql

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.connector.source.Boundedness
import org.apache.flink.api.connector.source.mocks.MockSource
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.config.{ExecutionConfigOptions, OptimizerConfigOptions}
import org.apache.flink.table.planner.utils.{TableTestBase, TableTestUtil}

import org.junit.{Before, Test}

class MultipleInputCreationTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def before(): Unit = {
    util.addTableSource[(Int, Long, String, Int)]("x", 'a, 'b, 'c, 'nx)
    util.addTableSource[(Int, Long, String, Int)]("y", 'd, 'e, 'f, 'ny)
    util.addTableSource[(Int, Long, String, Int)]("z", 'g, 'h, 'i, 'nz)
    util.addDataStream[(Int, Long, String)]("t", 'a, 'b, 'c)
    util.tableConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_MULTIPLE_INPUT_ENABLED, true)
  }

  @Test
  def testBasicMultipleInput(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,SortMergeJoin")
    val sql =
      """
        |SELECT * FROM
        |  (SELECT a FROM x INNER JOIN y ON x.a = y.d) T1
        |  INNER JOIN
        |  (SELECT d FROM y INNER JOIN t ON y.d = t.a) T2
        |  ON T1.a = T2.d
        |""".stripMargin
    util.verifyPlan(sql)
  }

  @Test
  def testManyMultipleInputs(): Unit = {
    //       y    z         t    y             t
    //       |    |         |    |             |
    // x -> [J -> J] ----> [J -> J] -> [Agg -> J -\
    //                \                            -> U]
    //                 \-> [J -> J] -> [Agg -> J -/
    //                      |    |             |
    //                      y    t             y
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED, false)
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,SortMergeJoin,SortAgg")
    val sql =
      """
        |WITH
        |  T1 AS (
        |    SELECT a, ny, nz FROM x
        |      LEFT JOIN y ON x.a = y.ny
        |      LEFT JOIN z ON x.a = z.nz),
        |  T2 AS (
        |    SELECT T1.a AS a, t.b AS b, d, T1.ny AS ny, nz FROM T1
        |      LEFT JOIN t ON T1.a = t.a
        |      INNER JOIN y ON T1.a = y.d),
        |  T3 AS (
        |    SELECT T1.a AS a, t.b AS b, d, T1.ny AS ny, nz FROM T1
        |      LEFT JOIN y ON T1.a = y.d
        |      INNER JOIN t ON T1.a = t.a),
        |  T4 AS (SELECT b, SUM(d) AS sd, SUM(ny) AS sy, SUM(nz) AS sz FROM T2 GROUP BY b),
        |  T5 AS (SELECT b, SUM(d) AS sd, SUM(ny) AS sy, SUM(nz) AS sz FROM T3 GROUP BY b)
        |SELECT * FROM
        |  (SELECT t.b, sd, sy, sz FROM T4 LEFT JOIN t ON T4.b = t.b)
        |  UNION ALL
        |  (SELECT y.e, sd, sy, sz FROM T5 LEFT JOIN y ON T5.b = y.e)
        |""".stripMargin
    util.verifyPlan(sql)
  }

  @Test
  def testJoinWithAggAsProbe(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,SortMergeJoin,SortAgg")
    val sql =
      """
        |WITH T AS (SELECT a, d FROM x INNER JOIN y ON x.a = y.d)
        |SELECT * FROM
        |  (SELECT a, COUNT(*) AS cnt FROM T GROUP BY a) T1
        |  LEFT JOIN
        |  (SELECT d, SUM(a) AS sm FROM T GROUP BY d) T2
        |  ON T1.a = T2.d
        |""".stripMargin
    util.verifyPlan(sql)
  }

  @Test
  def testKeepMultipleInputWithOneMemberForChainableSource(): Unit = {
    createChainableTableSource()
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,SortMergeJoin")
    val sql = "SELECT * FROM chainable LEFT JOIN x ON chainable.a = x.a"
    util.verifyPlan(sql)
  }

  @Test
  def testAvoidIncludingUnionFromInputSide(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,SortMergeJoin")
    val sql =
      """
        |SELECT * FROM
        |  (SELECT a FROM (SELECT a FROM x) UNION ALL (SELECT a FROM t)) T1
        |  LEFT JOIN y ON T1.a = y.d
        |""".stripMargin
    util.verifyPlan(sql)
  }

  @Test
  def testIncludeUnionForChainableSource(): Unit = {
    createChainableTableSource()
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,SortMergeJoin")
    val sql =
      """
        |SELECT * FROM
        |  (SELECT a FROM (SELECT a FROM chainable) UNION ALL (SELECT a FROM t)) T1
        |  LEFT JOIN y ON T1.a = y.d
        |""".stripMargin
    util.verifyPlan(sql)
  }

  @Test
  def testAvoidIncludingCalcAfterNonChainableSource(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,SortMergeJoin")
    val sql =
      """
        |SELECT * FROM x
        |  LEFT JOIN y ON x.a = y.d
        |  LEFT JOIN t ON x.a = t.a
        |  WHERE x.b > 10
        |""".stripMargin
    util.verifyPlan(sql)
  }

  @Test
  def testIncludeCalcForChainableSource(): Unit = {
    createChainableTableSource()
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,SortMergeJoin")
    val sql =
      """
        |SELECT * FROM chainable
        |  LEFT JOIN y ON chainable.a = y.d
        |  LEFT JOIN t ON chainable.a = t.a
        |  WHERE chainable.a > 10
        |""".stripMargin
    util.verifyPlan(sql)
  }

  @Test
  def testAvoidIncludingSingleton(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,SortMergeJoin,HashAgg")
    val sql =
      """
        |WITH T1 AS (SELECT COUNT(*) AS cnt FROM z)
        |SELECT * FROM
        |  (SELECT a FROM x INNER JOIN y ON x.a = y.d)
        |  UNION ALL
        |  (SELECT a FROM t FULL JOIN T1 ON t.a > T1.cnt)
        |""".stripMargin
    util.verifyPlan(sql)
  }

  def createChainableTableSource(): Unit = {
    val env = new StreamExecutionEnvironment(new LocalStreamEnvironment())
    val dataStream = env.fromSource(
      new MockSource(Boundedness.BOUNDED, 1),
      WatermarkStrategy.noWatermarks[Integer],
      "chainable").javaStream
    val tableEnv = util.tableEnv
    TableTestUtil.createTemporaryView[Integer](tableEnv, "chainable", dataStream, Some(Array('a)))
  }
}
