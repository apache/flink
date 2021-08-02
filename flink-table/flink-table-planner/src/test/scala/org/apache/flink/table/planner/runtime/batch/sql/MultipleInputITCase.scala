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

package org.apache.flink.table.planner.runtime.batch.sql

import org.apache.flink.api.common.BatchShuffleMode
import org.apache.flink.api.common.typeinfo.BasicTypeInfo.{INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.ExecutionOptions
import org.apache.flink.streaming.runtime.io.MultipleInputSelectionHandler
import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.types.Row

import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters
import org.junit.{Before, Test}

import scala.collection.JavaConversions._
import scala.util.Random

/**
 * IT cases for multiple input.
 *
 * <p>This test class works by comparing the results with and without multiple input.
 * The following IT cases are picked from
 * [[org.apache.flink.table.planner.plan.batch.sql.MultipleInputCreationTest]].
 */
@RunWith(classOf[Parameterized])
class MultipleInputITCase(shuffleMode: BatchShuffleMode) extends BatchTestBase {

  @Before
  override def before(): Unit = {
    super.before()

    registerCollection(
      "x",
      MultipleInputITCase.dataX,
      MultipleInputITCase.rowType,
      "a, b, c, nx",
      MultipleInputITCase.nullables)
    registerCollection(
      "y",
      MultipleInputITCase.dataY,
      MultipleInputITCase.rowType,
      "d, e, f, ny",
      MultipleInputITCase.nullables)
    registerCollection(
      "z",
      MultipleInputITCase.dataZ,
      MultipleInputITCase.rowType,
      "g, h, i, nz",
      MultipleInputITCase.nullables)
    registerCollection(
      "t",
      MultipleInputITCase.dataT,
      MultipleInputITCase.rowType,
      "a, b, c, nt",
      MultipleInputITCase.nullables)

    tEnv.getConfig.getConfiguration.set(ExecutionOptions.BATCH_SHUFFLE_MODE, shuffleMode)
  }

  @Test
  def testBasicMultipleInput(): Unit = {
    checkMultipleInputResult(
      """
        |SELECT * FROM
        |  (SELECT a FROM x INNER JOIN y ON x.a = y.d) T1
        |  INNER JOIN
        |  (SELECT d FROM y INNER JOIN t ON y.d = t.a) T2
        |  ON T1.a = T2.d
        |""".stripMargin)
  }

  @Test
  def testManyMultipleInputs(): Unit = {
    checkMultipleInputResult(
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
        |""".stripMargin)
  }

  @Test
  def testJoinWithAggAsProbe(): Unit = {
    checkMultipleInputResult(
      """
        |WITH T AS (SELECT a, d FROM x INNER JOIN y ON x.a = y.d)
        |SELECT * FROM
        |  (SELECT a, COUNT(*) AS cnt FROM T GROUP BY a) T1
        |  LEFT JOIN
        |  (SELECT d, SUM(a) AS sm FROM T GROUP BY d) T2
        |  ON T1.a = T2.d
        |""".stripMargin
    )
  }

  @Test
  def testNoPriorityConstraint(): Unit = {
    checkMultipleInputResult(
      """
        |SELECT * FROM x
        |  INNER JOIN y ON x.a = y.d
        |  INNER JOIN t ON x.a = t.a
        |""".stripMargin
    )
  }

  @Test
  def testRelatedInputs(): Unit = {
    checkMultipleInputResult(
      """
        |WITH
        |  T1 AS (SELECT x.a AS a, y.d AS b FROM y LEFT JOIN x ON y.d = x.a),
        |  T2 AS (
        |    SELECT a, b FROM
        |      (SELECT a, b FROM T1)
        |      UNION ALL
        |      (SELECT x.a AS a, x.b AS b FROM x))
        |SELECT * FROM T2 LEFT JOIN t ON T2.a = t.a
        |""".stripMargin
    )
  }

  @Test
  def testRelatedInputsWithAgg(): Unit = {
    checkMultipleInputResult(
      """
        |WITH
        |  T1 AS (SELECT x.a AS a, y.d AS b FROM y LEFT JOIN x ON y.d = x.a),
        |  T2 AS (
        |    SELECT a, b FROM
        |      (SELECT a, b FROM T1)
        |      UNION ALL
        |      (SELECT COUNT(x.a) AS a, x.b AS b FROM x GROUP BY x.b))
        |SELECT * FROM T2 LEFT JOIN t ON T2.a = t.a
        |""".stripMargin
    )
  }

  @Test
  def testDeadlockCausedByExchangeInAncestor(): Unit = {
    checkMultipleInputResult(
      """
        |WITH T1 AS (
        |  SELECT x1.*, x2.a AS k, (x1.b + x2.b) AS v
        |  FROM x x1 LEFT JOIN x x2 ON x1.a = x2.a WHERE x2.a > 0)
        |SELECT x.a, x.b, T1.* FROM x LEFT JOIN T1 ON x.a = T1.k WHERE x.a > 0 AND T1.v = 0
        |""".stripMargin
    )
  }

  @Test
  def testMaxSupportedInputs(): Unit = {
    val rowType = new RowTypeInfo(INT_TYPE_INFO, STRING_TYPE_INFO)
    val data = Seq(BatchTestBase.row(1, "test"))
    val nullables: Array[Boolean] = Array(true, true)
    registerCollection("left_table", data, rowType, "a, b", nullables)
    registerCollection("right_table", data, rowType, "c, d", nullables)

    val numJoins = MultipleInputSelectionHandler.MAX_SUPPORTED_INPUT_COUNT - 1

    val sql = new StringBuilder("SELECT t0.a, t0.b")
    for (i <- 1 to numJoins) {
      sql.append(s", t$i.c, t$i.d")
    }
    sql.append(" from left_table as t0")
    for (i <- 1 to numJoins) {
      sql.append(s" left join right_table as t$i on t0.a = t$i.c and t$i.c = 1")
    }
    checkMultipleInputResult(sql.toString())
  }

  def checkMultipleInputResult(sql: String): Unit = {
    tEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_MULTIPLE_INPUT_ENABLED, false)
    val expected = executeQuery(sql)
    tEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_MULTIPLE_INPUT_ENABLED, true)
    checkResult(sql, expected)
  }
}

object MultipleInputITCase {

  @Parameters(name = "shuffleMode: {0}")
  def parameters: Array[BatchShuffleMode] =
    Array(BatchShuffleMode.ALL_EXCHANGES_BLOCKING, BatchShuffleMode.ALL_EXCHANGES_PIPELINED)

  def generateRandomData(): Seq[Row] = {
    val data = new java.util.ArrayList[Row]()
    val numRows = Random.nextInt(30)
    lazy val strs = Seq("multiple", "input", "itcase")
    for (_ <- 0 until numRows) {
      data.add(BatchTestBase.row(
        Random.nextInt(3),
        Random.nextInt(3).longValue(),
        strs(Random.nextInt(3)),
        Random.nextInt(3)))
    }
    data
  }

  lazy val rowType = new RowTypeInfo(INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO, INT_TYPE_INFO)
  lazy val nullables: Array[Boolean] = Array(true, true, true, true)

  lazy val dataX: Seq[Row] = generateRandomData()
  lazy val dataY: Seq[Row] = generateRandomData()
  lazy val dataZ: Seq[Row] = generateRandomData()
  lazy val dataT: Seq[Row] = generateRandomData()
}
