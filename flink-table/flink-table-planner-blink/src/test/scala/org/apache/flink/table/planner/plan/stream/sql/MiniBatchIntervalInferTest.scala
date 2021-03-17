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

package org.apache.flink.table.planner.plan.stream.sql

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.planner.plan.utils.WindowEmitStrategy.{TABLE_EXEC_EMIT_EARLY_FIRE_DELAY, TABLE_EXEC_EMIT_EARLY_FIRE_ENABLED}
import org.apache.flink.table.planner.utils.TableTestBase
import org.apache.flink.table.types.logical.{BigIntType, IntType, VarCharType}

import java.time.Duration

import org.junit.{Before, Test}

class MiniBatchIntervalInferTest extends TableTestBase {
  private val util = streamTestUtil()

  val STRING = new VarCharType(VarCharType.MAX_LENGTH)
  val LONG = new BigIntType()
  val INT = new IntType()

  @Before
  def setup(): Unit = {
    util.addDataStream[(Int, String, Long)](
      "MyDataStream1", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)
    util.addDataStream[(Int, String, Long)](
      "MyDataStream2", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)

    // register tables using DDL
    util.addTable(
      s"""
         |CREATE TABLE MyTable1 (
         |  `a` INT,
         |  `b` STRING,
         |  `c` BIGINT,
         |  proctime AS PROCTIME(),
         |  rowtime TIMESTAMP(3)
         |) WITH (
         |  'connector' = 'values'
         |)
         |""".stripMargin)
    util.addTable(
      s"""
         |CREATE TABLE wmTable1 (
         |  WATERMARK FOR rowtime AS rowtime
         |) LIKE MyTable1 (INCLUDING ALL)
         |""".stripMargin)
    util.addTable(
      s"""
         |CREATE TABLE wmTable2 (
         |  WATERMARK FOR rowtime AS rowtime
         |) LIKE MyTable1 (INCLUDING ALL)
         |""".stripMargin)

    // enable mini-batch
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true)
  }

  @Test
  def testMiniBatchOnly(): Unit = {
    util.tableEnv.getConfig.getConfiguration
      .set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofSeconds(1))
    val sql = "SELECT b, COUNT(DISTINCT a), MAX(b), SUM(c) FROM MyTable1 GROUP BY b"
    util.verifyExecPlan(sql)
  }

  @Test
  def testMiniBatchOnlyForDataStream(): Unit = {
    util.tableEnv.getConfig.getConfiguration
        .set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofSeconds(1))
    val sql = "SELECT b, COUNT(DISTINCT a), MAX(b), SUM(c) FROM MyDataStream1 GROUP BY b"
    util.verifyExecPlan(sql)
  }

  @Test
  def testRedundantWatermarkDefinition(): Unit = {
    util.tableEnv.getConfig.getConfiguration
        .set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofSeconds(1))
    val sql = "SELECT b, COUNT(DISTINCT a), MAX(b), SUM(c) FROM wmTable1 GROUP BY b"
    util.verifyExecPlan(sql)
  }

  @Test
  def testWindowWithEarlyFire(): Unit = {
    val tableConfig = util.tableEnv.getConfig
    tableConfig.getConfiguration
        .set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofSeconds(1))
    withEarlyFireDelay(tableConfig, Time.milliseconds(500))
    val sql =
      """
        | SELECT b, SUM(cnt)
        | FROM (
        |   SELECT b,
        |     COUNT(a) as cnt,
        |     HOP_START(rowtime, INTERVAL '5' SECOND, INTERVAL '6' SECOND) as w_start,
        |     HOP_END(rowtime, INTERVAL '5' SECOND, INTERVAL '6' SECOND) as w_end
        |   FROM wmTable1
        |   GROUP BY b, HOP(rowtime, INTERVAL '5' SECOND, INTERVAL '6' SECOND)
        | )
        | GROUP BY b
      """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testWindowCascade(): Unit = {
    util.tableEnv.getConfig.getConfiguration
        .set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofSeconds(3))
    val sql =
      """
        | SELECT b,
        |   SUM(cnt)
        | FROM (
        |   SELECT b,
        |     COUNT(a) as cnt,
        |     TUMBLE_ROWTIME(rowtime, INTERVAL '10' SECOND) as rt
        |   FROM wmTable1
        |   GROUP BY b, TUMBLE(rowtime, INTERVAL '10' SECOND)
        | )
        | GROUP BY b, TUMBLE(rt, INTERVAL '5' SECOND)
      """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testIntervalJoinWithMiniBatch(): Unit = {
    util.tableEnv.getConfig.getConfiguration.set(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofSeconds(1))

    val sql =
      """
        | SELECT b, COUNT(a)
        | FROM (
        |   SELECT t1.a as a, t1.b as b
        |   FROM
        |     wmTable1 as t1 JOIN wmTable2 as t2
        |   ON
        |     t1.a = t2.a AND t1.rowtime BETWEEN t2.rowtime - INTERVAL '5' SECOND AND
        |     t2.rowtime + INTERVAL '10' SECOND
        | )
        | GROUP BY b
      """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testRowtimeRowsOverWithMiniBatch(): Unit = {
    util.tableEnv.getConfig.getConfiguration.set(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofSeconds(1))

    val sql =
      """
        | SELECT cnt, COUNT(c)
        | FROM (
        |   SELECT c, COUNT(a)
        |   OVER (PARTITION BY c ORDER BY rowtime ROWS BETWEEN 5 preceding AND CURRENT ROW) as cnt
        |   FROM wmTable1
        | )
        | GROUP BY cnt
      """.stripMargin

    util.verifyExecPlan(sql)
  }

  @Test
  def testTemporalTableFunctionJoinWithMiniBatch(): Unit = {
    util.addTableWithWatermark("Orders", util.tableEnv.from("MyDataStream1"), "rowtime", 0)
    util.addTableWithWatermark("RatesHistory", util.tableEnv.from("MyDataStream2"), "rowtime", 0)

    util.tableEnv.getConfig.getConfiguration.set(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofSeconds(1))

    util.addFunction(
      "Rates",
      util.tableEnv.from("RatesHistory").createTemporalTableFunction($"rowtime", $"b"))

    val sqlQuery =
      """
        | SELECT r_a, COUNT(o_a)
        | FROM (
        |   SELECT o.a as o_a, r.a as r_a
        |   FROM Orders As o,
        |   LATERAL TABLE (Rates(o.rowtime)) as r
        |   WHERE o.b = r.b
        | )
        | GROUP BY r_a
      """.stripMargin

    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testMultiOperatorNeedsWatermark1(): Unit = {
    // infer result: miniBatchInterval=[Rowtime, 0ms]
    util.tableEnv.getConfig.getConfiguration.set(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofSeconds(1))

    val sql =
      """
        | SELECT
        |   b, COUNT(a),
        |   TUMBLE_START(rt, INTERVAL '5' SECOND),
        |   TUMBLE_END(rt, INTERVAL '5' SECOND)
        | FROM (
        |   SELECT t1.a as a, t1.b as b, t1.rowtime as rt
        |   FROM
        |     wmTable1 as t1 JOIN wmTable2 as t2
        |   ON
        |     t1.a = t2.a AND t1.rowtime BETWEEN t2.rowtime - INTERVAL '5' SECOND AND
        |     t2.rowtime + INTERVAL '10' SECOND
        | )
        | GROUP BY b,TUMBLE(rt, INTERVAL '5' SECOND)
      """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testMultiOperatorNeedsWatermark2(): Unit = {
    util.tableEnv.getConfig.getConfiguration.set(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofSeconds(6))

    val sql =
      """
        | SELECT b, COUNT(a)
        | OVER (PARTITION BY b ORDER BY rt ROWS BETWEEN 5 preceding AND CURRENT ROW)
        | FROM (
        |  SELECT t1.a as a, t1.b as b, t1.rt as rt
        |  FROM
        |  (
        |    SELECT b,
        |     COUNT(a) as a,
        |     TUMBLE_ROWTIME(rowtime, INTERVAL '5' SECOND) as rt
        |    FROM wmTable1
        |    GROUP BY b, TUMBLE(rowtime, INTERVAL '5' SECOND)
        |  ) as t1
        |  JOIN
        |  (
        |    SELECT b,
        |     COUNT(a) as a,
        |     HOP_ROWTIME(rowtime, INTERVAL '5' SECOND, INTERVAL '6' SECOND) as rt
        |    FROM wmTable2
        |    GROUP BY b, HOP(rowtime, INTERVAL '5' SECOND, INTERVAL '6' SECOND)
        |  ) as t2
        |  ON
        |    t1.a = t2.a AND t1.rt BETWEEN t2.rt - INTERVAL '5' SECOND AND
        |    t2.rt + INTERVAL '10' SECOND
        | )
      """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testMultiOperatorNeedsWatermark3(): Unit = {
    util.tableEnv.getConfig.getConfiguration.set(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofSeconds(6))

    val sql =
      """
        |  SELECT t1.a, t1.b
        |  FROM (
        |    SELECT a, COUNT(b) as b FROM MyTable1 GROUP BY a
        |  ) as t1
        |  JOIN (
        |    SELECT b, COUNT(a) as a
        |    FROM (
        |      SELECT b, COUNT(a) as a,
        |         HOP_ROWTIME(rowtime, INTERVAL '5' SECOND, INTERVAL '6' SECOND) as rt
        |      FROM wmTable1
        |      GROUP BY b, HOP(rowtime, INTERVAL '5' SECOND, INTERVAL '6' SECOND)
        |    )
        |    GROUP BY b
        |  ) as t2
        |  ON t1.a = t2.a
      """.stripMargin
    util.verifyExecPlan(sql)
  }

  /**
    * Test watermarkInterval trait infer among optimize block
    */
  @Test
  def testMultipleWindowAggregates(): Unit = {
    val stmtSet = util.tableEnv.createStatementSet()
    util.addTable(
      s"""
         |CREATE TABLE T1 (
         | id1 INT,
         | rowtime TIMESTAMP(3),
         | `text` STRING,
         | WATERMARK FOR rowtime AS rowtime
         |) WITH (
         | 'connector' = 'values'
         |)
         |""".stripMargin)
    util.addTable(
      s"""
         |CREATE TABLE T2 (
         | id2 INT,
         | rowtime TIMESTAMP(3),
         | cnt INT,
         | name STRING,
         | goods STRING,
         | WATERMARK FOR rowtime AS rowtime
         |) WITH (
         | 'connector' = 'values'
         |)
         |""".stripMargin)

    util.tableEnv.getConfig.getConfiguration.set(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofMillis(500))
    util.tableEnv.getConfig.getConfiguration.setLong(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 300L)

    val table1 = util.tableEnv.sqlQuery(
      """
        |SELECT id1, T1.rowtime AS ts, text
        |  FROM T1, T2
        |WHERE id1 = id2
        |      AND T1.rowtime > T2.rowtime - INTERVAL '5' MINUTE
        |      AND T1.rowtime < T2.rowtime + INTERVAL '3' MINUTE
      """.stripMargin)
    util.tableEnv.createTemporaryView("TempTable1", table1)

    val table2 = util.tableEnv.sqlQuery(
      """
        |SELECT id1,
        |    LISTAGG(text, '#') as text,
        |    TUMBLE_ROWTIME(ts, INTERVAL '6' SECOND) as ts
        |FROM TempTable1
        |GROUP BY TUMBLE(ts, INTERVAL '6' SECOND), id1
      """.stripMargin)
    util.tableEnv.createTemporaryView("TempTable2", table2)

  val table3 = util.tableEnv.sqlQuery(
      """
        |SELECT id1,
        |    LISTAGG(text, '*')
        |FROM TempTable2
        |GROUP BY HOP(ts, INTERVAL '12' SECOND, INTERVAL '4' SECOND), id1
      """.stripMargin)
    val appendSink1 = util.createAppendTableSink(Array("a", "b"), Array(INT, STRING))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "appendSink1", appendSink1)
    stmtSet.addInsert("appendSink1", table3)

    val table4 = util.tableEnv.sqlQuery(
      """
        |SELECT id1,
        |    LISTAGG(text, '-')
        |FROM TempTable1
        |GROUP BY TUMBLE(ts, INTERVAL '9' SECOND), id1
      """.stripMargin)
    val appendSink2 = util.createAppendTableSink(Array("a", "b"), Array(INT, STRING))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "appendSink2", appendSink2)
    stmtSet.addInsert("appendSink2", table4)

    val table5 = util.tableEnv.sqlQuery(
      """
        |SELECT id1,
        |    COUNT(text)
        |FROM TempTable2
        |GROUP BY id1
      """.stripMargin)
    val appendSink3 = util.createRetractTableSink(Array("a", "b"), Array(INT, LONG))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "appendSink3", appendSink3)
    stmtSet.addInsert("appendSink3", table5)

    util.verifyExplain(stmtSet)
  }

  @Test
  def testMiniBatchOnDataStreamWithRowTime(): Unit = {
    util.addDataStream[(Long, Int, String)]("T1", 'long, 'int, 'str, 'rowtime.rowtime)
    util.tableEnv.getConfig.getConfiguration
      .set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofSeconds(1))
    val sql =
      """
        |SELECT long,
        |  COUNT(str) as cnt,
        |  TUMBLE_END(rowtime, INTERVAL '10' SECOND) as rt
        |FROM T1
        |GROUP BY long, TUMBLE(rowtime, INTERVAL '10' SECOND)
      """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testOverWindowMiniBatchOnDataStreamWithRowTime(): Unit = {
    util.addDataStream[(Long, Int, String)]("T1", 'long, 'int, 'str, 'rowtime.rowtime)
    util.tableEnv.getConfig.getConfiguration
      .set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofSeconds(3))
    val sql =
      """
        | SELECT cnt, COUNT(`int`)
        | FROM (
        |   SELECT `int`,
        |    COUNT(str) OVER
        |      (PARTITION BY long ORDER BY rowtime ROWS BETWEEN 5 preceding AND CURRENT ROW) as cnt
        |   FROM T1
        | )
        | GROUP BY cnt
      """.stripMargin

    util.verifyExecPlan(sql)
  }

  private def withEarlyFireDelay(tableConfig: TableConfig, interval: Time): Unit = {
    val intervalInMillis = interval.toMilliseconds
    val earlyFireDelay: Duration = tableConfig.getConfiguration
      .getOptional(TABLE_EXEC_EMIT_EARLY_FIRE_DELAY)
      .orElse(null)
    if (earlyFireDelay != null && (earlyFireDelay.toMillis != intervalInMillis)) { //
      // earlyFireInterval of the two query config is not equal and not the default
      throw new RuntimeException("Currently not support different earlyFireInterval configs in " +
        "one job")
    }
    tableConfig.getConfiguration.setBoolean(TABLE_EXEC_EMIT_EARLY_FIRE_ENABLED, Boolean.box(true))
    tableConfig.getConfiguration.set(
      TABLE_EXEC_EMIT_EARLY_FIRE_DELAY, Duration.ofMillis(intervalInMillis))
  }
}
