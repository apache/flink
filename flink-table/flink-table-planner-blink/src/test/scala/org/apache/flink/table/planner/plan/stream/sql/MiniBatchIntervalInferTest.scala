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
import org.apache.flink.table.planner.utils.{TableConfigUtils, TableTestBase}
import org.apache.flink.table.types.logical.{BigIntType, IntType, VarCharType}

import org.junit.{Before, Test}

class MiniBatchIntervalInferTest extends TableTestBase {
  private val util = streamTestUtil()

  val STRING = new VarCharType(VarCharType.MAX_LENGTH)
  val LONG = new BigIntType()
  val INT = new IntType()

  @Before
  def setup(): Unit = {
    util.addDataStream[(Int, String, Long)](
      "MyTable1", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)
    util.addDataStream[(Int, String, Long)](
      "MyTable2", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true)
  }

  @Test
  def testMiniBatchOnly(): Unit = {
    util.tableEnv.getConfig.getConfiguration
        .setString(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, "1 s")
    val sql = "SELECT b, COUNT(DISTINCT a), MAX(b), SUM(c) FROM MyTable1 GROUP BY b"
    util.verifyPlan(sql)
  }

  @Test
  def testRedundantWatermarkDefinition(): Unit = {
    util.tableEnv.getConfig.getConfiguration
        .setString(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, "1 s")
    util.addTableWithWatermark("MyTable3", util.tableEnv.from("MyTable1"), "rowtime", 0)
    val sql = "SELECT b, COUNT(DISTINCT a), MAX(b), SUM(c) FROM MyTable3 GROUP BY b"
    util.verifyPlan(sql)
  }

  @Test
  def testWindowWithEarlyFire(): Unit = {
    val tableConfig = util.tableEnv.getConfig
    tableConfig.getConfiguration
        .setString(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, "1 s")
    withEarlyFireDelay(tableConfig, Time.milliseconds(500))
    util.addTableWithWatermark("MyTable3", util.tableEnv.from("MyTable1"), "rowtime", 0)
    val sql =
      """
        | SELECT b, SUM(cnt)
        | FROM (
        |   SELECT b,
        |     COUNT(a) as cnt,
        |     HOP_START(rowtime, INTERVAL '5' SECOND, INTERVAL '6' SECOND) as w_start,
        |     HOP_END(rowtime, INTERVAL '5' SECOND, INTERVAL '6' SECOND) as w_end
        |   FROM MyTable3
        |   GROUP BY b, HOP(rowtime, INTERVAL '5' SECOND, INTERVAL '6' SECOND)
        | )
        | GROUP BY b
      """.stripMargin
    util.verifyPlan(sql)
  }

  @Test
  def testWindowCascade(): Unit = {
    util.tableEnv.getConfig.getConfiguration
        .setString(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, "3 s")
    util.addTableWithWatermark("MyTable3", util.tableEnv.from("MyTable1"), "rowtime", 0)
    val sql =
      """
        | SELECT b,
        |   SUM(cnt)
        | FROM (
        |   SELECT b,
        |     COUNT(a) as cnt,
        |     TUMBLE_ROWTIME(rowtime, INTERVAL '10' SECOND) as rt
        |   FROM MyTable3
        |   GROUP BY b, TUMBLE(rowtime, INTERVAL '10' SECOND)
        | )
        | GROUP BY b, TUMBLE(rt, INTERVAL '5' SECOND)
      """.stripMargin
    util.verifyPlan(sql)
  }

  @Test
  def testIntervalJoinWithMiniBatch(): Unit = {
    util.addTableWithWatermark("LeftT", util.tableEnv.from("MyTable1"), "rowtime", 0)
    util.addTableWithWatermark("RightT", util.tableEnv.from("MyTable2"), "rowtime", 0)
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, "1 s")

    val sql =
      """
        | SELECT b, COUNT(a)
        | FROM (
        |   SELECT t1.a as a, t1.b as b
        |   FROM
        |     LeftT as t1 JOIN RightT as t2
        |   ON
        |     t1.a = t2.a AND t1.rowtime BETWEEN t2.rowtime - INTERVAL '5' SECOND AND
        |     t2.rowtime + INTERVAL '10' SECOND
        | )
        | GROUP BY b
      """.stripMargin
    util.verifyPlan(sql)
  }

  @Test
  def testRowtimeRowsOverWithMiniBatch(): Unit = {
    util.addTableWithWatermark("MyTable3", util.tableEnv.from("MyTable1"), "rowtime", 0)
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, "1 s")

    val sql =
      """
        | SELECT cnt, COUNT(c)
        | FROM (
        |   SELECT c, COUNT(a)
        |   OVER (PARTITION BY c ORDER BY rowtime ROWS BETWEEN 5 preceding AND CURRENT ROW) as cnt
        |   FROM MyTable3
        | )
        | GROUP BY cnt
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testTemporalTableFunctionJoinWithMiniBatch(): Unit = {
    util.addTableWithWatermark("Orders", util.tableEnv.from("MyTable1"), "rowtime", 0)
    util.addTableWithWatermark("RatesHistory", util.tableEnv.from("MyTable2"), "rowtime", 0)

    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, "1 s")

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

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiOperatorNeedsWatermark1(): Unit = {
    // infer result: miniBatchInterval=[Rowtime, 0ms]
    util.addTableWithWatermark("LeftT", util.tableEnv.from("MyTable1"), "rowtime", 0)
    util.addTableWithWatermark("RightT", util.tableEnv.from("MyTable2"), "rowtime", 0)
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, "1 s")

    val sql =
      """
        | SELECT
        |   b, COUNT(a),
        |   TUMBLE_START(rt, INTERVAL '5' SECOND),
        |   TUMBLE_END(rt, INTERVAL '5' SECOND)
        | FROM (
        |   SELECT t1.a as a, t1.b as b, t1.rowtime as rt
        |   FROM
        |     LeftT as t1 JOIN RightT as t2
        |   ON
        |     t1.a = t2.a AND t1.rowtime BETWEEN t2.rowtime - INTERVAL '5' SECOND AND
        |     t2.rowtime + INTERVAL '10' SECOND
        | )
        | GROUP BY b,TUMBLE(rt, INTERVAL '5' SECOND)
      """.stripMargin
    util.verifyPlan(sql)
  }

  @Test
  def testMultiOperatorNeedsWatermark2(): Unit = {
    util.addTableWithWatermark("LeftT", util.tableEnv.from("MyTable1"), "rowtime", 0)
    util.addTableWithWatermark("RightT", util.tableEnv.from("MyTable2"), "rowtime", 0)
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, "6 s")

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
        |    FROM LeftT
        |    GROUP BY b, TUMBLE(rowtime, INTERVAL '5' SECOND)
        |  ) as t1
        |  JOIN
        |  (
        |    SELECT b,
        |     COUNT(a) as a,
        |     HOP_ROWTIME(rowtime, INTERVAL '5' SECOND, INTERVAL '6' SECOND) as rt
        |    FROM RightT
        |    GROUP BY b, HOP(rowtime, INTERVAL '5' SECOND, INTERVAL '6' SECOND)
        |  ) as t2
        |  ON
        |    t1.a = t2.a AND t1.rt BETWEEN t2.rt - INTERVAL '5' SECOND AND
        |    t2.rt + INTERVAL '10' SECOND
        | )
      """.stripMargin
    util.verifyPlan(sql)
  }

  @Test
  def testMultiOperatorNeedsWatermark3(): Unit = {
    util.addTableWithWatermark("RightT", util.tableEnv.from("MyTable2"), "rowtime", 0)
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, "6 s")

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
        |      FROM RightT
        |      GROUP BY b, HOP(rowtime, INTERVAL '5' SECOND, INTERVAL '6' SECOND)
        |    )
        |    GROUP BY b
        |  ) as t2
        |  ON t1.a = t2.a
      """.stripMargin
    util.verifyPlan(sql)
  }

  /**
    * Test watermarkInterval trait infer among optimize block
    */
  @Test
  def testMultipleWindowAggregates(): Unit = {
    val stmtSet = util.tableEnv.createStatementSet()
    util.addDataStream[(Int, Long, String)]("T1", 'id1, 'rowtime.rowtime, 'text)
    util.addDataStream[(Int, Long, Int, String, String)](
      "T2",
      'id2, 'rowtime.rowtime, 'cnt, 'name, 'goods)
    util.addTableWithWatermark("T3", util.tableEnv.from("T1"), "rowtime", 0)
    util.addTableWithWatermark("T4", util.tableEnv.from("T2"), "rowtime", 0)

    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, "500 ms")
    util.tableEnv.getConfig.getConfiguration.setLong(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 300L)

    val table1 = util.tableEnv.sqlQuery(
      """
        |SELECT id1, T3.rowtime AS ts, text
        |  FROM T3, T4
        |WHERE id1 = id2
        |      AND T3.rowtime > T4.rowtime - INTERVAL '5' MINUTE
        |      AND T3.rowtime < T4.rowtime + INTERVAL '3' MINUTE
      """.stripMargin)
    util.tableEnv.registerTable("TempTable1", table1)

    val table2 = util.tableEnv.sqlQuery(
      """
        |SELECT id1,
        |    LISTAGG(text, '#') as text,
        |    TUMBLE_ROWTIME(ts, INTERVAL '6' SECOND) as ts
        |FROM TempTable1
        |GROUP BY TUMBLE(ts, INTERVAL '6' SECOND), id1
      """.stripMargin)
    util.tableEnv.registerTable("TempTable2", table2)

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
      .setString(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, "1 s")
    val sql =
      """
        |SELECT long,
        |  COUNT(str) as cnt,
        |  TUMBLE_END(rowtime, INTERVAL '10' SECOND) as rt
        |FROM T1
        |GROUP BY long, TUMBLE(rowtime, INTERVAL '10' SECOND)
      """.stripMargin
    util.verifyPlan(sql)
  }

  @Test
  def testOverWindowMiniBatchOnDataStreamWithRowTime(): Unit = {
    util.addDataStream[(Long, Int, String)]("T1", 'long, 'int, 'str, 'rowtime.rowtime)
    util.tableEnv.getConfig.getConfiguration
      .setString(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, "3 s")
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

    util.verifyPlan(sql)
  }

  private def withEarlyFireDelay(tableConfig: TableConfig, interval: Time): Unit = {
    val intervalInMillis = interval.toMilliseconds
    val preEarlyFireInterval = TableConfigUtils.getMillisecondFromConfigDuration(
      tableConfig, TABLE_EXEC_EMIT_EARLY_FIRE_DELAY)
    if (preEarlyFireInterval != null && (preEarlyFireInterval != intervalInMillis)) { //
      // earlyFireInterval of the two query config is not equal and not the default
      throw new RuntimeException("Currently not support different earlyFireInterval configs in " +
        "one job")
    }
    tableConfig.getConfiguration.setBoolean(TABLE_EXEC_EMIT_EARLY_FIRE_ENABLED, Boolean.box(true))
    tableConfig.getConfiguration.setString(
      TABLE_EXEC_EMIT_EARLY_FIRE_DELAY, intervalInMillis + " ms")
  }
}
