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

package org.apache.flink.table.plan.stream.sql

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.util.TableTestBase

import org.junit.{Before, Test}

class MiniBatchIntervalInferTest extends TableTestBase {
  private val util = streamTestUtil()

  @Before
  def setup(): Unit = {
    util.addDataStream[(Int, String, Long)]("MyTable1", 'a, 'b, 'c, 'proctime, 'rowtime)
    util.addDataStream[(Int, String, Long)]("MyTable2", 'a, 'b, 'c, 'proctime, 'rowtime)
  }

  @Test
  def testMiniBatchOnly(): Unit = {
    util.tableEnv.getConfig.getConf
        .setLong(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY, 1000L)
    val sql = "SELECT b, COUNT(DISTINCT a), MAX(b), SUM(c) FROM MyTable1 GROUP BY b"
    util.verifyPlan(sql)
  }

  @Test
  def testRedundantWatermarkDefinition(): Unit = {
    util.tableEnv.getConfig.getConf
        .setLong(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY, 1000L)
    util.addTableWithWatermark("MyTable3", util.tableEnv.scan("MyTable1"), "rowtime", 0)
    val sql = "SELECT b, COUNT(DISTINCT a), MAX(b), SUM(c) FROM MyTable3 GROUP BY b"
    util.verifyPlan(sql)
  }

  @Test
  def testWindowWithEarlyFire(): Unit = {
    util.tableEnv.getConfig.getConf
        .setLong(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY, 1000L)
    util.tableEnv.getConfig.withEarlyFireInterval(Time.milliseconds(500))
    util.addTableWithWatermark("MyTable3", util.tableEnv.scan("MyTable1"), "rowtime", 0)
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
    util.tableEnv.getConfig.getConf
        .setLong(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY, 3000L)
    util.addTableWithWatermark("MyTable3", util.tableEnv.scan("MyTable1"), "rowtime", 0)
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
  def testWindowJoinWithMiniBatch(): Unit = {
    util.addTableWithWatermark("LeftT", util.tableEnv.scan("MyTable1"), "rowtime", 0)
    util.addTableWithWatermark("RightT", util.tableEnv.scan("MyTable2"), "rowtime", 0)
    util.tableEnv.getConfig.getConf.setLong(
      TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY, 1000L)

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
    util.addTableWithWatermark("MyTable3", util.tableEnv.scan("MyTable1"), "rowtime", 0)
    util.tableEnv.getConfig.getConf.setLong(
      TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY, 1000L)

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

  @Test(expected = classOf[NotImplementedError])
  // TODO remove the exception after TableImpl implements createTemporalTableFunction
  def testTemporalTableFunctionJoinWithMiniBatch(): Unit = {
    util.addTableWithWatermark("Orders", util.tableEnv.scan("MyTable1"), "rowtime", 0)
    util.addTableWithWatermark("RatesHistory", util.tableEnv.scan("MyTable2"), "rowtime", 0)

    util.tableEnv.getConfig.getConf.setLong(
      TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY, 1000L)

    util.addFunction(
      "Rates",
      util.tableEnv.scan("RatesHistory").createTemporalTableFunction("rowtime", "b"))

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
    util.addTableWithWatermark("LeftT", util.tableEnv.scan("MyTable1"), "rowtime", 0)
    util.addTableWithWatermark("RightT", util.tableEnv.scan("MyTable2"), "rowtime", 0)
    util.tableEnv.getConfig.getConf.setLong(
      TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY, 1000L)

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
    util.addTableWithWatermark("LeftT", util.tableEnv.scan("MyTable1"), "rowtime", 0)
    util.addTableWithWatermark("RightT", util.tableEnv.scan("MyTable2"), "rowtime", 0)
    util.tableEnv.getConfig.getConf.setLong(
      TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY, 6000L)

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
    util.addTableWithWatermark("RightT", util.tableEnv.scan("MyTable2"), "rowtime", 0)
    util.tableEnv.getConfig.getConf.setLong(
      TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY, 6000L)

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
}
