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

package org.apache.flink.table.planner.runtime.stream.sql


import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableConfig, Types}
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.{ConcatDistinctAggFunction, WeightedAvg}
import org.apache.flink.table.planner.plan.utils.WindowEmitStrategy.{TABLE_EXEC_EMIT_LATE_FIRE_DELAY, TABLE_EXEC_EMIT_LATE_FIRE_ENABLED}
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.runtime.utils.TimeTestUtil.TimestampAndWatermarkWithOffset
import org.apache.flink.table.planner.runtime.utils._
import org.apache.flink.table.planner.utils.TableConfigUtils.getMillisecondFromConfigDuration
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import java.math.BigDecimal
import java.util.concurrent.TimeUnit

import org.apache.flink.table.api.internal.TableEnvironmentInternal

@RunWith(classOf[Parameterized])
class WindowAggregateITCase(mode: StateBackendMode)
  extends StreamingWithStateTestBase(mode) {

  val data = List(
    (1L, 1, 1d, 1f, new BigDecimal("1"), "Hi", "a"),
    (2L, 2, 2d, 2f, new BigDecimal("2"), "Hallo", "a"),
    (3L, 2, 2d, 2f, new BigDecimal("2"), "Hello", "a"),
    (4L, 5, 5d, 5f, new BigDecimal("5"), "Hello", "a"),
    (7L, 3, 3d, 3f, new BigDecimal("3"), "Hello", "b"),
    (6L, 5, 5d, 5f, new BigDecimal("5"), "Hello", "a"),
    (8L, 3, 3d, 3f, new BigDecimal("3"), "Hello world", "a"),
    (16L, 4, 4d, 4f, new BigDecimal("4"), "Hello world", "b"),
    (32L, 4, 4d, 4f, new BigDecimal("4"), null.asInstanceOf[String], null.asInstanceOf[String]))

  @Test
  def testEventTimeSlidingWindow(): Unit = {
    tEnv.registerFunction("concat_distinct_agg", new ConcatDistinctAggFunction())

    val stream = failingDataSource(data)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset
          [(Long, Int, Double, Float, BigDecimal, String, String)](10L))
    val table = stream.toTable(tEnv,
      'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string, 'name)
    tEnv.registerTable("T1", table)

    val sql =
      """
        |SELECT
        |  `string`,
        |  HOP_START(rowtime, INTERVAL '0.004' SECOND, INTERVAL '0.005' SECOND),
        |  HOP_ROWTIME(rowtime, INTERVAL '0.004' SECOND, INTERVAL '0.005' SECOND),
        |  COUNT(1),
        |  SUM(1),
        |  COUNT(`int`),
        |  COUNT(DISTINCT `float`),
        |  concat_distinct_agg(name)
        |FROM T1
        |GROUP BY `string`, HOP(rowtime, INTERVAL '0.004' SECOND, INTERVAL '0.005' SECOND)
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "Hallo,1970-01-01T00:00,1970-01-01T00:00:00.004,1,1,1,1,a",
      "Hello world,1970-01-01T00:00:00.004,1970-01-01T00:00:00.008,1,1,1,1,a",
      "Hello world,1970-01-01T00:00:00.008,1970-01-01T00:00:00.012,1,1,1,1,a",
      "Hello world,1970-01-01T00:00:00.012,1970-01-01T00:00:00.016,1,1,1,1,b",
      "Hello world,1970-01-01T00:00:00.016,1970-01-01T00:00:00.020,1,1,1,1,b",
      "Hello,1970-01-01T00:00,1970-01-01T00:00:00.004,2,2,2,2,a",
      "Hello,1970-01-01T00:00:00.004,1970-01-01T00:00:00.008,3,3,3,2,a|b",
      "Hi,1970-01-01T00:00,1970-01-01T00:00:00.004,1,1,1,1,a",
      "null,1970-01-01T00:00:00.028,1970-01-01T00:00:00.032,1,1,1,1,null",
      "null,1970-01-01T00:00:00.032,1970-01-01T00:00:00.036,1,1,1,1,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testCascadingTumbleWindow(): Unit = {
    val stream = failingDataSource(data)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset
          [(Long, Int, Double, Float, BigDecimal, String, String)](10L))
    val table = stream.toTable(tEnv,
      'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string, 'name)
    tEnv.registerTable("T1", table)

    val sql =
      """
        |SELECT SUM(cnt)
        |FROM (
        |  SELECT COUNT(1) AS cnt, TUMBLE_ROWTIME(rowtime, INTERVAL '10' SECOND) AS ts
        |  FROM T1
        |  GROUP BY `int`, `string`, TUMBLE(rowtime, INTERVAL '10' SECOND)
        |)
        |GROUP BY TUMBLE(ts, INTERVAL '10' SECOND)
        |""".stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq("9")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testEventTimeSessionWindow(): Unit = {
    //To verify the "merge" functionality, we create this test with the following characteristics:
    // 1. set the Parallelism to 1, and have the test data out of order
    // 2. create a waterMark with 10ms offset to delay the window emission by 10ms
    val sessionData = List(
      (1L, 1, "Hello", "a"),
      (2L, 2, "Hello", "b"),
      (8L, 8, "Hello", "a"),
      (9L, 9, "Hello World", "b"),
      (4L, 4, "Hello", "c"),
      (16L, 16, "Hello", "d"))

    val stream = failingDataSource(sessionData)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset[(Long, Int, String, String)](10L))
    val table = stream.toTable(tEnv, 'rowtime.rowtime, 'int, 'string, 'name)
    tEnv.registerTable("T1", table)

    val sql =
      """
        |SELECT
        |  `string`,
        |  SESSION_START(rowtime, INTERVAL '0.005' SECOND),
        |  SESSION_ROWTIME(rowtime, INTERVAL '0.005' SECOND),
        |  COUNT(1),
        |  SUM(1),
        |  COUNT(`int`),
        |  SUM(`int`),
        |  COUNT(DISTINCT name)
        |FROM T1
        |GROUP BY `string`, SESSION(rowtime, INTERVAL '0.005' SECOND)
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "Hello World,1970-01-01T00:00:00.009,1970-01-01T00:00:00.013,1,1,1,9,1",
      "Hello,1970-01-01T00:00:00.016,1970-01-01T00:00:00.020,1,1,1,16,1",
      "Hello,1970-01-01T00:00:00.001,1970-01-01T00:00:00.012,4,4,4,15,3")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testEventTimeTumblingWindowWithAllowLateness(): Unit = {
    // wait 10 millisecond for late elements
    tEnv.getConfig.setIdleStateRetentionTime(
      Time.milliseconds(10), Time.minutes(6))
    // emit result without delay after watermark
    withLateFireDelay(tEnv.getConfig, Time.of(0, TimeUnit.NANOSECONDS))
    val data = List(
      (1L, 1, "Hi"),
      (2L, 2, "Hello"),
      (4L, 2, "Hello"),
      (8L, 3, "Hello world"),
      (4L, 3, "Hello"),         // out of order
      (16L, 3, "Hello world"),
      (9L, 4, "Hello world"),   // out of order
      (3L, 1, "Hi"))           // too late, drop

    val stream = failingDataSource(data)
      .assignTimestampsAndWatermarks(new TimestampAndWatermarkWithOffset[(Long, Int, String)](0L))
    val table = stream.toTable(tEnv, 'long, 'int, 'string, 'rowtime.rowtime)
    tEnv.registerTable("T1", table)
    tEnv.registerFunction("weightAvgFun", new WeightedAvg)

    val sql =
      """
        |SELECT
        |  `string`,
        |  TUMBLE_START(rowtime, INTERVAL '0.005' SECOND) as w_start,
        |  TUMBLE_END(rowtime, INTERVAL '0.005' SECOND),
        |  COUNT(DISTINCT `long`),
        |  COUNT(`int`),
        |  CAST(AVG(`int`) AS INT),
        |  weightAvgFun(`long`, `int`),
        |  MIN(`int`),
        |  MAX(`int`),
        |  SUM(`int`)
        |FROM T1
        |GROUP BY `string`, TUMBLE(rowtime, INTERVAL '0.005' SECOND)
      """.stripMargin

    val result = tEnv.sqlQuery(sql)

    val fieldTypes: Array[TypeInformation[_]] = Array(
      Types.STRING,
      Types.LOCAL_DATE_TIME,
      Types.LOCAL_DATE_TIME,
      Types.LONG,
      Types.LONG,
      Types.INT,
      Types.LONG,
      Types.INT,
      Types.INT,
      Types.INT)
    val fieldNames = fieldTypes.indices.map("f" + _).toArray

    val sink = new TestingUpsertTableSink(Array(0, 1)).configure(fieldNames, fieldTypes)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("MySink", sink)
    execInsertTableAndWaitResult(result, "MySink")

    val expected = Seq(
      "Hi,1970-01-01T00:00,1970-01-01T00:00:00.005,1,1,1,1,1,1,1",
      "Hello,1970-01-01T00:00,1970-01-01T00:00:00.005,2,3,2,3,2,3,7",
      "Hello world,1970-01-01T00:00:00.015,1970-01-01T00:00:00.020,1,1,3,16,3,3,3",
      "Hello world,1970-01-01T00:00:00.005,1970-01-01T00:00:00.010,2,2,3,8,3,4,7")
    assertEquals(expected.sorted.mkString("\n"), sink.getUpsertResults.sorted.mkString("\n"))
  }

  @Test
  def testDistinctAggWithMergeOnEventTimeSessionGroupWindow(): Unit = {
    // create a watermark with 10ms offset to delay the window emission by 10ms to verify merge
    val sessionWindowTestData = List(
      (1L, 2, "Hello"),       // (1, Hello)       - window
      (2L, 2, "Hello"),       // (1, Hello)       - window, deduped
      (8L, 2, "Hello"),       // (2, Hello)       - window, deduped during merge
      (10L, 3, "Hello"),      // (2, Hello)       - window, forwarded during merge
      (9L, 9, "Hello World"), // (1, Hello World) - window
      (4L, 1, "Hello"),       // (1, Hello)       - window, triggering merge
      (16L, 16, "Hello"))     // (3, Hello)       - window (not merged)

    val stream = failingDataSource(sessionWindowTestData)
      .assignTimestampsAndWatermarks(new TimestampAndWatermarkWithOffset[(Long, Int, String)](10L))
    val table = stream.toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)
    tEnv.registerTable("MyTable", table)

    val sqlQuery =
      """
        |SELECT c,
        |   COUNT(DISTINCT b),
        |   SESSION_END(rowtime, INTERVAL '0.005' SECOND)
        |FROM MyTable
        |GROUP BY c, SESSION(rowtime, INTERVAL '0.005' SECOND)
      """.stripMargin
    val sink = new TestingAppendSink
    tEnv.sqlQuery(sqlQuery).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "Hello World,1,1970-01-01T00:00:00.014", // window starts at [9L] till {14L}
      "Hello,1,1970-01-01T00:00:00.021",       // window starts at [16L] till {21L}, not merged
      "Hello,3,1970-01-01T00:00:00.015"        // window starts at [1L,2L],
      //   merged with [8L,10L], by [4L], till {15L}
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testMinMaxWithTumblingWindow(): Unit = {
    val stream = failingDataSource(data)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset[(
          Long, Int, Double, Float, BigDecimal, String, String)](10L))
    val table =
      stream.toTable(tEnv, 'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string, 'name)
    tEnv.registerTable("T1", table)
    tEnv.getConfig.getConfiguration.setBoolean("table.exec.emit.early-fire.enabled", true)
    tEnv.getConfig.getConfiguration.setString("table.exec.emit.early-fire.delay", "1000 ms")

    val sql =
      """
        |SELECT
        | MAX(max_ts),
        | MIN(min_ts),
        | `string`
        |FROM(
        | SELECT
        | `string`,
        | `int`,
        | MAX(rowtime) as max_ts,
        | MIN(rowtime) as min_ts
        | FROM T1
        | GROUP BY `string`, `int`, TUMBLE(rowtime, INTERVAL '10' SECOND))
        |GROUP BY `string`
      """.stripMargin
    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink)
    env.execute()
    val expected = Seq(
      "1970-01-01T00:00:00.001,1970-01-01T00:00:00.001,Hi",
      "1970-01-01T00:00:00.002,1970-01-01T00:00:00.002,Hallo",
      "1970-01-01T00:00:00.007,1970-01-01T00:00:00.003,Hello",
      "1970-01-01T00:00:00.016,1970-01-01T00:00:00.008,Hello world",
      "1970-01-01T00:00:00.032,1970-01-01T00:00:00.032,null")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  private def withLateFireDelay(tableConfig: TableConfig, interval: Time): Unit = {
    val intervalInMillis = interval.toMilliseconds
    val preLateFireInterval = getMillisecondFromConfigDuration(tableConfig,
      TABLE_EXEC_EMIT_LATE_FIRE_DELAY)
    if (preLateFireInterval != null && (preLateFireInterval != intervalInMillis)) {
      // lateFireInterval of the two query config is not equal and not the default
      throw new RuntimeException(
        "Currently not support different lateFireInterval configs in one job")
    }
    tableConfig.getConfiguration.setBoolean(TABLE_EXEC_EMIT_LATE_FIRE_ENABLED, true)
    tableConfig.getConfiguration.setString(
      TABLE_EXEC_EMIT_LATE_FIRE_DELAY, intervalInMillis + " ms")
  }
}
