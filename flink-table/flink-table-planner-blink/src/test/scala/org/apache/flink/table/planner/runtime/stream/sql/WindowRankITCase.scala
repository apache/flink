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

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.ConcatDistinctAggFunction
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.runtime.utils.{FailingCollectionSource, StreamingWithStateTestBase, TestData, TestingAppendSink}
import org.apache.flink.types.Row

import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}

@RunWith(classOf[Parameterized])
class WindowRankITCase(mode: StateBackendMode)
  extends StreamingWithStateTestBase(mode) {

  @Before
  override def before(): Unit = {
    super.before()
    // enable checkpoint, we are using failing source to force have a complete checkpoint
    // and cover restore path
    env.enableCheckpointing(100, CheckpointingMode.EXACTLY_ONCE)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0))
    FailingCollectionSource.reset()

    val dataId = TestValuesTableFactory.registerData(TestData.windowDataWithTimestamp)
    tEnv.executeSql(
      s"""
        |CREATE TABLE T1 (
        | `ts` STRING,
        | `int` INT,
        | `double` DOUBLE,
        | `float` FLOAT,
        | `bigdec` DECIMAL(10, 2),
        | `string` STRING,
        | `name` STRING,
        | `rowtime` AS TO_TIMESTAMP(`ts`),
        | WATERMARK for `rowtime` AS `rowtime` - INTERVAL '1' SECOND
        |) WITH (
        | 'connector' = 'values',
        | 'data-id' = '$dataId',
        | 'failing-source' = 'true'
        |)
        |""".stripMargin)
    tEnv.createFunction("concat_distinct_agg", classOf[ConcatDistinctAggFunction])
  }

  @Test
  def testEventTimeTumbleWindow(): Unit = {
    val sql =
      """
        |SELECT * FROM
        |(
        |  SELECT *,
        |    ROW_NUMBER() OVER(
        |      PARTITION BY window_start, window_end ORDER BY sum_b DESC) as rownum
        |  FROM (
        |    SELECT
        |      `name`,
        |      window_start,
        |      window_end,
        |      COUNT(*) as cnt,
        |      SUM(`bigdec`) as sum_b,
        |      MAX(`double`) as max_d,
        |      MIN(`float`) as min_f,
        |      COUNT(DISTINCT `string`) as uv,
        |      concat_distinct_agg(`string`) as distinct_str
        |    FROM TABLE(
        |      TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |  GROUP BY `name`, window_start, window_end
        |  )
        |)
        |WHERE rownum <= 2
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "a,2020-10-10T00:00,2020-10-10T00:00:05,4,11.10,5.0,1.0,2,Hi|Comment#1,1",
      "a,2020-10-10T00:00:05,2020-10-10T00:00:10,1,3.33,null,3.0,1,Comment#2,2",
      "b,2020-10-10T00:00:05,2020-10-10T00:00:10,2,6.66,6.0,3.0,2,Hello|Hi,1",
      "b,2020-10-10T00:00:15,2020-10-10T00:00:20,1,4.44,4.0,4.0,1,Hi,1",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:35,1,3.33,3.0,3.0,1,Comment#3,2",
      "null,2020-10-10T00:00:30,2020-10-10T00:00:35,1,7.77,7.0,7.0,0,null,1")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeTumbleWindowWithRankOffset(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM
        |(
        |  SELECT *,
        |    ROW_NUMBER() OVER(
        |      PARTITION BY window_start, window_end ORDER BY sum_b DESC) as rownum
        |  FROM (
        |    SELECT
        |      `name`,
        |      window_start,
        |      window_end,
        |      COUNT(*) as cnt,
        |      SUM(`bigdec`) as sum_b,
        |      MAX(`double`) as max_d,
        |      MIN(`float`) as min_f,
        |      COUNT(DISTINCT `string`) as uv,
        |      concat_distinct_agg(`string`) as distinct_str
        |    FROM TABLE(
        |      TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |  GROUP BY `name`, window_start, window_end
        |  )
        |)
        |WHERE rownum > 1 AND rownum <= 2
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "a,2020-10-10T00:00:05,2020-10-10T00:00:10,1,3.33,null,3.0,1,Comment#2,2",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:35,1,3.33,3.0,3.0,1,Comment#3,2")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeTumbleWindowWithoutRankNumber(): Unit = {
    val sql =
      """
        |SELECT `name`, window_start, window_end, cnt, sum_b, max_d, min_f, uv, distinct_str
        |FROM
        |(
        |  SELECT *,
        |    ROW_NUMBER() OVER(
        |      PARTITION BY window_start, window_end ORDER BY sum_b DESC) as rownum
        |  FROM (
        |    SELECT
        |      `name`,
        |      window_start,
        |      window_end,
        |      COUNT(*) as cnt,
        |      SUM(`bigdec`) as sum_b,
        |      MAX(`double`) as max_d,
        |      MIN(`float`) as min_f,
        |      COUNT(DISTINCT `string`) as uv,
        |      concat_distinct_agg(`string`) as distinct_str
        |    FROM TABLE(
        |      TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |  GROUP BY `name`, window_start, window_end
        |  )
        |)
        |WHERE rownum > 1 AND rownum <= 2
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "a,2020-10-10T00:00:05,2020-10-10T00:00:10,1,3.33,null,3.0,1,Comment#2",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:35,1,3.33,3.0,3.0,1,Comment#3")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeHopWindow(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM
        |(
        |  SELECT *,
        |    ROW_NUMBER() OVER(
        |      PARTITION BY window_start, window_end ORDER BY sum_b DESC) as rownum
        |  FROM (
        |    SELECT
        |      `name`,
        |      window_start,
        |      window_end,
        |      COUNT(*) as cnt,
        |      SUM(`bigdec`) as sum_b,
        |      MAX(`double`) as max_d,
        |      MIN(`float`) as min_f,
        |      COUNT(DISTINCT `string`) as uv,
        |      concat_distinct_agg(`string`) as distinct_str
        |  FROM TABLE(
        |    HOP(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '10' SECOND))
        |    GROUP BY `name`, window_start, window_end
        |  )
        |)
        |WHERE rownum <= 2
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "a,2020-10-09T23:59:55,2020-10-10T00:00:05,4,11.10,5.0,1.0,2,Hi|Comment#1,1",
      "a,2020-10-10T00:00,2020-10-10T00:00:10,6,19.98,5.0,1.0,3,Comment#2|Hi|Comment#1,1",
      "a,2020-10-10T00:00:05,2020-10-10T00:00:15,1,3.33,null,3.0,1,Comment#2,2",
      "b,2020-10-10T00:00,2020-10-10T00:00:10,2,6.66,6.0,3.0,2,Hello|Hi,2",
      "b,2020-10-10T00:00:05,2020-10-10T00:00:15,2,6.66,6.0,3.0,2,Hello|Hi,1",
      "b,2020-10-10T00:00:10,2020-10-10T00:00:20,1,4.44,4.0,4.0,1,Hi,1",
      "b,2020-10-10T00:00:15,2020-10-10T00:00:25,1,4.44,4.0,4.0,1,Hi,1",
      "b,2020-10-10T00:00:25,2020-10-10T00:00:35,1,3.33,3.0,3.0,1,Comment#3,2",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:40,1,3.33,3.0,3.0,1,Comment#3,2",
      "null,2020-10-10T00:00:25,2020-10-10T00:00:35,1,7.77,7.0,7.0,0,null,1",
      "null,2020-10-10T00:00:30,2020-10-10T00:00:40,1,7.77,7.0,7.0,0,null,1")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeHopWindowWithRankOffset(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM
        |(
        |  SELECT *,
        |    ROW_NUMBER() OVER(
        |      PARTITION BY window_start, window_end ORDER BY sum_b DESC) as rownum
        |  FROM (
        |    SELECT
        |      `name`,
        |      window_start,
        |      window_end,
        |      COUNT(*) as cnt,
        |      SUM(`bigdec`) as sum_b,
        |      MAX(`double`) as max_d,
        |      MIN(`float`) as min_f,
        |      COUNT(DISTINCT `string`) as uv,
        |      concat_distinct_agg(`string`) as distinct_str
        |  FROM TABLE(
        |    HOP(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '10' SECOND))
        |    GROUP BY `name`, window_start, window_end
        |  )
        |)
        |WHERE rownum > 1 AND rownum <= 2
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "a,2020-10-10T00:00:05,2020-10-10T00:00:15,1,3.33,null,3.0,1,Comment#2,2",
      "b,2020-10-10T00:00,2020-10-10T00:00:10,2,6.66,6.0,3.0,2,Hello|Hi,2",
      "b,2020-10-10T00:00:25,2020-10-10T00:00:35,1,3.33,3.0,3.0,1,Comment#3,2",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:40,1,3.33,3.0,3.0,1,Comment#3,2")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeHopWindowWithoutRankNumber(): Unit = {
    val sql =
      """
        |SELECT `name`, window_start, window_end, cnt, sum_b, max_d, min_f, uv, distinct_str
        |FROM
        |(
        |  SELECT *,
        |    ROW_NUMBER() OVER(
        |      PARTITION BY window_start, window_end ORDER BY sum_b DESC) as rownum
        |  FROM (
        |    SELECT
        |      `name`,
        |      window_start,
        |      window_end,
        |      COUNT(*) as cnt,
        |      SUM(`bigdec`) as sum_b,
        |      MAX(`double`) as max_d,
        |      MIN(`float`) as min_f,
        |      COUNT(DISTINCT `string`) as uv,
        |      concat_distinct_agg(`string`) as distinct_str
        |  FROM TABLE(
        |    HOP(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '10' SECOND))
        |    GROUP BY `name`, window_start, window_end
        |  )
        |)
        |WHERE rownum > 1 AND rownum <= 2
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "a,2020-10-10T00:00:05,2020-10-10T00:00:15,1,3.33,null,3.0,1,Comment#2",
      "b,2020-10-10T00:00,2020-10-10T00:00:10,2,6.66,6.0,3.0,2,Hello|Hi",
      "b,2020-10-10T00:00:25,2020-10-10T00:00:35,1,3.33,3.0,3.0,1,Comment#3",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:40,1,3.33,3.0,3.0,1,Comment#3")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeCumulateWindow(): Unit = {
    val sql =
      """
        |SELECT * FROM
        |(
        |  SELECT *,
        |    ROW_NUMBER() OVER(
        |      PARTITION BY window_start, window_end ORDER BY sum_b DESC) as rownum
        |  FROM (
        |    SELECT
        |      `name`,
        |      window_start,
        |      window_end,
        |      COUNT(*) as cnt,
        |      SUM(`bigdec`) as sum_b,
        |      MAX(`double`) as max_d,
        |      MIN(`float`) as min_f,
        |      COUNT(DISTINCT `string`) as uv,
        |      concat_distinct_agg(`string`) as distinct_str
        |    FROM TABLE(
        |      CUMULATE(
        |        TABLE T1,
        |        DESCRIPTOR(rowtime),
        |        INTERVAL '5' SECOND,
        |        INTERVAL '15' SECOND))
        |    GROUP BY `name`, window_start, window_end
        |  )
        |)
        |WHERE rownum <= 2
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "a,2020-10-10T00:00,2020-10-10T00:00:05,4,11.10,5.0,1.0,2,Hi|Comment#1,1",
      "a,2020-10-10T00:00,2020-10-10T00:00:10,6,19.98,5.0,1.0,3,Hi|Comment#1|Comment#2,1",
      "a,2020-10-10T00:00,2020-10-10T00:00:15,6,19.98,5.0,1.0,3,Hi|Comment#1|Comment#2,1",
      "b,2020-10-10T00:00,2020-10-10T00:00:10,2,6.66,6.0,3.0,2,Hello|Hi,2",
      "b,2020-10-10T00:00,2020-10-10T00:00:15,2,6.66,6.0,3.0,2,Hello|Hi,2",
      "b,2020-10-10T00:00:15,2020-10-10T00:00:20,1,4.44,4.0,4.0,1,Hi,1",
      "b,2020-10-10T00:00:15,2020-10-10T00:00:25,1,4.44,4.0,4.0,1,Hi,1",
      "b,2020-10-10T00:00:15,2020-10-10T00:00:30,1,4.44,4.0,4.0,1,Hi,1",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:35,1,3.33,3.0,3.0,1,Comment#3,2",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:40,1,3.33,3.0,3.0,1,Comment#3,2",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:45,1,3.33,3.0,3.0,1,Comment#3,2",
      "null,2020-10-10T00:00:30,2020-10-10T00:00:35,1,7.77,7.0,7.0,0,null,1",
      "null,2020-10-10T00:00:30,2020-10-10T00:00:40,1,7.77,7.0,7.0,0,null,1",
      "null,2020-10-10T00:00:30,2020-10-10T00:00:45,1,7.77,7.0,7.0,0,null,1")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeCumulateWindowWithRankOffset(): Unit = {
    val sql =
      """
        |SELECT * FROM
        |(
        |  SELECT *,
        |    ROW_NUMBER() OVER(
        |      PARTITION BY window_start, window_end ORDER BY sum_b DESC) as rownum
        |  FROM (
        |    SELECT
        |      `name`,
        |      window_start,
        |      window_end,
        |      COUNT(*) as cnt,
        |      SUM(`bigdec`) as sum_b,
        |      MAX(`double`) as max_d,
        |      MIN(`float`) as min_f,
        |      COUNT(DISTINCT `string`) as uv,
        |      concat_distinct_agg(`string`) as distinct_str
        |    FROM TABLE(
        |      CUMULATE(
        |        TABLE T1,
        |        DESCRIPTOR(rowtime),
        |        INTERVAL '5' SECOND,
        |        INTERVAL '15' SECOND))
        |    GROUP BY `name`, window_start, window_end
        |  )
        |)
        |WHERE rownum > 1 AND rownum <= 2
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "b,2020-10-10T00:00,2020-10-10T00:00:10,2,6.66,6.0,3.0,2,Hello|Hi,2",
      "b,2020-10-10T00:00,2020-10-10T00:00:15,2,6.66,6.0,3.0,2,Hello|Hi,2",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:35,1,3.33,3.0,3.0,1,Comment#3,2",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:40,1,3.33,3.0,3.0,1,Comment#3,2",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:45,1,3.33,3.0,3.0,1,Comment#3,2")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeCumulateWindowWithoutRankNumber(): Unit = {
    val sql =
      """
        |SELECT `name`, window_start, window_end, cnt, sum_b, max_d, min_f, uv, distinct_str
        |FROM
        |(
        |  SELECT *,
        |    ROW_NUMBER() OVER(
        |      PARTITION BY window_start, window_end ORDER BY sum_b DESC) as rownum
        |  FROM (
        |    SELECT
        |      `name`,
        |      window_start,
        |      window_end,
        |      COUNT(*) as cnt,
        |      SUM(`bigdec`) as sum_b,
        |      MAX(`double`) as max_d,
        |      MIN(`float`) as min_f,
        |      COUNT(DISTINCT `string`) as uv,
        |      concat_distinct_agg(`string`) as distinct_str
        |    FROM TABLE(
        |      CUMULATE(
        |        TABLE T1,
        |        DESCRIPTOR(rowtime),
        |        INTERVAL '5' SECOND,
        |        INTERVAL '15' SECOND))
        |    GROUP BY `name`, window_start, window_end
        |  )
        |)
        |WHERE rownum > 1 AND rownum <= 2
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "b,2020-10-10T00:00,2020-10-10T00:00:10,2,6.66,6.0,3.0,2,Hello|Hi",
      "b,2020-10-10T00:00,2020-10-10T00:00:15,2,6.66,6.0,3.0,2,Hello|Hi",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:35,1,3.33,3.0,3.0,1,Comment#3",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:40,1,3.33,3.0,3.0,1,Comment#3",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:45,1,3.33,3.0,3.0,1,Comment#3")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testTop1(): Unit = {
    val sql =
      """
        |SELECT * FROM
        |(
        |  SELECT *,
        |    ROW_NUMBER() OVER(
        |      PARTITION BY window_start, window_end ORDER BY sum_b DESC) as rownum
        |  FROM (
        |    SELECT
        |      `name`,
        |      window_start,
        |      window_end,
        |      COUNT(*) as cnt,
        |      SUM(`bigdec`) as sum_b
        |    FROM TABLE(
        |      TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |  GROUP BY `name`, window_start, window_end
        |  )
        |)
        |WHERE rownum <= 1
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "a,2020-10-10T00:00,2020-10-10T00:00:05,4,11.10,1",
      "b,2020-10-10T00:00:05,2020-10-10T00:00:10,2,6.66,1",
      "b,2020-10-10T00:00:15,2020-10-10T00:00:20,1,4.44,1",
      "null,2020-10-10T00:00:30,2020-10-10T00:00:35,1,7.77,1")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }
}
