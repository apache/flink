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
class WindowDeduplicateITCase(mode: StateBackendMode)
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
  def testTumbleWindowKeepLastRow(): Unit = {
    val sql =
      s"""
         |SELECT
         |  TO_TIMESTAMP(`ts`),
         |  `int`,
         |  `double`,
         |  `float`,
         |  `bigdec`,
         |  `string`,
         |  `name`,
         |  CAST(`rowtime` AS STRING),
         |  window_start,
         |  window_end,
         |  window_time
         |FROM (
         |  SELECT *,
         |    ROW_NUMBER() OVER(
         |      PARTITION BY window_start, window_end, `name` ORDER BY rowtime DESC) as rownum
         |  FROM TABLE(
         |      TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
         |)
         |WHERE rownum <= 1
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected =
      Seq(
        "2020-10-10T00:00:04,5,5.0,5.0,5.55,null,a,2020-10-10 00:00:04.000," +
          "2020-10-10T00:00,2020-10-10T00:00:05,2020-10-10T00:00:04.999",
        "2020-10-10T00:00:08,3,null,3.0,3.33,Comment#2,a,2020-10-10 00:00:08.000," +
          "2020-10-10T00:00:05,2020-10-10T00:00:10,2020-10-10T00:00:09.999",
        "2020-10-10T00:00:07,3,3.0,3.0,null,Hello,b,2020-10-10 00:00:07.000," +
          "2020-10-10T00:00:05,2020-10-10T00:00:10,2020-10-10T00:00:09.999",
        "2020-10-10T00:00:16,4,4.0,4.0,4.44,Hi,b,2020-10-10 00:00:16.000," +
          "2020-10-10T00:00:15,2020-10-10T00:00:20,2020-10-10T00:00:19.999",
        "2020-10-10T00:00:32,7,7.0,7.0,7.77,null,null,2020-10-10 00:00:32.000," +
          "2020-10-10T00:00:30,2020-10-10T00:00:35,2020-10-10T00:00:34.999",
        "2020-10-10T00:00:34,1,3.0,3.0,3.33,Comment#3,b,2020-10-10 00:00:34.000," +
          "2020-10-10T00:00:30,2020-10-10T00:00:35,2020-10-10T00:00:34.999")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testTumbleWindowKeepFirstRow(): Unit = {
    val sql =
      s"""
         |SELECT
         |  TO_TIMESTAMP(`ts`),
         |  `int`,
         |  `double`,
         |  `float`,
         |  `bigdec`,
         |  `string`,
         |  `name`,
         |  CAST(`rowtime` AS STRING),
         |  window_start,
         |  window_end,
         |  window_time
         |FROM (
         |  SELECT *,
         |    ROW_NUMBER() OVER(
         |      PARTITION BY window_start, window_end, `name` ORDER BY rowtime) as rownum
         |  FROM TABLE(
         |      TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
         |)
         |WHERE rownum <= 1
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected =
      Seq(
        "2020-10-10T00:00:01,1,1.0,1.0,1.11,Hi,a,2020-10-10 00:00:01.000," +
          "2020-10-10T00:00,2020-10-10T00:00:05,2020-10-10T00:00:04.999",
        "2020-10-10T00:00:08,3,null,3.0,3.33,Comment#2,a,2020-10-10 00:00:08.000," +
          "2020-10-10T00:00:05,2020-10-10T00:00:10,2020-10-10T00:00:09.999",
        "2020-10-10T00:00:06,6,6.0,6.0,6.66,Hi,b,2020-10-10 00:00:06.000," +
          "2020-10-10T00:00:05,2020-10-10T00:00:10,2020-10-10T00:00:09.999",
        "2020-10-10T00:00:16,4,4.0,4.0,4.44,Hi,b,2020-10-10 00:00:16.000," +
          "2020-10-10T00:00:15,2020-10-10T00:00:20,2020-10-10T00:00:19.999",
        "2020-10-10T00:00:32,7,7.0,7.0,7.77,null,null,2020-10-10 00:00:32.000," +
          "2020-10-10T00:00:30,2020-10-10T00:00:35,2020-10-10T00:00:34.999",
        "2020-10-10T00:00:34,1,3.0,3.0,3.33,Comment#3,b,2020-10-10 00:00:34.000," +
          "2020-10-10T00:00:30,2020-10-10T00:00:35,2020-10-10T00:00:34.999"
      )
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testTumbleWindowKeepLastRowWithCalc(): Unit = {
    val sql =
      """
        |SELECT
        |  `int`,
        |  `string`,
        |  `name`,
        |  window_start,
        |  window_end,
        |  window_time
        |FROM (
        |  SELECT *,
        |    ROW_NUMBER() OVER(
        |      PARTITION BY window_start, window_end, `name` ORDER BY rowtime DESC) as rownum
        |  FROM TABLE(
        |      TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |)
        |WHERE rownum <= 1
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected =
      Seq(
        "5,null,a,2020-10-10T00:00,2020-10-10T00:00:05,2020-10-10T00:00:04.999",
        "3,Comment#2,a,2020-10-10T00:00:05,2020-10-10T00:00:10,2020-10-10T00:00:09.999",
        "3,Hello,b,2020-10-10T00:00:05,2020-10-10T00:00:10,2020-10-10T00:00:09.999",
        "4,Hi,b,2020-10-10T00:00:15,2020-10-10T00:00:20,2020-10-10T00:00:19.999",
        "7,null,null,2020-10-10T00:00:30,2020-10-10T00:00:35,2020-10-10T00:00:34.999",
        "1,Comment#3,b,2020-10-10T00:00:30,2020-10-10T00:00:35,2020-10-10T00:00:34.999")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testCumulateWindowKeepLastRow(): Unit = {
    val sql =
      s"""
         |SELECT
         |  TO_TIMESTAMP(`ts`),
         |  `int`,
         |  `double`,
         |  `float`,
         |  `bigdec`,
         |  `string`,
         |  `name`,
         |  CAST(`rowtime` AS STRING),
         |  window_start,
         |  window_end,
         |  window_time
         |FROM (
         |  SELECT *,
         |    ROW_NUMBER() OVER(
         |      PARTITION BY window_start, window_end, `name` ORDER BY rowtime DESC) as rownum
         |  FROM TABLE(
         |      CUMULATE(
         |        TABLE T1,
         |        DESCRIPTOR(rowtime),
         |        INTERVAL '5' SECOND,
         |        INTERVAL '15' SECOND))
         |)
         |WHERE rownum <= 1
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected =
      Seq(
        "2020-10-10T00:00:04,5,5.0,5.0,5.55,null,a,2020-10-10 00:00:04.000," +
          "2020-10-10T00:00,2020-10-10T00:00:05,2020-10-10T00:00:04.999",
        "2020-10-10T00:00:08,3,null,3.0,3.33,Comment#2,a,2020-10-10 00:00:08.000," +
          "2020-10-10T00:00,2020-10-10T00:00:10,2020-10-10T00:00:09.999",
        "2020-10-10T00:00:08,3,null,3.0,3.33,Comment#2,a,2020-10-10 00:00:08.000," +
          "2020-10-10T00:00,2020-10-10T00:00:15,2020-10-10T00:00:14.999",
        "2020-10-10T00:00:07,3,3.0,3.0,null,Hello,b,2020-10-10 00:00:07.000," +
          "2020-10-10T00:00,2020-10-10T00:00:10,2020-10-10T00:00:09.999",
        "2020-10-10T00:00:07,3,3.0,3.0,null,Hello,b,2020-10-10 00:00:07.000," +
          "2020-10-10T00:00,2020-10-10T00:00:15,2020-10-10T00:00:14.999",
        "2020-10-10T00:00:16,4,4.0,4.0,4.44,Hi,b,2020-10-10 00:00:16.000," +
          "2020-10-10T00:00:15,2020-10-10T00:00:20,2020-10-10T00:00:19.999",
        "2020-10-10T00:00:16,4,4.0,4.0,4.44,Hi,b,2020-10-10 00:00:16.000," +
          "2020-10-10T00:00:15,2020-10-10T00:00:25,2020-10-10T00:00:24.999",
        "2020-10-10T00:00:16,4,4.0,4.0,4.44,Hi,b,2020-10-10 00:00:16.000," +
          "2020-10-10T00:00:15,2020-10-10T00:00:30,2020-10-10T00:00:29.999",
        "2020-10-10T00:00:32,7,7.0,7.0,7.77,null,null,2020-10-10 00:00:32.000," +
          "2020-10-10T00:00:30,2020-10-10T00:00:35,2020-10-10T00:00:34.999",
        "2020-10-10T00:00:32,7,7.0,7.0,7.77,null,null,2020-10-10 00:00:32.000," +
          "2020-10-10T00:00:30,2020-10-10T00:00:40,2020-10-10T00:00:39.999",
        "2020-10-10T00:00:32,7,7.0,7.0,7.77,null,null,2020-10-10 00:00:32.000," +
          "2020-10-10T00:00:30,2020-10-10T00:00:45,2020-10-10T00:00:44.999",
        "2020-10-10T00:00:34,1,3.0,3.0,3.33,Comment#3,b,2020-10-10 00:00:34.000," +
          "2020-10-10T00:00:30,2020-10-10T00:00:35,2020-10-10T00:00:34.999",
        "2020-10-10T00:00:34,1,3.0,3.0,3.33,Comment#3,b,2020-10-10 00:00:34.000," +
          "2020-10-10T00:00:30,2020-10-10T00:00:40,2020-10-10T00:00:39.999",
        "2020-10-10T00:00:34,1,3.0,3.0,3.33,Comment#3,b,2020-10-10 00:00:34.000," +
          "2020-10-10T00:00:30,2020-10-10T00:00:45,2020-10-10T00:00:44.999")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }
}
