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

import org.apache.flink.configuration.{Configuration, RestartStrategyOptions}
import org.apache.flink.core.execution.CheckpointingMode
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.{FailingCollectionSource, StreamingWithStateTestBase, TestData, TestingAppendSink}
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.{BeforeEach, TestTemplate}
import org.junit.jupiter.api.extension.ExtendWith

import java.time.Duration

@ExtendWith(Array(classOf[ParameterizedTestExtension]))
class WindowTableFunctionITCase(mode: StateBackendMode) extends StreamingWithStateTestBase(mode) {

  @BeforeEach
  override def before(): Unit = {
    super.before()
    // enable checkpoint, we are using failing source to force have a complete checkpoint
    // and cover restore path
    env.enableCheckpointing(100, CheckpointingMode.EXACTLY_ONCE)
    val configuration = new Configuration()
    configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixeddelay")
    configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, Int.box(1))
    configuration.set(
      RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY,
      Duration.ofMillis(0))
    env.configure(configuration, Thread.currentThread.getContextClassLoader)
    FailingCollectionSource.reset()

    val dataId = TestValuesTableFactory.registerData(TestData.windowDataWithTimestamp)
    tEnv.executeSql(s"""
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
  }

  @TestTemplate
  def testTumbleWindow(): Unit = {
    val sql =
      """
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
        |FROM TABLE(TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toDataStream.addSink(sink)
    env.execute()

    val expected = Seq(
      "2020-10-10T00:00:01,1,1.0,1.0,1.11,Hi,a,2020-10-10 00:00:01.000," +
        "2020-10-10T00:00,2020-10-10T00:00:05,2020-10-10T00:00:04.999",
      "2020-10-10T00:00:02,2,2.0,2.0,2.22,Comment#1,a,2020-10-10 00:00:02.000," +
        "2020-10-10T00:00,2020-10-10T00:00:05,2020-10-10T00:00:04.999",
      "2020-10-10T00:00:03,2,2.0,2.0,2.22,Comment#1,a,2020-10-10 00:00:03.000," +
        "2020-10-10T00:00,2020-10-10T00:00:05,2020-10-10T00:00:04.999",
      "2020-10-10T00:00:04,5,5.0,5.0,5.55,null,a,2020-10-10 00:00:04.000," +
        "2020-10-10T00:00,2020-10-10T00:00:05,2020-10-10T00:00:04.999",
      "2020-10-10T00:00:04,5,5.0,null,5.55,Hi,a,2020-10-10 00:00:04.000," +
        "2020-10-10T00:00,2020-10-10T00:00:05,2020-10-10T00:00:04.999",
      "2020-10-10T00:00:06,6,6.0,6.0,6.66,Hi,b,2020-10-10 00:00:06.000," +
        "2020-10-10T00:00:05,2020-10-10T00:00:10,2020-10-10T00:00:09.999",
      "2020-10-10T00:00:07,3,3.0,3.0,null,Hello,b,2020-10-10 00:00:07.000," +
        "2020-10-10T00:00:05,2020-10-10T00:00:10,2020-10-10T00:00:09.999",
      "2020-10-10T00:00:08,3,null,3.0,3.33,Comment#2,a,2020-10-10 00:00:08.000," +
        "2020-10-10T00:00:05,2020-10-10T00:00:10,2020-10-10T00:00:09.999",
      "2020-10-10T00:00:16,4,4.0,4.0,4.44,Hi,b,2020-10-10 00:00:16.000," +
        "2020-10-10T00:00:15,2020-10-10T00:00:20,2020-10-10T00:00:19.999",
      "2020-10-10T00:00:32,7,7.0,7.0,7.77,null,null,2020-10-10 00:00:32.000," +
        "2020-10-10T00:00:30,2020-10-10T00:00:35,2020-10-10T00:00:34.999",
      "2020-10-10T00:00:34,1,3.0,3.0,3.33,Comment#3,b,2020-10-10 00:00:34.000," +
        "2020-10-10T00:00:30,2020-10-10T00:00:35,2020-10-10T00:00:34.999"
    )
    assertThat(sink.getAppendResults.sorted.mkString("\n"))
      .isEqualTo(expected.sorted.mkString("\n"))
  }

  @TestTemplate
  def testTumbleWindowTVFWithOffset(): Unit = {
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
         |FROM TABLE(
         |    TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '1' SECOND))
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toDataStream.addSink(sink)
    env.execute()

    val expected =
      Seq(
        "2020-10-10T00:00:01,1,1.0,1.0,1.11,Hi,a,2020-10-10 00:00:01.000," +
          "2020-10-10T00:00:01,2020-10-10T00:00:06,2020-10-10T00:00:05.999",
        "2020-10-10T00:00:02,2,2.0,2.0,2.22,Comment#1,a,2020-10-10 00:00:02.000," +
          "2020-10-10T00:00:01,2020-10-10T00:00:06,2020-10-10T00:00:05.999",
        "2020-10-10T00:00:03,2,2.0,2.0,2.22,Comment#1,a,2020-10-10 00:00:03.000," +
          "2020-10-10T00:00:01,2020-10-10T00:00:06,2020-10-10T00:00:05.999",
        "2020-10-10T00:00:04,5,5.0,5.0,5.55,null,a,2020-10-10 00:00:04.000," +
          "2020-10-10T00:00:01,2020-10-10T00:00:06,2020-10-10T00:00:05.999",
        "2020-10-10T00:00:04,5,5.0,null,5.55,Hi,a,2020-10-10 00:00:04.000," +
          "2020-10-10T00:00:01,2020-10-10T00:00:06,2020-10-10T00:00:05.999",
        "2020-10-10T00:00:06,6,6.0,6.0,6.66,Hi,b,2020-10-10 00:00:06.000," +
          "2020-10-10T00:00:06,2020-10-10T00:00:11,2020-10-10T00:00:10.999",
        "2020-10-10T00:00:07,3,3.0,3.0,null,Hello,b,2020-10-10 00:00:07.000," +
          "2020-10-10T00:00:06,2020-10-10T00:00:11,2020-10-10T00:00:10.999",
        "2020-10-10T00:00:08,3,null,3.0,3.33,Comment#2,a,2020-10-10 00:00:08.000," +
          "2020-10-10T00:00:06,2020-10-10T00:00:11,2020-10-10T00:00:10.999",
        "2020-10-10T00:00:16,4,4.0,4.0,4.44,Hi,b,2020-10-10 00:00:16.000," +
          "2020-10-10T00:00:16,2020-10-10T00:00:21,2020-10-10T00:00:20.999",
        "2020-10-10T00:00:32,7,7.0,7.0,7.77,null,null,2020-10-10 00:00:32.000," +
          "2020-10-10T00:00:31,2020-10-10T00:00:36,2020-10-10T00:00:35.999",
        "2020-10-10T00:00:34,1,3.0,3.0,3.33,Comment#3,b,2020-10-10 00:00:34.000," +
          "2020-10-10T00:00:31,2020-10-10T00:00:36,2020-10-10T00:00:35.999"
      )
    assertThat(sink.getAppendResults.sorted.mkString("\n"))
      .isEqualTo(expected.sorted.mkString("\n"))
  }

  @TestTemplate
  def testTumbleWindowTVFWithNegativeOffset(): Unit = {
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
         |FROM TABLE(
         |  TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '-1' SECOND))
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toDataStream.addSink(sink)
    env.execute()

    val expected =
      Seq(
        "2020-10-10T00:00:01,1,1.0,1.0,1.11,Hi,a,2020-10-10 00:00:01.000," +
          "2020-10-09T23:59:59,2020-10-10T00:00:04,2020-10-10T00:00:03.999",
        "2020-10-10T00:00:02,2,2.0,2.0,2.22,Comment#1,a,2020-10-10 00:00:02.000," +
          "2020-10-09T23:59:59,2020-10-10T00:00:04,2020-10-10T00:00:03.999",
        "2020-10-10T00:00:03,2,2.0,2.0,2.22,Comment#1,a,2020-10-10 00:00:03.000," +
          "2020-10-09T23:59:59,2020-10-10T00:00:04,2020-10-10T00:00:03.999",
        "2020-10-10T00:00:04,5,5.0,5.0,5.55,null,a,2020-10-10 00:00:04.000," +
          "2020-10-10T00:00:04,2020-10-10T00:00:09,2020-10-10T00:00:08.999",
        "2020-10-10T00:00:04,5,5.0,null,5.55,Hi,a,2020-10-10 00:00:04.000," +
          "2020-10-10T00:00:04,2020-10-10T00:00:09,2020-10-10T00:00:08.999",
        "2020-10-10T00:00:06,6,6.0,6.0,6.66,Hi,b,2020-10-10 00:00:06.000," +
          "2020-10-10T00:00:04,2020-10-10T00:00:09,2020-10-10T00:00:08.999",
        "2020-10-10T00:00:07,3,3.0,3.0,null,Hello,b,2020-10-10 00:00:07.000," +
          "2020-10-10T00:00:04,2020-10-10T00:00:09,2020-10-10T00:00:08.999",
        "2020-10-10T00:00:08,3,null,3.0,3.33,Comment#2,a,2020-10-10 00:00:08.000," +
          "2020-10-10T00:00:04,2020-10-10T00:00:09,2020-10-10T00:00:08.999",
        "2020-10-10T00:00:16,4,4.0,4.0,4.44,Hi,b,2020-10-10 00:00:16.000," +
          "2020-10-10T00:00:14,2020-10-10T00:00:19,2020-10-10T00:00:18.999",
        "2020-10-10T00:00:32,7,7.0,7.0,7.77,null,null,2020-10-10 00:00:32.000," +
          "2020-10-10T00:00:29,2020-10-10T00:00:34,2020-10-10T00:00:33.999",
        "2020-10-10T00:00:34,1,3.0,3.0,3.33,Comment#3,b,2020-10-10 00:00:34.000," +
          "2020-10-10T00:00:34,2020-10-10T00:00:39,2020-10-10T00:00:38.999"
      )
    assertThat(sink.getAppendResults.sorted.mkString("\n"))
      .isEqualTo(expected.sorted.mkString("\n"))
  }

  @TestTemplate
  def testHopWindow(): Unit = {
    val sql =
      """
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
        |FROM TABLE(
        |  HOP(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '10' SECOND))
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toDataStream.addSink(sink)
    env.execute()

    val expected = Seq(
      "2020-10-10T00:00:01,1,1.0,1.0,1.11,Hi,a,2020-10-10 00:00:01.000," +
        "2020-10-09T23:59:55,2020-10-10T00:00:05,2020-10-10T00:00:04.999",
      "2020-10-10T00:00:01,1,1.0,1.0,1.11,Hi,a,2020-10-10 00:00:01.000," +
        "2020-10-10T00:00,2020-10-10T00:00:10,2020-10-10T00:00:09.999",
      "2020-10-10T00:00:02,2,2.0,2.0,2.22,Comment#1,a,2020-10-10 00:00:02.000," +
        "2020-10-09T23:59:55,2020-10-10T00:00:05,2020-10-10T00:00:04.999",
      "2020-10-10T00:00:02,2,2.0,2.0,2.22,Comment#1,a,2020-10-10 00:00:02.000," +
        "2020-10-10T00:00,2020-10-10T00:00:10,2020-10-10T00:00:09.999",
      "2020-10-10T00:00:03,2,2.0,2.0,2.22,Comment#1,a,2020-10-10 00:00:03.000," +
        "2020-10-09T23:59:55,2020-10-10T00:00:05,2020-10-10T00:00:04.999",
      "2020-10-10T00:00:03,2,2.0,2.0,2.22,Comment#1,a,2020-10-10 00:00:03.000," +
        "2020-10-10T00:00,2020-10-10T00:00:10,2020-10-10T00:00:09.999",
      "2020-10-10T00:00:04,5,5.0,5.0,5.55,null,a,2020-10-10 00:00:04.000," +
        "2020-10-09T23:59:55,2020-10-10T00:00:05,2020-10-10T00:00:04.999",
      "2020-10-10T00:00:04,5,5.0,5.0,5.55,null,a,2020-10-10 00:00:04.000," +
        "2020-10-10T00:00,2020-10-10T00:00:10,2020-10-10T00:00:09.999",
      "2020-10-10T00:00:04,5,5.0,null,5.55,Hi,a,2020-10-10 00:00:04.000," +
        "2020-10-09T23:59:55,2020-10-10T00:00:05,2020-10-10T00:00:04.999",
      "2020-10-10T00:00:04,5,5.0,null,5.55,Hi,a,2020-10-10 00:00:04.000," +
        "2020-10-10T00:00,2020-10-10T00:00:10,2020-10-10T00:00:09.999",
      "2020-10-10T00:00:06,6,6.0,6.0,6.66,Hi,b,2020-10-10 00:00:06.000," +
        "2020-10-10T00:00,2020-10-10T00:00:10,2020-10-10T00:00:09.999",
      "2020-10-10T00:00:06,6,6.0,6.0,6.66,Hi,b,2020-10-10 00:00:06.000," +
        "2020-10-10T00:00:05,2020-10-10T00:00:15,2020-10-10T00:00:14.999",
      "2020-10-10T00:00:07,3,3.0,3.0,null,Hello,b,2020-10-10 00:00:07.000," +
        "2020-10-10T00:00,2020-10-10T00:00:10,2020-10-10T00:00:09.999",
      "2020-10-10T00:00:07,3,3.0,3.0,null,Hello,b,2020-10-10 00:00:07.000," +
        "2020-10-10T00:00:05,2020-10-10T00:00:15,2020-10-10T00:00:14.999",
      "2020-10-10T00:00:08,3,null,3.0,3.33,Comment#2,a,2020-10-10 00:00:08.000," +
        "2020-10-10T00:00,2020-10-10T00:00:10,2020-10-10T00:00:09.999",
      "2020-10-10T00:00:08,3,null,3.0,3.33,Comment#2,a,2020-10-10 00:00:08.000," +
        "2020-10-10T00:00:05,2020-10-10T00:00:15,2020-10-10T00:00:14.999",
      "2020-10-10T00:00:16,4,4.0,4.0,4.44,Hi,b,2020-10-10 00:00:16.000," +
        "2020-10-10T00:00:10,2020-10-10T00:00:20,2020-10-10T00:00:19.999",
      "2020-10-10T00:00:16,4,4.0,4.0,4.44,Hi,b,2020-10-10 00:00:16.000," +
        "2020-10-10T00:00:15,2020-10-10T00:00:25,2020-10-10T00:00:24.999",
      "2020-10-10T00:00:32,7,7.0,7.0,7.77,null,null,2020-10-10 00:00:32.000," +
        "2020-10-10T00:00:25,2020-10-10T00:00:35,2020-10-10T00:00:34.999",
      "2020-10-10T00:00:32,7,7.0,7.0,7.77,null,null,2020-10-10 00:00:32.000," +
        "2020-10-10T00:00:30,2020-10-10T00:00:40,2020-10-10T00:00:39.999",
      "2020-10-10T00:00:34,1,3.0,3.0,3.33,Comment#3,b,2020-10-10 00:00:34.000," +
        "2020-10-10T00:00:25,2020-10-10T00:00:35,2020-10-10T00:00:34.999",
      "2020-10-10T00:00:34,1,3.0,3.0,3.33,Comment#3,b,2020-10-10 00:00:34.000," +
        "2020-10-10T00:00:30,2020-10-10T00:00:40,2020-10-10T00:00:39.999"
    )
    assertThat(sink.getAppendResults.sorted.mkString("\n"))
      .isEqualTo(expected.sorted.mkString("\n"))
  }

  @TestTemplate
  def testCumulateWindow(): Unit = {
    val sql =
      """
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
        |FROM TABLE(
        |  CUMULATE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '15' SECOND))
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toDataStream.addSink(sink)
    env.execute()

    val expected = Seq(
      "2020-10-10T00:00:01,1,1.0,1.0,1.11,Hi,a,2020-10-10 00:00:01.000," +
        "2020-10-10T00:00,2020-10-10T00:00:05,2020-10-10T00:00:04.999",
      "2020-10-10T00:00:01,1,1.0,1.0,1.11,Hi,a,2020-10-10 00:00:01.000," +
        "2020-10-10T00:00,2020-10-10T00:00:10,2020-10-10T00:00:09.999",
      "2020-10-10T00:00:01,1,1.0,1.0,1.11,Hi,a,2020-10-10 00:00:01.000," +
        "2020-10-10T00:00,2020-10-10T00:00:15,2020-10-10T00:00:14.999",
      "2020-10-10T00:00:02,2,2.0,2.0,2.22,Comment#1,a,2020-10-10 00:00:02.000," +
        "2020-10-10T00:00,2020-10-10T00:00:05,2020-10-10T00:00:04.999",
      "2020-10-10T00:00:02,2,2.0,2.0,2.22,Comment#1,a,2020-10-10 00:00:02.000," +
        "2020-10-10T00:00,2020-10-10T00:00:10,2020-10-10T00:00:09.999",
      "2020-10-10T00:00:02,2,2.0,2.0,2.22,Comment#1,a,2020-10-10 00:00:02.000," +
        "2020-10-10T00:00,2020-10-10T00:00:15,2020-10-10T00:00:14.999",
      "2020-10-10T00:00:03,2,2.0,2.0,2.22,Comment#1,a,2020-10-10 00:00:03.000," +
        "2020-10-10T00:00,2020-10-10T00:00:05,2020-10-10T00:00:04.999",
      "2020-10-10T00:00:03,2,2.0,2.0,2.22,Comment#1,a,2020-10-10 00:00:03.000," +
        "2020-10-10T00:00,2020-10-10T00:00:10,2020-10-10T00:00:09.999",
      "2020-10-10T00:00:03,2,2.0,2.0,2.22,Comment#1,a,2020-10-10 00:00:03.000," +
        "2020-10-10T00:00,2020-10-10T00:00:15,2020-10-10T00:00:14.999",
      "2020-10-10T00:00:04,5,5.0,5.0,5.55,null,a,2020-10-10 00:00:04.000," +
        "2020-10-10T00:00,2020-10-10T00:00:05,2020-10-10T00:00:04.999",
      "2020-10-10T00:00:04,5,5.0,5.0,5.55,null,a,2020-10-10 00:00:04.000," +
        "2020-10-10T00:00,2020-10-10T00:00:10,2020-10-10T00:00:09.999",
      "2020-10-10T00:00:04,5,5.0,5.0,5.55,null,a,2020-10-10 00:00:04.000," +
        "2020-10-10T00:00,2020-10-10T00:00:15,2020-10-10T00:00:14.999",
      "2020-10-10T00:00:04,5,5.0,null,5.55,Hi,a,2020-10-10 00:00:04.000," +
        "2020-10-10T00:00,2020-10-10T00:00:05,2020-10-10T00:00:04.999",
      "2020-10-10T00:00:04,5,5.0,null,5.55,Hi,a,2020-10-10 00:00:04.000," +
        "2020-10-10T00:00,2020-10-10T00:00:10,2020-10-10T00:00:09.999",
      "2020-10-10T00:00:04,5,5.0,null,5.55,Hi,a,2020-10-10 00:00:04.000," +
        "2020-10-10T00:00,2020-10-10T00:00:15,2020-10-10T00:00:14.999",
      "2020-10-10T00:00:06,6,6.0,6.0,6.66,Hi,b,2020-10-10 00:00:06.000," +
        "2020-10-10T00:00,2020-10-10T00:00:10,2020-10-10T00:00:09.999",
      "2020-10-10T00:00:06,6,6.0,6.0,6.66,Hi,b,2020-10-10 00:00:06.000," +
        "2020-10-10T00:00,2020-10-10T00:00:15,2020-10-10T00:00:14.999",
      "2020-10-10T00:00:07,3,3.0,3.0,null,Hello,b,2020-10-10 00:00:07.000," +
        "2020-10-10T00:00,2020-10-10T00:00:10,2020-10-10T00:00:09.999",
      "2020-10-10T00:00:07,3,3.0,3.0,null,Hello,b,2020-10-10 00:00:07.000," +
        "2020-10-10T00:00,2020-10-10T00:00:15,2020-10-10T00:00:14.999",
      "2020-10-10T00:00:08,3,null,3.0,3.33,Comment#2,a,2020-10-10 00:00:08.000," +
        "2020-10-10T00:00,2020-10-10T00:00:10,2020-10-10T00:00:09.999",
      "2020-10-10T00:00:08,3,null,3.0,3.33,Comment#2,a,2020-10-10 00:00:08.000," +
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
        "2020-10-10T00:00:30,2020-10-10T00:00:45,2020-10-10T00:00:44.999"
    )
    assertThat(sink.getAppendResults.sorted.mkString("\n"))
      .isEqualTo(expected.sorted.mkString("\n"))
  }

  @TestTemplate
  def testSessionWindow(): Unit = {
    val sql =
      """
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
        |FROM TABLE(SESSION(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toDataStream.addSink(sink)
    env.execute()

    val expected = Seq(
      "2020-10-10T00:00:01,1,1.0,1.0,1.11,Hi,a,2020-10-10 00:00:01.000," +
        "2020-10-10T00:00:01,2020-10-10T00:00:13,2020-10-10T00:00:12.999",
      "2020-10-10T00:00:02,2,2.0,2.0,2.22,Comment#1,a,2020-10-10 00:00:02.000," +
        "2020-10-10T00:00:01,2020-10-10T00:00:13,2020-10-10T00:00:12.999",
      "2020-10-10T00:00:03,2,2.0,2.0,2.22,Comment#1,a,2020-10-10 00:00:03.000," +
        "2020-10-10T00:00:01,2020-10-10T00:00:13,2020-10-10T00:00:12.999",
      "2020-10-10T00:00:04,5,5.0,5.0,5.55,null,a,2020-10-10 00:00:04.000," +
        "2020-10-10T00:00:01,2020-10-10T00:00:13,2020-10-10T00:00:12.999",
      "2020-10-10T00:00:04,5,5.0,null,5.55,Hi,a,2020-10-10 00:00:04.000," +
        "2020-10-10T00:00:01,2020-10-10T00:00:13,2020-10-10T00:00:12.999",
      "2020-10-10T00:00:06,6,6.0,6.0,6.66,Hi,b,2020-10-10 00:00:06.000," +
        "2020-10-10T00:00:01,2020-10-10T00:00:13,2020-10-10T00:00:12.999",
      "2020-10-10T00:00:07,3,3.0,3.0,null,Hello,b,2020-10-10 00:00:07.000," +
        "2020-10-10T00:00:01,2020-10-10T00:00:13,2020-10-10T00:00:12.999",
      "2020-10-10T00:00:08,3,null,3.0,3.33,Comment#2,a,2020-10-10 00:00:08.000," +
        "2020-10-10T00:00:01,2020-10-10T00:00:13,2020-10-10T00:00:12.999",
      "2020-10-10T00:00:16,4,4.0,4.0,4.44,Hi,b,2020-10-10 00:00:16.000," +
        "2020-10-10T00:00:16,2020-10-10T00:00:21,2020-10-10T00:00:20.999",
      "2020-10-10T00:00:32,7,7.0,7.0,7.77,null,null,2020-10-10 00:00:32.000," +
        "2020-10-10T00:00:32,2020-10-10T00:00:39,2020-10-10T00:00:38.999",
      "2020-10-10T00:00:34,1,3.0,3.0,3.33,Comment#3,b,2020-10-10 00:00:34.000," +
        "2020-10-10T00:00:32,2020-10-10T00:00:39,2020-10-10T00:00:38.999"
    )
    assertThat(sink.getAppendResults.sorted.mkString("\n"))
      .isEqualTo(expected.sorted.mkString("\n"))
  }

  @TestTemplate
  def testSessionWindowWithPartitionBy(): Unit = {
    val sql =
      """
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
        |FROM TABLE(SESSION(TABLE T1 PARTITION BY `name`, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toDataStream.addSink(sink)
    env.execute()

    val expected = Seq(
      "2020-10-10T00:00:01,1,1.0,1.0,1.11,Hi,a,2020-10-10 00:00:01.000," +
        "2020-10-10T00:00:01,2020-10-10T00:00:13,2020-10-10T00:00:12.999",
      "2020-10-10T00:00:02,2,2.0,2.0,2.22,Comment#1,a,2020-10-10 00:00:02.000," +
        "2020-10-10T00:00:01,2020-10-10T00:00:13,2020-10-10T00:00:12.999",
      "2020-10-10T00:00:03,2,2.0,2.0,2.22,Comment#1,a,2020-10-10 00:00:03.000," +
        "2020-10-10T00:00:01,2020-10-10T00:00:13,2020-10-10T00:00:12.999",
      "2020-10-10T00:00:04,5,5.0,5.0,5.55,null,a,2020-10-10 00:00:04.000," +
        "2020-10-10T00:00:01,2020-10-10T00:00:13,2020-10-10T00:00:12.999",
      "2020-10-10T00:00:04,5,5.0,null,5.55,Hi,a,2020-10-10 00:00:04.000," +
        "2020-10-10T00:00:01,2020-10-10T00:00:13,2020-10-10T00:00:12.999",
      "2020-10-10T00:00:06,6,6.0,6.0,6.66,Hi,b,2020-10-10 00:00:06.000," +
        "2020-10-10T00:00:06,2020-10-10T00:00:12,2020-10-10T00:00:11.999",
      "2020-10-10T00:00:07,3,3.0,3.0,null,Hello,b,2020-10-10 00:00:07.000," +
        "2020-10-10T00:00:06,2020-10-10T00:00:12,2020-10-10T00:00:11.999",
      "2020-10-10T00:00:08,3,null,3.0,3.33,Comment#2,a,2020-10-10 00:00:08.000," +
        "2020-10-10T00:00:01,2020-10-10T00:00:13,2020-10-10T00:00:12.999",
      "2020-10-10T00:00:16,4,4.0,4.0,4.44,Hi,b,2020-10-10 00:00:16.000," +
        "2020-10-10T00:00:16,2020-10-10T00:00:21,2020-10-10T00:00:20.999",
      "2020-10-10T00:00:32,7,7.0,7.0,7.77,null,null,2020-10-10 00:00:32.000," +
        "2020-10-10T00:00:32,2020-10-10T00:00:37,2020-10-10T00:00:36.999",
      "2020-10-10T00:00:34,1,3.0,3.0,3.33,Comment#3,b,2020-10-10 00:00:34.000," +
        "2020-10-10T00:00:34,2020-10-10T00:00:39,2020-10-10T00:00:38.999"
    )
    assertThat(sink.getAppendResults.sorted.mkString("\n"))
      .isEqualTo(expected.sorted.mkString("\n"))
  }

  /**
   * Tests CUMULATE window with emit-only-on-update mode enabled.
   *
   * <p>When emit_only_on_update is set to true, the window only emits results when new data arrives
   * within the step interval. This reduces unnecessary outputs for sparse data streams.
   */
  @TestTemplate
  def testCumulateWindowWithEmitOnlyOnUpdate(): Unit = {
    val sql =
      """
        |SELECT
        |  sum(`int`),
        |  window_start,
        |  window_end
        |FROM TABLE(
        |  CUMULATE(
        |    TABLE T1,
        |    DESCRIPTOR(rowtime),
        |    INTERVAL '5' SECOND,
        |    INTERVAL '15' SECOND,
        |    INTERVAL '0' SECOND,
        |    true))
        |GROUP BY window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toDataStream.addSink(sink)
    env.execute()

    // With emit_only_on_update = true, outputs are only emitted when new data arrives
    // within the step interval. Data in the first window period [00:00:00, 00:00:15):
    // - [00:00:00, 00:00:05): 1+2+2+5 = 10 (late event hasn't arrived yet)
    // - [00:00:00, 00:00:10): += 6+3+3+5(late) = 17, total = 27
    // - [00:00:00, 00:00:15): no new data in this step, skipped due to emitOnlyOnUpdate
    //
    // Second window period [00:00:15, 00:00:30):
    // - [00:00:15, 00:00:20): 4
    // - [00:00:15, 00:00:25): no new data, skipped
    // - [00:00:15, 00:00:30): no new data, skipped
    //
    // Third window period [00:00:30, 00:00:45):
    // - [00:00:30, 00:00:35): 7+1 = 8
    // - [00:00:30, 00:00:40): no new data, skipped
    // - [00:00:30, 00:00:45): no new data, skipped
    val expected = Seq(
      "10,2020-10-10T00:00,2020-10-10T00:00:05",
      "27,2020-10-10T00:00,2020-10-10T00:00:10",
      "4,2020-10-10T00:00:15,2020-10-10T00:00:20",
      "8,2020-10-10T00:00:30,2020-10-10T00:00:35"
    )
    assertThat(sink.getAppendResults.sorted.mkString("\n"))
      .isEqualTo(expected.sorted.mkString("\n"))
  }

  /**
   * Tests CUMULATE window with emit-only-on-update mode disabled (default behavior).
   *
   * <p>When emit_only_on_update is set to false (or not specified), the window emits results
   * for every step, even if no new data arrives. This is the default cumulate behavior.
   */
  @TestTemplate
  def testCumulateWindowWithEmitOnlyOnUpdateDisabled(): Unit = {
    val sql =
      """
        |SELECT
        |  sum(`int`),
        |  window_start,
        |  window_end
        |FROM TABLE(
        |  CUMULATE(
        |    TABLE T1,
        |    DESCRIPTOR(rowtime),
        |    INTERVAL '5' SECOND,
        |    INTERVAL '15' SECOND,
        |    INTERVAL '0' SECOND,
        |    false))
        |GROUP BY window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toDataStream.addSink(sink)
    env.execute()

    // With emit_only_on_update = false, outputs are emitted for every step interval.
    // This is the same as the default CUMULATE behavior.
    // Data in the first window period [00:00:00, 00:00:15):
    // - [00:00:00, 00:00:05): 1+2+2+5+5 = 15
    // - [00:00:00, 00:00:10): 15 + 6+3+3 = 27
    // - [00:00:00, 00:00:15): 27 (no new data, but still emitted)
    //
    // Second window period [00:00:15, 00:00:30):
    // - [00:00:15, 00:00:20): 4
    // - [00:00:15, 00:00:25): 4 (no new data, but still emitted)
    // - [00:00:15, 00:00:30): 4 (no new data, but still emitted)
    //
    // Third window period [00:00:30, 00:00:45):
    // - [00:00:30, 00:00:35): 7+1 = 8
    // - [00:00:30, 00:00:40): 8 (no new data, but still emitted)
    // - [00:00:30, 00:00:45): 8 (no new data, but still emitted)
    val expected = Seq(
      "15,2020-10-10T00:00,2020-10-10T00:00:05",
      "27,2020-10-10T00:00,2020-10-10T00:00:10",
      "27,2020-10-10T00:00,2020-10-10T00:00:15",
      "4,2020-10-10T00:00:15,2020-10-10T00:00:20",
      "4,2020-10-10T00:00:15,2020-10-10T00:00:25",
      "4,2020-10-10T00:00:15,2020-10-10T00:00:30",
      "8,2020-10-10T00:00:30,2020-10-10T00:00:35",
      "8,2020-10-10T00:00:30,2020-10-10T00:00:40",
      "8,2020-10-10T00:00:30,2020-10-10T00:00:45"
    )
    assertThat(sink.getAppendResults.sorted.mkString("\n"))
      .isEqualTo(expected.sorted.mkString("\n"))
  }

  /**
   * Tests CUMULATE window with emit-only-on-update mode and multi-key grouping.
   *
   * <p>This test verifies that emit-only-on-update works correctly when data is grouped
   * by multiple keys (name + window).
   */
  @TestTemplate
  def testCumulateWindowWithEmitOnlyOnUpdateAndMultiKey(): Unit = {
    val sql =
      """
        |SELECT
        |  `name`,
        |  sum(`int`),
        |  window_start,
        |  window_end
        |FROM TABLE(
        |  CUMULATE(
        |    TABLE T1,
        |    DESCRIPTOR(rowtime),
        |    INTERVAL '5' SECOND,
        |    INTERVAL '15' SECOND,
        |    INTERVAL '0' SECOND,
        |    true))
        |GROUP BY `name`, window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toDataStream.addSink(sink)
    env.execute()

    // With emit_only_on_update = true and grouping by name:
    // For key 'a':
    // - [00:00:00, 00:00:05): 1+2+2+5+5 = 15
    // - [00:00:00, 00:00:10): 15 + 3 = 18
    // - [00:00:00, 00:00:15): no new data for 'a', skipped
    //
    // For key 'b':
    // - [00:00:00, 00:00:05): no data for 'b' in this step
    // - [00:00:00, 00:00:10): 6+3 = 9
    // - [00:00:00, 00:00:15): no new data for 'b', skipped
    // - [00:00:15, 00:00:20): 4
    // - [00:00:30, 00:00:35): 1
    //
    // For key null:
    // - [00:00:30, 00:00:35): 7
    val expected = Seq(
      "a,15,2020-10-10T00:00,2020-10-10T00:00:05",
      "a,18,2020-10-10T00:00,2020-10-10T00:00:10",
      "b,9,2020-10-10T00:00,2020-10-10T00:00:10",
      "b,4,2020-10-10T00:00:15,2020-10-10T00:00:20",
      "b,1,2020-10-10T00:00:30,2020-10-10T00:00:35",
      "null,7,2020-10-10T00:00:30,2020-10-10T00:00:35"
    )
    assertThat(sink.getAppendResults.sorted.mkString("\n"))
      .isEqualTo(expected.sorted.mkString("\n"))
  }
}
