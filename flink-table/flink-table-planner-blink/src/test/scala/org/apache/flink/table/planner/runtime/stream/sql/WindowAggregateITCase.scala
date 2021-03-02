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
class WindowAggregateITCase(mode: StateBackendMode)
  extends StreamingWithStateTestBase(mode) {

  // -------------------------------------------------------------------------------
  // Expected output data for TUMBLE WINDOW tests
  // Result of CUBE(name), ROLLUP(name), GROUPING SETS((`name`),()) should be same
  // -------------------------------------------------------------------------------
  val TumbleWindowGroupSetExpectedData = Seq(
    "0,a,2020-10-10T00:00,2020-10-10T00:00:05,4,11.10,5.0,1.0,2,Hi|Comment#1",
    "0,a,2020-10-10T00:00:05,2020-10-10T00:00:10,1,3.33,null,3.0,1,Comment#2",
    "0,b,2020-10-10T00:00:05,2020-10-10T00:00:10,2,6.66,6.0,3.0,2,Hello|Hi",
    "0,b,2020-10-10T00:00:15,2020-10-10T00:00:20,1,4.44,4.0,4.0,1,Hi",
    "0,b,2020-10-10T00:00:30,2020-10-10T00:00:35,1,3.33,3.0,3.0,1,Comment#3",
    "0,null,2020-10-10T00:00:30,2020-10-10T00:00:35,1,7.77,7.0,7.0,0,null",
    "1,null,2020-10-10T00:00,2020-10-10T00:00:05,4,11.10,5.0,1.0,2,Hi|Comment#1",
    "1,null,2020-10-10T00:00:05,2020-10-10T00:00:10,3,9.99,6.0,3.0,3,Hello|Hi|Comment#2",
    "1,null,2020-10-10T00:00:15,2020-10-10T00:00:20,1,4.44,4.0,4.0,1,Hi",
    "1,null,2020-10-10T00:00:30,2020-10-10T00:00:35,2,11.10,7.0,3.0,1,Comment#3"
  )

  val TumbleWindowCubeExpectedData = TumbleWindowGroupSetExpectedData

  val TumbleWindowRollupExpectedData = TumbleWindowGroupSetExpectedData

  // -------------------------------------------------------------------------------
  // Expected output data for HOP WINDOW tests
  // Result of CUBE(name), ROLLUP(name), GROUPING SETS((`name`),()) should be same
  // -------------------------------------------------------------------------------
  val HopWindowGroupSetExpectedData = Seq(
    "0,a,2020-10-09T23:59:55,2020-10-10T00:00:05,4,11.10,5.0,1.0,2,Hi|Comment#1",
    "0,a,2020-10-10T00:00,2020-10-10T00:00:10,5,14.43,5.0,1.0,3,Comment#2|Hi|Comment#1",
    "0,a,2020-10-10T00:00:05,2020-10-10T00:00:15,1,3.33,null,3.0,1,Comment#2",
    "0,b,2020-10-10T00:00,2020-10-10T00:00:10,2,6.66,6.0,3.0,2,Hello|Hi",
    "0,b,2020-10-10T00:00:05,2020-10-10T00:00:15,2,6.66,6.0,3.0,2,Hello|Hi",
    "0,b,2020-10-10T00:00:10,2020-10-10T00:00:20,1,4.44,4.0,4.0,1,Hi",
    "0,b,2020-10-10T00:00:15,2020-10-10T00:00:25,1,4.44,4.0,4.0,1,Hi",
    "0,b,2020-10-10T00:00:25,2020-10-10T00:00:35,1,3.33,3.0,3.0,1,Comment#3",
    "0,b,2020-10-10T00:00:30,2020-10-10T00:00:40,1,3.33,3.0,3.0,1,Comment#3",
    "0,null,2020-10-10T00:00:25,2020-10-10T00:00:35,1,7.77,7.0,7.0,0,null",
    "0,null,2020-10-10T00:00:30,2020-10-10T00:00:40,1,7.77,7.0,7.0,0,null",
    "1,null,2020-10-09T23:59:55,2020-10-10T00:00:05,4,11.10,5.0,1.0,2,Hi|Comment#1",
    "1,null,2020-10-10T00:00,2020-10-10T00:00:10,7,21.09,6.0,1.0,4,Hello|Hi|Comment#2|Comment#1",
    "1,null,2020-10-10T00:00:05,2020-10-10T00:00:15,3,9.99,6.0,3.0,3,Hello|Hi|Comment#2",
    "1,null,2020-10-10T00:00:10,2020-10-10T00:00:20,1,4.44,4.0,4.0,1,Hi",
    "1,null,2020-10-10T00:00:15,2020-10-10T00:00:25,1,4.44,4.0,4.0,1,Hi",
    "1,null,2020-10-10T00:00:25,2020-10-10T00:00:35,2,11.10,7.0,3.0,1,Comment#3",
    "1,null,2020-10-10T00:00:30,2020-10-10T00:00:40,2,11.10,7.0,3.0,1,Comment#3"
  )
  val HopWindowCubeExpectedData = HopWindowGroupSetExpectedData

  val HopWindowRollupExpectedData = HopWindowGroupSetExpectedData

  // -------------------------------------------------------------------------------
  // Expected output data for CUMULATE WINDOW tests
  // Result of CUBE(name), ROLLUP(name), GROUPING SETS((`name`),()) should be same
  // -------------------------------------------------------------------------------
  val CumulateWindowGroupSetExpectedData = Seq(
    "0,a,2020-10-10T00:00,2020-10-10T00:00:05,4,11.10,5.0,1.0,2,Hi|Comment#1",
    "0,a,2020-10-10T00:00,2020-10-10T00:00:10,5,14.43,5.0,1.0,3,Hi|Comment#1|Comment#2",
    "0,a,2020-10-10T00:00,2020-10-10T00:00:15,5,14.43,5.0,1.0,3,Hi|Comment#1|Comment#2",
    "0,b,2020-10-10T00:00,2020-10-10T00:00:10,2,6.66,6.0,3.0,2,Hello|Hi",
    "0,b,2020-10-10T00:00,2020-10-10T00:00:15,2,6.66,6.0,3.0,2,Hello|Hi",
    "0,b,2020-10-10T00:00:15,2020-10-10T00:00:20,1,4.44,4.0,4.0,1,Hi",
    "0,b,2020-10-10T00:00:15,2020-10-10T00:00:25,1,4.44,4.0,4.0,1,Hi",
    "0,b,2020-10-10T00:00:15,2020-10-10T00:00:30,1,4.44,4.0,4.0,1,Hi",
    "0,b,2020-10-10T00:00:30,2020-10-10T00:00:35,1,3.33,3.0,3.0,1,Comment#3",
    "0,b,2020-10-10T00:00:30,2020-10-10T00:00:40,1,3.33,3.0,3.0,1,Comment#3",
    "0,b,2020-10-10T00:00:30,2020-10-10T00:00:45,1,3.33,3.0,3.0,1,Comment#3",
    "0,null,2020-10-10T00:00:30,2020-10-10T00:00:35,1,7.77,7.0,7.0,0,null",
    "0,null,2020-10-10T00:00:30,2020-10-10T00:00:40,1,7.77,7.0,7.0,0,null",
    "0,null,2020-10-10T00:00:30,2020-10-10T00:00:45,1,7.77,7.0,7.0,0,null",
    "1,null,2020-10-10T00:00,2020-10-10T00:00:05,4,11.10,5.0,1.0,2,Hi|Comment#1",
    "1,null,2020-10-10T00:00,2020-10-10T00:00:10,7,21.09,6.0,1.0,4,Hi|Comment#1|Hello|Comment#2",
    "1,null,2020-10-10T00:00,2020-10-10T00:00:15,7,21.09,6.0,1.0,4,Hi|Comment#1|Hello|Comment#2",
    "1,null,2020-10-10T00:00:15,2020-10-10T00:00:20,1,4.44,4.0,4.0,1,Hi",
    "1,null,2020-10-10T00:00:15,2020-10-10T00:00:25,1,4.44,4.0,4.0,1,Hi",
    "1,null,2020-10-10T00:00:15,2020-10-10T00:00:30,1,4.44,4.0,4.0,1,Hi",
    "1,null,2020-10-10T00:00:30,2020-10-10T00:00:35,2,11.10,7.0,3.0,1,Comment#3",
    "1,null,2020-10-10T00:00:30,2020-10-10T00:00:40,2,11.10,7.0,3.0,1,Comment#3",
    "1,null,2020-10-10T00:00:30,2020-10-10T00:00:45,2,11.10,7.0,3.0,1,Comment#3"
  )

  val CumulateWindowCubeExpectedData = CumulateWindowGroupSetExpectedData

  val CumulateWindowRollupExpectedData = CumulateWindowGroupSetExpectedData

  @Before
  override def before(): Unit = {
    super.before()
    // enable checkpoint, we are using failing source to force have a complete checkpoint
    // and cover restore path
    env.enableCheckpointing(100, CheckpointingMode.EXACTLY_ONCE)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0))
    FailingCollectionSource.reset()

    val dataId = TestValuesTableFactory.registerData(TestData.windowData)
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
        |SELECT
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  SUM(`bigdec`),
        |  MAX(`double`),
        |  MIN(`float`),
        |  COUNT(DISTINCT `string`),
        |  concat_distinct_agg(`string`)
        |FROM TABLE(
        |   TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |GROUP BY `name`, window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "a,2020-10-10T00:00,2020-10-10T00:00:05,4,11.10,5.0,1.0,2,Hi|Comment#1",
      "a,2020-10-10T00:00:05,2020-10-10T00:00:10,1,3.33,null,3.0,1,Comment#2",
      "b,2020-10-10T00:00:05,2020-10-10T00:00:10,2,6.66,6.0,3.0,2,Hello|Hi",
      "b,2020-10-10T00:00:15,2020-10-10T00:00:20,1,4.44,4.0,4.0,1,Hi",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:35,1,3.33,3.0,3.0,1,Comment#3",
      "null,2020-10-10T00:00:30,2020-10-10T00:00:35,1,7.77,7.0,7.0,0,null")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeTumbleWindow_GroupingSets(): Unit = {
    val sql =
      """
        |SELECT
        |  GROUPING_ID(`name`),
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  SUM(`bigdec`),
        |  MAX(`double`),
        |  MIN(`float`),
        |  COUNT(DISTINCT `string`),
        |  concat_distinct_agg(`string`)
        |FROM TABLE(
        |   TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |GROUP BY GROUPING SETS((`name`),()), window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    assertEquals(
      TumbleWindowGroupSetExpectedData.sorted.mkString("\n"),
      sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeTumbleWindow_Cube(): Unit = {
    val sql =
      """
        |SELECT
        |  GROUPING_ID(`name`),
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  SUM(`bigdec`),
        |  MAX(`double`),
        |  MIN(`float`),
        |  COUNT(DISTINCT `string`),
        |  concat_distinct_agg(`string`)
        |FROM TABLE(
        |   TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |GROUP BY CUBE(`name`), window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    assertEquals(
      TumbleWindowCubeExpectedData.sorted.mkString("\n"),
      sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeTumbleWindow_Rollup(): Unit = {
    val sql =
      """
        |SELECT
        |  GROUPING_ID(`name`),
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  SUM(`bigdec`),
        |  MAX(`double`),
        |  MIN(`float`),
        |  COUNT(DISTINCT `string`),
        |  concat_distinct_agg(`string`)
        |FROM TABLE(
        |   TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |GROUP BY ROLLUP(`name`), window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    assertEquals(
      TumbleWindowRollupExpectedData.sorted.mkString("\n"),
      sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testTumbleWindowOutputWindowTime(): Unit = {
    val sql =
      """
        |SELECT
        |  `name`,
        |  window_start,
        |  window_end,
        |  window_time,
        |  COUNT(*)
        |FROM TABLE(
        |   TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |GROUP BY `name`, window_start, window_end, window_time
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "a,2020-10-10T00:00,2020-10-10T00:00:05,2020-10-10T00:00:04.999,4",
      "a,2020-10-10T00:00:05,2020-10-10T00:00:10,2020-10-10T00:00:09.999,1",
      "b,2020-10-10T00:00:05,2020-10-10T00:00:10,2020-10-10T00:00:09.999,2",
      "b,2020-10-10T00:00:15,2020-10-10T00:00:20,2020-10-10T00:00:19.999,1",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:35,2020-10-10T00:00:34.999,1",
      "null,2020-10-10T00:00:30,2020-10-10T00:00:35,2020-10-10T00:00:34.999,1")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testTumbleWindowGroupOnWindowOnly(): Unit = {
    val sql =
      """
        |SELECT
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  SUM(`bigdec`),
        |  MAX(`double`),
        |  MIN(`float`),
        |  COUNT(DISTINCT `string`),
        |  concat_distinct_agg(`string`)
        |FROM TABLE(
        |   TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |GROUP BY window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "2020-10-10T00:00,2020-10-10T00:00:05,4,11.10,5.0,1.0,2,Hi|Comment#1",
      "2020-10-10T00:00:05,2020-10-10T00:00:10,3,9.99,6.0,3.0,3,Hello|Hi|Comment#2",
      "2020-10-10T00:00:15,2020-10-10T00:00:20,1,4.44,4.0,4.0,1,Hi",
      "2020-10-10T00:00:30,2020-10-10T00:00:35,2,11.10,7.0,3.0,1,Comment#3")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testTumbleWindowWithoutOutputWindowColumns(): Unit = {
    val sql =
      """
        |SELECT
        |  COUNT(*),
        |  SUM(`bigdec`),
        |  MAX(`double`),
        |  MIN(`float`),
        |  COUNT(DISTINCT `string`),
        |  concat_distinct_agg(`string`)
        |FROM TABLE(
        |   TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |GROUP BY window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "4,11.10,5.0,1.0,2,Hi|Comment#1",
      "3,9.99,6.0,3.0,3,Hello|Hi|Comment#2",
      "1,4.44,4.0,4.0,1,Hi",
      "2,11.10,7.0,3.0,1,Comment#3")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeHopWindow(): Unit = {
    val sql =
      """
        |SELECT
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  SUM(`bigdec`),
        |  MAX(`double`),
        |  MIN(`float`),
        |  COUNT(DISTINCT `string`),
        |  concat_distinct_agg(`string`)
        |FROM TABLE(
        |   HOP(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '10' SECOND))
        |GROUP BY `name`, window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "a,2020-10-09T23:59:55,2020-10-10T00:00:05,4,11.10,5.0,1.0,2,Hi|Comment#1",
      "a,2020-10-10T00:00,2020-10-10T00:00:10,5,14.43,5.0,1.0,3,Comment#2|Hi|Comment#1",
      "a,2020-10-10T00:00:05,2020-10-10T00:00:15,1,3.33,null,3.0,1,Comment#2",
      "b,2020-10-10T00:00,2020-10-10T00:00:10,2,6.66,6.0,3.0,2,Hello|Hi",
      "b,2020-10-10T00:00:05,2020-10-10T00:00:15,2,6.66,6.0,3.0,2,Hello|Hi",
      "b,2020-10-10T00:00:10,2020-10-10T00:00:20,1,4.44,4.0,4.0,1,Hi",
      "b,2020-10-10T00:00:15,2020-10-10T00:00:25,1,4.44,4.0,4.0,1,Hi",
      "b,2020-10-10T00:00:25,2020-10-10T00:00:35,1,3.33,3.0,3.0,1,Comment#3",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:40,1,3.33,3.0,3.0,1,Comment#3",
      "null,2020-10-10T00:00:25,2020-10-10T00:00:35,1,7.77,7.0,7.0,0,null",
      "null,2020-10-10T00:00:30,2020-10-10T00:00:40,1,7.77,7.0,7.0,0,null")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeHopWindow_GroupingSets(): Unit = {
    val sql =
      """
        |SELECT
        |  GROUPING_ID(`name`),
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  SUM(`bigdec`),
        |  MAX(`double`),
        |  MIN(`float`),
        |  COUNT(DISTINCT `string`),
        |  concat_distinct_agg(`string`)
        |FROM TABLE(
        |   HOP(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '10' SECOND))
        |GROUP BY GROUPING SETS((`name`),()), window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    assertEquals(
      HopWindowGroupSetExpectedData.sorted.mkString("\n"),
      sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeHopWindow_Cube(): Unit = {
    val sql =
      """
        |SELECT
        |  GROUPING_ID(`name`),
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  SUM(`bigdec`),
        |  MAX(`double`),
        |  MIN(`float`),
        |  COUNT(DISTINCT `string`),
        |  concat_distinct_agg(`string`)
        |FROM TABLE(
        |   HOP(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '10' SECOND))
        |GROUP BY CUBE(`name`), window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    assertEquals(
      HopWindowCubeExpectedData.sorted.mkString("\n"),
      sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeHopWindow_Rollup(): Unit = {
    val sql =
      """
        |SELECT
        |  GROUPING_ID(`name`),
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  SUM(`bigdec`),
        |  MAX(`double`),
        |  MIN(`float`),
        |  COUNT(DISTINCT `string`),
        |  concat_distinct_agg(`string`)
        |FROM TABLE(
        |   HOP(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '10' SECOND))
        |GROUP BY ROLLUP(`name`), window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    assertEquals(
      HopWindowRollupExpectedData.sorted.mkString("\n"),
      sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeCumulateWindow(): Unit = {
    val sql =
      """
        |SELECT
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  SUM(`bigdec`),
        |  MAX(`double`),
        |  MIN(`float`),
        |  COUNT(DISTINCT `string`),
        |  concat_distinct_agg(`string`)
        |FROM TABLE(
        |   CUMULATE(
        |     TABLE T1,
        |     DESCRIPTOR(rowtime),
        |     INTERVAL '5' SECOND,
        |     INTERVAL '15' SECOND))
        |GROUP BY `name`, window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "a,2020-10-10T00:00,2020-10-10T00:00:05,4,11.10,5.0,1.0,2,Hi|Comment#1",
      "a,2020-10-10T00:00,2020-10-10T00:00:10,5,14.43,5.0,1.0,3,Hi|Comment#1|Comment#2",
      "a,2020-10-10T00:00,2020-10-10T00:00:15,5,14.43,5.0,1.0,3,Hi|Comment#1|Comment#2",
      "b,2020-10-10T00:00,2020-10-10T00:00:10,2,6.66,6.0,3.0,2,Hello|Hi",
      "b,2020-10-10T00:00,2020-10-10T00:00:15,2,6.66,6.0,3.0,2,Hello|Hi",
      "b,2020-10-10T00:00:15,2020-10-10T00:00:20,1,4.44,4.0,4.0,1,Hi",
      "b,2020-10-10T00:00:15,2020-10-10T00:00:25,1,4.44,4.0,4.0,1,Hi",
      "b,2020-10-10T00:00:15,2020-10-10T00:00:30,1,4.44,4.0,4.0,1,Hi",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:35,1,3.33,3.0,3.0,1,Comment#3",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:40,1,3.33,3.0,3.0,1,Comment#3",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:45,1,3.33,3.0,3.0,1,Comment#3",
      "null,2020-10-10T00:00:30,2020-10-10T00:00:35,1,7.77,7.0,7.0,0,null",
      "null,2020-10-10T00:00:30,2020-10-10T00:00:40,1,7.77,7.0,7.0,0,null",
      "null,2020-10-10T00:00:30,2020-10-10T00:00:45,1,7.77,7.0,7.0,0,null")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeCumulateWindow_GroupingSets(): Unit = {
    val sql =
      """
        |SELECT
        |  GROUPING_ID(`name`),
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  SUM(`bigdec`),
        |  MAX(`double`),
        |  MIN(`float`),
        |  COUNT(DISTINCT `string`),
        |  concat_distinct_agg(`string`)
        |FROM TABLE(
        |   CUMULATE(
        |     TABLE T1,
        |     DESCRIPTOR(rowtime),
        |     INTERVAL '5' SECOND,
        |     INTERVAL '15' SECOND))
        |GROUP BY GROUPING SETS((`name`),()), window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    assertEquals(
      CumulateWindowGroupSetExpectedData.sorted.mkString("\n"),
      sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeCumulateWindow_Cube(): Unit = {
    val sql =
      """
        |SELECT
        |  GROUPING_ID(`name`),
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  SUM(`bigdec`),
        |  MAX(`double`),
        |  MIN(`float`),
        |  COUNT(DISTINCT `string`),
        |  concat_distinct_agg(`string`)
        |FROM TABLE(
        |   CUMULATE(
        |     TABLE T1,
        |     DESCRIPTOR(rowtime),
        |     INTERVAL '5' SECOND,
        |     INTERVAL '15' SECOND))
        |GROUP BY Cube(`name`), window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    assertEquals(
      CumulateWindowCubeExpectedData.sorted.mkString("\n"),
      sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeCumulateWindow_Rollup(): Unit = {
    val sql =
      """
        |SELECT
        |  GROUPING_ID(`name`),
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  SUM(`bigdec`),
        |  MAX(`double`),
        |  MIN(`float`),
        |  COUNT(DISTINCT `string`),
        |  concat_distinct_agg(`string`)
        |FROM TABLE(
        |   CUMULATE(
        |     TABLE T1,
        |     DESCRIPTOR(rowtime),
        |     INTERVAL '5' SECOND,
        |     INTERVAL '15' SECOND))
        |GROUP BY ROLLUP(`name`), window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    assertEquals(
      CumulateWindowRollupExpectedData.sorted.mkString("\n"),
      sink.getAppendResults.sorted.mkString("\n"))
  }
}
