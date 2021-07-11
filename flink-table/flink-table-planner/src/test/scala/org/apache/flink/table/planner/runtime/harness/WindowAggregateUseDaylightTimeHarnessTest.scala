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

package org.apache.flink.table.planner.runtime.harness

import org.apache.flink.api.scala._
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.data.{RowData, TimestampData}
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.{HEAP_BACKEND, StateBackendMode}
import org.apache.flink.table.planner.runtime.utils.TestData
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor
import org.apache.flink.table.runtime.util.StreamRecordUtils.binaryRecord
import org.apache.flink.types.Row
import org.apache.flink.types.RowKind.INSERT
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}
import java.time.{LocalDateTime, ZoneId}
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.{Collection => JCollection}
import java.util.TimeZone

import scala.collection.JavaConversions._

/**
 * Harness tests for processing-time window aggregate in timezone using DaylightTime.
 * We can't test them in [[WindowAggregateITCase]] because the result is non-deterministic,
 * therefore we use harness to test them.
 */
@RunWith(classOf[Parameterized])
class WindowAggregateUseDaylightTimeHarnessTest(backend: StateBackendMode, timeZone: TimeZone)
  extends HarnessTestBase(backend) {

  @Before
  override def before(): Unit = {
    super.before()
    val dataId = TestValuesTableFactory.registerData(TestData.windowDataWithTimestamp)
    tEnv.getConfig.setLocalTimeZone(timeZone.toZoneId)
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
         | proctime AS PROCTIME()
         |) WITH (
         | 'connector' = 'values',
         | 'data-id' = '$dataId'
         |)
         |""".stripMargin)
  }

  @Test
  def testProcessingTimeWindow(): Unit = {
    val sql =
      """
        |SELECT
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  MAX(`double`),
        |  COUNT(DISTINCT `string`)
        |FROM TABLE(
        |   CUMULATE(
        |     TABLE T1,
        |     DESCRIPTOR(proctime),
        |     INTERVAL '1' HOUR,
        |     INTERVAL '3' HOUR))
        |GROUP BY `name`, window_start, window_end
      """.stripMargin
    val t1 = tEnv.sqlQuery(sql)
    val testHarness = createHarnessTester(t1.toAppendStream[Row], "WindowAggregate")
    // window aggregate put window properties at the end of aggs
    val assertor = new RowDataHarnessAssertor(
      Array(
        DataTypes.STRING().getLogicalType,
        DataTypes.BIGINT().getLogicalType,
        DataTypes.DOUBLE().getLogicalType,
        DataTypes.BIGINT().getLogicalType,
        DataTypes.TIMESTAMP_LTZ(3).getLogicalType,
        DataTypes.TIMESTAMP_LTZ(3).getLogicalType))

    testHarness.open()
    ingestData(testHarness)

    val expected = new ConcurrentLinkedQueue[Object]()
    if (timeZone.useDaylightTime()){
      // two [2021-03-14T00:00:00, 2021-03-14T02:00:00] windows with same agg value
      expected.add(record("a", 1L, 1.0D, 1L, ts("2021-03-14T00:00:00"), ts("2021-03-14T01:00:00")))
      expected.add(record("a", 2L, 2.0D, 2L, ts("2021-03-14T00:00:00"), ts("2021-03-14T02:00:00")))
      expected.add(record("a", 2L, 2.0D, 2L, ts("2021-03-14T00:00:00"), ts("2021-03-14T03:00:00")))
      expected.add(record("a", 1L, 2.0D, 1L, ts("2021-03-14T03:00:00"), ts("2021-03-14T04:00:00")))
      expected.add(record("a", 2L, 5.0D, 1L, ts("2021-03-14T03:00:00"), ts("2021-03-14T05:00:00")))
      expected.add(record("a", 3L, 5.0D, 1L, ts("2021-03-14T03:00:00"), ts("2021-03-14T06:00:00")))

      // [2021-11-07T00:00:00, 2021-11-07T02:00:00] window contains 3 hours data
      expected.add(record("a", 1L, 3.0D, 1L, ts("2021-11-07T00:00:00"), ts("2021-11-07T01:00:00")))
      expected.add(record("a", 3L, 3.0D, 2L, ts("2021-11-07T00:00:00"), ts("2021-11-07T02:00:00")))
      expected.add(record("a", 4L, 3.0D, 2L, ts("2021-11-07T00:00:00"), ts("2021-11-07T03:00:00")))
      expected.add(record("a", 1L, 3.0D, 1L, ts("2021-11-07T03:00:00"), ts("2021-11-07T04:00:00")))
    } else {
      expected.add(record("a", 1L, 1.0D, 1L, ts("2021-03-14T06:00:00"), ts("2021-03-14T09:00:00")))
      expected.add(record("a", 1L, 2.0D, 1L, ts("2021-03-14T09:00:00"), ts("2021-03-14T10:00:00")))
      expected.add(record("a", 2L, 2.0D, 1L, ts("2021-03-14T09:00:00"), ts("2021-03-14T11:00:00")))
      expected.add(record("a", 3L, 5.0D, 1L, ts("2021-03-14T09:00:00"), ts("2021-03-14T12:00:00")))
      expected.add(record("a", 1L, 5.0D, 0L, ts("2021-03-14T12:00:00"), ts("2021-03-14T13:00:00")))
      expected.add(record("a", 1L, 5.0D, 0L, ts("2021-03-14T12:00:00"), ts("2021-03-14T14:00:00")))
      expected.add(record("a", 1L, 5.0D, 0L, ts("2021-03-14T12:00:00"), ts("2021-03-14T15:00:00")))

      expected.add(record("a", 1L, 3.0D, 1L, ts("2021-11-07T06:00:00"), ts("2021-11-07T08:00:00")))
      expected.add(record("a", 2L, 3.0D, 1L, ts("2021-11-07T06:00:00"), ts("2021-11-07T09:00:00")))
      expected.add(record("a", 1L, 3.0D, 1L, ts("2021-11-07T09:00:00"), ts("2021-11-07T10:00:00")))
      expected.add(record("a", 2L, 3.0D, 1L, ts("2021-11-07T09:00:00"), ts("2021-11-07T11:00:00")))
      expected.add(record("a", 3L, 3.0D, 1L, ts("2021-11-07T09:00:00"), ts("2021-11-07T12:00:00")))

    }

    assertor.assertOutputEquals("result mismatch", expected, testHarness.getOutput)
    testHarness.close()
  }

  private def ingestData(
      testHarness: KeyedOneInputStreamOperatorTestHarness[RowData, RowData, RowData]): Unit = {

    // Los_Angeles local time in epoch mills.
    // The DaylightTime in Los_Angele start at time 2021-03-14 02:00:00
    //  long epoch1  = 1615708800000L;  2021-03-14 00:00:00
    //  long epoch2  = 1615712400000L;  2021-03-14 01:00:00
    //  long epoch3  = 1615716000000L;  2021-03-14 03:00:00, skip one hour (2021-03-14 02:00:00)
    //  long epoch4  = 1615719600000L;  2021-03-14 04:00:0

    testHarness.setProcessingTime(1615708800000L)
    testHarness.processElement(record("a", 1d, "Hi", null))
    testHarness.setProcessingTime(1615712400000L)
    testHarness.processElement(record("a", 2d, "Comment#1", null))
    testHarness.setProcessingTime(1615716000000L)
    testHarness.processElement(record("a", 2d, "Comment#1", null))
    testHarness.setProcessingTime(1615719600000L)
    testHarness.processElement(record("a", 5d, null, null))
    testHarness.setProcessingTime(1615723200000L)
    testHarness.processElement(record("a", 5d, null, null))

    // Los_Angeles local time in epoch mills.
    // The DaylightTime in Los_Angele end at time 2021-11-07 02:00:00
    //  long epoch5  = 1636268400000L;  2021-11-07 00:00:00
    //  long epoch6  = 1636272000000L;  the first local timestamp 2021-11-07 01:00:00
    //  long epoch7  = 1636275600000L;  rollback to  2021-11-07 01:00:00
    //  long epoch8  = 1636279200000L;  2021-11-07 02:00:00
    //  long epoch9  = 1636282800000L;  2021-11-07 03:00:00
    //  long epoch10 = 1636286400000L;  2021-11-07 04:00:00
    testHarness.setProcessingTime(1636268400000L)
    testHarness.processElement(record("a", 3d, "Hi", null))
    testHarness.setProcessingTime(1636272000000L)
    testHarness.processElement(record("a", 3d, null, null))
    testHarness.setProcessingTime(1636275600000L)
    testHarness.processElement(record("a", 3d, "Comment#3", null))
    testHarness.setProcessingTime(1636279200000L)
    testHarness.processElement(record("a", 3d, "Comment#3", null))
    testHarness.setProcessingTime(1636282800000L)
    testHarness.processElement(record("a", 3d, "Comment#3", null))
    testHarness.setProcessingTime(1636286400000L)
  }

  private def ts(dt: String): TimestampData = {
    TimestampData.fromEpochMillis(
      LocalDateTime.parse(dt).atZone(ZoneId.of("UTC")).toInstant.toEpochMilli)
  }

  private def record(args: Any*): StreamRecord[RowData] = {
    val objs = args.map {
      case l: Long => Long.box(l)
      case d: Double => Double.box(d)
      case arg@_ => arg.asInstanceOf[Object]
    }.toArray
    binaryRecord(INSERT, objs: _*)
  }
}

object WindowAggregateUseDaylightTimeHarnessTest {

  @Parameterized.Parameters(name = "StateBackend={0}, TimeZone={1}")
  def parameters(): JCollection[Array[Object]] = {
    Seq[Array[AnyRef]](
      Array(HEAP_BACKEND, TimeZone.getTimeZone("America/Los_Angeles")),
      Array(HEAP_BACKEND, TimeZone.getTimeZone("UTC")))
  }
}
