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

package org.apache.flink.table.planner.runtime.stream.table

import java.time.{LocalDateTime, ZoneOffset}
import java.util.TimeZone

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.factories.TestValuesTableFactory.TestSinkContextTableSink
import org.apache.flink.table.planner.runtime.utils.{AbstractExactlyOnceSink, StreamingTestBase, TestSinkUtil, TestingRetractSink}
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.JavaConversions._

/**
 * Test the conversion between [[Table]] and [[DataStream]] should
 * not loss row time attribute.
 */
final class TableToDataStreamITCase extends StreamingTestBase {

  @Test
  def testHasRowtimeFromTableToAppendStream(): Unit = {
    val data = List(
      rowOf(localDateTime(1L), "A"),
      rowOf(localDateTime(2L), "B"),
      rowOf(localDateTime(3L), "C"),
      rowOf(localDateTime(4L), "D"),
      rowOf(localDateTime(7L), "E"))

    val dataId: String = TestValuesTableFactory.registerData(data)

    val sourceDDL =
      s"""
         |CREATE TABLE src (
         |  ts TIMESTAMP(3),
         |  a STRING,
         |  WATERMARK FOR ts AS ts - INTERVAL '0.005' SECOND
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$dataId'
         |)
      """.stripMargin

    tEnv.executeSql(sourceDDL)
    val dataStream = tEnv.sqlQuery("SELECT a, ts FROM src").toAppendStream[Row]

    val expected = List(
      "+I[A, 1970-01-01T00:00:01], 1000",
      "+I[B, 1970-01-01T00:00:02], 2000",
      "+I[C, 1970-01-01T00:00:03], 3000",
      "+I[D, 1970-01-01T00:00:04], 4000",
      "+I[E, 1970-01-01T00:00:07], 7000")

    val sink = new StringWithTimestampSink[Row]
    dataStream.addSink(sink)
    env.execute("TableToAppendStream")
    assertEquals(expected, sink.getResults.sorted)

  }

  @Test
  def testHasRowtimeFromTableToRetractStream(): Unit = {
    val data = List(
      rowOf(localDateTime(1L), "A"),
      rowOf(localDateTime(2L), "A"),
      rowOf(localDateTime(3L), "C"),
      rowOf(localDateTime(4L), "D"),
      rowOf(localDateTime(7L), "E"))

    val dataId: String = TestValuesTableFactory.registerData(data)

    val sourceDDL =
      s"""
         |CREATE TABLE src (
         |  ts TIMESTAMP(3),
         |  a STRING,
         |  WATERMARK FOR ts AS ts - INTERVAL '0.005' SECOND
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$dataId'
         |)
      """.stripMargin

    tEnv.executeSql(sourceDDL)
    val dataStream = tEnv.sqlQuery(
      """
        |SELECT a, ts
        |FROM (
        |  SELECT *,
        |    ROW_NUMBER() OVER (PARTITION BY a ORDER BY ts DESC) as rowNum
        |  FROM src
        |)
        |WHERE rowNum = 1
      """.stripMargin
    ).toRetractStream[Row]

    val sink = new StringWithTimestampRetractSink[Row]
    dataStream.addSink(sink)
    env.execute("TableToRetractStream")

    val expected = List(
      "A,1970-01-01T00:00:02,2000",
      "C,1970-01-01T00:00:03,3000",
      "D,1970-01-01T00:00:04,4000",
      "E,1970-01-01T00:00:07,7000")
    assertEquals(expected, sink.getRetractResults.sorted)

    val expectedRetract = List(
      "(true,A,1970-01-01T00:00:01,1000)",
      "(false,A,1970-01-01T00:00:01,1000)",
      "(true,A,1970-01-01T00:00:02,2000)",
      "(true,C,1970-01-01T00:00:03,3000)",
      "(true,D,1970-01-01T00:00:04,4000)",
      "(true,E,1970-01-01T00:00:07,7000)")
    assertEquals(expectedRetract.sorted, sink.getRawResults.sorted)
  }

  @Test
  def testHasRowtimeFromDataStreamToTableBackDataStream(): Unit = {
    val data = Seq(
      (1L, "A"),
      (2L, "B"),
      (3L, "C"),
      (4L, "D"),
      (7L, "E"))

    val ds1 = env.fromCollection(data)
      // second to millisecond
      .assignAscendingTimestamps(_._1 * 1000L)
    val table = ds1.toTable(tEnv, 'ts, 'a, 'rowtime.rowtime)
    tEnv.registerTable("t1", table)

    val ds2 = tEnv.sqlQuery(
      """
        | SELECT CONCAT(a, '_'), ts, rowtime
        | FROM t1
      """.stripMargin
    ).toAppendStream[Row]

    val expected = List(
      "+I[A_, 1, 1970-01-01T00:00:01], 1000",
      "+I[B_, 2, 1970-01-01T00:00:02], 2000",
      "+I[C_, 3, 1970-01-01T00:00:03], 3000",
      "+I[D_, 4, 1970-01-01T00:00:04], 4000",
      "+I[E_, 7, 1970-01-01T00:00:07], 7000")

    val sink = new StringWithTimestampSink[Row]
    ds2.addSink(sink)
    env.execute("DataStreamToTableBackDataStream")
    assertEquals(expected, sink.getResults.sorted)
  }

  @Test
  def testHasRowtimeFromTableToExternalSystem(): Unit = {
    val data = List(
      rowOf("1970-01-01 00:00:00.001", localDateTime(1L), 1, 1d),
      rowOf("1970-01-01 00:00:00.002", localDateTime(2L), 1, 2d),
      rowOf("1970-01-01 00:00:00.003", localDateTime(3L), 1, 2d),
      rowOf("1970-01-01 00:00:00.004", localDateTime(4L), 1, 5d),
      rowOf("1970-01-01 00:00:00.007", localDateTime(7L), 1, 3d),
      rowOf("1970-01-01 00:00:00.008", localDateTime(8L), 1, 3d),
      rowOf("1970-01-01 00:00:00.016", localDateTime(16L), 1, 4d))

    val dataId: String = TestValuesTableFactory.registerData(data)

    val sourceDDL =
      s"""
         |CREATE TABLE src (
         |  log_ts STRING,
         |  ts TIMESTAMP(3),
         |  a INT,
         |  b DOUBLE,
         |  WATERMARK FOR ts AS ts - INTERVAL '0.001' SECOND
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$dataId'
         |)
      """.stripMargin

    val sinkDDL =
      s"""
         |CREATE TABLE sink (
         |  log_ts STRING,
         |  ts TIMESTAMP(3),
         |  a INT,
         |  b DOUBLE
         |) WITH (
         |  'connector' = 'values',
         |  'table-sink-class' = '${classOf[TestSinkContextTableSink].getName}'
         |)
      """.stripMargin

    tEnv.executeSql(sourceDDL)
    tEnv.executeSql(sinkDDL)

    //---------------------------------------------------------------------------------------
    // Verify writing out a source directly with the rowtime attribute
    //---------------------------------------------------------------------------------------

    tEnv.executeSql("INSERT INTO sink SELECT * FROM src").await()

    val expected = List(1000, 2000, 3000, 4000, 7000, 8000, 16000)
    assertEquals(expected.sorted, TestSinkContextTableSink.ROWTIMES.sorted)

    val sinkDDL2 =
      s"""
         |CREATE TABLE sink2 (
         |  window_rowtime TIMESTAMP(3),
         |  b DOUBLE
         |) WITH (
         |  'connector' = 'values',
         |  'table-sink-class' = '${classOf[TestSinkContextTableSink].getName}'
         |)
      """.stripMargin
    tEnv.executeSql(sinkDDL2)

    //---------------------------------------------------------------------------------------
    // Verify writing out with additional operator to generate a new rowtime attribute
    //---------------------------------------------------------------------------------------

    tEnv.executeSql(
      """
        |INSERT INTO sink2
        |SELECT
        |  TUMBLE_ROWTIME(ts, INTERVAL '5' SECOND),
        |  SUM(b)
        |FROM src
        |GROUP BY TUMBLE(ts, INTERVAL '5' SECOND)
        |""".stripMargin
    ).await()

    val expected2 = List(4999, 9999, 19999)
    assertEquals(expected2.sorted, TestSinkContextTableSink.ROWTIMES.sorted)
  }

  private def localDateTime(epochSecond: Long): LocalDateTime = {
    LocalDateTime.ofEpochSecond(epochSecond, 0, ZoneOffset.UTC)
  }
}

/**
 * Append test Sink that outputs record with timestamp.
 */
final class StringWithTimestampSink[T] extends AbstractExactlyOnceSink[T]() {

  override def invoke(value: T, context: SinkFunction.Context): Unit = {
    localResults += s"${value.toString}, ${context.timestamp()}"
  }

  override def getResults: List[String] = super.getResults
}

/**
 * Retract test Sink that outputs record with timestamp.
 */
final class StringWithTimestampRetractSink[T](tz: TimeZone) extends
  TestingRetractSink(tz) {

  def this() {
    this(TimeZone.getTimeZone("UTC"))
  }

  override def invoke(v: (Boolean, Row), context: SinkFunction.Context): Unit = {
    this.synchronized {
      val rowString = s"${TestSinkUtil.rowToString(v._2, tz)},${context.timestamp()}"

      val tupleString = "(" + v._1.toString + "," + rowString + ")"
      localResults += tupleString
      if (v._1) {
        localRetractResults += rowString
      } else {
        val index = localRetractResults.indexOf(rowString)
        if (index >= 0) {
          localRetractResults.remove(index)
        } else {
          throw new RuntimeException("Tried to retract a value that wasn't added first. " +
            "This is probably an incorrectly implemented test. " +
            "Try to set the parallelism of the sink to 1.")
        }
      }
    }
  }

  override def getResults: List[String] = super.getResults
}
