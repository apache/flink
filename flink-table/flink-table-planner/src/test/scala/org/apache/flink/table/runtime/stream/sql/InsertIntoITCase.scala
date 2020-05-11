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

package org.apache.flink.table.runtime.stream.sql

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.runtime.stream.table.{RowCollector, TestRetractSink, TestUpsertSink}
import org.apache.flink.table.runtime.utils.{StreamTestData, StreamingWithStateTestBase}
import org.apache.flink.table.utils.MemoryTableSourceSinkUtil
import org.apache.flink.test.util.TestBaseUtils

import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._

class InsertIntoITCase extends StreamingWithStateTestBase {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.getConfig.enableObjectReuse()
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().build()
  val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

  @Test
  def testInsertIntoAppendStreamToTableSink(): Unit = {
    MemoryTableSourceSinkUtil.clear()

    val input = StreamTestData.get3TupleDataStream(env)
      .assignAscendingTimestamps(r => r._2)

    tEnv.createTemporaryView("sourceTable", input, 'a, 'b, 'c, 't.rowtime)

    val fieldNames = Array("d", "e", "t")
    val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING, Types.SQL_TIMESTAMP, Types.LONG)
    val sink = new MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink

    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "targetTable", sink.configure(fieldNames, fieldTypes))

    tEnv.sqlUpdate(
      s"""INSERT INTO targetTable
         |SELECT c, t, b
         |FROM sourceTable
         |WHERE a < 3 OR a > 19
       """.stripMargin)

    tEnv.execute("job name")

    val expected = Seq(
      "Hi,1970-01-01 00:00:00.001,1",
      "Hello,1970-01-01 00:00:00.002,2",
      "Comment#14,1970-01-01 00:00:00.006,6",
      "Comment#15,1970-01-01 00:00:00.006,6").mkString("\n")

    TestBaseUtils.compareResultAsText(MemoryTableSourceSinkUtil.tableData.asJava, expected)
  }

  @Test
  def testInsertIntoUpdatingTableToRetractSink(): Unit = {
    val t = StreamTestData.get3TupleDataStream(env)
      .assignAscendingTimestamps(_._1.toLong)

    tEnv.createTemporaryView("sourceTable", t, 'id, 'num, 'text)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "targetTable",
      new TestRetractSink().configure(
        Array("len", "cntid", "sumnum"),
        Array(Types.INT, Types.LONG, Types.LONG)))

    tEnv.sqlUpdate(
      s"""INSERT INTO targetTable
         |SELECT len, COUNT(id) AS cntid, SUM(num) AS sumnum
         |FROM (SELECT id, num, CHAR_LENGTH(text) AS len FROM sourceTable)
         |GROUP BY len
       """.stripMargin)

     tEnv.execute("job name")
    val results = RowCollector.getAndClearValues

    val retracted = RowCollector.retractResults(results).sorted
    val expected = List(
      "2,1,1",
      "5,1,2",
      "11,1,2",
      "25,1,3",
      "10,7,39",
      "14,1,3",
      "9,9,41").sorted
    assertEquals(expected, retracted)

  }

  @Test
  def testInsertIntoAppendTableToRetractSink(): Unit = {
    val t = StreamTestData.get3TupleDataStream(env)
      .assignAscendingTimestamps(_._1.toLong)

    tEnv.createTemporaryView("sourceTable", t, 'id, 'num, 'text, 'rowtime.rowtime)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "targetTable",
      new TestRetractSink().configure(
        Array("wend", "cntid", "sumnum"),
        Array(Types.SQL_TIMESTAMP, Types.LONG, Types.LONG)
      )
    )

    tEnv.sqlUpdate(
      s"""INSERT INTO targetTable
         |SELECT
         |  TUMBLE_END(rowtime, INTERVAL '0.005' SECOND) AS wend,
         |  COUNT(id) AS cntid,
         |  SUM(num) AS sumnum
         |FROM sourceTable
         |GROUP BY TUMBLE(rowtime, INTERVAL '0.005' SECOND)
       """.stripMargin)

     tEnv.execute("job name")
    val results = RowCollector.getAndClearValues

    assertFalse(
      "Received retraction messages for append only table",
      results.exists(!_.f0))

    val retracted = RowCollector.retractResults(results).sorted
    val expected = List(
      "1970-01-01 00:00:00.005,4,8",
      "1970-01-01 00:00:00.01,5,18",
      "1970-01-01 00:00:00.015,5,24",
      "1970-01-01 00:00:00.02,5,29",
      "1970-01-01 00:00:00.025,2,12")
      .sorted
    assertEquals(expected, retracted)

  }

  @Test
  def testInsertIntoUpdatingTableWithFullKeyToUpsertSink(): Unit = {
    val t = StreamTestData.get3TupleDataStream(env)
      .assignAscendingTimestamps(_._1.toLong)

    tEnv.createTemporaryView("sourceTable", t, 'id, 'num, 'text)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "targetTable",
      new TestUpsertSink(Array("cnt", "cTrue"), false).configure(
        Array("cnt", "cntid", "cTrue"),
        Array(Types.LONG, Types.LONG, Types.BOOLEAN)
      )
    )

    tEnv.sqlUpdate(
      s"""INSERT INTO targetTable
         |SELECT cnt, COUNT(len) AS cntid, cTrue
         |FROM
         |  (SELECT CHAR_LENGTH(text) AS len, (id > 0) AS cTrue, COUNT(id) AS cnt
         |   FROM sourceTable
         |   GROUP BY CHAR_LENGTH(text), (id > 0)
         |   )
         |GROUP BY cnt, cTrue
       """.stripMargin)

     tEnv.execute("job name")
    val results = RowCollector.getAndClearValues

    assertTrue(
      "Results must include delete messages",
      results.exists(_.f0 == false)
    )

    val retracted = RowCollector.upsertResults(results, Array(0, 2)).sorted
    val expected = List(
      "1,5,true",
      "7,1,true",
      "9,1,true").sorted
    assertEquals(expected, retracted)

  }

  @Test
  def testInsertIntoAppendingTableWithFullKey1ToUpsertSink(): Unit = {
    val t = StreamTestData.get3TupleDataStream(env)
      .assignAscendingTimestamps(_._1.toLong)

    tEnv.createTemporaryView("sourceTable", t, 'id, 'num, 'text, 'rowtime.rowtime)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "targetTable",
      new TestUpsertSink(Array("wend", "num"), true).configure(
        Array("num", "wend", "cntid"),
        Array(Types.LONG, Types.SQL_TIMESTAMP, Types.LONG)
      )
    )

    tEnv.sqlUpdate(
      s"""INSERT INTO targetTable
         |SELECT
         |  num,
         |  TUMBLE_END(rowtime, INTERVAL '0.005' SECOND) AS wend,
         |  COUNT(id) AS cntid
         |FROM sourceTable
         |GROUP BY TUMBLE(rowtime, INTERVAL '0.005' SECOND), num
       """.stripMargin)

     tEnv.execute("job name")
    val results = RowCollector.getAndClearValues

    assertFalse(
      "Received retraction messages for append only table",
      results.exists(!_.f0))

    val retracted = RowCollector.upsertResults(results, Array(0, 1)).sorted
    val expected = List(
      "1,1970-01-01 00:00:00.005,1",
      "2,1970-01-01 00:00:00.005,2",
      "3,1970-01-01 00:00:00.005,1",
      "3,1970-01-01 00:00:00.01,2",
      "4,1970-01-01 00:00:00.01,3",
      "4,1970-01-01 00:00:00.015,1",
      "5,1970-01-01 00:00:00.015,4",
      "5,1970-01-01 00:00:00.02,1",
      "6,1970-01-01 00:00:00.02,4",
      "6,1970-01-01 00:00:00.025,2").sorted
    assertEquals(expected, retracted)
  }

  @Test
  def testInsertIntoAppendingTableWithFullKey2ToUpsertSink(): Unit = {
    val t = StreamTestData.get3TupleDataStream(env)
      .assignAscendingTimestamps(_._1.toLong)

    tEnv.createTemporaryView("sourceTable", t, 'id, 'num, 'text, 'rowtime.rowtime)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "targetTable",
      new TestUpsertSink(Array("wstart", "wend", "num"), true).configure(
        Array("wstart", "wend", "num", "cntid"),
        Array(Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP, Types.LONG, Types.LONG)
      )
    )

    tEnv.sqlUpdate(
      s"""INSERT INTO targetTable
         |SELECT
         |  TUMBLE_START(rowtime, INTERVAL '0.005' SECOND) AS wstart,
         |  TUMBLE_END(rowtime, INTERVAL '0.005' SECOND) AS wend,
         |  num,
         |  COUNT(id) AS cntid
         |FROM sourceTable
         |GROUP BY TUMBLE(rowtime, INTERVAL '0.005' SECOND), num
       """.stripMargin)

     tEnv.execute("job name")
    val results = RowCollector.getAndClearValues

    assertFalse(
      "Received retraction messages for append only table",
      results.exists(!_.f0))

    val retracted = RowCollector.upsertResults(results, Array(0, 1, 2)).sorted
    val expected = List(
      "1970-01-01 00:00:00.0,1970-01-01 00:00:00.005,1,1",
      "1970-01-01 00:00:00.0,1970-01-01 00:00:00.005,2,2",
      "1970-01-01 00:00:00.0,1970-01-01 00:00:00.005,3,1",
      "1970-01-01 00:00:00.005,1970-01-01 00:00:00.01,3,2",
      "1970-01-01 00:00:00.005,1970-01-01 00:00:00.01,4,3",
      "1970-01-01 00:00:00.01,1970-01-01 00:00:00.015,4,1",
      "1970-01-01 00:00:00.01,1970-01-01 00:00:00.015,5,4",
      "1970-01-01 00:00:00.015,1970-01-01 00:00:00.02,5,1",
      "1970-01-01 00:00:00.015,1970-01-01 00:00:00.02,6,4",
      "1970-01-01 00:00:00.02,1970-01-01 00:00:00.025,6,2").sorted
    assertEquals(expected, retracted)
  }

  @Test
  def testInsertIntoAppendingTableWithoutFullKey1ToUpsertSink(): Unit = {
    val t = StreamTestData.get3TupleDataStream(env)
      .assignAscendingTimestamps(_._1.toLong)

    tEnv.createTemporaryView("sourceTable", t, 'id, 'num, 'text, 'rowtime.rowtime)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "targetTable",
      new TestUpsertSink(null, true).configure(
        Array("wend", "cntid"),
        Array(Types.SQL_TIMESTAMP, Types.LONG)
      )
    )

    tEnv.sqlUpdate(
      s"""INSERT INTO targetTable
         |SELECT
         |  TUMBLE_END(rowtime, INTERVAL '0.005' SECOND) AS wend,
         |  COUNT(id) AS cntid
         |FROM sourceTable
         |GROUP BY TUMBLE(rowtime, INTERVAL '0.005' SECOND), num
       """.stripMargin)

     tEnv.execute("job name")
    val results = RowCollector.getAndClearValues

    assertFalse(
      "Received retraction messages for append only table",
      results.exists(!_.f0))

    val retracted = results.map(_.f1.toString).sorted
    val expected = List(
      "1970-01-01 00:00:00.005,1",
      "1970-01-01 00:00:00.005,2",
      "1970-01-01 00:00:00.005,1",
      "1970-01-01 00:00:00.01,2",
      "1970-01-01 00:00:00.01,3",
      "1970-01-01 00:00:00.015,1",
      "1970-01-01 00:00:00.015,4",
      "1970-01-01 00:00:00.02,1",
      "1970-01-01 00:00:00.02,4",
      "1970-01-01 00:00:00.025,2").sorted
    assertEquals(expected, retracted)
  }

  @Test
  def testInsertIntoAppendingTableWithoutFullKey2ToUpsertSink(): Unit = {
    val t = StreamTestData.get3TupleDataStream(env)
      .assignAscendingTimestamps(_._1.toLong)

    tEnv.createTemporaryView("sourceTable", t, 'id, 'num, 'text, 'rowtime.rowtime)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "targetTable",
      new TestUpsertSink(null, true).configure(
        Array("num", "cntid"),
        Array(Types.LONG, Types.LONG)
      )
    )

    tEnv.sqlUpdate(
      s"""INSERT INTO targetTable
         |SELECT
         |  num,
         |  COUNT(id) AS cntid
         |FROM sourceTable
         |GROUP BY TUMBLE(rowtime, INTERVAL '0.005' SECOND), num
       """.stripMargin)

     tEnv.execute("job name")
    val results = RowCollector.getAndClearValues

    assertFalse(
      "Received retraction messages for append only table",
      results.exists(!_.f0))

    val retracted = results.map(_.f1.toString).sorted
    val expected = List(
      "1,1",
      "2,2",
      "3,1",
      "3,2",
      "4,3",
      "4,1",
      "5,4",
      "5,1",
      "6,4",
      "6,2").sorted
    assertEquals(expected, retracted)
  }
}
