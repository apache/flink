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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.{StreamingTestBase, TestingAppendSink}
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.TimeTestUtil.EventTimeSourceFunction
import org.apache.flink.table.planner.utils.TestTableSourceSinks.createWithoutTimeAttributesTableSource
import org.apache.flink.table.utils.DateTimeUtils.toLocalDateTime
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class TableScanITCase extends StreamingTestBase {

  @Test
  def testTableSourceWithoutTimeAttribute(): Unit = {
    val tableName = "MyTable"
    createWithoutTimeAttributesTableSource(tEnv, tableName)
    val sqlQuery = s"SELECT * from $tableName"
    val result = tEnv.sqlQuery(sqlQuery).toDataStream
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = Seq("Mary,1,1", "Bob,2,3")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @Test
  def testProctimeTableSource(): Unit = {
    val tableName = "MyTable"
    val dataId =
      TestValuesTableFactory.registerData(
        Seq(
          row("Mary"),
          row("Peter"),
          row("Bob"),
          row("Liz")
        ))
    tEnv.executeSql(s"""
                       |create table $tableName (
                       |  name string,
                       |  ptime as proctime()
                       |) with (
                       |  'connector' = 'values',
                       |  'data-id' = '$dataId'
                       |)
                       |""".stripMargin)

    val sqlQuery = s"SELECT name FROM $tableName"
    val result = tEnv.sqlQuery(sqlQuery).toDataStream
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = Seq("Mary", "Peter", "Bob", "Liz")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @Test
  def testRowtimeTableSource(): Unit = {
    val tableName = "MyTable"
    val dataId =
      TestValuesTableFactory.registerData(
        Seq(
          row(Int.box(1), toLocalDateTime(11), "Mary"),
          row(Int.box(2), toLocalDateTime(12), "Peter"),
          row(Int.box(3), toLocalDateTime(13), "Bob"),
          row(Int.box(4), toLocalDateTime(14), "Liz")
        ))
    tEnv.executeSql(s"""
                       |create table $tableName (
                       |  key int,
                       |  rowtime timestamp(3),
                       |  payload string,
                       |  watermark for rowtime as rowtime
                       |) with (
                       |  'connector' = 'values',
                       |  'bounded' = 'true',
                       |  'data-id' = '$dataId'
                       |)
                       |""".stripMargin)

    val sqlQuery =
      s"""
         |SELECT
         |  CAST(TUMBLE_START(rowtime, INTERVAL '0.005' SECOND) AS VARCHAR),
         |  COUNT(payload)
         |FROM $tableName
         |GROUP BY TUMBLE(rowtime, INTERVAL '0.005' SECOND)
       """.stripMargin
    val result = tEnv.sqlQuery(sqlQuery).toDataStream
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = Seq("1970-01-01 00:00:00.010,4")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @Test
  def testRowtimeTableSourcePreserveWatermarks(): Unit = {
    val tableName = "MyTable"

    // rows with timestamps and watermarks
    val data = Seq(
      Right(1L),
      Left(5L, (1, 5L, "A")),
      Left(2L, (2, 1L, "B")),
      Right(10L),
      Left(8L, (6, 8L, "C")),
      Right(20L),
      Left(21L, (6, 21L, "D")),
      Right(30L)
    )

    val t = env
      .addSource(new EventTimeSourceFunction[(Int, Long, String)](data))
      .returns(implicitly[TypeInformation[(Int, Long, String)]])
      .setMaxParallelism(1)
      .setMaxParallelism(1)
      .toTable(tEnv, 'id, 'rtime.rowtime, 'name)
    tEnv.createTemporaryView(tableName, t)

    val sqlQuery = s"SELECT id, name FROM $tableName"
    val sink = new TestingAppendSink

    tEnv
      .sqlQuery(sqlQuery)
      .toDataStream
      // append current watermark to each row to verify that original watermarks were preserved
      .process(new ProcessFunction[Row, Row] {

        override def processElement(
            value: Row,
            ctx: ProcessFunction[Row, Row]#Context,
            out: Collector[Row]): Unit = {
          val res = new Row(3)
          res.setField(0, value.getField(0))
          res.setField(1, value.getField(1))
          res.setField(2, ctx.timerService().currentWatermark())
          out.collect(res)
        }
      })
      .addSink(sink)
    env.execute()

    val expected = Seq("1,A,1", "2,B,1", "6,C,10", "6,D,20")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

}
