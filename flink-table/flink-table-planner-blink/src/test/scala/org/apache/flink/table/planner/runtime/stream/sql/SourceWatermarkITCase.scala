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

import org.apache.flink.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.data.TimestampData
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.JavaFunc5
import org.apache.flink.table.planner.runtime.utils.{StreamingTestBase, TestingAppendSink}
import org.apache.flink.table.utils.LegacyRowResource
import org.apache.flink.types.Row

import org.junit.Assert.assertEquals
import org.junit.{Rule, Test}

import java.sql.Timestamp
import java.time.LocalDateTime

import scala.collection.JavaConverters._

class SourceWatermarkITCase extends StreamingTestBase {

  @Rule
  def usesLegacyRows: LegacyRowResource = LegacyRowResource.INSTANCE

  @Test
  def testSimpleWatermarkPushDown(): Unit = {
    val data = Seq(
      row(1, 2L, LocalDateTime.parse("2020-11-21T19:00:05.23")),
      row(2, 3L, LocalDateTime.parse("2020-11-21T21:00:05.23"))
    )

    val dataId = TestValuesTableFactory.registerData(data)

    val ddl =
      s"""
         | CREATE Table VirtualTable (
         |   a INT,
         |   b BIGINT,
         |   c TIMESTAMP(3),
         |   d as c - INTERVAL '5' second,
         |   WATERMARK FOR d as d + INTERVAL '5' second
         | ) with (
         |   'connector' = 'values',
         |   'bounded' = 'false',
         |   'enable-watermark-push-down' = 'true',
         |   'disable-lookup' = 'true',
         |   'data-id' = '$dataId'
         | )
         |""".stripMargin

    tEnv.executeSql(ddl)

    val expectedWatermarkOutput = Seq(
      "2020-11-21T19:00:05.230",
      "2020-11-21T21:00:05.230")
    val expectedData = Seq(
      "1,2,2020-11-21T19:00:05.230",
      "2,3,2020-11-21T21:00:05.230"
    )

    val query = "SELECT a, b, c FROM VirtualTable"
    val result = tEnv.sqlQuery(query).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val actualWatermark = TestValuesTableFactory.getWatermarkOutput("VirtualTable")
      .asScala
      .map(x => TimestampData.fromEpochMillis(x.getTimestamp).toLocalDateTime.toString)
      .toList

    assertEquals(expectedWatermarkOutput, actualWatermark)
    assertEquals(expectedData.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testWatermarkWithNestedRow(): Unit = {
    val data = Seq(
      row(0, 0L, row("h1", row("h2", null))),
      row(1, 2L, row("i1", row("i2", LocalDateTime.parse("2020-11-21T19:00:05.23")))),
      row(2, 3L, row("j1", row("j2", LocalDateTime.parse("2020-11-21T21:00:05.23")))),
      row(3, 4L, row("k1", row("k2", null)))
    )

    val dataId = TestValuesTableFactory.registerData(data)

    val ddl =
      s"""
         | CREATE Table NestedTable (
         |   a INT,
         |   b BIGINT,
         |   c ROW<name STRING, d ROW<e STRING, f TIMESTAMP(3)>>,
         |   g as c.d.f,
         |   WATERMARK FOR g as g - INTERVAL '5' second
         | ) with (
         |   'connector' = 'values',
         |   'bounded' = 'false',
         |   'enable-watermark-push-down' = 'true',
         |   'disable-lookup' = 'true',
         |   'data-id' = '$dataId'
         | )
         |""".stripMargin

    tEnv.executeSql(ddl)

    val expectedWatermarkOutput = Seq(
      TimestampData.fromEpochMillis(Long.MinValue).toString,
      "2020-11-21T19:00:00.230",
      "2020-11-21T21:00:00.230",
      "2020-11-21T21:00:00.230")
    val expectedData = Seq(
      "0,0,h2,null",
      "1,2,i2,2020-11-21T19:00:05.230",
      "2,3,j2,2020-11-21T21:00:05.230",
      "3,4,k2,null"
    )

    val query = "SELECT a, b, c.d FROM NestedTable"
    val result = tEnv.sqlQuery(query).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val actualWatermark = TestValuesTableFactory.getWatermarkOutput("NestedTable")
      .asScala
      .map(x => TimestampData.fromEpochMillis(x.getTimestamp).toLocalDateTime.toString)
      .toList

    assertEquals(expectedWatermarkOutput, actualWatermark)
    assertEquals(expectedData.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testWatermarkWithMultiInputUdf(): Unit = {
    JavaFunc5.closeCalled = false
    JavaFunc5.openCalled = false
    tEnv.createTemporarySystemFunction("func", new JavaFunc5)
    val data = Seq(
      row(1000, 2L, LocalDateTime.parse("2020-11-21T19:00:05.23")),
      row(2000, 3L, LocalDateTime.parse("2020-11-21T21:00:05.23"))
    )

    val dataId = TestValuesTableFactory.registerData(data)

    val ddl =
      s"""
         | CREATE Table UdfTable (
         |   a INT,
         |   b BIGINT,
         |   c timestamp(3),
         |   d as func(c, a),
         |   WATERMARK FOR c as func(func(d, a), a)
         | ) with (
         |   'connector' = 'values',
         |   'bounded' = 'false',
         |   'enable-watermark-push-down' = 'true',
         |   'disable-lookup' = 'true',
         |   'data-id' = '$dataId'
         | )
         |""".stripMargin

    tEnv.executeSql(ddl)

    val expectedWatermarkOutput = Seq(
      "2020-11-21T19:00:02.230", "2020-11-21T20:59:59.230"
    )
    val expectedData = Seq(
      "2000,3,2020-11-21T21:00:03.230"
    )

    val query = "SELECT a, b, d FROM UdfTable WHERE b > 2"
    val result = tEnv.sqlQuery(query).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val actualWatermark = TestValuesTableFactory.getWatermarkOutput("UdfTable")
      .asScala
      .map(x => TimestampData.fromEpochMillis(x.getTimestamp).toLocalDateTime.toString)
      .toList

    assertEquals(expectedWatermarkOutput, actualWatermark)
    assertEquals(expectedData.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testWatermarkWithMetadata(): Unit = {
    val data = Seq(
      row(1, 2L, Timestamp.valueOf("2020-11-21 19:00:05.23").toInstant.toEpochMilli),
      row(1, 3L, Timestamp.valueOf("2020-11-21 21:00:05.23").toInstant.toEpochMilli)
    )

    val dataId = TestValuesTableFactory.registerData(data)

    val ddl =
      s"""
        | CREATE TABLE MetadataTable(
        |   a INT,
        |   b BIGINT,
        |   originTime BIGINT METADATA,
        |   rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(originTime/1000), 'yyyy-MM-dd HH:mm:ss'),
        |   WATERMARK FOR rowtime AS rowtime
        | ) WITH (
        |   'connector' = 'values',
        |   'enable-watermark-push-down' = 'true',
        |   'bounded' = 'false',
        |   'disable-lookup' = 'true',
        |   'readable-metadata' = 'originTime:BIGINT',
        |   'data-id' = '$dataId'
        | )
        |""".stripMargin

    tEnv.executeSql(ddl)

    val expectedWatermarkOutput = List(
      "2020-11-21T19:00:05",
      "2020-11-21T21:00:05"
    )
    val expectedData = Seq("1")

    val query = "SELECT a FROM MetadataTable WHERE b > 2"
    val result = tEnv.sqlQuery(query).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val actualWatermark = TestValuesTableFactory.getWatermarkOutput("MetadataTable")
      .asScala
      .map(x => TimestampData.fromEpochMillis(x.getTimestamp).toLocalDateTime.toString).toList

    assertEquals(expectedWatermarkOutput, actualWatermark)
    assertEquals(expectedData.sorted, sink.getAppendResults.sorted)
  }
}
