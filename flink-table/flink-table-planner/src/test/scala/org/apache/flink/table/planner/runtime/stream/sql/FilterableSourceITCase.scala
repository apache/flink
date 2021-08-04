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

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.table.api.bridge.scala.tableConversions
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.{StreamingTestBase, TestData, TestingAppendSink}
import org.apache.flink.table.utils.LegacyRowResource
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.{Rule, Test}

import java.time.LocalDateTime

/**
 * Tests for pushing filters into a table scan
 */
class FilterableSourceITCase extends StreamingTestBase {

  @Rule
  def usesLegacyRows: LegacyRowResource = LegacyRowResource.INSTANCE

  @Test
  def testFilterPushdown(): Unit = {
    val data = Seq(
      row(1, 2L, LocalDateTime.parse("2020-11-21T19:00:05.23")),
      row(2, 3L, LocalDateTime.parse("2020-11-21T21:00:05.23"))
    )

    val dataId = TestValuesTableFactory.registerData(data)
    val ddl =
      s"""
         | CREATE TABLE MyTable(
         |   a INT,
         |   b BIGINT,
         |   c TIMESTAMP(3),
         |   WATERMARK FOR c AS c
         | ) WITH (
         |   'connector' = 'values',
         |   'enable-watermark-push-down' = 'true',
         |   'filterable-fields' = 'a;c;d',
         |   'bounded' = 'false',
         |   'disable-lookup' = 'true',
         |   'data-id' = '$dataId'
         | )
         |""".stripMargin

    tEnv.executeSql(ddl)

    val query = "SELECT * FROM MyTable WHERE a > 1"
    val expectedData = Seq("2,3,2020-11-21T21:00:05.230")

    val result = tEnv.sqlQuery(query).toAppendStream[Row]
    val sink = new TestingAppendSink()
    result.addSink(sink)

    env.execute()
    assertEquals(expectedData.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testWithRejectedFilter(): Unit = {
    val data = Seq(
      row(1, 2L, LocalDateTime.parse("2020-11-21T19:00:05.23")),
      row(2, 3L, LocalDateTime.parse("2020-11-21T21:00:05.23"))
    )

    val dataId = TestValuesTableFactory.registerData(data)

    // Reject the filter by leaving out 'a' from 'filterable-fields'
    val ddl =
      s"""
         | CREATE TABLE MyTable(
         |   a INT,
         |   b BIGINT,
         |   c TIMESTAMP(3),
         |   WATERMARK FOR c AS c
         | ) WITH (
         |   'connector' = 'values',
         |   'enable-watermark-push-down' = 'true',
         |   'filterable-fields' = 'c;d',
         |   'bounded' = 'false',
         |   'disable-lookup' = 'true',
         |   'data-id' = '$dataId'
         | )
         |""".stripMargin

    tEnv.executeSql(ddl)

    val query = "SELECT * FROM MyTable WHERE a > 1"
    val expectedData = Seq("2,3,2020-11-21T21:00:05.230")

    val result = tEnv.sqlQuery(query).toAppendStream[Row]
    val sink = new TestingAppendSink()
    result.addSink(sink)

    env.execute()
    assertEquals(expectedData.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testProjectWithWatermarkFilterPushdown(): Unit = {
    val data = Seq(
      row(1, 2L, "Hello", LocalDateTime.parse("2020-11-21T19:00:05.23")),
      row(2, 3L, "World", LocalDateTime.parse("2020-11-21T21:00:05.23"))
    )

    val dataId = TestValuesTableFactory.registerData(data)
    val ddl =
      s"""
         |CREATE TABLE TableWithWatermark (
         |  a int,
         |  b bigint,
         |  c string,
         |  d timestamp(3),
         |  WATERMARK FOR d as d
         |) WITH (
         |  'connector' = 'values',
         |  'filterable-fields' = 'c',
         |  'enable-watermark-push-down' = 'true',
         |  'data-id' = '$dataId',
         |  'bounded' = 'false',
         |  'disable-lookup' = 'true'
         |)
       """.stripMargin
    tEnv.executeSql(ddl)

    val result =
      tEnv.sqlQuery   (
        "select a,b from TableWithWatermark WHERE LOWER(c) = 'world'"
      ).toAppendStream[Row]

    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("2,3")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }
}
