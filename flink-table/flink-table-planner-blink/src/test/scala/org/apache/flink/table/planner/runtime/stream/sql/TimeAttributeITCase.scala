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
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.JavaFunc5
import org.apache.flink.table.planner.runtime.utils.{StreamingTestBase, TestingAppendSink}
import org.apache.flink.types.Row
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

import java.sql.Timestamp
import java.util.TimeZone

import scala.collection.JavaConverters._

/**
  * Integration tests for time attributes defined in DDL.
  */
class TimeAttributeITCase extends StreamingTestBase {

  val data = List(
    row(utcTimestamp(1L), 1, 1d),
    row(utcTimestamp(2L), 1, 2d),
    row(utcTimestamp(3L), 1, 2d),
    row(utcTimestamp(4L), 1, 5d),
    row(utcTimestamp(7L), 1, 3d),
    row(utcTimestamp(8L), 1, 3d),
    row(utcTimestamp(16L), 1, 4d))
  TestCollectionTableFactory.reset()
  TestCollectionTableFactory.initData(data.asJava)
  TestCollectionTableFactory.isStreaming = true

  @Test
  def testWindowAggregateOnWatermark(): Unit = {
    val ddl =
      """
        |CREATE TABLE src (
        |  ts TIMESTAMP(3),
        |  a INT,
        |  b DOUBLE,
        |  WATERMARK FOR ts AS ts - INTERVAL '0.001' SECOND
        |) WITH (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val query =
      """
        |SELECT TUMBLE_END(ts, INTERVAL '0.003' SECOND), COUNT(ts), SUM(b)
        |FROM src
        |GROUP BY TUMBLE(ts, INTERVAL '0.003' SECOND)
      """.stripMargin
    tEnv.sqlUpdate(ddl)
    val sink = new TestingAppendSink()
    tEnv.sqlQuery(query).toAppendStream[Row].addSink(sink)
    tEnv.execute("SQL JOB")

    val expected = Seq(
      "1970-01-01T00:00:00.003,2,3.0",
      "1970-01-01T00:00:00.006,2,7.0",
      "1970-01-01T00:00:00.009,2,6.0",
      "1970-01-01T00:00:00.018,1,4.0")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testWindowAggregateOnCustomizedWatermark(): Unit = {
    JavaFunc5.openCalled = false
    JavaFunc5.closeCalled = false
    tEnv.registerFunction("myFunc", new JavaFunc5)
    val ddl =
      """
        |CREATE TABLE src (
        |  ts TIMESTAMP(3),
        |  a INT,
        |  b DOUBLE,
        |  WATERMARK FOR ts AS myFunc(ts, a)
        |) WITH (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val query =
      """
        |SELECT TUMBLE_END(ts, INTERVAL '0.003' SECOND), COUNT(ts), SUM(b)
        |FROM src
        |GROUP BY TUMBLE(ts, INTERVAL '0.003' SECOND)
      """.stripMargin
    tEnv.sqlUpdate(ddl)
    val sink = new TestingAppendSink()
    tEnv.sqlQuery(query).toAppendStream[Row].addSink(sink)
    tEnv.execute("SQL JOB")

    val expected = Seq(
      "1970-01-01T00:00:00.003,2,3.0",
      "1970-01-01T00:00:00.006,2,7.0",
      "1970-01-01T00:00:00.009,2,6.0",
      "1970-01-01T00:00:00.018,1,4.0")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertTrue(JavaFunc5.openCalled)
    assertTrue(JavaFunc5.closeCalled)
  }

  @Test
  def testWindowAggregateOnNestedRowtime(): Unit = {
    val ddl =
      """
        |CREATE TABLE src (
        |  col ROW<
        |    ts TIMESTAMP(3),
        |    a INT,
        |    b DOUBLE>,
        |  WATERMARK FOR col.ts AS col.ts - INTERVAL '0.001' SECOND
        |) WITH (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val query =
      """
        |SELECT TUMBLE_END(col.ts, INTERVAL '0.003' SECOND), COUNT(*)
        |FROM src
        |GROUP BY TUMBLE(col.ts, INTERVAL '0.003' SECOND)
      """.stripMargin
    tEnv.sqlUpdate(ddl)
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage(
      "Nested field 'col.ts' as rowtime attribute is not supported right now.")
    tEnv.sqlQuery(query)
  }

  // ------------------------------------------------------------------------------------------

  private def utcTimestamp(ts: Long): Timestamp = {
    new Timestamp(ts - TimeZone.getDefault.getOffset(ts))
  }

  private def row(args: Any*):Row = {
    val row = new Row(args.length)
    0 until args.length foreach {
      i => row.setField(i, args(i))
    }
    row
  }

}
