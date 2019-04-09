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

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.StreamingWithMiniBatchTestBase.MiniBatchMode
import org.apache.flink.table.runtime.utils._
import org.apache.flink.table.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.runtime.utils.TimeTestUtil.TimestampAndWatermarkWithOffset
import org.apache.flink.types.Row

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(classOf[Parameterized])
class DeduplicateITCase(miniBatch: MiniBatchMode, mode: StateBackendMode)
  extends StreamingWithMiniBatchTestBase(miniBatch, mode) {

  @Test
  def testFirstRowOnProctime(): Unit = {
    val t = failingDataSource(StreamTestData.get3TupleData)
      .toTable(tEnv, 'a, 'b, 'c, 'proctime)
    tEnv.registerTable("T", t)

    val sql =
      """
        |SELECT a, b, c
        |FROM (
        |  SELECT *,
        |    ROW_NUMBER() OVER (PARTITION BY b ORDER BY proctime) as rowNum
        |  FROM T
        |)
        |WHERE rowNum = 1
      """.stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("1,1,Hi", "2,2,Hello", "4,3,Hello world, how are you?",
      "7,4,Comment#1", "11,5,Comment#5", "16,6,Comment#10")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLastRowOnProctime(): Unit = {
    val t = failingDataSource(StreamTestData.get3TupleData)
      .toTable(tEnv, 'a, 'b, 'c, 'proctime)
    tEnv.registerTable("T", t)

    val sql =
      """
        |SELECT a, b, c
        |FROM (
        |  SELECT *,
        |    ROW_NUMBER() OVER (PARTITION BY b ORDER BY proctime DESC) as rowNum
        |  FROM T
        |)
        |WHERE rowNum = 1
      """.stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("1,1,Hi", "3,2,Hello world", "6,3,Luke Skywalker",
      "10,4,Comment#4", "15,5,Comment#9", "21,6,Comment#15")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  // TODO Deduplicate does not support sort on rowtime now, so it is translated to Rank currently
  @Test
  def testFirstRowOnRowtime(): Unit = {
    val data = List(
      (3L, 2L, "Hello world", 3),
      (2L, 2L, "Hello", 2),
      (6L, 3L, "Luke Skywalker", 6),
      (5L, 3L, "I am fine.", 5),
      (7L, 4L, "Comment#1", 7),
      (9L, 4L, "Comment#3", 9),
      (10L, 4L, "Comment#4", 10),
      (8L, 4L, "Comment#2", 8),
      (1L, 1L, "Hi", 1),
      (4L, 3L, "Helloworld, how are you?", 4))

    val t = failingDataSource(data)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset[(Long, Long, String, Int)](10L))
      .toTable(tEnv, 'rowtime, 'key, 'str, 'int)
    tEnv.registerTable("T", t)

    val sql =
      """
        |SELECT key, str, `int`
        |FROM (
        |  SELECT *,
        |    ROW_NUMBER() OVER (PARTITION BY key ORDER BY rowtime) as rowNum
        |  FROM T
        |)
        |WHERE rowNum = 1
      """.stripMargin

    val sink = new TestingUpsertTableSink(Array(1))
    val table = tEnv.sqlQuery(sql)
    writeToSink(table, sink)

    env.execute()
    val expected = List("1,Hi,1", "2,Hello,2", "3,Helloworld, how are you?,4", "4,Comment#1,7")
    assertEquals(expected.sorted, sink.getUpsertResults.sorted)
  }

  // TODO Deduplicate does not support sort on rowtime now, so it is translated to Rank currently
  @Test
  def testLastRowOnRowtime(): Unit = {
    val data = List(
      (3L, 2L, "Hello world", 3),
      (2L, 2L, "Hello", 2),
      (6L, 3L, "Luke Skywalker", 6),
      (5L, 3L, "I am fine.", 5),
      (7L, 4L, "Comment#1", 7),
      (9L, 4L, "Comment#3", 9),
      (10L, 4L, "Comment#4", 10),
      (8L, 4L, "Comment#2", 8),
      (1L, 1L, "Hi", 1),
      (4L, 3L, "Helloworld, how are you?", 4))

    val t = failingDataSource(data)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset[(Long, Long, String, Int)](10L))
      .toTable(tEnv, 'rowtime, 'key, 'str, 'int)
    tEnv.registerTable("T", t)

    val sql =
      """
        |SELECT key, str, `int`
        |FROM (
        |  SELECT *,
        |    ROW_NUMBER() OVER (PARTITION BY key ORDER BY rowtime DESC) as rowNum
        |  FROM T
        |)
        |WHERE rowNum = 1
      """.stripMargin

    val sink = new TestingUpsertTableSink(Array(1))
    val table = tEnv.sqlQuery(sql)
    writeToSink(table, sink)

    env.execute()
    val expected = List("1,Hi,1", "2,Hello world,3", "3,Luke Skywalker,6", "4,Comment#4,10")
    assertEquals(expected.sorted, sink.getUpsertResults.sorted)
  }

}
