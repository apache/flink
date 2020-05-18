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
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.runtime.utils.TimeTestUtil.TimestampAndWatermarkWithOffset
import org.apache.flink.table.planner.runtime.utils._
import org.apache.flink.types.Row

import org.junit.Assert._
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(classOf[Parameterized])
class TemporalSortITCase(mode: StateBackendMode) extends StreamingWithStateTestBase(mode) {

  @Test
  def testOnlyEventTimeOrderBy(): Unit = {
    val data = List(
      (3L, 2L, "Hello world", 3),
      (2L, 2L, "Hello", 2),
      (6L, 3L, "Luke Skywalker", 6),
      (5L, 3L, "I am fine.", 5),
      (7L, 4L, "Comment#1", 7),
      (9L, 4L, "Comment#3", 9),
      (10L, 4L, "Comment#4", 10),
      (8L, 4L, "Comment#2", 8),
      (1L, 1L, "Hi", 2),
      (1L, 1L, "Hi", 1),
      (4L, 3L, "Helloworld, how are you?", 4))

    val t = failingDataSource(data)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset[(Long, Long, String, Int)](10L))
      .toTable(tEnv, 'rowtime.rowtime, 'key, 'str, 'int)
    tEnv.registerTable("T", t)

    val sqlQuery = "SELECT key, str, `int` FROM T ORDER BY rowtime"

    val sink = new TestingRetractSink
    val results = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    results.addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "1,Hi,2",
      "1,Hi,1",
      "2,Hello,2",
      "2,Hello world,3",
      "3,Helloworld, how are you?,4",
      "3,I am fine.,5",
      "3,Luke Skywalker,6",
      "4,Comment#1,7",
      "4,Comment#2,8",
      "4,Comment#3,9",
      "4,Comment#4,10")

    assertEquals(expected, sink.getRetractResults)
  }

  @Test
  def testEventTimeAndOtherFieldOrderBy(): Unit = {
    val data = List(
      (3L, 2L, "Hello world", 3),
      (2L, 2L, "Hello", 2),
      (6L, 3L, "Luke Skywalker", 6),
      (5L, 3L, "I am fine.", 5),
      (7L, 4L, "Comment#1", 7),
      (9L, 4L, "Comment#3", 9),
      (10L, 4L, "Comment#4", 10),
      (8L, 4L, "Comment#2", 8),
      (1L, 1L, "Hi", 2),
      (1L, 1L, "Hi", 1),
      (4L, 3L, "Helloworld, how are you?", 4))

    val t = failingDataSource(data)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset[(Long, Long, String, Int)](10L))
      .toTable(tEnv, 'rowtime.rowtime, 'key, 'str, 'int)
    tEnv.registerTable("T", t)

    val sqlQuery = "SELECT key, str, `int` FROM T ORDER BY rowtime, `int`"

    val sink = new TestingRetractSink
    val results = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    results.addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "1,Hi,1",
      "1,Hi,2",
      "2,Hello,2",
      "2,Hello world,3",
      "3,Helloworld, how are you?,4",
      "3,I am fine.,5",
      "3,Luke Skywalker,6",
      "4,Comment#1,7",
      "4,Comment#2,8",
      "4,Comment#3,9",
      "4,Comment#4,10")

    assertEquals(expected, sink.getRetractResults)
  }

  @Test
  def testProcTimeOrderBy(): Unit = {
    val t = failingDataSource(TestData.tupleData3)
      .toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)
    tEnv.registerTable("T", t)

    val sql = "SELECT a, b, c FROM T ORDER BY proctime"

    val sink = new TestingRetractSink
    val results = tEnv.sqlQuery(sql).toRetractStream[Row]
    results.addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "1,1,Hi",
      "2,2,Hello",
      "3,2,Hello world",
      "4,3,Hello world, how are you?",
      "5,3,I am fine.",
      "6,3,Luke Skywalker",
      "7,4,Comment#1",
      "8,4,Comment#2",
      "9,4,Comment#3",
      "10,4,Comment#4",
      "11,5,Comment#5",
      "12,5,Comment#6",
      "13,5,Comment#7",
      "14,5,Comment#8",
      "15,5,Comment#9",
      "16,6,Comment#10",
      "17,6,Comment#11",
      "18,6,Comment#12",
      "19,6,Comment#13",
      "20,6,Comment#14",
      "21,6,Comment#15")

     assertEquals(expected, sink.getRetractResults)
  }

}
