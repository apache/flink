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
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.TimeTestUtil.EventTimeSourceFunction
import org.apache.flink.table.runtime.utils.{StreamITCase, StreamingWithStateTestBase}
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.mutable

class CepITCase extends StreamingWithStateTestBase {

  @Test
  def testSimpleCEP() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val data = new mutable.MutableList[(Int, String)]
    data.+=((1, "a"))
    data.+=((2, "z"))
    data.+=((3, "b"))
    data.+=((4, "c"))
    data.+=((5, "d"))
    data.+=((6, "a"))
    data.+=((7, "b"))
    data.+=((8, "c"))
    data.+=((9, "h"))

    val t = env.fromCollection(data).toTable(tEnv).as('id, 'name)
    tEnv.registerTable("MyTable", t)

    val sqlQuery =
      s"""
         |SELECT T.aid, T.bid, T.cid
         |FROM MyTable
         |MATCH_RECOGNIZE (
         |  MEASURES
         |    A.id AS aid,
         |    B.id AS bid,
         |    C.id AS cid
         |  PATTERN (A B C)
         |  DEFINE
         |    A AS A.name = 'a',
         |    B AS B.name = 'b',
         |    C AS C.name = 'c'
         |) AS T
         |""".stripMargin

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList("6,7,8")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testAllRowsPerMatch() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val data = new mutable.MutableList[(Int, String)]
    data.+=((1, "a"))
    data.+=((2, "z"))
    data.+=((3, "b"))
    data.+=((4, "c"))
    data.+=((5, "d"))
    data.+=((6, "a"))
    data.+=((7, "b"))
    data.+=((8, "c"))
    data.+=((9, "h"))

    val t = env.fromCollection(data).toTable(tEnv).as('id, 'name)
    tEnv.registerTable("MyTable", t)

    val sqlQuery =
      s"""
         |SELECT *
         |FROM MyTable
         |MATCH_RECOGNIZE (
         |  MEASURES
         |    A.id AS aid,
         |    B.id AS bid,
         |    C.id AS cid
         |  ALL ROWS PER MATCH
         |  PATTERN (A B C)
         |  DEFINE
         |    A AS A.name = 'a',
         |    B AS B.name = 'b',
         |    C AS C.name = 'c'
         |) AS T
         |""".stripMargin

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList("6,a,6,null,null", "7,b,6,7,null", "8,c,6,7,8")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testFinalFirst() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val data = new mutable.MutableList[(String, Long, Int, Int)]
    data.+=(("ACME", 1L, 12, 1))
    data.+=(("ACME", 2L, 17, 2))
    data.+=(("ACME", 3L, 13, 3))
    data.+=(("ACME", 4L, 15, 4))
    data.+=(("ACME", 5L, 20, 5))
    data.+=(("ACME", 6L, 24, 6))
    data.+=(("ACME", 7L, 25, 7))
    data.+=(("ACME", 8L, 19, 8))

    val t = env.fromCollection(data).toTable(tEnv).as('symbol, 'tstamp, 'price, 'tax)
    tEnv.registerTable("Ticker", t)

    val sqlQuery =
      s"""
         |SELECT *
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  MEASURES
         |    STRT.tstamp AS start_tstamp,
         |    FIRST(DOWN.tstamp) AS bottom_tstamp,
         |    FIRST(UP.tstamp) AS end_tstamp,
         |    FIRST(DOWN.price + DOWN.tax + 1) AS bottom_total,
         |    FIRST(UP.price + UP.tax) AS end_total
         |  ONE ROW PER MATCH
         |  PATTERN (STRT DOWN+ UP+)
         |  DEFINE
         |    DOWN AS DOWN.price < PREV(DOWN.price),
         |    UP AS UP.price > PREV(UP.price)
         |) AS T
         |""".stripMargin

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List("2,3,4,17,19", "2,3,4,17,19", "2,3,4,17,19", "2,3,4,17,19")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testFinalLast() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val data = new mutable.MutableList[(String, Long, Int, Int)]
    data.+=(("ACME", 1L, 12, 1))
    data.+=(("ACME", 2L, 17, 2))
    data.+=(("ACME", 3L, 13, 3))
    data.+=(("ACME", 4L, 15, 4))
    data.+=(("ACME", 5L, 20, 5))
    data.+=(("ACME", 6L, 24, 6))
    data.+=(("ACME", 7L, 25, 7))
    data.+=(("ACME", 8L, 19, 8))

    val t = env.fromCollection(data).toTable(tEnv).as('symbol, 'tstamp, 'price, 'tax)
    tEnv.registerTable("Ticker", t)

    val sqlQuery =
      s"""
         |SELECT *
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  MEASURES
         |    STRT.tstamp AS start_tstamp,
         |    LAST(DOWN.tstamp) AS bottom_tstamp,
         |    LAST(UP.tstamp) AS end_tstamp,
         |    LAST(DOWN.price + DOWN.tax) AS bottom_total,
         |    LAST(UP.price + UP.tax + 1) AS end_total
         |  ONE ROW PER MATCH
         |  PATTERN (STRT DOWN+ UP+)
         |  DEFINE
         |    DOWN AS DOWN.price < PREV(DOWN.price),
         |    UP AS UP.price > PREV(UP.price)
         |) AS T
         |""".stripMargin

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List("2,3,4,16,20", "2,3,5,16,26", "2,3,6,16,31", "2,3,7,16,33")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testPrev() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val data = new mutable.MutableList[(String, Long, Int)]
    data.+=(("ACME", 1L, 12))
    data.+=(("ACME", 2L, 17))
    data.+=(("ACME", 3L, 13))
    data.+=(("ACME", 4L, 11))
    data.+=(("ACME", 5L, 14))
    data.+=(("ACME", 6L, 12))
    data.+=(("ACME", 7L, 13))
    data.+=(("ACME", 8L, 19))

    val t = env.fromCollection(data).toTable(tEnv).as('symbol, 'tstamp, 'price)
    tEnv.registerTable("Ticker", t)

    val sqlQuery =
      s"""
         |SELECT *
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  MEASURES
         |    STRT.tstamp AS start_tstamp,
         |    LAST(DOWN.tstamp) AS up_days,
         |    LAST(UP.tstamp) AS total_days
         |  PATTERN (STRT DOWN+ UP+)
         |  DEFINE
         |    DOWN AS DOWN.price < PREV(DOWN.price),
         |    UP AS UP.price > PREV(UP.price, 2)
         |) AS T
         |""".stripMargin

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List("2,4,5", "2,4,6", "3,4,5", "3,4,6")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testRunningFirst() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val data = new mutable.MutableList[(String, Long, Int, Int)]
    data.+=(("ACME", 1L, 12, 1))
    data.+=(("ACME", 2L, 17, 2))
    data.+=(("ACME", 3L, 13, 4))
    data.+=(("ACME", 4L, 11, 3))
    data.+=(("ACME", 5L, 20, 5))
    data.+=(("ACME", 6L, 24, 4))
    data.+=(("ACME", 7L, 25, 3))
    data.+=(("ACME", 8L, 19, 8))

    val t = env.fromCollection(data).toTable(tEnv).as('symbol, 'tstamp, 'price, 'tax)
    tEnv.registerTable("Ticker", t)

    val sqlQuery =
      s"""
         |SELECT *
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  MEASURES
         |    STRT.tstamp AS start_tstamp,
         |    LAST(DOWN.tstamp) AS bottom_tstamp,
         |    LAST(UP.tstamp) AS end_tstamp
         |  ONE ROW PER MATCH
         |  PATTERN (STRT DOWN+ UP+)
         |  DEFINE
         |    DOWN AS DOWN.price < PREV(DOWN.price),
         |    UP AS UP.price > PREV(UP.price) AND UP.tax > FIRST(DOWN.tax)
         |) AS T
         |""".stripMargin

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List("2,4,5", "3,4,5", "3,4,6")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testRunningLast() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val data = new mutable.MutableList[(String, Long, Int, Int)]
    data.+=(("ACME", 1L, 12, 1))
    data.+=(("ACME", 2L, 17, 2))
    data.+=(("ACME", 3L, 13, 4))
    data.+=(("ACME", 4L, 11, 3))
    data.+=(("ACME", 5L, 20, 4))
    data.+=(("ACME", 6L, 24, 4))
    data.+=(("ACME", 7L, 25, 3))
    data.+=(("ACME", 8L, 19, 8))

    val t = env.fromCollection(data).toTable(tEnv).as('symbol, 'tstamp, 'price, 'tax)
    tEnv.registerTable("Ticker", t)

    val sqlQuery =
      s"""
         |SELECT *
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  MEASURES
         |    STRT.tstamp AS start_tstamp,
         |    LAST(DOWN.tstamp) AS bottom_tstamp,
         |    LAST(UP.tstamp) AS end_tstamp
         |  ONE ROW PER MATCH
         |  PATTERN (STRT DOWN+ UP+)
         |  DEFINE
         |    DOWN AS DOWN.price < PREV(DOWN.price),
         |    UP AS UP.price > PREV(UP.price) AND UP.tax > LAST(DOWN.tax)
         |) AS T
         |""".stripMargin

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List("2,4,5", "2,4,6", "3,4,5", "3,4,6")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testWithinEventTime() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val data = new mutable.MutableList[Either[(Long, (String, Int, Int)), Long]]
    data.+=(Left((3000L, ("ACME", 17, 2))))
    data.+=(Left((1000L, ("ACME", 12, 1))))
    data.+=(Right(4000L))
    data.+=(Left((5000L, ("ACME", 13, 3))))
    data.+=(Left((7000L, ("ACME", 15, 4))))
    data.+=(Right(8000L))
    data.+=(Left((9000L, ("ACME", 20, 5))))
    data.+=(Right(13000L))
    data.+=(Left((15000L, ("ACME", 19, 8))))
    data.+=(Right(16000L))

    val t = env.addSource(new EventTimeSourceFunction[(String, Int, Int)](data))
      .toTable(tEnv, 'symbol, 'price, 'tax, 'tstamp.rowtime)
    tEnv.registerTable("Ticker", t)

    val sqlQuery =
      s"""
         |SELECT *
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  PARTITION BY symbol
         |  ORDER BY tstamp
         |  MEASURES
         |    STRT.tstamp AS start_tstamp,
         |    FIRST(DOWN.tstamp) AS bottom_tstamp,
         |    FIRST(UP.tstamp) AS end_tstamp,
         |    FIRST(DOWN.price + DOWN.tax + 1) AS bottom_total,
         |    FIRST(UP.price + UP.tax) AS end_total
         |  ONE ROW PER MATCH
         |  PATTERN (STRT DOWN+ UP+) within interval '5' second
         |  DEFINE
         |    DOWN AS DOWN.price < PREV(DOWN.price),
         |    UP AS UP.price > PREV(UP.price)
         |) AS T
         |""".stripMargin

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List(
      "ACME,1970-01-01 00:00:03.0,1970-01-01 00:00:05.0,1970-01-01 00:00:07.0,17,19")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }
}
