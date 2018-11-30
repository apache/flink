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

import java.sql.Timestamp
import java.util.TimeZone

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableConfig, TableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.runtime.utils.TimeTestUtil.EventTimeSourceFunction
import org.apache.flink.table.runtime.utils.{StreamITCase, StreamingWithStateTestBase}
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.mutable

class MatchRecognizeITCase extends StreamingWithStateTestBase {

  @Test
  def testSimpleCEP(): Unit = {
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

    val t = env.fromCollection(data).toTable(tEnv,'id, 'name, 'proctime.proctime)
    tEnv.registerTable("MyTable", t)

    val sqlQuery =
      s"""
        |SELECT T.aid, T.bid, T.cid
        |FROM MyTable
        |MATCH_RECOGNIZE (
        |  ORDER BY proctime
        |  MEASURES
        |    `A"`.id AS aid,
        |    \u006C.id AS bid,
        |    C.id AS cid
        |  PATTERN (`A"` \u006C C)
        |  DEFINE
        |    `A"` AS name = 'a',
        |    \u006C AS name = 'b',
        |    C AS name = 'c'
        |) AS T
        |""".stripMargin

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList("6,7,8")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testSimpleCEPWithNulls(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val data = new mutable.MutableList[(Int, String, String)]
    data.+=((1, "a", null))
    data.+=((2, "b", null))
    data.+=((3, "c", null))
    data.+=((4, "d", null))
    data.+=((5, null, null))
    data.+=((6, "a", null))
    data.+=((7, "b", null))
    data.+=((8, "c", null))
    data.+=((9, null, null))

    val t = env.fromCollection(data).toTable(tEnv,'id, 'name, 'nullField, 'proctime.proctime)
    tEnv.registerTable("MyTable", t)

    val sqlQuery =
      s"""
         |SELECT T.aid, T.bNull, T.cid, T.aNull
         |FROM MyTable
         |MATCH_RECOGNIZE (
         |  ORDER BY proctime
         |  MEASURES
         |    A.id AS aid,
         |    A.nullField AS aNull,
         |    LAST(B.nullField) AS bNull,
         |    C.id AS cid
         |  PATTERN (A B C)
         |  DEFINE
         |    A AS name = 'a' AND nullField IS NULL,
         |    B AS name = 'b' AND LAST(A.nullField) IS NULL,
         |    C AS name = 'c'
         |) AS T
         |""".stripMargin

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList("1,null,3,null", "6,null,8,null")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testCodeSplitsAreProperlyGenerated(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableConfig = new TableConfig
    tableConfig.setMaxGeneratedCodeLength(1)
    val tEnv = TableEnvironment.getTableEnvironment(env, tableConfig)
    StreamITCase.clear

    val data = new mutable.MutableList[(Int, String, String, String)]
    data.+=((1, "a", "key1", "second_key3"))
    data.+=((2, "b", "key1", "second_key3"))
    data.+=((3, "c", "key1", "second_key3"))
    data.+=((4, "d", "key", "second_key"))
    data.+=((5, "e", "key", "second_key"))
    data.+=((6, "a", "key2", "second_key4"))
    data.+=((7, "b", "key2", "second_key4"))
    data.+=((8, "c", "key2", "second_key4"))
    data.+=((9, "f", "key", "second_key"))

    val t = env.fromCollection(data)
      .toTable(tEnv, 'id, 'name, 'key1, 'key2, 'proctime.proctime)
    tEnv.registerTable("MyTable", t)

    val sqlQuery =
      s"""
         |SELECT *
         |FROM MyTable
         |MATCH_RECOGNIZE (
         |  PARTITION BY key1, key2
         |  ORDER BY proctime
         |  MEASURES
         |    A.id AS aid,
         |    A.key1 AS akey1,
         |    LAST(B.id) AS bid,
         |    C.id AS cid,
         |    C.key2 AS ckey2
         |  PATTERN (A B C)
         |  DEFINE
         |    A AS name = 'a' AND key1 LIKE '%key%' AND id > 0,
         |    B AS name = 'b' AND LAST(A.name, 2) IS NULL,
         |    C AS name = 'c' AND LAST(A.name) = 'a'
         |) AS T
         |""".stripMargin

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList(
      "key1,second_key3,1,key1,2,3,second_key3",
      "key2,second_key4,6,key2,7,8,second_key4")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventsAreProperlyOrdered(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val data = Seq(
      Left(2L, (12, 1, "a", 1)),
      Left(1L, (11, 2, "b", 2)),
      Left(3L, (10, 3, "c", 3)), //event time order breaks this match
      Right(3L),
      Left(4L, (8, 4, "a", 4)),
      Left(4L, (9, 5, "b", 5)),
      Left(5L, (7, 6, "c", 6)), //secondary order breaks this match
      Right(5L),
      Left(6L, (6, 8, "a", 7)),
      Left(6L, (6, 7, "b", 8)),
      Left(8L, (4, 9, "c", 9)), //ternary order breaks this match
      Right(8L),
      Left(9L, (3, 10, "a", 10)),
      Left(10L, (2, 11, "b", 11)),
      Left(11L, (1, 12, "c", 12)),
      Right(11L)
    )

    val t = env.addSource(new EventTimeSourceFunction[(Int, Int, String, Int)](data))
      .toTable(tEnv, 'secondaryOrder, 'ternaryOrder, 'name, 'id,'tstamp.rowtime)
    tEnv.registerTable("MyTable", t)

    val sqlQuery =
      s"""
         |SELECT T.aid, T.bid, T.cid
         |FROM MyTable
         |MATCH_RECOGNIZE (
         |  ORDER BY tstamp, secondaryOrder DESC, ternaryOrder ASC
         |  MEASURES
         |    A.id AS aid,
         |    B.id AS bid,
         |    C.id AS cid
         |  PATTERN (A B C)
         |  DEFINE
         |    A AS name = 'a',
         |    B AS name = 'b',
         |    C AS name = 'c'
         |) AS T
         |""".stripMargin

    val table = tEnv.sqlQuery(sqlQuery)

    val result = table.toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList("10,11,12")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testMatchRecognizeAppliedToWindowedGrouping(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val data = new mutable.MutableList[(String, Long, Int, Int)]
    //first window
    data.+=(("ACME", Time.seconds(1).toMilliseconds, 1, 1))
    data.+=(("ACME", Time.seconds(2).toMilliseconds, 2, 2))
    //second window
    data.+=(("ACME", Time.seconds(4).toMilliseconds, 1, 4))
    data.+=(("ACME", Time.seconds(5).toMilliseconds, 1, 3))
    //third window
    data.+=(("ACME", Time.seconds(7).toMilliseconds, 2, 3))
    data.+=(("ACME", Time.seconds(8).toMilliseconds, 2, 3))

    data.+=(("ACME1", Time.seconds(1).toMilliseconds, 20, 4))
    data.+=(("ACME1", Time.seconds(1).toMilliseconds, 24, 4))
    data.+=(("ACME1", Time.seconds(1).toMilliseconds, 25, 3))
    data.+=(("ACME1", Time.seconds(1).toMilliseconds, 19, 8))

    val t = env.fromCollection(data)
      .assignAscendingTimestamps(e => e._2)
      .toTable(tEnv, 'symbol, 'tstamp.rowtime, 'price, 'tax)
    tEnv.registerTable("Ticker", t)

    val sqlQuery =
      s"""
         |SELECT *
         |FROM (
         |   SELECT
         |      symbol,
         |      SUM(price) as price,
         |      TUMBLE_ROWTIME(tstamp, interval '3' second) as rowTime,
         |      TUMBLE_START(tstamp, interval '3' second) as startTime
         |   FROM Ticker
         |   GROUP BY symbol, TUMBLE(tstamp, interval '3' second)
         |)
         |MATCH_RECOGNIZE (
         |  PARTITION BY symbol
         |  ORDER BY rowTime
         |  MEASURES
         |    B.price as dPrice,
         |    B.startTime as dTime
         |  ONE ROW PER MATCH
         |  PATTERN (A B)
         |  DEFINE
         |    B AS B.price < A.price
         |)
         |""".stripMargin

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List("ACME,2,1970-01-01 00:00:03.0")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testLogicalOffsets(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val data = new mutable.MutableList[(String, Long, Int, Int)]
    data.+=(("ACME", 1L, 19, 1))
    data.+=(("ACME", 2L, 17, 2))
    data.+=(("ACME", 3L, 13, 3))
    data.+=(("ACME", 4L, 20, 4))
    data.+=(("ACME", 5L, 20, 5))
    data.+=(("ACME", 6L, 26, 6))
    data.+=(("ACME", 7L, 20, 7))
    data.+=(("ACME", 8L, 25, 8))

    val t = env.fromCollection(data)
      .toTable(tEnv, 'symbol, 'tstamp, 'price, 'tax, 'proctime.proctime)
    tEnv.registerTable("Ticker", t)

    val sqlQuery =
      s"""
         |SELECT *
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  ORDER BY proctime
         |  MEASURES
         |    FIRST(DOWN.tstamp) AS start_tstamp,
         |    LAST(DOWN.tstamp) AS bottom_tstamp,
         |    UP.tstamp AS end_tstamp,
         |    FIRST(DOWN.price + DOWN.tax + 1) AS bottom_total,
         |    UP.price + UP.tax AS end_total
         |  ONE ROW PER MATCH
         |  AFTER MATCH SKIP PAST LAST ROW
         |  PATTERN (DOWN{2,} UP)
         |  DEFINE
         |    DOWN AS price < LAST(DOWN.price, 1) OR LAST(DOWN.price, 1) IS NULL,
         |    UP AS price < FIRST(DOWN.price)
         |) AS T
         |""".stripMargin

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List("6,7,8,33,33")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testLogicalOffsetsWithStarVariable(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val data = new mutable.MutableList[(Int, String, Long, Int)]
    data.+=((1, "ACME", 1L, 20))
    data.+=((2, "ACME", 2L, 19))
    data.+=((3, "ACME", 3L, 18))
    data.+=((4, "ACME", 4L, 17))
    data.+=((5, "ACME", 5L, 16))
    data.+=((6, "ACME", 6L, 15))
    data.+=((7, "ACME", 7L, 14))
    data.+=((8, "ACME", 8L, 20))

    val t = env.fromCollection(data)
      .toTable(tEnv, 'id, 'symbol, 'tstamp, 'price, 'proctime.proctime)
    tEnv.registerTable("Ticker", t)

    val sqlQuery =
      s"""
         |SELECT *
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  ORDER BY proctime
         |  MEASURES
         |    FIRST(id, 0) as id0,
         |    FIRST(id, 1) as id1,
         |    FIRST(id, 2) as id2,
         |    FIRST(id, 3) as id3,
         |    FIRST(id, 4) as id4,
         |    FIRST(id, 5) as id5,
         |    FIRST(id, 6) as id6,
         |    FIRST(id, 7) as id7,
         |    LAST(id, 0) as id8,
         |    LAST(id, 1) as id9,
         |    LAST(id, 2) as id10,
         |    LAST(id, 3) as id11,
         |    LAST(id, 4) as id12,
         |    LAST(id, 5) as id13,
         |    LAST(id, 6) as id14,
         |    LAST(id, 7) as id15
         |  ONE ROW PER MATCH
         |  AFTER MATCH SKIP PAST LAST ROW
         |  PATTERN (`DOWN"`{2,} UP)
         |  DEFINE
         |    `DOWN"` AS price < LAST(price, 1) OR LAST(price, 1) IS NULL,
         |    UP AS price = FIRST(price) AND price > FIRST(price, 3) AND price = LAST(price, 7)
         |) AS T
         |""".stripMargin

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List("1,2,3,4,5,6,7,8,8,7,6,5,4,3,2,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testLogicalOffsetOutsideOfRangeInMeasures(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val data = new mutable.MutableList[(String, Long, Int, Int)]
    data.+=(("ACME", 1L, 19, 1))
    data.+=(("ACME", 2L, 17, 2))
    data.+=(("ACME", 3L, 13, 3))
    data.+=(("ACME", 4L, 20, 4))

    val t = env.fromCollection(data)
      .toTable(tEnv, 'symbol, 'tstamp, 'price, 'tax, 'proctime.proctime)
    tEnv.registerTable("Ticker", t)

    val sqlQuery =
      s"""
         |SELECT *
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  ORDER BY proctime
         |  MEASURES
         |    FIRST(DOWN.price) as first,
         |    LAST(DOWN.price) as last,
         |    FIRST(DOWN.price, 5) as nullPrice
         |  ONE ROW PER MATCH
         |  AFTER MATCH SKIP PAST LAST ROW
         |  PATTERN (DOWN{2,} UP)
         |  DEFINE
         |    DOWN AS price < LAST(DOWN.price, 1) OR LAST(DOWN.price, 1) IS NULL,
         |    UP AS price > LAST(DOWN.price)
         |) AS T
         |""".stripMargin

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List("19,13,null")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testAccessingProctime(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val data = new mutable.MutableList[(Int, String)]
    data.+=((1, "a"))

    val t = env.fromCollection(data).toTable(tEnv,'id, 'name, 'proctime.proctime)
    tEnv.registerTable("MyTable", t)

    val sqlQuery =
      s"""
         |SELECT T.aid
         |FROM MyTable
         |MATCH_RECOGNIZE (
         |  ORDER BY proctime
         |  MEASURES
         |    A.id AS aid,
         |    A.proctime AS aProctime,
         |    LAST(A.proctime + INTERVAL '1' second) as calculatedField
         |  PATTERN (A)
         |  DEFINE
         |    A AS proctime >= (CURRENT_TIMESTAMP - INTERVAL '1' day)
         |) AS T
         |""".stripMargin

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List("1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)

    // We do not assert the proctime in the result, cause it is currently
    // accessed from System.currentTimeMillis(), so there is no graceful way to assert the proctime
  }

  @Test
  def testPartitioningByTimeIndicator(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val data = new mutable.MutableList[(Int, String)]
    data.+=((1, "a"))

    val t = env.fromCollection(data).toTable(tEnv,'id, 'name, 'proctime.proctime)
    tEnv.registerTable("MyTable", t)

    val sqlQuery =
      s"""
         |SELECT T.aid
         |FROM MyTable
         |MATCH_RECOGNIZE (
         |  ORDER BY proctime
         |  MEASURES
         |    A.id AS aid,
         |    A.proctime AS aProctime,
         |    LAST(A.proctime + INTERVAL '1' second) as calculatedField
         |  PATTERN (A)
         |  DEFINE
         |    A AS proctime >= (CURRENT_TIMESTAMP - INTERVAL '1' day)
         |) AS T
         |""".stripMargin

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List("1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)

    // We do not assert the proctime in the result, cause it is currently
    // accessed from System.currentTimeMillis(), so there is no graceful way to assert the proctime
  }
}

class ToMillis extends ScalarFunction {
  def eval(t: Timestamp): Long = {
    t.toInstant.toEpochMilli + TimeZone.getDefault.getOffset(t.toInstant.toEpochMilli)
  }
}
