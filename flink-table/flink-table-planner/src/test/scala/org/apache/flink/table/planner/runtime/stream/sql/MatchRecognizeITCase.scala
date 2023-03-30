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

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.{AggregateFunction, FunctionContext, ScalarFunction}
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.WeightedAvg
import org.apache.flink.table.planner.runtime.utils.{StreamingWithStateTestBase, TestingAppendSink, UserDefinedFunctionTestUtils}
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.runtime.utils.TimeTestUtil.EventTimeSourceFunction
import org.apache.flink.table.planner.utils.TableTestUtil
import org.apache.flink.types.Row

import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.sql.Timestamp
import java.time.{Instant, ZoneId}
import java.util.TimeZone

import scala.collection.mutable

@RunWith(classOf[Parameterized])
class MatchRecognizeITCase(backend: StateBackendMode) extends StreamingWithStateTestBase(backend) {

  @Test
  def testSimplePattern(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

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

    val t = env.fromCollection(data).toTable(tEnv, 'id, 'name, 'proctime.proctime)
    tEnv.createTemporaryView("MyTable", t)

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

    val sink = new TestingAppendSink()
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(sink)
    env.execute()

    val expected = mutable.MutableList("6,7,8")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testSimplePatternWithNulls(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

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

    val t = env.fromCollection(data).toTable(tEnv, 'id, 'name, 'nullField, 'proctime.proctime)
    tEnv.createTemporaryView("MyTable", t)

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

    val sink = new TestingAppendSink()
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(sink)
    env.execute()

    val expected = mutable.MutableList("1,null,3,null", "6,null,8,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testCodeSplitsAreProperlyGenerated(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)
    // TODO: this code is ported from old planner,
    //  However code split is not supported in planner yet.
    tEnv.getConfig.setMaxGeneratedCodeLength(1)

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

    val t = env
      .fromCollection(data)
      .toTable(tEnv, 'id, 'name, 'key1, 'key2, 'proctime.proctime)
    tEnv.createTemporaryView("MyTable", t)

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

    val sink = new TestingAppendSink()
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      "key1,second_key3,1,key1,2,3,second_key3",
      "key2,second_key4,6,key2,7,8,second_key4")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testEventsAreProperlyOrdered(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val data = Seq(
      Left(2L, (12, 1, "a", 1)),
      Left(1L, (11, 2, "b", 2)),
      Left(3L, (10, 3, "c", 3)), // event time order breaks this match
      Right(3L),
      Left(4L, (8, 4, "a", 4)),
      Left(4L, (9, 5, "b", 5)),
      Left(5L, (7, 6, "c", 6)), // secondary order breaks this match
      Right(5L),
      Left(6L, (6, 8, "a", 7)),
      Left(6L, (6, 7, "b", 8)),
      Left(8L, (4, 9, "c", 9)), // ternary order breaks this match
      Right(8L),
      Left(9L, (3, 10, "a", 10)),
      Left(10L, (2, 11, "b", 11)),
      Left(11L, (1, 12, "c", 12)),
      Right(11L)
    )

    val t = env
      .addSource(new EventTimeSourceFunction[(Int, Int, String, Int)](data))
      .toTable(tEnv, 'secondaryOrder, 'ternaryOrder, 'name, 'id, 'rowtime.rowtime)
    tEnv.createTemporaryView("MyTable", t)

    val sqlQuery =
      s"""
         |SELECT T.aid, T.bid, T.cid
         |FROM MyTable
         |MATCH_RECOGNIZE (
         |  ORDER BY rowtime, secondaryOrder DESC, ternaryOrder ASC
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

    val sink = new TestingAppendSink()
    val result = table.toAppendStream[Row]
    result.addSink(sink)
    env.execute()

    val expected = mutable.MutableList("10,11,12")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testMatchRecognizeAppliedToWindowedGrouping(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val data = new mutable.MutableList[(String, Long, Int, Int)]
    // first window
    data.+=(("ACME", Time.seconds(1).toMilliseconds, 1, 1))
    data.+=(("ACME", Time.seconds(2).toMilliseconds, 2, 2))
    // second window
    data.+=(("ACME", Time.seconds(4).toMilliseconds, 1, 4))
    data.+=(("ACME", Time.seconds(5).toMilliseconds, 1, 3))
    // third window
    data.+=(("ACME", Time.seconds(7).toMilliseconds, 2, 3))
    data.+=(("ACME", Time.seconds(8).toMilliseconds, 2, 3))

    data.+=(("ACME1", Time.seconds(1).toMilliseconds, 20, 4))
    data.+=(("ACME1", Time.seconds(1).toMilliseconds, 24, 4))
    data.+=(("ACME1", Time.seconds(1).toMilliseconds, 25, 3))
    data.+=(("ACME1", Time.seconds(1).toMilliseconds, 19, 8))

    val t = env
      .fromCollection(data)
      .assignAscendingTimestamps(e => e._2)
      .toTable(tEnv, 'symbol, 'rowtime.rowtime, 'price, 'tax)
    tEnv.createTemporaryView("Ticker", t)

    val sqlQuery =
      s"""
         |SELECT *
         |FROM (
         |   SELECT
         |      symbol,
         |      SUM(price) as price,
         |      TUMBLE_ROWTIME(rowtime, interval '3' second) as rowTime,
         |      TUMBLE_START(rowtime, interval '3' second) as startTime
         |   FROM Ticker
         |   GROUP BY symbol, TUMBLE(rowtime, interval '3' second)
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

    val sink = new TestingAppendSink()
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(sink)
    env.execute()

    val expected = List("ACME,2,1970-01-01T00:00:03")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testWindowedGroupingAppliedToMatchRecognize(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val data = new mutable.MutableList[(String, Long, Int, Int)]
    // first window
    data.+=(("ACME", Time.seconds(1).toMilliseconds, 1, 1))
    data.+=(("ACME", Time.seconds(2).toMilliseconds, 2, 2))
    // second window
    data.+=(("ACME", Time.seconds(4).toMilliseconds, 1, 4))
    data.+=(("ACME", Time.seconds(5).toMilliseconds, 1, 3))

    val tickerEvents = env
      .fromCollection(data)
      .assignAscendingTimestamps(tickerEvent => tickerEvent._2)
      .toTable(tEnv, 'symbol, 'rowtime.rowtime, 'price, 'tax)

    tEnv.createTemporaryView("Ticker", tickerEvents)

    val sqlQuery =
      s"""
         |SELECT
         |  symbol,
         |  SUM(price) as price,
         |  TUMBLE_ROWTIME(matchRowtime, interval '3' second) as rowTime,
         |  TUMBLE_START(matchRowtime, interval '3' second) as startTime
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  PARTITION BY symbol
         |  ORDER BY rowtime
         |  MEASURES
         |    A.price as price,
         |    A.tax as tax,
         |    MATCH_ROWTIME() as matchRowtime
         |  ONE ROW PER MATCH
         |  PATTERN (A)
         |  DEFINE
         |    A AS A.price > 0
         |) AS T
         |GROUP BY symbol, TUMBLE(matchRowtime, interval '3' second)
         |""".stripMargin

    val sink = new TestingAppendSink()
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(sink)
    env.execute()

    val expected = List(
      "ACME,3,1970-01-01T00:00:02.999,1970-01-01T00:00",
      "ACME,2,1970-01-01T00:00:05.999,1970-01-01T00:00:03")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testWindowedGroupingAppliedToMatchRecognizeOnLtzRowtime(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val data: Seq[Row] = Seq(
      // first window
      rowOf("ACME", Instant.ofEpochSecond(1), 1, 1),
      rowOf("ACME", Instant.ofEpochSecond(2), 2, 2),
      // second window
      rowOf("ACME", Instant.ofEpochSecond(3), 1, 4),
      rowOf("ACME", Instant.ofEpochSecond(4), 1, 3)
    )

    tEnv.getConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))

    val dataId = TestValuesTableFactory.registerData(data)
    tEnv.executeSql(s"""
                       |CREATE TABLE Ticker (
                       | `symbol` STRING,
                       | `ts_ltz` TIMESTAMP_LTZ(3),
                       | `price` INT,
                       | `tax` INT,
                       | WATERMARK FOR `ts_ltz` AS `ts_ltz` - INTERVAL '1' SECOND
                       |) WITH (
                       | 'connector' = 'values',
                       | 'data-id' = '$dataId'
                       |)
                       |""".stripMargin)

    val sqlQuery =
      s"""
         |SELECT
         |  symbol,
         |  SUM(price) as price,
         |  TUMBLE_ROWTIME(matchRowtime, interval '3' second) as rowTime,
         |  TUMBLE_START(matchRowtime, interval '3' second) as startTime
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  PARTITION BY symbol
         |  ORDER BY ts_ltz
         |  MEASURES
         |    A.price as price,
         |    A.tax as tax,
         |    MATCH_ROWTIME(ts_ltz) as matchRowtime
         |  ONE ROW PER MATCH
         |  PATTERN (A)
         |  DEFINE
         |    A AS A.price > 0
         |) AS T
         |GROUP BY symbol, TUMBLE(matchRowtime, interval '3' second)
         |""".stripMargin

    val sink = new TestingAppendSink()
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(sink)
    env.execute()

    val expected = List(
      "ACME,3,1970-01-01T00:00:02.999Z,1970-01-01T08:00",
      "ACME,2,1970-01-01T00:00:05.999Z,1970-01-01T08:00:03")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testLogicalOffsets(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val data = new mutable.MutableList[(String, Long, Int, Int)]
    data.+=(("ACME", 1L, 19, 1))
    data.+=(("ACME", 2L, 17, 2))
    data.+=(("ACME", 3L, 13, 3))
    data.+=(("ACME", 4L, 20, 4))
    data.+=(("ACME", 5L, 20, 5))
    data.+=(("ACME", 6L, 26, 6))
    data.+=(("ACME", 7L, 20, 7))
    data.+=(("ACME", 8L, 25, 8))

    val t = env
      .fromCollection(data)
      .toTable(tEnv, 'symbol, 'tstamp, 'price, 'tax, 'proctime.proctime)
    tEnv.createTemporaryView("Ticker", t)

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

    val sink = new TestingAppendSink()
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(sink)
    env.execute()

    val expected = List("6,7,8,33,33")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testPartitionByWithParallelSource(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val data = new mutable.MutableList[(String, Long, Int, Int)]
    data.+=(("ACME", 1L, 19, 1))
    data.+=(("ACME", 2L, 17, 2))
    data.+=(("ACME", 3L, 13, 3))
    data.+=(("ACME", 4L, 20, 4))

    val t = env
      .fromCollection(data)
      .assignAscendingTimestamps(tickerEvent => tickerEvent._2)
      .setParallelism(env.getParallelism)
      .toTable(tEnv, 'symbol, 'rowtime.rowtime, 'price, 'tax)
    tEnv.createTemporaryView("Ticker", t)

    val sqlQuery =
      s"""
         |SELECT *
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  PARTITION BY symbol
         |  ORDER BY rowtime
         |  MEASURES
         |    DOWN.tax AS bottom_tax,
         |    UP.tax AS end_tax
         |  ONE ROW PER MATCH
         |  AFTER MATCH SKIP PAST LAST ROW
         |  PATTERN (DOWN UP)
         |  DEFINE
         |    DOWN AS DOWN.price = 13,
         |    UP AS UP.price = 20
         |) AS T
         |""".stripMargin

    val sink = new TestingAppendSink()
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(sink)
    env.execute()

    val expected = List("ACME,3,4")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testLogicalOffsetsWithStarVariable(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val data = new mutable.MutableList[(Int, String, Long, Int)]
    data.+=((1, "ACME", 1L, 20))
    data.+=((2, "ACME", 2L, 19))
    data.+=((3, "ACME", 3L, 18))
    data.+=((4, "ACME", 4L, 17))
    data.+=((5, "ACME", 5L, 16))
    data.+=((6, "ACME", 6L, 15))
    data.+=((7, "ACME", 7L, 14))
    data.+=((8, "ACME", 8L, 20))

    val t = env
      .fromCollection(data)
      .toTable(tEnv, 'id, 'symbol, 'tstamp, 'price, 'proctime.proctime)
    tEnv.createTemporaryView("Ticker", t)

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

    val sink = new TestingAppendSink()
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(sink)
    env.execute()

    val expected = List("1,2,3,4,5,6,7,8,8,7,6,5,4,3,2,1")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testLogicalOffsetOutsideOfRangeInMeasures(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val data = new mutable.MutableList[(String, Long, Int, Int)]
    data.+=(("ACME", 1L, 19, 1))
    data.+=(("ACME", 2L, 17, 2))
    data.+=(("ACME", 3L, 13, 3))
    data.+=(("ACME", 4L, 20, 4))

    val t = env
      .fromCollection(data)
      .toTable(tEnv, 'symbol, 'tstamp, 'price, 'tax, 'proctime.proctime)
    tEnv.createTemporaryView("Ticker", t)

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

    val sink = new TestingAppendSink()
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(sink)
    env.execute()

    val expected = List("19,13,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  /**
   * This query checks:
   *
   *   1. count(D.price) produces 0, because no rows matched to D 2. sum(D.price) produces null,
   *      because no rows matched to D 3. aggregates that take multiple parameters work 4.
   *      aggregates with expressions work
   */
  @Test
  def testAggregates(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)
    tEnv.getConfig.setMaxGeneratedCodeLength(1)

    val data = new mutable.MutableList[(Int, String, Long, Double, Int)]
    data.+=((1, "a", 1, 0.8, 1))
    data.+=((2, "z", 2, 0.8, 3))
    data.+=((3, "b", 1, 0.8, 2))
    data.+=((4, "c", 1, 0.8, 5))
    data.+=((5, "d", 4, 0.1, 5))
    data.+=((6, "a", 2, 1.5, 2))
    data.+=((7, "b", 2, 0.8, 3))
    data.+=((8, "c", 1, 0.8, 2))
    data.+=((9, "h", 4, 0.8, 3))
    data.+=((10, "h", 4, 0.8, 3))
    data.+=((11, "h", 2, 0.8, 3))
    data.+=((12, "h", 2, 0.8, 3))

    val t = env
      .fromCollection(data)
      .toTable(tEnv, 'id, 'name, 'price, 'rate, 'weight, 'proctime.proctime)
    tEnv.createTemporaryView("MyTable", t)
    tEnv.createTemporarySystemFunction("weightedAvg", classOf[WeightedAvg])

    val sqlQuery =
      s"""
         |SELECT *
         |FROM MyTable
         |MATCH_RECOGNIZE (
         |  ORDER BY proctime
         |  MEASURES
         |    FIRST(id) as startId,
         |    SUM(A.price) AS sumA,
         |    COUNT(D.price) AS countD,
         |    SUM(D.price) as sumD,
         |    weightedAvg(price, weight) as wAvg,
         |    AVG(B.price) AS avgB,
         |    SUM(B.price * B.rate) as sumExprB,
         |    LAST(id) as endId
         |  AFTER MATCH SKIP PAST LAST ROW
         |  PATTERN (A+ B+ C D? E )
         |  DEFINE
         |    A AS SUM(A.price) < 6,
         |    B AS SUM(B.price * B.rate) < SUM(A.price) AND
         |         SUM(B.price * B.rate) > 0.2 AND
         |         SUM(B.price) >= 1 AND
         |         AVG(B.price) >= 1 AND
         |         weightedAvg(price, weight) > 1
         |) AS T
         |""".stripMargin

    val sink = new TestingAppendSink()
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(sink)
    env.execute()

    val expected = mutable.MutableList("1,5,0,null,2,3,3.4,8", "9,4,0,null,3,4,3.2,12")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testAggregatesWithNullInputs(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)
    tEnv.getConfig.setMaxGeneratedCodeLength(1)

    val data = new mutable.MutableList[Row]
    data.+=(Row.of(Int.box(1), "a", Int.box(10)))
    data.+=(Row.of(Int.box(2), "z", Int.box(10)))
    data.+=(Row.of(Int.box(3), "b", null))
    data.+=(Row.of(Int.box(4), "c", null))
    data.+=(Row.of(Int.box(5), "d", Int.box(3)))
    data.+=(Row.of(Int.box(6), "c", Int.box(3)))
    data.+=(Row.of(Int.box(7), "c", Int.box(3)))
    data.+=(Row.of(Int.box(8), "c", Int.box(3)))
    data.+=(Row.of(Int.box(9), "c", Int.box(2)))

    val t = env
      .fromCollection(data)(
        Types.ROW(
          BasicTypeInfo.INT_TYPE_INFO,
          BasicTypeInfo.STRING_TYPE_INFO,
          BasicTypeInfo.INT_TYPE_INFO))
      .toTable(tEnv, 'id, 'name, 'price, 'proctime.proctime)
    tEnv.createTemporaryView("MyTable", t)
    tEnv.registerFunction("weightedAvg", new WeightedAvg)

    val sqlQuery =
      s"""
         |SELECT *
         |FROM MyTable
         |MATCH_RECOGNIZE (
         |  ORDER BY proctime
         |  MEASURES
         |    SUM(A.price) as sumA,
         |    COUNT(A.id) as countAId,
         |    COUNT(A.price) as countAPrice,
         |    COUNT(*) as countAll,
         |    COUNT(price) as countAllPrice,
         |    LAST(id) as endId
         |  AFTER MATCH SKIP PAST LAST ROW
         |  PATTERN (A+ C)
         |  DEFINE
         |    A AS SUM(A.price) < 30,
         |    C AS C.name = 'c'
         |) AS T
         |""".stripMargin

    val sink = new TestingAppendSink()
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(sink)
    env.execute()

    val expected = mutable.MutableList("29,7,5,8,6,8")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testAccessingCurrentTime(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val data = new mutable.MutableList[(Int, String)]
    data.+=((1, "a"))

    val t = env.fromCollection(data).toTable(tEnv, 'id, 'name, 'proctime.proctime)
    tEnv.createTemporaryView("MyTable", t)

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

    val sink = new TestingAppendSink()
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(sink)
    env.execute()

    val expected = List("1")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)

    // We do not assert the proctime in the result, cause it is currently
    // accessed from System.currentTimeMillis(), so there is no graceful way to assert the proctime
  }

  @Test
  def testUserDefinedFunctions(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)
    tEnv.getConfig.setMaxGeneratedCodeLength(1)

    val data = new mutable.MutableList[(Int, String, Long)]
    data.+=((1, "a", 1))
    data.+=((2, "a", 1))
    data.+=((3, "a", 1))
    data.+=((4, "a", 1))
    data.+=((5, "a", 1))
    data.+=((6, "b", 1))
    data.+=((7, "a", 1))
    data.+=((8, "a", 1))
    data.+=((9, "f", 1))

    val t = env
      .fromCollection(data)
      .toTable(tEnv, 'id, 'name, 'price, 'proctime.proctime)
    tEnv.createTemporaryView("MyTable", t)
    tEnv.registerFunction("prefix", new PrefixingScalarFunc)
    tEnv.registerFunction("countFrom", new RichAggFunc)
    val prefix = "PREF"
    val startFrom = 4
    UserDefinedFunctionTestUtils
      .setJobParameters(env, Map("prefix" -> prefix, "start" -> startFrom.toString))

    val sqlQuery =
      s"""
         |SELECT *
         |FROM MyTable
         |MATCH_RECOGNIZE (
         |  ORDER BY proctime
         |  MEASURES
         |    FIRST(id) as firstId,
         |    prefix(A.name) as prefixedNameA,
         |    countFrom(A.price) as countFromA,
         |    LAST(id) as lastId
         |  AFTER MATCH SKIP PAST LAST ROW
         |  PATTERN (A+ C)
         |  DEFINE
         |    A AS prefix(A.name) = '$prefix:a' AND countFrom(A.price) <= ${startFrom + 4}
         |) AS T
         |""".stripMargin

    val sink = new TestingAppendSink()
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(sink)
    env.execute()

    val expected = mutable.MutableList("1,PREF:a,8,5", "7,PREF:a,6,9")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }
}

@SerialVersionUID(1L)
class ToMillis extends ScalarFunction {
  def eval(t: Timestamp): Long = {
    t.toInstant.toEpochMilli + TimeZone.getDefault.getOffset(t.toInstant.toEpochMilli)
  }
}

@SerialVersionUID(1L)
private class PrefixingScalarFunc extends ScalarFunction {

  private var prefix = "ERROR_VALUE"

  override def open(context: FunctionContext): Unit = {
    prefix = context.getJobParameter("prefix", "")
  }

  def eval(value: String): String = {
    s"$prefix:$value"
  }
}

private case class CountAcc(var count: Long)

private class RichAggFunc extends AggregateFunction[Long, CountAcc] {

  private var start: Long = 0

  override def open(context: FunctionContext): Unit = {
    start = context.getJobParameter("start", "0").toLong
  }

  override def close(): Unit = {
    start = 0
  }

  def accumulate(countAcc: CountAcc, value: Long): Unit = {
    countAcc.count += value
  }

  override def createAccumulator(): CountAcc = CountAcc(start)

  override def getValue(accumulator: CountAcc): Long = accumulator.count
}
