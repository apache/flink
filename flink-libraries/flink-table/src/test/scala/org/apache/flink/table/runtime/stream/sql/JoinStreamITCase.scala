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
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.Null
import org.apache.flink.table.runtime.utils.StreamingWithMiniBatchTestBase.MiniBatchMode
import org.apache.flink.table.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.runtime.utils._
import org.apache.flink.table.api.types.DataTypes
import org.apache.flink.types.Row

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.{Seq, mutable}

@RunWith(classOf[Parameterized])
class JoinStreamITCase(miniBatch: MiniBatchMode, mode: StateBackendMode)
  extends StreamingWithMiniBatchTestBase(miniBatch, mode) {

  val data2 = List(
    (1, 1L, "Hi"),
    (2, 2L, "Hello"),
    (3, 2L, "Hello world")
  )
  val data = List(
    (1, 1L, 0, "Hallo", 1L),
    (2, 2L, 1, "Hallo Welt", 2L),
    (2, 3L, 2, "Hallo Welt wie", 1L),
    (3, 4L, 3, "Hallo Welt wie gehts?", 2L),
    (3, 5L, 4, "ABC", 2L),
    (3, 6L, 5, "BCD", 3L)
  )
  val dataCannotBeJoin = List(
    (2, 3L, 2, "Hallo Welt wie", 1L),
    (3, 4L, 3, "Hallo Welt wie gehts?", 2L),
    (3, 5L, 4, "ABC", 2L),
    (3, 6L, 5, "BCD", 3L)
  )

  override def before(): Unit = {
    super.before()
    val tableA = failingDataSource(StreamTestData.getSmall3TupleData)
      .toTable(tEnv, 'a1, 'a2, 'a3)
    val tableB = failingDataSource(StreamTestData.get5TupleData)
      .toTable(tEnv, 'b1, 'b2, 'b3, 'b4, 'b5)
    tEnv.registerTable("A", tableA)
    tEnv.registerTable("B", tableB)
  }



  // Tests for inner join.
  override def after(): Unit = {}

  /** test proctime inner join **/
  @Test
  def testProcessTimeInnerJoin(): Unit = {
    env.setParallelism(1)

    val sqlQuery =
      """
        |SELECT t2.a, t2.c, t1.c
        |FROM T1 as t1 join T2 as t2 ON
        |  t1.a = t2.a AND
        |  t1.proctime BETWEEN t2.proctime - INTERVAL '5' SECOND AND
        |    t2.proctime + INTERVAL '5' SECOND
        |""".stripMargin

    val data1 = new mutable.MutableList[(Int, Long, String)]
    data1.+=((1, 1L, "Hi1"))
    data1.+=((1, 2L, "Hi2"))
    data1.+=((1, 5L, "Hi3"))
    data1.+=((2, 7L, "Hi5"))
    data1.+=((1, 9L, "Hi6"))
    data1.+=((1, 8L, "Hi8"))

    val data2 = new mutable.MutableList[(Int, Long, String)]
    data2.+=((1, 1L, "HiHi"))
    data2.+=((2, 2L, "HeHe"))

    val t1 = env.fromCollection(data1).toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)
      .select(('a === 1)?(Null(DataTypes.INT), 'a) as 'a, 'b, 'c, 'proctime) // test null values
    val t2 = env.fromCollection(data2).toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)
      .select(('a === 1)?(Null(DataTypes.INT), 'a) as 'a, 'b, 'c, 'proctime) // test null values

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sink = new TestingAppendSink
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(sink)
    env.execute()
  }

  /** test proctime inner join with other condition **/
  @Test
  def testProcessTimeInnerJoinWithOtherConditions(): Unit = {
    env.setParallelism(2)

    val sqlQuery =
      """
        |SELECT t2.a, t2.c, t1.c
        |FROM T1 as t1 JOIN T2 as t2 ON
        |  t1.a = t2.a AND
        |  t1.proctime BETWEEN t2.proctime - interval '5' SECOND AND
        |    t2.proctime + interval '5' second AND
        |  t1.b = t2.b
        |""".stripMargin

    val data1 = new mutable.MutableList[(String, Long, String)]
    data1.+=(("1", 1L, "Hi1"))
    data1.+=(("1", 2L, "Hi2"))
    data1.+=(("1", 5L, "Hi3"))
    data1.+=(("2", 7L, "Hi5"))
    data1.+=(("1", 9L, "Hi6"))
    data1.+=(("1", 8L, "Hi8"))

    val data2 = new mutable.MutableList[(String, Long, String)]
    data2.+=(("1", 5L, "HiHi"))
    data2.+=(("2", 2L, "HeHe"))

    // For null key test
    data1.+=((null.asInstanceOf[String], 20L, "leftNull"))
    data2.+=((null.asInstanceOf[String], 20L, "rightNull"))

    val t1 = env.fromCollection(data1).toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)
    val t2 = env.fromCollection(data2).toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sink = new TestingAppendSink
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(sink)
    env.execute()

    // Assert there is no result with null keys.
    assertFalse(sink.getAppendResults.contains("null"))
  }

  /** test rowtime inner join **/
  @Test
  def testRowTimeInnerJoin(): Unit = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_MINIBATCH_JOIN_ENABLED, false)
    tEnv.getConfig.getConf.remove(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY)
    val sqlQuery =
      """
        |SELECT t2.key, t2.id, t1.id
        |FROM T1 as t1 join T2 as t2 ON
        |  t1.key = t2.key AND
        |  t1.rt BETWEEN t2.rt - INTERVAL '5' SECOND AND
        |    t2.rt + INTERVAL '6' SECOND
        |""".stripMargin

    val data1 = new mutable.MutableList[(String, String, Long)]
    // for boundary test
    data1.+=(("A", "LEFT0.999", 999L))
    data1.+=(("A", "LEFT1", 1000L))
    data1.+=(("A", "LEFT2", 2000L))
    data1.+=(("A", "LEFT3", 3000L))
    data1.+=(("B", "LEFT4", 4000L))
    data1.+=(("A", "LEFT5", 5000L))
    data1.+=(("A", "LEFT6", 6000L))
    // test null key
    data1.+=((null.asInstanceOf[String], "LEFT8", 8000L))

    val data2 = new mutable.MutableList[(String, String, Long)]
    data2.+=(("A", "RIGHT6", 6000L))
    data2.+=(("B", "RIGHT7", 7000L))
    // test null key
    data2.+=((null.asInstanceOf[String], "RIGHT10", 10000L))

    val t1 = env.fromCollection(data1).assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rt.rowtime)
    val t2 = env.fromCollection(data2).assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rt.rowtime)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)
    val sink = new TestingAppendSink
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(sink)
    env.execute()
    val expected = mutable.MutableList("A,RIGHT6,LEFT1", "A,RIGHT6,LEFT2", "A,RIGHT6,LEFT3",
      "A,RIGHT6,LEFT5",
      "A,RIGHT6,LEFT6",
      "B,RIGHT7,LEFT4")
    assertEquals(expected, sink.getAppendResults.sorted)
  }

  /** test row time inner join with equi-times **/
  @Test
  def testRowTimeInnerJoinWithEquiTimeAttrs(): Unit = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_MINIBATCH_JOIN_ENABLED, false)
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_MINIBATCH_JOIN_ENABLED, false)
    tEnv.getConfig.getConf.remove(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY)

    val sqlQuery =
      """
        |SELECT t2.key, t2.id, t1.id
        |FROM T1 AS t1 JOIN T2 AS t2 ON
        |t1.key = t2.key AND
        |t2.rt = t1.rt
      """.stripMargin

    val data1 = new mutable.MutableList[(Int, Long, String, Long)]

    data1.+=((4, 4000L, "A", 4000L))
    data1.+=((5, 5000L, "A", 5000L))
    data1.+=((6, 6000L, "A", 6000L))
    data1.+=((6, 6000L, "B", 6000L))

    val data2 = new mutable.MutableList[(String, String, Long)]
    data2.+=(("A", "R-5", 5000L))
    data2.+=(("B", "R-6", 6000L))

    val t1 = env.fromCollection(data1)
      .assignTimestampsAndWatermarks(new Row4WatermarkExtractor)
      .toTable(tEnv, 'id, 'tm, 'key, 'rt.rowtime)
    val t2 = env.fromCollection(data2)
      .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rt.rowtime)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sink = new TestingAppendSink
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(sink)
    env.execute()

    val expected = mutable.MutableList[String](
      "A,R-5,5",
      "B,R-6,6"
    )

    assertEquals(expected.toList.sorted, sink.getAppendResults.sorted)
  }

  /** test rowtime inner join with other conditions **/
  @Test
  def testRowTimeInnerJoinWithOtherConditions(): Unit = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_MINIBATCH_JOIN_ENABLED, false)
    tEnv.getConfig.getConf.remove(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY)

    val sqlQuery =
      """
        |SELECT t2.a, t1.c, t2.c
        |FROM T1 as t1 JOIN T2 as t2 ON
        |  t1.a = t2.a AND
        |  t1.rt > t2.rt - INTERVAL '5' SECOND AND
        |    t1.rt < t2.rt - INTERVAL '1' SECOND AND
        |  t1.b < t2.b AND
        |  t1.b > 2
        |""".stripMargin

    val data1 = new mutable.MutableList[(Int, Long, String, Long)]
    data1.+=((1, 4L, "LEFT1", 1000L))
    // for boundary test
    data1.+=((1, 8L, "LEFT1.1", 1001L))
    // predicate (t1.b > 2) push down
    data1.+=((1, 2L, "LEFT2", 2000L))
    data1.+=((1, 7L, "LEFT3", 3000L))
    data1.+=((2, 5L, "LEFT4", 4000L))
    // for boundary test
    data1.+=((1, 4L, "LEFT4.9", 4999L))
    data1.+=((1, 4L, "LEFT5", 5000L))
    data1.+=((1, 10L, "LEFT6", 6000L))

    val data2 = new mutable.MutableList[(Int, Long, String, Long)]
    // just for watermark
    data2.+=((1, 1L, "RIGHT1", 1000L))
    data2.+=((1, 9L, "RIGHT6", 6000L))
    data2.+=((2, 14L, "RIGHT7", 7000L))
    data2.+=((1, 4L, "RIGHT8", 8000L))

    val t1 = env.fromCollection(data1)
      .assignTimestampsAndWatermarks(new Row4WatermarkExtractor)
      .toTable(tEnv, 'a, 'b, 'c, 'rt.rowtime)
    val t2 = env.fromCollection(data2)
      .assignTimestampsAndWatermarks(new Row4WatermarkExtractor)
      .toTable(tEnv, 'a, 'b, 'c, 'rt.rowtime)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)
    val sink = new TestingAppendSink
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(sink)
    env.execute()

    // There may be two expected results according to the process order.
    val expected = mutable.MutableList[String]("1,LEFT3,RIGHT6",
      "1,LEFT1.1,RIGHT6",
      "2,LEFT4,RIGHT7",
      "1,LEFT4.9,RIGHT6")
    assertEquals(expected.toList.sorted, sink.getAppendResults.sorted)
  }

  /** test rowtime inner join with another time condition **/
  @Test
  def testRowTimeInnerJoinWithOtherTimeCondition(): Unit = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_MINIBATCH_JOIN_ENABLED, false)
    tEnv.getConfig.getConf.remove(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY)

    val sqlQuery =
      """
        |SELECT t2.a, t1.c, t2.c
        |FROM T1 as t1 JOIN T2 as t2 ON
        |  t1.a = t2.a AND
        |  t1.rt > t2.rt - INTERVAL '4' SECOND AND
        |    t1.rt < t2.rt AND
        |  QUARTER(t1.rt) = t2.a
        |""".stripMargin

    val data1 = new mutable.MutableList[(Int, Long, String, Long)]
    data1.+=((1, 4L, "LEFT1", 1000L))
    data1.+=((1, 2L, "LEFT2", 2000L))
    data1.+=((1, 7L, "LEFT3", 3000L))
    data1.+=((2, 5L, "LEFT4", 4000L))
    data1.+=((1, 4L, "LEFT5", 5000L))
    data1.+=((1, 10L, "LEFT6", 6000L))

    val data2 = new mutable.MutableList[(Int, Long, String, Long)]
    data2.+=((1, 1L, "RIGHT1", 1000L))
    data2.+=((1, 9L, "RIGHT6", 6000L))
    data2.+=((2, 8, "RIGHT7", 7000L))
    data2.+=((1, 4L, "RIGHT8", 8000L))

    val t1 = env.fromCollection(data1)
      .assignTimestampsAndWatermarks(new Row4WatermarkExtractor)
      .toTable(tEnv, 'a, 'b, 'c, 'rt.rowtime)
    val t2 = env.fromCollection(data2)
      .assignTimestampsAndWatermarks(new Row4WatermarkExtractor)
      .toTable(tEnv, 'a, 'b, 'c, 'rt.rowtime)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)
    val sink = new TestingAppendSink
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(sink)
    env.execute()

    val expected = mutable.MutableList[String](
      "1,LEFT3,RIGHT6",
      "1,LEFT5,RIGHT6",
      "1,LEFT5,RIGHT8",
      "1,LEFT6,RIGHT8")

    assertEquals(expected.toList.sorted, sink.getAppendResults.sorted)
  }

  /** test rowtime inner join with window aggregation **/
  @Test
  def testRowTimeInnerJoinWithWindowAggregateOnFirstTime(): Unit = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_MINIBATCH_JOIN_ENABLED, false)
    tEnv.getConfig.getConf.remove(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY)

    val sqlQuery =
      """
        |SELECT t1.key, TUMBLE_END(t1.rt, INTERVAL '4' SECOND), COUNT(t2.key)
        |FROM T1 AS t1 join T2 AS t2 ON
        |  t1.key = t2.key AND
        |  t1.rt BETWEEN t2.rt - INTERVAL '5' SECOND AND
        |    t2.rt + INTERVAL '5' SECOND
        |GROUP BY TUMBLE(t1.rt, INTERVAL '4' SECOND), t1.key
        |""".stripMargin

    val data1 = new mutable.MutableList[(String, String, Long)]
    data1.+=(("A", "L-1", 1000L)) // no joining record
    data1.+=(("A", "L-2", 2000L)) // 1 joining record
    data1.+=(("A", "L-3", 3000L)) // 2 joining records
    //data1.+=(("B", "L-8", 2000L))  // 1 joining records
    data1.+=(("B", "L-4", 4000L)) // 1 joining record
    data1.+=(("C", "L-5", 4000L)) // no joining record
    data1.+=(("A", "L-6", 10000L)) // 2 joining records
    data1.+=(("A", "L-7", 13000L)) // 1 joining record

    val data2 = new mutable.MutableList[(String, String, Long)]
    data2.+=(("A", "R-1", 7000L)) // 3 joining records
    data2.+=(("B", "R-4", 7000L)) // 1 joining records
    data2.+=(("A", "R-3", 8000L)) // 3 joining records
    data2.+=(("D", "R-2", 8000L)) // no joining record

    val t1 = env.fromCollection(data1)
      .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rt.rowtime)
    val t2 = env.fromCollection(data2)
      .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rt.rowtime)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sink = new TestingAppendSink
    val t_r = tEnv.sqlQuery(sqlQuery)
    val result = t_r.toAppendStream[Row]
    result.addSink(sink)
    env.execute()
    val expected = mutable.MutableList[String](
      "A,1970-01-01 00:00:04.0,3",
      "A,1970-01-01 00:00:12.0,2",
      "A,1970-01-01 00:00:16.0,1",
      //"B,1970-01-01 00:00:04.0,1",
      "B,1970-01-01 00:00:08.0,1")
    assertEquals(expected.toList.sorted, sink.getAppendResults.sorted)
  }

  /** test row time inner join with window aggregation **/
  @Test
  def testRowTimeInnerJoinWithWindowAggregateOnSecondTime(): Unit = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_MINIBATCH_JOIN_ENABLED, false)
    tEnv.getConfig.getConf.remove(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY)

    val sqlQuery =
      """
        |SELECT t2.key, TUMBLE_END(t2.rt, INTERVAL '4' SECOND), COUNT(t1.key)
        |FROM T1 AS t1 join T2 AS t2 ON
        | t1.key = t2.key AND
        | t1.rt BETWEEN t2.rt - INTERVAL '5' SECOND AND
        | t2.rt + INTERVAL '5' SECOND
        | GROUP BY TUMBLE(t2.rt, INTERVAL '4' SECOND), t2.key
      """.stripMargin

    val data1 = new mutable.MutableList[(String, String, Long)]
    data1.+=(("A", "L-1", 1000L)) // no joining record
    data1.+=(("A", "L-2", 2000L)) // 1 joining record
    data1.+=(("A", "L-3", 3000L)) // 2 joining records
    data1.+=(("B", "L-4", 4000L)) // 1 joining record
    data1.+=(("C", "L-5", 4000L)) // no joining record
    data1.+=(("A", "L-6", 10000L)) // 2 joining records
    data1.+=(("A", "L-7", 13000L)) // 1 joining record

    val data2 = new mutable.MutableList[(String, String, Long)]
    data2.+=(("A", "R-1", 7000L)) // 3 joining records
    data2.+=(("B", "R-4", 7000L)) // 1 joining records
    data2.+=(("A", "R-3", 8000L)) // 3 joining records
    data2.+=(("D", "R-2", 8000L)) // no joining record

    val t1 = env.fromCollection(data1)
      .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rt.rowtime)
    val t2 = env.fromCollection(data2)
      .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rt.rowtime)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sink = new TestingAppendSink
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(sink)
    env.execute()
    val expected = mutable.MutableList[String](
      "A,1970-01-01 00:00:08.0,3",
      "A,1970-01-01 00:00:12.0,3",
      "B,1970-01-01 00:00:08.0,1")
    assertEquals(expected.toList.sorted, sink.getAppendResults.sorted)
  }

  /** Tests for left outer join **/
  @Test
  def testProcTimeLeftOuterJoin(): Unit = {
    env.setParallelism(1)

    val sqlQuery =
      """
        |SELECT t2.a, t2.c, t1.c
        |FROM T1 AS t1 LEFT OUTER JOIN T2 AS t2 ON
        | t1.a = t2.a AND
        | t1.proctime BETWEEN t2.proctime - INTERVAL '5' SECOND AND
        | t2.proctime + INTERVAL '3' SECOND
      """.stripMargin

    val data1 = new mutable.MutableList[(Int, Long, String)]
    data1.+=((1, 1L, "Hi1"))
    data1.+=((1, 2L, "Hi2"))
    data1.+=((1, 5L, "Hi3"))
    data1.+=((2, 7L, "Hi5"))

    val data2 = new mutable.MutableList[(Int, Long, String)]
    data2.+=((1, 1L, "HiHi"))
    data2.+=((2, 2L, "HeHe"))

    val t1 = env.fromCollection(data1)
      .toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)
      .select('a, 'b, 'c, 'proctime)
    val t2 = env.fromCollection(data2)
      .toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)
      .select('a, 'b, 'c, 'proctime)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sink = new TestingAppendSink
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(sink)
    env.execute()

  }

  /** Tests row time left outer join **/
  @Test
  def testRowTimeLeftOuterJoin(): Unit = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_MINIBATCH_JOIN_ENABLED, false)
    tEnv.getConfig.getConf.remove(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY)

    val sqlQuery =
      """
        |SELECT t1.key, t2.id, t1.id
        |FROM T1 AS t1 LEFT OUTER JOIN  T2 AS t2 ON
        | t1.key = t2.key AND
        | t1.rt BETWEEN t2.rt - INTERVAL '5' SECOND AND
        | t2.rt + INTERVAL '6' SECOND AND
        | t1.id <> 'L-5'
      """.stripMargin

    val data1 = new mutable.MutableList[(String, String, Long)]
    // for boundary test
    data1.+=(("A", "L-1", 1000L))
    data1.+=(("A", "L-2", 2000L))
    data1.+=(("B", "L-4", 4000L))
    data1.+=(("B", "L-5", 5000L))
    data1.+=(("A", "L-6", 6000L))
    data1.+=(("C", "L-7", 7000L))
    data1.+=(("A", "L-10", 10000L))
    data1.+=(("A", "L-12", 12000L))
    data1.+=(("A", "L-20", 20000L))

    val data2 = new mutable.MutableList[(String, String, Long)]
    data2.+=(("A", "R-6", 6000L))
    data2.+=(("B", "R-7", 7000L))
    data2.+=(("D", "R-8", 8000L))
    data2.+=(("A", "R-11", 11000L))

    val t1 = env.fromCollection(data1)
      .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rt.rowtime)

    val t2 = env.fromCollection(data2)
      .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rt.rowtime)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sink = new TestingAppendSink
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(sink)
    env.execute()
    val expected = mutable.MutableList[String](
      "A,R-6,L-1",
      "A,R-6,L-2",
      "A,R-6,L-6",
      "A,R-6,L-10",
      "A,R-6,L-12",
      "B,R-7,L-4",
      "A,R-11,L-6",
      "A,R-11,L-10",
      "A,R-11,L-12",
      "B,null,L-5",
      "C,null,L-7",
      "A,null,L-20")

    assertEquals(expected.toList.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testRowTimeLeftOuterJoinNegativeWindowSize(): Unit = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_MINIBATCH_JOIN_ENABLED, false)
    tEnv.getConfig.getConf.remove(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY)

    val sqlQuery =
      """
        |SELECT t2.key, t2.id, t1.id
        |FROM T1 AS t1 LEFT OUTER JOIN T2 AS t2 ON
        | t1.key = t2.key AND
        |  t1.rt BETWEEN t2.rt + INTERVAL '3' SECOND AND
        |  t2.rt + INTERVAL '1' SECOND
      """.stripMargin

    val data1 = new mutable.MutableList[(String, String, Long)]
    // for boundary test
    data1.+=(("A", "L-1", 1000L))
    data1.+=(("B", "L-4", 4000L))
    data1.+=(("C", "L-7", 7000L))

    val data2 = new mutable.MutableList[(String, String, Long)]
    data2.+=(("A", "R-6", 6000L))
    data2.+=(("B", "R-7", 7000L))
    data2.+=(("D", "R-8", 8000L))

    val t1 = env.fromCollection(data1)
      .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rt.rowtime)
    val t2 = env.fromCollection(data2)
      .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rt.rowtime)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sink = new TestingAppendSink
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(sink)
    env.execute()

    val expected = mutable.MutableList[String](
      "null,null,L-1",
      "null,null,L-4",
      "null,null,L-7"
    )
    assertEquals(expected.toList.sorted, sink.getAppendResults.sorted)
  }

  //Test for right outer join
  @Test
  def testProcTimeRightOuterJoin(): Unit = {
    env.setParallelism(1)

    val sqlQuery =
      """
        |SELECT t2.a, t2.c, t1.c
        |FROM T1 as t1 RIGHT  OUTER JOIN T2 as t2 ON
        | t1.a = t2.a AND
        | t1.proctime BETWEEN t2.proctime -  INTERVAL '5' SECOND AND
        | t2.proctime + INTERVAL '3' SECOND
      """.stripMargin

    val data1 = new mutable.MutableList[(Int, Long, String)]
    data1.+=((1, 1L, "Hi1"))
    data1.+=((1, 2L, "Hi2"))
    data1.+=((1, 5L, "Hi3"))
    data1.+=((2, 7L, "Hi5"))

    val data2 = new mutable.MutableList[(Int, Long, String)]
    data2.+=((1, 1L, "HiHi"))
    data2.+=((2, 2L, "HeHe"))

    val t1 = env.fromCollection(data1).toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)
      .select('a, 'b, 'c, 'proctime)
    val t2 = env.fromCollection(data2).toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)
      .select('a, 'b, 'c, 'proctime)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sink = new TestingAppendSink
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(sink)
    env.execute()

  }

  @Test
  def testRowTimeRightOuterJoin(): Unit = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_MINIBATCH_JOIN_ENABLED, false)
    tEnv.getConfig.getConf.remove(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY)

    val sqlQuery =
      """
        |SELECT t2.key, t2.id, t1.id
        |FROM T1 AS t1 RIGHT OUTER JOIN T2 AS t2 ON
        | t1.key = t2.key AND
        | t1.rt BETWEEN t2.rt - INTERVAL '5' SECOND AND
        | t2.rt + INTERVAL '6' SECOND AND
        | t2.id <> 'R-5'
      """.stripMargin

    val data1 = new mutable.MutableList[(String, String, Long)]
    // for boundary test
    data1.+=(("A", "L-1", 1000L))
    data1.+=(("A", "L-2", 2000L))
    data1.+=(("B", "L-4", 4000L))
    data1.+=(("A", "L-6", 6000L))
    data1.+=(("C", "L-7", 7000L))
    data1.+=(("A", "L-10", 10000L))
    data1.+=(("A", "L-12", 12000L))

    val data2 = new mutable.MutableList[(String, String, Long)]
    data2.+=(("A", "R-5", 5000L))
    data2.+=(("A", "R-6", 6000L))
    data2.+=(("B", "R-7", 7000L))
    data2.+=(("D", "R-8", 8000L))
    data2.+=(("A", "R-20", 20000L))

    val t1 = env.fromCollection(data1)
      .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rt.rowtime)
    val t2 = env.fromCollection(data2)
      .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rt.rowtime)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sink = new TestingAppendSink
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(sink)
    env.execute()
    val expected = mutable.MutableList[String](
      "A,R-5,null",
      "A,R-6,L-1",
      "A,R-6,L-2",
      "A,R-6,L-6",
      "A,R-6,L-10",
      "A,R-6,L-12",
      "A,R-20,null",
      "B,R-7,L-4",
      "D,R-8,null"
    )

    assertEquals(expected.toList.sorted, sink.getAppendResults.sorted)

  }

  @Test
  def testRowTimeRightOuterJoinNegativeWindowSize(): Unit = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_MINIBATCH_JOIN_ENABLED, false)
    tEnv.getConfig.getConf.remove(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY)

    val sqlQuery =
      """
        |SELECT t2.key, t2.id, t1.id
        |FROM T1 AS t1 RIGHT OUTER JOIN T2 AS t2 ON
        |t1.key = t2.key AND
        |t1.rt BETWEEN t2.rt + INTERVAL '5' SECOND AND
        |t2.rt + INTERVAL '1' SECOND
      """.stripMargin

    val data1 = new mutable.MutableList[(String, String, Long)]
    // for boundary test
    data1.+=(("A", "L-1", 1000L))
    data1.+=(("B", "L-4", 4000L))
    data1.+=(("C", "L-7", 7000L))

    val data2 = new mutable.MutableList[(String, String, Long)]
    data2.+=(("A", "R-6", 6000L))
    data2.+=(("B", "R-7", 7000L))
    data2.+=(("D", "R-8", 8000L))

    val t1 = env.fromCollection(data1)
      .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rt.rowtime)
    val t2 = env.fromCollection(data2)
      .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rt.rowtime)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sink = new TestingAppendSink
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(sink)
    env.execute()

    val expected = mutable.MutableList[String](
      "A,R-6,null",
      "B,R-7,null",
      "D,R-8,null"
    )
    assertEquals(expected.toList.sorted, sink.getAppendResults.sorted)
  }

  //Tests for full outer join
  @Test
  def testProcTimeFullOuterJoin(): Unit = {
    env.setParallelism(1)

    val sqlQuery =
      """
        |SELECT t2.a, t2.c, t1.c
        |FROM T1 as t1 FULL OUTER JOIN T2 as t2 ON
        |t1.a = t2.a AND
        |t1.proctime BETWEEN t2.proctime -  INTERVAL '5' SECOND AND
        |t2.proctime
      """.stripMargin

    val data1 = new mutable.MutableList[(Int, Long, String)]
    data1.+=((1, 1L, "Hi1"))
    data1.+=((1, 2L, "Hi2"))
    data1.+=((1, 5L, "Hi3"))
    data1.+=((2, 7L, "Hi5"))

    val data2 = new mutable.MutableList[(Int, Long, String)]
    data2.+=((1, 1L, "HiHi"))
    data2.+=((2, 2L, "HeHe"))

    val t1 = env.fromCollection(data1).toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)
      .select('a, 'b, 'c, 'proctime)
    val t2 = env.fromCollection(data2).toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)
      .select('a, 'b, 'c, 'proctime)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sink = new TestingAppendSink
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(sink)
    env.execute()
  }

  @Test
  def testRowTimeFullOuterJoin(): Unit = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_MINIBATCH_JOIN_ENABLED, false)
    tEnv.getConfig.getConf.remove(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY)

    val sqlQuery =
      """
        |SELECT t2.key, t2.id, t1.id
        |FROM T1 AS t1 FULL OUTER JOIN T2 AS t2 ON
        |t1.key = t2.key AND
        |t1.rt BETWEEN t2.rt - INTERVAL '5' SECOND AND
        |t2.rt + INTERVAL '6' SECOND AND
        |NOT (t1.id = 'L-5' OR t2.id = 'R-5')
      """.stripMargin

    val data1 = new mutable.MutableList[(String, String, Long)]
    // for boundary test
    data1.+=(("A", "L-1", 1000L))
    data1.+=(("A", "L-2", 2000L))
    data1.+=(("B", "L-4", 4000L))
    data1.+=(("B", "L-5", 5000L))
    data1.+=(("A", "L-6", 6000L))
    data1.+=(("C", "L-7", 7000L))
    data1.+=(("A", "L-10", 10000L))
    data1.+=(("A", "L-12", 12000L))
    data1.+=(("A", "L-20", 20000L))

    val data2 = new mutable.MutableList[(String, String, Long)]
    data2.+=(("A", "R-5", 5000L))
    data2.+=(("A", "R-6", 6000L))
    data2.+=(("B", "R-7", 7000L))
    data2.+=(("D", "R-8", 8000L))

    val t1 = env.fromCollection(data1)
      .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rt.rowtime)
    val t2 = env.fromCollection(data2)
      .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rt.rowtime)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sink = new TestingAppendSink
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(sink)
    env.execute()

    val expected = mutable.MutableList[String](
      "A,R-6,L-1",
      "A,R-6,L-2",
      "A,R-6,L-6",
      "A,R-6,L-10",
      "A,R-6,L-12",
      "B,R-7,L-4",
      "A,R-5,null",
      "D,R-8,null",
      "null,null,L-5",
      "null,null,L-7",
      "null,null,L-20"
    )
    assertEquals(expected.toList.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testRowTimeFullOuterJoinNegativeWindowSize(): Unit = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_MINIBATCH_JOIN_ENABLED, false)
    tEnv.getConfig.getConf.remove(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY)

    val sqlQuery =
      """
        |SELECT t2.key, t2.id, t1.id
        |FROM T1 AS t1 FULL OUTER JOIN T2 AS t2 ON
        |t1.key = t2.key AND
        |t1.rt BETWEEN t2.rt + INTERVAL '5' SECOND AND
        |t2.rt + INTERVAL '4' SECOND
      """.stripMargin

    val data1 = new mutable.MutableList[(String, String, Long)]
    // for boundary test
    data1.+=(("A", "L-1", 1000L))
    data1.+=(("B", "L-4", 4000L))
    data1.+=(("C", "L-7", 7000L))

    val data2 = new mutable.MutableList[(String, String, Long)]
    data2.+=(("A", "R-6", 6000L))
    data2.+=(("B", "R-7", 7000L))
    data2.+=(("D", "R-8", 8000L))

    val t1 = env.fromCollection(data1)
      .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rt.rowtime)
    val t2 = env.fromCollection(data2)
      .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rt.rowtime)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sink = new TestingAppendSink
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(sink)
    env.execute()

    val expected = mutable.MutableList[String](
      "null,null,L-1",
      "null,null,L-4",
      "null,null,L-7",
      "A,R-6,null",
      "B,R-7,null",
      "D,R-8,null"
    )
    assertEquals(expected.toList.sorted, sink.getAppendResults.sorted)
  }

  /** test non-window inner join **/
  @Test
  def testNonWindowInnerJoin(): Unit = {
    val data1 = new mutable.MutableList[(Int, Long, String)]
    data1.+=((1, 1L, "Hi1"))
    data1.+=((1, 2L, "Hi2"))
    data1.+=((1, 2L, "Hi2"))
    data1.+=((1, 5L, "Hi3"))
    data1.+=((2, 7L, "Hi5"))
    data1.+=((1, 9L, "Hi6"))
    data1.+=((1, 8L, "Hi8"))
    data1.+=((3, 8L, "Hi9"))

    val data2 = new mutable.MutableList[(Int, Long, String)]
    data2.+=((1, 1L, "HiHi"))
    data2.+=((2, 2L, "HeHe"))
    data2.+=((3, 2L, "HeHe"))

    val t1 = failingDataSource(data1).toTable(tEnv, 'a, 'b, 'c)
      .select(('a === 3) ? (Null(DataTypes.INT), 'a) as 'a, 'b, 'c)
    val t2 = failingDataSource(data2).toTable(tEnv, 'a, 'b, 'c)
      .select(('a === 3) ? (Null(DataTypes.INT), 'a) as 'a, 'b, 'c)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sqlQuery =
      """
        |SELECT t2.a, t2.c, t1.c
        |FROM T1 as t1 JOIN T2 as t2 ON
        |  t1.a = t2.a AND
        |  t1.b > t2.b
        |""".stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sqlQuery).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      "1,HiHi,Hi2",
      "1,HiHi,Hi2",
      "1,HiHi,Hi3",
      "1,HiHi,Hi6",
      "1,HiHi,Hi8",
      "2,HeHe,Hi5")

    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testIsNullInnerJoinWithNullCond(): Unit = {
    val data1 = new mutable.MutableList[(Int, Long, String)]
    data1.+=((1, 1L, "Hi1"))
    data1.+=((1, 2L, "Hi2"))
    data1.+=((1, 2L, "Hi2"))
    data1.+=((1, 5L, "Hi3"))
    data1.+=((2, 7L, "Hi5"))
    data1.+=((1, 9L, "Hi6"))
    data1.+=((1, 8L, "Hi8"))
    data1.+=((3, 8L, "Hi9"))

    val data2 = new mutable.MutableList[(Int, Long, String)]
    data2.+=((1, 1L, "HiHi"))
    data2.+=((2, 2L, "HeHe"))
    data2.+=((3, 2L, "HeHe"))

    val t1 = failingDataSource(data1).toTable(tEnv, 'a, 'b, 'c)
      .select(('a === 3) ? (Null(DataTypes.INT), 'a) as 'a, 'b, 'c)
    val t2 = failingDataSource(data2).toTable(tEnv, 'a, 'b, 'c)
      .select(('a === 3) ? (Null(DataTypes.INT), 'a) as 'a, 'b, 'c)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sqlQuery =
      """
        |SELECT t2.a, t2.c, t1.c
        |FROM T1 as t1 JOIN T2 as t2 ON
        |  ((t1.a is null AND t2.a is null) OR
        |  (t1.a = t2.a))
        |  AND t1.b > t2.b
        |""".stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sqlQuery).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      "1,HiHi,Hi2",
      "1,HiHi,Hi2",
      "1,HiHi,Hi3",
      "1,HiHi,Hi6",
      "1,HiHi,Hi8",
      "2,HeHe,Hi5",
      "null,HeHe,Hi9")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testJoin(): Unit = {

    val sqlQuery = "SELECT c, g FROM Table3, Table5 WHERE b = e"

    val ds1 = failingDataSource(StreamTestData.getSmall3TupleData)
      .toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(StreamTestData.get5TupleData)
      .toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    tEnv.registerTable("Table3", ds1)
    tEnv.registerTable("Table5", ds2)

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testInnerJoin(): Unit = {
    val ds1 = failingDataSource(data2).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(data).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    tEnv.registerTable("ds1", ds1)
    tEnv.registerTable("ds2", ds2)
    val query = "SELECT b, c, e, g FROM ds1 JOIN ds2 ON b = e"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(query).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq("1,Hi,1,Hallo", "2,Hello world,2,Hallo Welt", "2,Hello,2,Hallo Welt")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testInnerJoin2(): Unit = {
    val query = "SELECT a1, b1 FROM A JOIN B ON a1 = b1"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("3,3", "1,1", "3,3", "2,2", "3,3", "2,2")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testJoinWithFilter(): Unit = {
    val sqlQuery = "SELECT c, g FROM Table3, Table5 WHERE b = e AND b < 2"

    val ds1 = failingDataSource(StreamTestData.getSmall3TupleData)
      .toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(StreamTestData.get5TupleData)
      .toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    tEnv.registerTable("Table3", ds1)
    tEnv.registerTable("Table5", ds2)

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("Hi,Hallo")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testInnerJoinWithNonEquiJoinPredicate(): Unit = {
    val sqlQuery = "SELECT c, g FROM Table3, Table5 WHERE b = e AND a < 6 AND h < b"

    val ds1 = failingDataSource(StreamTestData.get3TupleData)
      .toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(StreamTestData.get5TupleData)
      .toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    tEnv.registerTable("Table3", ds1)
    tEnv.registerTable("Table5", ds2)

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("Hello world, how are you?,Hallo Welt wie", "I am fine.,Hallo Welt wie")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testJoinWithMultipleKeys(): Unit = {
    val sqlQuery = "SELECT c, g FROM Table3, Table5 WHERE a = d AND b = h"

    val ds1 = failingDataSource(StreamTestData.get3TupleData)
      .toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(StreamTestData.get5TupleData)
      .toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    tEnv.registerTable("Table3", ds1)
    tEnv.registerTable("Table5", ds2)

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt wie gehts?", "Hello world,ABC",
      "I am fine.,HIJ", "I am fine.,IJK")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testJoinWithAlias(): Unit = {
    val sqlQuery =
      "SELECT Table5.c, T.`1-_./Ü` FROM (SELECT a, b, c AS `1-_./Ü` FROM Table3) AS T, Table5 " +
        "WHERE a = d AND a < 4"

    val ds1 = failingDataSource(StreamTestData.get3TupleData)
      .toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(StreamTestData.get5TupleData)
      .toTable(tEnv, 'd, 'e, 'f, 'g, 'c)
    tEnv.registerTable("Table3", ds1)
    tEnv.registerTable("Table5", ds2)

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,Hi", "2,Hello", "1,Hello",
      "2,Hello world", "2,Hello world", "3,Hello world")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testDataStreamJoinWithAggregation(): Unit = {
    val sqlQuery = "SELECT COUNT(g), COUNT(b) FROM Table3, Table5 WHERE a = d"

    val ds1 = failingDataSource(StreamTestData.getSmall3TupleData)
      .toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(StreamTestData.get5TupleData)
      .toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    tEnv.registerTable("Table3", ds1)
    tEnv.registerTable("Table5", ds2)

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("6,6")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftOuterJoin(): Unit = {
    val ds1 = failingDataSource(data2).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(dataCannotBeJoin).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    tEnv.registerTable("ds1", ds1)
    tEnv.registerTable("ds2", ds2)
    val query = "SELECT b, c, e, g FROM ds1 LEFT OUTER JOIN ds2 ON b = e"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,Hi,null,null", "2,Hello world,null,null", "2,Hello,null,null")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftOuterJoinWithRetraction(): Unit = {
    val ds1 = failingDataSource(data2).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(data).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    tEnv.registerTable("ds1", ds1)
    tEnv.registerTable("ds2", ds2)
    val query = "SELECT b, c, e, g FROM ds1 LEFT OUTER JOIN ds2 ON b = e"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,Hi,1,Hallo", "2,Hello world,2,Hallo Welt", "2,Hello,2,Hallo Welt")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testStreamJoinWithSameRecord(): Unit = {
    val data1 = List(
      (1, 1),
      (1, 1),
      (2, 2),
      (2, 2),
      (3, 3),
      (3, 3),
      (4, 4),
      (4, 4),
      (5, 5),
      (5, 5))

    val data2 = List(
      (1, 1),
      (2, 2),
      (3, 3),
      (4, 4),
      (5, 5),
      (6, 6),
      (7, 7),
      (8, 8),
      (9, 9),
      (10, 10))

    val table1 = failingDataSource(data1).toTable(tEnv, 'pk, 'a)
    val table2 = failingDataSource(data2).toTable(tEnv, 'pk, 'a)
    tEnv.registerTable("ds1", table1)
    tEnv.registerTable("ds2", table2)

    val sql =
      """
        |SELECT
        |  ds1.pk as leftPk,
        |  ds1.a as leftA,
        |  ds2.pk as rightPk,
        |  ds2.a as rightA
        |FROM ds1 JOIN ds2 ON ds1.pk = ds2.pk
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq("1,1,1,1", "1,1,1,1",
      "2,2,2,2", "2,2,2,2",
      "3,3,3,3", "3,3,3,3",
      "4,4,4,4", "4,4,4,4",
      "5,5,5,5", "5,5,5,5")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testFullOuterJoin(): Unit = {
    val sqlQuery = "SELECT c, g FROM Table3 FULL OUTER JOIN Table5 ON b = e"

    val ds1 = failingDataSource(StreamTestData.getSmall3TupleData)
      .toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(StreamTestData.get5TupleData)
      .toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    tEnv.registerTable("Table3", ds1)
    tEnv.registerTable("Table5", ds2)

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt",
      "null,Hallo Welt wie", "null,Hallo Welt wie gehts?", "null,ABC", "null,BCD",
      "null,CDE", "null,DEF", "null,EFG", "null,FGH", "null,GHI", "null,HIJ",
      "null,IJK", "null,JKL", "null,KLM")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftOuterJoin2(): Unit = {
    val sqlQuery = "SELECT c, g FROM Table5 LEFT OUTER JOIN Table3 ON b = e"

    val ds1 = failingDataSource(StreamTestData.getSmall3TupleData)
      .toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(StreamTestData.get5TupleData)
      .toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    tEnv.registerTable("Table3", ds1)
    tEnv.registerTable("Table5", ds2)

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt",
      "null,Hallo Welt wie", "null,Hallo Welt wie gehts?", "null,ABC", "null,BCD",
      "null,CDE", "null,DEF", "null,EFG", "null,FGH", "null,GHI", "null,HIJ",
      "null,IJK", "null,JKL", "null,KLM")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testRightOuterJoin(): Unit = {
    val sqlQuery = "SELECT c, g FROM Table3 RIGHT OUTER JOIN Table5 ON b = e"

    val ds1 = failingDataSource(StreamTestData.getSmall3TupleData)
      .toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(StreamTestData.get5TupleData)
      .toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    tEnv.registerTable("Table3", ds1)
    tEnv.registerTable("Table5", ds2)

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt",
      "null,Hallo Welt wie", "null,Hallo Welt wie gehts?", "null,ABC", "null,BCD",
      "null,CDE", "null,DEF", "null,EFG", "null,FGH", "null,GHI", "null,HIJ",
      "null,IJK", "null,JKL", "null,KLM")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }



  @Test
  def testInnerJoinWithEqualPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, b1 FROM ($query1) JOIN ($query2) ON a1 = b1"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,1", "2,2", "3,3")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testInnerJoinWithPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, a2, b1, b2 FROM ($query1) JOIN ($query2) ON a2 = b2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,1,1,1")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftJoinNonEqui(): Unit = {
    val query = "SELECT a1, b1 FROM A LEFT JOIN B ON a1 = b1 AND a2 > b2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("3,null", "1,null", "2,null")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftJoinWithEqualPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, b1 FROM ($query1) LEFT JOIN ($query2) ON a1 = b1 AND a2 > b2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,null", "3,null", "2,null")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftJoinWithRightNotPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query = s"SELECT a1, b1 FROM ($query1) LEFT JOIN B ON a1 = b1 AND a2 > b2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,null", "3,null", "2,null")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftJoinWithPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, a2, b1, b2 FROM ($query1) LEFT JOIN ($query2) ON a2 = b2 AND a1 > b1"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,1,null,null", "3,2,null,null", "2,2,null,null")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftJoin(): Unit = {
    val query = "SELECT a1, b1 FROM A LEFT JOIN B ON a1 = b1"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,1", "2,2", "3,3", "2,2", "3,3", "3,3")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftJoinWithEqualPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, b1 FROM ($query1) LEFT JOIN ($query2) ON a1 = b1"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("2,2", "1,1", "3,3")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftJoinWithRightNotPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query = s"SELECT a1, b1 FROM ($query1) LEFT JOIN B ON a1 = b1"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("3,3", "3,3", "3,3", "2,2", "2,2", "1,1")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftJoinWithPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, a2, b1, b2 FROM ($query1) LEFT JOIN ($query2) ON a2 = b2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,1,1,1", "3,2,null,null", "2,2,null,null")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testRightJoinNonEqui(): Unit = {
    val query = "SELECT a1, b1 FROM A RIGHT JOIN B ON a1 = b1 AND a2 > b2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("null,2", "null,1", "null,3", "null,3", "null,2", "null,5", "null,3",
      "null,5", "null,4", "null,5", "null,4", "null,5", "null,4", "null,5", "null,4")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testRightJoinWithEqualPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, b1 FROM ($query1) RIGHT JOIN ($query2) ON a1 = b1 AND a2 > b2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("null,1", "null,3", "null,2", "null,5", "null,4")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testRightJoinWithRightNotPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query = s"SELECT a1, b1 FROM ($query1) RIGHT JOIN B ON a1 = b1 AND a2 > b2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("null,2", "null,1", "null,3", "null,2", "null,3", "null,5", "null,5",
      "null,3", "null,5", "null,5", "null,4", "null,5", "null,4", "null,4", "null,4")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testRightJoinWithPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, a2, b1, b2 FROM ($query1) RIGHT JOIN ($query2) ON a2 = b2 AND a1 > b1"

    env.setParallelism(1)
    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("null,null,3,15", "null,null,4,34", "null,null,2,5", "null,null,5,65",
      "null,null,1,1")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testRightJoin(): Unit = {
    val query = "SELECT a1, b1 FROM A RIGHT JOIN B ON a1 = b1"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("2,2", "3,3", "3,3", "2,2", "3,3", "null,5", "null,4", "1,1", "null,5",
      "null,4", "null,5", "null,5", "null,5", "null,4", "null,4")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testRightJoinWithEqualPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, b1 FROM ($query1) RIGHT JOIN ($query2) ON a1 = b1"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,1", "2,2", "null,5", "3,3", "null,4")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testRightJoinWithRightNotPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query = s"SELECT a1, b1 FROM ($query1) RIGHT JOIN B ON a1 = b1"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("null,4", "null,4", "null,4", "null,4", "null,5", "null,5", "null,5",
      "null,5", "null,5", "1,1", "2,2", "3,3", "3,3", "3,3", "2,2")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testRightJoinWithPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, a2, b1, b2 FROM ($query1) RIGHT JOIN ($query2) ON a2 = b2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("null,null,3,15", "null,null,4,34", "null,null,5,65",
      "1,1,1,1", "null,null,2,5")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testFullJoinNonEqui(): Unit = {
    val query = "SELECT a1, b1 FROM A FULL JOIN B ON a1 = b1 AND a2 > b2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,null", "3,null", "2,null", "null,3", "null,2", "null,2", "null,3",
      "null,5", "null,3", "null,5", "null,4", "null,5", "null,4", "null,1", "null,5", "null,4",
      "null,5", "null,4")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testFullJoinWithEqualPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, b1 FROM ($query1) FULL JOIN ($query2) ON a1 = b1 AND a2 > b2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("null,2", "null,5", "null,3", "null,4", "3,null", "1,null", "null,1", "2," +
      "null")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testFullJoinWithFullNotPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query = s"SELECT a1, b1 FROM ($query1) FULL JOIN B ON a1 = b1 AND a2 > b2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("null,2", "null,1", "null,2", "null,5", "null,5", "null,5", "null,5",
      "null,5", "null,3", "null,3", "null,3", "null,4", "null,4", "null,4", "null,4", "3,null",
      "1,null", "2,null")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testFullJoinWithPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, a2, b1, b2 FROM ($query1) FULL JOIN ($query2) ON a2 = b2 AND a1 > b1"

    env.setParallelism(1)
    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,1,null,null", "null,null,5,65", "null,null,2,5", "2,2,null,null", "3,2," +
      "null,null", "null,null,3,15", "null,null,4,34", "null,null,1,1")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testFullJoin(): Unit = {
    val query = "SELECT a1, b1 FROM A FULL JOIN B ON a1 = b1"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,1", "null,5", "null,5", "null,5", "null,4", "null,5", "null,4", "null," +
      "5", "null,4", "null,4", "2,2", "2,2", "3,3", "3,3", "3,3")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testFullJoinWithEqualPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, b1 FROM ($query1) FULL JOIN ($query2) ON a1 = b1"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("null,4", "1,1", "3,3", "2,2", "null,5")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testFullJoinWithFullNotPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query = s"SELECT a1, b1 FROM ($query1) FULL JOIN B ON a1 = b1"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("null,4", "null,4", "null,4", "null,4", "null,5", "null,5", "null,5",
      "null,5", "null,5", "3,3", "3,3", "3,3", "1,1", "2,2", "2,2")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def FullJoinWithPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, a2, b1, b2 FROM ($query1) FULL JOIN ($query2) ON a2 = b2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("null,null,3,15", "null,null,4,34", "null,null,5,65", "3,2,null,null", "2," +
      "2,null,null", "null,null,2,5", "1,1,1,1")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testNullLeftOuterJoin(): Unit = {
    val data1 = new mutable.MutableList[(Int, Long)]
    data1.+=((1, 1L))
    data1.+=((3, 8L))
    data1.+=((4, 2L))

    val data2 = new mutable.MutableList[(Int, Long)]
    data2.+=((1, 1L))
    data2.+=((2, 2L))
    data2.+=((3, 2L))

    val t1 = failingDataSource(data1).toTable(tEnv, 'a, 'b)
      .select(('a === 3) ? (Null(DataTypes.INT), 'a) as 'a, 'b)
    val t2 = failingDataSource(data2).toTable(tEnv, 'a, 'b)
      .select(('a === 3) ? (Null(DataTypes.INT), 'a) as 'a, 'b)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sqlQuery =
      """
        |SELECT t1.a, t1.b, t2.a, t2.b
        |FROM T1 as t1 LEFT OUTER JOIN T2 as t2 ON t1.a = t2.a
        |""".stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = mutable.MutableList(
      "1,1,1,1",
      "4,2,null,null",
      "null,8,null,null"
    )

    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testNullLeftOuterJoinWithNullCond(): Unit = {
    val data1 = new mutable.MutableList[(Int, Long)]
    data1.+=((1, 1L))
    data1.+=((3, 8L))
    data1.+=((4, 2L))

    val data2 = new mutable.MutableList[(Int, Long)]
    data2.+=((1, 1L))
    data2.+=((2, 2L))
    data2.+=((3, 2L))

    val t1 = failingDataSource(data1).toTable(tEnv, 'a, 'b)
      .select(('a === 3) ? (Null(DataTypes.INT), 'a) as 'a, 'b)
    val t2 = failingDataSource(data2).toTable(tEnv, 'a, 'b)
      .select(('a === 3) ? (Null(DataTypes.INT), 'a) as 'a, 'b)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sqlQuery =
      """
        |SELECT t1.a, t1.b, t2.a, t2.b
        |FROM T1 as t1 LEFT OUTER JOIN T2 as t2 ON t1.a = t2.a
        |OR (t1.a is null AND t2.a is null)
        |""".stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = mutable.MutableList(
      "1,1,1,1",
      "4,2,null,null",
      "null,8,null,2"
    )

    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testNullRightOuterJoin(): Unit = {
    val data1 = new mutable.MutableList[(Int, Long)]
    data1.+=((1, 1L))
    data1.+=((3, 8L))
    data1.+=((4, 2L))

    val data2 = new mutable.MutableList[(Int, Long)]
    data2.+=((1, 1L))
    data2.+=((2, 2L))
    data2.+=((3, 2L))

    val t1 = failingDataSource(data1).toTable(tEnv, 'a, 'b)
      .select(('a === 3) ? (Null(DataTypes.INT), 'a) as 'a, 'b)
    val t2 = failingDataSource(data2).toTable(tEnv, 'a, 'b)
      .select(('a === 3) ? (Null(DataTypes.INT), 'a) as 'a, 'b)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sqlQuery =
      """
        |SELECT t1.a, t1.b, t2.a, t2.b
        |FROM T1 as t1 RIGHT OUTER JOIN T2 as t2 ON t1.a = t2.a
        |""".stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = mutable.MutableList(
      "1,1,1,1",
      "null,null,2,2",
      "null,null,null,2"
    )

    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testNullRightOuterJoinWithNullCond(): Unit = {
    val data1 = new mutable.MutableList[(Int, Long)]
    data1.+=((1, 1L))
    data1.+=((3, 8L))
    data1.+=((4, 2L))

    val data2 = new mutable.MutableList[(Int, Long)]
    data2.+=((1, 1L))
    data2.+=((2, 2L))
    data2.+=((3, 2L))

    val t1 = failingDataSource(data1).toTable(tEnv, 'a, 'b)
      .select(('a === 3) ? (Null(DataTypes.INT), 'a) as 'a, 'b)
    val t2 = failingDataSource(data2).toTable(tEnv, 'a, 'b)
      .select(('a === 3) ? (Null(DataTypes.INT), 'a) as 'a, 'b)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sqlQuery =
      """
        |SELECT t1.a, t1.b, t2.a, t2.b
        |FROM T1 as t1 RIGHT OUTER JOIN T2 as t2 ON t1.a = t2.a
        |OR (t1.a is null AND t2.a is null)
        |""".stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = mutable.MutableList(
      "1,1,1,1",
      "null,null,2,2",
      "null,8,null,2"
    )

    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testNullFullOuterJoin(): Unit = {
    val data1 = new mutable.MutableList[(Int, Long)]
    data1.+=((1, 1L))
    data1.+=((3, 8L))
    data1.+=((4, 2L))

    val data2 = new mutable.MutableList[(Int, Long)]
    data2.+=((1, 1L))
    data2.+=((2, 2L))
    data2.+=((3, 2L))

    val t1 = failingDataSource(data1).toTable(tEnv, 'a, 'b)
      .select(('a === 3) ? (Null(DataTypes.INT), 'a) as 'a, 'b)
    val t2 = failingDataSource(data2).toTable(tEnv, 'a, 'b)
      .select(('a === 3) ? (Null(DataTypes.INT), 'a) as 'a, 'b)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sqlQuery =
      """
        |SELECT t1.a, t1.b, t2.a, t2.b
        |FROM T1 as t1 FULL OUTER JOIN T2 as t2 ON t1.a = t2.a
        |""".stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = mutable.MutableList(
      "1,1,1,1",
      "null,null,2,2",
      "4,2,null,null",
      "null,8,null,null",
      "null,null,null,2"
    )

    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testNullFullOuterJoinWithNullCond(): Unit = {
    val data1 = new mutable.MutableList[(Int, Long)]
    data1.+=((1, 1L))
    data1.+=((3, 8L))
    data1.+=((4, 2L))

    val data2 = new mutable.MutableList[(Int, Long)]
    data2.+=((1, 1L))
    data2.+=((2, 2L))
    data2.+=((3, 2L))

    val t1 = failingDataSource(data1).toTable(tEnv, 'a, 'b)
      .select(('a === 3) ? (Null(DataTypes.INT), 'a) as 'a, 'b)
    val t2 = failingDataSource(data2).toTable(tEnv, 'a, 'b)
      .select(('a === 3) ? (Null(DataTypes.INT), 'a) as 'a, 'b)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sqlQuery =
      """
        |SELECT t1.a, t1.b, t2.a, t2.b
        |FROM T1 as t1 FULL OUTER JOIN T2 as t2 ON t1.a = t2.a
        |OR (t1.a is null AND t2.a is null)
        |""".stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = mutable.MutableList(
      "1,1,1,1",
      "null,null,2,2",
      "4,2,null,null",
      "null,8,null,2"
    )

    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testJoinWithoutWatermark(): Unit = {
    // NOTE: Different from AggregateITCase, we do not set stream time characteristic
    // of environment to event time, so that emitWatermark() actually does nothing.
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val data1 = new mutable.MutableList[(Int, Long)]
    data1 .+= ((1, 1L))
    data1 .+= ((2, 2L))
    data1 .+= ((3, 3L))
    val data2 = new mutable.MutableList[(Int, Long)]
    data2 .+= ((1, -1L))
    data2 .+= ((2, -2L))
    data2 .+= ((3, -3L))

    val t1 = failingDataSource(data1).toTable(tEnv, 'a, 'b)
    tEnv.registerTable("T1", t1)
    val t2 = failingDataSource(data2).toTable(tEnv, 'a, 'c)
    tEnv.registerTable("T2", t2)

    val t3 = tEnv.sqlQuery(
      "select T1.a, b, c from T1, T2 WHERE T1.a = T2.a")
    val sink = new TestingRetractSink
    t3.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    val expected = List("1,1,-1", "2,2,-2", "3,3,-3")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testBigDataOfJoin(): Unit = {
    env.setParallelism(1)

    val data = new mutable.MutableList[(Int, Long, String)]
    for (i <- 0 until 500) {
      data.+=((i % 10, i, i.toString))
    }

    val t1 = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c)
    val t2 = failingDataSource(data).toTable(tEnv, 'd, 'e, 'f)
    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sql =
      """
        |SELECT COUNT(DISTINCT b) FROM (SELECT b FROM T1, T2 WHERE b = e)
      """.stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List("500")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }
}

private class Row4WatermarkExtractor extends
  AssignerWithPunctuatedWatermarks[(Int, Long, String, Long)] {
  override def checkAndGetNextWatermark(lastElement: (Int, Long, String, Long),
    extractedTimestamp: Long): Watermark = {
    new Watermark(extractedTimestamp - 1)
  }

  override def extractTimestamp(element: (Int, Long, String, Long),
    previousElementTimestamp: Long): Long = {
    element._4
  }
}

private class Row3WatermarkExtractor2 extends
  AssignerWithPunctuatedWatermarks[(String, String, Long)] {
  override def checkAndGetNextWatermark(lastElement: (String, String, Long),
    extractedTimestamp: Long): Watermark = {
    new Watermark(extractedTimestamp - 1)
  }

  override def extractTimestamp(
    element: (String, String, Long),
    previousElementTimestamp: Long): Long = {
    element._3
  }
}
