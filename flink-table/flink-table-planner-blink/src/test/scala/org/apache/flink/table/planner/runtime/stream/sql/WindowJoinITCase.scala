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
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.runtime.utils._
import org.apache.flink.types.Row

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.mutable

@RunWith(classOf[Parameterized])
class WindowJoinITCase(mode: StateBackendMode) extends StreamingWithStateTestBase(mode) {

  // Tests for inner join.
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

    val tmp1 = env.fromCollection(data1).toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)
    tEnv.registerTable("TmpT1", tmp1)
    val subquery1 = "SELECT IF(a = 1, CAST(NULL AS INT), a) as a, b, c, proctime FROM TmpT1"
    val t1 = tEnv.sqlQuery(subquery1)
    tEnv.registerTable("T1", t1)

    val tmp2 = env.fromCollection(data2).toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)
    tEnv.registerTable("TmpT2", tmp2)
    val subquery2 = "SELECT IF(a = 1, CAST(NULL AS INT), a) as a, b, c, proctime FROM TmpT2"
    val t2 = tEnv.sqlQuery(subquery2)
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
    val sqlQuery =
      """
        |SELECT t2.key, t2.id, t1.id
        |FROM T1 as t1 join T2 as t2 ON
        |  t1.key = t2.key AND
        |  t1.rowtime BETWEEN t2.rowtime - INTERVAL '5' SECOND AND
        |    t2.rowtime + INTERVAL '6' SECOND
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

    val t1 = env.fromCollection(data1)
      .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rowtime.rowtime)
    val t2 = env.fromCollection(data2)
      .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rowtime.rowtime)

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

  @Test
  def testUnboundedAggAfterRowtimeInnerJoin(): Unit = {
    val innerSql=
      """
        |SELECT t2.key as key, t2.id as id1, t1.id as id2
        |FROM T1 as t1 join T2 as t2 ON
        |  t1.key = t2.key AND
        |  t1.rowtime BETWEEN t2.rowtime - INTERVAL '5' SECOND AND
        |    t2.rowtime + INTERVAL '6' SECOND
        |""".stripMargin

    val sqlQuery = "SELECT key, COUNT(DISTINCT id1), COUNT(DISTINCT id2) FROM (" +
      innerSql + ") GROUP BY key"

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

    val t1 = env.fromCollection(data1)
      .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rowtime.rowtime)
    val t2 = env.fromCollection(data2)
      .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rowtime.rowtime)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)
    val sink = new TestingRetractSink
    val result = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    result.addSink(sink)
    env.execute()
    val expected = mutable.MutableList("A,1,5", "B,1,1")
    assertEquals(expected, sink.getRetractResults.sorted)
  }

  /** test row time inner join with equi-times **/
  @Test
  def testRowTimeInnerJoinWithEquiTimeAttrs(): Unit = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sqlQuery =
      """
        |SELECT t2.key, t2.id, t1.id
        |FROM T1 AS t1 JOIN T2 AS t2 ON
        |t1.key = t2.key AND
        |t2.rowtime = t1.rowtime
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
      .toTable(tEnv, 'id, 'tm, 'key, 'rowtime)
    val t2 = env.fromCollection(data2)
      .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rowtime)

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
    val sqlQuery =
      """
        |SELECT t2.a, t1.c, t2.c
        |FROM T1 as t1 JOIN T2 as t2 ON
        |  t1.a = t2.a AND
        |  t1.rowtime > t2.rowtime - INTERVAL '5' SECOND AND
        |    t1.rowtime < t2.rowtime - INTERVAL '1' SECOND AND
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
      .toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)
    val t2 = env.fromCollection(data2)
      .assignTimestampsAndWatermarks(new Row4WatermarkExtractor)
      .toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)

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
    val sqlQuery =
      """
        |SELECT t2.a, t1.c, t2.c
        |FROM T1 as t1 JOIN T2 as t2 ON
        |  t1.a = t2.a AND
        |  t1.rowtime > t2.rowtime - INTERVAL '4' SECOND AND
        |    t1.rowtime < t2.rowtime AND
        |  QUARTER(t1.rowtime) = t2.a
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
      .toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)
    val t2 = env.fromCollection(data2)
      .assignTimestampsAndWatermarks(new Row4WatermarkExtractor)
      .toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)

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
    val sqlQuery =
      """
        |SELECT t1.key, TUMBLE_END(t1.rowtime, INTERVAL '4' SECOND), COUNT(t2.key)
        |FROM T1 AS t1 join T2 AS t2 ON
        |  t1.key = t2.key AND
        |  t1.rowtime BETWEEN t2.rowtime - INTERVAL '5' SECOND AND
        |    t2.rowtime + INTERVAL '5' SECOND
        |GROUP BY TUMBLE(t1.rowtime, INTERVAL '4' SECOND), t1.key
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
      .toTable(tEnv, 'key, 'id, 'rowtime.rowtime)
    val t2 = env.fromCollection(data2)
      .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rowtime.rowtime)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sink = new TestingAppendSink
    val t_r = tEnv.sqlQuery(sqlQuery)
    val result = t_r.toAppendStream[Row]
    result.addSink(sink)
    env.execute()
    val expected = mutable.MutableList[String](
      "A,1970-01-01T00:00:04,3",
      "A,1970-01-01T00:00:12,2",
      "A,1970-01-01T00:00:16,1",
      //"B,1970-01-01T00:00:04,1",
      "B,1970-01-01T00:00:08,1")
    assertEquals(expected.toList.sorted, sink.getAppendResults.sorted)
  }

  /** test row time inner join with window aggregation **/
  @Test
  def testRowTimeInnerJoinWithWindowAggregateOnSecondTime(): Unit = {
    val sqlQuery =
      """
        |SELECT t2.key, TUMBLE_END(t2.rowtime, INTERVAL '4' SECOND), COUNT(t1.key)
        |FROM T1 AS t1 join T2 AS t2 ON
        | t1.key = t2.key AND
        | t1.rowtime BETWEEN t2.rowtime - INTERVAL '5' SECOND AND
        | t2.rowtime + INTERVAL '5' SECOND
        | GROUP BY TUMBLE(t2.rowtime, INTERVAL '4' SECOND), t2.key
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
      .toTable(tEnv, 'key, 'id, 'rowtime.rowtime)
    val t2 = env.fromCollection(data2)
      .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rowtime.rowtime)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sink = new TestingAppendSink
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(sink)
    env.execute()
    val expected = mutable.MutableList[String](
      "A,1970-01-01T00:00:08,3",
      "A,1970-01-01T00:00:12,3",
      "B,1970-01-01T00:00:08,1")
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
    val t2 = env.fromCollection(data2)
      .toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)

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
    val sqlQuery =
      """
        |SELECT t1.key, t2.id, t1.id
        |FROM T1 AS t1 LEFT OUTER JOIN  T2 AS t2 ON
        | t1.key = t2.key AND
        | t1.rowtime BETWEEN t2.rowtime - INTERVAL '5' SECOND AND
        | t2.rowtime + INTERVAL '6' SECOND AND
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
      .toTable(tEnv, 'key, 'id, 'rowtime.rowtime)

    val t2 = env.fromCollection(data2)
      .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rowtime.rowtime)

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
    val sqlQuery =
      """
        |SELECT t2.key, t2.id, t1.id
        |FROM T1 AS t1 LEFT OUTER JOIN T2 AS t2 ON
        | t1.key = t2.key AND
        |  t1.rowtime BETWEEN t2.rowtime + INTERVAL '3' SECOND AND
        |  t2.rowtime + INTERVAL '1' SECOND
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
      .toTable(tEnv, 'key, 'id, 'rowtime.rowtime)
    val t2 = env.fromCollection(data2)
      .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rowtime.rowtime)

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
    val t2 = env.fromCollection(data2).toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sink = new TestingAppendSink
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(sink)
    env.execute()

  }

  @Test
  def testRowTimeRightOuterJoin(): Unit = {
    val sqlQuery =
      """
        |SELECT t2.key, t2.id, t1.id
        |FROM T1 AS t1 RIGHT OUTER JOIN T2 AS t2 ON
        | t1.key = t2.key AND
        | t1.rowtime BETWEEN t2.rowtime - INTERVAL '5' SECOND AND
        | t2.rowtime + INTERVAL '6' SECOND AND
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
      .toTable(tEnv, 'key, 'id, 'rowtime.rowtime)
    val t2 = env.fromCollection(data2)
      .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rowtime.rowtime)

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
    val sqlQuery =
      """
        |SELECT t2.key, t2.id, t1.id
        |FROM T1 AS t1 RIGHT OUTER JOIN T2 AS t2 ON
        |t1.key = t2.key AND
        |t1.rowtime BETWEEN t2.rowtime + INTERVAL '5' SECOND AND
        |t2.rowtime + INTERVAL '1' SECOND
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
      .toTable(tEnv, 'key, 'id, 'rowtime.rowtime)
    val t2 = env.fromCollection(data2)
      .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rowtime.rowtime)

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
    val t2 = env.fromCollection(data2).toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sink = new TestingAppendSink
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(sink)
    env.execute()
  }

  @Test
  def testRowTimeFullOuterJoin(): Unit = {
    val sqlQuery =
      """
        |SELECT t2.key, t2.id, t1.id
        |FROM T1 AS t1 FULL OUTER JOIN T2 AS t2 ON
        |t1.key = t2.key AND
        |t1.rowtime BETWEEN t2.rowtime - INTERVAL '5' SECOND AND
        |t2.rowtime + INTERVAL '6' SECOND AND
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
      .toTable(tEnv, 'key, 'id, 'rowtime.rowtime)
    val t2 = env.fromCollection(data2)
      .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rowtime.rowtime)

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
    val sqlQuery =
      """
        |SELECT t2.key, t2.id, t1.id
        |FROM T1 AS t1 FULL OUTER JOIN T2 AS t2 ON
        |t1.key = t2.key AND
        |t1.rowtime BETWEEN t2.rowtime + INTERVAL '5' SECOND AND
        |t2.rowtime + INTERVAL '4' SECOND
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
      .toTable(tEnv, 'key, 'id, 'rowtime.rowtime)
    val t2 = env.fromCollection(data2)
      .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rowtime.rowtime)

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
}

private class Row4WatermarkExtractor
  extends AssignerWithPunctuatedWatermarks[(Int, Long, String, Long)] {

  override def checkAndGetNextWatermark(
      lastElement: (Int, Long, String, Long),
      extractedTimestamp: Long): Watermark = {
    new Watermark(extractedTimestamp - 1)
  }

  override def extractTimestamp(
      element: (Int, Long, String, Long),
      previousElementTimestamp: Long): Long = {
    element._4
  }
}

private class Row3WatermarkExtractor2
  extends AssignerWithPunctuatedWatermarks[(String, String, Long)] {

  override def checkAndGetNextWatermark(
      lastElement: (String, String, Long),
      extractedTimestamp: Long): Watermark = {
    new Watermark(extractedTimestamp - 1)
  }

  override def extractTimestamp(
      element: (String, String, Long),
      previousElementTimestamp: Long): Long = {
    element._3
  }
}
