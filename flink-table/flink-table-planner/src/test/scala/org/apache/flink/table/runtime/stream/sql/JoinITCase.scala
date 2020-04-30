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

import java.util
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.{EnvironmentSettings, Types}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.{StreamITCase, StreamTestData, StreamingWithStateTestBase}
import org.apache.flink.types.Row

import org.junit.Assert.assertEquals
import org.junit._

import scala.collection.mutable

class JoinITCase extends StreamingWithStateTestBase {

  @Before
  def clear(): Unit = {
    StreamITCase.clear
  }

  // Tests for inner join.
  /** test proctime inner join **/
  @Test
  def testProcessTimeInnerJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setStateBackend(getStateBackend)
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
      .select(('a === 1)?(nullOf(Types.INT), 'a) as 'a, 'b, 'c, 'proctime) // test null values
    val t2 = env.fromCollection(data2).toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)
      .select(('a === 1)?(nullOf(Types.INT), 'a) as 'a, 'b, 'c, 'proctime) // test null values

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()
  }

  /** test proctime inner join with other condition **/
  @Test
  def testProcessTimeInnerJoinWithOtherConditions(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setStateBackend(getStateBackend)
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

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    // Assert there is no result with null keys.
    Assert.assertFalse(StreamITCase.testResults.toString().contains("null"))
  }

  /** test rowtime inner join **/
  @Test
  def testRowTimeInnerJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setStateBackend(getStateBackend)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

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

    val t1 = env.fromCollection(data1)
      .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rt.rowtime)
    val t2 = env.fromCollection(data2)
      .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rt.rowtime)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()
    val expected = new java.util.ArrayList[String]
    expected.add("A,RIGHT6,LEFT1")
    expected.add("A,RIGHT6,LEFT2")
    expected.add("A,RIGHT6,LEFT3")
    expected.add("A,RIGHT6,LEFT5")
    expected.add("A,RIGHT6,LEFT6")
    expected.add("B,RIGHT7,LEFT4")
    StreamITCase.compareWithList(expected)
  }

  /** test rowtime inner join with equi-times **/
  @Test
  def testRowTimeInnerJoinWithEquiTimeAttrs(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setStateBackend(getStateBackend)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sqlQuery =
      """
        |SELECT t2.key, t2.id, t1.id
        |FROM T1 as t1 join T2 as t2 ON
        |  t1.key = t2.key AND
        |  t2.rt = t1.rt
        |""".stripMargin

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

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()
    val expected = new java.util.ArrayList[String]
    expected.add("A,R-5,5")
    expected.add("B,R-6,6")
    StreamITCase.compareWithList(expected)
  }

  /** test rowtime inner join with other conditions **/
  @Test
  def testRowTimeInnerJoinWithOtherConditions(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setStateBackend(getStateBackend)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

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

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    // There may be two expected results according to the process order.
    val expected = new util.ArrayList[String]
    expected.add("1,LEFT3,RIGHT6")
    expected.add("1,LEFT1.1,RIGHT6")
    expected.add("2,LEFT4,RIGHT7")
    expected.add("1,LEFT4.9,RIGHT6")
    StreamITCase.compareWithList(expected)
  }

  /** test rowtime inner join with another time condition **/
  @Test
  def testRowTimeInnerJoinWithOtherTimeCondition(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setStateBackend(getStateBackend)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

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

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = new java.util.ArrayList[String]
    expected.add("1,LEFT3,RIGHT6")
    expected.add("1,LEFT5,RIGHT6")
    expected.add("1,LEFT5,RIGHT8")
    expected.add("1,LEFT6,RIGHT8")
    StreamITCase.compareWithList(expected)
  }

  /** test rowtime inner join with window aggregation **/
  @Test
  def testRowTimeInnerJoinWithWindowAggregateOnFirstTime(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setStateBackend(getStateBackend)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

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
    data1.+=(("A", "L-1", 1000L))  // no joining record
    data1.+=(("A", "L-2", 2000L))  // 1 joining record
    data1.+=(("A", "L-3", 3000L))  // 2 joining records
    data1.+=(("B", "L-4", 4000L))  // 1 joining record
    data1.+=(("C", "L-5", 4000L))  // no joining record
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

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()
    val expected = new java.util.ArrayList[String]
    expected.add("A,1970-01-01 00:00:04.0,3")
    expected.add("A,1970-01-01 00:00:12.0,2")
    expected.add("A,1970-01-01 00:00:16.0,1")
    expected.add("B,1970-01-01 00:00:08.0,1")
    StreamITCase.compareWithList(expected)
  }

  /** test rowtime inner join with window aggregation **/
  @Test
  def testRowTimeInnerJoinWithWindowAggregateOnSecondTime(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setStateBackend(getStateBackend)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sqlQuery =
      """
        |SELECT t2.key, TUMBLE_END(t2.rt, INTERVAL '4' SECOND), COUNT(t1.key)
        |FROM T1 AS t1 join T2 AS t2 ON
        |  t1.key = t2.key AND
        |  t1.rt BETWEEN t2.rt - INTERVAL '5' SECOND AND
        |    t2.rt + INTERVAL '5' SECOND
        |GROUP BY TUMBLE(t2.rt, INTERVAL '4' SECOND), t2.key
        |""".stripMargin

    val data1 = new mutable.MutableList[(String, String, Long)]
    data1.+=(("A", "L-1", 1000L))  // no joining record
    data1.+=(("A", "L-2", 2000L))  // 1 joining record
    data1.+=(("A", "L-3", 3000L))  // 2 joining records
    data1.+=(("B", "L-4", 4000L))  // 1 joining record
    data1.+=(("C", "L-5", 4000L))  // no joining record
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

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()
    val expected = new java.util.ArrayList[String]
    expected.add("A,1970-01-01 00:00:08.0,3")
    expected.add("A,1970-01-01 00:00:12.0,3")
    expected.add("B,1970-01-01 00:00:08.0,1")
    StreamITCase.compareWithList(expected)
  }

  // Tests for left outer join
  @Test
  def testProcTimeLeftOuterJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setStateBackend(getStateBackend)
    env.setParallelism(1)

    val sqlQuery =
      """
        |SELECT t2.a, t2.c, t1.c
        |FROM T1 as t1 LEFT OUTER JOIN T2 as t2 ON
        |  t1.a = t2.a AND
        |  t1.proctime BETWEEN t2.proctime - INTERVAL '5' SECOND AND
        |    t2.proctime + INTERVAL '3' SECOND
        |""".stripMargin

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

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()
  }

  @Test
  def testRowTimeLeftOuterJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setStateBackend(getStateBackend)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sqlQuery =
      """
        |SELECT t1.key, t2.id, t1.id
        |FROM T1 AS t1 LEFT OUTER JOIN T2 AS t2 ON
        |  t1.key = t2.key AND
        |  t1.rt BETWEEN t2.rt - INTERVAL '5' SECOND AND
        |    t2.rt + INTERVAL '6' SECOND AND
        |  t1.id <> 'L-5'
        |""".stripMargin

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

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()
    val expected = new java.util.ArrayList[String]
    expected.add("A,R-6,L-1")
    expected.add("A,R-6,L-2")
    expected.add("A,R-6,L-6")
    expected.add("A,R-6,L-10")
    expected.add("A,R-6,L-12")
    expected.add("B,R-7,L-4")
    expected.add("A,R-11,L-6")
    expected.add("A,R-11,L-10")
    expected.add("A,R-11,L-12")
    expected.add("B,null,L-5")
    expected.add("C,null,L-7")
    expected.add("A,null,L-20")
    StreamITCase.compareWithList(expected)
  }

  @Test
  def testRowTimeLeftOuterJoinNegativeWindowSize(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setStateBackend(getStateBackend)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sqlQuery =
      """
        |SELECT t2.key, t2.id, t1.id
        |FROM T1 AS t1 LEFT OUTER JOIN T2 AS t2 ON
        |  t1.key = t2.key AND
        |  t1.rt BETWEEN t2.rt + INTERVAL '3' SECOND AND
        |    t2.rt + INTERVAL '1' SECOND
        |""".stripMargin

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

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()
    val expected = new java.util.ArrayList[String]
    expected.add("null,null,L-1")
    expected.add("null,null,L-4")
    expected.add("null,null,L-7")
    StreamITCase.compareWithList(expected)
  }

  // Tests for right outer join
  @Test
  def testProcTimeRightOuterJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setStateBackend(getStateBackend)
    env.setParallelism(1)

    val sqlQuery =
      """
        |SELECT t2.a, t2.c, t1.c
        |FROM T1 as t1 RIGHT OUTER JOIN T2 as t2 ON
        |  t1.a = t2.a AND
        |  t1.proctime BETWEEN t2.proctime - INTERVAL '5' SECOND AND
        |    t2.proctime + INTERVAL '3' SECOND
        |""".stripMargin

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

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()
  }

  @Test
  def testRowTimeRightOuterJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setStateBackend(getStateBackend)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sqlQuery =
      """
        |SELECT t2.key, t2.id, t1.id
        |FROM T1 AS t1 RIGHT OUTER JOIN T2 AS t2 ON
        |  t1.key = t2.key AND
        |  t1.rt BETWEEN t2.rt - INTERVAL '5' SECOND AND
        |    t2.rt + INTERVAL '6' SECOND AND
        |  t2.id <> 'R-5'
        |""".stripMargin

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

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()
    val expected = new java.util.ArrayList[String]
    expected.add("A,R-5,null")
    expected.add("A,R-6,L-1")
    expected.add("A,R-6,L-2")
    expected.add("A,R-6,L-6")
    expected.add("A,R-6,L-10")
    expected.add("A,R-6,L-12")
    expected.add("A,R-20,null")
    expected.add("B,R-7,L-4")
    expected.add("D,R-8,null")
    StreamITCase.compareWithList(expected)
  }

  @Test
  def testRowTimeRightOuterJoinNegativeWindowSize(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setStateBackend(getStateBackend)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sqlQuery =
      """
        |SELECT t2.key, t2.id, t1.id
        |FROM T1 AS t1 RIGHT OUTER JOIN T2 AS t2 ON
        |  t1.key = t2.key AND
        |  t1.rt BETWEEN t2.rt + INTERVAL '5' SECOND AND
        |    t2.rt + INTERVAL '1' SECOND
        |""".stripMargin

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

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()
    val expected = new java.util.ArrayList[String]
    expected.add("A,R-6,null")
    expected.add("B,R-7,null")
    expected.add("D,R-8,null")
    StreamITCase.compareWithList(expected)
  }

  // Tests for full outer join
  @Test
  def testProcTimeFullOuterJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setStateBackend(getStateBackend)
    env.setParallelism(1)

    val sqlQuery =
      """
        |SELECT t2.a, t2.c, t1.c
        |FROM T1 as t1 Full OUTER JOIN T2 as t2 ON
        |  t1.a = t2.a AND
        |  t1.proctime BETWEEN t2.proctime - INTERVAL '5' SECOND AND
        |    t2.proctime
        |""".stripMargin

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

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()
  }

  @Test
  def testRowTimeFullOuterJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setStateBackend(getStateBackend)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sqlQuery =
      """
        |SELECT t2.key, t2.id, t1.id
        |FROM T1 AS t1 FULL OUTER JOIN T2 AS t2 ON
        |  t1.key = t2.key AND
        |  t1.rt BETWEEN t2.rt - INTERVAL '5' SECOND AND
        |    t2.rt + INTERVAL '6' SECOND AND
        |  NOT (t1.id = 'L-5' OR t2.id = 'R-5')
        |""".stripMargin

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

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()
    val expected = new java.util.ArrayList[String]
    expected.add("A,R-6,L-1")
    expected.add("A,R-6,L-2")
    expected.add("A,R-6,L-6")
    expected.add("A,R-6,L-10")
    expected.add("A,R-6,L-12")
    expected.add("B,R-7,L-4")
    expected.add("A,R-5,null")
    expected.add("D,R-8,null")
    expected.add("null,null,L-5")
    expected.add("null,null,L-7")
    expected.add("null,null,L-20")
    StreamITCase.compareWithList(expected)
  }

  @Test
  def testRowTimeFullOuterJoinNegativeWindowSize(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setStateBackend(getStateBackend)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sqlQuery =
      """
        |SELECT t2.key, t2.id, t1.id
        |FROM T1 AS t1 FULL OUTER JOIN T2 AS t2 ON
        |  t1.key = t2.key AND
        |  t1.rt BETWEEN t2.rt + INTERVAL '5' SECOND AND
        |    t2.rt + INTERVAL '4' SECOND
        |""".stripMargin

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

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()
    val expected = new java.util.ArrayList[String]
    expected.add("null,null,L-1")
    expected.add("null,null,L-4")
    expected.add("null,null,L-7")
    expected.add("A,R-6,null")
    expected.add("B,R-7,null")
    expected.add("D,R-8,null")
    StreamITCase.compareWithList(expected)
  }

  /** test non-window inner join **/
  @Test
  def testNonWindowInnerJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setStateBackend(getStateBackend)

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

    val t1 = env.fromCollection(data1).toTable(tEnv, 'a, 'b, 'c)
      .select(('a === 3) ? (nullOf(Types.INT), 'a) as 'a, 'b, 'c)
    val t2 = env.fromCollection(data2).toTable(tEnv, 'a, 'b, 'c)
      .select(('a === 3) ? (nullOf(Types.INT), 'a) as 'a, 'b, 'c)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sqlQuery =
      """
        |SELECT t2.a, t2.c, t1.c
        |FROM T1 as t1 JOIN T2 as t2 ON
        |  t1.a = t2.a AND
        |  t1.b > t2.b
        |""".stripMargin

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList(
      "1,HiHi,Hi2",
      "1,HiHi,Hi2",
      "1,HiHi,Hi3",
      "1,HiHi,Hi6",
      "1,HiHi,Hi8",
      "2,HeHe,Hi5",
      "null,HeHe,Hi9")

    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testInnerJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setStateBackend(getStateBackend)
    val sqlQuery = "SELECT c, g FROM Table3, Table5 WHERE b = e"

    val ds1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    tEnv.registerTable("Table3", ds1)
    tEnv.registerTable("Table5", ds2)

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = Seq("Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt")
    val results = result.toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testInnerJoinWithFilter(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setStateBackend(getStateBackend)

    val sqlQuery = "SELECT c, g FROM Table3, Table5 WHERE b = e AND b < 2"

    val ds1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    tEnv.registerTable("Table3", ds1)
    tEnv.registerTable("Table5", ds2)

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = Seq("Hi,Hallo")
    val results = result.toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testInnerJoinWithNonEquiJoinPredicate(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setStateBackend(getStateBackend)

    val sqlQuery = "SELECT c, g FROM Table3, Table5 WHERE b = e AND a < 6 AND h < b"

    val ds1 = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    tEnv.registerTable("Table3", ds1)
    tEnv.registerTable("Table5", ds2)

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = Seq("Hello world, how are you?,Hallo Welt wie", "I am fine.,Hallo Welt wie")
    val results = result.toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testInnerJoinWithMultipleKeys(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setStateBackend(getStateBackend)

    val sqlQuery = "SELECT c, g FROM Table3, Table5 WHERE a = d AND b = h"

    val ds1 = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    tEnv.registerTable("Table3", ds1)
    tEnv.registerTable("Table5", ds2)

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = Seq(
      "Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt wie gehts?", "Hello world,ABC",
      "I am fine.,HIJ", "I am fine.,IJK")
    val results = result.toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testInnerJoinWithAlias(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setStateBackend(getStateBackend)

    val sqlQuery =
      "SELECT Table5.c, T.`1-_./Ü` FROM (SELECT a, b, c AS `1-_./Ü` FROM Table3) AS T, Table5 " +
        "WHERE a = d AND a < 4"

    val ds1 = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'c)
    tEnv.registerTable("Table3", ds1)
    tEnv.registerTable("Table5", ds2)

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = Seq("1,Hi", "2,Hello", "1,Hello",
      "2,Hello world", "2,Hello world", "3,Hello world")
    val results = result.toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testInnerJoinWithAggregation(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setParallelism(1)
    env.setStateBackend(getStateBackend)

    val sqlQuery = "SELECT COUNT(g), COUNT(b) FROM Table3, Table5 WHERE a = d"

    val ds1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    tEnv.registerTable("Table3", ds1)
    tEnv.registerTable("Table5", ds2)

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = Seq("6,6")
    val results = result.toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testLeftJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    StreamITCase.clear
    env.setStateBackend(getStateBackend)

    val sqlQuery = "SELECT c, g FROM Table5 LEFT OUTER JOIN Table3 ON b = e"

    val ds1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    tEnv.registerTable("Table3", ds1)
    tEnv.registerTable("Table5", ds2)

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = Seq("Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt",
      "null,Hallo Welt wie", "null,Hallo Welt wie gehts?", "null,ABC", "null,BCD",
      "null,CDE", "null,DEF", "null,EFG", "null,FGH", "null,GHI", "null,HIJ",
      "null,IJK", "null,JKL", "null,KLM")
    val results = result.toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testRightJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setStateBackend(getStateBackend)

    val sqlQuery = "SELECT c, g FROM Table3 RIGHT OUTER JOIN Table5 ON b = e"

    val ds1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    tEnv.registerTable("Table3", ds1)
    tEnv.registerTable("Table5", ds2)

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = Seq("Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt",
      "null,Hallo Welt wie", "null,Hallo Welt wie gehts?", "null,ABC", "null,BCD",
      "null,CDE", "null,DEF", "null,EFG", "null,FGH", "null,GHI", "null,HIJ",
      "null,IJK", "null,JKL", "null,KLM")
    val results = result.toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testLeftSingleRightJoinEqualPredicate(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setStateBackend(getStateBackend)

    val sqlQuery =
      "SELECT a, cnt FROM (SELECT COUNT(*) AS cnt FROM B) RIGHT JOIN A ON cnt = a"

    val ds1 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
    val ds2 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'f, 'g, 'h)
    tEnv.registerTable("A", ds1)
    tEnv.registerTable("B", ds2)

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = Seq(
      "1,null", "2,null", "2,null", "3,3", "3,3", "3,3", "4,null", "4,null", "4," +
      "null", "4,null", "5,null", "5,null", "5,null", "5,null", "5,null")
    val results = result.toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testFullOuterJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setStateBackend(getStateBackend)

    val sqlQuery = "SELECT c, g FROM Table3 FULL OUTER JOIN Table5 ON b = e"

    val ds1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    tEnv.registerTable("Table3", ds1)
    tEnv.registerTable("Table5", ds2)

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = Seq("Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt",
      "null,Hallo Welt wie", "null,Hallo Welt wie gehts?", "null,ABC", "null,BCD",
      "null,CDE", "null,DEF", "null,EFG", "null,FGH", "null,GHI", "null,HIJ",
      "null,IJK", "null,JKL", "null,KLM")
    val results = result.toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
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
