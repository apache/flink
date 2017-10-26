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
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.{StreamITCase, StreamingWithStateTestBase}
import org.apache.flink.types.Row
import org.hamcrest.CoreMatchers
import org.junit._

import scala.collection.mutable

class JoinITCase extends StreamingWithStateTestBase {

  /** test proctime inner join **/
  @Test
  def testProcessTimeInnerJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStateBackend(getStateBackend)
    StreamITCase.clear
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
    val t2 = env.fromCollection(data2).toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)

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
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStateBackend(getStateBackend)
    StreamITCase.clear
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
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStateBackend(getStateBackend)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    StreamITCase.clear

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

  /** test rowtime inner join with other conditions **/
  @Test
  def testRowTimeInnerJoinWithOtherConditions(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStateBackend(getStateBackend)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    StreamITCase.clear

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
    val expected1 = new mutable.MutableList[String]
    expected1+= "1,LEFT3,RIGHT6"
    expected1+= "1,LEFT1.1,RIGHT6"
    expected1+= "2,LEFT4,RIGHT7"
    expected1+= "1,LEFT4.9,RIGHT6"

    val expected2 = new mutable.MutableList[String]
    expected2+= "1,LEFT3,RIGHT6"
    expected2+= "1,LEFT1.1,RIGHT6"
    expected2+= "2,LEFT4,RIGHT7"
    expected2+= "1,LEFT4.9,RIGHT6"

    Assert.assertThat(
      StreamITCase.testResults.sorted,
      CoreMatchers.either(CoreMatchers.is(expected1.sorted)).
        or(CoreMatchers.is(expected2.sorted)))
  }

  /** test rowtime inner join with window aggregation **/
  @Test
  def testRowTimeInnerJoinWithWindowAggregateOnFirstTime(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStateBackend(getStateBackend)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    StreamITCase.clear

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
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStateBackend(getStateBackend)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    StreamITCase.clear

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
