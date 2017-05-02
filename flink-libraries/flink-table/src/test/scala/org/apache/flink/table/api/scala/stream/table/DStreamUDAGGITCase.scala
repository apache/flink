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

package org.apache.flink.table.api.scala.stream.table

import java.math.BigDecimal

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.streaming.api.datastream.{DataStream => JDataStream}
import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironment => JavaExecutionEnv}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment => ScalaExecutionEnv}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.java.utils.UserDefinedAggFunctions.{WeightedAvg, WeightedAvgWithMerge, WeightedAvgWithRetract}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.stream.table.DStreamUDAGGITCase.TimestampAndWatermarkWithOffset
import org.apache.flink.table.api.scala.stream.utils.StreamITCase
import org.apache.flink.table.api.{SlidingWindow, TableEnvironment, Types}
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.apache.flink.table.functions.aggfunctions.CountAggFunction
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit.Test
import org.mockito.Mockito.{mock, when}

import scala.collection.mutable

/**
  * We only test some aggregations until better testing of constructed DataStream
  * programs is possible.
  */
class DStreamUDAGGITCase
  extends StreamingMultipleProgramsTestBase {

  val data = List(
    //('long, 'int, 'double, 'float, 'bigdec, 'string)
    (1000L, 1, 1d, 1f, new BigDecimal("1"), "Hello"),
    (2000L, 2, 2d, 2f, new BigDecimal("2"), "Hello"),
    (3000L, 3, 3d, 3f, new BigDecimal("3"), "Hello"),
    (5000L, 5, 5d, 5f, new BigDecimal("5"), "Hi"),
    (6000L, 6, 6d, 6f, new BigDecimal("6"), "Hi"),
    (7000L, 7, 7d, 7f, new BigDecimal("7"), "Hi"),
    (8000L, 8, 8d, 8f, new BigDecimal("8"), "Hello"),
    (9000L, 9, 9d, 9f, new BigDecimal("9"), "Hello"),
    (4000L, 4, 4d, 4f, new BigDecimal("4"), "Hello"),
    (10000L, 10, 10d, 10f, new BigDecimal("10"), "Hi"),
    (11000L, 11, 11d, 11f, new BigDecimal("11"), "Hi"),
    (12000L, 12, 12d, 12f, new BigDecimal("12"), "Hi"),
    (16000L, 16, 16d, 16f, new BigDecimal("16"), "Hello"))

  @Test
  def testUdaggSlidingWindowGroupedAggregate(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env.fromCollection(data).map(t => (t._1, t._2, t._3, t._4, t._6))
    val table = stream.toTable(tEnv, 'long, 'int, 'double, 'float, 'string)

    val countFun = new CountAggFunction

    val weightAvgFun = new WeightedAvg

    val windowedTable = table
      .window(Slide over 4.rows every 2.rows as 'w)
      .groupBy('w, 'string)
      .select(
        'string,
        countFun('float),
        'double.sum,
        weightAvgFun('long, 'int),
        weightAvgFun('int, 'int))

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq(
      "Hello,2,3.0,1666,1", "Hi,2,11.0,5545,5", "Hello,4,14.0,5571,5",
      "Hello,4,24.0,7083,7", "Hi,4,28.0,7500,7", "Hi,4,40.0,10350,10")
    assertEquals(expected, StreamITCase.testResults)
  }

  @Test
  def testUdaggSessionWindowGroupedAggregate(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val countFun = new CountAggFunction

    val weightAvgWithMergeFun = new WeightedAvgWithMerge

    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampAndWatermarkWithOffset(10000))
    val table = stream.toTable(tEnv, 'long, 'int, 'double, 'float, 'bigdec, 'string)

    val windowedTable = table
      .window(Session withGap 5.second on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select(
        'string,
        countFun('bigdec),
        'float.sum,
        weightAvgWithMergeFun('long, 'int),
        weightAvgWithMergeFun('int, 'int))

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq("Hello,6,27.0,6481,6", "Hi,6,51.0,9313,9", "Hello,1,16.0,16000,16")
    assertEquals(expected, StreamITCase.testResults)
  }

  @Test
  def testUdaggProcTimeUnBoundedPartitionedRowOver(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()
    StreamITCase.clear
    val stream = env.fromCollection(data).map(t => (t._4, t._1, t._2, t._3, t._6))
    val table = stream.toTable(tEnv, 'float, 'long, 'int, 'double, 'string)

    val countFun = new CountAggFunction

    val weightedAvgWithRetractFun = new WeightedAvgWithRetract

    val windowedTable = table
      .window(
        Over partitionBy 'string orderBy 'proctime preceding UNBOUNDED_ROW as 'w)
      .select(
        'string,
        'float,
        countFun('string) over 'w,
        'double.sum over 'w,
        weightedAvgWithRetractFun('long, 'int) over 'w,
        weightedAvgWithRetractFun('int, 'int) over 'w)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq(
      "Hello,1.0,1,1.0,1000,1", "Hello,2.0,2,3.0,1666,1", "Hello,3.0,3,6.0,2333,2",
      "Hi,5.0,1,5.0,5000,5", "Hi,6.0,2,11.0,5545,5", "Hi,7.0,3,18.0,6111,6",
      "Hello,8.0,4,14.0,5571,5", "Hello,9.0,5,23.0,6913,6", "Hello,4.0,6,27.0,6481,6",
      "Hi,10.0,4,28.0,7500,7", "Hi,11.0,5,39.0,8487,8", "Hi,12.0,6,51.0,9313,9",
      "Hello,16.0,7,43.0,10023,10")
    assertEquals(expected, StreamITCase.testResults)
  }

  @Test
  def testUdaggProcTimeUnBoundedPartitionedRowOverSQL(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()
    StreamITCase.clear
    val stream = env.fromCollection(data).map(t => (t._4, t._1, t._2, t._3, t._6))
    val table = stream.toTable(tEnv, 'f, 'l, 'i, 'd, 's)

    tEnv.registerTable("T1", table)
    tEnv.registerFunction("countFun", new CountAggFunction)
    tEnv.registerFunction("wAvgWithRetract", new WeightedAvgWithRetract)
    val sqlQuery = "SELECT " +
      "s, " +
      "f, " +
      "countFun(i) OVER (PARTITION BY s ORDER BY ProcTime() RANGE UNBOUNDED preceding)," +
      "sum(d) OVER (PARTITION BY s ORDER BY ProcTime() RANGE UNBOUNDED preceding)," +
      "wAvgWithRetract(l,i) OVER (PARTITION BY s ORDER BY ProcTime() RANGE UNBOUNDED preceding)," +
      "wAvgWithRetract(i,i) OVER (PARTITION BY s ORDER BY ProcTime() RANGE UNBOUNDED preceding)" +
      "from T1"

    val results = tEnv.sql(sqlQuery).toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq(
      "Hello,1.0,1,1.0,1000,1", "Hello,2.0,2,3.0,1666,1", "Hello,3.0,3,6.0,2333,2",
      "Hi,5.0,1,5.0,5000,5", "Hi,6.0,2,11.0,5545,5", "Hi,7.0,3,18.0,6111,6",
      "Hello,8.0,4,14.0,5571,5", "Hello,9.0,5,23.0,6913,6", "Hello,4.0,6,27.0,6481,6",
      "Hi,10.0,4,28.0,7500,7", "Hi,11.0,5,39.0,8487,8", "Hi,12.0,6,51.0,9313,9",
      "Hello,16.0,7,43.0,10023,10")
    assertEquals(expected, StreamITCase.testResults)
  }

  @Test
  def testUdaggGroupedAggregateSQL(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()
    StreamITCase.clear
    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampAndWatermarkWithOffset(10000))
      .map(t => (t._4, t._1, t._2, t._3, t._6))
    val table = stream.toTable(tEnv, 'f, 'l, 'i, 'd, 's)

    tEnv.registerTable("T1", table)
    tEnv.registerFunction("countFun", new CountAggFunction)
    tEnv.registerFunction("wAvgWithRetract", new WeightedAvgWithRetract)
    val sqlQuery =
      "SELECT s, countFun(i), SUM(d)" +
        "FROM T1 " +
        "GROUP BY s, TUMBLE(rowtime(), INTERVAL '5' SECOND)"

    val results = tEnv.sql(sqlQuery).toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq(
      "Hello,4,10.0", "Hi,3,18.0", "Hello,2,17.0", "Hi,3,33.0", "Hello,1,16.0")
    assertEquals(expected, StreamITCase.testResults)
  }

  @Test
  def testUdaggJavaAPI(): Unit = {
    // mock
    val ds = mock(classOf[DataStream[Row]])
    val jDs = mock(classOf[JDataStream[Row]])
    val typeInfo = new RowTypeInfo(Seq(Types.INT, Types.LONG, Types.STRING): _*)
    when(ds.javaStream).thenReturn(jDs)
    when(jDs.getType).thenReturn(typeInfo)
    // Scala environment
    val env = mock(classOf[ScalaExecutionEnv])
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val in1 = ds.toTable(tableEnv).as('int, 'long, 'string)

    // Java environment
    val javaEnv = mock(classOf[JavaExecutionEnv])
    val javaTableEnv = TableEnvironment.getTableEnvironment(javaEnv)
    val in2 = javaTableEnv.fromDataStream(jDs).as("int, long, string")

    // Java API
    javaTableEnv.registerFunction("myCountFun", new CountAggFunction)
    javaTableEnv.registerFunction("weightAvgFun", new WeightedAvg)
    var javaTable = in2.window((new SlidingWindow(4.rows, 2.rows)).as("w"))
      .groupBy("w, string")
      .select(
        "string, " +
        "myCountFun(string), " +
        "int.sum, " +
        "weightAvgFun(long, int), " +
        "weightAvgFun(int, int)")

    // Scala API
    val myCountFun = new CountAggFunction
    val weightAvgFun = new WeightedAvg
    var scalaTable = in1.window(Slide over 4.rows every 2.rows as 'w)
      .groupBy('w, 'string)
      .select(
        'string, myCountFun('string), 'int.sum, weightAvgFun('long, 'int),
        weightAvgFun('int, 'int))

    val helper = new TableTestBase
    helper.verifyTableEquals(scalaTable, javaTable)
  }
}

object DStreamUDAGGITCase {

  class TimestampAndWatermarkWithOffset(offset: Int)
    extends AssignerWithPunctuatedWatermarks[(Long, Int, Double, Float, BigDecimal, String)] {

    override def checkAndGetNextWatermark(
        lastElement: (Long, Int, Double, Float, BigDecimal, String),
        extractedTimestamp: Long)
    : Watermark = {
      new Watermark(extractedTimestamp - offset)
    }

    override def extractTimestamp(
        element: (Long, Int, Double, Float, BigDecimal, String),
        previousElementTimestamp: Long): Long = {
      element._1
    }
  }

}
