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
package org.apache.flink.table.runtime.datastream

import java.lang.{Integer => JInt, Long => JLong}

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironment => EnvStreamExecutionEnvironment}
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.scala.stream.utils.StreamingWithStateTestBase
import org.apache.flink.table.api.{StreamQueryConfig, TableEnvironment, Types}
import org.apache.flink.table.api.java.utils.UserDefinedAggFunctions.WeightedAvgWithMerge
import org.apache.flink.table.api.scala.Session
import org.apache.flink.table.api.scala.stream.utils.StreamITCase
import org.apache.flink.table.functions.aggfunctions.CountAggFunction
import org.apache.flink.types.Row
import org.apache.flink.table.runtime.datastream.AdjustWatermarkITCase.{TimestampAndWatermark, TestStreamTableSource}
import org.apache.flink.table.sources.{DefinedRowtimeAttribute, StreamTableSource}
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.mutable

class AdjustWatermarkITCase extends StreamingWithStateTestBase {
  private var queryConfig = new StreamQueryConfig()

  @Test
  def testDataStreamSource(): Unit = {
    queryConfig = queryConfig.withLateDataTimeOffset(Time.milliseconds(10))
    val sessionWindowTestData = List(
      (1L, 1, "Hello"),
      (2L, 2, "Hello"),
      (8L, 8, "Hello"),
      (9L, 9, "Hello World"),
      (4L, 4, "Hello"),
      (16L, 16, "Hello"))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val countFun = new CountAggFunction
    val weightAvgFun = new WeightedAvgWithMerge

    val stream = env
      .fromCollection(sessionWindowTestData)
      .assignTimestampsAndWatermarks(new TimestampAndWatermark)
    val table = stream.toTable(tEnv, 'long, 'int, 'string, 'rowtime.rowtime)

    val windowedTable = table
      .window(Session withGap 5.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select(
        'string, countFun('int), 'int.avg,
        weightAvgFun('long, 'int), weightAvgFun('int, 'int))

    val results = windowedTable.toAppendStream[Row](queryConfig)
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq("Hello World,1,9,9,9", "Hello,1,16,16,16", "Hello,4,3,5,5")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testStreamTableSource(): Unit = {
    queryConfig = queryConfig.withLateDataTimeOffset(Time.seconds(10))
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    tEnv.registerTableSource("MyTable", new TestStreamTableSource)

    val countFun = new CountAggFunction

    tEnv.registerFunction("countFun",countFun)

    val sqlQuery =
      "SELECT c," +
        "  countFun(a), sum(b), " +
        "  TUMBLE_START(rowtime, INTERVAL '5' SECOND), " +
        "  TUMBLE_END(rowtime, INTERVAL '5' SECOND)" +
        "FROM MyTable " +
        "GROUP BY c, TUMBLE(rowtime, INTERVAL '5' SECOND)"

    val result = tEnv.sql(sqlQuery).toAppendStream[Row](queryConfig)
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList(
      "Hello world,1,9,1970-01-01 00:00:05.0,1970-01-01 00:00:10.0",
      "Hello,1,16,1970-01-01 00:00:15.0,1970-01-01 00:00:20.0",
      "Hello,1,8,1970-01-01 00:00:05.0,1970-01-01 00:00:10.0",
      "Hello,2,6,1970-01-01 00:00:00.0,1970-01-01 00:00:05.0",
      "Hi,1,1,1970-01-01 00:00:00.0,1970-01-01 00:00:05.0")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)

  }
}

object AdjustWatermarkITCase {

  class TimestampAndWatermark extends AssignerWithPunctuatedWatermarks[(Long, Int, String)] {

    override def checkAndGetNextWatermark(
        lastElement: (Long, Int, String),
        extractedTimestamp: Long)
    : Watermark = {
      new Watermark(extractedTimestamp)
    }

    override def extractTimestamp(
        element: (Long, Int, String),
        previousElementTimestamp: Long): Long = {
      element._1
    }
  }

  class TestStreamTableSource extends StreamTableSource[Row] with DefinedRowtimeAttribute {

    override def getDataStream(execEnv: EnvStreamExecutionEnvironment): DataStream[Row] = {

      val datas: List[Row] = List(
        Row.of(JLong.valueOf(1000L), JInt.valueOf(1), "Hi"),
        Row.of(JLong.valueOf(2000L), JInt.valueOf(2), "Hello"),
        Row.of(JLong.valueOf(8000L), JInt.valueOf(8), "Hello"),
        Row.of(JLong.valueOf(9000L), JInt.valueOf(9), "Hello world"),
        Row.of(JLong.valueOf(4000L), JInt.valueOf(4), "Hello"),
        Row.of(JLong.valueOf(16000L), JInt.valueOf(16), "Hello"))

      var dataWithTsAndWatermark: Seq[Either[(Long, Row), Long]] = Seq[Either[(Long, Row), Long]]()
      datas.foreach {
        data =>
          val left = Left(data.getField(0).asInstanceOf[Long], data)
          val right = Right(data.getField(0).asInstanceOf[Long])
          dataWithTsAndWatermark = dataWithTsAndWatermark ++ Seq(left) ++ Seq(right)
      }

      execEnv
      .addSource(
        new SourceFunction[Row] {
          override def run(ctx: SourceContext[Row]): Unit = {
            dataWithTsAndWatermark.foreach {
              case Left(t) => ctx.collectWithTimestamp(t._2, t._1)
              case Right(w) => ctx.emitWatermark(new Watermark(w))
            }
          }

          override def cancel(): Unit = ???
        }).returns(getReturnType)
    }

    override def getRowtimeAttribute: String = "rowtime"

    override def getReturnType: TypeInformation[Row] = {
      new RowTypeInfo(
        Array(Types.LONG, Types.INT, Types.STRING)
        .asInstanceOf[Array[TypeInformation[_]]],
        Array("a", "b", "c"))
    }
  }

}
