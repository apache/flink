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
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.runtime.utils.{StreamingWithStateTestBase, TestingAppendSink}
import org.apache.flink.table.planner.utils.TableTestUtil
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.sql.Timestamp

import scala.collection.mutable

@RunWith(classOf[Parameterized])
class TemporalJoinITCase(state: StateBackendMode)
  extends StreamingWithStateTestBase(state) {

  /**
    * Because of nature of the processing time, we can not (or at least it is not that easy)
    * validate the result here. Instead of that, here we are just testing whether there are no
    * exceptions in a full blown ITCase. Actual correctness is tested in unit tests.
    */
  @Test
  def testProcessTimeInnerJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val sqlQuery =
      """
        |SELECT
        |  o.amount * r.rate AS amount
        |FROM
        |  Orders AS o,
        |  LATERAL TABLE (Rates(o.proctime)) AS r
        |WHERE r.currency = o.currency
        |""".stripMargin

    val ordersData = new mutable.MutableList[(Long, String)]
    ordersData.+=((2L, "Euro"))
    ordersData.+=((1L, "US Dollar"))
    ordersData.+=((50L, "Yen"))
    ordersData.+=((3L, "Euro"))
    ordersData.+=((5L, "US Dollar"))

    val ratesHistoryData = new mutable.MutableList[(String, Long)]
    ratesHistoryData.+=(("US Dollar", 102L))
    ratesHistoryData.+=(("Euro", 114L))
    ratesHistoryData.+=(("Yen", 1L))
    ratesHistoryData.+=(("Euro", 116L))
    ratesHistoryData.+=(("Euro", 119L))

    val orders = env
      .fromCollection(ordersData)
      .toTable(tEnv, 'amount, 'currency, 'proctime.proctime)
    val ratesHistory = env
      .fromCollection(ratesHistoryData)
      .toTable(tEnv, 'currency, 'rate, 'proctime.proctime)

    tEnv.registerTable("Orders", orders)
    tEnv.registerTable("RatesHistory", ratesHistory)
    tEnv.registerFunction(
      "Rates",
      ratesHistory.createTemporalTableFunction("proctime", "currency"))

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new TestingAppendSink)
    env.execute()
  }

  @Test
  def testEventTimeInnerJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sqlQuery =
      """
        |SELECT
        |  o.amount * r.rate AS amount
        |FROM
        |  Orders AS o,
        |  LATERAL TABLE (Rates(o.rowtime)) AS r
        |WHERE r.currency = o.currency
        |""".stripMargin

    val ordersData = new mutable.MutableList[(Long, String, Timestamp)]
    ordersData.+=((2L, "Euro", new Timestamp(2L)))
    ordersData.+=((1L, "US Dollar", new Timestamp(3L)))
    ordersData.+=((50L, "Yen", new Timestamp(4L)))
    ordersData.+=((3L, "Euro", new Timestamp(5L)))

    val ratesHistoryData = new mutable.MutableList[(String, Long, Timestamp)]
    ratesHistoryData.+=(("US Dollar", 102L, new Timestamp(1L)))
    ratesHistoryData.+=(("Euro", 114L, new Timestamp(1L)))
    ratesHistoryData.+=(("Yen", 1L, new Timestamp(1L)))
    ratesHistoryData.+=(("Euro", 116L, new Timestamp(5L)))
    ratesHistoryData.+=(("Euro", 119L, new Timestamp(7L)))

    var expectedOutput = new mutable.HashSet[String]()
    expectedOutput += (2 * 114).toString
    expectedOutput += (3 * 116).toString

    val orders = env
      .fromCollection(ordersData)
      .assignTimestampsAndWatermarks(new TimestampExtractor[Long, String]())
      .toTable(tEnv, 'amount, 'currency, 'rowtime.rowtime)
    val ratesHistory = env
      .fromCollection(ratesHistoryData)
      .assignTimestampsAndWatermarks(new TimestampExtractor[String, Long]())
      .toTable(tEnv, 'currency, 'rate, 'rowtime.rowtime)

    tEnv.registerTable("Orders", orders)
    tEnv.registerTable("RatesHistory", ratesHistory)
    tEnv.registerTable("FilteredRatesHistory",
      tEnv.sqlQuery("SELECT * FROM RatesHistory WHERE rate > 110"))
    tEnv.registerFunction(
      "Rates",
      tEnv
        .scan("FilteredRatesHistory")
        .createTemporalTableFunction("rowtime", "currency"))
    tEnv.registerTable("TemporalJoinResult", tEnv.sqlQuery(sqlQuery))

    // Scan from registered table to test for interplay between
    // LogicalCorrelateToTemporalTableJoinRule and TableScanRule
    val result = tEnv.scan("TemporalJoinResult").toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    assertEquals(expectedOutput, sink.getAppendResults.toSet)
  }
}

class TimestampExtractor[T1, T2]
  extends BoundedOutOfOrdernessTimestampExtractor[(T1, T2, Timestamp)](Time.seconds(10))  {
  override def extractTimestamp(element: (T1, T2, Timestamp)): Long = {
    element._3.getTime
  }
}
