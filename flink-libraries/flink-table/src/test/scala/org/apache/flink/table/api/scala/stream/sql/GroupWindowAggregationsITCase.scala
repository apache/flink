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
package org.apache.flink.table.api.scala.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.java.utils.Pojos.Pojo2
import org.apache.flink.table.api.java.utils.UserDefinedAggFunctions.WeightedAvg
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.stream.table.GroupWindowAggregationsITCase.TimestampAndWatermarkWithOffset
import org.apache.flink.table.api.scala.stream.utils.{StreamITCase, StreamingWithStateTestBase}
import org.apache.flink.table.functions.aggfunctions.CountAggFunction
import org.junit.Assert._
import org.junit._

import scala.collection.mutable

class GroupWindowAggregationsITCase extends StreamingWithStateTestBase {

  val data = List(
    (1000L, 1, "Hi"),
    (2000L, 2, "Hello"),
    (4000L, 2, "Hello"),
    (8000L, 3, "Hello world"),
    (16000L, 3, "Hello world"))

  @Test
  def testEventTimeTumblingWindowWithPojoType(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampAndWatermarkWithOffset(0L))

    val table = stream.toTable(tEnv, 'rowtime.rowtime, 'num, 'name)
    tEnv.registerTable("T1", table)

    val countFun = new CountAggFunction
    val weightAvgFun = new WeightedAvg

    tEnv.registerFunction("countFun", countFun)
    tEnv.registerFunction("weightAvgFun", weightAvgFun)

    val querySql =
      "SELECT " +
        "name as string," +
        "countFun(name) as myCnt," +
        "avg(num) as myAvg," +
        "weightAvgFun(num, num) as myWeightAvg," +
        "min(num) as myMin," +
        "max(num) as myMax," +
        "sum(num) as mySum," +
        "TUMBLE_START(rowtime, INTERVAL '5' SECOND) as winStart," +
        "TUMBLE_END(rowtime, INTERVAL '5' SECOND) as winEnd " +
        "FROM T1 GROUP BY name, " +
        "TUMBLE(rowtime, INTERVAL '5' SECOND)"

    println(querySql)

    val results = tEnv.sql(querySql).toAppendStream[Pojo2]
    results.addSink(new StreamITCase.StringSink[Pojo2])
    env.execute()

    val expected = Seq(
      "Pojo2{string='Hello world', myCnt=1, myAvg=3, myWeightAvg=3, myMin=3, myMax=3, mySum=3, " +
        "winStart=1970-01-01 00:00:05.0, winEnd=1970-01-01 00:00:10.0}",
      "Pojo2{string='Hello world', myCnt=1, myAvg=3, myWeightAvg=3, myMin=3, myMax=3, mySum=3, " +
        "winStart=1970-01-01 00:00:15.0, winEnd=1970-01-01 00:00:20.0}",
      "Pojo2{string='Hello', myCnt=2, myAvg=2, myWeightAvg=2, myMin=2, myMax=2, mySum=4, " +
        "winStart=1970-01-01 00:00:00.0, winEnd=1970-01-01 00:00:05.0}",
      "Pojo2{string='Hi', myCnt=1, myAvg=1, myWeightAvg=1, myMin=1, myMax=1, mySum=1, " +
        "winStart=1970-01-01 00:00:00.0, winEnd=1970-01-01 00:00:05.0}"
      )

    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }
}

