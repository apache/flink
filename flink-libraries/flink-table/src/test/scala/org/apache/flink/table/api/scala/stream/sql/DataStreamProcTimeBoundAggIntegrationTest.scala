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
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{ TableEnvironment, TableException }
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.stream.utils.{
  StreamingWithStateTestBase,
  StreamITCase,
  StreamTestData
}
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit._
import scala.collection.mutable

class DataStreamProcTimeBoundAggIntegrationTest extends StreamingWithStateTestBase {

  // ----------------------------------------------------------------------------------------------
  // Sliding windows
  // ----------------------------------------------------------------------------------------------

  @Test
  def testMaxAggregatation(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT a, MAX(c) OVER (PARTITION BY a ORDER BY procTime()" +
      "RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW) AS maxC FROM MyTable"

    val t = StreamTestData.get5TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c, 'd, 'e)
    tEnv.registerTable("MyTable", t)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,0",
      "2,1",
      "2,2",
      "3,3",
      "3,4",
      "3,5",
      "4,6",
      "4,7",
      "4,8",
      "4,9",
      "5,10",
      "5,11",
      "5,12",
      "5,13",
      "5,14")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testMinAggregatation(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT a, MIN(c) OVER (PARTITION BY a ORDER BY procTime()" +
      "RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW) AS minC FROM MyTable"

    val t = StreamTestData.get5TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c, 'd, 'e)
    tEnv.registerTable("MyTable", t)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,0",
      "2,1",
      "2,1",
      "3,3",
      "3,3",
      "3,3",
      "4,6",
      "4,6",
      "4,6",
      "4,6",
      "5,10",
      "5,10",
      "5,10",
      "5,10",
      "5,10")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testSumAggregatation(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT a, SUM(c) OVER (PARTITION BY a ORDER BY procTime()" +
      "RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW) AS sumC FROM MyTable"

    val t = StreamTestData.get5TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c, 'd, 'e)
    tEnv.registerTable("MyTable", t)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,0",
      "2,1",
      "2,3",
      "3,12",
      "3,3",
      "3,7",
      "4,13",
      "4,21",
      "4,30",
      "4,6",
      "5,10",
      "5,21",
      "5,33",
      "5,46",
      "5,60")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testAvgAggregatation(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT a, AVG(c) OVER (PARTITION BY a ORDER BY procTime()" +
      "RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW) AS avgC FROM MyTable"

    val t = StreamTestData.get5TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c, 'd, 'e)
    tEnv.registerTable("MyTable", t)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,0",
      "2,1",
      "2,1",
      "3,3",
      "3,3",
      "3,4",
      "4,6",
      "4,6",
      "4,7",
      "4,7",
      "5,10",
      "5,10",
      "5,11",
      "5,11",
      "5,12")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

}


