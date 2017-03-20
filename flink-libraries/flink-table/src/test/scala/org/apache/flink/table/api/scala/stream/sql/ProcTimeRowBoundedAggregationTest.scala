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
import org.junit.Assert._
import org.junit._
import scala.collection.mutable
import org.apache.flink.types.Row

class ProcTimeRowBoundedAggregationTest extends StreamingWithStateTestBase {

  @Test
  def testMaxAggregatation(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    StreamITCase.testResults = mutable.MutableList()

    val t = StreamTestData.get5TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c, 'd, 'e)
    tEnv.registerTable("MyTable", t)

    val sqlQuery = "SELECT a, MAX(c) OVER (" +
      "ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) " +
      "AS sumC " +
      "FROM MyTable"

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

    val sqlQuery = "SELECT a, MIN(c)" +
      "OVER (PARTITION BY a ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) " +
      "AS avgC " +
      "FROM MyTable"

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
      "4,7",
      "5,10",
      "5,10",
      "5,10",
      "5,11",
      "5,12")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testSumAggregatation(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT a, SUM(c) OVER " +
      "(PARTITION BY a ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) " +
      "AS sumC " +
      "FROM MyTable"

    val t = StreamTestData.get5TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c, 'd, 'e)
    tEnv.registerTable("MyTable", t)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,0",
      "2,1",
      "2,3",
      "3,3",
      "3,7",
      "3,12",
      "4,6",
      "4,13",
      "4,21",
      "4,24",
      "5,10",
      "5,21",
      "5,33",
      "5,36",
      "5,39")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testSumMinAggregatation(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    StreamITCase.testResults = mutable.MutableList()

    val t = StreamTestData.get5TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c, 'd, 'e)
    tEnv.registerTable("MyTable", t)

    val sqlQuery = "SELECT a,  " +
      " SUM(c) OVER (" +
      " PARTITION BY a ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sumC , " +
      " MIN(c) OVER (" +
      " PARTITION BY a ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS minC " +
      " FROM MyTable"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,0,0",
      "2,1,1",
      "2,3,1",
      "3,3,3",
      "3,7,3",
      "3,12,3",
      "4,6,6",
      "4,13,6",
      "4,21,6",
      "4,24,7",
      "5,10,10",
      "5,21,10",
      "5,33,10",
      "5,36,11",
      "5,39,12")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testSumUnpartitionedAggregatation(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    StreamITCase.testResults = mutable.MutableList()

    val t = StreamTestData.get5TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c, 'd, 'e)
    tEnv.registerTable("MyTable", t)

    val sqlQuery = "SELECT SUM(c) OVER " +
      "(ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) " +
      "AS sumC " +
      "FROM MyTable"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "0",
      "1",
      "3",
      "6",
      "9",
      "12",
      "15",
      "18",
      "21",
      "24",
      "27",
      "30",
      "33",
      "36",
      "39")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testAvgAggregatation(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT a, AVG(c) OVER " +
      "(PARTITION BY a ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) " +
      "AS avgC, e " +
      "FROM MyTable"

    val t = StreamTestData.get5TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c, 'd, 'e)
    tEnv.registerTable("MyTable", t)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,0,1",
      "2,1,1",
      "2,1,2",
      "3,3,2",
      "3,3,2",
      "3,4,3",
      "4,6,2",
      "4,6,1",
      "4,7,1",
      "4,8,2",
      "5,10,1",
      "5,10,3",
      "5,11,3",
      "5,12,2",
      "5,13,2")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testAvgAggregatation2(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    StreamITCase.testResults = mutable.MutableList()

    val t = StreamTestData.get5TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c, 'd, 'e)
    tEnv.registerTable("MyTable", t)

    val sqlQuery = "SELECT a, AVG(c) OVER " +
      "(PARTITION BY a ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) " +
      "AS avgC, d " +
      "FROM MyTable"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,0,Hallo",
      "2,1,Hallo Welt",
      "2,1,Hallo Welt wie",
      "3,3,Hallo Welt wie gehts?",
      "3,3,ABC",
      "3,4,BCD",
      "4,6,CDE",
      "4,6,DEF",
      "4,7,EFG",
      "4,8,FGH",
      "5,10,GHI",
      "5,10,HIJ",
      "5,11,IJK",
      "5,12,JKL",
      "5,13,KLM")

    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

}
