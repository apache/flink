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

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.tuple.Tuple1
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.TimeTestUtil.EventTimeSourceFunction
import org.apache.flink.table.api.StreamQueryConfig
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.MultiArgCount
import org.apache.flink.table.runtime.utils.{StreamITCase, StreamTestData, StreamingWithStateTestBase}
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit._

import scala.collection.mutable

class OverWindowITCase extends StreamingWithStateTestBase {

  val data = List(
    (1L, 1, "Hello"),
    (2L, 2, "Hello"),
    (3L, 3, "Hello"),
    (4L, 4, "Hello"),
    (5L, 5, "Hello"),
    (6L, 6, "Hello"),
    (7L, 7, "Hello World"),
    (8L, 8, "Hello World"),
    (20L, 20, "Hello World"))

  @Test
  def testProcTimeBoundedPartitionedRowsOver(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = StreamTableEnvironment.create(env)
    env.setParallelism(1)
    StreamITCase.clear

    val t = StreamTestData.get5TupleDataStream(env)
      .toTable(tEnv, 'a, 'b, 'c, 'd, 'e, 'proctime.proctime)
    tEnv.registerTable("MyTable", t)

    val sqlQuery = "SELECT a, " +
      "  SUM(c) OVER (" +
      "    PARTITION BY a ORDER BY proctime ROWS BETWEEN 4 PRECEDING AND CURRENT ROW), " +
      "  MIN(c) OVER (" +
      "    PARTITION BY a ORDER BY proctime ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) " +
      "FROM MyTable"

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List(
      "1,0,0",
      "2,1,1",
      "2,3,1",
      "3,3,3",
      "3,7,3",
      "3,12,3",
      "4,6,6",
      "4,13,6",
      "4,21,6",
      "4,30,6",
      "5,10,10",
      "5,21,10",
      "5,33,10",
      "5,46,10",
      "5,60,10")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testProcTimeBoundedNonPartitionedRowsOver(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    env.setStateBackend(getStateBackend)
    env.setParallelism(1)
    StreamITCase.clear

    val t = StreamTestData.get5TupleDataStream(env)
      .toTable(tEnv, 'a, 'b, 'c, 'd, 'e, 'proctime.proctime)
    tEnv.registerTable("MyTable", t)

    val sqlQuery = "SELECT a, " +
      "  SUM(c) OVER (" +
      "    ORDER BY proctime ROWS BETWEEN 10 PRECEDING AND CURRENT ROW), " +
      "  MIN(c) OVER (" +
      "    ORDER BY proctime ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) " +
      "FROM MyTable"
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List(
      "1,0,0",
      "2,1,0",
      "2,3,0",
      "3,6,0",
      "3,10,0",
      "3,15,0",
      "4,21,0",
      "4,28,0",
      "4,36,0",
      "4,45,0",
      "5,55,0",
      "5,66,1",
      "5,77,2",
      "5,88,3",
      "5,99,4")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testProcTimeUnboundedPartitionedRangeOver(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear

    // for sum aggregation ensure that every time the order of each element is consistent
    env.setParallelism(1)

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val sqlQuery = "SELECT " +
      "c, " +
      "count(a) OVER (PARTITION BY c ORDER BY proctime RANGE UNBOUNDED preceding), " +
      "sum(a) OVER (PARTITION BY c ORDER BY proctime RANGE UNBOUNDED preceding), " +
      "sum(2) OVER (PARTITION BY c ORDER BY proctime RANGE UNBOUNDED preceding) " +
      "from T1"

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List(
      "Hello World,1,7,2", "Hello World,2,15,4", "Hello World,3,35,6",
      "Hello,1,1,2", "Hello,2,3,4", "Hello,3,6,6", "Hello,4,10,8", "Hello,5,15,10", "Hello,6,21,12")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testProcTimeUnboundedPartitionedRowsOver(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val sqlQuery = "SELECT c, cnt1 from " +
      "(SELECT " +
      "c, " +
      "count(1) " +
      " OVER (PARTITION BY c ORDER BY proctime ROWS BETWEEN UNBOUNDED preceding AND CURRENT ROW) " +
      "as cnt1 from T1)"

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List(
      "Hello World,1", "Hello World,2", "Hello World,3",
      "Hello,1", "Hello,2", "Hello,3", "Hello,4", "Hello,5", "Hello,6")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)

  }

  @Test
  def testProcTimeUnboundedNonPartitionedRangeOver(): Unit = {
    val queryConfig =
      new StreamQueryConfig().withIdleStateRetentionTime(Time.hours(2), Time.hours(3))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear

    // for sum aggregation ensure that every time the order of each element is consistent
    env.setParallelism(1)

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val sqlQuery = "SELECT " +
      "c, " +
      "count(a) OVER (ORDER BY proctime  RANGE UNBOUNDED preceding), " +
      "sum(a) OVER (ORDER BY proctime RANGE UNBOUNDED preceding) " +
      "from T1"

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row](queryConfig)
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List(
      "Hello World,7,28", "Hello World,8,36", "Hello World,9,56",
      "Hello,1,1", "Hello,2,3", "Hello,3,6", "Hello,4,10", "Hello,5,15", "Hello,6,21")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testProcTimeUnboundedNonPartitionedRowsOver(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val sqlQuery = "SELECT " +
      "count(a) OVER (ORDER BY proctime ROWS BETWEEN UNBOUNDED preceding AND CURRENT ROW) " +
      "from T1"

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List("1", "2", "3", "4", "5", "6", "7", "8", "9")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testRowTimeBoundedPartitionedRangeOver(): Unit = {
    val data = Seq(
      Left((1500L, (1L, 15, "Hello"))),
      Left((1600L, (1L, 16, "Hello"))),
      Left((1000L, (1L, 1, "Hello"))),
      Left((2000L, (2L, 2, "Hello"))),
      Right(1000L),
      Left((2000L, (2L, 2, "Hello"))),
      Left((2000L, (2L, 3, "Hello"))),
      Left((3000L, (3L, 3, "Hello"))),
      Right(2000L),
      Left((4000L, (4L, 4, "Hello"))),
      Right(3000L),
      Left((5000L, (5L, 5, "Hello"))),
      Right(5000L),
      Left((6000L, (6L, 6, "Hello"))),
      Left((6500L, (6L, 65, "Hello"))),
      Right(7000L),
      Left((9000L, (6L, 9, "Hello"))),
      Left((9500L, (6L, 18, "Hello"))),
      Left((9000L, (6L, 9, "Hello"))),
      Right(10000L),
      Left((10000L, (7L, 7, "Hello World"))),
      Left((11000L, (7L, 17, "Hello World"))),
      Left((11000L, (7L, 77, "Hello World"))),
      Right(12000L),
      Left((14000L, (7L, 18, "Hello World"))),
      Right(14000L),
      Left((15000L, (8L, 8, "Hello World"))),
      Right(17000L),
      Left((20000L, (20L, 20, "Hello World"))),
      Right(19000L))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear

    val t1 = env
      .addSource[(Long, Int, String)](new EventTimeSourceFunction[(Long, Int, String)](data))
      .toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)

    tEnv.registerTable("T1", t1)
    tEnv.registerFunction("LTCNT", new LargerThanCount)

    val sqlQuery = "SELECT " +
      "  c, b, " +
      "  LTCNT(a, CAST('4' AS BIGINT)) OVER (PARTITION BY c ORDER BY rowtime RANGE " +
      "    BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW), " +
      "  COUNT(a) OVER (PARTITION BY c ORDER BY rowtime RANGE " +
      "    BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW), " +
      "  SUM(a) OVER (PARTITION BY c ORDER BY rowtime RANGE " +
      "    BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW) " +
      " FROM T1"

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List(
      "Hello,1,0,1,1", "Hello,15,0,2,2", "Hello,16,0,3,3",
      "Hello,2,0,6,9", "Hello,3,0,6,9", "Hello,2,0,6,9",
      "Hello,3,0,4,9",
      "Hello,4,0,2,7",
      "Hello,5,1,2,9",
      "Hello,6,2,2,11", "Hello,65,2,2,12",
      "Hello,9,2,2,12", "Hello,9,2,2,12", "Hello,18,3,3,18",
      "Hello World,7,1,1,7", "Hello World,17,3,3,21", "Hello World,77,3,3,21",
      "Hello World,18,1,1,7",
      "Hello World,8,2,2,15",
      "Hello World,20,1,1,20")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testRowTimeBoundedPartitionedRowsOver(): Unit = {
    val data = Seq(
      Left((1L, (1L, 1, "Hello"))),
      Left((2L, (2L, 2, "Hello"))),
      Left((1L, (1L, 1, "Hello"))),
      Left((2L, (2L, 2, "Hello"))),
      Left((2L, (2L, 2, "Hello"))),
      Left((1L, (1L, 1, "Hello"))),
      Left((3L, (7L, 7, "Hello World"))),
      Left((1L, (7L, 7, "Hello World"))),
      Left((1L, (7L, 7, "Hello World"))),
      Right(2L),
      Left((3L, (3L, 3, "Hello"))),
      Left((4L, (4L, 4, "Hello"))),
      Left((5L, (5L, 5, "Hello"))),
      Left((6L, (6L, 6, "Hello"))),
      Left((20L, (20L, 20, "Hello World"))),
      Right(6L),
      Left((8L, (8L, 8, "Hello World"))),
      Left((7L, (7L, 7, "Hello World"))),
      Right(20L))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear

    val t1 = env
      .addSource[(Long, Int, String)](new EventTimeSourceFunction[(Long, Int, String)](data))
      .toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)

    tEnv.registerTable("T1", t1)
    tEnv.registerFunction("LTCNT", new LargerThanCount)

    val sqlQuery = "SELECT " +
      " c, a, " +
      "  LTCNT(a, CAST('4' AS BIGINT)) " +
      "    OVER (PARTITION BY c ORDER BY rowtime ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), " +
      "  COUNT(1) " +
      "    OVER (PARTITION BY c ORDER BY rowtime ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), " +
      "  SUM(a) " +
      "    OVER (PARTITION BY c ORDER BY rowtime ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) " +
      "FROM T1"

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List(
      "Hello,1,0,1,1", "Hello,1,0,2,2", "Hello,1,0,3,3",
      "Hello,2,0,3,4", "Hello,2,0,3,5", "Hello,2,0,3,6",
      "Hello,3,0,3,7", "Hello,4,0,3,9", "Hello,5,1,3,12",
      "Hello,6,2,3,15",
      "Hello World,7,1,1,7", "Hello World,7,2,2,14", "Hello World,7,3,3,21",
      "Hello World,7,3,3,21", "Hello World,8,3,3,22", "Hello World,20,3,3,35")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testRowTimeBoundedNonPartitionedRangeOver(): Unit = {
    val data = Seq(
      Left((1500L, (1L, 15, "Hello"))),
      Left((1600L, (1L, 16, "Hello"))),
      Left((1000L, (1L, 1, "Hello"))),
      Left((2000L, (2L, 2, "Hello"))),
      Right(1000L),
      Left((2000L, (2L, 2, "Hello"))),
      Left((2000L, (2L, 3, "Hello"))),
      Left((3000L, (3L, 3, "Hello"))),
      Right(2000L),
      Left((4000L, (4L, 4, "Hello"))),
      Right(3000L),
      Left((5000L, (5L, 5, "Hello"))),
      Right(5000L),
      Left((6000L, (6L, 6, "Hello"))),
      Left((6500L, (6L, 65, "Hello"))),
      Right(7000L),
      Left((9000L, (6L, 9, "Hello"))),
      Left((9500L, (6L, 18, "Hello"))),
      Left((9000L, (6L, 9, "Hello"))),
      Right(10000L),
      Left((10000L, (7L, 7, "Hello World"))),
      Left((11000L, (7L, 17, "Hello World"))),
      Left((11000L, (7L, 77, "Hello World"))),
      Right(12000L),
      Left((14000L, (7L, 18, "Hello World"))),
      Right(14000L),
      Left((15000L, (8L, 8, "Hello World"))),
      Right(17000L),
      Left((20000L, (20L, 20, "Hello World"))),
      Right(19000L))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear

    val t1 = env
      .addSource[(Long, Int, String)](new EventTimeSourceFunction[(Long, Int, String)](data))
      .toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)

    tEnv.registerTable("T1", t1)

    val sqlQuery = "SELECT " +
      "  c, b, " +
      "  COUNT(a) " +
      "    OVER (ORDER BY rowtime RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW), " +
      "  SUM(a) " +
      "    OVER (ORDER BY rowtime RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW) " +
      " FROM T1"

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List(
      "Hello,1,1,1", "Hello,15,2,2", "Hello,16,3,3",
      "Hello,2,6,9", "Hello,3,6,9", "Hello,2,6,9",
      "Hello,3,4,9",
      "Hello,4,2,7",
      "Hello,5,2,9",
      "Hello,6,2,11", "Hello,65,2,12",
      "Hello,9,2,12", "Hello,9,2,12", "Hello,18,3,18",
      "Hello World,7,4,25", "Hello World,17,3,21", "Hello World,77,3,21", "Hello World,18,1,7",
      "Hello World,8,2,15",
      "Hello World,20,1,20")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testRowTimeBoundedNonPartitionedRowsOver(): Unit = {

    val data = Seq(
      Left((2L, (2L, 2, "Hello"))),
      Left((2L, (2L, 2, "Hello"))),
      Left((1L, (1L, 1, "Hello"))),
      Left((1L, (1L, 1, "Hello"))),
      Left((2L, (2L, 2, "Hello"))),
      Left((1L, (1L, 1, "Hello"))),
      Left((20L, (20L, 20, "Hello World"))), // early row
      Right(3L),
      Left((2L, (2L, 2, "Hello"))), // late row
      Left((3L, (3L, 3, "Hello"))),
      Left((4L, (4L, 4, "Hello"))),
      Left((5L, (5L, 5, "Hello"))),
      Left((6L, (6L, 6, "Hello"))),
      Left((7L, (7L, 7, "Hello World"))),
      Right(7L),
      Left((9L, (9L, 9, "Hello World"))),
      Left((8L, (8L, 8, "Hello World"))),
      Left((8L, (8L, 8, "Hello World"))),
      Right(20L))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    env.setParallelism(1)
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear

    val t1 = env
      .addSource[(Long, Int, String)](new EventTimeSourceFunction[(Long, Int, String)](data))
      .toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)

    tEnv.registerTable("T1", t1)

    val sqlQuery = "SELECT " +
      "c, a, " +
      "  COUNT(a) OVER (ORDER BY rowtime ROWS BETWEEN 2 preceding AND CURRENT ROW), " +
      "  SUM(a) OVER (ORDER BY rowtime ROWS BETWEEN 2 preceding AND CURRENT ROW) " +
      "FROM T1"

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List(
      "Hello,1,1,1", "Hello,1,2,2", "Hello,1,3,3",
      "Hello,2,3,4", "Hello,2,3,5", "Hello,2,3,6",
      "Hello,3,3,7",
      "Hello,4,3,9", "Hello,5,3,12",
      "Hello,6,3,15", "Hello World,7,3,18",
      "Hello World,8,3,21", "Hello World,8,3,23",
      "Hello World,9,3,25",
      "Hello World,20,3,37")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testRowTimeUnBoundedPartitionedRangeOver(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    env.setParallelism(1)
    StreamITCase.clear

    val sqlQuery = "SELECT a, b, c, " +
      "  LTCNT(b, CAST('4' AS BIGINT)) OVER(" +
      "    PARTITION BY a ORDER BY rowtime RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), " +
      "  SUM(b) OVER (" +
      "    PARTITION BY a ORDER BY rowtime RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), " +
      "  COUNT(b) OVER (" +
      "    PARTITION BY a ORDER BY rowtime RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), " +
      "  AVG(b) OVER (" +
      "    PARTITION BY a ORDER BY rowtime RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), " +
      "  MAX(b) OVER (" +
      "    PARTITION BY a ORDER BY rowtime RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), " +
      "  MIN(b) OVER (" +
      "    PARTITION BY a ORDER BY rowtime RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) " +
      "FROM T1"

    val data = Seq(
      Left(14000005L, (1, 1L, "Hi")),
      Left(14000000L, (2, 1L, "Hello")),
      Left(14000002L, (1, 1L, "Hello")),
      Left(14000002L, (1, 2L, "Hello")),
      Left(14000002L, (1, 3L, "Hello world")),
      Left(14000003L, (2, 2L, "Hello world")),
      Left(14000003L, (2, 3L, "Hello world")),
      Right(14000020L),
      Left(14000021L, (1, 4L, "Hello world")),
      Left(14000022L, (1, 5L, "Hello world")),
      Left(14000022L, (1, 6L, "Hello world")),
      Left(14000022L, (1, 7L, "Hello world")),
      Left(14000023L, (2, 4L, "Hello world")),
      Left(14000023L, (2, 5L, "Hello world")),
      Right(14000030L))

    val t1 = env
      .addSource(new EventTimeSourceFunction[(Int, Long, String)](data))
      .toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)

    tEnv.registerTable("T1", t1)
    tEnv.registerFunction("LTCNT", new LargerThanCount)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List(
      "1,1,Hello,0,6,3,2,3,1",
      "1,2,Hello,0,6,3,2,3,1",
      "1,3,Hello world,0,6,3,2,3,1",
      "1,1,Hi,0,7,4,1,3,1",
      "2,1,Hello,0,1,1,1,1,1",
      "2,2,Hello world,0,6,3,2,3,1",
      "2,3,Hello world,0,6,3,2,3,1",
      "1,4,Hello world,0,11,5,2,4,1",
      "1,5,Hello world,3,29,8,3,7,1",
      "1,6,Hello world,3,29,8,3,7,1",
      "1,7,Hello world,3,29,8,3,7,1",
      "2,4,Hello world,1,15,5,3,5,1",
      "2,5,Hello world,1,15,5,3,5,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testRowTimeUnBoundedPartitionedRowsOver(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT a, b, c, " +
      "LTCNT(b, CAST('4' AS BIGINT)) over(" +
      "partition by a order by rowtime rows between unbounded preceding and current row), " +
      "SUM(b) over (" +
      "partition by a order by rowtime rows between unbounded preceding and current row), " +
      "count(b) over (" +
      "partition by a order by rowtime rows between unbounded preceding and current row), " +
      "avg(b) over (" +
      "partition by a order by rowtime rows between unbounded preceding and current row), " +
      "max(b) over (" +
      "partition by a order by rowtime rows between unbounded preceding and current row), " +
      "min(b) over (" +
      "partition by a order by rowtime rows between unbounded preceding and current row) " +
      "from T1"

    val data = Seq(
      Left(14000005L, (1, 1L, "Hi")),
      Left(14000000L, (2, 1L, "Hello")),
      Left(14000002L, (3, 1L, "Hello")),
      Left(14000003L, (1, 2L, "Hello")),
      Left(14000004L, (1, 3L, "Hello world")),
      Left(14000007L, (3, 2L, "Hello world")),
      Left(14000008L, (2, 2L, "Hello world")),
      Right(14000010L),
      Left(14000012L, (1, 5L, "Hello world")),
      Left(14000021L, (1, 6L, "Hello world")),
      Left(14000023L, (2, 5L, "Hello world")),
      Right(14000020L),
      Left(14000024L, (3, 5L, "Hello world")),
      Left(14000026L, (1, 7L, "Hello world")),
      Left(14000025L, (1, 8L, "Hello world")),
      Left(14000022L, (1, 9L, "Hello world")),
      Right(14000030L))

    val t1 = env.addSource(new EventTimeSourceFunction[(Int, Long, String)](data))
             .toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)

    tEnv.registerTable("T1", t1)
    tEnv.registerFunction("LTCNT", new LargerThanCount)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList(
      "1,2,Hello,0,2,1,2,2,2",
      "1,3,Hello world,0,5,2,2,3,2",
      "1,1,Hi,0,6,3,2,3,1",
      "2,1,Hello,0,1,1,1,1,1",
      "2,2,Hello world,0,3,2,1,2,1",
      "3,1,Hello,0,1,1,1,1,1",
      "3,2,Hello world,0,3,2,1,2,1",
      "1,5,Hello world,1,11,4,2,5,1",
      "1,6,Hello world,2,17,5,3,6,1",
      "1,9,Hello world,3,26,6,4,9,1",
      "1,8,Hello world,4,34,7,4,9,1",
      "1,7,Hello world,5,41,8,5,9,1",
      "2,5,Hello world,1,8,3,2,5,1",
      "3,5,Hello world,1,8,3,2,5,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testRowTimeUnBoundedNonPartitionedRangeOver(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    env.setParallelism(1)
    StreamITCase.clear

    val sqlQuery = "SELECT a, b, c, " +
      "  SUM(b) OVER (ORDER BY rowtime RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), " +
      "  COUNT(b) OVER (ORDER BY rowtime RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), " +
      "  AVG(b) OVER (ORDER BY rowtime RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), " +
      "  MAX(b) OVER (ORDER BY rowtime RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), " +
      "  MIN(b) OVER (ORDER BY rowtime RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) " +
      "FROM T1"

    val data = Seq(
      Left(14000005L, (1, 1L, "Hi")),
      Left(14000000L, (2, 1L, "Hello")),
      Left(14000002L, (1, 1L, "Hello")),
      Left(14000002L, (1, 2L, "Hello")),
      Left(14000002L, (1, 3L, "Hello world")),
      Left(14000003L, (2, 2L, "Hello world")),
      Left(14000003L, (2, 3L, "Hello world")),
      Right(14000020L),
      Left(14000021L, (1, 4L, "Hello world")),
      Left(14000022L, (1, 5L, "Hello world")),
      Left(14000022L, (1, 6L, "Hello world")),
      Left(14000022L, (1, 7L, "Hello world")),
      Left(14000023L, (2, 4L, "Hello world")),
      Left(14000023L, (2, 5L, "Hello world")),
      Right(14000030L))

    val t1 = env
      .addSource(new EventTimeSourceFunction[(Int, Long, String)](data))
      .toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List(
      "2,1,Hello,1,1,1,1,1",
      "1,1,Hello,7,4,1,3,1",
      "1,2,Hello,7,4,1,3,1",
      "1,3,Hello world,7,4,1,3,1",
      "2,2,Hello world,12,6,2,3,1",
      "2,3,Hello world,12,6,2,3,1",
      "1,1,Hi,13,7,1,3,1",
      "1,4,Hello world,17,8,2,4,1",
      "1,5,Hello world,35,11,3,7,1",
      "1,6,Hello world,35,11,3,7,1",
      "1,7,Hello world,35,11,3,7,1",
      "2,4,Hello world,44,13,3,7,1",
      "2,5,Hello world,44,13,3,7,1")

    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testRowTimeUnBoundedNonPartitionedRowsOver(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    StreamITCase.clear
    env.setParallelism(1)

    val sqlQuery = "SELECT a, b, c, " +
      "  SUM(b) OVER (ORDER BY rowtime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), " +
      "  COUNT(b) OVER (ORDER BY rowtime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), " +
      "  AVG(b) OVER (ORDER BY rowtime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), " +
      "  MAX(b) OVER (ORDER BY rowtime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), " +
      "  MIN(b) OVER (ORDER BY rowtime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) " +
      "FROM T1"

    val data = Seq(
      Left(14000005L, (1, 1L, "Hi")),
      Left(14000000L, (2, 2L, "Hello")),
      Left(14000002L, (3, 5L, "Hello")),
      Left(14000003L, (1, 3L, "Hello")),
      Left(14000004L, (3, 7L, "Hello world")),
      Left(14000007L, (4, 9L, "Hello world")),
      Left(14000008L, (5, 8L, "Hello world")),
      Right(14000010L),
      // this element will be discard because it is late
      Left(14000008L, (6, 8L, "Hello world")),
      Right(14000020L),
      Left(14000021L, (6, 8L, "Hello world")),
      Right(14000030L)
    )

    val t1 = env
      .addSource(new EventTimeSourceFunction[(Int, Long, String)](data))
      .toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList(
      "2,2,Hello,2,1,2,2,2",
      "3,5,Hello,7,2,3,5,2",
      "1,3,Hello,10,3,3,5,2",
      "3,7,Hello world,17,4,4,7,2",
      "1,1,Hi,18,5,3,7,1",
      "4,9,Hello world,27,6,4,9,1",
      "5,8,Hello world,35,7,5,9,1",
      "6,8,Hello world,43,8,5,9,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  /** test sliding event-time unbounded window with partition by **/
  @Test
  def testRowTimeUnBoundedPartitionedRowsOver2(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    StreamITCase.clear
    env.setParallelism(1)

    val sqlQuery = "SELECT a, b, c, " +
      "SUM(b) over (" +
      "partition by a order by rowtime rows between unbounded preceding and current row), " +
      "count(b) over (" +
      "partition by a order by rowtime rows between unbounded preceding and current row), " +
      "avg(b) over (" +
      "partition by a order by rowtime rows between unbounded preceding and current row), " +
      "max(b) over (" +
      "partition by a order by rowtime rows between unbounded preceding and current row), " +
      "min(b) over (" +
      "partition by a order by rowtime rows between unbounded preceding and current row) " +
      "from T1"

    val data = Seq(
      Left(14000005L, (1, 1L, "Hi")),
      Left(14000000L, (2, 1L, "Hello")),
      Left(14000002L, (3, 1L, "Hello")),
      Left(14000003L, (1, 2L, "Hello")),
      Left(14000004L, (1, 3L, "Hello world")),
      Left(14000007L, (3, 2L, "Hello world")),
      Left(14000008L, (2, 2L, "Hello world")),
      Right(14000010L),
      // the next 3 elements are late
      Left(14000008L, (1, 4L, "Hello world")),
      Left(14000008L, (2, 3L, "Hello world")),
      Left(14000008L, (3, 3L, "Hello world")),
      Left(14000012L, (1, 5L, "Hello world")),
      Right(14000020L),
      Left(14000021L, (1, 6L, "Hello world")),
      // the next 3 elements are late
      Left(14000019L, (1, 6L, "Hello world")),
      Left(14000018L, (2, 4L, "Hello world")),
      Left(14000018L, (3, 4L, "Hello world")),
      Left(14000022L, (2, 5L, "Hello world")),
      Left(14000022L, (3, 5L, "Hello world")),
      Left(14000024L, (1, 7L, "Hello world")),
      Left(14000023L, (1, 8L, "Hello world")),
      Left(14000021L, (1, 9L, "Hello world")),
      Right(14000030L)
    )

    val t1 = env.addSource(new EventTimeSourceFunction[(Int, Long, String)](data))
      .toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List(
      "1,2,Hello,2,1,2,2,2",
      "1,3,Hello world,5,2,2,3,2",
      "1,1,Hi,6,3,2,3,1",
      "2,1,Hello,1,1,1,1,1",
      "2,2,Hello world,3,2,1,2,1",
      "3,1,Hello,1,1,1,1,1",
      "3,2,Hello world,3,2,1,2,1",
      "1,5,Hello world,11,4,2,5,1",
      "1,6,Hello world,17,5,3,6,1",
      "1,9,Hello world,26,6,4,9,1",
      "1,8,Hello world,34,7,4,9,1",
      "1,7,Hello world,41,8,5,9,1",
      "2,5,Hello world,8,3,2,5,1",
      "3,5,Hello world,8,3,2,5,1"
    )
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testProcTimeDistinctBoundedPartitionedRowsOver(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = StreamTableEnvironment.create(env)
    env.setParallelism(1)
    StreamITCase.clear

    val t = StreamTestData.get5TupleDataStream(env)
      .toTable(tEnv, 'a, 'b, 'c, 'd, 'e, 'proctime.proctime)
    tEnv.registerTable("MyTable", t)

    val sqlQuery = "SELECT a, " +
      "  SUM(DISTINCT e) OVER (" +
      "    PARTITION BY a ORDER BY proctime ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), " +
      "  MIN(DISTINCT e) OVER (" +
      "    PARTITION BY a ORDER BY proctime ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), " +
      "  COLLECT(DISTINCT e) OVER (" +
      "    PARTITION BY a ORDER BY proctime ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) " +
      "FROM MyTable"

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List(
      "1,1,1,{1=1}",
      "2,2,2,{2=1}",
      "2,3,1,{1=1, 2=1}",
      "3,2,2,{2=1}",
      "3,2,2,{2=1}",
      "3,5,2,{2=1, 3=1}",
      "4,2,2,{2=1}",
      "4,3,1,{1=1, 2=1}",
      "4,3,1,{1=1, 2=1}",
      "4,3,1,{1=1, 2=1}",
      "5,1,1,{1=1}",
      "5,4,1,{1=1, 3=1}",
      "5,4,1,{1=1, 3=1}",
      "5,6,1,{1=1, 2=1, 3=1}",
      "5,5,2,{2=1, 3=1}")
    assertEquals(expected, StreamITCase.testResults)
  }

  @Test
  def testProcTimeDistinctUnboundedPartitionedRowsOver(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = StreamTableEnvironment.create(env)
    env.setParallelism(1)
    StreamITCase.clear

    val t = StreamTestData.get5TupleDataStream(env)
      .toTable(tEnv, 'a, 'b, 'c, 'd, 'e, 'proctime.proctime)
    tEnv.registerTable("MyTable", t)
    tEnv.registerFunction("myCount", new MultiArgCount)

    val sqlQuery = "SELECT a, " +
      "  COUNT(e) OVER (" +
      "    PARTITION BY a ORDER BY proctime RANGE UNBOUNDED preceding), " +
      "  SUM(DISTINCT e) OVER (" +
      "    PARTITION BY a ORDER BY proctime RANGE UNBOUNDED preceding), " +
      "  MIN(DISTINCT e) OVER (" +
      "    PARTITION BY a ORDER BY proctime RANGE UNBOUNDED preceding), " +
      "  myCount(DISTINCT e, 1) OVER (" +
      "    PARTITION BY a ORDER BY proctime RANGE UNBOUNDED preceding), " +
      "  myCount(DISTINCT 1, e) OVER (" +
      "    PARTITION BY a ORDER BY proctime RANGE UNBOUNDED preceding) " +
      "FROM MyTable"

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List(
      "1,1,1,1,1,1",
      "2,1,2,2,1,1",
      "2,2,3,1,2,2",
      "3,1,2,2,1,1",
      "3,2,2,2,1,1",
      "3,3,5,2,2,2",
      "4,1,2,2,1,1",
      "4,2,3,1,2,2",
      "4,3,3,1,2,2",
      "4,4,3,1,2,2",
      "5,1,1,1,1,1",
      "5,2,4,1,2,2",
      "5,3,4,1,2,2",
      "5,4,6,1,3,3",
      "5,5,6,1,3,3")
    assertEquals(expected, StreamITCase.testResults)
  }

  @Test
  def testProcTimeDistinctUnboundedPartitionedRangeOverWithNullValues(): Unit = {
    val data = List(
      (1L, 1, null),
      (2L, 1, null),
      (3L, 2, null),
      (4L, 1, "Hello"),
      (5L, 1, "Hello"),
      (6L, 2, "Hello"),
      (7L, 1, "Hello World"),
      (8L, 2, "Hello World"),
      (9L, 2, "Hello World"),
      (10L, 1, null))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear

    // for sum aggregation ensure that every time the order of each element is consistent
    env.setParallelism(1)

    val table = env.fromCollection(data)
      .assignAscendingTimestamps(_._1)
      .toTable(tEnv, 'a, 'b, 'c, 'rtime.rowtime)

    tEnv.registerTable("MyTable", table)
    tEnv.registerFunction("CntNullNonNull", new CountNullNonNull)
    tEnv.registerFunction("myCount", new MultiArgCount)

    val sqlQuery = "SELECT " +
      "  c, " +
      "  b, " +
      "  COUNT(DISTINCT c) " +
      "    OVER (PARTITION BY b ORDER BY rtime RANGE UNBOUNDED preceding), " +
      "  CntNullNonNull(DISTINCT c) " +
      "    OVER (PARTITION BY b ORDER BY rtime RANGE UNBOUNDED preceding), " +
      "  myCount(DISTINCT c, 1) " +
      "    OVER (PARTITION BY b ORDER BY rtime RANGE UNBOUNDED preceding), " +
      "  myCount(DISTINCT 1, c) " +
      "    OVER (PARTITION BY b ORDER BY rtime RANGE UNBOUNDED preceding)" +
      "FROM " +
      "  MyTable"

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List(
      "null,1,0,0|1,0,0", "null,1,0,0|1,0,0", "null,2,0,0|1,0,0", "null,1,2,2|1,2,2",
      "Hello,1,1,1|1,1,1", "Hello,1,1,1|1,1,1", "Hello,2,1,1|1,1,1",
      "Hello World,1,2,2|1,2,2", "Hello World,2,2,2|1,2,2", "Hello World,2,2,2|1,2,2")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testProcTimeDistinctPairWithNulls(): Unit = {

    val data = List(
      ("A", null),
      ("A", null),
      ("B", null),
      (null, "Hello"),
      ("A", "Hello"),
      ("A", "Hello"),
      (null, "Hello World"),
      (null, "Hello World"),
      ("A", "Hello World"),
      ("B", "Hello World"))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = StreamTableEnvironment.create(env)
    env.setParallelism(1)
    StreamITCase.clear

    val table = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'proctime.proctime)
    tEnv.registerTable("MyTable", table)
    tEnv.registerFunction("PairCount", new CountPairs)

    val sqlQuery = "SELECT a, b, " +
      "  PairCount(a, b) OVER (ORDER BY proctime RANGE UNBOUNDED preceding), " +
      "  PairCount(DISTINCT a, b) OVER (ORDER BY proctime RANGE UNBOUNDED preceding) " +
      "FROM MyTable"

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List(
      "A,null,1,1",
      "A,null,2,1",
      "B,null,3,2",
      "null,Hello,4,3",
      "A,Hello,5,4",
      "A,Hello,6,4",
      "null,Hello World,7,5",
      "null,Hello World,8,5",
      "A,Hello World,9,6",
      "B,Hello World,10,7")
    assertEquals(expected, StreamITCase.testResults)
  }
}

object OverWindowITCase {

  class EventTimeSourceFunction[T](
      dataWithTimestampList: Seq[Either[(Long, T), Long]]) extends SourceFunction[T] {
    override def run(ctx: SourceContext[T]): Unit = {
      dataWithTimestampList.foreach {
        case Left(t) => ctx.collectWithTimestamp(t._2, t._1)
        case Right(w) => ctx.emitWatermark(new Watermark(w))
      }
    }

    override def cancel(): Unit = ???
  }
}

/** Counts how often the first argument was larger than the second argument. */
class LargerThanCount extends AggregateFunction[Long, Tuple1[Long]] {

  def accumulate(acc: Tuple1[Long], a: Long, b: Long): Unit = {
    if (a > b) acc.f0 += 1
  }

  def retract(acc: Tuple1[Long], a: Long, b: Long): Unit = {
    if (a > b) acc.f0 -= 1
  }

  override def createAccumulator(): Tuple1[Long] = Tuple1.of(0L)

  override def getValue(acc: Tuple1[Long]): Long = acc.f0
}

class CountNullNonNull extends AggregateFunction[String, Tuple2[Long, Long]] {

  override def createAccumulator(): Tuple2[Long, Long] = Tuple2.of(0L, 0L)

  override def getValue(acc: Tuple2[Long, Long]): String = s"${acc.f0}|${acc.f1}"

  def accumulate(acc: Tuple2[Long, Long], v: String): Unit = {
    if (v == null) {
      acc.f1 += 1
    } else {
      acc.f0 += 1
    }
  }

  def retract(acc: Tuple2[Long, Long], v: String): Unit = {
    if (v == null) {
      acc.f1 -= 1
    } else {
      acc.f0 -= 1
    }
  }
}

class CountPairs extends AggregateFunction[Long, Tuple1[Long]] {

  def accumulate(acc: Tuple1[Long], a: String, b: String): Unit = {
    acc.f0 += 1
  }

  def retract(acc: Tuple1[Long], a: String, b: String): Unit = {
    acc.f0 -= 1
  }

  override def createAccumulator(): Tuple1[Long] = Tuple1.of(0L)

  override def getValue(acc: Tuple1[Long]): Long = acc.f0
}
