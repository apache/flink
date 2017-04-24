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
import org.apache.flink.table.api.scala.stream.sql.SqlITCase.EventTimeSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.{TableEnvironment, TableException}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.stream.utils.{StreamITCase, StreamTestData, StreamingWithStateTestBase}
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.collection.mutable

class SqlITCase extends StreamingWithStateTestBase {

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

  /** test selection **/
  @Test
  def testSelectExpressionFromTable(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT a * 2, b - 1 FROM MyTable"

    val t = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", t)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("2,0", "4,1", "6,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  /** test filtering with registered table **/
  @Test
  def testSimpleFilter(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT * FROM MyTable WHERE a = 3"

    val t = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", t)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("3,2,Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  /** test filtering with registered datastream **/
  @Test
  def testDatastreamFilter(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT * FROM MyTable WHERE _1 = 3"

    val t = StreamTestData.getSmall3TupleDataStream(env)
    tEnv.registerDataStream("MyTable", t)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("3,2,Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  /** test union with registered tables **/
  @Test
  def testUnion(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT * FROM T1 " +
      "UNION ALL " +
      "SELECT * FROM T2"

    val t1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("T1", t1)
    val t2 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("T2", t2)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,1,Hi", "1,1,Hi",
      "2,2,Hello", "2,2,Hello",
      "3,2,Hello world", "3,2,Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  /** test union with filter **/
  @Test
  def testUnionWithFilter(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT * FROM T1 WHERE a = 3 " +
      "UNION ALL " +
      "SELECT * FROM T2 WHERE a = 2"

    val t1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("T1", t1)
    val t2 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("T2", t2)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "2,2,Hello",
      "3,2,Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  /** test union of a table and a datastream **/
  @Test
  def testUnionTableWithDataSet(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT c FROM T1 WHERE a = 3 " +
      "UNION ALL " +
      "SELECT c FROM T2 WHERE a = 2"

    val t1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("T1", t1)
    val t2 = StreamTestData.get3TupleDataStream(env)
    tEnv.registerDataStream("T2", t2, 'a, 'b, 'c)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("Hello", "Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testUnboundPartitionedProcessingWindowWithRange(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    // for sum aggregation ensure that every time the order of each element is consistent
    env.setParallelism(1)

    val t1 = env.fromCollection(data).toTable(tEnv).as('a, 'b, 'c)

    tEnv.registerTable("T1", t1)

    val sqlQuery = "SELECT " +
      "c, " +
      "count(a) OVER (PARTITION BY c ORDER BY ProcTime()  RANGE UNBOUNDED preceding) as cnt1, " +
      "sum(a) OVER (PARTITION BY c ORDER BY ProcTime() RANGE UNBOUNDED preceding) as cnt2 " +
      "from T1"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "Hello World,1,7", "Hello World,2,15", "Hello World,3,35",
      "Hello,1,1", "Hello,2,3", "Hello,3,6", "Hello,4,10", "Hello,5,15", "Hello,6,21")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testUnboundPartitionedProcessingWindowWithRow(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val t1 = env.fromCollection(data).toTable(tEnv).as('a, 'b, 'c)

    tEnv.registerTable("T1", t1)

    val sqlQuery = "SELECT " +
      "c, " +
      "count(a) OVER (PARTITION BY c ORDER BY ProcTime() ROWS BETWEEN UNBOUNDED preceding AND " +
      "CURRENT ROW)" +
      "from T1"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "Hello World,1", "Hello World,2", "Hello World,3",
      "Hello,1", "Hello,2", "Hello,3", "Hello,4", "Hello,5", "Hello,6")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testUnboundNonPartitionedProcessingWindowWithRange(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    // for sum aggregation ensure that every time the order of each element is consistent
    env.setParallelism(1)

    val t1 = env.fromCollection(data).toTable(tEnv).as('a, 'b, 'c)

    tEnv.registerTable("T1", t1)

    val sqlQuery = "SELECT " +
      "c, " +
      "count(a) OVER (ORDER BY ProcTime()  RANGE UNBOUNDED preceding) as cnt1, " +
      "sum(a) OVER (ORDER BY ProcTime() RANGE UNBOUNDED preceding) as cnt2 " +
      "from T1"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "Hello World,7,28", "Hello World,8,36", "Hello World,9,56",
      "Hello,1,1", "Hello,2,3", "Hello,3,6", "Hello,4,10", "Hello,5,15", "Hello,6,21")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testUnboundNonPartitionedProcessingWindowWithRow(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val t1 = env.fromCollection(data).toTable(tEnv).as('a, 'b, 'c)

    tEnv.registerTable("T1", t1)

    val sqlQuery = "SELECT " +
      "count(a) OVER (ORDER BY ProcTime() ROWS BETWEEN UNBOUNDED preceding AND CURRENT ROW)" +
      "from T1"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("1", "2", "3", "4", "5", "6", "7", "8", "9")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testBoundPartitionedEventTimeWindowWithRow(): Unit = {
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
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t1 = env
      .addSource[(Long, Int, String)](new EventTimeSourceFunction[(Long, Int, String)](data))
      .toTable(tEnv).as('a, 'b, 'c)

    tEnv.registerTable("T1", t1)

    val sqlQuery = "SELECT " +
      "c, a, " +
      "count(a) OVER (PARTITION BY c ORDER BY RowTime() ROWS BETWEEN 2 preceding AND CURRENT ROW)" +
      ", sum(a) OVER (PARTITION BY c ORDER BY RowTime() ROWS BETWEEN 2 preceding AND CURRENT ROW)" +
      " from T1"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "Hello,1,1,1", "Hello,1,2,2", "Hello,1,3,3",
      "Hello,2,3,4", "Hello,2,3,5", "Hello,2,3,6",
      "Hello,3,3,7", "Hello,4,3,9", "Hello,5,3,12",
      "Hello,6,3,15",
      "Hello World,7,1,7", "Hello World,7,2,14", "Hello World,7,3,21",
      "Hello World,7,3,21", "Hello World,8,3,22", "Hello World,20,3,35")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testBoundNonPartitionedEventTimeWindowWithRow(): Unit = {

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
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t1 = env
      .addSource[(Long, Int, String)](new EventTimeSourceFunction[(Long, Int, String)](data))
      .toTable(tEnv).as('a, 'b, 'c)

    tEnv.registerTable("T1", t1)

    val sqlQuery = "SELECT " +
      "c, a, " +
      "count(a) OVER (ORDER BY RowTime() ROWS BETWEEN 2 preceding AND CURRENT ROW)," +
      "sum(a) OVER (ORDER BY RowTime() ROWS BETWEEN 2 preceding AND CURRENT ROW)" +
      "from T1"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
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
  def testBoundPartitionedEventTimeWindowWithRange(): Unit = {
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
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t1 = env
      .addSource[(Long, Int, String)](new EventTimeSourceFunction[(Long, Int, String)](data))
      .toTable(tEnv).as('a, 'b, 'c)

    tEnv.registerTable("T1", t1)

    val sqlQuery = "SELECT " +
      "c, b, " +
      "count(a) OVER (PARTITION BY c ORDER BY RowTime() RANGE BETWEEN INTERVAL '1' SECOND " +
      "preceding AND CURRENT ROW)" +
      ", sum(a) OVER (PARTITION BY c ORDER BY RowTime() RANGE BETWEEN INTERVAL '1' SECOND " +
      " preceding AND CURRENT ROW)" +
      " from T1"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "Hello,1,1,1", "Hello,15,2,2", "Hello,16,3,3",
      "Hello,2,6,9", "Hello,3,6,9", "Hello,2,6,9",
      "Hello,3,4,9",
      "Hello,4,2,7",
      "Hello,5,2,9",
      "Hello,6,2,11", "Hello,65,2,12",
      "Hello,9,2,12", "Hello,9,2,12", "Hello,18,3,18",
      "Hello World,7,1,7", "Hello World,17,3,21", "Hello World,77,3,21", "Hello World,18,1,7",
      "Hello World,8,2,15",
      "Hello World,20,1,20")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testBoundNonPartitionedEventTimeWindowWithRange(): Unit = {
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
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t1 = env
      .addSource[(Long, Int, String)](new EventTimeSourceFunction[(Long, Int, String)](data))
      .toTable(tEnv).as('a, 'b, 'c)

    tEnv.registerTable("T1", t1)

    val sqlQuery = "SELECT " +
      "c, b, " +
      "count(a) OVER (ORDER BY RowTime() RANGE BETWEEN INTERVAL '1' SECOND " +
      "preceding AND CURRENT ROW)" +
      ", sum(a) OVER (ORDER BY RowTime() RANGE BETWEEN INTERVAL '1' SECOND " +
      " preceding AND CURRENT ROW)" +
      " from T1"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
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

  /**
    * All aggregates must be computed on the same window.
    */
  @Test(expected = classOf[TableException])
  def testMultiWindow(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val t1 = env.fromCollection(data).toTable(tEnv).as('a, 'b, 'c)

    tEnv.registerTable("T1", t1)

    val sqlQuery = "SELECT " +
      "c, " +
      "count(a) OVER (PARTITION BY c ORDER BY ProcTime() RANGE UNBOUNDED preceding) as cnt1, " +
      "sum(a) OVER (PARTITION BY b ORDER BY ProcTime() RANGE UNBOUNDED preceding) as cnt2 " +
      "from T1"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()
  }

  /** test sliding event-time unbounded window with partition by **/
  @Test
  def testUnboundedEventTimeRowWindowWithPartition(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    StreamITCase.testResults = mutable.MutableList()
    env.setParallelism(1)

    val sqlQuery = "SELECT a, b, c, " +
      "SUM(b) over (" +
      "partition by a order by rowtime() rows between unbounded preceding and current row), " +
      "count(b) over (" +
      "partition by a order by rowtime() rows between unbounded preceding and current row), " +
      "avg(b) over (" +
      "partition by a order by rowtime() rows between unbounded preceding and current row), " +
      "max(b) over (" +
      "partition by a order by rowtime() rows between unbounded preceding and current row), " +
      "min(b) over (" +
      "partition by a order by rowtime() rows between unbounded preceding and current row) " +
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
      .toTable(tEnv).as('a, 'b, 'c)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
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

  /** test sliding event-time unbounded window with partition by **/
  @Test
  def testUnboundedEventTimeRowWindowWithPartitionMultiThread(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT a, b, c, " +
      "SUM(b) over (" +
      "partition by a order by rowtime() rows between unbounded preceding and current row), " +
      "count(b) over (" +
      "partition by a order by rowtime() rows between unbounded preceding and current row), " +
      "avg(b) over (" +
      "partition by a order by rowtime() rows between unbounded preceding and current row), " +
      "max(b) over (" +
      "partition by a order by rowtime() rows between unbounded preceding and current row), " +
      "min(b) over (" +
      "partition by a order by rowtime() rows between unbounded preceding and current row) " +
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
      Right(14000020L),
      Left(14000021L, (1, 6L, "Hello world")),
      Left(14000023L, (2, 5L, "Hello world")),
      Left(14000024L, (3, 5L, "Hello world")),
      Left(14000026L, (1, 7L, "Hello world")),
      Left(14000025L, (1, 8L, "Hello world")),
      Left(14000022L, (1, 9L, "Hello world")),
      Right(14000030L)
    )

    val t1 = env.addSource(new EventTimeSourceFunction[(Int, Long, String)](data))
      .toTable(tEnv).as('a, 'b, 'c)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
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

  /** test sliding event-time unbounded window without partitiion by **/
  @Test
  def testUnboundedEventTimeRowWindowWithoutPartition(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    StreamITCase.testResults = mutable.MutableList()
    env.setParallelism(1)

    val sqlQuery = "SELECT a, b, c, " +
      "SUM(b) over (order by rowtime() rows between unbounded preceding and current row), " +
      "count(b) over (order by rowtime() rows between unbounded preceding and current row), " +
      "avg(b) over (order by rowtime() rows between unbounded preceding and current row), " +
      "max(b) over (order by rowtime() rows between unbounded preceding and current row), " +
      "min(b) over (order by rowtime() rows between unbounded preceding and current row) " +
      "from T1"

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

    val t1 = env.addSource(new EventTimeSourceFunction[(Int, Long, String)](data))
      .toTable(tEnv).as('a, 'b, 'c)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
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

  /** test sliding event-time unbounded window without partitiion by and arrive early **/
  @Test
  def testUnboundedEventTimeRowWindowArriveEarly(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    StreamITCase.testResults = mutable.MutableList()
    env.setParallelism(1)

    val sqlQuery = "SELECT a, b, c, " +
      "SUM(b) over (order by rowtime() rows between unbounded preceding and current row), " +
      "count(b) over (order by rowtime() rows between unbounded preceding and current row), " +
      "avg(b) over (order by rowtime() rows between unbounded preceding and current row), " +
      "max(b) over (order by rowtime() rows between unbounded preceding and current row), " +
      "min(b) over (order by rowtime() rows between unbounded preceding and current row) " +
      "from T1"

    val data = Seq(
      Left(14000005L, (1, 1L, "Hi")),
      Left(14000000L, (2, 2L, "Hello")),
      Left(14000002L, (3, 5L, "Hello")),
      Left(14000003L, (1, 3L, "Hello")),
      // next three elements are early
      Left(14000012L, (3, 7L, "Hello world")),
      Left(14000013L, (4, 9L, "Hello world")),
      Left(14000014L, (5, 8L, "Hello world")),
      Right(14000010L),
      Left(14000011L, (6, 8L, "Hello world")),
      // next element is early
      Left(14000021L, (6, 8L, "Hello world")),
      Right(14000020L),
      Right(14000030L)
    )

    val t1 = env.addSource(new EventTimeSourceFunction[(Int, Long, String)](data))
      .toTable(tEnv).as('a, 'b, 'c)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "2,2,Hello,2,1,2,2,2",
      "3,5,Hello,7,2,3,5,2",
      "1,3,Hello,10,3,3,5,2",
      "1,1,Hi,11,4,2,5,1",
      "6,8,Hello world,19,5,3,8,1",
      "3,7,Hello world,26,6,4,8,1",
      "4,9,Hello world,35,7,5,9,1",
      "5,8,Hello world,43,8,5,9,1",
      "6,8,Hello world,51,9,5,9,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  /** test sliding event-time non-partitioned unbounded RANGE window **/
  @Test
  def testUnboundedNonPartitionedEventTimeRangeWindow(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    StreamITCase.testResults = mutable.MutableList()
    env.setParallelism(1)

    val sqlQuery = "SELECT a, b, c, " +
      "SUM(b) over (order by rowtime() range between unbounded preceding and current row), " +
      "count(b) over (order by rowtime() range between unbounded preceding and current row), " +
      "avg(b) over (order by rowtime() range between unbounded preceding and current row), " +
      "max(b) over (order by rowtime() range between unbounded preceding and current row), " +
      "min(b) over (order by rowtime() range between unbounded preceding and current row) " +
      "from T1"

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
      Right(14000030L)
    )

    val t1 = env.addSource(new EventTimeSourceFunction[(Int, Long, String)](data))
      .toTable(tEnv).as('a, 'b, 'c)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
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
      "2,5,Hello world,44,13,3,7,1"
    )
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  /** test sliding event-time unbounded RANGE window **/
  @Test
  def testUnboundedPartitionedEventTimeRangeWindow(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    StreamITCase.testResults = mutable.MutableList()
    env.setParallelism(1)

    val sqlQuery = "SELECT a, b, c, " +
      "SUM(b) over (" +
      "partition by a order by rowtime() range between unbounded preceding and current row), " +
      "count(b) over (" +
      "partition by a order by rowtime() range between unbounded preceding and current row), " +
      "avg(b) over (" +
      "partition by a order by rowtime() range between unbounded preceding and current row), " +
      "max(b) over (" +
      "partition by a order by rowtime() range between unbounded preceding and current row), " +
      "min(b) over (" +
      "partition by a order by rowtime() range between unbounded preceding and current row) " +
      "from T1"

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
      Right(14000030L)
    )

    val t1 = env.addSource(new EventTimeSourceFunction[(Int, Long, String)](data))
      .toTable(tEnv).as('a, 'b, 'c)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,1,Hello,6,3,2,3,1",
      "1,2,Hello,6,3,2,3,1",
      "1,3,Hello world,6,3,2,3,1",
      "1,1,Hi,7,4,1,3,1",
      "2,1,Hello,1,1,1,1,1",
      "2,2,Hello world,6,3,2,3,1",
      "2,3,Hello world,6,3,2,3,1",
      "1,4,Hello world,11,5,2,4,1",
      "1,5,Hello world,29,8,3,7,1",
      "1,6,Hello world,29,8,3,7,1",
      "1,7,Hello world,29,8,3,7,1",
      "2,4,Hello world,15,5,3,5,1",
      "2,5,Hello world,15,5,3,5,1"
    )
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testPartitionedProcTimeOverWindow(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
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
  def testPartitionedProcTimeOverWindow2(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    StreamITCase.testResults = mutable.MutableList()

    val t = StreamTestData.get5TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c, 'd, 'e)
    tEnv.registerTable("MyTable", t)

    val sqlQuery = "SELECT a,  " +
      " SUM(c) OVER (" +
      " PARTITION BY a ORDER BY procTime() ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS sumC , " +
      " MIN(c) OVER (" +
      " PARTITION BY a ORDER BY procTime() ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS minC " +
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
      "4,30,6",
      "5,10,10",
      "5,21,10",
      "5,33,10",
      "5,46,10",
      "5,60,10")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }


  @Test
  def testNonPartitionedProcTimeOverWindow(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    StreamITCase.testResults = mutable.MutableList()

    val t = StreamTestData.get5TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c, 'd, 'e)
    tEnv.registerTable("MyTable", t)

    val sqlQuery = "SELECT a,  " +
      " SUM(c) OVER (" +
      " ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sumC , " +
      " MIN(c) OVER (" +
      " ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS minC " +
      " FROM MyTable"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,0,0",
      "2,1,0",
      "2,3,0",
      "3,6,1",
      "3,9,2",
      "3,12,3",
      "4,15,4",
      "4,18,5",
      "4,21,6",
      "4,24,7",
      "5,27,8",
      "5,30,9",
      "5,33,10",
      "5,36,11",
      "5,39,12")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testNonPartitionedProcTimeOverWindow2(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    StreamITCase.testResults = mutable.MutableList()

    val t = StreamTestData.get5TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c, 'd, 'e)
    tEnv.registerTable("MyTable", t)

    val sqlQuery = "SELECT a,  " +
      " SUM(c) OVER (" +
      " ORDER BY procTime() ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) AS sumC , " +
      " MIN(c) OVER (" +
      " ORDER BY procTime() ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) AS minC " +
      " FROM MyTable"
    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
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

}

object SqlITCase {

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
