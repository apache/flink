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
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.{TableEnvironment, TableException}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.stream.utils.{StreamITCase, StreamTestData, StreamingWithStateTestBase}
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit._

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

  /**
    *  All aggregates must be computed on the same window.
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

    val t1 = env.addSource[(Int, Long, String)](new SourceFunction[(Int, Long, String)] {
      override def run(ctx: SourceContext[(Int, Long, String)]): Unit = {
        ctx.collectWithTimestamp((1, 1L, "Hi"), 14000005L)
        ctx.collectWithTimestamp((2, 1L, "Hello"), 14000000L)
        ctx.collectWithTimestamp((3, 1L, "Hello"), 14000002L)
        ctx.collectWithTimestamp((1, 2L, "Hello"), 14000003L)
        ctx.collectWithTimestamp((1, 3L, "Hello world"), 14000004L)
        ctx.collectWithTimestamp((3, 2L, "Hello world"), 14000007L)
        ctx.collectWithTimestamp((2, 2L, "Hello world"), 14000008L)
        ctx.emitWatermark(new Watermark(14000010L))
        // the next 3 elements will be discard cause later.
        ctx.collectWithTimestamp((1, 4L, "Hello world"), 14000008L)
        ctx.collectWithTimestamp((2, 3L, "Hello world"), 14000008L)
        ctx.collectWithTimestamp((3, 3L, "Hello world"), 14000008L)
        ctx.collectWithTimestamp((1, 5L, "Hello world"), 14000012L)
        ctx.emitWatermark(new Watermark(14000020L))
        ctx.collectWithTimestamp((1, 6L, "Hello world"), 14000021L)
        // the next 3 elements will be discard cause late
        ctx.collectWithTimestamp((1, 6L, "Hello world"), 14000019L)
        ctx.collectWithTimestamp((2, 4L, "Hello world"), 14000018L)
        ctx.collectWithTimestamp((3, 4L, "Hello world"), 14000018L)
        ctx.collectWithTimestamp((2, 5L, "Hello world"), 14000022L)
        ctx.collectWithTimestamp((3, 5L, "Hello world"), 14000022L)
        ctx.collectWithTimestamp((1, 7L, "Hello world"), 14000024L)
        ctx.collectWithTimestamp((1, 8L, "Hello world"), 14000023L)
        ctx.collectWithTimestamp((1, 9L, "Hello world"), 14000021L)
        ctx.emitWatermark(new Watermark(14000030L))
      }

      override def cancel(): Unit = {}
    }).toTable(tEnv).as('a, 'b, 'c)

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

    val t1 = env.addSource[(Int, Long, String)](new SourceFunction[(Int, Long, String)] {
      override def run(ctx: SourceContext[(Int, Long, String)]): Unit = {
        ctx.collectWithTimestamp((1, 1L, "Hi"), 14000005L)
        ctx.collectWithTimestamp((2, 1L, "Hello"), 14000000L)
        ctx.collectWithTimestamp((3, 1L, "Hello"), 14000002L)
        ctx.collectWithTimestamp((1, 2L, "Hello"), 14000003L)
        ctx.collectWithTimestamp((1, 3L, "Hello world"), 14000004L)
        ctx.collectWithTimestamp((3, 2L, "Hello world"), 14000007L)
        ctx.collectWithTimestamp((2, 2L, "Hello world"), 14000008L)
        ctx.emitWatermark(new Watermark(14000010L))
        ctx.collectWithTimestamp((1, 5L, "Hello world"), 14000012L)
        ctx.emitWatermark(new Watermark(14000020L))
        ctx.collectWithTimestamp((1, 6L, "Hello world"), 14000021L)
        ctx.collectWithTimestamp((2, 5L, "Hello world"), 14000023L)
        ctx.collectWithTimestamp((3, 5L, "Hello world"), 14000024L)
        ctx.collectWithTimestamp((1, 7L, "Hello world"), 14000026L)
        ctx.collectWithTimestamp((1, 8L, "Hello world"), 14000025L)
        ctx.collectWithTimestamp((1, 9L, "Hello world"), 14000022L)
        ctx.emitWatermark(new Watermark(14000030L))
      }

      override def cancel(): Unit = {}
    }).toTable(tEnv).as('a, 'b, 'c)

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
      "SUM(b) over (order by rowtime() range between unbounded preceding and current row), " +
      "count(b) over (order by rowtime() range between unbounded preceding and current row), " +
      "avg(b) over (order by rowtime() range between unbounded preceding and current row), " +
      "max(b) over (order by rowtime() range between unbounded preceding and current row), " +
      "min(b) over (order by rowtime() range between unbounded preceding and current row) " +
      "from T1"

    val t1 = env.addSource[(Int, Long, String)](new SourceFunction[(Int, Long, String)] {
      override def run(ctx: SourceContext[(Int, Long, String)]): Unit = {
        ctx.collectWithTimestamp((1, 1L, "Hi"), 14000005L)
        ctx.collectWithTimestamp((2, 2L, "Hello"), 14000000L)
        ctx.collectWithTimestamp((3, 5L, "Hello"), 14000002L)
        ctx.collectWithTimestamp((1, 3L, "Hello"), 14000003L)
        ctx.collectWithTimestamp((3, 7L, "Hello world"), 14000004L)
        ctx.collectWithTimestamp((4, 9L, "Hello world"), 14000007L)
        ctx.collectWithTimestamp((5, 8L, "Hello world"), 14000008L)
        ctx.emitWatermark(new Watermark(14000010L))
        // this element will be discard cause late
        ctx.collectWithTimestamp((6, 8L, "Hello world"), 14000008L)
        ctx.emitWatermark(new Watermark(15000020L))
        ctx.collectWithTimestamp((6, 8L, "Hello world"), 15000021L)
        ctx.emitWatermark(new Watermark(15000030L))
      }

      override def cancel(): Unit = {}
    }).toTable(tEnv).as('a, 'b, 'c)

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
      "SUM(b) over (order by rowtime() range between unbounded preceding and current row), " +
      "count(b) over (order by rowtime() range between unbounded preceding and current row), " +
      "avg(b) over (order by rowtime() range between unbounded preceding and current row), " +
      "max(b) over (order by rowtime() range between unbounded preceding and current row), " +
      "min(b) over (order by rowtime() range between unbounded preceding and current row) " +
      "from T1"

    val t1 = env.addSource[(Int, Long, String)](new SourceFunction[(Int, Long, String)] {
      override def run(ctx: SourceContext[(Int, Long, String)]): Unit = {
        ctx.collectWithTimestamp((1, 1L, "Hi"), 14000005L)
        ctx.collectWithTimestamp((2, 2L, "Hello"), 14000000L)
        ctx.collectWithTimestamp((3, 5L, "Hello"), 14000002L)
        ctx.collectWithTimestamp((1, 3L, "Hello"), 14000003L)
        ctx.collectWithTimestamp((3, 7L, "Hello world"), 14000012L)
        ctx.collectWithTimestamp((4, 9L, "Hello world"), 14000013L)
        ctx.collectWithTimestamp((5, 8L, "Hello world"), 14000014L)
        ctx.emitWatermark(new Watermark(14000010L))
        ctx.collectWithTimestamp((6, 8L, "Hello world"), 14000011L)
        ctx.collectWithTimestamp((6, 8L, "Hello world"), 14000021L)
        ctx.emitWatermark(new Watermark(14000020L))
        ctx.emitWatermark(new Watermark(14000030L))
      }

      override def cancel(): Unit = {}
    }).toTable(tEnv).as('a, 'b, 'c)

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
}
