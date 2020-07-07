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

package org.apache.flink.table.planner.runtime.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.{CountDistinct, CountDistinctWithRetractAndReset, WeightedAvg}
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.JavaFunc0
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.runtime.utils.TimeTestUtil.EventTimeProcessOperator
import org.apache.flink.table.planner.runtime.utils.{StreamingWithStateTestBase, TestingAppendSink}
import org.apache.flink.table.planner.utils.CountAggFunction
import org.apache.flink.types.Row

import org.junit.Assert._
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.mutable

@RunWith(classOf[Parameterized])
class OverWindowITCase(mode: StateBackendMode) extends StreamingWithStateTestBase(mode) {

  @Before
  def setupEnv(): Unit = {
    // unaligned checkpoints are regenerating watermarks after recovery of in-flight data
    // https://issues.apache.org/jira/browse/FLINK-18405
    env.getCheckpointConfig.enableUnalignedCheckpoints(false)
  }

  @Test
  def testProcTimeUnBoundedPartitionedRowOver(): Unit = {

    val data = List(
      (1L, 1, "Hello"),
      (2L, 2, "Hello"),
      (3L, 3, "Hello"),
      (4L, 4, "Hello"),
      (5L, 5, "Hello"),
      (6L, 6, "Hello"),
      (7L, 7, "Hello World"),
      (8L, 8, "Hello World"),
      (8L, 8, "Hello World"),
      (20L, 20, "Hello World"),
      (20L, 20, null.asInstanceOf[String]))

    val stream = failingDataSource(data)
    val table = stream.toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)
    val countFun = new CountAggFunction
    val weightAvgFun = new WeightedAvg
    val countDist = new CountDistinct

    val windowedTable = table//.select('a, 'b, 'c, proctime() as 'proctime)
      .window(
      Over partitionBy 'c orderBy 'proctime preceding UNBOUNDED_ROW as 'w)
      .select('c,
        countFun('b) over 'w as 'mycount,
        weightAvgFun('a, 'b) over 'w as 'wAvg,
        countDist('a) over 'w as 'countDist)
      .select('c, 'mycount, 'wAvg, 'countDist)

    val sink = new TestingAppendSink
    windowedTable.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "Hello World,1,7,1", "Hello World,2,7,2", "Hello World,3,7,2", "Hello World,4,13,3",
      "Hello,1,1,1", "Hello,2,1,2", "Hello,3,2,3", "Hello,4,3,4", "Hello,5,3,5", "Hello,6,4,6",
      "null,1,20,1")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testOverWindowWithConstant(): Unit = {

    val data = List(
      (1L, 1, "Hello"),
      (2L, 2, "Hello"),
      (3L, 3, "Hello"),
      (4L, 4, "Hello"),
      (5L, 5, "Hello"),
      (6L, 6, "Hello"),
      (7L, 7, "Hello World"),
      (8L, 8, "Hello World"),
      (8L, 8, "Hello World"),
      (20L, 20, "Hello World"))

    val stream = failingDataSource(data)
    val table = stream.toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)
    val weightAvgFun = new WeightedAvg

    val windowedTable = table
      .window(
        Over partitionBy 'c orderBy 'proctime preceding UNBOUNDED_ROW as 'w)
      .select('c, weightAvgFun('a, 42, 'b, "2") over 'w as 'wAvg)

    val sink = new TestingAppendSink
    windowedTable.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "Hello World,12", "Hello World,9", "Hello World,9", "Hello World,9", "Hello,3",
      "Hello,3", "Hello,4", "Hello,4", "Hello,5", "Hello,5")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testRowTimeUnBoundedPartitionedRangeOver(): Unit = {
    val data: Seq[Either[(Long, (Int, Long, String)), Long]] = Seq(
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

    val source = failingDataSource(data)
    val table = source.transform("TimeAssigner", new EventTimeProcessOperator[(Int, Long, String)])
      .setParallelism(source.parallelism)
      .toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)
    val countFun = new CountAggFunction
    val weightAvgFun = new WeightedAvg
    val plusOne = new JavaFunc0
    val countDist = new CountDistinct

    val windowedTable = table
      .window(Over partitionBy 'a orderBy 'rowtime preceding UNBOUNDED_RANGE following
        CURRENT_RANGE as 'w)
      .select(
        'a, 'b, 'c,
        'b.sum over 'w,
        "SUM:".toExpr + ('b.sum over 'w),
        countFun('b) over 'w,
        (countFun('b) over 'w) + 1,
        plusOne(countFun('b) over 'w),
        array('b.avg over 'w, 'b.max over 'w),
        'b.avg over 'w,
        'b.max over 'w,
        'b.min over 'w,
        ('b.min over 'w).abs(),
        weightAvgFun('b, 'a) over 'w,
        countDist('c) over 'w as 'countDist)

    val sink = new TestingAppendSink
    windowedTable.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      "1,1,Hello,6,SUM:6,3,4,4,[2, 3],2,3,1,1,2,2",
      "1,2,Hello,6,SUM:6,3,4,4,[2, 3],2,3,1,1,2,2",
      "1,3,Hello world,6,SUM:6,3,4,4,[2, 3],2,3,1,1,2,2",
      "1,1,Hi,7,SUM:7,4,5,5,[1, 3],1,3,1,1,1,3",
      "2,1,Hello,1,SUM:1,1,2,2,[1, 1],1,1,1,1,1,1",
      "2,2,Hello world,6,SUM:6,3,4,4,[2, 3],2,3,1,1,2,2",
      "2,3,Hello world,6,SUM:6,3,4,4,[2, 3],2,3,1,1,2,2",
      "1,4,Hello world,11,SUM:11,5,6,6,[2, 4],2,4,1,1,2,3",
      "1,5,Hello world,29,SUM:29,8,9,9,[3, 7],3,7,1,1,3,3",
      "1,6,Hello world,29,SUM:29,8,9,9,[3, 7],3,7,1,1,3,3",
      "1,7,Hello world,29,SUM:29,8,9,9,[3, 7],3,7,1,1,3,3",
      "2,4,Hello world,15,SUM:15,5,6,6,[3, 5],3,5,1,1,3,2",
      "2,5,Hello world,15,SUM:15,5,6,6,[3, 5],3,5,1,1,3,2"
    )

    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testProcTimeBoundedPartitionedRowsOver(): Unit = {

    val data = List(
      (1, 1L, 0, "Hallo", 1L),
      (2, 2L, 1, "Hallo Welt", 2L),
      (2, 3L, 2, "Hallo Welt wie", 1L),
      (3, 4L, 3, "Hallo Welt wie gehts?", 2L),
      (3, 5L, 4, "ABC", 2L),
      (3, 6L, 5, "BCD", 3L),
      (4, 7L, 6, "CDE", 2L),
      (4, 8L, 7, "DEF", 1L),
      (4, 9L, 8, "EFG", 1L),
      (4, 10L, 9, "FGH", 2L),
      (5, 11L, 10, "GHI", 1L),
      (5, 12L, 11, "HIJ", 3L),
      (5, 13L, 12, "IJK", 3L),
      (5, 14L, 13, "JKL", 2L),
      (5, 15L, 14, "KLM", 2L))

    val countDist = new CountDistinctWithRetractAndReset
    val stream = failingDataSource(data)
    val table = stream.toTable(tEnv, 'a, 'b, 'c, 'd, 'e, 'proctime.proctime)

    val windowedTable = table.select('a, 'b, 'c, 'd, 'e, 'proctime)
      .window(Over partitionBy 'a orderBy 'proctime preceding 4.rows following CURRENT_ROW as 'w)
      .select('a, 'c.sum over 'w, 'c.min over 'w, countDist('e) over 'w)

    val sink = new TestingAppendSink
    windowedTable.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      "1,0,0,1",
      "2,1,1,1",
      "2,3,1,2",
      "3,3,3,1",
      "3,7,3,1",
      "3,12,3,2",
      "4,6,6,1",
      "4,13,6,2",
      "4,21,6,2",
      "4,30,6,2",
      "5,10,10,1",
      "5,21,10,2",
      "5,33,10,2",
      "5,46,10,3",
      "5,60,10,3")

    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testProcTimeBoundedPartitionedRowsOverWithJavaAPI(): Unit = {

    val data = List(
      (1, 1L, 0, "Hallo", 1L),
      (2, 2L, 1, "Hallo Welt", 2L),
      (2, 3L, 2, "Hallo Welt wie", 1L),
      (3, 4L, 3, "Hallo Welt wie gehts?", 2L),
      (3, 5L, 4, "ABC", 2L),
      (3, 6L, 5, "BCD", 3L),
      (4, 7L, 6, "CDE", 2L),
      (4, 8L, 7, "DEF", 1L),
      (4, 9L, 8, "EFG", 1L),
      (4, 10L, 9, "FGH", 2L),
      (5, 11L, 10, "GHI", 1L),
      (5, 12L, 11, "HIJ", 3L),
      (5, 13L, 12, "IJK", 3L),
      (5, 14L, 13, "JKL", 2L),
      (5, 15L, 14, "KLM", 2L))

    val countDist = new CountDistinctWithRetractAndReset
    val stream = failingDataSource(data)
    val table = stream.toTable(tEnv, 'a, 'b, 'c, 'd, 'e, 'proctime.proctime)

    val windowedTable = table.select($"a", $"b", $"c", $"d", $"e", $"proctime")
      .window(Over
        .partitionBy($("a"))
        .orderBy($("proctime"))
        .preceding(Expressions.rowInterval(4L))
        .following(Expressions.CURRENT_ROW)
        .as("w"))
      .select('a, 'c.sum over 'w, 'c.min over 'w, countDist('e) over 'w)

    val sink = new TestingAppendSink
    windowedTable.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      "1,0,0,1",
      "2,1,1,1",
      "2,3,1,2",
      "3,3,3,1",
      "3,7,3,1",
      "3,12,3,2",
      "4,6,6,1",
      "4,13,6,2",
      "4,21,6,2",
      "4,30,6,2",
      "5,10,10,1",
      "5,21,10,2",
      "5,33,10,2",
      "5,46,10,3",
      "5,60,10,3")

    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testRowTimeBoundedPartitionedRowOver(): Unit = {
    val data: Seq[Either[(Long, (Long, Int, String)), Long]] = Seq(
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

    val countDist = new CountDistinctWithRetractAndReset
    val source = failingDataSource(data)
    val table = source.transform("TimeAssigner", new EventTimeProcessOperator[(Long, Int, String)])
      .setParallelism(source.parallelism)
      .toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)

    val windowedTable = table
      .window(Over partitionBy 'c orderBy 'rowtime preceding 2.rows following CURRENT_ROW as 'w)
      .select('c, 'a, 'a.count over 'w, 'a.sum over 'w, countDist('a) over 'w)

    val sink = new TestingAppendSink
    windowedTable.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      "Hello,1,1,1,1", "Hello,1,2,2,1", "Hello,1,3,3,1",
      "Hello,2,3,4,2", "Hello,2,3,5,2", "Hello,2,3,6,1",
      "Hello,3,3,7,2", "Hello,4,3,9,3", "Hello,5,3,12,3",
      "Hello,6,3,15,3",
      "Hello World,7,1,7,1", "Hello World,7,2,14,1", "Hello World,7,3,21,1",
      "Hello World,7,3,21,1", "Hello World,8,3,22,2", "Hello World,20,3,35,3")

    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testRowTimeBoundedPartitionedRangeOver(): Unit = {
    val data: Seq[Either[(Long, (Long, Int, String)), Long]] = Seq(
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

    val countDist = new CountDistinctWithRetractAndReset
    val source = failingDataSource(data)
    val table = source.transform("TimeAssigner", new EventTimeProcessOperator[(Long, Int, String)])
      .setParallelism(source.parallelism)
      .toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)

    val windowedTable = table
      .window(
        Over partitionBy 'c orderBy 'rowtime preceding 1.seconds following CURRENT_RANGE as 'w)
      .select('c, 'b, 'a.count over 'w, 'a.sum over 'w, countDist('a) over 'w)

    val sink = new TestingAppendSink
    windowedTable.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      "Hello,1,1,1,1", "Hello,15,2,2,1", "Hello,16,3,3,1",
      "Hello,2,6,9,2", "Hello,3,6,9,2", "Hello,2,6,9,2",
      "Hello,3,4,9,2",
      "Hello,4,2,7,2",
      "Hello,5,2,9,2",
      "Hello,6,2,11,2", "Hello,65,2,12,1",
      "Hello,9,2,12,1", "Hello,9,2,12,1", "Hello,18,3,18,1",
      "Hello World,7,1,7,1", "Hello World,17,3,21,1",
      "Hello World,77,3,21,1", "Hello World,18,1,7,1",
      "Hello World,8,2,15,2",
      "Hello World,20,1,20,1")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testOverAggWithDiv(): Unit = {
    val data: Seq[Either[(Long, (Long, Int, String)), Long]] = Seq(
      Left((2L, (2L, 2, "Hello"))),
      Left((1L, (1L, 1, "Hello"))),
      Left((1L, (7L, 7, "Hello World"))),
      Right(2L),
      Left((6L, (6L, 6, "Hello"))),
      Left((20L, (20L, 20, "Hello World"))),
      Right(6L))

    val source = failingDataSource(data)
    val table = source.transform("TimeAssigner", new EventTimeProcessOperator[(Long, Int, String)])
      .setParallelism(source.parallelism)
      .toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)

    val windowedTable = table
      .window(Over partitionBy 'c orderBy 'rowtime preceding 2.rows following CURRENT_ROW as 'w)
      .select('c, 'a, 'a.count over 'w, ('a / 'a).sum over 'w)

    val sink = new TestingAppendSink
    windowedTable.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      "Hello World,20,2,2", "Hello World,7,1,1", "Hello,1,1,1",
      "Hello,2,2,2", "Hello,6,3,3")

    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

}
