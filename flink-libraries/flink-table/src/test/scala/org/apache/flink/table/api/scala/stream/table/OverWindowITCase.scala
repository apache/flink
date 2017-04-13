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

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.stream.table.OverWindowITCase.{RowTimeSourceFunction}
import org.apache.flink.table.api.scala.stream.utils.{StreamITCase, StreamingWithStateTestBase}
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class OverWindowITCase extends StreamingWithStateTestBase {

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
      (20L, 20, "Hello World"))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()
    StreamITCase.clear
    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'a, 'b, 'c)

    val windowedTable = table
      .window(
        Over partitionBy 'c orderBy 'procTime preceding UNBOUNDED_ROW as 'w)
      .select('c, 'b.count over 'w as 'mycount)
      .select('c, 'mycount)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq(
      "Hello World,1", "Hello World,2", "Hello World,3",
      "Hello,1", "Hello,2", "Hello,3", "Hello,4", "Hello,5", "Hello,6")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testRowTimeUnBoundedPartitionedRangeOver(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    StreamITCase.testResults = mutable.MutableList()
    StreamITCase.clear
    env.setParallelism(1)

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
    val table = env
      .addSource(new RowTimeSourceFunction[(Int, Long, String)](data))
      .toTable(tEnv).as('a, 'b, 'c)

    val windowedTable = table
      .window(Over partitionBy 'a orderBy 'rowtime preceding UNBOUNDED_RANGE following
         CURRENT_RANGE as 'w)
      .select(
        'a, 'b, 'c,
        'b.sum over 'w,
        'b.count over 'w,
        'b.avg over 'w,
        'b.max over 'w,
        'b.min over 'w)

    val result = windowedTable.toDataStream[Row]
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
  def testProcTimeBoundedPartitionedRangeOver(): Unit = {

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

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv).as('a, 'b, 'c, 'd, 'e)

    val windowedTable = table
      .window(Over partitionBy 'a orderBy 'proctime preceding 4.rows following CURRENT_ROW as 'w)
      .select('a, 'c.sum over 'w, 'c.min over 'w)
    val result = windowedTable.toDataStream[Row]
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
  def testRowTimeBoundedPartitionedRowOver(): Unit = {
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
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val table = env.addSource[(Long, Int, String)](
      new RowTimeSourceFunction[(Long, Int, String)](data)).toTable(tEnv).as('a, 'b, 'c)

    val windowedTable = table
      .window(Over partitionBy 'c orderBy 'rowtime preceding 2.rows following CURRENT_ROW as 'w)
      .select('c, 'a, 'a.count over 'w, 'a.sum over 'w)

    val result = windowedTable.toDataStream[Row]
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
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val table = env.addSource[(Long, Int, String)](
      new RowTimeSourceFunction[(Long, Int, String)](data)).toTable(tEnv).as('a, 'b, 'c)

    val windowedTable = table
      .window(
        Over partitionBy 'c orderBy 'rowtime preceding 1.seconds following CURRENT_RANGE as 'w)
      .select('c, 'b, 'a.count over 'w, 'a.sum over 'w)

    val result = windowedTable.toDataStream[Row]
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
}

object OverWindowITCase {

  class RowTimeSourceFunction[T](
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
