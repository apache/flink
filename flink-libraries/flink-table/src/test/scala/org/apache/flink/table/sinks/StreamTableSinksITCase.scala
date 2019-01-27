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

package org.apache.flink.table.sinks

import java.util.TimeZone

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.TableException
import org.apache.flink.table.runtime.utils._
import org.apache.flink.api.scala._
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit.{Ignore, Test}

import scala.collection.mutable

class StreamTableSinksITCase extends StreamingTestBase {

  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  @Test(expected = classOf[TableException])
  def testAppendSinkOnUpdatingTable(): Unit = {
    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'id, 'num, 'text)

    t.groupBy('text)
      .select('text, 'id.count, 'num.sum)
      .toAppendStream[Row].addSink(new TestingAppendSink)

    // must fail because table is not append-only
    env.execute()
  }

  @Test
  def testUpsertSinkOnUpdatingTableWithJoin(): Unit = {

    val t = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'id, 'num, 'text)

    val tableSink = new TestingUpsertTableSink(Array(0))
    val t1 = t.groupBy('id)
      .select('id as 'a)

    val t2 = t.groupBy('id)
      .select('id as 'd)

    t1.join(t2, 'a === 'd)
      .writeToSink(tableSink)
    env.execute()

    val expected = List(
      "1,1",
      "2,2",
      "3,3").sorted
    assertEquals(expected, tableSink.getUpsertResults.sorted)
  }

  @Test
  def testAppendSinkOnAppendTable(): Unit = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val t = StreamTestData.get3TupleDataStream(env)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    val tableSink = new TestingAppendSink
    t.window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w)
      .select('w.end, 'id.count, 'num.sum)
      .toAppendStream[Row].addSink(tableSink)

    env.execute()

    val expected = List(
      "1970-01-01 00:00:00.005,4,8",
      "1970-01-01 00:00:00.01,5,18",
      "1970-01-01 00:00:00.015,5,24",
      "1970-01-01 00:00:00.02,5,29",
      "1970-01-01 00:00:00.025,2,12")
      .sorted
    assertEquals(expected, tableSink.getAppendResults.sorted)
  }

  @Test
  def testRetractSinkOnUpdatingTable(): Unit = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val t = StreamTestData.get3TupleDataStream(env)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text)

    val tableSink = new TestingRetractSink
    t.select('id, 'num, 'text.charLength() as 'len)
      .groupBy('len)
      .select('len, 'id.count, 'num.sum)
      .toRetractStream[Row].addSink(tableSink).setParallelism(1)

    env.execute()

    val expected = List(
      "2,1,1",
      "5,1,2",
      "11,1,2",
      "25,1,3",
      "10,7,39",
      "14,1,3",
      "9,9,41").sorted
    assertEquals(expected, tableSink.getRetractResults.sorted)
  }

  @Test
  def testRetractSinkOnAppendTable(): Unit = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val t = StreamTestData.get3TupleDataStream(env)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    val tableSink = new TestingRetractSink
    t.window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w)
      .select('w.end, 'id.count, 'num.sum)
      .toRetractStream[Row].addSink(tableSink).setParallelism(1)

    env.execute()

    val expected = List(
      "1970-01-01 00:00:00.005,4,8",
      "1970-01-01 00:00:00.01,5,18",
      "1970-01-01 00:00:00.015,5,24",
      "1970-01-01 00:00:00.02,5,29",
      "1970-01-01 00:00:00.025,2,12")
      .sorted
    assertEquals(expected, tableSink.getRetractResults.sorted)
  }

  @Test
  def testUpsertSinkOnUpdatingTableWithFullKey(): Unit = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val t = StreamTestData.get3TupleDataStream(env)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text)

    val tableSink = new TestingUpsertTableSink(Array(0,2))
    t.select('id, 'num, 'text.charLength() as 'len, ('id > 0) as 'cTrue)
      .groupBy('len, 'cTrue)
      .select('len, 'id.count as 'cnt, 'cTrue)
      .groupBy('cnt, 'cTrue)
      .select('cnt, 'len.count, 'cTrue)
      .writeToSink(tableSink)

    env.execute()

    val expected = List(
      "1,5,true",
      "7,1,true",
      "9,1,true").sorted
    assertEquals(expected, tableSink.getUpsertResults.sorted)
  }

  @Ignore
  @Test(expected = classOf[TableException])
  def testUpsertSinkOnUpdatingTableWithoutFullKey(): Unit = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val t = StreamTestData.get3TupleDataStream(env)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text)

    t.select('id, 'num, 'text.charLength() as 'len, ('id > 0) as 'cTrue)
      .groupBy('len, 'cTrue)
      .select('len, 'id.count, 'num.sum)
      .writeToSink(new TestingUpsertTableSink(Array(0,1)))
//      .writeToSink(new TestUpsertSink(Array("len", "cTrue"), false))

    // must fail because table is updating table without full key
    env.execute()
  }

  @Test
  def testUpsertSinkOnAppendingTableWithFullKey1(): Unit = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val t = StreamTestData.get3TupleDataStream(env)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    val tableSink = new TestingUpsertTableSink(Array(0,1))
    t.window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w, 'num)
      .select('num, 'w.end as 'wend, 'id.count)
      .writeToSink(tableSink)

    env.execute()
    val expected = List(
      "1,1970-01-01 00:00:00.005,1",
      "2,1970-01-01 00:00:00.005,2",
      "3,1970-01-01 00:00:00.005,1",
      "3,1970-01-01 00:00:00.01,2",
      "4,1970-01-01 00:00:00.01,3",
      "4,1970-01-01 00:00:00.015,1",
      "5,1970-01-01 00:00:00.015,4",
      "5,1970-01-01 00:00:00.02,1",
      "6,1970-01-01 00:00:00.02,4",
      "6,1970-01-01 00:00:00.025,2").sorted
    assertEquals(expected, tableSink.getUpsertResults.sorted)
  }

  @Test
  def testUpsertSinkOnAppendingTableWithFullKey2(): Unit = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val t = StreamTestData.get3TupleDataStream(env)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    val tableSink = new TestingUpsertTableSink(Array(0,1,2))
    t.window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w, 'num)
      .select('w.start as 'wstart, 'w.end as 'wend, 'num, 'id.count)
      .writeToSink(tableSink)

    env.execute()
    val expected = List(
      "1970-01-01 00:00:00.0,1970-01-01 00:00:00.005,1,1",
      "1970-01-01 00:00:00.0,1970-01-01 00:00:00.005,2,2",
      "1970-01-01 00:00:00.0,1970-01-01 00:00:00.005,3,1",
      "1970-01-01 00:00:00.005,1970-01-01 00:00:00.01,3,2",
      "1970-01-01 00:00:00.005,1970-01-01 00:00:00.01,4,3",
      "1970-01-01 00:00:00.01,1970-01-01 00:00:00.015,4,1",
      "1970-01-01 00:00:00.01,1970-01-01 00:00:00.015,5,4",
      "1970-01-01 00:00:00.015,1970-01-01 00:00:00.02,5,1",
      "1970-01-01 00:00:00.015,1970-01-01 00:00:00.02,6,4",
      "1970-01-01 00:00:00.02,1970-01-01 00:00:00.025,6,2").sorted
    assertEquals(expected, tableSink.getUpsertResults.sorted)
  }

  @Test
  def testUpsertSinkOnAppendingTableWithoutFullKey1(): Unit = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val t = StreamTestData.get3TupleDataStream(env)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    val tableSink = new TestingUpsertTableSink(Array())
    t.window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w, 'num)
      .select('w.end as 'wend, 'id.count as 'cnt)
      .writeToSink(tableSink)

    env.execute()

    val expected = List(
      "(true,1970-01-01 00:00:00.005,1)",
      "(true,1970-01-01 00:00:00.005,2)",
      "(true,1970-01-01 00:00:00.005,1)",
      "(true,1970-01-01 00:00:00.01,2)",
      "(true,1970-01-01 00:00:00.01,3)",
      "(true,1970-01-01 00:00:00.015,1)",
      "(true,1970-01-01 00:00:00.015,4)",
      "(true,1970-01-01 00:00:00.02,1)",
      "(true,1970-01-01 00:00:00.02,4)",
      "(true,1970-01-01 00:00:00.025,2)").sorted
    assertEquals(expected, tableSink.getRawResults.sorted)
  }

  @Test
  def testUpsertSinkOnAppendingTableWithoutFullKey2(): Unit = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val t = StreamTestData.get3TupleDataStream(env)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    val tableSink = new TestingUpsertTableSink(Array())
    t.window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w, 'num)
      .select('num, 'id.count as 'cnt)
      .writeToSink(tableSink)

    env.execute()

    val expected = List(
      "(true,1,1)",
      "(true,2,2)",
      "(true,3,1)",
      "(true,3,2)",
      "(true,4,3)",
      "(true,4,1)",
      "(true,5,4)",
      "(true,5,1)",
      "(true,6,4)",
      "(true,6,2)").sorted
    assertEquals(expected, tableSink.getRawResults.sorted)
  }

  @Test
  def testPartitinalSink(): Unit = {
    def get3TupleData: Seq[(Int, Long, String)] = {
      val data = new mutable.MutableList[(Int, Long, String)]
      data.+=((3, 1L, "Hi"))
      data.+=((5, 2L, "Hello"))
      data.+=((1, 3L, "Hello world"))
      data.+=((2, 4L, "Hello world, how are you?"))
      data.+=((1, 5L, "I am fine."))
      data.+=((3, 6L, "Luke Skywalker"))
      data.+=((5, 7L, "Comment#1"))
      data
    }

    val t = env.fromCollection(get3TupleData)
        .toTable(tEnv, 'id, 'num, 'text)
    val globalPartitionalResults = mutable.HashMap.empty[Int, mutable.HashSet[Any]]
    val testingRetractSink = new TestingRetractTableSink
    testingRetractSink.setPartitionField("id")
    testingRetractSink.setPartitionalSink(new TestPartitionalSink(0, globalPartitionalResults))
    t.groupBy('num).select('id.sum as 'id, 'num).writeToSink(testingRetractSink)
    env.execute()
  }
}

