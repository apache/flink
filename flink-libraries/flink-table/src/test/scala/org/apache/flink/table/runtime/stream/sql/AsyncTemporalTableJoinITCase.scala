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

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.runtime.utils.TemporalTableUtils.{TestingTemporalTableSource, TestingTemporalTableSourceWithDoubleKey}
import org.apache.flink.table.runtime.utils.{StreamingWithStateTestBase, TestingAppendSink, TestingRetractSink}
import org.apache.flink.table.sources.LookupConfig
import org.apache.flink.types.Row
import org.apache.flink.util.ExceptionUtils
import org.junit.Assert.{assertEquals, assertTrue, fail}
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(classOf[Parameterized])
class AsyncTemporalTableJoinITCase(backend: StateBackendMode)
  extends StreamingWithStateTestBase(backend) {

  val data = List(
    (1, 12, "Julian"),
    (2, 15, "Hello"),
    (3, 15, "Fabian"),
    (8, 11, "Hello world"),
    (9, 12, "Hello world!"))

  @Test
  def testAsyncJoinTemporalTableOnMultiKeyFields(): Unit = {
    val stream: DataStream[(Int, Int, String)] = failingDataSource(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    // pk is (id: Int, name: String)
    val temporalTable = new TestingTemporalTableSourceWithDoubleKey(true)
    tEnv.registerTableSource("csvTemporal", temporalTable)

    // test left table's join key define order diffs from right's
    val sql =
      """
        |SELECT t1.id, t1.len, D.name
        |FROM (select content, id, len FROM T) t1
        |JOIN csvTemporal for system_time as of PROCTIME() AS D
        |ON t1.content = D.name AND t1.id = D.id
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian",
      "3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, temporalTable.getFetcherResourceCount)
  }

  @Test
  def testAsyncJoinTemporalTable(): Unit = {
    val stream: DataStream[(Int, Int, String)] = failingDataSource(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val temporalTable = new TestingTemporalTableSource(true)
    tEnv.registerTableSource("csvTemporal", temporalTable)

    val sql = "SELECT T.id, T.len, T.content, D.name FROM T JOIN csvTemporal " +
      "for system_time as of PROCTIME() AS D ON T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian,Julian",
      "2,15,Hello,Jark",
      "3,15,Fabian,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, temporalTable.getFetcherResourceCount)
  }

  @Test
  def testAsyncJoinTemporalTableWithPushDown(): Unit = {
    val stream: DataStream[(Int, Int, String)] = failingDataSource(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val temporalTable = new TestingTemporalTableSource(true)
    tEnv.registerTableSource("csvTemporal", temporalTable)

    val sql = "SELECT T.id, T.len, T.content, D.name FROM T JOIN csvTemporal " +
      "for system_time as of PROCTIME() AS D ON T.id = D.id AND D.age > 20"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "2,15,Hello,Jark",
      "3,15,Fabian,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, temporalTable.getFetcherResourceCount)
  }

  @Test
  def testAsyncJoinTemporalTableWithNonEqualFilter(): Unit = {
    val stream: DataStream[(Int, Int, String)] = failingDataSource(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val temporalTable = new TestingTemporalTableSource(true)
    tEnv.registerTableSource("csvTemporal", temporalTable)

    val sql = "SELECT T.id, T.len, T.content, D.name, D.age FROM T JOIN csvTemporal " +
      "for system_time as of PROCTIME() AS D ON T.id = D.id WHERE T.len <= D.age"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "2,15,Hello,Jark,22",
      "3,15,Fabian,Fabian,33")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, temporalTable.getFetcherResourceCount)
  }

  @Test
  def testAsyncLeftJoinTemporalTableWithLocalPredicate(): Unit = {
    val stream: DataStream[(Int, Int, String)] = failingDataSource(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val temporalTable = new TestingTemporalTableSource(true)
    tEnv.registerTableSource("csvTemporal", temporalTable)

    val sql = "SELECT T.id, T.len, T.content, D.name, D.age FROM T LEFT JOIN csvTemporal " +
      "for system_time as of PROCTIME() AS D ON T.id = D.id " +
      "AND T.len > 1 AND D.age > 20 AND D.name = 'Fabian' " +
      "WHERE T.id > 1"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "2,15,Hello,null,null",
      "3,15,Fabian,Fabian,33",
      "8,11,Hello world,null,null",
      "9,12,Hello world!,null,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, temporalTable.getFetcherResourceCount)
  }

  @Test
  def testAsyncJoinTemporalTableOnMultiFields(): Unit = {
    val stream: DataStream[(Int, Int, String)] = failingDataSource(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val temporalTable = new TestingTemporalTableSource(true)
    tEnv.registerTableSource("csvTemporal", temporalTable)

    val sql = "SELECT T.id, T.len, D.name FROM T JOIN csvTemporal " +
      "for system_time as of PROCTIME() AS D ON T.id = D.id AND T.content = D.name"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian",
      "3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, temporalTable.getFetcherResourceCount)
  }

  @Test
  def testAsyncJoinTemporalTableOnMultiFieldsWithUdf(): Unit = {
    val stream: DataStream[(Int, Int, String)] = failingDataSource(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val temporalTable = new TestingTemporalTableSource(true)
    tEnv.registerTableSource("csvTemporal", temporalTable)
    tEnv.registerFunction("mod1", TestMod)
    tEnv.registerFunction("wrapper1", TestWrapperUdf)

    val sql = "SELECT T.id, T.len, wrapper1(D.name) as name FROM T JOIN csvTemporal " +
      "for system_time as of PROCTIME() AS D " +
      "ON mod1(T.id, 4) = D.id AND T.content = D.name"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian",
      "3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, temporalTable.getFetcherResourceCount)
  }

  @Test
  def testMinibatchAggAndAsyncLeftJoinTemporalTable(): Unit = {
    val stream: DataStream[(Int, Int, String)] = failingDataSource(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.getConfig.getConf.setLong(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY, 2)
    tEnv.getConfig.getConf.setLong(TableConfigOptions.SQL_EXEC_MINIBATCH_SIZE, 1)
    tEnv.registerTable("T", streamTable)

    val asyncConfig = new LookupConfig
    asyncConfig.setAsyncBufferCapacity(1)
    asyncConfig.setAsyncTimeoutMs(5000L)
    val temporalTable = new TestingTemporalTableSource(true, asyncConfig, delayedReturn = 1000L)
    tEnv.registerTableSource("csvTemporal", temporalTable)

    val sql1 = "SELECT max(id) as id from T group by len"

    val table1 = tEnv.sqlQuery(sql1)
    tEnv.registerTable("t1", table1)

    val sql2 = "SELECT t1.id, D.name, D.age FROM t1 LEFT JOIN csvTemporal " +
      "for system_time as of PROCTIME() AS D ON t1.id = D.id"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql2).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "3,Fabian,33",
      "8,null,null",
      "9,null,null")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }


  @Test
  def testAsyncLeftJoinTemporalTable(): Unit = {
    val stream: DataStream[(Int, Int, String)] = failingDataSource(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val temporalTable = new TestingTemporalTableSource(true)
    tEnv.registerTableSource("csvTemporal", temporalTable)

    val sql = "SELECT T.id, T.len, D.name, D.age FROM T LEFT JOIN csvTemporal " +
      "for system_time as of PROCTIME() AS D ON T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian,11",
      "2,15,Jark,22",
      "3,15,Fabian,33",
      "8,11,null,null",
      "9,12,null,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, temporalTable.getFetcherResourceCount)
  }

  @Test
  def testExceptionThrownFromAsyncJoinTemporalTable(): Unit = {
    env.setRestartStrategy(RestartStrategies.noRestart())
    val stream: DataStream[(Int, Int, String)] = env.fromCollection(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val temporalTable = new TestingTemporalTableSource(true)
    tEnv.registerTableSource("csvTemporal", temporalTable)
    tEnv.registerFunction("errorFunc", TestExceptionThrown)

    val sql = "SELECT T.id, T.len, D.name, D.age FROM T LEFT JOIN csvTemporal " +
      "for system_time as of PROCTIME() AS D ON T.id = D.id " +
      "where errorFunc(D.name) > cast(1000 as decimal(10,4))"  // should exception here

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)

    try {
      env.execute()
    } catch {
      case t: Throwable =>
        val exception = ExceptionUtils.findThrowable(t, classOf[NumberFormatException])
        assertTrue(exception.isPresent)
        assertTrue(exception.get().getMessage.contains("Cannot parse"))
        return
    }
    fail("NumberFormatException is expected here!")
  }

}
