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
package org.apache.flink.table.planner.runtime.stream.sql

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.runtime.utils.UserDefinedFunctionTestUtils._
import org.apache.flink.table.planner.runtime.utils.{InMemoryLookupableTableSource, StreamingWithStateTestBase, TestingAppendSink, TestingRetractSink}
import org.apache.flink.types.Row
import org.apache.flink.util.ExceptionUtils

import org.junit.Assert.{assertEquals, assertTrue, fail}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}

@RunWith(classOf[Parameterized])
class AsyncLookupJoinITCase(backend: StateBackendMode)
  extends StreamingWithStateTestBase(backend) {

  val data = List(
    (1L, 12, "Julian"),
    (2L, 15, "Hello"),
    (3L, 15, "Fabian"),
    (8L, 11, "Hello world"),
    (9L, 12, "Hello world!"))

  val userData = List(
    (11, 1L, "Julian"),
    (22, 2L, "Jark"),
    (33, 3L, "Fabian"))

  val userTableSource = InMemoryLookupableTableSource.builder()
    .data(userData)
    .field("age", Types.INT)
    .field("id", Types.LONG)
    .field("name", Types.STRING)
    .enableAsync()
    .build()

  val userTableSourceWith2Keys = InMemoryLookupableTableSource.builder()
    .data(userData)
    .field("age", Types.INT)
    .field("id", Types.LONG)
    .field("name", Types.STRING)
    .enableAsync()
    .build()


  // TODO: remove this until [FLINK-12351] is fixed.
  //  currently AsyncWaitOperator doesn't copy input element which is a bug
  @Before
  override def before(): Unit = {
    super.before()
    env.getConfig.disableObjectReuse()
  }

  @Test
  def testAsyncJoinTemporalTableOnMultiKeyFields(): Unit = {
    val streamTable = failingDataSource(data)
      .toTable(tEnv, 'id, 'len, 'content, 'proctime.proctime)
    tEnv.registerTable("T", streamTable)

    // pk is (id: Long, name: String)
    tEnv.registerTableSource("userTable", userTableSourceWith2Keys)

    // test left table's join key define order diffs from right's
    val sql =
      """
        |SELECT t1.id, t1.len, D.name
        |FROM (select content, id, len, proctime FROM T) t1
        |JOIN userTable for system_time as of t1.proctime AS D
        |ON t1.content = D.name AND t1.id = D.id
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian",
      "3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, userTableSourceWith2Keys.getResourceCounter)
  }

  @Test
  def testAsyncJoinTemporalTable(): Unit = {
    val streamTable = failingDataSource(data)
      .toTable(tEnv, 'id, 'len, 'content, 'proctime.proctime)
    tEnv.registerTable("T", streamTable)

    tEnv.registerTableSource("userTable", userTableSource)

    val sql = "SELECT T.id, T.len, T.content, D.name FROM T JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian,Julian",
      "2,15,Hello,Jark",
      "3,15,Fabian,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, userTableSource.getResourceCounter)
  }

  @Test
  def testAsyncJoinTemporalTableWithPushDown(): Unit = {
    val streamTable = env.fromCollection(data)
      .toTable(tEnv, 'id, 'len, 'content, 'proctime.proctime)
    tEnv.registerTable("T", streamTable)

    tEnv.registerTableSource("userTable", userTableSource)

    val sql = "SELECT T.id, T.len, T.content, D.name FROM T JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.id = D.id AND D.age > 20"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "2,15,Hello,Jark",
      "3,15,Fabian,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, userTableSource.getResourceCounter)
  }

  @Test
  def testAsyncJoinTemporalTableWithNonEqualFilter(): Unit = {
    val streamTable = failingDataSource(data)
      .toTable(tEnv, 'id, 'len, 'content, 'proctime.proctime)
    tEnv.registerTable("T", streamTable)

    tEnv.registerTableSource("userTable", userTableSource)

    val sql = "SELECT T.id, T.len, T.content, D.name, D.age FROM T JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.id = D.id WHERE T.len <= D.age"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "2,15,Hello,Jark,22",
      "3,15,Fabian,Fabian,33")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, userTableSource.getResourceCounter)
  }

  @Test
  def testAsyncLeftJoinTemporalTableWithLocalPredicate(): Unit = {
    val streamTable = failingDataSource(data)
      .toTable(tEnv, 'id, 'len, 'content, 'proctime.proctime)
    tEnv.registerTable("T", streamTable)

    tEnv.registerTableSource("userTable", userTableSource)

    val sql = "SELECT T.id, T.len, T.content, D.name, D.age FROM T LEFT JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.id = D.id " +
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
    assertEquals(0, userTableSource.getResourceCounter)
  }

  @Test
  def testAsyncJoinTemporalTableOnMultiFields(): Unit = {
    val streamTable = failingDataSource(data)
      .toTable(tEnv, 'id, 'len, 'content, 'proctime.proctime)
    tEnv.registerTable("T", streamTable)

    tEnv.registerTableSource("userTable", userTableSource)

    val sql = "SELECT T.id, T.len, D.name FROM T JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.id = D.id AND T.content = D.name"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian",
      "3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, userTableSource.getResourceCounter)
  }

  @Test
  def testAsyncJoinTemporalTableOnMultiFieldsWithUdf(): Unit = {
    val streamTable = failingDataSource(data)
      .toTable(tEnv, 'id, 'len, 'content, 'proctime.proctime)
    tEnv.registerTable("T", streamTable)

    tEnv.registerTableSource("userTable", userTableSource)
    tEnv.registerFunction("mod1", TestMod)
    tEnv.registerFunction("wrapper1", TestWrapperUdf)

    val sql = "SELECT T.id, T.len, wrapper1(D.name) as name FROM T JOIN userTable " +
      "for system_time as of T.proctime AS D " +
      "ON mod1(T.id, 4) = D.id AND T.content = D.name"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian",
      "3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, userTableSource.getResourceCounter)
  }

  @Test
  def testAsyncJoinTemporalTableWithUdfFilter(): Unit = {
    val streamTable = failingDataSource(data)
      .toTable(tEnv, 'id, 'len, 'content, 'proctime.proctime)
    tEnv.registerTable("T", streamTable)

    tEnv.registerTableSource("userTable", userTableSource)
    tEnv.registerFunction("add", new TestAddWithOpen)

    val sql = "SELECT T.id, T.len, T.content, D.name FROM T JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.id = D.id " +
      "WHERE add(T.id, D.id) > 3 AND add(T.id, 2) > 3 AND add (D.id, 2) > 3"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "2,15,Hello,Jark",
      "3,15,Fabian,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, userTableSource.getResourceCounter)
    assertEquals(0, TestAddWithOpen.aliveCounter.get())
  }

  @Test
  def testAggAndAsyncLeftJoinTemporalTable(): Unit = {
    val streamTable = failingDataSource(data)
      .toTable(tEnv, 'id, 'len, 'content, 'proctime.proctime)
    tEnv.registerTable("T", streamTable)

    tEnv.registerTableSource("userTable", userTableSource)

    val sql1 = "SELECT max(id) as id, PROCTIME() as proctime from T group by len"

    val table1 = tEnv.sqlQuery(sql1)
    tEnv.registerTable("t1", table1)

    val sql2 = "SELECT t1.id, D.name, D.age FROM t1 LEFT JOIN userTable " +
      "for system_time as of t1.proctime AS D ON t1.id = D.id"

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
    val streamTable = failingDataSource(data)
      .toTable(tEnv, 'id, 'len, 'content, 'proctime.proctime)
    tEnv.registerTable("T", streamTable)

    tEnv.registerTableSource("userTable", userTableSource)

    val sql = "SELECT T.id, T.len, D.name, D.age FROM T LEFT JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.id = D.id"

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
    assertEquals(0, userTableSource.getResourceCounter)
  }

  @Test
  def testExceptionThrownFromAsyncJoinTemporalTable(): Unit = {
    env.setRestartStrategy(RestartStrategies.noRestart())
    val streamTable = env.fromCollection(data)
      .toTable(tEnv, 'id, 'len, 'content, 'proctime.proctime)
    tEnv.registerTable("T", streamTable)

    tEnv.registerTableSource("userTable", userTableSource)
    tEnv.registerFunction("errorFunc", TestExceptionThrown)

    val sql = "SELECT T.id, T.len, D.name, D.age FROM T LEFT JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.id = D.id " +
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
