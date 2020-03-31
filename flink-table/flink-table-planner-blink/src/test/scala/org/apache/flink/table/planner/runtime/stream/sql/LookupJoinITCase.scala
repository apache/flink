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

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.runtime.utils.UserDefinedFunctionTestUtils.TestAddWithOpen
import org.apache.flink.table.planner.runtime.utils.{InMemoryLookupableTableSource, StreamingTestBase, TestingAppendSink}
import org.apache.flink.types.Row

import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

import java.lang.{Integer => JInt, Long => JLong}

class LookupJoinITCase extends StreamingTestBase {

  val data = List(
    (1L, 12, "Julian"),
    (2L, 15, "Hello"),
    (3L, 15, "Fabian"),
    (8L, 11, "Hello world"),
    (9L, 12, "Hello world!"))

  val dataWithNull = List(
    Row.of(null, new JInt(15), "Hello"),
    Row.of(new JLong(3), new JInt(15), "Fabian"),
    Row.of(null, new JInt(11), "Hello world"),
    Row.of(new JLong(9), new JInt(12), "Hello world!"))

  val dataRowType:TypeInformation[Row] = new RowTypeInfo(
    BasicTypeInfo.LONG_TYPE_INFO,
    BasicTypeInfo.INT_TYPE_INFO,
    BasicTypeInfo.STRING_TYPE_INFO)

  val userData = List(
    (11, 1L, "Julian"),
    (22, 2L, "Jark"),
    (33, 3L, "Fabian"))

  val userTableSource = InMemoryLookupableTableSource.builder()
    .data(userData)
    .field("age", Types.INT)
    .field("id", Types.LONG)
    .field("name", Types.STRING)
    .build()

  val userTableSourceWith2Keys = InMemoryLookupableTableSource.builder()
    .data(userData)
    .field("age", Types.INT)
    .field("id", Types.LONG)
    .field("name", Types.STRING)
    .build()

  val userDataWithNull = List(
    (11, 1L, "Julian"),
    (22, null, "Hello"),
    (33, 3L, "Fabian"),
    (44, null, "Hello world")
  )

  val userWithNullDataTableSourceWith2Keys = InMemoryLookupableTableSource.builder()
    .data(userDataWithNull)
    .field("age", Types.INT)
    .field("id", Types.LONG)
    .field("name", Types.STRING)
    .build()

  @Test
  def testJoinTemporalTable(): Unit = {
    val streamTable = env.fromCollection(data)
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
  def testJoinTemporalTableWithUdfFilter(): Unit = {
    val streamTable = env.fromCollection(data)
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
  def testJoinTemporalTableOnConstantKey(): Unit = {
    val streamTable = env.fromCollection(data)
      .toTable(tEnv, 'id, 'len, 'content, 'proctime.proctime)
    tEnv.registerTable("T", streamTable)

    tEnv.registerTableSource("userTable", userTableSource)

    val sql = "SELECT T.id, T.len, T.content, D.name FROM T JOIN userTable " +
      "for system_time as of T.proctime AS D ON D.id = 1"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian,Julian", "2,15,Hello,Julian", "3,15,Fabian,Julian",
      "8,11,Hello world,Julian", "9,12,Hello world!,Julian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, userTableSource.getResourceCounter)
  }

  @Test
  def testJoinTemporalTableOnNullableKey(): Unit = {
    implicit val tpe: TypeInformation[Row] = dataRowType
    val streamTable = env.fromCollection(dataWithNull)
      .toTable(tEnv, 'id, 'len, 'content, 'proctime.proctime)
    tEnv.registerTable("T", streamTable)

    tEnv.registerTableSource("userTable", userTableSource)

    val sql = "SELECT T.id, T.len, D.name FROM T JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq("3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, userTableSource.getResourceCounter)
  }

  @Test
  def testJoinTemporalTableWithPushDown(): Unit = {
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
  def testJoinTemporalTableWithNonEqualFilter(): Unit = {
    val streamTable = env.fromCollection(data)
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
  def testJoinTemporalTableOnMultiFields(): Unit = {
    val streamTable = env.fromCollection(data)
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
  def testJoinTemporalTableOnMultiKeyFields(): Unit = {
    val streamTable = env.fromCollection(data)
      .toTable(tEnv, 'id, 'len, 'content, 'proctime.proctime)
    tEnv.registerTable("T", streamTable)

    tEnv.registerTableSource("userTable", userTableSourceWith2Keys)

    val sql = "SELECT T.id, T.len, D.name FROM T JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.content = D.name AND T.id = D.id"

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
  def testJoinTemporalTableOnMultiKeyFields2(): Unit = {
    val streamTable = env.fromCollection(data)
      .toTable(tEnv, 'id, 'len, 'content, 'proctime.proctime)
    tEnv.registerTable("T", streamTable)

    // pk is (id: Int, name: String)
    tEnv.registerTableSource("userTable", userTableSourceWith2Keys)

    // test left table's join key define order diffs from right's
    val sql = "SELECT t1.id, t1.len, D.name FROM (select proctime, content, id, len FROM T) t1 " +
      "JOIN userTable for system_time as of t1.proctime AS D " +
      "ON t1.content = D.name AND t1.id = D.id"

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
  def testJoinTemporalTableOnMultiKeyFieldsWithConstantKey(): Unit = {
    val streamTable = env.fromCollection(data)
      .toTable(tEnv, 'id, 'len, 'content, 'proctime.proctime)
    tEnv.registerTable("T", streamTable)

    tEnv.registerTableSource("userTable", userTableSourceWith2Keys)

    val sql = "SELECT T.id, T.len, D.name FROM T JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.content = D.name AND 3 = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq("3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, userTableSourceWith2Keys.getResourceCounter)
  }

  @Test
  def testJoinTemporalTableOnMultiKeyFieldsWithStringConstantKey(): Unit = {
    val streamTable = env.fromCollection(data)
      .toTable(tEnv, 'id, 'len, 'content, 'proctime.proctime)
    tEnv.registerTable("T", streamTable)

    tEnv.registerTableSource("userTable", userTableSourceWith2Keys)

    val sql = "SELECT T.id, T.len, D.name FROM T JOIN userTable " +
      "for system_time as of T.proctime AS D ON D.name = 'Fabian' AND T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq("3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, userTableSourceWith2Keys.getResourceCounter)
  }

  @Test
  def testJoinTemporalTableOnMultiConstantKey(): Unit = {
    val streamTable = env.fromCollection(data)
      .toTable(tEnv, 'id, 'len, 'content, 'proctime.proctime)
    tEnv.registerTable("T", streamTable)

    tEnv.registerTableSource("userTable", userTableSourceWith2Keys)

    val sql = "SELECT T.id, T.len, D.name FROM T JOIN userTable " +
      "for system_time as of T.proctime AS D ON D.name = 'Fabian' AND 3 = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Fabian",
      "2,15,Fabian",
      "3,15,Fabian",
      "8,11,Fabian",
      "9,12,Fabian"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, userTableSourceWith2Keys.getResourceCounter)
  }

  @Test
  def testLeftJoinTemporalTable(): Unit = {
    val streamTable = env.fromCollection(data)
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
  def testLeftJoinTemporalTableOnNullableKey(): Unit = {
    implicit val tpe: TypeInformation[Row] = dataRowType
    val streamTable = env.fromCollection(dataWithNull)
      .toTable(tEnv, 'id, 'len, 'content, 'proctime.proctime)
    tEnv.registerTable("T", streamTable)

    tEnv.registerTableSource("userTable", userTableSource)

    val sql = "SELECT T.id, T.len, D.name FROM T LEFT OUTER JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "null,15,null",
      "3,15,Fabian",
      "null,11,null",
      "9,12,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, userTableSource.getResourceCounter)
  }

  @Test
  def testLeftJoinTemporalTableOnMultKeyFields(): Unit = {
    val streamTable = env.fromCollection(data)
      .toTable(tEnv, 'id, 'len, 'content, 'proctime.proctime)
    tEnv.registerTable("T", streamTable)

    tEnv.registerTableSource("userTable", userTableSource)

    val sql = "SELECT T.id, T.len, D.name, D.age FROM T LEFT JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.id = D.id and T.content = D.name"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian,11",
      "2,15,null,null",
      "3,15,Fabian,33",
      "8,11,null,null",
      "9,12,null,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, userTableSource.getResourceCounter)
  }

  @Test
  def testJoinTemporalTableOnMultiKeyFieldsWithNullData(): Unit = {
    implicit val tpe: TypeInformation[Row] = dataRowType
    val streamTable = env.fromCollection(dataWithNull)
      .toTable(tEnv, 'id, 'len, 'content, 'proctime.proctime)
    tEnv.registerTable("T", streamTable)

    tEnv.registerTableSource("userTable", userWithNullDataTableSourceWith2Keys)

    val sql = "SELECT T.id, T.len, D.name FROM T JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.content = D.name AND T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, userTableSourceWith2Keys.getResourceCounter)
  }

  @Test
  def testLeftJoinTemporalTableOnMultiKeyFieldsWithNullData(): Unit = {
    implicit val tpe: TypeInformation[Row] = dataRowType
    val streamTable = env.fromCollection(dataWithNull)
      .toTable(tEnv, 'id, 'len, 'content, 'proctime.proctime)
    tEnv.registerTable("T", streamTable)

    tEnv.registerTableSource("userTable", userWithNullDataTableSourceWith2Keys)

    val sql = "SELECT D.id, T.len, D.name FROM T LEFT JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.content = D.name AND T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "null,15,null",
      "3,15,Fabian",
      "null,11,null",
      "null,12,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, userTableSourceWith2Keys.getResourceCounter)
  }

  @Test
  def testJoinTemporalTableOnNullConstantKey(): Unit = {
    implicit val tpe: TypeInformation[Row] = dataRowType
    val streamTable = env.fromCollection(dataWithNull)
      .toTable(tEnv, 'id, 'len, 'content, 'proctime.proctime)
    tEnv.registerTable("T", streamTable)

    tEnv.registerTableSource("userTable", userWithNullDataTableSourceWith2Keys)

    val sql = "SELECT T.id, T.len, T.content FROM T JOIN userTable " +
      "for system_time as of T.proctime AS D ON D.id = null"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    assertTrue(sink.getAppendResults.isEmpty)
    assertEquals(0, userTableSource.getResourceCounter)
  }

  @Test
  def testJoinTemporalTableOnMultiKeyFieldsWithNullConstantKey(): Unit = {
    val streamTable = env.fromCollection(data)
      .toTable(tEnv, 'id, 'len, 'content, 'proctime.proctime)
    tEnv.registerTable("T", streamTable)

    tEnv.registerTableSource("userTable", userTableSourceWith2Keys)

    val sql = "SELECT T.id, T.len, D.name FROM T JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.content = D.name AND null = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    assertTrue(sink.getAppendResults.isEmpty)
    assertEquals(0, userTableSourceWith2Keys.getResourceCounter)
  }

}
