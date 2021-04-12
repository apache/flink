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

import org.apache.flink.api.scala._
import org.apache.flink.table.api.{TableSchema, Types}
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.{HEAP_BACKEND, ROCKSDB_BACKEND, StateBackendMode}
import org.apache.flink.table.planner.runtime.utils.UserDefinedFunctionTestUtils._
import org.apache.flink.table.planner.runtime.utils.{InMemoryLookupableTableSource, StreamingWithStateTestBase, TestingAppendSink, TestingRetractSink}
import org.apache.flink.types.Row
import org.apache.flink.util.ExceptionUtils
import org.junit.Assert.{assertEquals, assertTrue, fail}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{After, Before, Test}

import java.lang.{Boolean => JBoolean}
import java.util.{Collection => JCollection}

import scala.collection.JavaConversions._

@RunWith(classOf[Parameterized])
class AsyncLookupJoinITCase(legacyTableSource: Boolean, backend: StateBackendMode)
  extends StreamingWithStateTestBase(backend) {

  val data = List(
    rowOf(1L, 12, "Julian"),
    rowOf(2L, 15, "Hello"),
    rowOf(3L, 15, "Fabian"),
    rowOf(8L, 11, "Hello world"),
    rowOf(9L, 12, "Hello world!"))

  val userData = List(
    rowOf(11, 1L, "Julian"),
    rowOf(22, 2L, "Jark"),
    rowOf(33, 3L, "Fabian"))

  @Before
  override def before(): Unit = {
    super.before()
    // TODO: remove this until [FLINK-12351] is fixed.
    //  currently AsyncWaitOperator doesn't copy input element which is a bug
    env.getConfig.disableObjectReuse()
    
    createScanTable("src", data)
    createLookupTable("user_table", userData)
  }

  @After
  override def after(): Unit = {
    super.after()
    if (legacyTableSource) {
      assertEquals(0, InMemoryLookupableTableSource.RESOURCE_COUNTER.get())
    } else {
      assertEquals(0, TestValuesTableFactory.RESOURCE_COUNTER.get())
    }
  }

  private def createLookupTable(tableName: String, data: List[Row]): Unit = {
    if (legacyTableSource) {
      val userSchema = TableSchema.builder()
        .field("age", Types.INT)
        .field("id", Types.LONG)
        .field("name", Types.STRING)
        .build()
      InMemoryLookupableTableSource.createTemporaryTable(
        tEnv, isAsync = true, data, userSchema, tableName)
    } else {
      val dataId = TestValuesTableFactory.registerData(data)
      tEnv.executeSql(
        s"""
           |CREATE TABLE $tableName (
           |  `age` INT,
           |  `id` BIGINT,
           |  `name` STRING
           |) WITH (
           |  'connector' = 'values',
           |  'data-id' = '$dataId',
           |  'async' = 'true'
           |)
           |""".stripMargin)
    }
  }

  private def createScanTable(tableName: String, data: List[Row]): Unit = {
    val dataId = TestValuesTableFactory.registerData(data)
    tEnv.executeSql(
      s"""
         |CREATE TABLE $tableName (
         |  `id` BIGINT,
         |  `len` INT,
         |  `content` STRING,
         |  `proctime` AS PROCTIME()
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$dataId'
         |)
         |""".stripMargin)
  }

  @Test
  def testAsyncJoinTemporalTableOnMultiKeyFields(): Unit = {
    // test left table's join key define order diffs from right's
    val sql =
      """
        |SELECT t1.id, t1.len, D.name
        |FROM (select content, id, len, proctime FROM src AS T) t1
        |JOIN user_table for system_time as of t1.proctime AS D
        |ON t1.content = D.name AND t1.id = D.id
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian",
      "3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testAsyncJoinTemporalTable(): Unit = {
    val sql = "SELECT T.id, T.len, T.content, D.name FROM src AS T JOIN user_table " +
      "for system_time as of T.proctime AS D ON T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian,Julian",
      "2,15,Hello,Jark",
      "3,15,Fabian,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testAsyncJoinTemporalTableWithPushDown(): Unit = {
    val sql = "SELECT T.id, T.len, T.content, D.name FROM src AS T JOIN user_table " +
      "for system_time as of T.proctime AS D ON T.id = D.id AND D.age > 20"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "2,15,Hello,Jark",
      "3,15,Fabian,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testAsyncJoinTemporalTableWithNonEqualFilter(): Unit = {
    val sql = "SELECT T.id, T.len, T.content, D.name, D.age FROM src AS T JOIN user_table " +
      "for system_time as of T.proctime AS D ON T.id = D.id WHERE T.len <= D.age"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "2,15,Hello,Jark,22",
      "3,15,Fabian,Fabian,33")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testAsyncLeftJoinTemporalTableWithLocalPredicate(): Unit = {
    val sql = "SELECT T.id, T.len, T.content, D.name, D.age FROM src AS T LEFT JOIN user_table " +
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
  }

  @Test
  def testAsyncJoinTemporalTableOnMultiFields(): Unit = {
    val sql = "SELECT T.id, T.len, D.name FROM src AS T JOIN user_table " +
      "for system_time as of T.proctime AS D ON T.id = D.id AND T.content = D.name"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian",
      "3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testAsyncJoinTemporalTableOnMultiFieldsWithUdf(): Unit = {
    tEnv.registerFunction("mod1", TestMod)
    tEnv.registerFunction("wrapper1", TestWrapperUdf)

    val sql = "SELECT T.id, T.len, wrapper1(D.name) as name FROM src AS T JOIN user_table " +
      "for system_time as of T.proctime AS D " +
      "ON mod1(T.id, 4) = D.id AND T.content = D.name"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian",
      "3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testAsyncJoinTemporalTableWithUdfFilter(): Unit = {
    tEnv.registerFunction("add", new TestAddWithOpen)

    val sql = "SELECT T.id, T.len, T.content, D.name FROM src AS T JOIN user_table " +
      "for system_time as of T.proctime AS D ON T.id = D.id " +
      "WHERE add(T.id, D.id) > 3 AND add(T.id, 2) > 3 AND add (D.id, 2) > 3"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "2,15,Hello,Jark",
      "3,15,Fabian,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, TestAddWithOpen.aliveCounter.get())
  }

  @Test
  def testAggAndAsyncLeftJoinTemporalTable(): Unit = {
    val sql1 = "SELECT max(id) as id, PROCTIME() as proctime FROM src AS T group by len"

    val table1 = tEnv.sqlQuery(sql1)
    tEnv.registerTable("t1", table1)

    val sql2 = "SELECT t1.id, D.name, D.age FROM t1 LEFT JOIN user_table " +
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
    val sql = "SELECT T.id, T.len, D.name, D.age FROM src AS T LEFT JOIN user_table " +
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
  }

  @Test
  def testExceptionThrownFromAsyncJoinTemporalTable(): Unit = {
    tEnv.registerFunction("errorFunc", TestExceptionThrown)

    val sql = "SELECT T.id, T.len, D.name, D.age FROM src AS T LEFT JOIN user_table " +
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

object AsyncLookupJoinITCase {
  @Parameterized.Parameters(name = "LegacyTableSource={0}, StateBackend={1}")
  def parameters(): JCollection[Array[Object]] = {
    Seq[Array[AnyRef]](
      Array(JBoolean.TRUE, HEAP_BACKEND),
      Array(JBoolean.TRUE, ROCKSDB_BACKEND),
      Array(JBoolean.FALSE, HEAP_BACKEND),
      Array(JBoolean.FALSE, ROCKSDB_BACKEND)
    )
  }
}
