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
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.UserDefinedFunctionTestUtils.TestAddWithOpen
import org.apache.flink.table.planner.runtime.utils.{InMemoryLookupableTableSource, StreamingTestBase, TestingAppendSink}
import org.apache.flink.types.Row

import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{After, Before, Test}

import java.lang.{Boolean => JBoolean}
import java.time.LocalDateTime
import java.util.{Collection => JCollection}

import scala.collection.JavaConversions._

@RunWith(classOf[Parameterized])
class LookupJoinITCase(legacyTableSource: Boolean) extends StreamingTestBase {

  val data = List(
    rowOf(1L, 12, "Julian"),
    rowOf(2L, 15, "Hello"),
    rowOf(3L, 15, "Fabian"),
    rowOf(8L, 11, "Hello world"),
    rowOf(9L, 12, "Hello world!"))

  val dataWithNull = List(
    rowOf(null, 15, "Hello"),
    rowOf(3L, 15, "Fabian"),
    rowOf(null, 11, "Hello world"),
    rowOf(9L, 12, "Hello world!"))

  val userData = List(
    rowOf(11, 1L, "Julian"),
    rowOf(22, 2L, "Jark"),
    rowOf(33, 3L, "Fabian"),
    rowOf(11, 4L, "Hello world"),
    rowOf(11, 5L, "Hello world"))

  val userDataWithNull = List(
    rowOf(11, 1L, "Julian"),
    rowOf(22, null, "Hello"),
    rowOf(33, 3L, "Fabian"),
    rowOf(44, null, "Hello world"))

  @Before
  override def before(): Unit = {
    super.before()
    createScanTable("src", data)
    createScanTable("nullable_src", dataWithNull)
    createLookupTable("user_table", userData)
    createLookupTable("nullable_user_table", userDataWithNull)
    createLookupTableWithComputedColumn("userTableWithComputedColumn", userData)
  }
  
  @After
  override def after(): Unit = {
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
        tEnv, isAsync = false, data, userSchema, tableName)
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
           |  'data-id' = '$dataId'
           |)
           |""".stripMargin)
    }
  }

  private def createLookupTableWithComputedColumn(tableName: String, data: List[Row]): Unit = {
    if (!legacyTableSource) {
      val dataId = TestValuesTableFactory.registerData(data)
      tEnv.executeSql(
        s"""
           |CREATE TABLE $tableName (
           |  `age` INT,
           |  `id` BIGINT,
           |  `name` STRING,
           |  `nominal_age` as age + 1
           |) WITH (
           |  'connector' = 'values',
           |  'data-id' = '$dataId'
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
  def testJoinTemporalTable(): Unit = {
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
  def testJoinTemporalTableWithUdfFilter(): Unit = {
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
  def testJoinTemporalTableWithUdfEqualFilter(): Unit = {
    val sql =
      """
        |SELECT
        |  T.id, T.len, T.content, D.name
        |FROM
        |  src AS T JOIN user_table for system_time as of T.proctime AS D
        |ON T.id = D.id
        |WHERE CONCAT('Hello-', D.name) = 'Hello-Jark'
        |""".stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq("2,15,Hello,Jark")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testJoinTemporalTableOnConstantKey(): Unit = {
    val sql = "SELECT T.id, T.len, T.content, D.name FROM src AS T JOIN user_table " +
      "for system_time as of T.proctime AS D ON D.id = 1"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian,Julian", "2,15,Hello,Julian", "3,15,Fabian,Julian",
      "8,11,Hello world,Julian", "9,12,Hello world!,Julian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testJoinTemporalTableOnNullableKey(): Unit = {
    val sql = "SELECT T.id, T.len, D.name FROM nullable_src AS T JOIN user_table " +
      "for system_time as of T.proctime AS D ON T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq("3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testJoinTemporalTableWithPushDown(): Unit = {
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
  def testJoinTemporalTableWithNonEqualFilter(): Unit = {
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
  def testJoinTemporalTableOnMultiFields(): Unit = {
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
  def testJoinTemporalTableOnMultiKeyFields(): Unit = {
    val sql = "SELECT T.id, T.len, D.name FROM src AS T JOIN user_table " +
      "for system_time as of T.proctime AS D ON T.content = D.name AND T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian",
      "3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testJoinTemporalTableOnMultiKeyFields2(): Unit = {
    // test left table's join key define order diffs from right's
    val sql = "SELECT t1.id, t1.len, D.name FROM " +
      "(select proctime, content, id, len FROM src) t1 " +
      "JOIN user_table for system_time as of t1.proctime AS D " +
      "ON t1.content = D.name AND t1.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian",
      "3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testJoinTemporalTableOnMultiKeyFieldsWithConstantKey(): Unit = {
    val sql = "SELECT T.id, T.len, D.name FROM src AS T JOIN user_table " +
      "for system_time as of T.proctime AS D ON T.content = D.name AND 3 = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq("3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testJoinTemporalTableOnMultiKeyFieldsWithStringConstantKey(): Unit = {
    val sql = "SELECT T.id, T.len, D.name FROM src AS T JOIN user_table " +
      "for system_time as of T.proctime AS D ON D.name = 'Fabian' AND T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq("3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testJoinTemporalTableOnMultiConstantKey(): Unit = {
    val sql = "SELECT T.id, T.len, D.name FROM src AS T JOIN user_table " +
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
  }

  @Test
  def testLeftJoinTemporalTable(): Unit = {
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
  def testLeftJoinTemporalTableOnNullableKey(): Unit = {
    val sql = "SELECT T.id, T.len, D.name FROM nullable_src AS T LEFT OUTER JOIN user_table " +
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
  }

  @Test
  def testLeftJoinTemporalTableOnMultKeyFields(): Unit = {
    val sql = "SELECT T.id, T.len, D.name, D.age FROM src AS T LEFT JOIN user_table " +
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
  }

  @Test
  def testJoinTemporalTableOnMultiKeyFieldsWithNullData(): Unit = {
    val sql = "SELECT T.id, T.len, D.name FROM nullable_src AS T JOIN nullable_user_table " +
      "for system_time as of T.proctime AS D ON T.content = D.name AND T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testLeftJoinTemporalTableOnMultiKeyFieldsWithNullData(): Unit = {
    val sql = "SELECT D.id, T.len, D.name FROM nullable_src AS T LEFT JOIN nullable_user_table " +
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
  }

  @Test
  def testJoinTemporalTableOnNullConstantKey(): Unit = {
    val sql = "SELECT T.id, T.len, T.content FROM nullable_src AS T JOIN nullable_user_table " +
      "for system_time as of T.proctime AS D ON D.id = null"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    assertTrue(sink.getAppendResults.isEmpty)
  }

  @Test
  def testJoinTemporalTableOnMultiKeyFieldsWithNullConstantKey(): Unit = {
    val sql = "SELECT T.id, T.len, D.name FROM src AS T JOIN user_table " +
      "for system_time as of T.proctime AS D ON T.content = D.name AND null = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    assertTrue(sink.getAppendResults.isEmpty)
  }

  @Test
  def testJoinTemporalTableOnMultiKeyFieldsWithUDF(): Unit = {
    val sql = "SELECT T.id, T.content, D.age, D.id FROM src AS T JOIN user_table " +
      "for system_time as of T.proctime AS D " +
      "ON T.id = D.id + 4 AND T.content = concat(D.name, '!') AND D.age = 11"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "9,Hello world!,11,5")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testJoinTemporalTableWithComputedColumn(): Unit = {
    if (legacyTableSource) {
      //Computed column do not support in legacyTableSource.
      return
    }
    val sql = s"SELECT T.id, T.len, T.content, D.name, D.age, D.nominal_age " +
      "FROM src AS T JOIN userTableWithComputedColumn " +
      "for system_time as of T.proctime AS D ON T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian,Julian,11,12",
      "2,15,Hello,Jark,22,23",
      "3,15,Fabian,Fabian,33,34")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testJoinTemporalTableWithComputedColumnAndPushDown(): Unit = {
    if (legacyTableSource) {
      //Computed column do not support in legacyTableSource.
      return
    }
    val sql = s"SELECT T.id, T.len, T.content, D.name, D.age, D.nominal_age " +
      "FROM src AS T JOIN userTableWithComputedColumn " +
      "for system_time as of T.proctime AS D ON T.id = D.id and D.nominal_age > 12"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "2,15,Hello,Jark,22,23",
      "3,15,Fabian,Fabian,33,34")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testCurrentDateInJoinCondition(): Unit = {
    val id1 = TestValuesTableFactory.registerData(
      Seq(Row.of("abc", LocalDateTime.of(
        2000, 1, 1, 0, 0))))
    val ddl1 =
      s"""
         |CREATE TABLE Ta (
         |  id VARCHAR,
         |  ts TIMESTAMP,
         |  proc AS PROCTIME()
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$id1',
         |  'bounded' = 'true'
         |)
         |""".stripMargin
    tEnv.executeSql(ddl1)

    val id2 = TestValuesTableFactory.registerData(
      Seq(Row.of("abc", LocalDateTime.of(
        2000, 1, 2, 0, 0))))
    val ddl2 =
      s"""
         |CREATE TABLE Tb (
         |  id VARCHAR,
         |  ts TIMESTAMP
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$id2',
         |  'bounded' = 'true'
         |)
         |""".stripMargin
    tEnv.executeSql(ddl2)

    val sql =
      """
        |SELECT * FROM Ta AS t1
        |INNER JOIN Tb FOR SYSTEM_TIME AS OF t1.proc AS t2
        |ON t1.id = t2.id
        |WHERE
        |  CAST(coalesce(t1.ts, t2.ts) AS VARCHAR)
        |  >=
        |  CONCAT(CAST(CURRENT_DATE AS VARCHAR), ' 00:00:00')
        |""".stripMargin
    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()
    assertEquals(Seq(), sink.getAppendResults)
  }
}

object LookupJoinITCase {
  @Parameterized.Parameters(name = "LegacyTableSource={0}")
  def parameters(): JCollection[Array[Object]] = {
    Seq[Array[AnyRef]](Array(JBoolean.TRUE), Array(JBoolean.FALSE))
  }
}
