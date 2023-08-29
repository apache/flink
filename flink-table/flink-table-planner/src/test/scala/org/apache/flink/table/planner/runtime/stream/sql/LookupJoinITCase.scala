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
import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.connector.source.lookup.LookupOptions
import org.apache.flink.table.connector.source.lookup.LookupOptions.{LookupCacheType, ReloadStrategy}
import org.apache.flink.table.data.GenericRowData
import org.apache.flink.table.data.binary.BinaryStringData
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.{InMemoryLookupableTableSource, StreamingTestBase, TestingAppendSink, TestingRetractSink}
import org.apache.flink.table.planner.runtime.utils.UserDefinedFunctionTestUtils.TestAddWithOpen
import org.apache.flink.table.runtime.functions.table.fullcache.inputformat.FullCacheTestInputFormat
import org.apache.flink.table.runtime.functions.table.lookup.LookupCacheManager
import org.apache.flink.types.Row

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.IterableAssert.assertThatIterable
import org.junit.{After, Before, Test}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.lang.{Boolean => JBoolean}
import java.time.LocalDateTime
import java.util.{Collection => JCollection}

import scala.collection.JavaConversions._

@RunWith(classOf[Parameterized])
class LookupJoinITCase(legacyTableSource: Boolean, cacheType: LookupCacheType)
  extends StreamingTestBase {

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
    if (legacyTableSource) {
      InMemoryLookupableTableSource.RESOURCE_COUNTER.set(0)
    } else {
      TestValuesTableFactory.RESOURCE_COUNTER.set(0)
      FullCacheTestInputFormat.OPEN_CLOSED_COUNTER.set(0)
    }
    createScanTable("src", data)
    createScanTable("nullable_src", dataWithNull)
    createLookupTable("user_table", userData)
    createLookupTable("nullable_user_table", userDataWithNull)
    // lookup will start from the 2nd time, first lookup will always get null result
    createLookupTable("user_table_with_lookup_threshold2", userData, 2)
    // lookup will start from the 3rd time, first lookup will always get null result
    createLookupTable("user_table_with_lookup_threshold3", userData, 3)
    createLookupTableWithComputedColumn("userTableWithComputedColumn", userData)
  }

  @After
  override def after(): Unit = {
    if (legacyTableSource) {
      assertEquals(0, InMemoryLookupableTableSource.RESOURCE_COUNTER.get())
    } else {
      assertEquals(0, TestValuesTableFactory.RESOURCE_COUNTER.get())
      assertEquals(0, FullCacheTestInputFormat.OPEN_CLOSED_COUNTER.get())
    }
  }

  /** The lookupThreshold only works for new table source (not legacyTableSource). */
  private def createLookupTable(
      tableName: String,
      data: List[Row],
      lookupThreshold: Int = -1): Unit = {
    if (legacyTableSource) {
      val userSchema = TableSchema
        .builder()
        .field("age", Types.INT)
        .field("id", Types.LONG)
        .field("name", Types.STRING)
        .build()
      InMemoryLookupableTableSource.createTemporaryTable(
        tEnv,
        isAsync = false,
        data,
        userSchema,
        tableName)
    } else {
      val dataId = TestValuesTableFactory.registerData(data)
      val cacheOptions =
        if (cacheType == LookupCacheType.PARTIAL)
          s"""
             |  '${LookupOptions.CACHE_TYPE.key()}' = '${LookupCacheType.PARTIAL}',
             |  '${LookupOptions.PARTIAL_CACHE_MAX_ROWS.key()}' = '${Long.MaxValue}',
             |""".stripMargin
        else if (cacheType == LookupCacheType.FULL)
          s"""
             |  '${LookupOptions.CACHE_TYPE.key()}' = '${LookupCacheType.FULL}',
             |  '${LookupOptions.FULL_CACHE_RELOAD_STRATEGY.key()}' = '${ReloadStrategy.PERIODIC}',
             |  '${LookupOptions.FULL_CACHE_PERIODIC_RELOAD_INTERVAL.key()}' = '${Long.MaxValue}',
             |""".stripMargin
        else ""
      val lookupThresholdOption = if (lookupThreshold > 0) {
        s"'start-lookup-threshold'='$lookupThreshold',"
      } else ""

      tEnv.executeSql(s"""
                         |CREATE TABLE $tableName (
                         |  `age` INT,
                         |  `id` BIGINT,
                         |  `name` STRING
                         |) WITH (
                         |  $cacheOptions
                         |  $lookupThresholdOption
                         |  'connector' = 'values',
                         |  'data-id' = '$dataId'
                         |)
                         |""".stripMargin)
    }
  }

  private def createLookupTableWithComputedColumn(tableName: String, data: List[Row]): Unit = {
    if (!legacyTableSource) {
      val dataId = TestValuesTableFactory.registerData(data)
      val cacheOptions =
        if (cacheType == LookupCacheType.PARTIAL)
          s"""
             |  '${LookupOptions.CACHE_TYPE.key()}' = '${LookupCacheType.PARTIAL}',
             |  '${LookupOptions.PARTIAL_CACHE_MAX_ROWS.key()}' = '${Long.MaxValue}',
             |""".stripMargin
        else if (cacheType == LookupCacheType.FULL)
          s"""
             |  '${LookupOptions.CACHE_TYPE.key()}' = '${LookupCacheType.FULL}',
             |  '${LookupOptions.FULL_CACHE_RELOAD_STRATEGY.key()}' = '${ReloadStrategy.PERIODIC}',
             |  '${LookupOptions.FULL_CACHE_PERIODIC_RELOAD_INTERVAL.key()}' = '${Long.MaxValue}',
             |""".stripMargin
        else ""
      tEnv.executeSql(s"""
                         |CREATE TABLE $tableName (
                         |  `age` INT,
                         |  `id` BIGINT,
                         |  `name` STRING,
                         |  `nominal_age` as age + 1
                         |) WITH (
                         |  $cacheOptions
                         |  'connector' = 'values',
                         |  'data-id' = '$dataId'
                         |)
                         |""".stripMargin)
    }
  }

  private def createScanTable(tableName: String, data: List[Row]): Unit = {
    val dataId = TestValuesTableFactory.registerData(data)
    tEnv.executeSql(s"""
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

    val expected = Seq("1,12,Julian,Julian", "2,15,Hello,Jark", "3,15,Fabian,Fabian")
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

    val expected = Seq("2,15,Hello,Jark", "3,15,Fabian,Fabian")
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
      "1,12,Julian,Julian",
      "2,15,Hello,Julian",
      "3,15,Fabian,Julian",
      "8,11,Hello world,Julian",
      "9,12,Hello world!,Julian")
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

    val expected = Seq("2,15,Hello,Jark", "3,15,Fabian,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testJoinTemporalTableWithNonEqualFilter(): Unit = {
    val sql = "SELECT T.id, T.len, T.content, D.name, D.age FROM src AS T JOIN user_table " +
      "for system_time as of T.proctime AS D ON T.id = D.id WHERE T.len <= D.age"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq("2,15,Hello,Jark,22", "3,15,Fabian,Fabian,33")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testJoinTemporalTableOnMultiFields(): Unit = {
    val sql = "SELECT T.id, T.len, D.name FROM src AS T JOIN user_table " +
      "for system_time as of T.proctime AS D ON T.id = D.id AND T.content = D.name"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq("1,12,Julian", "3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testJoinTemporalTableOnMultiKeyFields(): Unit = {
    val sql = "SELECT T.id, T.len, D.name FROM src AS T JOIN user_table " +
      "for system_time as of T.proctime AS D ON T.content = D.name AND T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq("1,12,Julian", "3,15,Fabian")
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

    val expected = Seq("1,12,Julian", "3,15,Fabian")
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

    val expected =
      Seq("1,12,Julian,11", "2,15,Jark,22", "3,15,Fabian,33", "8,11,null,null", "9,12,null,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testLeftJoinTemporalTableWithPreFilter(): Unit = {
    val sql = "SELECT T.id, T.len, D.name, D.age FROM src AS T LEFT JOIN user_table " +
      "for system_time as of T.proctime AS D ON T.id = D.id AND T.len < 15"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected =
      Seq("1,12,Julian,11", "2,15,null,null", "3,15,null,null", "8,11,null,null", "9,12,null,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testLeftJoinTemporalTableWithUdfPreFilter(): Unit = {
    tEnv.registerFunction("add", new TestAddWithOpen)
    // use the new api when FLINK-32986 is resolved
    // tEnv.createTemporaryFunction("add", classOf[TestAddWithOpen])

    // 'add(T.id, 2) > 4' is equal to 'T.id > 2', here we are testing a udf
    val sql = "SELECT T.id, T.len, T.content, D.name FROM src AS T LEFT JOIN user_table " +
      "for system_time as of T.proctime AS D ON T.id = D.id AND add(T.id, 2) > 4"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian,null",
      "2,15,Hello,null",
      "3,15,Fabian,Fabian",
      "8,11,Hello world,null",
      "9,12,Hello world!,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, TestAddWithOpen.aliveCounter.get())
  }

  @Test
  def testLeftJoinTemporalTableOnNullableKey(): Unit = {
    val sql = "SELECT T.id, T.len, D.name FROM nullable_src AS T LEFT OUTER JOIN user_table " +
      "for system_time as of T.proctime AS D ON T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq("null,15,null", "3,15,Fabian", "null,11,null", "9,12,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testLeftJoinTemporalTableOnMultKeyFields(): Unit = {
    val sql = "SELECT T.id, T.len, D.name, D.age FROM src AS T LEFT JOIN user_table " +
      "for system_time as of T.proctime AS D ON T.id = D.id and T.content = D.name"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected =
      Seq("1,12,Julian,11", "2,15,null,null", "3,15,Fabian,33", "8,11,null,null", "9,12,null,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testJoinTemporalTableOnMultiKeyFieldsWithNullData(): Unit = {
    val sql = "SELECT T.id, T.len, D.name FROM nullable_src AS T JOIN nullable_user_table " +
      "for system_time as of T.proctime AS D ON T.content = D.name AND T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq("3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testLeftJoinTemporalTableOnMultiKeyFieldsWithNullData(): Unit = {
    val sql = "SELECT D.id, T.len, D.name FROM nullable_src AS T LEFT JOIN nullable_user_table " +
      "for system_time as of T.proctime AS D ON T.content = D.name AND T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq("null,15,null", "3,15,Fabian", "null,11,null", "null,12,null")
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

    val expected = Seq("9,Hello world!,11,5")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testJoinTemporalTableWithComputedColumn(): Unit = {
    if (legacyTableSource) {
      // Computed column do not support in legacyTableSource.
      return
    }
    val sql = s"SELECT T.id, T.len, T.content, D.name, D.age, D.nominal_age " +
      "FROM src AS T JOIN userTableWithComputedColumn " +
      "for system_time as of T.proctime AS D ON T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected =
      Seq("1,12,Julian,Julian,11,12", "2,15,Hello,Jark,22,23", "3,15,Fabian,Fabian,33,34")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testJoinTemporalTableWithComputedColumnAndPushDown(): Unit = {
    if (legacyTableSource) {
      // Computed column do not support in legacyTableSource.
      return
    }
    val sql = s"SELECT T.id, T.len, T.content, D.name, D.age, D.nominal_age " +
      "FROM src AS T JOIN userTableWithComputedColumn " +
      "for system_time as of T.proctime AS D ON T.id = D.id and D.nominal_age > 12"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq("2,15,Hello,Jark,22,23", "3,15,Fabian,Fabian,33,34")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testCurrentDateInJoinCondition(): Unit = {
    val id1 =
      TestValuesTableFactory.registerData(Seq(Row.of("abc", LocalDateTime.of(2000, 1, 1, 0, 0))))
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

    val id2 =
      TestValuesTableFactory.registerData(Seq(Row.of("abc", LocalDateTime.of(2000, 1, 2, 0, 0))))
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

  @Test
  def testLookupCacheSharingAcrossSubtasks(): Unit = {
    if (cacheType == LookupCacheType.NONE) {
      return
    }
    // Keep the cache for later validation
    LookupCacheManager.keepCacheOnRelease(true)
    try {
      // Use datagen source here to support parallel running
      val sourceDdl =
        s"""
           |CREATE TABLE T (
           |  id BIGINT,
           |  proc AS PROCTIME()
           |) WITH (
           |  'connector' = 'datagen',
           |  'fields.id.kind' = 'sequence',
           |  'fields.id.start' = '1',
           |  'fields.id.end' = '6'
           |)
           |""".stripMargin
      tEnv.executeSql(sourceDdl)
      val sql =
        """
          |SELECT T.id, D.name, D.age FROM T 
          |LEFT JOIN user_table FOR SYSTEM_TIME AS OF T.proc AS D 
          |ON T.id = D.id
          |""".stripMargin
      val sink = new TestingAppendSink
      tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
      env.execute()

      // Validate that only one cache is registered
      val managedCaches = LookupCacheManager.getInstance().getManagedCaches
      assertThat(managedCaches.size()).isEqualTo(1)

      val numEntries = if (cacheType == LookupCacheType.PARTIAL) 6 else userData.size
      // Validate 6 entries are cached for PARTIAL and all entries for FULL
      val cache = managedCaches.get(managedCaches.keySet().iterator().next()).getCache
      assertThat(cache.size()).isEqualTo(numEntries)

      // Validate contents of cached entries
      assertThatIterable(cache.getIfPresent(GenericRowData.of(jl(1L))))
        .containsExactlyInAnyOrder(
          GenericRowData.of(ji(11), jl(1L), BinaryStringData.fromString("Julian")))
      assertThatIterable(cache.getIfPresent(GenericRowData.of(jl(2L))))
        .containsExactlyInAnyOrder(
          GenericRowData.of(ji(22), jl(2L), BinaryStringData.fromString("Jark")))
      assertThatIterable(cache.getIfPresent(GenericRowData.of(jl(3L))))
        .containsExactlyInAnyOrder(
          GenericRowData.of(ji(33), jl(3L), BinaryStringData.fromString("Fabian")))
      assertThatIterable(cache.getIfPresent(GenericRowData.of(jl(4L))))
        .containsExactlyInAnyOrder(
          GenericRowData.of(ji(11), jl(4L), BinaryStringData.fromString("Hello world")))
      assertThatIterable(cache.getIfPresent(GenericRowData.of(jl(5L))))
        .containsExactlyInAnyOrder(
          GenericRowData.of(ji(11), jl(5L), BinaryStringData.fromString("Hello world")))
      assertThatIterable(cache.getIfPresent(GenericRowData.of(jl(6L))))
        .isEmpty()
    } finally {
      LookupCacheManager.getInstance().checkAllReleased()
      LookupCacheManager.getInstance().clear()
      LookupCacheManager.keepCacheOnRelease(false)
    }
  }

  def ji(i: Int): java.lang.Integer = {
    new java.lang.Integer(i)
  }

  def jl(l: Long): java.lang.Long = {
    new java.lang.Long(l)
  }

  @Test
  def testAggAndLeftJoinWithTryResolveMode(): Unit = {
    tEnv.getConfig.set(
      OptimizerConfigOptions.TABLE_OPTIMIZER_NONDETERMINISTIC_UPDATE_STRATEGY,
      OptimizerConfigOptions.NonDeterministicUpdateStrategy.TRY_RESOLVE)

    val sql1 = "SELECT max(id) as id, PROCTIME() as proctime FROM src AS T group by len"

    val table1 = tEnv.sqlQuery(sql1)
    tEnv.createTemporaryView("t1", table1)

    val sql2 = "SELECT t1.id, D.name, D.age FROM t1 LEFT JOIN user_table " +
      "for system_time as of t1.proctime AS D ON t1.id = D.id"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql2).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("3,Fabian,33", "8,null,null", "9,null,null")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testAggAndLeftJoinAllConstantKeyWithTryResolveMode(): Unit = {
    tEnv.getConfig.set(
      OptimizerConfigOptions.TABLE_OPTIMIZER_NONDETERMINISTIC_UPDATE_STRATEGY,
      OptimizerConfigOptions.NonDeterministicUpdateStrategy.TRY_RESOLVE)

    val sql1 = "SELECT max(id) as id, PROCTIME() as proctime FROM src AS T group by len"

    val table1 = tEnv.sqlQuery(sql1)
    tEnv.createTemporaryView("t1", table1)

    val sql2 = "SELECT t1.id, D.name, D.age FROM t1 LEFT JOIN user_table " +
      "for system_time as of t1.proctime AS D ON D.id = 3"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql2).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("3,Fabian,33", "8,Fabian,33", "9,Fabian,33")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testAggAndJoinAllConstantKeyWithTryResolveMode(): Unit = {
    // in fact this case will omit materialization because not right column was required from sink
    tEnv.getConfig.set(
      OptimizerConfigOptions.TABLE_OPTIMIZER_NONDETERMINISTIC_UPDATE_STRATEGY,
      OptimizerConfigOptions.NonDeterministicUpdateStrategy.TRY_RESOLVE)

    val sql1 = "SELECT max(id) as id, PROCTIME() as proctime FROM src AS T group by len"

    val table1 = tEnv.sqlQuery(sql1)
    tEnv.createTemporaryView("t1", table1)

    val sql2 = "SELECT t1.id FROM t1 LEFT JOIN user_table " +
      "for system_time as of t1.proctime AS D ON D.id = 3"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql2).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("3", "8", "9")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  private def getRetryLookupHint(lookupTable: String, maxAttempts: Int): String = {
    s"""
       |/*+ LOOKUP('table'='$lookupTable', 'retry-predicate'='lookup_miss',
       | 'retry-strategy'='fixed_delay',
       |  'fixed-delay'='5 ms',
       |   'max-attempts'='$maxAttempts')
       |*/""".stripMargin
  }

  @Test
  def testJoinTemporalTableWithRetry(): Unit = {
    val maxRetryTwiceHint = getRetryLookupHint("D", 2)
    val sink = new TestingAppendSink
    tEnv
      .sqlQuery(s"""
                   |SELECT $maxRetryTwiceHint T.id, T.len, T.content, D.name FROM src AS T
                   |JOIN user_table for system_time as of T.proctime AS D
                   |ON T.id = D.id
                   |""".stripMargin)
      .toAppendStream[Row]
      .addSink(sink)
    env.execute()

    // the result is deterministic because the test data of lookup source is static
    val expected = Seq("1,12,Julian,Julian", "2,15,Hello,Jark", "3,15,Fabian,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testJoinTemporalTableWithLookupThresholdWithInsufficientRetry(): Unit = {
    val maxRetryOnceHint = getRetryLookupHint("D", 1)
    val sink = new TestingAppendSink
    tEnv
      .sqlQuery(s"""
                   |SELECT $maxRetryOnceHint T.id, T.len, T.content, D.name FROM src AS T
                   |JOIN user_table_with_lookup_threshold3 for system_time as of T.proctime AS D
                   |ON T.id = D.id
                   |""".stripMargin)
      .toAppendStream[Row]
      .addSink(sink)
    env.execute()

    val expected = if (legacyTableSource || cacheType == LookupCacheType.FULL) {
      // legacy lookup source and full caching lookup do not support retry
      Seq("1,12,Julian,Julian", "2,15,Hello,Jark", "3,15,Fabian,Fabian")
    } else {
      // the user_table_with_lookup_threshold3 will return null result before 3rd lookup
      Seq()
    }
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testJoinTemporalTableWithLookupThresholdWithSufficientRetry(): Unit = {
    val maxRetryTwiceHint = getRetryLookupHint("D", 2)

    val sink = new TestingAppendSink
    tEnv
      .sqlQuery(s"""
                   |SELECT $maxRetryTwiceHint T.id, T.len, T.content, D.name FROM src AS T
                   |JOIN user_table_with_lookup_threshold2 for system_time as of T.proctime AS D
                   |ON T.id = D.id
                   |""".stripMargin)
      .toAppendStream[Row]
      .addSink(sink)
    env.execute()

    val expected = Seq("1,12,Julian,Julian", "2,15,Hello,Jark", "3,15,Fabian,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testJoinTemporalTableWithLookupThresholdWithLargerRetry(): Unit = {
    // max times beyond the lookup threshold of 'user_table_with_lookup_threshold2'
    val largerRetryHint = getRetryLookupHint("D", 10)

    val sink = new TestingAppendSink
    tEnv
      .sqlQuery(s"""
                   |SELECT $largerRetryHint T.id, T.len, T.content, D.name FROM src AS T
                   |JOIN user_table_with_lookup_threshold2 for system_time as of T.proctime AS D
                   |ON T.id = D.id
                   |""".stripMargin)
      .toAppendStream[Row]
      .addSink(sink)
    env.execute()

    val expected = Seq("1,12,Julian,Julian", "2,15,Hello,Jark", "3,15,Fabian,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }
}

object LookupJoinITCase {

  val LEGACY_TABLE_SOURCE: JBoolean = JBoolean.TRUE;
  val DYNAMIC_TABLE_SOURCE: JBoolean = JBoolean.FALSE;

  @Parameterized.Parameters(name = "LegacyTableSource={0}, cacheType={1}")
  def parameters(): JCollection[Array[Object]] = {
    Seq[Array[AnyRef]](
      Array(LEGACY_TABLE_SOURCE, LookupCacheType.NONE),
      Array(DYNAMIC_TABLE_SOURCE, LookupCacheType.NONE),
      Array(DYNAMIC_TABLE_SOURCE, LookupCacheType.PARTIAL),
      Array(DYNAMIC_TABLE_SOURCE, LookupCacheType.FULL)
    )
  }
}
