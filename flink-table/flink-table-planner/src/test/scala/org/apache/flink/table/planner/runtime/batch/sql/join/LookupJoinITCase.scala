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
package org.apache.flink.table.planner.runtime.batch.sql.join

import org.apache.flink.table.api.{TableSchema, Types}
import org.apache.flink.table.connector.source.lookup.LookupOptions
import org.apache.flink.table.connector.source.lookup.LookupOptions.{LookupCacheType, ReloadStrategy}
import org.apache.flink.table.data.GenericRowData
import org.apache.flink.table.data.binary.BinaryStringData
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.{BatchTestBase, InMemoryLookupableTableSource}
import org.apache.flink.table.runtime.functions.table.fullcache.inputformat.FullCacheTestInputFormat
import org.apache.flink.table.runtime.functions.table.lookup.LookupCacheManager
import org.apache.flink.testutils.junit.extensions.parameterized.{Parameter, ParameterizedTestExtension, Parameters}
import org.apache.flink.types.Row

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assumptions.assumeThat
import org.assertj.core.api.IterableAssert.assertThatIterable
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestTemplate}
import org.junit.jupiter.api.extension.ExtendWith

import java.lang.{Boolean => JBoolean}
import java.util

import scala.collection.JavaConversions._

@ExtendWith(Array(classOf[ParameterizedTestExtension]))
class LookupJoinITCase extends BatchTestBase {

  @Parameter var legacyTableSource: Boolean = _

  @Parameter(value = 1)
  var isAsyncMode: Boolean = _

  @Parameter(value = 2)
  var cacheType: LookupCacheType = _

  val data = List(
    rowOf(1L, 12L, "Julian"),
    rowOf(2L, 15L, "Hello"),
    rowOf(3L, 15L, "Fabian"),
    rowOf(8L, 11L, "Hello world"),
    rowOf(9L, 12L, "Hello world!"))

  val dataWithNull = List(
    rowOf(null, 15L, "Hello"),
    rowOf(3L, 15L, "Fabian"),
    rowOf(null, 11L, "Hello world"),
    rowOf(9L, 12L, "Hello world!"))

  val userData = List(rowOf(11, 1L, "Julian"), rowOf(22, 2L, "Jark"), rowOf(33, 3L, "Fabian"))

  val userDataWithNull = List(
    rowOf(11, 1L, "Julian"),
    rowOf(22, null, "Hello"),
    rowOf(33, 3L, "Fabian"),
    rowOf(44, null, "Hello world"))

  @BeforeEach
  override def before() {
    super.before()
    if (legacyTableSource) {
      InMemoryLookupableTableSource.RESOURCE_COUNTER.set(0)
    } else {
      TestValuesTableFactory.RESOURCE_COUNTER.set(0)
      FullCacheTestInputFormat.OPEN_CLOSED_COUNTER.set(0)
    }
    createScanTable("T", data)
    createScanTable("nullableT", dataWithNull)

    createLookupTable("userTable", userData)
    createLookupTable("userTableWithNull", userDataWithNull)
    createLookupTableWithComputedColumn("userTableWithComputedColumn", userData)

    // TODO: enable object reuse until [FLINK-12351] is fixed.
    env.getConfig.disableObjectReuse()
  }

  @AfterEach
  override def after(): Unit = {
    if (legacyTableSource) {
      assertThat(InMemoryLookupableTableSource.RESOURCE_COUNTER.get()).isEqualTo(0)
    } else {
      assertThat(TestValuesTableFactory.RESOURCE_COUNTER.get()).isEqualTo(0)
      assertThat(FullCacheTestInputFormat.OPEN_CLOSED_COUNTER.get()).isEqualTo(0)
    }
  }

  private def createLookupTable(tableName: String, data: List[Row]): Unit = {
    if (legacyTableSource) {
      val userSchema = TableSchema
        .builder()
        .field("age", Types.INT)
        .field("id", Types.LONG)
        .field("name", Types.STRING)
        .build()
      InMemoryLookupableTableSource.createTemporaryTable(
        tEnv,
        isAsyncMode,
        data,
        userSchema,
        tableName,
        isBounded = true)
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

      tEnv.executeSql(s"""
                         |CREATE TABLE $tableName (
                         |  `age` INT,
                         |  `id` BIGINT,
                         |  `name` STRING
                         |) WITH (
                         |  $cacheOptions
                         |  'connector' = 'values',
                         |  'data-id' = '$dataId',
                         |  'async' = '$isAsyncMode',
                         |  'bounded' = 'true'
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
                         |  'data-id' = '$dataId',
                         |  'async' = '$isAsyncMode',
                         |  'bounded' = 'true'
                         |)
                         |""".stripMargin)
    }
  }

  private def createScanTable(tableName: String, data: List[Row]): Unit = {
    val dataId = TestValuesTableFactory.registerData(data)
    tEnv.executeSql(s"""
                       |CREATE TABLE $tableName (
                       |  `id` BIGINT,
                       |  `len` BIGINT,
                       |  `content` STRING,
                       |  `proctime` AS PROCTIME()
                       |) WITH (
                       |  'connector' = 'values',
                       |  'data-id' = '$dataId',
                       |  'bounded' = 'true'
                       |)
                       |""".stripMargin)
  }

  @TestTemplate
  def testLeftJoinTemporalTableWithLocalPredicate(): Unit = {
    val sql = s"SELECT T.id, T.len, T.content, D.name, D.age FROM T LEFT JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.id = D.id " +
      "AND T.len > 1 AND D.age > 20 AND D.name = 'Fabian' " +
      "WHERE T.id > 1"

    val expected = Seq(
      BatchTestBase.row(2, 15, "Hello", null, null),
      BatchTestBase.row(3, 15, "Fabian", "Fabian", 33),
      BatchTestBase.row(8, 11, "Hello world", null, null),
      BatchTestBase.row(9, 12, "Hello world!", null, null)
    )
    checkResult(sql, expected)
  }

  @TestTemplate
  def testJoinTemporalTable(): Unit = {
    val sql = s"SELECT T.id, T.len, T.content, D.name FROM T JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.id = D.id"

    val expected = Seq(
      BatchTestBase.row(1, 12, "Julian", "Julian"),
      BatchTestBase.row(2, 15, "Hello", "Jark"),
      BatchTestBase.row(3, 15, "Fabian", "Fabian"))
    checkResult(sql, expected)
  }

  @TestTemplate
  def testJoinTemporalTableWithPushDown(): Unit = {
    val sql = s"SELECT T.id, T.len, T.content, D.name FROM T JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.id = D.id AND D.age > 20"

    val expected =
      Seq(BatchTestBase.row(2, 15, "Hello", "Jark"), BatchTestBase.row(3, 15, "Fabian", "Fabian"))
    checkResult(sql, expected)
  }

  @TestTemplate
  def testJoinTemporalTableWithNonEqualFilter(): Unit = {
    val sql = s"SELECT T.id, T.len, T.content, D.name, D.age FROM T JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.id = D.id WHERE T.len <= D.age"

    val expected = Seq(
      BatchTestBase.row(2, 15, "Hello", "Jark", 22),
      BatchTestBase.row(3, 15, "Fabian", "Fabian", 33))
    checkResult(sql, expected)
  }

  @TestTemplate
  def testJoinTemporalTableOnMultiFields(): Unit = {
    val sql = s"SELECT T.id, T.len, D.name FROM T JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.id = D.id AND T.content = D.name"

    val expected = Seq(BatchTestBase.row(1, 12, "Julian"), BatchTestBase.row(3, 15, "Fabian"))
    checkResult(sql, expected)
  }

  @TestTemplate
  def testJoinTemporalTableOnMultiFieldsWithUdf(): Unit = {
    val sql = s"SELECT T.id, T.len, D.name FROM T JOIN userTable " +
      "for system_time as of T.proctime AS D ON mod(T.id, 4) = D.id AND T.content = D.name"

    val expected = Seq(BatchTestBase.row(1, 12, "Julian"), BatchTestBase.row(3, 15, "Fabian"))
    checkResult(sql, expected)
  }

  @TestTemplate
  def testJoinTemporalTableOnMultiKeyFields(): Unit = {
    val sql = s"SELECT T.id, T.len, D.name FROM T JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.content = D.name AND T.id = D.id"

    val expected = Seq(BatchTestBase.row(1, 12, "Julian"), BatchTestBase.row(3, 15, "Fabian"))
    checkResult(sql, expected)
  }

  @TestTemplate
  def testLeftJoinTemporalTable(): Unit = {
    val sql = s"SELECT T.id, T.len, D.name, D.age FROM T LEFT JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.id = D.id"

    val expected = Seq(
      BatchTestBase.row(1, 12, "Julian", 11),
      BatchTestBase.row(2, 15, "Jark", 22),
      BatchTestBase.row(3, 15, "Fabian", 33),
      BatchTestBase.row(8, 11, null, null),
      BatchTestBase.row(9, 12, null, null)
    )
    checkResult(sql, expected)
  }

  @TestTemplate
  def testJoinTemporalTableOnMultiKeyFieldsWithNullData(): Unit = {
    val sql = s"SELECT T.id, T.len, D.name FROM nullableT T JOIN userTableWithNull " +
      "for system_time as of T.proctime AS D ON T.content = D.name AND T.id = D.id"

    val expected = Seq(BatchTestBase.row(3, 15, "Fabian"))
    checkResult(sql, expected)
  }

  @TestTemplate
  def testLeftJoinTemporalTableOnMultiKeyFieldsWithNullData(): Unit = {
    val sql = s"SELECT D.id, T.len, D.name FROM nullableT T LEFT JOIN userTableWithNull " +
      "for system_time as of T.proctime AS D ON T.content = D.name AND T.id = D.id"
    val expected = Seq(
      BatchTestBase.row(null, 15, null),
      BatchTestBase.row(3, 15, "Fabian"),
      BatchTestBase.row(null, 11, null),
      BatchTestBase.row(null, 12, null))
    checkResult(sql, expected)
  }

  @TestTemplate
  def testJoinTemporalTableOnNullConstantKey(): Unit = {
    val sql = s"SELECT T.id, T.len, T.content FROM T JOIN userTable " +
      "for system_time as of T.proctime AS D ON D.id = null"
    val expected = Seq()
    checkResult(sql, expected)
  }

  @TestTemplate
  def testJoinTemporalTableOnMultiKeyFieldsWithNullConstantKey(): Unit = {
    val sql = s"SELECT T.id, T.len, D.name FROM T JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.content = D.name AND null = D.id"
    val expected = Seq()
    checkResult(sql, expected)
  }

  @TestTemplate
  def testJoinTemporalTableWithComputedColumn(): Unit = {
    // Computed column do not support in legacyTableSource.
    assumeThat(legacyTableSource).isFalse

    val sql = s"SELECT T.id, T.len, T.content, D.name, D.age, D.nominal_age " +
      "FROM T JOIN userTableWithComputedColumn " +
      "for system_time as of T.proctime AS D ON T.id = D.id"

    val expected = Seq(
      BatchTestBase.row(1, 12, "Julian", "Julian", 11, 12),
      BatchTestBase.row(2, 15, "Hello", "Jark", 22, 23),
      BatchTestBase.row(3, 15, "Fabian", "Fabian", 33, 34))
    checkResult(sql, expected)
  }

  @TestTemplate
  def testJoinTemporalTableWithComputedColumnAndPushDown(): Unit = {
    // Computed column do not support in legacyTableSource.
    assumeThat(legacyTableSource).isFalse

    val sql = s"SELECT T.id, T.len, T.content, D.name, D.age, D.nominal_age " +
      "FROM T JOIN userTableWithComputedColumn " +
      "for system_time as of T.proctime AS D ON T.id = D.id and D.nominal_age > 12"

    val expected = Seq(
      BatchTestBase.row(2, 15, "Hello", "Jark", 22, 23),
      BatchTestBase.row(3, 15, "Fabian", "Fabian", 33, 34))
    checkResult(sql, expected)
  }

  @TestTemplate
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
           |CREATE TABLE datagen_source (
           |  id BIGINT,
           |  proc AS PROCTIME()
           |) WITH (
           |  'connector' = 'datagen',
           |  'fields.id.kind' = 'sequence',
           |  'fields.id.start' = '1',
           |  'fields.id.end' = '6',
           |  'number-of-rows' = '6'
           |)
           |""".stripMargin
      tEnv.executeSql(sourceDdl)
      val sql =
        """
          |SELECT T.id, D.name, D.age FROM datagen_source as T 
          |LEFT JOIN userTable FOR SYSTEM_TIME AS OF T.proc AS D 
          |ON T.id = D.id
          |""".stripMargin
      executeQuery(parseQuery(sql))

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
      assertThatIterable(cache.getIfPresent(GenericRowData.of(jl(4L)))).isEmpty()
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
}

object LookupJoinITCase {

  val LEGACY_TABLE_SOURCE: JBoolean = JBoolean.TRUE;
  val DYNAMIC_TABLE_SOURCE: JBoolean = JBoolean.FALSE;
  val ASYNC_MODE: JBoolean = JBoolean.TRUE;
  val SYNC_MODE: JBoolean = JBoolean.FALSE;

  @Parameters(name = "LegacyTableSource={0}, isAsyncMode = {1}, cacheType = {2}")
  def parameters(): util.Collection[Array[java.lang.Object]] = {
    Seq[Array[AnyRef]](
      Array(LEGACY_TABLE_SOURCE, ASYNC_MODE, LookupCacheType.NONE),
      Array(LEGACY_TABLE_SOURCE, SYNC_MODE, LookupCacheType.NONE),
      Array(DYNAMIC_TABLE_SOURCE, ASYNC_MODE, LookupCacheType.NONE),
      Array(DYNAMIC_TABLE_SOURCE, SYNC_MODE, LookupCacheType.NONE),
      Array(DYNAMIC_TABLE_SOURCE, ASYNC_MODE, LookupCacheType.PARTIAL),
      Array(DYNAMIC_TABLE_SOURCE, SYNC_MODE, LookupCacheType.PARTIAL),
      Array(DYNAMIC_TABLE_SOURCE, SYNC_MODE, LookupCacheType.FULL)
    )
  }
}
