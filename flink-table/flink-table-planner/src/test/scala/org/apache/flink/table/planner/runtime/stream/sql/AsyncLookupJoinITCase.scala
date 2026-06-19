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

import org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.tableConversions
import org.apache.flink.table.api.config.{ExecutionConfigOptions, OptimizerConfigOptions}
import org.apache.flink.table.api.config.ExecutionConfigOptions.AsyncOutputMode
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.source.lookup.LookupOptions
import org.apache.flink.table.data.GenericRowData
import org.apache.flink.table.data.binary.BinaryStringData
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow
import org.apache.flink.table.planner.runtime.utils.{StreamingWithStateTestBase, TestingRetractSink, TestSinkUtil}
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.{HEAP_BACKEND, ROCKSDB_BACKEND, StateBackendMode}
import org.apache.flink.table.planner.runtime.utils.UserDefinedFunctionTestUtils._
import org.apache.flink.table.runtime.functions.table.lookup.LookupCacheManager
import org.apache.flink.testutils.junit.extensions.parameterized.{ParameterizedTestExtension, Parameters}
import org.apache.flink.types.Row

import org.assertj.core.api.Assertions.{assertThat, assertThatIterable, assertThatThrownBy}
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestTemplate}
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.extension.ExtendWith

import java.lang.{Boolean => JBoolean}
import java.util.{Collection => JCollection}

import scala.collection.JavaConversions._

@ExtendWith(Array(classOf[ParameterizedTestExtension]))
class AsyncLookupJoinITCase(
    backend: StateBackendMode,
    objectReuse: Boolean,
    asyncOutputMode: AsyncOutputMode,
    keyOrdered: Boolean,
    enableCache: Boolean)
  extends StreamingWithStateTestBase(backend) {

  val data = List(
    rowOf(1L, 12, "Julian"),
    rowOf(2L, 15, "Hello"),
    rowOf(3L, 15, "Fabian"),
    rowOf(8L, 11, "Hello world"),
    rowOf(9L, 12, "Hello world!"))

  var cdcRowData =
    List(
      changelogRow("+I", jl(1L), Int.box(12), "Julian"),
      changelogRow("-U", jl(1L), Int.box(12), "Julian"),
      changelogRow("+U", jl(1L), Int.box(13), "Julian"),
      changelogRow("-D", jl(1L), Int.box(13), "Julian"),
      changelogRow("+I", jl(1L), Int.box(14), "Julian"),
      changelogRow("-U", jl(1L), Int.box(14), "Julian"),
      changelogRow("+U", jl(1L), Int.box(15), "Julian"),
      changelogRow("+I", jl(2L), Int.box(16), "Hello"),
      changelogRow("-U", jl(2L), Int.box(16), "Hello"),
      changelogRow("+U", jl(2L), Int.box(17), "Hello"),
      changelogRow("-U", jl(2L), Int.box(17), "Hello"),
      changelogRow("+U", jl(2L), Int.box(18), "Hello"),
      changelogRow("+I", jl(3L), Int.box(19), "Fabian"),
      changelogRow("-D", jl(3L), Int.box(19), "Fabian")
    )

  val userData = List(rowOf(11, 1L, "Julian"), rowOf(22, 2L, "Jark"), rowOf(33, 3L, "Fabian"))

  @BeforeEach
  override def before(): Unit = {
    super.before()
    TestValuesTableFactory.RESOURCE_COUNTER.set(0)
    if (objectReuse) {
      env.getConfig.enableObjectReuse()
    } else {
      env.getConfig.disableObjectReuse()
    }

    tEnv.getConfig.set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_OUTPUT_MODE, asyncOutputMode)
    tEnv.getConfig.set(
      ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_KEY_ORDERED,
      Boolean.box(keyOrdered))

    createScanTable("src", data, isCdc = false)
    createScanTable("cdc_src", cdcRowData, isCdc = true)
    createLookupTable("user_table", userData)
    // lookup will start from the 2nd time, first lookup will always get null result
    createLookupTable("user_table_with_lookup_threshold2", userData, 2)
    // lookup will start from the 3rd time, first lookup will always get null result
    createLookupTable("user_table_with_lookup_threshold3", userData, 3)
  }

  @AfterEach
  override def after(): Unit = {
    super.after()
    assertThat(TestValuesTableFactory.RESOURCE_COUNTER).hasValue(0)
  }

  private def createLookupTable(
      tableName: String,
      data: List[Row],
      lookupThreshold: Int = -1): Unit = {
    val dataId = TestValuesTableFactory.registerData(data)
    val cacheOptions =
      if (enableCache) {
        s"""
           |  '${LookupOptions.CACHE_TYPE.key()}' = '${LookupOptions.LookupCacheType.PARTIAL}',
           |  '${LookupOptions.PARTIAL_CACHE_MAX_ROWS.key()}' = '${Long.MaxValue}',
           |""".stripMargin
      } else { "" }
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
                       |  'data-id' = '$dataId',
                       |  'async' = 'true'
                       |)
                       |""".stripMargin)
  }

  // TODO a base class or utility class is better to reuse code for this and LookupJoinITCase
  private def getAsyncRetryLookupHint(lookupTable: String, maxAttempts: Int): String = {
    s"""
       |/*+ LOOKUP('table'='$lookupTable', 
       | 'async'='true', 
       | 'time-out'='300s',
       | 'retry-predicate'='lookup_miss',
       | 'retry-strategy'='fixed_delay',
       | 'fixed-delay'='1 ms',
       | 'max-attempts'='$maxAttempts')
       |*/""".stripMargin
  }

  private def createScanTable(tableName: String, data: List[Row], isCdc: Boolean): Unit = {
    val dataId = TestValuesTableFactory.registerData(data)
    tEnv.executeSql(s"""
                       |CREATE TABLE $tableName (
                       |  `id` BIGINT ${if (isCdc) "PRIMARY KEY NOT ENFORCED" else ""},
                       |  `len` INT,
                       |  `content` STRING,
                       |  `proctime` AS PROCTIME()
                       |) WITH (
                       |  'connector' = 'values',
                       |  'data-id' = '$dataId',
                       |  'changelog-mode' = '${if (isCdc) "I,UA,UB,D" else "I"}'
                       |)
                       |""".stripMargin)
  }

  @TestTemplate
  def testKeyOrderedAsyncJoinTableWithCdc(): Unit = {
    assumeTrue(keyOrdered)
    val sql =
      """
        |SELECT t1.id, t1.len, D.name
        |FROM (select content, id, len, proctime FROM cdc_src AS T) t1
        |JOIN user_table for system_time as of t1.proctime AS D
        |ON t1.content = D.name AND t1.id = D.id
      """.stripMargin

    assertResult(sql, List("+I[1, 15, Julian]"))
  }

  @TestTemplate
  def testAsyncJoinTemporalTableOnMultiKeyFields(): Unit = {
    // test left table's join key define order diffs from right's
    val sql =
      """
        |SELECT t1.id, t1.len, D.name
        |FROM (select content, id, len, proctime FROM src AS T) t1
        |JOIN user_table for system_time as of t1.proctime AS D
        |ON t1.content = D.name AND t1.id = D.id
      """.stripMargin

    assertResult(sql, List("+I[1, 12, Julian]", "+I[3, 15, Fabian]"))
  }

  @TestTemplate
  def testAsyncJoinTemporalTable(): Unit = {
    val sql = "SELECT T.id, T.len, T.content, D.name FROM src AS T JOIN user_table " +
      "for system_time as of T.proctime AS D ON T.id = D.id"

    assertResult(
      sql,
      List("+I[1, 12, Julian, Julian]", "+I[2, 15, Hello, Jark]", "+I[3, 15, Fabian, Fabian]"))
  }

  @TestTemplate
  def testAsyncJoinTemporalTableWithPushDown(): Unit = {
    val sql = "SELECT T.id, T.len, T.content, D.name FROM src AS T JOIN user_table " +
      "for system_time as of T.proctime AS D ON T.id = D.id AND D.age > 20"

    assertResult(sql, List("+I[2, 15, Hello, Jark]", "+I[3, 15, Fabian, Fabian]"))
  }

  @TestTemplate
  def testAsyncJoinTemporalTableWithNonEqualFilter(): Unit = {
    val sql = "SELECT T.id, T.len, T.content, D.name, D.age FROM src AS T JOIN user_table " +
      "for system_time as of T.proctime AS D ON T.id = D.id WHERE T.len <= D.age"

    assertResult(sql, List("+I[2, 15, Hello, Jark, 22]", "+I[3, 15, Fabian, Fabian, 33]"))
  }

  @TestTemplate
  def testAsyncLeftJoinTemporalTableWithLocalPredicate(): Unit = {
    val sql = "SELECT T.id, T.len, T.content, D.name, D.age FROM src AS T LEFT JOIN user_table " +
      "for system_time as of T.proctime AS D ON T.id = D.id " +
      "AND T.len > 1 AND D.age > 20 AND D.name = 'Fabian' " +
      "WHERE T.id > 1"

    assertResult(
      sql,
      List(
        "+I[2, 15, Hello, null, null]",
        "+I[3, 15, Fabian, Fabian, 33]",
        "+I[8, 11, Hello world, null, null]",
        "+I[9, 12, Hello world!, null, null]")
    )
  }

  @TestTemplate
  def testAsyncJoinTemporalTableOnMultiFields(): Unit = {
    val sql = "SELECT T.id, T.len, D.name FROM src AS T JOIN user_table " +
      "for system_time as of T.proctime AS D ON T.id = D.id AND T.content = D.name"

    assertResult(sql, List("+I[1, 12, Julian]", "+I[3, 15, Fabian]"))
  }

  @TestTemplate
  def testAsyncJoinTemporalTableOnMultiFieldsWithUdf(): Unit = {
    tEnv.createTemporarySystemFunction("mod1", TestMod)
    tEnv.createTemporarySystemFunction("wrapper1", TestWrapperUdf)

    val sql = "SELECT T.id, T.len, wrapper1(D.name) as name FROM src AS T JOIN user_table " +
      "for system_time as of T.proctime AS D " +
      "ON mod1(T.id, 4) = D.id AND T.content = D.name"

    assertResult(sql, List("+I[1, 12, Julian]", "+I[3, 15, Fabian]"))
  }

  @TestTemplate
  def testAsyncJoinTemporalTableWithUdfFilter(): Unit = {
    tEnv.createTemporarySystemFunction("add", new TestAddWithOpen)

    val sql = "SELECT T.id, T.len, T.content, D.name FROM src AS T JOIN user_table " +
      "for system_time as of T.proctime AS D ON T.id = D.id " +
      "WHERE add(T.id, D.id) > 3 AND add(T.id, 2) > 3 AND add (D.id, 2) > 3"

    assertResult(sql, List("+I[2, 15, Hello, Jark]", "+I[3, 15, Fabian, Fabian]"))
  }

  @TestTemplate
  def testAggAndAsyncLeftJoinTemporalTable(): Unit = {
    val sql1 = "SELECT max(id) as id, PROCTIME() as proctime FROM src AS T group by len"

    val table1 = tEnv.sqlQuery(sql1)
    tEnv.createTemporaryView("t1", table1)

    val sql2 = "SELECT t1.id, D.name, D.age FROM t1 LEFT JOIN user_table " +
      "for system_time as of t1.proctime AS D ON t1.id = D.id"

    assertResult(sql2, List("+I[3, Fabian, 33]", "+I[8, null, null]", "+I[9, null, null]"))
  }

  @TestTemplate
  def testAggAndAsyncLeftJoinWithTryResolveMode(): Unit = {
    // will require a sync lookup function because input has update on TRY_RESOLVE mode
    // there's no test sources that have both sync and async lookup functions
    tEnv.getConfig.set(
      OptimizerConfigOptions.TABLE_OPTIMIZER_NONDETERMINISTIC_UPDATE_STRATEGY,
      OptimizerConfigOptions.NonDeterministicUpdateStrategy.TRY_RESOLVE)

    val sql1 = "SELECT max(id) as id, PROCTIME() as proctime FROM src AS T group by len"

    val table1 = tEnv.sqlQuery(sql1)
    tEnv.createTemporaryView("t1", table1)

    val sql2 = "SELECT t1.id, D.name, D.age FROM t1 LEFT JOIN user_table " +
      "for system_time as of t1.proctime AS D ON t1.id = D.id"

    assertThatThrownBy(
      () => {
        assertResult(sql2, List("+I[3, Fabian, 33]", "+I[8, null, null]", "+I[9, null, null]"))
      })
      .hasMessageContaining("Required sync lookup function by planner")
      .isInstanceOf[TableException]

  }

  @TestTemplate
  def testAsyncLeftJoinTemporalTable(): Unit = {
    val sql = "SELECT T.id, T.len, D.name, D.age FROM src AS T LEFT JOIN user_table " +
      "for system_time as of T.proctime AS D ON T.id = D.id"

    assertResult(
      sql,
      List(
        "+I[1, 12, Julian, 11]",
        "+I[2, 15, Jark, 22]",
        "+I[3, 15, Fabian, 33]",
        "+I[8, 11, null, null]",
        "+I[9, 12, null, null]"))
  }

  @TestTemplate
  def testExceptionThrownFromAsyncJoinTemporalTable(): Unit = {
    tEnv.createTemporarySystemFunction("errorFunc", TestExceptionThrown)

    val sql = "SELECT T.id, T.len, D.name, D.age FROM src AS T LEFT JOIN user_table " +
      "for system_time as of T.proctime AS D ON T.id = D.id " +
      "where errorFunc(D.name) > cast(1000 as decimal(10,4))" // should exception here

    assertThatThrownBy(() => assertResult(sql, List()))
      .satisfies(anyCauseMatches(classOf[NumberFormatException], "Cannot parse"))
  }

  @TestTemplate
  def testLookupCacheSharingAcrossSubtasks(): Unit = {
    if (!enableCache) {
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
      val sink = new TestingRetractSink()
      tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink)
      env.execute()

      // Validate that only one cache is registered
      val managedCaches = LookupCacheManager.getInstance().getManagedCaches
      assertThat(managedCaches.size()).isEqualTo(1)

      // Validate 6 entries are cached
      val cache = managedCaches.get(managedCaches.keySet().iterator().next()).getCache
      assertThat(cache.size()).isEqualTo(6)

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

  @TestTemplate
  def testAsyncJoinTemporalTableWithRetry(): Unit = {
    val maxRetryTwiceHint = getAsyncRetryLookupHint("D", 2)
    val sql = s"""
                 |SELECT $maxRetryTwiceHint T.id, T.len, T.content, D.name FROM src AS T
                 |JOIN user_table for system_time as of T.proctime AS D
                 |ON T.id = D.id
                 |""".stripMargin

    assertResult(
      sql,
      List("+I[1, 12, Julian, Julian]", "+I[2, 15, Hello, Jark]", "+I[3, 15, Fabian, Fabian]"))
  }

  @TestTemplate
  def testAsyncJoinTemporalTableWithLookupThresholdWithInsufficientRetry(): Unit = {
    val maxRetryOnceHint = getAsyncRetryLookupHint("D", 1)
    val sql = s"""
                 |SELECT $maxRetryOnceHint T.id, T.len, T.content, D.name FROM src AS T
                 |JOIN user_table_with_lookup_threshold3 for system_time as of T.proctime AS D
                 |ON T.id = D.id
                 |""".stripMargin
    assertResult(sql, List())
  }

  @TestTemplate
  def testAsyncJoinTemporalTableWithLookupThresholdWithSufficientRetry(): Unit = {
    // When enable async retry, there should left enough time for the async operator doing delayed
    // retry work, but due the fast finish of testing bounded source, it has no assurance of the
    // max attempts number, it only ensures at least one retry for each element in current version
    // so we can only use a max lookup threshold to 2 to get a deterministic results
    val maxRetryTwiceHint = getAsyncRetryLookupHint("D", 2)
    val sql = s"""
                 |SELECT $maxRetryTwiceHint T.id, T.len, T.content, D.name FROM src AS T
                 |JOIN user_table_with_lookup_threshold2 for system_time as of T.proctime AS D
                 |ON T.id = D.id
                 |""".stripMargin
    assertResult(
      sql,
      List("+I[1, 12, Julian, Julian]", "+I[2, 15, Hello, Jark]", "+I[3, 15, Fabian, Fabian]"))
  }

  def assertResult(sql: String, expected: List[String]): Unit = {
    val result = tEnv.sqlQuery(sql)
    TestSinkUtil.addValuesSink(tEnv, "MySink", result, ChangelogMode.all())
    result.executeInsert("MySink").await()

    assertThat(
      TestValuesTableFactory
        .getResultsAsStrings("MySink")
        .sorted
        .toList).isEqualTo(expected.sorted)
  }

}

object AsyncLookupJoinITCase {

  val ENABLE_OBJECT_REUSE: JBoolean = JBoolean.TRUE;
  val DISABLE_OBJECT_REUSE: JBoolean = JBoolean.FALSE;
  val ENABLE_CACHE: JBoolean = JBoolean.TRUE;
  val DISABLE_CACHE: JBoolean = JBoolean.FALSE;
  val ENABLE_KEY_ORDERED: JBoolean = JBoolean.TRUE;
  val DISABLE_KEY_ORDERED: JBoolean = JBoolean.FALSE;

  @Parameters(
    name =
      "StateBackend={0}, ObjectReuse={1}, AsyncOutputMode={2}, keyOrdered={3}, EnableCache={4}")
  def parameters(): JCollection[Array[Object]] = {
    Seq[Array[AnyRef]](
      Array(
        HEAP_BACKEND,
        ENABLE_OBJECT_REUSE,
        AsyncOutputMode.ALLOW_UNORDERED,
        ENABLE_KEY_ORDERED,
        DISABLE_CACHE),
      Array(
        HEAP_BACKEND,
        ENABLE_OBJECT_REUSE,
        AsyncOutputMode.ALLOW_UNORDERED,
        DISABLE_KEY_ORDERED,
        DISABLE_CACHE),
      Array(
        ROCKSDB_BACKEND,
        DISABLE_OBJECT_REUSE,
        AsyncOutputMode.ORDERED,
        DISABLE_KEY_ORDERED,
        DISABLE_CACHE),
      Array(
        HEAP_BACKEND,
        DISABLE_OBJECT_REUSE,
        AsyncOutputMode.ORDERED,
        DISABLE_KEY_ORDERED,
        DISABLE_CACHE),
      Array(
        HEAP_BACKEND,
        ENABLE_OBJECT_REUSE,
        AsyncOutputMode.ORDERED,
        DISABLE_KEY_ORDERED,
        DISABLE_CACHE),
      Array(
        ROCKSDB_BACKEND,
        DISABLE_OBJECT_REUSE,
        AsyncOutputMode.ALLOW_UNORDERED,
        DISABLE_KEY_ORDERED,
        DISABLE_CACHE),
      Array(
        ROCKSDB_BACKEND,
        ENABLE_OBJECT_REUSE,
        AsyncOutputMode.ALLOW_UNORDERED,
        ENABLE_KEY_ORDERED,
        DISABLE_CACHE),
      Array(
        ROCKSDB_BACKEND,
        ENABLE_OBJECT_REUSE,
        AsyncOutputMode.ALLOW_UNORDERED,
        DISABLE_KEY_ORDERED,
        DISABLE_CACHE),
      Array(
        HEAP_BACKEND,
        DISABLE_OBJECT_REUSE,
        AsyncOutputMode.ORDERED,
        DISABLE_KEY_ORDERED,
        ENABLE_CACHE),
      Array(
        HEAP_BACKEND,
        ENABLE_OBJECT_REUSE,
        AsyncOutputMode.ALLOW_UNORDERED,
        DISABLE_KEY_ORDERED,
        ENABLE_CACHE),
      Array(
        HEAP_BACKEND,
        DISABLE_OBJECT_REUSE,
        AsyncOutputMode.ALLOW_UNORDERED,
        ENABLE_KEY_ORDERED,
        ENABLE_CACHE)
    )
  }
}
