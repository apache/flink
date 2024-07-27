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
import org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches
import org.apache.flink.table.api.{TableException, TableSchema, Types}
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.config.{ExecutionConfigOptions, OptimizerConfigOptions}
import org.apache.flink.table.api.config.ExecutionConfigOptions.AsyncOutputMode
import org.apache.flink.table.connector.source.lookup.LookupOptions
import org.apache.flink.table.data.GenericRowData
import org.apache.flink.table.data.binary.BinaryStringData
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.{InMemoryLookupableTableSource, StreamingWithStateTestBase, TestingAppendSink, TestingRetractSink}
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.{HEAP_BACKEND, ROCKSDB_BACKEND, StateBackendMode}
import org.apache.flink.table.planner.runtime.utils.UserDefinedFunctionTestUtils._
import org.apache.flink.table.runtime.functions.table.lookup.LookupCacheManager
import org.apache.flink.testutils.junit.extensions.parameterized.{ParameterizedTestExtension, Parameters}
import org.apache.flink.types.Row

import org.assertj.core.api.Assertions.{assertThat, assertThatIterable, assertThatThrownBy}
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestTemplate}
import org.junit.jupiter.api.extension.ExtendWith

import java.lang.{Boolean => JBoolean}
import java.util.{Collection => JCollection}

import scala.collection.JavaConversions._

@ExtendWith(Array(classOf[ParameterizedTestExtension]))
class AsyncLookupJoinITCase(
    legacyTableSource: Boolean,
    backend: StateBackendMode,
    objectReuse: Boolean,
    asyncOutputMode: AsyncOutputMode,
    enableCache: Boolean)
  extends StreamingWithStateTestBase(backend) {

  val data = List(
    rowOf(1L, 12, "Julian"),
    rowOf(2L, 15, "Hello"),
    rowOf(3L, 15, "Fabian"),
    rowOf(8L, 11, "Hello world"),
    rowOf(9L, 12, "Hello world!"))

  val userData = List(rowOf(11, 1L, "Julian"), rowOf(22, 2L, "Jark"), rowOf(33, 3L, "Fabian"))

  @BeforeEach
  override def before(): Unit = {
    super.before()
    if (legacyTableSource) {
      InMemoryLookupableTableSource.RESOURCE_COUNTER.set(0)
    } else {
      TestValuesTableFactory.RESOURCE_COUNTER.set(0)
    }
    if (objectReuse) {
      env.getConfig.enableObjectReuse()
    } else {
      env.getConfig.disableObjectReuse()
    }

    tEnv.getConfig.set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_OUTPUT_MODE, asyncOutputMode)

    createScanTable("src", data)
    createLookupTable("user_table", userData)
    // lookup will start from the 2nd time, first lookup will always get null result
    createLookupTable("user_table_with_lookup_threshold2", userData, 2)
    // lookup will start from the 3rd time, first lookup will always get null result
    createLookupTable("user_table_with_lookup_threshold3", userData, 3)
  }

  @AfterEach
  override def after(): Unit = {
    super.after()
    if (legacyTableSource) {
      assertThat(InMemoryLookupableTableSource.RESOURCE_COUNTER).hasValue(0)
    } else {
      assertThat(TestValuesTableFactory.RESOURCE_COUNTER).hasValue(0)
    }
  }

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
        isAsync = true,
        data,
        userSchema,
        tableName)
    } else {
      val dataId = TestValuesTableFactory.registerData(data)
      val cacheOptions =
        if (enableCache)
          s"""
             |  '${LookupOptions.CACHE_TYPE.key()}' = '${LookupOptions.LookupCacheType.PARTIAL}',
             |  '${LookupOptions.PARTIAL_CACHE_MAX_ROWS.key()}' = '${Long.MaxValue}',
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
                         |  'data-id' = '$dataId',
                         |  'async' = 'true'
                         |)
                         |""".stripMargin)
    }
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

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toDataStream.addSink(sink)
    env.execute()

    val expected = Seq("1,12,Julian", "3,15,Fabian")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testAsyncJoinTemporalTable(): Unit = {
    val sql = "SELECT T.id, T.len, T.content, D.name FROM src AS T JOIN user_table " +
      "for system_time as of T.proctime AS D ON T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toDataStream.addSink(sink)
    env.execute()

    val expected = Seq("1,12,Julian,Julian", "2,15,Hello,Jark", "3,15,Fabian,Fabian")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testAsyncJoinTemporalTableWithPushDown(): Unit = {
    val sql = "SELECT T.id, T.len, T.content, D.name FROM src AS T JOIN user_table " +
      "for system_time as of T.proctime AS D ON T.id = D.id AND D.age > 20"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toDataStream.addSink(sink)
    env.execute()

    val expected = Seq("2,15,Hello,Jark", "3,15,Fabian,Fabian")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testAsyncJoinTemporalTableWithNonEqualFilter(): Unit = {
    val sql = "SELECT T.id, T.len, T.content, D.name, D.age FROM src AS T JOIN user_table " +
      "for system_time as of T.proctime AS D ON T.id = D.id WHERE T.len <= D.age"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toDataStream.addSink(sink)
    env.execute()

    val expected = Seq("2,15,Hello,Jark,22", "3,15,Fabian,Fabian,33")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testAsyncLeftJoinTemporalTableWithLocalPredicate(): Unit = {
    val sql = "SELECT T.id, T.len, T.content, D.name, D.age FROM src AS T LEFT JOIN user_table " +
      "for system_time as of T.proctime AS D ON T.id = D.id " +
      "AND T.len > 1 AND D.age > 20 AND D.name = 'Fabian' " +
      "WHERE T.id > 1"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toDataStream.addSink(sink)
    env.execute()

    val expected = Seq(
      "2,15,Hello,null,null",
      "3,15,Fabian,Fabian,33",
      "8,11,Hello world,null,null",
      "9,12,Hello world!,null,null")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testAsyncJoinTemporalTableOnMultiFields(): Unit = {
    val sql = "SELECT T.id, T.len, D.name FROM src AS T JOIN user_table " +
      "for system_time as of T.proctime AS D ON T.id = D.id AND T.content = D.name"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toDataStream.addSink(sink)
    env.execute()

    val expected = Seq("1,12,Julian", "3,15,Fabian")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testAsyncJoinTemporalTableOnMultiFieldsWithUdf(): Unit = {
    tEnv.createTemporarySystemFunction("mod1", TestMod)
    tEnv.createTemporarySystemFunction("wrapper1", TestWrapperUdf)

    val sql = "SELECT T.id, T.len, wrapper1(D.name) as name FROM src AS T JOIN user_table " +
      "for system_time as of T.proctime AS D " +
      "ON mod1(T.id, 4) = D.id AND T.content = D.name"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toDataStream.addSink(sink)
    env.execute()

    val expected = Seq("1,12,Julian", "3,15,Fabian")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testAsyncJoinTemporalTableWithUdfFilter(): Unit = {
    tEnv.createTemporarySystemFunction("add", new TestAddWithOpen)

    val sql = "SELECT T.id, T.len, T.content, D.name FROM src AS T JOIN user_table " +
      "for system_time as of T.proctime AS D ON T.id = D.id " +
      "WHERE add(T.id, D.id) > 3 AND add(T.id, 2) > 3 AND add (D.id, 2) > 3"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toDataStream.addSink(sink)
    env.execute()

    val expected = Seq("2,15,Hello,Jark", "3,15,Fabian,Fabian")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
    assertThat(TestAddWithOpen.aliveCounter).hasValue(0)
  }

  @TestTemplate
  def testAggAndAsyncLeftJoinTemporalTable(): Unit = {
    val sql1 = "SELECT max(id) as id, PROCTIME() as proctime FROM src AS T group by len"

    val table1 = tEnv.sqlQuery(sql1)
    tEnv.createTemporaryView("t1", table1)

    val sql2 = "SELECT t1.id, D.name, D.age FROM t1 LEFT JOIN user_table " +
      "for system_time as of t1.proctime AS D ON t1.id = D.id"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql2).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("3,Fabian,33", "8,null,null", "9,null,null")
    assertThat(sink.getRetractResults.sorted).isEqualTo(expected.sorted)
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

    val sink = new TestingRetractSink
    assertThatThrownBy(
      () => {
        tEnv.sqlQuery(sql2).toRetractStream[Row].addSink(sink).setParallelism(1)

        env.execute()

        val expected = Seq("3,Fabian,33", "8,null,null", "9,null,null")
        assertThat(sink.getRetractResults.sorted).isEqualTo(expected.sorted)
      })
      .hasMessageContaining("Required sync lookup function by planner")
      .isInstanceOf[TableException]

  }

  @TestTemplate
  def testAsyncLeftJoinTemporalTable(): Unit = {
    val sql = "SELECT T.id, T.len, D.name, D.age FROM src AS T LEFT JOIN user_table " +
      "for system_time as of T.proctime AS D ON T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toDataStream.addSink(sink)
    env.execute()

    val expected =
      Seq("1,12,Julian,11", "2,15,Jark,22", "3,15,Fabian,33", "8,11,null,null", "9,12,null,null")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testExceptionThrownFromAsyncJoinTemporalTable(): Unit = {
    tEnv.createTemporarySystemFunction("errorFunc", TestExceptionThrown)

    val sql = "SELECT T.id, T.len, D.name, D.age FROM src AS T LEFT JOIN user_table " +
      "for system_time as of T.proctime AS D ON T.id = D.id " +
      "where errorFunc(D.name) > cast(1000 as decimal(10,4))" // should exception here

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toDataStream.addSink(sink)

    assertThatThrownBy(() => env.execute())
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
      val sink = new TestingAppendSink
      tEnv.sqlQuery(sql).toDataStream.addSink(sink)
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
    val sink = new TestingAppendSink
    tEnv
      .sqlQuery(s"""
                   |SELECT $maxRetryTwiceHint T.id, T.len, T.content, D.name FROM src AS T
                   |JOIN user_table for system_time as of T.proctime AS D
                   |ON T.id = D.id
                   |""".stripMargin)
      .toDataStream
      .addSink(sink)
    env.execute()

    // the result is deterministic because the test data of lookup source is static
    val expected = Seq("1,12,Julian,Julian", "2,15,Hello,Jark", "3,15,Fabian,Fabian")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testAsyncJoinTemporalTableWithLookupThresholdWithInsufficientRetry(): Unit = {
    val maxRetryOnceHint = getAsyncRetryLookupHint("D", 1)
    val sink = new TestingAppendSink
    tEnv
      .sqlQuery(s"""
                   |SELECT $maxRetryOnceHint T.id, T.len, T.content, D.name FROM src AS T
                   |JOIN user_table_with_lookup_threshold3 for system_time as of T.proctime AS D
                   |ON T.id = D.id
                   |""".stripMargin)
      .toDataStream
      .addSink(sink)
    env.execute()

    val expected = if (legacyTableSource) {
      // test legacy lookup source do not support lookup threshold
      // also legacy lookup source do not support retry
      Seq("1,12,Julian,Julian", "2,15,Hello,Jark", "3,15,Fabian,Fabian")
    } else {
      // the user_table_with_lookup_threshold3 will return null result before 3rd lookup
      Seq()
    }
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testAsyncJoinTemporalTableWithLookupThresholdWithSufficientRetry(): Unit = {
    // When enable async retry, there should left enough time for the async operator doing delayed
    // retry work, but due the fast finish of testing bounded source, it has no assurance of the
    // max attempts number, it only ensures at least one retry for each element in current version
    // so we can only use a max lookup threshold to 2 to get a deterministic results
    val maxRetryTwiceHint = getAsyncRetryLookupHint("D", 2)

    val sink = new TestingAppendSink
    tEnv
      .sqlQuery(s"""
                   |SELECT $maxRetryTwiceHint T.id, T.len, T.content, D.name FROM src AS T
                   |JOIN user_table_with_lookup_threshold2 for system_time as of T.proctime AS D
                   |ON T.id = D.id
                   |""".stripMargin)
      .toDataStream
      .addSink(sink)
    env.execute()

    val expected = Seq("1,12,Julian,Julian", "2,15,Hello,Jark", "3,15,Fabian,Fabian")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

}

object AsyncLookupJoinITCase {

  val LEGACY_TABLE_SOURCE: JBoolean = JBoolean.TRUE;
  val DYNAMIC_TABLE_SOURCE: JBoolean = JBoolean.FALSE;
  val ENABLE_OBJECT_REUSE: JBoolean = JBoolean.TRUE;
  val DISABLE_OBJECT_REUSE: JBoolean = JBoolean.FALSE;
  val ENABLE_CACHE: JBoolean = JBoolean.TRUE;
  val DISABLE_CACHE: JBoolean = JBoolean.FALSE;

  @Parameters(name =
    "LegacyTableSource={0}, StateBackend={1}, ObjectReuse={2}, AsyncOutputMode={3}, EnableCache={4}")
  def parameters(): JCollection[Array[Object]] = {
    Seq[Array[AnyRef]](
      Array(
        LEGACY_TABLE_SOURCE,
        HEAP_BACKEND,
        ENABLE_OBJECT_REUSE,
        AsyncOutputMode.ALLOW_UNORDERED,
        DISABLE_CACHE),
      Array(
        LEGACY_TABLE_SOURCE,
        ROCKSDB_BACKEND,
        DISABLE_OBJECT_REUSE,
        AsyncOutputMode.ORDERED,
        DISABLE_CACHE),
      Array(
        DYNAMIC_TABLE_SOURCE,
        HEAP_BACKEND,
        DISABLE_OBJECT_REUSE,
        AsyncOutputMode.ORDERED,
        DISABLE_CACHE),
      Array(
        DYNAMIC_TABLE_SOURCE,
        HEAP_BACKEND,
        ENABLE_OBJECT_REUSE,
        AsyncOutputMode.ORDERED,
        DISABLE_CACHE),
      Array(
        DYNAMIC_TABLE_SOURCE,
        ROCKSDB_BACKEND,
        DISABLE_OBJECT_REUSE,
        AsyncOutputMode.ALLOW_UNORDERED,
        DISABLE_CACHE),
      Array(
        DYNAMIC_TABLE_SOURCE,
        ROCKSDB_BACKEND,
        ENABLE_OBJECT_REUSE,
        AsyncOutputMode.ALLOW_UNORDERED,
        DISABLE_CACHE),
      Array(
        DYNAMIC_TABLE_SOURCE,
        HEAP_BACKEND,
        DISABLE_OBJECT_REUSE,
        AsyncOutputMode.ORDERED,
        ENABLE_CACHE),
      Array(
        DYNAMIC_TABLE_SOURCE,
        HEAP_BACKEND,
        ENABLE_OBJECT_REUSE,
        AsyncOutputMode.ALLOW_UNORDERED,
        ENABLE_CACHE)
    )
  }
}
