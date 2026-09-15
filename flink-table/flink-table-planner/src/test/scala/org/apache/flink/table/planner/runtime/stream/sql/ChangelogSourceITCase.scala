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

import org.apache.flink.core.testutils.EachCallbackWrapper
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.config.{ExecutionConfigOptions, OptimizerConfigOptions}
import org.apache.flink.table.planner.JBigDecimal
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow
import org.apache.flink.table.planner.runtime.stream.sql.ChangelogSourceITCase._
import org.apache.flink.table.planner.runtime.utils.{StreamingWithMiniBatchTestBase, TestData, TestingRetractSink}
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedTableFunctions.StringSplit
import org.apache.flink.table.planner.runtime.utils.StreamingWithMiniBatchTestBase.{MiniBatchMode, MiniBatchOff, MiniBatchOn}
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.{HEAP_BACKEND, ROCKSDB_BACKEND, StateBackendMode}
import org.apache.flink.table.utils.LegacyRowExtension
import org.apache.flink.testutils.junit.extensions.parameterized.{ParameterizedTestExtension, Parameters}
import org.apache.flink.types.{Row, RowKind}

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.{BeforeEach, TestTemplate}
import org.junit.jupiter.api.extension.{ExtendWith, RegisterExtension}

import java.lang.{Long => JLong}
import java.util

import scala.collection.JavaConversions._

/** Integration tests for operations on changelog source, including upsert source. */
@ExtendWith(Array(classOf[ParameterizedTestExtension]))
class ChangelogSourceITCase(
    sourceMode: SourceMode,
    miniBatch: MiniBatchMode,
    state: StateBackendMode
) extends StreamingWithMiniBatchTestBase(miniBatch, state) {

  @RegisterExtension private val _: EachCallbackWrapper[LegacyRowExtension] =
    new EachCallbackWrapper[LegacyRowExtension](new LegacyRowExtension)

  @BeforeEach
  override def before(): Unit = {
    super.before()
    val orderDataId = TestValuesTableFactory.registerData(TestData.ordersData)
    tEnv.executeSql(s"""
                       |CREATE TABLE orders (
                       |  amount BIGINT,
                       |  currency STRING
                       |) WITH (
                       | 'connector' = 'values',
                       | 'data-id' = '$orderDataId',
                       | 'changelog-mode' = 'I'
                       |)
                       |""".stripMargin)
    sourceMode match {
      case CHANGELOG_SOURCE => registerChangelogSource()
      case CHANGELOG_SOURCE_WITH_EVENTS_DUPLICATE => registerChangelogSourceWithEventsDuplicate()
      case UPSERT_SOURCE => registerUpsertSource()
      case NO_UPDATE_SOURCE => registerNoUpdateSource()
    }
  }

  @TestTemplate
  def testToRetractStream(): Unit = {
    val result = tEnv.sqlQuery(s"SELECT * FROM users").toRetractStream[Row]
    val sink = new TestingRetractSink()
    result.addSink(sink).setParallelism(result.getParallelism)
    env.execute()

    val expected = Seq(
      "user1,Tom,tom123@gmail.com,8.10,16.20",
      "user3,Bailey,bailey@qq.com,9.99,19.98",
      "user4,Tina,tina@gmail.com,11.30,22.60")
    assertThat(sink.getRetractResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testToUpsertSink(): Unit = {
    val sinkDDL =
      s"""
         |CREATE TABLE user_sink (
         |  user_id STRING PRIMARY KEY NOT ENFORCED,
         |  user_name STRING,
         |  email STRING,
         |  balance DECIMAL(18,2),
         |  balance2 DECIMAL(18,2)
         |) WITH (
         | 'connector' = 'values',
         | 'sink-insert-only' = 'false'
         |)
         |""".stripMargin
    val dml =
      s"""
         |INSERT INTO user_sink
         |SELECT * FROM users
         |ON CONFLICT DO DEDUPLICATE
         |""".stripMargin
    tEnv.executeSql(sinkDDL)
    tEnv.executeSql(dml).await()

    val expected = Seq(
      "user1,Tom,tom123@gmail.com,8.10,16.20",
      "user3,Bailey,bailey@qq.com,9.99,19.98",
      "user4,Tina,tina@gmail.com,11.30,22.60")
    assertThat(TestValuesTableFactory.getResultsAsStrings("user_sink").sorted)
      .isEqualTo(expected.sorted)

    sourceMode match {
      // verify the update_before messages haven been filtered when scanning changelog source
      // the CHANGELOG_SOURCE has I,UA,UB,D but no primary key, so we can not omit UB
      case CHANGELOG_SOURCE_WITH_EVENTS_DUPLICATE =>
        val rawResult = TestValuesTableFactory.getRawResultsAsStrings("user_sink")
        val hasUB = rawResult.exists(r => r.startsWith("-U"))
        assertThat(hasUB).isFalse
      case _ => // do nothing
    }
  }

  @TestTemplate
  def testAggregate(): Unit = {
    val query =
      s"""
         |SELECT count(*), sum(balance), max(email)
         |FROM users
         |""".stripMargin

    val result = tEnv.sqlQuery(query).toRetractStream[Row]
    val sink = new TestingRetractSink()
    result.addSink(sink).setParallelism(result.getParallelism)
    env.execute()

    val expected = Seq("3,29.39,tom123@gmail.com")
    assertThat(sink.getRetractResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testAggregateToUpsertSink(): Unit = {
    val sinkDDL =
      s"""
         |CREATE TABLE user_sink (
         |  `scope` STRING,
         |  cnt BIGINT,
         |  sum_balance DECIMAL(18,2),
         |  max_email STRING,
         |  PRIMARY KEY (`scope`) NOT ENFORCED
         |) WITH (
         | 'connector' = 'values',
         | 'sink-insert-only' = 'false'
         |)
         |""".stripMargin
    val dml =
      s"""
         |INSERT INTO user_sink
         |SELECT 'ALL', count(*), sum(balance), max(email)
         |FROM users
         |GROUP BY 'ALL'
         |ON CONFLICT DO DEDUPLICATE
         |""".stripMargin
    tEnv.executeSql(sinkDDL)
    tEnv.executeSql(dml).await()

    val expected = Seq("ALL,3,29.39,tom123@gmail.com")
    assertThat(TestValuesTableFactory.getResultsAsStrings("user_sink").sorted)
      .isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testGroupByNonPrimaryKey(): Unit = {
    val sinkDDL =
      s"""
         |CREATE TABLE user_sink (
         |  balance DECIMAL(18,2),
         |  cnt BIGINT,
         |  max_email STRING,
         |  PRIMARY KEY (balance) NOT ENFORCED
         |) WITH (
         | 'connector' = 'values',
         | 'sink-insert-only' = 'false'
         |)
         |""".stripMargin
    // Note: balance2 is computed as balance*2 which results in a higher precision DECIMAL type.
    // Since the sink's balance column has fixed precision, this requires ON CONFLICT handling
    // because narrowing DECIMAL casts (e.g., DECIMAL(28,2) -> DECIMAL(18,2)) are not injective.
    val dml =
      s"""
         |INSERT INTO user_sink
         |SELECT balance2, count(*), max(email)
         |FROM users
         |GROUP BY balance2
         |ON CONFLICT DO DEDUPLICATE
         |""".stripMargin
    tEnv.executeSql(sinkDDL)
    tEnv.executeSql(dml).await()

    val expected =
      Seq("16.20,1,tom123@gmail.com", "19.98,1,bailey@qq.com", "22.60,1,tina@gmail.com")
    assertThat(TestValuesTableFactory.getResultsAsStrings("user_sink").sorted)
      .isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testFilter(): Unit = {
    val sinkDDL =
      s"""
         |CREATE TABLE user_sink (
         |  user_id STRING PRIMARY KEY NOT ENFORCED,
         |  user_name STRING,
         |  email STRING,
         |  balance DECIMAL(18,2),
         |  balance2 DECIMAL(18,2)
         |) WITH (
         | 'connector' = 'values',
         | 'sink-insert-only' = 'false'
         |)
         |""".stripMargin

    // the sink is an upsert sink, but the update_before must be sent,
    // otherwise "user1=8.10" can't be removed
    val dml =
      s"""
         |INSERT INTO user_sink
         |SELECT * FROM users WHERE balance > 9
         |ON CONFLICT DO DEDUPLICATE
         |""".stripMargin
    tEnv.executeSql(sinkDDL)
    tEnv.executeSql(dml).await()

    val expected =
      Seq("user3,Bailey,bailey@qq.com,9.99,19.98", "user4,Tina,tina@gmail.com,11.30,22.60")
    assertThat(TestValuesTableFactory.getResultsAsStrings("user_sink").sorted)
      .isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testRegularJoin(): Unit = {
    val sql =
      s"""
         |SELECT o.currency, o.amount, r.rate, o.amount * r.rate
         |FROM orders AS o JOIN rates AS r
         |ON o.currency = r.currency
         |""".stripMargin

    val sink = new TestingRetractSink
    val result = tEnv.sqlQuery(sql).toRetractStream[Row]
    result.addSink(sink).setParallelism(result.getParallelism)
    env.execute()

    val expected =
      Seq("Euro,2,119,238", "Euro,3,119,357", "US Dollar,1,102,102", "US Dollar,5,102,510")
    assertThat(sink.getRetractResults.sorted).isEqualTo(expected.sorted)
  }

  // ------------------------------------------------------------------------------------------

  private def registerChangelogSource(): Unit = {
    val userDataId: String = TestValuesTableFactory.registerData(TestData.userChangelog)
    tEnv.executeSql(s"""
                       |CREATE TABLE users (
                       |  user_id STRING,
                       |  user_name STRING,
                       |  email STRING,
                       |  balance DECIMAL(18,2),
                       |  balance2 AS balance * 2
                       |) WITH (
                       | 'connector' = 'values',
                       | 'data-id' = '$userDataId',
                       | 'changelog-mode' = 'I,UA,UB,D',
                       | 'disable-lookup' = 'true'
                       |)
                       |""".stripMargin)
    val ratesDataId = TestValuesTableFactory.registerData(TestData.ratesHistoryData)
    tEnv.executeSql(s"""
                       |CREATE TABLE rates (
                       |  currency STRING,
                       |  rate BIGINT
                       |) WITH (
                       |  'connector' = 'values',
                       |  'data-id' = '$ratesDataId',
                       |  'changelog-mode' = 'I,UB,UA,D',
                       |  'disable-lookup' = 'true'
                       |)
      """.stripMargin)
  }

  private def registerChangelogSourceWithEventsDuplicate(): Unit = {
    tEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_SOURCE_CDC_EVENTS_DUPLICATE, Boolean.box(true))
    val userChangelog: Seq[Row] = Seq(
      changelogRow("+I", "user1", "Tom", "tom@gmail.com", new JBigDecimal("10.02")),
      changelogRow("+I", "user2", "Jack", "jack@hotmail.com", new JBigDecimal("71.2")),
      changelogRow("+I", "user2", "Jack", "jack@hotmail.com", new JBigDecimal("71.2")), // dup
      changelogRow("-U", "user1", "Tom", "tom@gmail.com", new JBigDecimal("10.02")),
      changelogRow("+U", "user1", "Tom", "tom123@gmail.com", new JBigDecimal("8.1")),
      changelogRow("-U", "user1", "Tom", "tom@gmail.com", new JBigDecimal("10.02")), // dup
      changelogRow("+U", "user1", "Tom", "tom123@gmail.com", new JBigDecimal("8.1")), // dup
      changelogRow("+I", "user3", "Bailey", "bailey@gmail.com", new JBigDecimal("9.99")),
      changelogRow("-D", "user2", "Jack", "jack@hotmail.com", new JBigDecimal("71.2")),
      changelogRow("-D", "user2", "Jack", "jack@hotmail.com", new JBigDecimal("71.2")), // dup
      changelogRow("+I", "user4", "Tina", "tina@gmail.com", new JBigDecimal("11.3")),
      changelogRow("-U", "user3", "Bailey", "bailey@gmail.com", new JBigDecimal("9.99")),
      changelogRow("+U", "user3", "Bailey", "bailey@qq.com", new JBigDecimal("9.99"))
    )
    val userDataId = TestValuesTableFactory.registerData(userChangelog)
    tEnv.executeSql(s"""
                       |CREATE TABLE users (
                       |  user_id STRING,
                       |  user_name STRING,
                       |  email STRING,
                       |  balance DECIMAL(18,2),
                       |  balance2 AS balance * 2,
                       |  PRIMARY KEY (user_name, user_id) NOT ENFORCED
                       |) WITH (
                       | 'connector' = 'values',
                       | 'data-id' = '$userDataId',
                       | 'changelog-mode' = 'UA,D',
                       | 'disable-lookup' = 'true'
                       |)
                       |""".stripMargin)
    val ratesChangelog: Seq[Row] = Seq(
      changelogRow("+I", "US Dollar", JLong.valueOf(102L)),
      changelogRow("+I", "Euro", JLong.valueOf(114L)),
      changelogRow("+I", "Euro", JLong.valueOf(114L)), // dup
      changelogRow("+I", "Yen", JLong.valueOf(1L)),
      changelogRow("-U", "Euro", JLong.valueOf(114L)),
      changelogRow("+U", "Euro", JLong.valueOf(116L)),
      changelogRow("-U", "Euro", JLong.valueOf(116L)),
      changelogRow("+U", "Euro", JLong.valueOf(119L)),
      changelogRow("-U", "Euro", JLong.valueOf(116L)), // dup
      changelogRow("+U", "Euro", JLong.valueOf(119L)), // dup
      changelogRow("-D", "Yen", JLong.valueOf(1L)),
      changelogRow("-D", "Yen", JLong.valueOf(1L)) // dup
    )
    val ratesDataId = TestValuesTableFactory.registerData(ratesChangelog)
    tEnv.executeSql(s"""
                       |CREATE TABLE rates (
                       |  currency STRING,
                       |  rate BIGINT,
                       |  PRIMARY KEY (currency) NOT ENFORCED
                       |) WITH (
                       |  'connector' = 'values',
                       |  'data-id' = '$ratesDataId',
                       |  'changelog-mode' = 'UA,D',
                       |  'disable-lookup' = 'true'
                       |)
      """.stripMargin)
  }

  private def registerUpsertSource(): Unit = {
    val userDataId = TestValuesTableFactory.registerData(TestData.userUpsertlog)
    tEnv.executeSql(s"""
                       |CREATE TABLE users (
                       |  user_id STRING,
                       |  user_name STRING,
                       |  email STRING,
                       |  balance DECIMAL(18,2),
                       |  balance2 AS balance * 2,
                       |  PRIMARY KEY (user_name, user_id) NOT ENFORCED
                       |) WITH (
                       | 'connector' = 'values',
                       | 'data-id' = '$userDataId',
                       | 'changelog-mode' = 'UA,D',
                       | 'disable-lookup' = 'true'
                       |)
                       |""".stripMargin)
    val ratesDataId = TestValuesTableFactory.registerData(TestData.ratesUpsertData)
    tEnv.executeSql(s"""
                       |CREATE TABLE rates (
                       |  currency STRING,
                       |  rate BIGINT,
                       |  PRIMARY KEY (currency) NOT ENFORCED
                       |) WITH (
                       |  'connector' = 'values',
                       |  'data-id' = '$ratesDataId',
                       |  'changelog-mode' = 'UA,D',
                       |  'disable-lookup' = 'true'
                       |)
      """.stripMargin)
  }

  private def registerNoUpdateSource(): Unit = {
    // only contains INSERT and DELETE
    val userChangelog = convertToNoUpdateData(TestData.userChangelog)
    val userDataId = TestValuesTableFactory.registerData(userChangelog)
    tEnv.executeSql(s"""
                       |CREATE TABLE users (
                       |  user_id STRING,
                       |  user_name STRING,
                       |  email STRING,
                       |  balance DECIMAL(18,2),
                       |  balance2 AS balance * 2
                       |) WITH (
                       | 'connector' = 'values',
                       | 'data-id' = '$userDataId',
                       | 'changelog-mode' = 'I,D',
                       | 'disable-lookup' = 'true'
                       |)
                       |""".stripMargin)
    val ratesChangelog = convertToNoUpdateData(TestData.ratesHistoryData)
    val ratesDataId = TestValuesTableFactory.registerData(ratesChangelog)
    tEnv.executeSql(s"""
                       |CREATE TABLE rates (
                       |  currency STRING,
                       |  rate BIGINT
                       |) WITH (
                       |  'connector' = 'values',
                       |  'data-id' = '$ratesDataId',
                       |  'changelog-mode' = 'I,D',
                       |  'disable-lookup' = 'true'
                       |)
      """.stripMargin)
  }

  private def convertToNoUpdateData(data: Seq[Row]): Seq[Row] = {
    data.map {
      row =>
        row.getKind match {
          case RowKind.INSERT | RowKind.DELETE => row
          case RowKind.UPDATE_BEFORE =>
            val ret = Row.copy(row)
            ret.setKind(RowKind.DELETE)
            ret
          case RowKind.UPDATE_AFTER =>
            val ret = Row.copy(row)
            ret.setKind(RowKind.INSERT)
            ret
        }
    }
  }

  private def registerValuesSource(
      name: String,
      columns: String,
      rows: Seq[Row],
      changelogMode: String,
      extraOptions: Map[String, String] = Map.empty): Unit = {
    val dataId = TestValuesTableFactory.registerData(rows)
    val withOptions = Map(
      "connector" -> "values",
      "data-id" -> dataId,
      "changelog-mode" -> changelogMode) ++ extraOptions
    tEnv.executeSql(s"CREATE TABLE $name ($columns) WITH (${renderOptions(withOptions)})")
  }

  private def registerValuesSink(name: String, columns: String): Unit = {
    val withOptions = Map(
      "connector" -> "values",
      "sink-insert-only" -> "false",
      "sink-changelog-mode-enforced" -> "I,UA,D")
    tEnv.executeSql(s"CREATE TABLE $name ($columns) WITH (${renderOptions(withOptions)})")
  }

  private def renderOptions(options: Map[String, String]): String =
    options.map { case (k, v) => s"'$k' = '$v'" }.mkString(", ")

  private def assertSinkEmpty(sinkName: String): Unit =
    assertSinkContains(sinkName, List.empty)

  private def assertSinkContains(sinkName: String, expected: List[String]): Unit =
    assertThat(TestValuesTableFactory.getResultsAsStrings(sinkName).sorted)
      .isEqualTo(expected.sorted)

  @TestTemplate
  def testFilterPushedDownOnNonUpsertKey(): Unit = {
    registerValuesSource(
      "t",
      "a int primary key not enforced, b varchar, c int",
      Seq(
        changelogRow("+I", Int.box(1), "tom", Int.box(1)),
        changelogRow("-U", Int.box(1), "tom", Int.box(1)),
        changelogRow("+U", Int.box(1), "tom", Int.box(2))),
      "I,UA,UB,D",
      Map("filterable-fields" -> "c")
    )
    registerValuesSink("s", "a int primary key not enforced, b varchar, c int")

    tEnv.executeSql("insert into s select * from t where c < 2").await()
    assertSinkEmpty("s")
  }

  @TestTemplate
  def testFilterAndProjectPushedDownOnNonUpsertKey(): Unit = {
    // Sink reorders to (b, c, a) so the PK lands at projected index 2, exercising the remap.
    registerValuesSource(
      "t",
      "a int primary key not enforced, b varchar, c int",
      Seq(
        changelogRow("+I", Int.box(1), "tom", Int.box(1)),
        changelogRow("-U", Int.box(1), "tom", Int.box(1)),
        changelogRow("+U", Int.box(1), "tom", Int.box(2))),
      "I,UA,UB,D",
      Map("filterable-fields" -> "c")
    )
    registerValuesSink("s", "b varchar, c int, a int primary key not enforced")

    tEnv.executeSql("insert into s select b, c, a from t where c < 2").await()
    assertSinkEmpty("s")
  }

  @TestTemplate
  def testFilterOnUpsertKeyAndProjectPushedDown(): Unit = {
    // Filter on PK + reordering projection — remap resolves filter ref to PK in projected
    // coords so UB drop is allowed and the upsert sink converges on the latest +U row.
    registerValuesSource(
      "t",
      "a int primary key not enforced, b varchar, c int",
      Seq(
        changelogRow("+I", Int.box(1), "tom", Int.box(10)),
        changelogRow("-U", Int.box(1), "tom", Int.box(10)),
        changelogRow("+U", Int.box(1), "tom", Int.box(20))),
      "I,UA,UB,D",
      Map("filterable-fields" -> "a")
    )
    registerValuesSink("s", "b varchar, c int, a int primary key not enforced")

    tEnv.executeSql("insert into s select b, c, a from t where a > 0").await()
    assertSinkContains("s", List("tom,20,1"))
  }

  @TestTemplate
  def testJoinWithNonEquiConditionOnLeftNonUpsertKey(): Unit = {
    registerValuesSource(
      "t1",
      "pk int primary key not enforced, val int",
      Seq(
        changelogRow("+I", Int.box(1), Int.box(10)),
        changelogRow("-U", Int.box(1), Int.box(10)),
        changelogRow("+U", Int.box(1), Int.box(20))),
      "I,UA,UB,D"
    )
    registerValuesSource(
      "t2",
      "pk int primary key not enforced, val int",
      Seq(changelogRow("+I", Int.box(1), Int.box(100))),
      "I")
    registerValuesSink("s", "pk1 int, val1 int, pk2 int, val2 int")

    tEnv
      .executeSql("insert into s select * from t1 join t2 on t1.pk = t2.pk and t1.val < 15")
      .await()
    assertSinkEmpty("s")
  }

  @TestTemplate
  def testJoinWithNonEquiConditionOnRightNonUpsertKey(): Unit = {
    registerValuesSource(
      "t1",
      "pk int primary key not enforced, val int",
      Seq(changelogRow("+I", Int.box(1), Int.box(10))),
      "I")
    registerValuesSource(
      "t2",
      "pk int primary key not enforced, val int",
      Seq(
        changelogRow("+I", Int.box(1), Int.box(100)),
        changelogRow("-U", Int.box(1), Int.box(100)),
        changelogRow("+U", Int.box(1), Int.box(200))),
      "I,UA,UB"
    )
    registerValuesSink("s", "pk1 int, val1 int, pk2 int, val2 int")

    tEnv
      .executeSql("insert into s select * from t1 join t2 on t1.pk = t2.pk and t2.val < 150")
      .await()
    assertSinkEmpty("s")
  }

  @TestTemplate
  def testMultiJoinWithPostJoinFilterOnNonUpsertKey(): Unit = {
    tEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, java.lang.Boolean.TRUE)

    registerValuesSource(
      "t1",
      "pk int primary key not enforced, val int",
      Seq(
        changelogRow("+I", Int.box(1), Int.box(10)),
        changelogRow("-U", Int.box(1), Int.box(10)),
        changelogRow("+U", Int.box(1), Int.box(20))),
      "I,UA,UB,D"
    )
    registerValuesSource(
      "t2",
      "pk int primary key not enforced, val int",
      Seq(changelogRow("+I", Int.box(1), Int.box(100))),
      "I")
    registerValuesSource(
      "t3",
      "pk int primary key not enforced, val int",
      Seq(changelogRow("+I", Int.box(1), Int.box(1000))),
      "I")
    registerValuesSink("s", "pk1 int, val1 int, pk2 int, val2 int, pk3 int, val3 int")

    tEnv
      .executeSql(
        "insert into s select * from t1 join t2 on t1.pk = t2.pk " +
          "join t3 on t1.pk = t3.pk where t1.val < 15")
      .await()
    assertSinkEmpty("s")
  }

  @TestTemplate
  def testMultiJoinWithNonEquiJoinConditionOnNonUpsertKey(): Unit = {
    // The cross-input non-equi predicate t1.val < t2.val lives in the multi-join's per-input join
    // condition (not the post-join filter) and is evaluated at runtime, so UB must be kept.
    tEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, java.lang.Boolean.TRUE)

    registerValuesSource(
      "t1",
      "pk int primary key not enforced, val int",
      Seq(
        changelogRow("+I", Int.box(1), Int.box(10)),
        changelogRow("-U", Int.box(1), Int.box(10)),
        changelogRow("+U", Int.box(1), Int.box(20))),
      "I,UA,UB,D"
    )
    registerValuesSource(
      "t2",
      "pk int primary key not enforced, val int",
      Seq(changelogRow("+I", Int.box(1), Int.box(15))),
      "I")
    registerValuesSource(
      "t3",
      "pk int primary key not enforced, val int",
      Seq(changelogRow("+I", Int.box(1), Int.box(100))),
      "I")
    registerValuesSink("s", "pk1 int, val1 int, pk2 int, val2 int, pk3 int, val3 int")

    tEnv
      .executeSql(
        "insert into s select * from t1 join t2 on t1.pk = t2.pk and t1.val < t2.val " +
          "join t3 on t1.pk = t3.pk")
      .await()
    assertSinkEmpty("s")
  }

  @TestTemplate
  def testMultiJoinWithNonCommonEquiKeyOnNonUpsertColumn(): Unit = {
    // t1.val = t3.val3 is a cross-input equi key that only touches the t1<->t3 step, so it is not
    // part of the common join key {pk}; it must stay in the residual because t1.val is non-upsert
    // and the predicate is evaluated at runtime in t3's per-input join condition.
    tEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, java.lang.Boolean.TRUE)

    registerValuesSource(
      "t1",
      "pk int primary key not enforced, val int",
      Seq(
        changelogRow("+I", Int.box(1), Int.box(100)),
        changelogRow("-U", Int.box(1), Int.box(100)),
        changelogRow("+U", Int.box(1), Int.box(200))),
      "I,UA,UB,D"
    )
    registerValuesSource(
      "t2",
      "pk int primary key not enforced, val int",
      Seq(changelogRow("+I", Int.box(1), Int.box(15))),
      "I")
    registerValuesSource(
      "t3",
      "pk int primary key not enforced, val3 int",
      Seq(changelogRow("+I", Int.box(1), Int.box(100))),
      "I")
    registerValuesSink("s", "pk1 int, val1 int, pk2 int, val2 int, pk3 int, val3 int")

    tEnv
      .executeSql(
        "insert into s select * from t1 join t2 on t1.pk = t2.pk " +
          "join t3 on t1.pk = t3.pk and t1.val = t3.val3")
      .await()
    assertSinkEmpty("s")
  }

  @TestTemplate
  def testTemporalJoinWithRemainingConditionOnLeftNonUpsertKey(): Unit = {
    // Rowtime variant; proctime temporal joins don't support CDC streaming inputs.
    val ts = (0 to 2).map(i => java.time.LocalDateTime.parse(s"2025-01-01T10:00:0$i"))
    registerValuesSource(
      "orders_t",
      """order_id bigint,
        |  currency varchar,
        |  amount bigint,
        |  order_time TIMESTAMP(3),
        |  WATERMARK FOR order_time AS order_time,
        |  PRIMARY KEY (order_id) NOT ENFORCED""".stripMargin,
      Seq(
        changelogRow("+I", Long.box(1L), "USD", Long.box(150L), ts(0)),
        changelogRow("-U", Long.box(1L), "USD", Long.box(150L), ts(1)),
        changelogRow("+U", Long.box(1L), "USD", Long.box(50L), ts(2))
      ),
      "I,UA,UB,D"
    )
    registerValuesSource(
      "rates_t",
      """currency varchar,
        |  rate bigint,
        |  rate_time TIMESTAMP(3),
        |  WATERMARK FOR rate_time AS rate_time,
        |  PRIMARY KEY (currency) NOT ENFORCED""".stripMargin,
      Seq(changelogRow("+I", "USD", Long.box(1L), ts(0))),
      "I",
      Map("disable-lookup" -> "true")
    )
    registerValuesSink("s", "order_id bigint, amount bigint")

    tEnv
      .executeSql("""
                    |INSERT INTO s
                    |SELECT order_id, amount FROM orders_t
                    |JOIN rates_t FOR SYSTEM_TIME AS OF orders_t.order_time
                    |  ON orders_t.currency = rates_t.currency
                    |  AND orders_t.amount > 100
                    |""".stripMargin)
      .await()

    assertSinkEmpty("s")
  }

  @TestTemplate
  def testLookupJoinWithFilterOnLookupTable(): Unit = {
    // Streaming UK {user_id} doesn't cover join key {region_id}, so an update to region_id
    // changes the looked-up row and can flip the `regions.active = true` filter.
    registerValuesSource(
      "users_t",
      "user_id int primary key not enforced, region_id int, proctime as PROCTIME()",
      Seq(
        changelogRow("+I", Int.box(1), Int.box(10)),
        changelogRow("-U", Int.box(1), Int.box(10)),
        changelogRow("+U", Int.box(1), Int.box(20))),
      "I,UA,UB,D"
    )
    registerValuesSource(
      "regions_t",
      "region_id int primary key not enforced, country varchar, active boolean",
      Seq(
        changelogRow("+I", Int.box(10), "US", Boolean.box(true)),
        changelogRow("+I", Int.box(20), "ES", Boolean.box(false))),
      "I"
    )
    registerValuesSink("s", "user_id int, country varchar")

    tEnv
      .executeSql("""
                    |INSERT INTO s
                    |SELECT user_id, country FROM users_t
                    |JOIN regions_t FOR SYSTEM_TIME AS OF users_t.proctime
                    |  ON users_t.region_id = regions_t.region_id
                    |WHERE regions_t.active = true
                    |""".stripMargin)
      .await()

    assertSinkEmpty("s")
  }

  @TestTemplate
  def testCorrelateWithConditionOnTableFunctionOutput(): Unit = {
    // TVF reads non-upsert column `tags`, so its output and the filter outcome both flip on
    // streaming-side updates.
    tEnv.createTemporarySystemFunction("STRING_SPLIT", new StringSplit())
    registerValuesSource(
      "users_t",
      "user_id int primary key not enforced, tags varchar",
      Seq(
        changelogRow("+I", Int.box(1), "a,x"),
        changelogRow("-U", Int.box(1), "a,x"),
        changelogRow("+U", Int.box(1), "b,y")),
      "I,UA,UB,D"
    )
    registerValuesSink("s", "user_id int, tag varchar")

    tEnv
      .executeSql("""
                    |INSERT INTO s
                    |SELECT user_id, T.tag
                    |FROM users_t, LATERAL TABLE(STRING_SPLIT(tags, ',')) AS T(tag)
                    |WHERE T.tag = 'a'
                    |""".stripMargin)
      .await()

    assertSinkEmpty("s")
  }

}

object ChangelogSourceITCase {

  case class SourceMode(mode: String) {
    override def toString: String = mode
  }

  val CHANGELOG_SOURCE: SourceMode = SourceMode("CHANGELOG")
  val CHANGELOG_SOURCE_WITH_EVENTS_DUPLICATE: SourceMode = SourceMode("CHANGELOG_WITH_EVENTS_DUP")
  val UPSERT_SOURCE: SourceMode = SourceMode("UPSERT")
  val NO_UPDATE_SOURCE: SourceMode = SourceMode("NO_UPDATE")

  @Parameters(name = "Source={0}, MiniBatch={1}, StateBackend={2}")
  def parameters(): util.Collection[Array[java.lang.Object]] = {
    Seq[Array[AnyRef]](
      Array(CHANGELOG_SOURCE, MiniBatchOff, HEAP_BACKEND),
      Array(CHANGELOG_SOURCE, MiniBatchOff, ROCKSDB_BACKEND),
      Array(CHANGELOG_SOURCE_WITH_EVENTS_DUPLICATE, MiniBatchOn, ROCKSDB_BACKEND),
      Array(UPSERT_SOURCE, MiniBatchOff, HEAP_BACKEND),
      Array(UPSERT_SOURCE, MiniBatchOff, ROCKSDB_BACKEND),
      // upsert source supports minibatch, we enable minibatch only for RocksDB to save time
      Array(UPSERT_SOURCE, MiniBatchOn, ROCKSDB_BACKEND),
      // we only test not_update for RocksDB to save time
      Array(NO_UPDATE_SOURCE, MiniBatchOff, ROCKSDB_BACKEND)
    )
  }
}
