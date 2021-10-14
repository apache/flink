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
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.planner.JBigDecimal
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow
import org.apache.flink.table.planner.runtime.stream.sql.ChangelogSourceITCase._
import org.apache.flink.table.planner.runtime.utils.StreamingWithMiniBatchTestBase.{MiniBatchMode, MiniBatchOff, MiniBatchOn}
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.{HEAP_BACKEND, ROCKSDB_BACKEND, StateBackendMode}
import org.apache.flink.table.planner.runtime.utils.{StreamingWithMiniBatchTestBase, TestData, TestingRetractSink}
import org.apache.flink.table.utils.LegacyRowResource
import org.apache.flink.types.{Row, RowKind}

import org.junit.Assert.{assertEquals, assertFalse}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Rule, Test}

import java.lang.{Long => JLong}
import java.util

import scala.collection.JavaConversions._
import scala.collection.Seq

/**
 * Integration tests for operations on changelog source, including upsert source.
 */
@RunWith(classOf[Parameterized])
class ChangelogSourceITCase(
    sourceMode: SourceMode,
    miniBatch: MiniBatchMode,
    state: StateBackendMode)
  extends StreamingWithMiniBatchTestBase(miniBatch, state) {

  @Rule
  def usesLegacyRows: LegacyRowResource = LegacyRowResource.INSTANCE

  @Before
  override def before(): Unit = {
    super.before()
    val orderDataId = TestValuesTableFactory.registerData(TestData.ordersData)
    tEnv.executeSql(
      s"""
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

  @Test
  def testToRetractStream(): Unit = {
    val result = tEnv.sqlQuery(s"SELECT * FROM users").toRetractStream[Row]
    val sink = new TestingRetractSink()
    result.addSink(sink).setParallelism(result.parallelism)
    env.execute()

    val expected = Seq(
      "user1,Tom,tom123@gmail.com,8.10,16.20",
      "user3,Bailey,bailey@qq.com,9.99,19.98",
      "user4,Tina,tina@gmail.com,11.30,22.60")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
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
         |""".stripMargin
    tEnv.executeSql(sinkDDL)
    tEnv.executeSql(dml).await()

    val expected = Seq(
      "user1,Tom,tom123@gmail.com,8.10,16.20",
      "user3,Bailey,bailey@qq.com,9.99,19.98",
      "user4,Tina,tina@gmail.com,11.30,22.60")
    assertEquals(expected.sorted, TestValuesTableFactory.getResults("user_sink").sorted)

    // verify the update_before messages haven been filtered when scanning changelog source
    sourceMode match {
      case CHANGELOG_SOURCE | CHANGELOG_SOURCE_WITH_EVENTS_DUPLICATE =>
        val rawResult = TestValuesTableFactory.getRawResults("user_sink")
        val hasUB = rawResult.exists(r => r.startsWith("-U"))
        assertFalse(
          s"Sink result shouldn't contain UPDATE_BEFORE, but is:\n ${rawResult.mkString("\n")}",
          hasUB)
      case _ => // do nothing
    }
  }

  @Test
  def testAggregate(): Unit = {
    val query =
      s"""
         |SELECT count(*), sum(balance), max(email)
         |FROM users
         |""".stripMargin

    val result = tEnv.sqlQuery(query).toRetractStream[Row]
    val sink = new TestingRetractSink()
    result.addSink(sink).setParallelism(result.parallelism)
    env.execute()

    val expected = Seq("3,29.39,tom123@gmail.com")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
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
         |""".stripMargin
    tEnv.executeSql(sinkDDL)
    tEnv.executeSql(dml).await()

    val expected = Seq("ALL,3,29.39,tom123@gmail.com")
    assertEquals(expected.sorted, TestValuesTableFactory.getResults("user_sink").sorted)
  }

  @Test
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
    val dml =
      s"""
         |INSERT INTO user_sink
         |SELECT balance2, count(*), max(email)
         |FROM users
         |GROUP BY balance2
         |""".stripMargin
    tEnv.executeSql(sinkDDL)
    tEnv.executeSql(dml).await()

    val expected = Seq(
      "16.20,1,tom123@gmail.com",
      "19.98,1,bailey@qq.com",
      "22.60,1,tina@gmail.com")
    assertEquals(expected.sorted, TestValuesTableFactory.getResults("user_sink").sorted)
  }

  @Test
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
         |""".stripMargin
    tEnv.executeSql(sinkDDL)
    tEnv.executeSql(dml).await()

    val expected = Seq(
      "user3,Bailey,bailey@qq.com,9.99,19.98",
      "user4,Tina,tina@gmail.com,11.30,22.60")
    assertEquals(expected.sorted, TestValuesTableFactory.getResults("user_sink").sorted)
  }

  @Test
  def testRegularJoin(): Unit = {
    val sql =
      s"""
        |SELECT o.currency, o.amount, r.rate, o.amount * r.rate
        |FROM orders AS o JOIN rates AS r
        |ON o.currency = r.currency
        |""".stripMargin

    val sink = new TestingRetractSink
    val result = tEnv.sqlQuery(sql).toRetractStream[Row]
    result.addSink(sink).setParallelism(result.parallelism)
    env.execute()

    val expected = Seq(
      "Euro,2,119,238", "Euro,3,119,357",
      "US Dollar,1,102,102", "US Dollar,5,102,510")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  // ------------------------------------------------------------------------------------------

  private def registerChangelogSource(): Unit = {
    val userDataId: String = TestValuesTableFactory.registerData(TestData.userChangelog)
    tEnv.executeSql(
      s"""
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
    tEnv.executeSql(
      s"""
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
    tEnv.getConfig.getConfiguration.setBoolean(
      ExecutionConfigOptions.TABLE_EXEC_SOURCE_CDC_EVENTS_DUPLICATE, true)
    val userChangelog: Seq[Row] = Seq(
      changelogRow("+I", "user1", "Tom", "tom@gmail.com", new JBigDecimal("10.02")),
      changelogRow("+I", "user2", "Jack", "jack@hotmail.com", new JBigDecimal("71.2")),
      changelogRow("+I", "user2", "Jack", "jack@hotmail.com", new JBigDecimal("71.2")), // dup
      changelogRow("-U", "user1", "Tom", "tom@gmail.com", new JBigDecimal("10.02")),
      changelogRow("+U", "user1", "Tom", "tom123@gmail.com", new JBigDecimal("8.1")),
      changelogRow("-U", "user1", "Tom", "tom@gmail.com", new JBigDecimal("10.02")),  // dup
      changelogRow("+U", "user1", "Tom", "tom123@gmail.com", new JBigDecimal("8.1")), // dup
      changelogRow("+I", "user3", "Bailey", "bailey@gmail.com", new JBigDecimal("9.99")),
      changelogRow("-D", "user2", "Jack", "jack@hotmail.com", new JBigDecimal("71.2")),
      changelogRow("-D", "user2", "Jack", "jack@hotmail.com", new JBigDecimal("71.2")), // dup
      changelogRow("+I", "user4", "Tina", "tina@gmail.com", new JBigDecimal("11.3")),
      changelogRow("-U", "user3", "Bailey", "bailey@gmail.com", new JBigDecimal("9.99")),
      changelogRow("+U", "user3", "Bailey", "bailey@qq.com", new JBigDecimal("9.99")))
    val userDataId = TestValuesTableFactory.registerData(userChangelog)
    tEnv.executeSql(
      s"""
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
      changelogRow("-U", "Euro", JLong.valueOf(116L)),  // dup
      changelogRow("+U", "Euro", JLong.valueOf(119L)),  // dup
      changelogRow("-D", "Yen", JLong.valueOf(1L)),
      changelogRow("-D", "Yen", JLong.valueOf(1L)) // dup
    )
    val ratesDataId = TestValuesTableFactory.registerData(ratesChangelog)
    tEnv.executeSql(
      s"""
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
    tEnv.executeSql(
      s"""
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
    tEnv.executeSql(
      s"""
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
    tEnv.executeSql(
      s"""
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
    tEnv.executeSql(
      s"""
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
    data.map { row =>
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

}

object ChangelogSourceITCase {

  case class SourceMode(mode: String) {
    override def toString: String = mode
  }

  val CHANGELOG_SOURCE: SourceMode = SourceMode("CHANGELOG")
  val CHANGELOG_SOURCE_WITH_EVENTS_DUPLICATE: SourceMode = SourceMode("CHANGELOG_WITH_EVENTS_DUP")
  val UPSERT_SOURCE: SourceMode = SourceMode("UPSERT")
  val NO_UPDATE_SOURCE: SourceMode = SourceMode("NO_UPDATE")

  @Parameterized.Parameters(name = "Source={0}, MiniBatch={1}, StateBackend={2}")
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
