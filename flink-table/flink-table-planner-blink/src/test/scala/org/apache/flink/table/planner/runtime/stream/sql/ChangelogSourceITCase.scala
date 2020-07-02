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
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.{StreamingWithStateTestBase, TestData, TestingRetractSink}
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.{Row, RowKind}
import org.junit.Assert.assertEquals
import org.junit.{Before, Test}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConversions._
import scala.collection.Seq

@RunWith(classOf[Parameterized])
class ChangelogSourceITCase(state: StateBackendMode) extends StreamingWithStateTestBase(state) {

  val dataId: String = TestValuesTableFactory.registerData(TestData.userChangelog)

  @Before
  override def before(): Unit = {
    super.before()
    val ddl =
      s"""
         |CREATE TABLE user_logs (
         |  user_id STRING,
         |  user_name STRING,
         |  email STRING,
         |  balance DECIMAL(18,2),
         |  balance2 AS balance * 2
         |) WITH (
         | 'connector' = 'values',
         | 'data-id' = '$dataId',
         | 'changelog-mode' = 'I,UA,UB,D'
         |)
         |""".stripMargin
    tEnv.executeSql(ddl)
  }

  @Test
  def testChangelogSourceAndToRetractStream(): Unit = {
    val result = tEnv.sqlQuery("SELECT * FROM user_logs").toRetractStream[Row]
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
  def testChangelogSourceAndUpsertSink(): Unit = {
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
         |SELECT * FROM user_logs
         |""".stripMargin
    tEnv.executeSql(sinkDDL)
    execInsertSqlAndWaitResult(dml)

    val expected = Seq(
      "user1,Tom,tom123@gmail.com,8.10,16.20",
      "user3,Bailey,bailey@qq.com,9.99,19.98",
      "user4,Tina,tina@gmail.com,11.30,22.60")
    assertEquals(expected.sorted, TestValuesTableFactory.getResults("user_sink").sorted)
  }

  @Test
  def testAggregateOnChangelogSource(): Unit = {
    val query =
      s"""
         |SELECT count(*), sum(balance), max(email)
         |FROM user_logs
         |""".stripMargin

    val result = tEnv.sqlQuery(query).toRetractStream[Row]
    val sink = new TestingRetractSink()
    result.addSink(sink).setParallelism(result.parallelism)
    env.execute()

    val expected = Seq("3,29.39,tom123@gmail.com")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testAggregateOnChangelogSourceAndUpsertSink(): Unit = {
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
         |FROM user_logs
         |GROUP BY 'ALL'
         |""".stripMargin
    tEnv.executeSql(sinkDDL)
    execInsertSqlAndWaitResult(dml)

    val expected = Seq("ALL,3,29.39,tom123@gmail.com")
    assertEquals(expected.sorted, TestValuesTableFactory.getResults("user_sink").sorted)
  }

  @Test
  def testAggregateOnInsertDeleteChangelogSource(): Unit = {
    // only contains INSERT and DELETE
    val userChangelog = TestData.userChangelog.map { row =>
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
    val dataId = TestValuesTableFactory.registerData(userChangelog)
    val ddl =
      s"""
         |CREATE TABLE user_logs2 (
         |  user_id STRING,
         |  user_name STRING,
         |  email STRING,
         |  balance DECIMAL(18,2)
         |) WITH (
         | 'connector' = 'values',
         | 'data-id' = '$dataId',
         | 'changelog-mode' = 'I,D'
         |)
         |""".stripMargin
    tEnv.executeSql(ddl)

    val query =
      s"""
         |SELECT count(*), sum(balance), max(email)
         |FROM user_logs2
         |""".stripMargin

    val result = tEnv.sqlQuery(query).toRetractStream[Row]
    val sink = new TestingRetractSink()
    result.addSink(sink).setParallelism(result.parallelism)
    env.execute()

    val expected = Seq("3,29.39,tom123@gmail.com")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }
}
