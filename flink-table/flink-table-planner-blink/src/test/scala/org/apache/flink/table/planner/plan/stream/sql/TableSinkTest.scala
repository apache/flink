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

package org.apache.flink.table.planner.plan.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.utils.TableTestBase
import org.apache.flink.table.types.logical.LogicalType

import org.junit.Test

class TableSinkTest extends TableTestBase {

  private val util = streamTestUtil()
  util.addDataStream[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

  val STRING: LogicalType = DataTypes.STRING().getLogicalType
  val LONG: LogicalType = DataTypes.BIGINT().getLogicalType
  val INT: LogicalType = DataTypes.INT().getLogicalType

  @Test
  def testInsertMismatchTypeForEmptyChar(): Unit = {
    util.addTable(
      s"""
         |CREATE TABLE my_sink (
         |  name STRING,
         |  email STRING,
         |  message_offset BIGINT
         |) WITH (
         |  'connector' = 'values'
         |)
         |""".stripMargin)
    thrown.expect(classOf[ValidationException])
    thrown.expectMessage(
      "Query schema: [a: INT, EXPR$1: CHAR(0) NOT NULL, EXPR$2: CHAR(0) NOT NULL]\n" +
        "Sink schema: [name: STRING, email: STRING, message_offset: BIGINT]")
    util.verifyPlanInsert("INSERT INTO my_sink SELECT a, '', '' FROM MyTable")
  }

  @Test
  def testExceptionForAppendSink(): Unit = {
    util.addTable(
      s"""
         |CREATE TABLE appendSink (
         |  `a` BIGINT
         |) WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'true'
         |)
         |""".stripMargin)
    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql(
      "INSERT INTO appendSink SELECT COUNT(*) AS cnt FROM MyTable GROUP BY a")

    thrown.expect(classOf[TableException])
    thrown.expectMessage("Table sink 'default_catalog.default_database.appendSink' doesn't " +
      "support consuming update changes which is produced by node " +
      "GroupAggregate(groupBy=[a], select=[a, COUNT(*) AS cnt])")
    util.verifyPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testExceptionForOverAggregate(): Unit = {
    util.addTable(
      s"""
         |CREATE TABLE retractSink1 (
         |  `cnt` BIGINT
         |) WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'false'
         |)
         |""".stripMargin)
    util.addTable(
      s"""
         |CREATE TABLE retractSink2 (
         |  `cnt` BIGINT,
         |  `total` BIGINT
         |) WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'false'
         |)
         |""".stripMargin)
    val table = util.tableEnv.sqlQuery("SELECT COUNT(*) AS cnt FROM MyTable GROUP BY a")
    util.tableEnv.createTemporaryView("TempTable", table)
    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql("INSERT INTO retractSink1 SELECT * FROM TempTable")

    stmtSet.addInsertSql(
      "INSERT INTO retractSink2 SELECT cnt, SUM(cnt) OVER (ORDER BY PROCTIME()) FROM TempTable")

    thrown.expect(classOf[TableException])
    thrown.expectMessage("OverAggregate doesn't support consuming update changes " +
      "which is produced by node GroupAggregate(groupBy=[a], select=[a, COUNT(*) AS cnt])")
    util.verifyPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testAppendSink(): Unit = {
    util.addTable(
      s"""
         |CREATE TABLE appendSink (
         |  `a` BIGINT,
         |  `b` STRING
         |) WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'true'
         |)
         |""".stripMargin)
    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql("INSERT INTO appendSink SELECT a + b, c FROM MyTable")
    util.verifyPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testRetractSink1(): Unit = {
    util.addTable(
      s"""
         |CREATE TABLE retractSink (
         |  `a` INT,
         |  `cnt` BIGINT
         |) WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'false'
         |)
         |""".stripMargin)
    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql(
      "INSERT INTO retractSink SELECT a, COUNT(*) AS cnt FROM MyTable GROUP BY a")
    util.verifyPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testRetractSink2(): Unit = {
    util.addTable(
      s"""
         |CREATE TABLE retractSink (
         |  `cnt` BIGINT,
         |  `a` BIGINT
         |) WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'false'
         |)
         |""".stripMargin)
    val dml =
      """
        |INSERT INTO retractSink
        |SELECT cnt, COUNT(a) AS a FROM (
        |    SELECT a, COUNT(*) AS cnt FROM MyTable GROUP BY a) t
        |GROUP BY cnt
      """.stripMargin
    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql(dml)
    util.verifyPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testUpsertSink(): Unit = {
    util.addTable(
      s"""
         |CREATE TABLE upsertSink (
         |  `a` INT,
         |  `cnt` BIGINT,
         |  PRIMARY KEY (a) NOT ENFORCED
         |) WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'false'
         |)
         |""".stripMargin)
    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql(
      "INSERT INTO upsertSink SELECT a, COUNT(*) AS cnt FROM MyTable GROUP BY a")
    util.verifyPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testUpsertSinkWithFilter(): Unit = {
    util.addTable(
      s"""
         |CREATE TABLE upsertSink (
         |  `a` INT,
         |  `cnt` BIGINT,
         |  PRIMARY KEY (a) NOT ENFORCED
         |) WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'false'
         |)
         |""".stripMargin)
    val sql =
      """
        |INSERT INTO upsertSink
        |SELECT *
        |FROM (SELECT a, COUNT(*) AS cnt FROM MyTable GROUP BY a)
        |WHERE cnt < 10
        |""".stripMargin
    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql(sql)
    // a filter after aggregation, the Aggregation and Calc should produce UPDATE_BEFORE
    util.verifyPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testRetractAndUpsertSink(): Unit = {
    util.addTable(
      s"""
         |CREATE TABLE retractSink (
         |  `b` BIGINT,
         |  `cnt` BIGINT
         |) WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'false'
         |)
         |""".stripMargin)
    util.addTable(
      s"""
         |CREATE TABLE upsertSink (
         |  `b` BIGINT,
         |  `cnt` BIGINT,
         |  PRIMARY KEY (b) NOT ENFORCED
         |) WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'false'
         |)
         |""".stripMargin)

    val table = util.tableEnv.sqlQuery("SELECT b, COUNT(a) AS cnt FROM MyTable GROUP BY b")
    util.tableEnv.createTemporaryView("TempTable", table)

    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql(
      "INSERT INTO retractSink SELECT b, cnt FROM TempTable WHERE b < 4")
    stmtSet.addInsertSql(
      "INSERT INTO upsertSink SELECT b, cnt FROM TempTable WHERE b >= 4 AND b < 6")
    stmtSet.addInsertSql(
      "INSERT INTO upsertSink " +
        "SELECT cnt, COUNT(b) AS frequency FROM TempTable WHERE b < 4 GROUP BY cnt")

    util.verifyPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testAppendUpsertAndRetractSink(): Unit = {
    util.addDataStream[(Int, Long, String)]("MyTable2", 'd, 'e, 'f)
    util.addDataStream[(Int, Long, String)]("MyTable3", 'i, 'j, 'k)
    util.addTable(
      s"""
         |CREATE TABLE appendSink (
         |  `a` INT,
         |  `b` BIGINT
         |) WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'true'
         |)
         |""".stripMargin)

    val table = util.tableEnv.sqlQuery(
      "SELECT a, b FROM MyTable UNION ALL SELECT d, e FROM MyTable2")
    util.tableEnv.createTemporaryView("TempTable", table)
    val stmtSet = util.tableEnv.createStatementSet()

    stmtSet.addInsertSql("INSERT INTO appendSink SELECT * FROM TempTable")

    util.addTable(
      s"""
         |CREATE TABLE retractSink (
         |  `total_sum` INT
         |) WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'false'
         |)
         |""".stripMargin)
    val table1 = util.tableEnv.sqlQuery(
      "SELECT a, b FROM TempTable UNION ALL SELECT i, j FROM MyTable3")
    util.tableEnv.createTemporaryView("TempTable1", table1)
    stmtSet.addInsertSql("INSERT INTO retractSink SELECT SUM(a) AS total_sum FROM TempTable1")

    util.addTable(
      s"""
         |CREATE TABLE upsertSink (
         |  `a` INT,
         |  `total_min` BIGINT,
         |  PRIMARY KEY (a) NOT ENFORCED
         |) WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'false'
         |)
         |""".stripMargin)
    stmtSet.addInsertSql(
      "INSERT INTO upsertSink SELECT a, MIN(b) AS total_min FROM TempTable1 GROUP BY a")

    util.verifyPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

}
