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
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{ExplainDetail, TableException}
import org.apache.flink.table.planner.utils.TableTestBase
import org.apache.flink.table.types.logical.{BigIntType, IntType, VarCharType}

import org.junit.Test

class LegacySinkTest extends TableTestBase {

  private val util = streamTestUtil()
  util.addDataStream[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

  val STRING = new VarCharType(VarCharType.MAX_LENGTH)
  val LONG = new BigIntType()
  val INT = new IntType()

  @Test
  def testExceptionForAppendSink(): Unit = {
    val table = util.tableEnv.sqlQuery("SELECT COUNT(*) AS cnt FROM MyTable GROUP BY a")
    val appendSink = util.createAppendTableSink(Array("a"), Array(LONG))

    thrown.expect(classOf[TableException])
    thrown.expectMessage("AppendStreamTableSink doesn't support consuming update " +
      "changes which is produced by node GroupAggregate(groupBy=[a], select=[a, COUNT(*) AS cnt])")
    util.verifyPlanInsert(table, appendSink, "appendSink")
  }

  @Test
  def testExceptionForOverAggregate(): Unit = {
    val stmtSet = util.tableEnv.createStatementSet()
    val table = util.tableEnv.sqlQuery("SELECT COUNT(*) AS cnt FROM MyTable GROUP BY a")
    util.tableEnv.createTemporaryView("TempTable", table)

    val retractSink = util.createRetractTableSink(Array("cnt"), Array(LONG))
    util.tableEnv.registerTableSink("retractSink1", retractSink)
    stmtSet.addInsert("retractSink1", table)

    val table2 = util.tableEnv.sqlQuery(
      "SELECT cnt, SUM(cnt) OVER (ORDER BY PROCTIME()) FROM TempTable")
    val retractSink2 = util.createRetractTableSink(Array("cnt", "total"), Array(LONG, LONG))
    util.tableEnv.registerTableSink("retractSink2", retractSink2)
    stmtSet.addInsert("retractSink2", table2)

    thrown.expect(classOf[TableException])
    thrown.expectMessage("OverAggregate doesn't support consuming update changes " +
      "which is produced by node GroupAggregate(groupBy=[a], select=[a, COUNT(*) AS cnt])")
    util.verifyPlan(stmtSet)
  }

  @Test
  def testAppendSink(): Unit = {
    val table = util.tableEnv.sqlQuery("SELECT a + b, c FROM MyTable")
    val appendSink = util.createAppendTableSink(Array("d", "c"), Array(LONG, STRING))
    util.verifyPlanInsert(table, appendSink, "appendSink", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testRetractSink1(): Unit = {
    val table = util.tableEnv.sqlQuery("SELECT a, COUNT(*) AS cnt FROM MyTable GROUP BY a")
    val retractSink = util.createRetractTableSink(Array("a", "cnt"), Array(INT, LONG))
    util.verifyPlanInsert(table, retractSink, "retractSink", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testRetractSink2(): Unit = {
    val sqlQuery =
      """
        |SELECT cnt, COUNT(a) AS a FROM (
        |    SELECT a, COUNT(*) AS cnt FROM MyTable GROUP BY a) t
        |GROUP BY cnt
      """.stripMargin
    val table = util.tableEnv.sqlQuery(sqlQuery)
    val retractSink = util.createRetractTableSink(Array("cnt", "a"), Array(LONG, LONG))
    util.verifyPlanInsert(table, retractSink, "retractSink", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testUpsertSink1(): Unit = {
    val table = util.tableEnv.sqlQuery("SELECT a, COUNT(*) AS cnt FROM MyTable GROUP BY a")
    val upsertSink = util.createUpsertTableSink(Array(0), Array("a", "cnt"), Array(INT, LONG))
    util.verifyPlanInsert(table, upsertSink, "upsertSink", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testUpsertSink2(): Unit = {
    val sqlQuery =
      """
        |with t1 AS (SELECT a AS a1, b FROM MyTable WHERE a <= 10),
        |     t2 AS (SELECT * from MyTable WHERE a >= 0),
        |     t3 AS (SELECT a AS a2, c from t2 where b >= 5),
        |     t4 AS (SELECT a AS a3, c AS c1 FROM t2 WHERE b < 5),
        |     t5 AS (SELECT a1, b, c AS c2 FROM t1, t3 where a1 = a2)
        |SELECT a1, b, c1 FROM t4, t5 WHERE a1 = a3
      """.stripMargin
    val table = util.tableEnv.sqlQuery(sqlQuery)
    val upsertSink = util.createUpsertTableSink(Array(), Array("a1", "b", "c1"),
      Array(INT, LONG, STRING))
    util.verifyPlanInsert(table, upsertSink, "upsertSink", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testUpsertSinkWithFilter(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (SELECT a, COUNT(*) AS cnt FROM MyTable GROUP BY a)
        |WHERE cnt < 10
        |""".stripMargin
    val table = util.tableEnv.sqlQuery(sql)
    val upsertSink = util.createUpsertTableSink(Array(0), Array("a", "cnt"), Array(INT, LONG))
    // a filter after aggregation, the Aggregation and Calc should produce UPDATE_BEFORE
    util.verifyPlanInsert(table, upsertSink, "upsertSink", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testRetractAndUpsertSink(): Unit = {
    val stmtSet = util.tableEnv.createStatementSet()
    val table = util.tableEnv.sqlQuery("SELECT b, COUNT(a) AS cnt FROM MyTable GROUP BY b")
    util.tableEnv.registerTable("TempTable", table)

    val table1 = util.tableEnv.sqlQuery("SELECT b, cnt FROM TempTable WHERE b < 4")
    val retractSink = util.createRetractTableSink(Array("b", "cnt"), Array(LONG, LONG))
    util.tableEnv.registerTableSink("retractSink", retractSink)
    stmtSet.addInsert("retractSink", table1)

    val table2 = util.tableEnv.sqlQuery("SELECT b, cnt FROM TempTable WHERE b >= 4 AND b < 6")
    val upsertSink = util.createUpsertTableSink(Array(), Array("b", "cnt"), Array(LONG, LONG))
    util.tableEnv.registerTableSink("upsertSink", upsertSink)
    stmtSet.addInsert("upsertSink", table2)

    util.verifyPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testUpsertAndUpsertSink(): Unit = {
    val stmtSet = util.tableEnv.createStatementSet()
    val table = util.tableEnv.sqlQuery("SELECT b, COUNT(a) AS cnt FROM MyTable GROUP BY b")
    util.tableEnv.registerTable("TempTable", table)

    val table1 = util.tableEnv.sqlQuery(
      "SELECT cnt, COUNT(b) AS frequency FROM TempTable WHERE b < 4 GROUP BY cnt")
    val upsertSink1 = util.createUpsertTableSink(Array(0), Array("b", "cnt"), Array(LONG, LONG))
    util.tableEnv.registerTableSink("upsertSink1", upsertSink1)
    stmtSet.addInsert("upsertSink1", table1)

    val table2 = util.tableEnv.sqlQuery("SELECT b, cnt FROM TempTable WHERE b >= 4 AND b < 6")
    val upsertSink2 = util.createUpsertTableSink(Array(), Array("b", "cnt"), Array(LONG, LONG))
    util.tableEnv.registerTableSink("upsertSink2", upsertSink2)
    stmtSet.addInsert("upsertSink2", table2)

    util.verifyPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testAppendUpsertAndRetractSink(): Unit = {
    val stmtSet = util.tableEnv.createStatementSet()
    util.addDataStream[(Int, Long, String)]("MyTable2", 'd, 'e, 'f)
    util.addDataStream[(Int, Long, String)]("MyTable3", 'i, 'j, 'k)

    val table = util.tableEnv.sqlQuery(
      "SELECT a, b FROM MyTable UNION ALL SELECT d, e FROM MyTable2")
    util.tableEnv.registerTable("TempTable", table)

    val appendSink = util.createAppendTableSink(Array("a", "b"), Array(INT, LONG))
    util.tableEnv.registerTableSink("appendSink", appendSink)
    stmtSet.addInsert("appendSink", table)

    val table1 = util.tableEnv.sqlQuery(
      "SELECT a, b FROM TempTable UNION ALL SELECT i, j FROM MyTable3")
    util.tableEnv.registerTable("TempTable1", table1)

    val table2 = util.tableEnv.sqlQuery("SELECT SUM(a) AS total_sum FROM TempTable1")
    val retractSink = util.createRetractTableSink(Array("total_sum"), Array(INT))
    util.tableEnv.registerTableSink("retractSink", retractSink)
    stmtSet.addInsert("retractSink", table2)

    val table3 = util.tableEnv.sqlQuery("SELECT MIN(a) AS total_min FROM TempTable1")
    val upsertSink = util.createUpsertTableSink(Array(), Array("total_min"), Array(INT))
    util.tableEnv.registerTableSink("upsertSink", upsertSink)
    stmtSet.addInsert("upsertSink", table3)

    util.verifyPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

}
