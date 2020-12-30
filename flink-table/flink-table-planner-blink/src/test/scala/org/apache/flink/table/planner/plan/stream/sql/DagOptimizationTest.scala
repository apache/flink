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
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.planner.plan.optimize.RelNodeBlockPlanBuilder
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.NonDeterministicUdf
import org.apache.flink.table.planner.utils.{TableFunc1, TableTestBase}
import org.apache.flink.table.types.logical.{BigIntType, IntType, VarCharType}

import org.junit.Test

class DagOptimizationTest extends TableTestBase {
  private val util = streamTestUtil()
  util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
  util.addTableSource[(Int, Long, String)]("MyTable1", 'd, 'e, 'f)

  val STRING = new VarCharType(VarCharType.MAX_LENGTH)
  val LONG = new BigIntType()
  val INT = new IntType()

  @Test
  def testSingleSink1(): Unit = {
    val table = util.tableEnv.sqlQuery("SELECT c, COUNT(a) AS cnt FROM MyTable GROUP BY c")
    val retractSink = util.createRetractTableSink(Array("c", "cnt"), Array(STRING, LONG))
    util.verifyRelPlanInsert(table, retractSink, "retractSink", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testSingleSink2(): Unit = {
    val table1 = util.tableEnv.sqlQuery("SELECT a as a1, b FROM MyTable WHERE a <= 10")
    util.tableEnv.registerTable("table1", table1)
    val table2 = util.tableEnv.sqlQuery("SELECT a, b, c FROM MyTable WHERE a >= 0")
    util.tableEnv.registerTable("table2", table2)
    val table3 = util.tableEnv.sqlQuery("SELECT a AS a2, c FROM table2 WHERE b >= 5")
    util.tableEnv.registerTable("table3", table3)
    val table4 = util.tableEnv.sqlQuery("SELECT a AS a3, c as c1 FROM table2 WHERE b < 5")
    util.tableEnv.registerTable("table4", table4)
    val table5 = util.tableEnv.sqlQuery("SELECT a1, b, c as c2 FROM table1, table3 WHERE a1 = a2")
    util.tableEnv.registerTable("table5", table5)
    val table6 = util.tableEnv.sqlQuery("SELECT a1, b, c1 FROM table4, table5 WHERE a1 = a3")

    val appendSink = util.createAppendTableSink(Array("a1", "b", "c1"), Array(INT, LONG, STRING))
    util.verifyRelPlanInsert(table6, appendSink, "appendSink", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testSingleSink3(): Unit = {
    util.addDataStream[(Int, Long, String, Double, Boolean)]("MyTable2", 'a, 'b, 'c, 'd, 'e)
    val table1 = util.tableEnv.sqlQuery("SELECT a AS a1, b as b1 FROM MyTable WHERE a <= 10")
    util.tableEnv.registerTable("table1", table1)
    val table2 = util.tableEnv.sqlQuery("SELECT a, b1 FROM table1, MyTable2 WHERE a = a1")
    util.tableEnv.registerTable("table2", table2)
    val table3 = util.tableEnv.sqlQuery("SELECT * FROM table1 UNION ALL SELECT * FROM table2")

    val appendSink = util.createAppendTableSink(Array("a1", "b1"), Array(INT, LONG))
    util.verifyRelPlanInsert(table3, appendSink, "appendSink", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testSingleSink4(): Unit = {
    val table1 = util.tableEnv.sqlQuery("SELECT a as a1, b FROM MyTable WHERE a <= 10")
    util.tableEnv.registerTable("table1", table1)
    val table2 = util.tableEnv.sqlQuery("SELECT a, b, c FROM MyTable WHERE a >= 0")
    util.tableEnv.registerTable("table2", table2)
    val table3 = util.tableEnv.sqlQuery("SELECT a AS a2, c FROM table2 WHERE b >= 5")
    util.tableEnv.registerTable("table3", table3)
    val table4 = util.tableEnv.sqlQuery("SELECT a AS a3, c AS c1 FROM table2 WHERE b < 5")
    util.tableEnv.registerTable("table4", table4)
    val table5 = util.tableEnv.sqlQuery("SELECT a1, b, c AS c2 from table1, table3 WHERE a1 = a2")
    util.tableEnv.registerTable("table5", table5)
    val table6 = util.tableEnv.sqlQuery("SELECT a3, b as b1, c1 FROM table4, table5 WHERE a1 = a3")
    util.tableEnv.registerTable("table6", table6)
    val table7 = util.tableEnv.sqlQuery("SELECT a1, b1, c1 FROM table1, table6 WHERE a1 = a3")

    val appendSink = util.createAppendTableSink(Array("a", "b", "c"), Array(INT, LONG, STRING))
    util.verifyRelPlanInsert(table7, appendSink, "appendSink", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testSingleSinkWithUDTF(): Unit = {
    util.addTableSource[(Int, Long, Int, String, Long)]("MyTable2", 'i, 'j, 'k, 'l, 'm)
    util.addFunction("split", new TableFunc1)

    val sqlQuery =
      """
        |select * from
        |    (SELECT * FROM MyTable, MyTable1, MyTable2 WHERE b = e AND a = i) t,
        |    LATERAL TABLE(split(c)) as T(s)
      """.stripMargin

    val table = util.tableEnv.sqlQuery(sqlQuery)
    val appendSink = util.createAppendTableSink(
      Array("a", "b", "c", "d", "e", "f", "i", "j", "k", "l", "m", "s"),
      Array(INT, LONG, STRING, INT, LONG, STRING, INT, LONG, INT, STRING, LONG, STRING))
    util.verifyRelPlanInsert(table, appendSink, "appendSink", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testSingleSinkSplitOnUnion(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      RelNodeBlockPlanBuilder.TABLE_OPTIMIZER_UNIONALL_AS_BREAKPOINT_ENABLED, false)

    val sqlQuery = "SELECT SUM(a) AS total_sum FROM " +
      "(SELECT a, c FROM MyTable UNION ALL SELECT d, f FROM MyTable1)"
    val table = util.tableEnv.sqlQuery(sqlQuery)
    val retractSink = util.createRetractTableSink(Array("total_sum"), Array(INT))
    util.verifyRelPlanInsert(table, retractSink, "retractSink", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testMultiSinks1(): Unit = {
    val stmtSet = util.tableEnv.createStatementSet()
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      RelNodeBlockPlanBuilder.TABLE_OPTIMIZER_REUSE_OPTIMIZE_BLOCK_WITH_DIGEST_ENABLED, true)
    val table1 = util.tableEnv.sqlQuery("SELECT SUM(a) AS sum_a, c FROM MyTable GROUP BY c")
    util.tableEnv.registerTable("table1", table1)
    val table2 = util.tableEnv.sqlQuery("SELECT SUM(sum_a) AS total_sum FROM table1")
    val table3 = util.tableEnv.sqlQuery("SELECT MIN(sum_a) AS total_min FROM table1")

    val retractSink1 = util.createRetractTableSink(Array("total_sum"), Array(INT))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "retractSink1", retractSink1)
    stmtSet.addInsert("retractSink1", table2)

    val retractSink2 = util.createRetractTableSink(Array("total_min"), Array(INT))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "retractSink2", retractSink2)
    stmtSet.addInsert("retractSink2", table3)

    util.verifyRelPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testMultiSinks2(): Unit = {
    val stmtSet = util.tableEnv.createStatementSet()
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      RelNodeBlockPlanBuilder.TABLE_OPTIMIZER_UNIONALL_AS_BREAKPOINT_ENABLED, true)
    util.addTableSource[(Int, Long, String, Double, Boolean)]("MyTable2", 'a, 'b, 'c, 'd, 'e)

    val table1 = util.tableEnv.sqlQuery("SELECT a as a1, b as b1 FROM MyTable WHERE a <= 10")
    util.tableEnv.registerTable("table1", table1)
    val table2 = util.tableEnv.sqlQuery("SELECT a, b1 from table1, MyTable2 where a = a1")
    util.tableEnv.registerTable("table2", table2)
    val table3 = util.tableEnv.sqlQuery("SELECT * FROM table1 UNION ALL SELECT * FROM table2")

    val appendSink1 = util.createAppendTableSink(Array("a", "b1"), Array(INT, LONG))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "appendSink1", appendSink1)
    stmtSet.addInsert("appendSink1", table3)

    val appendSink2 = util.createAppendTableSink(Array("a", "b1"), Array(INT, LONG))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "appendSink2", appendSink2)
    stmtSet.addInsert("appendSink2", table3)

    util.verifyRelPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testMultiSinks3(): Unit = {
    val stmtSet = util.tableEnv.createStatementSet()
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      RelNodeBlockPlanBuilder.TABLE_OPTIMIZER_UNIONALL_AS_BREAKPOINT_ENABLED, true)
    util.addTableSource[(Int, Long, String, Double, Boolean)]("MyTable2", 'a, 'b, 'c, 'd, 'e)

    val table1 = util.tableEnv.sqlQuery("SELECT a AS a1, b AS b1 FROM MyTable WHERE a <= 10")
    util.tableEnv.registerTable("table1", table1)
    val table2 = util.tableEnv.sqlQuery("SELECT a, b1 FROM table1, MyTable2 WHERE a = a1")
    util.tableEnv.registerTable("table2", table2)
    val table3 = util.tableEnv.sqlQuery("SELECT * FROM table1 UNION ALL SELECT * FROM table2")

    val appendSink1 = util.createAppendTableSink(Array("a", "b1"), Array(INT, LONG))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "appendSink1", appendSink1)
    stmtSet.addInsert("appendSink1", table2)

    val appendSink2 = util.createAppendTableSink(Array("a", "b1"), Array(INT, LONG))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "appendSink2", appendSink2)
    stmtSet.addInsert("appendSink2", table3)

    util.verifyRelPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testMultiSinks4(): Unit = {
    val stmtSet = util.tableEnv.createStatementSet()
    val table1 = util.tableEnv.sqlQuery("SELECT a as a1, b FROM MyTable WHERE a <= 10")
    util.tableEnv.registerTable("table1", table1)
    val table2 = util.tableEnv.sqlQuery("SELECT a, b, c FROM MyTable WHERE a >= 0")
    util.tableEnv.registerTable("table2", table2)
    val table3 = util.tableEnv.sqlQuery("SELECT a as a2, c FROM table2 WHERE b >= 5")
    util.tableEnv.registerTable("table3", table3)
    val table4 = util.tableEnv.sqlQuery("SELECT a as a3, c as c1 FROM table2 WHERE b < 5")
    util.tableEnv.registerTable("table4", table4)
    val table5 = util.tableEnv.sqlQuery("SELECT a1, b, c as c2 FROM table1, table3 WHERE a1 = a2")
    util.tableEnv.registerTable("table5", table5)
    val table6 = util.tableEnv.sqlQuery("SELECT a1, b, c1 FROM table4, table5 WHERE a1 = a3")

    val appendSink1 = util.createAppendTableSink(Array("a1", "b", "c2"), Array(INT, LONG, STRING))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "appendSink1", appendSink1)
    stmtSet.addInsert("appendSink1", table5)

    val appendSink2 = util.createAppendTableSink(Array("a1", "b", "c1"), Array(INT, LONG, STRING))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "appendSink2", appendSink2)
    stmtSet.addInsert("appendSink2", table6)

    util.verifyRelPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testMultiSinks5(): Unit = {
    val stmtSet = util.tableEnv.createStatementSet()
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      RelNodeBlockPlanBuilder.TABLE_OPTIMIZER_REUSE_OPTIMIZE_BLOCK_WITH_DIGEST_ENABLED, true)
    // test with non-deterministic udf
    util.tableEnv.registerFunction("random_udf", new NonDeterministicUdf())
    val table1 = util.tableEnv.sqlQuery("SELECT random_udf(a) AS a, c FROM MyTable")
    util.tableEnv.registerTable("table1", table1)
    val table2 = util.tableEnv.sqlQuery("SELECT SUM(a) AS total_sum FROM table1")
    val table3 = util.tableEnv.sqlQuery("SELECT MIN(a) AS total_min FROM table1")

    val retractSink1 = util.createRetractTableSink(Array("total_sum"), Array(INT))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "retractSink1", retractSink1)
    stmtSet.addInsert("retractSink1", table2)

    val retractSink2 = util.createRetractTableSink(Array("total_min"), Array(INT))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "retractSink2", retractSink2)
    stmtSet.addInsert("retractSink2", table3)

    util.verifyRelPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testMultiSinksWithUDTF(): Unit = {
    val stmtSet = util.tableEnv.createStatementSet()
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      RelNodeBlockPlanBuilder.TABLE_OPTIMIZER_UNIONALL_AS_BREAKPOINT_ENABLED, false)
    util.addFunction("split", new TableFunc1)
    val sqlQuery1 =
      """
        |SELECT  a, b - MOD(b, 300) AS b, c FROM MyTable
        |WHERE b >= UNIX_TIMESTAMP('${startTime}')
      """.stripMargin
    val table1 = util.tableEnv.sqlQuery(sqlQuery1)
    util.tableEnv.registerTable("table1", table1)

    val sqlQuery2 =
      "SELECT a, b, c1 AS c FROM table1, LATERAL TABLE(split(c)) AS T(c1) WHERE c <> '' "
    val table2 = util.tableEnv.sqlQuery(sqlQuery2)
    util.tableEnv.registerTable("table2", table2)

    val sqlQuery3 = "SELECT a, b, COUNT(DISTINCT c) AS total_c FROM table2 GROUP BY a, b"
    val table3 = util.tableEnv.sqlQuery(sqlQuery3)
    util.tableEnv.registerTable("table3", table3)

    val sqlQuery4 = "SELECT a, total_c FROM table3 UNION ALL SELECT a, 0 AS total_c FROM table1"
    val table4 = util.tableEnv.sqlQuery(sqlQuery4)
    util.tableEnv.registerTable("table4", table4)

    val sqlQuery5 = "SELECT * FROM table4 WHERE a > 50"
    val table5 = util.tableEnv.sqlQuery(sqlQuery5)
    val retractSink1 = util.createRetractTableSink(Array("a", "total_c"), Array(INT, LONG))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "retractSink1", retractSink1)
    stmtSet.addInsert("retractSink1", table5)

    val sqlQuery6 = "SELECT * FROM table4 WHERE a < 50"
    val table6 = util.tableEnv.sqlQuery(sqlQuery6)
    val retractSink2 = util.createRetractTableSink(Array("a", "total_c"), Array(INT, LONG))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "retractSink2", retractSink2)
    stmtSet.addInsert("retractSink2", table6)

    util.verifyRelPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testMultiSinksSplitOnUnion1(): Unit = {
    val stmtSet = util.tableEnv.createStatementSet()
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      RelNodeBlockPlanBuilder.TABLE_OPTIMIZER_UNIONALL_AS_BREAKPOINT_ENABLED, false)

    val table = util.tableEnv.sqlQuery(
      "SELECT a, c FROM MyTable UNION ALL SELECT d, f FROM MyTable1")
    util.tableEnv.registerTable("TempTable", table)

    val table1 = util.tableEnv.sqlQuery("SELECT SUM(a) AS total_sum FROM TempTable")
    val upsertSink = util.createUpsertTableSink(Array(), Array("total_sum"), Array(INT))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal]
      .registerTableSinkInternal("upsertSink", upsertSink)
    stmtSet.addInsert("upsertSink", table1)

    val table3 = util.tableEnv.sqlQuery("SELECT MIN(a) AS total_min FROM TempTable")
    val retractSink = util.createRetractTableSink(Array("total_min"), Array(INT))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "retractSink", retractSink)
    stmtSet.addInsert("retractSink", table3)

    util.verifyRelPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testMultiSinksSplitOnUnion2(): Unit = {
    val stmtSet = util.tableEnv.createStatementSet()
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      RelNodeBlockPlanBuilder.TABLE_OPTIMIZER_REUSE_OPTIMIZE_BLOCK_WITH_DIGEST_ENABLED, true)
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      RelNodeBlockPlanBuilder.TABLE_OPTIMIZER_UNIONALL_AS_BREAKPOINT_ENABLED, false)
    util.addTableSource[(Int, Long, String)]("MyTable2", 'a, 'b, 'c)

    val sqlQuery1 =
      """
        |SELECT a, c FROM MyTable
        |UNION ALL
        |SELECT d, f FROM MyTable1
        |UNION ALL
        |SELECT a, c FROM MyTable2
      """.stripMargin
    val table = util.tableEnv.sqlQuery(sqlQuery1)
    util.tableEnv.registerTable("TempTable", table)

    val table1 = util.tableEnv.sqlQuery("SELECT SUM(a) AS total_sum FROM TempTable")
    val retractSink1 = util.createRetractTableSink(Array("total_sum"), Array(INT))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "retractSink1", retractSink1)
    stmtSet.addInsert("retractSink1", table1)

    val table2 = util.tableEnv.sqlQuery("SELECT MIN(a) AS total_min FROM TempTable")
    val retractSink2 = util.createRetractTableSink(Array("total_min"), Array(INT))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "retractSink2", retractSink2)
    stmtSet.addInsert("retractSink2", table2)

    val sqlQuery2 = "SELECT a FROM (SELECT a, c FROM MyTable UNION ALL SELECT d, f FROM MyTable1)"
    val table3 = util.tableEnv.sqlQuery(sqlQuery2)
    val appendSink3 = util.createAppendTableSink(Array("a"), Array(INT))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "appendSink3", appendSink3)
    stmtSet.addInsert("appendSink3", table3)

    util.verifyRelPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testMultiSinksSplitOnUnion3(): Unit = {
    val stmtSet = util.tableEnv.createStatementSet()
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      RelNodeBlockPlanBuilder.TABLE_OPTIMIZER_UNIONALL_AS_BREAKPOINT_ENABLED, false)
    util.addTableSource[(Int, Long, String)]("MyTable2", 'a, 'b, 'c)

    val sqlQuery1 = "SELECT a, c FROM MyTable UNION ALL SELECT d, f FROM MyTable1"
    val table = util.tableEnv.sqlQuery(sqlQuery1)
    util.tableEnv.registerTable("TempTable", table)

    val appendSink = util.createAppendTableSink(Array("a", "c"), Array(INT, STRING))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal]
      .registerTableSinkInternal("appendSink", appendSink)
    stmtSet.addInsert("appendSink", table)

    val sqlQuery2 = "SELECT a, c FROM TempTable UNION ALL SELECT a, c FROM MyTable2"
    val table1 = util.tableEnv.sqlQuery(sqlQuery2)
    util.tableEnv.registerTable("TempTable1", table1)

    val table2 = util.tableEnv.sqlQuery("SELECT SUM(a) AS total_sum FROM TempTable1")
    val retractSink = util.createRetractTableSink(Array("total_sum"), Array(INT))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "retractSink", retractSink)
    stmtSet.addInsert("retractSink", table2)

    val table3 = util.tableEnv.sqlQuery("SELECT MIN(a) AS total_min FROM TempTable1")
    val upsertSink = util.createUpsertTableSink(Array(), Array("total_min"), Array(INT))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal]
      .registerTableSinkInternal("upsertSink", upsertSink)
    stmtSet.addInsert("upsertSink", table3)

    util.verifyRelPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testMultiSinksSplitOnUnion4(): Unit = {
    val stmtSet = util.tableEnv.createStatementSet()
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      RelNodeBlockPlanBuilder.TABLE_OPTIMIZER_UNIONALL_AS_BREAKPOINT_ENABLED, false)
    util.addTableSource[(Int, Long, String)]("MyTable2", 'a, 'b, 'c)

    val sqlQuery =
      """
        |SELECT a, c FROM MyTable
        |UNION ALL
        |SELECT d, f FROM MyTable1
        |UNION ALL
        |SELECT a, c FROM MyTable2
      """.stripMargin
    val table = util.tableEnv.sqlQuery(sqlQuery)
    util.tableEnv.registerTable("TempTable", table)

    val table1 = util.tableEnv.sqlQuery("SELECT SUM(a) AS total_sum FROM TempTable")
    val upsertSink = util.createUpsertTableSink(Array(), Array("total_sum"), Array(INT))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "upsertSink", upsertSink)
    stmtSet.addInsert("upsertSink", table1)

    val table2 = util.tableEnv.sqlQuery("SELECT MIN(a) AS total_min FROM TempTable")
    val retractSink = util.createRetractTableSink(Array("total_min"), Array(INT))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "retractSink", retractSink)
    stmtSet.addInsert("retractSink", table2)

    util.verifyRelPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testUnionAndAggWithDifferentGroupings(): Unit = {
    val sqlQuery =
      """
        |SELECT b, c, SUM(a) AS a_sum FROM MyTable GROUP BY b, c
        |UNION ALL
        |SELECT 1 AS b, c, SUM(a) AS a_sum FROM MyTable GROUP BY c
      """.stripMargin
    val table = util.tableEnv.sqlQuery(sqlQuery)

    val upsertSink = util.createUpsertTableSink(Array(), Array("b", "c", "a_sum"),
      Array(LONG, STRING, INT))
    util.verifyRelPlanInsert(table, upsertSink, "upsertSink", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testUpdateAsRetractConsumedAtSinkBlock(): Unit = {
    val stmtSet = util.tableEnv.createStatementSet()
    val table = util.tableEnv.sqlQuery("SELECT a, b, c FROM MyTable")
    util.tableEnv.registerTable("TempTable", table)

    val sqlQuery =
      s"""
         |SELECT * FROM (
         |  SELECT a, b, c,
         |      ROW_NUMBER() OVER (PARTITION BY b ORDER BY c DESC) as rank_num
         |  FROM TempTable)
         |WHERE rank_num <= 10
      """.stripMargin
    val table1 = util.tableEnv.sqlQuery(sqlQuery)
    val retractSink = util.createRetractTableSink(
      Array("a", "b", "c", "rank_num"), Array(INT, LONG, STRING, LONG))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "retractSink", retractSink)
    stmtSet.addInsert("retractSink", table1)

    val upsertSink = util.createUpsertTableSink(Array(), Array("a", "b"), Array(INT, LONG))
    val table2 = util.tableEnv.sqlQuery("SELECT a, b FROM TempTable WHERE a < 6")
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "upsertSink", upsertSink)
    stmtSet.addInsert("upsertSink", table2)

    util.verifyRelPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testUpdateAsRetractConsumedAtSourceBlock(): Unit = {
    val stmtSet = util.tableEnv.createStatementSet()
    val sqlQuery =
      s"""
         |SELECT * FROM (
         |   SELECT a, b, c,
         |      ROW_NUMBER() OVER (PARTITION BY b ORDER BY c DESC) as rank_num
         |  FROM MyTable)
         |WHERE rank_num <= 10
      """.stripMargin
    val table = util.tableEnv.sqlQuery(sqlQuery)
    util.tableEnv.registerTable("TempTable", table)

    val table1 = util.tableEnv.sqlQuery("SELECT a FROM TempTable WHERE a > 6")
    val retractSink = util.createRetractTableSink(Array("a"), Array(INT))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "retractSink", retractSink)
    stmtSet.addInsert("retractSink", table1)

    val table2 = util.tableEnv.sqlQuery("SELECT a, b FROM TempTable WHERE a < 6")
    val upsertSink = util.createUpsertTableSink(Array(), Array("a", "b"), Array(INT, LONG))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "upsertSink", upsertSink)
    stmtSet.addInsert("upsertSink", table2)

    util.verifyRelPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testMultiLevelViews(): Unit = {
    val stmtSet = util.tableEnv.createStatementSet()
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      RelNodeBlockPlanBuilder.TABLE_OPTIMIZER_UNIONALL_AS_BREAKPOINT_ENABLED, false)

    val table1 = util.tableEnv.sqlQuery("SELECT a, b, c FROM MyTable WHERE c LIKE '%hello%'")
    util.tableEnv.registerTable("TempTable1", table1)
    val appendSink = util.createAppendTableSink(Array("a", "b", "c"), Array(INT, LONG, STRING))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "appendSink", appendSink)
    stmtSet.addInsert("appendSink", table1)

    val table2 = util.tableEnv.sqlQuery("SELECT a, b, c FROM MyTable WHERE c LIKE '%world%'")
    util.tableEnv.registerTable("TempTable2", table2)

    val sqlQuery =
      """
        |SELECT b, COUNT(a) AS cnt FROM (
        | (SELECT * FROM TempTable1)
        | UNION ALL
        | (SELECT * FROM TempTable2)
        |) t
        |GROUP BY b
      """.stripMargin
    val table3 = util.tableEnv.sqlQuery(sqlQuery)
    util.tableEnv.registerTable("TempTable3", table3)

    val table4 = util.tableEnv.sqlQuery("SELECT b, cnt FROM TempTable3 WHERE b < 4")
    val retractSink = util.createRetractTableSink(Array("b", "cnt"), Array(LONG, LONG))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "retractSink", retractSink)
    stmtSet.addInsert("retractSink", table4)

    val table5 = util.tableEnv.sqlQuery("SELECT b, cnt FROM TempTable3 WHERE b >=4 AND b < 6")
    val upsertSink = util.createUpsertTableSink(Array(), Array("b", "cnt"), Array(LONG, LONG))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "upsertSink", upsertSink)
    stmtSet.addInsert("upsertSink", table5)

    util.verifyRelPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testSharedUnionNode(): Unit = {
    val stmtSet = util.tableEnv.createStatementSet()
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      RelNodeBlockPlanBuilder.TABLE_OPTIMIZER_UNIONALL_AS_BREAKPOINT_ENABLED, false)

    val table1 = util.tableEnv.sqlQuery("SELECT a, b, c FROM MyTable WHERE c LIKE '%hello%'")
    util.tableEnv.registerTable("TempTable1", table1)
    val appendSink = util.createAppendTableSink(Array("a", "b", "c"), Array(INT, LONG, STRING))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "appendSink", appendSink)
    stmtSet.addInsert("appendSink", table1)

    val table2 = util.tableEnv.sqlQuery("SELECT a, b, c FROM MyTable WHERE c LIKE '%world%'")
    util.tableEnv.registerTable("TempTable2", table2)

    val sqlQuery1 =
      """
        |SELECT * FROM TempTable1
        |UNION ALL
        |SELECT * FROM TempTable2
      """.stripMargin
    val table3 = util.tableEnv.sqlQuery(sqlQuery1)
    util.tableEnv.registerTable("TempTable3", table3)

    val table4 = util.tableEnv.sqlQuery("SELECT * FROM TempTable3 WHERE b >= 5")
    val retractSink1 = util.createRetractTableSink(Array("a", "b", "c"), Array(INT, LONG, STRING))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "retractSink1", retractSink1)
    stmtSet.addInsert("retractSink1", table4)

    val table5 = util.tableEnv.sqlQuery("SELECT b, count(a) as cnt FROM TempTable3 GROUP BY b")
    util.tableEnv.registerTable("TempTable4", table5)

    val table6 = util.tableEnv.sqlQuery("SELECT b, cnt FROM TempTable4 WHERE b < 4")
    val retractSink2 = util.createRetractTableSink(Array("b", "cnt"), Array(LONG, LONG))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "retractSink2", retractSink2)
    stmtSet.addInsert("retractSink2", table6)

    util.tableEnv.sqlQuery("SELECT b, cnt FROM TempTable4 WHERE b >=4 AND b < 6")
    val upsertSink = util.createUpsertTableSink(Array(), Array("b", "cnt"), Array(LONG, LONG))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "upsertSink", upsertSink)
    stmtSet.addInsert("upsertSink", table6)

    util.verifyRelPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

}
