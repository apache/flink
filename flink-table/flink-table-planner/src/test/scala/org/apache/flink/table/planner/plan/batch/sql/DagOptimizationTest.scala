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
package org.apache.flink.table.planner.plan.batch.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.planner.plan.optimize.RelNodeBlockPlanBuilder
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.NonDeterministicUdf
import org.apache.flink.table.planner.utils.{TableFunc1, TableTestBase}
import org.apache.flink.table.types.logical._

import org.junit.Test

import java.sql.Timestamp

class DagOptimizationTest extends TableTestBase {
  private val util = batchTestUtil()
  util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
  util.addTableSource[(Int, Long, String)]("MyTable1", 'd, 'e, 'f)

  val STRING = new VarCharType(VarCharType.MAX_LENGTH)
  val LONG = new BigIntType()
  val INT = new IntType()
  val DOUBLE = new DoubleType()
  val TIMESTAMP = new TimestampType(3)

  @Test
  def testSingleSink1(): Unit = {
    val table = util.tableEnv.sqlQuery("SELECT c, COUNT(a) AS cnt FROM MyTable GROUP BY c")
    val appendSink = util.createCollectTableSink(Array("c", "cnt"), Array(STRING, LONG))
    util.verifyExecPlanInsert(table, appendSink, "appendSink")
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

    val appendSink = util.createCollectTableSink(Array("a1", "b", "c1"), Array(INT, LONG, STRING))
    util.verifyExecPlanInsert(table6, appendSink, "appendSink")
  }

  @Test
  def testSingleSink3(): Unit = {
    util.addDataStream[(Int, Long, String, Double, Boolean)]("MyTable2", 'a, 'b, 'c, 'd, 'e)
    val table1 = util.tableEnv.sqlQuery("SELECT a AS a1, b as b1 FROM MyTable WHERE a <= 10")
    util.tableEnv.registerTable("table1", table1)
    val table2 = util.tableEnv.sqlQuery("SELECT a, b1 FROM table1, MyTable2 WHERE a = a1")
    util.tableEnv.registerTable("table2", table2)
    val table3 = util.tableEnv.sqlQuery("SELECT * FROM table1 UNION ALL SELECT * FROM table2")

    val appendSink = util.createCollectTableSink(Array("a1", "b1"), Array(INT, LONG))
    util.verifyExecPlanInsert(table3, appendSink, "appendSink")
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

    val sink = util.createCollectTableSink(Array("a", "b", "c"), Array(INT, LONG, STRING))
    util.verifyExecPlanInsert(table7, sink, "sink")
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
    val sink = util.createCollectTableSink(
      Array("a", "b", "c", "d", "e", "f", "i", "j", "k", "l", "m", "s"),
      Array(INT, LONG, STRING, INT, LONG, STRING, INT, LONG, INT, STRING, LONG, STRING))
    util.verifyExecPlanInsert(table, sink, "sink")
  }

  @Test
  def testSingleSinkSplitOnUnion(): Unit = {
    val sqlQuery = "SELECT SUM(a) AS total_sum FROM " +
      "(SELECT a, c FROM MyTable UNION ALL SELECT d, f FROM MyTable1)"
    val table = util.tableEnv.sqlQuery(sqlQuery)
    val sink = util.createCollectTableSink(Array("total_sum"), Array(INT))
    util.verifyExecPlanInsert(table, sink, "sink")
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

    val sink1 = util.createCollectTableSink(Array("total_sum"), Array(INT))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("sink1", sink1)
    stmtSet.addInsert("sink1", table2)

    val sink2 = util.createCollectTableSink(Array("total_min"), Array(INT))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("sink2", sink2)
    stmtSet.addInsert("sink2", table3)

    util.verifyExecPlan(stmtSet)
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

    val sink1 = util.createCollectTableSink(Array("a", "b1"), Array(INT, LONG))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("sink1", sink1)
    stmtSet.addInsert("sink1", table3)

    val sink2 = util.createCollectTableSink(Array("a", "b1"), Array(INT, LONG))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("sink2", sink2)
    stmtSet.addInsert("sink2", table3)

    util.verifyExecPlan(stmtSet)
  }

  @Test
  def testMultiSinks3(): Unit = {
    val stmtSet = util.tableEnv.createStatementSet()
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      RelNodeBlockPlanBuilder.TABLE_OPTIMIZER_UNIONALL_AS_BREAKPOINT_ENABLED, false)
    util.addTableSource[(Int, Long, String, Double, Boolean)]("MyTable2", 'a, 'b, 'c, 'd, 'e)

    val table1 = util.tableEnv.sqlQuery("SELECT a AS a1, b AS b1 FROM MyTable WHERE a <= 10")
    util.tableEnv.registerTable("table1", table1)
    val table2 = util.tableEnv.sqlQuery("SELECT a, b1 FROM table1, MyTable2 WHERE a = a1")
    util.tableEnv.registerTable("table2", table2)
    val table3 = util.tableEnv.sqlQuery("SELECT * FROM table1 UNION ALL SELECT * FROM table2")

    val sink1 = util.createCollectTableSink(Array("a", "b1"), Array(INT, LONG))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("sink1", sink1)
    stmtSet.addInsert("sink1", table2)

    val sink2 = util.createCollectTableSink(Array("a", "b1"), Array(INT, LONG))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("sink2", sink2)
    stmtSet.addInsert("sink2", table3)

    util.verifyExecPlan(stmtSet)
  }

  @Test
  def testMultiSinks4(): Unit = {
    val stmtSet = util.tableEnv.createStatementSet()
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      RelNodeBlockPlanBuilder.TABLE_OPTIMIZER_REUSE_OPTIMIZE_BLOCK_WITH_DIGEST_ENABLED, true)

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

    val sink1 = util.createCollectTableSink(Array("a1", "b", "c2"), Array(INT, LONG, STRING))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("sink1", sink1)
    stmtSet.addInsert("sink1", table5)

    val sink2 = util.createCollectTableSink(Array("a1", "b", "c1"), Array(INT, LONG, STRING))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("sink2", sink2)
    stmtSet.addInsert("sink2", table6)

    util.verifyExecPlan(stmtSet)
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

    val sink1 = util.createCollectTableSink(Array("total_sum"), Array(INT))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("sink1", sink1)
    stmtSet.addInsert("sink1", table2)

    val sink2 = util.createCollectTableSink(Array("total_min"), Array(INT))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("sink2", sink2)
    stmtSet.addInsert("sink2", table3)

    util.verifyExecPlan(stmtSet)
  }

  @Test
  def testMultiLevelViews(): Unit = {
    val stmtSet = util.tableEnv.createStatementSet()
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      RelNodeBlockPlanBuilder.TABLE_OPTIMIZER_UNIONALL_AS_BREAKPOINT_ENABLED, false)

    val table1 = util.tableEnv.sqlQuery("SELECT a, b, c FROM MyTable WHERE c LIKE '%hello%'")
    util.tableEnv.registerTable("TempTable1", table1)
    val sink1 = util.createCollectTableSink(Array("a", "b", "c"), Array(INT, LONG, STRING))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("sink1", sink1)
    stmtSet.addInsert("sink1", table1)

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
    val sink2 = util.createCollectTableSink(Array("b", "cnt"), Array(LONG, LONG))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("sink2", sink2)
    stmtSet.addInsert("sink2", table4)

    val table5 = util.tableEnv.sqlQuery("SELECT b, cnt FROM TempTable3 WHERE b >=4 AND b < 6")
    val sink3 = util.createCollectTableSink(Array("b", "cnt"), Array(LONG, LONG))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("sink3", sink3)
    stmtSet.addInsert("sink3", table5)

    util.verifyExecPlan(stmtSet)
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
    val sink1 = util.createCollectTableSink(Array("a", "total_c"), Array(INT, LONG))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("sink1", sink1)
    stmtSet.addInsert("sink1", table5)

    val sqlQuery6 = "SELECT * FROM table4 WHERE a < 50"
    val table6 = util.tableEnv.sqlQuery(sqlQuery6)
    val sink2 = util.createCollectTableSink(Array("a", "total_c"), Array(INT, LONG))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("sink2", sink2)
    stmtSet.addInsert("sink2", table6)

    util.verifyExecPlan(stmtSet)
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
    val sink1 = util.createCollectTableSink( Array("total_sum"), Array(INT))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("sink1", sink1)
    stmtSet.addInsert("sink1", table1)

    val table3 = util.tableEnv.sqlQuery("SELECT MIN(a) AS total_min FROM TempTable")
    val sink2 = util.createCollectTableSink(Array("total_min"), Array(INT))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("sink2", sink2)
    stmtSet.addInsert("sink2", table3)

    util.verifyExecPlan(stmtSet)
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
    val sink1 = util.createCollectTableSink(Array("total_sum"), Array(INT))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("sink1", sink1)
    stmtSet.addInsert("sink1", table1)

    val table2 = util.tableEnv.sqlQuery("SELECT MIN(a) AS total_min FROM TempTable")
    val sink2 = util.createCollectTableSink(Array("total_min"), Array(INT))
     util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("sink2", sink2)
    stmtSet.addInsert("sink2", table2)

    val sqlQuery2 = "SELECT a FROM (SELECT a, c FROM MyTable UNION ALL SELECT d, f FROM MyTable1)"
    val table3 = util.tableEnv.sqlQuery(sqlQuery2)
    val sink3 = util.createCollectTableSink(Array("a"), Array(INT))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("sink3", sink3)
    stmtSet.addInsert("sink3", table3)

    util.verifyExecPlan(stmtSet)
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

    val sink1 = util.createCollectTableSink(Array("a", "c"), Array(INT, STRING))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("sink1", sink1)
    stmtSet.addInsert("sink1", table)

    val sqlQuery2 = "SELECT a, c FROM TempTable UNION ALL SELECT a, c FROM MyTable2"
    val table1 = util.tableEnv.sqlQuery(sqlQuery2)
    util.tableEnv.registerTable("TempTable1", table1)

    val table2 = util.tableEnv.sqlQuery("SELECT SUM(a) AS total_sum FROM TempTable1")
    val sink2 = util.createCollectTableSink(Array("total_sum"), Array(INT))
     util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("sink2", sink2)
    stmtSet.addInsert("sink2", table2)

    val table3 = util.tableEnv.sqlQuery("SELECT MIN(a) AS total_min FROM TempTable1")
    val sink3 = util.createCollectTableSink(Array("total_min"), Array(INT))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("sink3", sink3)
    stmtSet.addInsert("sink3", table3)

    util.verifyExecPlan(stmtSet)
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
    val sink1 = util.createCollectTableSink(Array("total_sum"), Array(INT))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("sink1", sink1)
    stmtSet.addInsert("sink1", table1)

    val table2 = util.tableEnv.sqlQuery("SELECT MIN(a) AS total_min FROM TempTable")
    val sink2 = util.createCollectTableSink(Array("total_min"), Array(INT))
     util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("sink2", sink2)
    stmtSet.addInsert("sink2", table2)

    util.verifyExecPlan(stmtSet)
  }

  @Test
  def testMultiSinksWithWindow(): Unit = {
    val stmtSet = util.tableEnv.createStatementSet()
    util.addTableSource[(Int, Double, Int, Timestamp)]("MyTable2", 'a, 'b, 'c, 'ts)
    val sqlQuery1 =
      """
        |SELECT
        |    a,
        |    SUM (CAST (c AS DOUBLE)) AS sum_c,
        |    TUMBLE_END(ts, INTERVAL '15' SECOND) AS `time`,
        |    TUMBLE_START(ts, INTERVAL '15' SECOND) AS window_start,
        |    TUMBLE_END (ts, INTERVAL '15' SECOND) AS window_end
        |FROM
        |    MyTable2
        |GROUP BY
        |    TUMBLE (ts, INTERVAL '15' SECOND), a
      """.stripMargin

    val sqlQuery2 =
      """
        |SELECT
        |    a,
        |    SUM (CAST (c AS DOUBLE)) AS sum_c,
        |    TUMBLE_END(ts, INTERVAL '15' SECOND) AS `time`
        |FROM
        |    MyTable2
        |GROUP BY
        |    TUMBLE (ts, INTERVAL '15' SECOND), a
      """.stripMargin

    val table1 = util.tableEnv.sqlQuery(sqlQuery1)
    val sink1 = util.createCollectTableSink(
      Array("a", "sum_c", "time", "window_start", "window_end"),
      Array(INT, DOUBLE, TIMESTAMP, TIMESTAMP, TIMESTAMP))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("sink1", sink1)
    stmtSet.addInsert("sink1", table1)

    val table2 = util.tableEnv.sqlQuery(sqlQuery2)
    val sink2 = util.createCollectTableSink(Array("a", "sum_c", "time"),
      Array(INT, DOUBLE, TIMESTAMP))
     util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("sink2", sink2)
    stmtSet.addInsert("sink2", table2)

    util.verifyExecPlan(stmtSet)
  }

}
