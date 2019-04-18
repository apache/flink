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

package org.apache.flink.table.plan.stream.sql.join

import org.apache.flink.api.scala._
import org.apache.flink.table.api.{TableException, TableImpl, ValidationException}
import org.apache.flink.table.plan.util.WindowJoinUtil
import org.apache.flink.table.util.{StreamTableTestUtil, TableTestBase}

import org.apache.calcite.rel.logical.LogicalJoin
import org.junit.Assert.assertEquals
import org.junit.Test

class WindowJoinTest extends TableTestBase {

  private val util: StreamTableTestUtil = streamTestUtil()
  util.addDataStream[(Int, String, Long)]("MyTable", 'a, 'b, 'c, 'proctime, 'rowtime)
  util.addDataStream[(Int, String, Long)]("MyTable2", 'a, 'b, 'c, 'proctime, 'rowtime)

  /** There should exist exactly two time conditions **/
  @Test(expected = classOf[TableException])
  def testWindowJoinSingleTimeCondition(): Unit = {
    val sql =
      """
        |SELECT t2.a FROM MyTable t1 JOIN MyTable2 t2 ON
        |  t1.a = t2.a AND t1.proctime > t2.proctime - INTERVAL '5' SECOND
      """.stripMargin
    util.verifyPlan(sql)
  }

  /** Both time attributes in a join condition must be of the same type **/
  @Test(expected = classOf[TableException])
  def testWindowJoinDiffTimeIndicator(): Unit = {
    val sql =
      """
        |SELECT t2.a FROM MyTable t1 JOIN MyTable2 t2 ON
        |  t1.a = t2.a AND
        |  t1.proctime > t2.proctime - INTERVAL '5' SECOND AND
        |  t1.proctime < t2.rowtime + INTERVAL '5' SECOND
      """.stripMargin
    util.verifyPlan(sql)
  }

  /** The time conditions should be an And condition **/
  @Test(expected = classOf[TableException])
  def testWindowJoinNotCnfCondition(): Unit = {
    val sql =
      """
        |SELECT t2.a FROM MyTable t1 JOIN MyTable2 t2 ON
        |  t1.a = t2.a AND
        |  (t1.proctime > t2.proctime - INTERVAL '5' SECOND OR
        |   t1.proctime < t2.rowtime + INTERVAL '5' SECOND)
      """.stripMargin
    util.verifyPlan(sql)
  }

  /** Validates that no rowtime attribute is in the output schema **/
  @Test(expected = classOf[TableException])
  def testNoRowtimeAttributeInResult(): Unit = {
    val sql =
      """
        |SELECT * FROM MyTable t1, MyTable2 t2 WHERE
        |  t1.a = t2.a AND
        |  t1.proctime BETWEEN t2.proctime - INTERVAL '5' SECOND AND t2.proctime
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testProcessingTimeInnerJoinWithOnClause(): Unit = {
    val sqlQuery =
      """
        |SELECT t1.a, t2.b FROM MyTable t1 JOIN MyTable2 t2 ON
        |    t1.a = t2.a AND
        |    t1.proctime BETWEEN t2.proctime - INTERVAL '1' HOUR AND t2.proctime + INTERVAL '1' HOUR
      """.stripMargin

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testProcessingTimeInnerJoinWithWhereClause(): Unit = {
    val sqlQuery =
      """
        |SELECT t1.a, t2.b FROM MyTable t1, MyTable2 t2 WHERE
        |    t1.a = t2.a AND
        |    t1.proctime BETWEEN t2.proctime - INTERVAL '1' HOUR AND t2.proctime + INTERVAL '1' HOUR
      """.stripMargin

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRowTimeInnerJoinWithOnClause(): Unit = {
    val sqlQuery =
      """
        |SELECT t1.a, t2.b FROM MyTable t1 JOIN MyTable2 t2 ON
        |  t1.a = t2.a AND
        |  t1.rowtime BETWEEN t2.rowtime - INTERVAL '10' SECOND AND t2.rowtime + INTERVAL '1' HOUR
      """.stripMargin

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRowTimeInnerJoinWithWhereClause(): Unit = {
    val sqlQuery =
      """
        |SELECT t1.a, t2.b FROM MyTable t1, MyTable2 t2 WHERE
        |  t1.a = t2.a AND
        |  t1.rowtime BETWEEN t2.rowtime - INTERVAL '10' MINUTE AND t2.rowtime + INTERVAL '1' HOUR
      """.stripMargin

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testJoinWithEquiProcTime(): Unit = {
    val sqlQuery =
      """
        |SELECT t1.a, t2.b FROM MyTable t1, MyTable2 t2 WHERE
        |  t1.a = t2.a AND t1.proctime = t2.proctime
      """.stripMargin

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testJoinWithEquiRowTime(): Unit = {
    val sqlQuery =
      """
        |SELECT t1.a, t2.b FROM MyTable t1, MyTable2 t2 WHERE
        |  t1.a = t2.a AND t1.rowtime = t2.rowtime
        """.stripMargin

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testJoinWithNullLiteral(): Unit = {
    val sqlQuery =
      """
        |WITH T1 AS (SELECT a, b, c, proctime, CAST(null AS BIGINT) AS nullField FROM MyTable),
        |     T2 AS (SELECT a, b, c, proctime, CAST(12 AS BIGINT) AS nullField FROM MyTable2)
        |
        |SELECT t2.a, t2.c, t1.c
        |FROM T1 AS t1
        |JOIN T2 AS t2 ON t1.a = t2.a AND t1.nullField = t2.nullField AND
        |  t1.proctime BETWEEN t2.proctime - INTERVAL '5' SECOND AND
        |  t2.proctime + INTERVAL '5' SECOND
      """.stripMargin

    util.verifyPlan(sqlQuery)
  }

  @Test(expected = classOf[ValidationException])
  // FIXME remove expected exception after TUMBLE added
  def testRowTimeInnerJoinAndWindowAggregationOnFirst(): Unit = {
    val sqlQuery =
      """
        |SELECT t1.b, SUM(t2.a) AS aSum, COUNT(t2.b) AS bCnt
        |FROM MyTable t1, MyTable2 t2
        |WHERE t1.a = t2.a AND
        |  t1.rowtime BETWEEN t2.rowtime - INTERVAL '10' MINUTE AND t2.rowtime + INTERVAL '1' HOUR
        |GROUP BY TUMBLE(t1.rowtime, INTERVAL '6' HOUR), t1.b
      """.stripMargin

    util.verifyPlan(sqlQuery)
  }

  @Test(expected = classOf[ValidationException])
  // FIXME remove expected exception after TUMBLE added
  def testRowTimeInnerJoinAndWindowAggregationOnSecond(): Unit = {
    val sqlQuery =
      """
        |SELECT t2.b, SUM(t1.a) AS aSum, COUNT(t1.b) AS bCnt
        |FROM MyTable t1, MyTable2 t2
        |WHERE t1.a = t2.a AND
        |  t1.rowtime BETWEEN t2.rowtime - INTERVAL '10' MINUTE AND t2.rowtime + INTERVAL '1' HOUR
        |GROUP BY TUMBLE(t2.rowtime, INTERVAL '6' HOUR), t2.b
      """.stripMargin

    util.verifyPlan(sqlQuery)
  }

  // Tests for left outer join
  @Test
  def testProcTimeLeftOuterJoin(): Unit = {
    val sqlQuery =
      """
        |SELECT t1.a, t2.b
        |FROM MyTable t1 LEFT OUTER JOIN MyTable2 t2 ON
        |  t1.a = t2.a AND
        |  t1.proctime BETWEEN t2.proctime - INTERVAL '1' HOUR AND t2.proctime + INTERVAL '1' HOUR
      """.stripMargin

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRowTimeLeftOuterJoin(): Unit = {
    val sqlQuery =
      """
        |SELECT t1.a, t2.b
        |FROM MyTable t1 LEFT OUTER JOIN MyTable2 t2 ON
        |  t1.a = t2.a AND
        |  t1.rowtime BETWEEN t2.rowtime - INTERVAL '10' SECOND AND t2.rowtime + INTERVAL '1' HOUR
      """.stripMargin

    util.verifyPlan(sqlQuery)
  }

  // Tests for right outer join
  @Test
  def testProcTimeRightOuterJoin(): Unit = {
    val sqlQuery =
      """
        |SELECT t1.a, t2.b
        |FROM MyTable t1 RIGHT OUTER JOIN MyTable2 t2 ON
        |  t1.a = t2.a AND
        |  t1.proctime BETWEEN t2.proctime - INTERVAL '1' HOUR AND t2.proctime + INTERVAL '1' HOUR
      """.stripMargin

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRowTimeRightOuterJoin(): Unit = {

    val sqlQuery =
      """
        |SELECT t1.a, t2.b
        |FROM MyTable t1 RIGHT OUTER JOIN MyTable2 t2 ON
        |  t1.a = t2.a AND
        |  t1.rowtime BETWEEN t2.rowtime - INTERVAL '10' SECOND AND t2.rowtime + INTERVAL '1' HOUR
      """.stripMargin

    util.verifyPlan(sqlQuery)
  }

  // Tests for full outer join
  @Test
  def testProcTimeFullOuterJoin(): Unit = {
    val sqlQuery =
      """
        |SELECT t1.a, t2.b
        |FROM MyTable t1 Full OUTER JOIN MyTable2 t2 ON
        |  t1.a = t2.a AND
        |  t1.proctime BETWEEN t2.proctime - INTERVAL '1' HOUR AND t2.proctime + INTERVAL '1' HOUR
      """.stripMargin

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRowTimeFullOuterJoin(): Unit = {
    val sqlQuery =
      """
        |SELECT t1.a, t2.b
        |FROM MyTable t1 FULL OUTER JOIN MyTable2 t2 ON
        |  t1.a = t2.a AND
        |  t1.rowtime BETWEEN t2.rowtime - INTERVAL '10' SECOND AND t2.rowtime + INTERVAL '1' HOUR
      """.stripMargin

    util.verifyPlan(sqlQuery)
  }

  // Test for outer join optimization
  @Test
  def testOuterJoinOpt(): Unit = {
    val sqlQuery =
      """
        |SELECT t1.a, t2.b
        |FROM MyTable t1 FULL OUTER JOIN MyTable2 t2 ON
        |  t1.a = t2.a AND
        |  t1.rowtime BETWEEN t2.rowtime - INTERVAL '10' SECOND AND t2.rowtime + INTERVAL '1' HOUR
        |  WHERE t1.b LIKE t2.b
      """.stripMargin

    util.verifyPlan(sqlQuery)
  }

  // Other tests
  @Test
  def testJoinTimeBoundary(): Unit = {
    verifyTimeBoundary(
      "t1.proctime BETWEEN t2.proctime - INTERVAL '1' HOUR AND t2.proctime + INTERVAL '1' HOUR",
      -3600000,
      3600000,
      "proctime")

    verifyTimeBoundary(
      "t1.proctime > t2.proctime - INTERVAL '1' SECOND AND " +
        "t1.proctime < t2.proctime + INTERVAL '1' SECOND",
      -999,
      999,
      "proctime")

    verifyTimeBoundary(
      "t1.rowtime >= t2.rowtime - INTERVAL '1' SECOND AND " +
        "t1.rowtime <= t2.rowtime + INTERVAL '1' SECOND",
      -1000,
      1000,
      "rowtime")

    verifyTimeBoundary(
      "t1.rowtime >= t2.rowtime AND " +
        "t1.rowtime <= t2.rowtime + INTERVAL '1' SECOND",
      0,
      1000,
      "rowtime")

    verifyTimeBoundary(
      "t1.rowtime >= t2.rowtime + INTERVAL '1' SECOND AND " +
        "t1.rowtime <= t2.rowtime + INTERVAL '10' SECOND",
      1000,
      10000,
      "rowtime")

    verifyTimeBoundary(
      "t2.rowtime - INTERVAL '1' SECOND <= t1.rowtime AND " +
        "t2.rowtime + INTERVAL '10' SECOND >= t1.rowtime",
      -1000,
      10000,
      "rowtime")

    verifyTimeBoundary(
      "t1.rowtime - INTERVAL '2' SECOND >= t2.rowtime + INTERVAL '1' SECOND " +
        "- INTERVAL '10' SECOND AND t1.rowtime <= t2.rowtime + INTERVAL '10' SECOND",
      -7000,
      10000,
      "rowtime")

    verifyTimeBoundary(
      "t1.rowtime >= t2.rowtime - INTERVAL '10' SECOND AND " +
        "t1.rowtime <= t2.rowtime - INTERVAL '5' SECOND",
      -10000,
      -5000,
      "rowtime")
  }

  @Test
  def testJoinRemainConditionConvert(): Unit = {
    util.addDataStream[(Int, Long, Int)]("MyTable3", 'a, 'rowtime, 'c, 'proctime)
    util.addDataStream[(Int, Long, Int)]("MyTable4", 'a, 'rowtime, 'c, 'proctime)
    val query =
      """
        |SELECT t1.a, t2.c FROM MyTable3 AS t1 JOIN MyTable4 AS t2 ON
        |    t1.a = t2.a AND
        |    t1.rowtime >= t2.rowtime - INTERVAL '10' SECOND AND
        |    t1.rowtime <= t2.rowtime - INTERVAL '5' SECOND AND
        |    t1.c > t2.c
      """.stripMargin
    verifyRemainConditionConvert(
      query,
      ">($2, $6)")

    val query1 =
      """
        |SELECT t1.a, t2.c FROM MyTable3 as t1 JOIN MyTable4 AS t2 ON
        |    t1.a = t2.a AND
        |    t1.rowtime >= t2.rowtime - INTERVAL '10' SECOND AND
        |    t1.rowtime <= t2.rowtime - INTERVAL '5' SECOND
      """.stripMargin
    verifyRemainConditionConvert(
      query1,
      "")

    util.addDataStream[(Int, Long, Int)]("MyTable5", 'a, 'b, 'c, 'proctime)
    util.addDataStream[(Int, Long, Int)]("MyTable6", 'a, 'b, 'c, 'proctime)
    val query2 =
      """
        |SELECT t1.a, t2.c FROM MyTable5 AS t1 JOIN MyTable6 AS t2 ON
        |    t1.a = t2.a AND
        |    t1.proctime >= t2.proctime - INTERVAL '10' SECOND AND
        |    t1.proctime <= t2.proctime - INTERVAL '5' SECOND AND
        |    t1.c > t2.c
      """.stripMargin
    verifyRemainConditionConvert(
      query2,
      ">($2, $6)")
  }

  private def verifyTimeBoundary(
      timeConditionSql: String,
      expLeftSize: Long,
      expRightSize: Long,
      expTimeType: String): Unit = {
    val query =
      s"""
         |SELECT t1.a, t2.b FROM MyTable AS t1 JOIN MyTable2 AS t2 ON
         |    t1.a = t2.a AND
         |    $timeConditionSql
      """.stripMargin

    val table = util.tableEnv.sqlQuery(query)
    val relNode = table.asInstanceOf[TableImpl].getRelNode
    val joinNode = relNode.getInput(0).asInstanceOf[LogicalJoin]
    val rexNode = joinNode.getCondition
    val (windowBounds, _) = WindowJoinUtil.extractWindowBoundsFromPredicate(
      rexNode,
      joinNode.getLeft.getRowType.getFieldCount,
      joinNode.getRowType,
      joinNode.getCluster.getRexBuilder,
      util.tableEnv.getConfig)

    val timeTypeStr = if (windowBounds.get.isEventTime) "rowtime" else "proctime"
    assertEquals(expLeftSize, windowBounds.get.leftLowerBound)
    assertEquals(expRightSize, windowBounds.get.leftUpperBound)
    assertEquals(expTimeType, timeTypeStr)
  }

  private def verifyRemainConditionConvert(
      sqlQuery: String,
      expectConditionStr: String): Unit = {

    val table = util.tableEnv.sqlQuery(sqlQuery)
    val relNode = table.asInstanceOf[TableImpl].getRelNode
    val joinNode = relNode.getInput(0).asInstanceOf[LogicalJoin]
    val joinInfo = joinNode.analyzeCondition
    val rexNode = joinInfo.getRemaining(joinNode.getCluster.getRexBuilder)
    val (_, remainCondition) =
      WindowJoinUtil.extractWindowBoundsFromPredicate(
        rexNode,
        joinNode.getLeft.getRowType.getFieldCount,
        joinNode.getRowType,
        joinNode.getCluster.getRexBuilder,
        util.tableEnv.getConfig)
    val actual: String = remainCondition.getOrElse("").toString
    assertEquals(expectConditionStr, actual)
  }

}
