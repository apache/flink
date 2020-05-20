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

package org.apache.flink.table.planner.plan.stream.sql.agg

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{ExplainDetail, TableException, Types, ValidationException}
import org.apache.flink.table.planner.utils.{StreamTableTestUtil, TableTestBase}
import org.apache.flink.table.runtime.typeutils.DecimalDataTypeInfo

import org.junit.Test

class AggregateTest extends TableTestBase {

  private val util: StreamTableTestUtil = streamTestUtil()
  util.addTableSource[(Int, String, Long)](
    "MyTable", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)
  util.addTableSource[(Int, Long, String, Boolean)]("T", 'a, 'b, 'c, 'd)
  util.addTableSource[(Long, Int, String)]("T1", 'a, 'b, 'c)
  util.addTableSource[(Long, Int, String)]("T2", 'a, 'b, 'c)
  util.addTableSource("MyTable1",
    Array[TypeInformation[_]](
      Types.BYTE, Types.SHORT, Types.INT, Types.LONG, Types.FLOAT, Types.DOUBLE, Types.BOOLEAN,
      Types.STRING, Types.LOCAL_DATE, Types.LOCAL_TIME, Types.LOCAL_DATE_TIME,
      DecimalDataTypeInfo.of(30, 20), DecimalDataTypeInfo.of(10, 5)),
    Array("byte", "short", "int", "long", "float", "double", "boolean",
      "string", "date", "time", "timestamp", "decimal3020", "decimal105"))

  @Test(expected = classOf[ValidationException])
  def testGroupingOnNonExistentField(): Unit = {
    util.verifyPlan("SELECT COUNT(*) FROM MyTable GROUP BY foo")
  }

  @Test(expected = classOf[ValidationException])
  def testGroupingInvalidSelection(): Unit = {
    util.verifyPlan("SELECT b FROM MyTable GROUP BY a")
  }

  @Test
  def testCannotCountOnMultiFields(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("We now only support the count of one field")
    util.verifyPlan("SELECT b, COUNT(a, c) FROM MyTable GROUP BY b")
  }

  @Test
  def testAggWithMiniBatch(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true)
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, "1 s")
    util.verifyPlan("SELECT b, COUNT(DISTINCT a), MAX(b), SUM(c)  FROM MyTable GROUP BY b")
  }

  @Test
  def testAggAfterUnionWithMiniBatch(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true)
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, "1 s")
    val query =
      """
        |SELECT a, sum(b), count(distinct c)
        |FROM (
        |  SELECT * FROM T1
        |  UNION ALL
        |  SELECT * FROM T2
        |) GROUP BY a
      """.stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testGroupByWithoutWindow(): Unit = {
    util.verifyPlan("SELECT COUNT(a) FROM MyTable GROUP BY b")
  }

  @Test
  def testLocalGlobalAggAfterUnion(): Unit = {
    // enable local global optimize
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true)
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, "1 s")

    val sql =
      """
        |SELECT a, SUM(b), COUNT(DISTINCT c)
        |FROM (
        |  SELECT * FROM T1
        |  UNION ALL
        |  SELECT * FROM T2
        |) GROUP BY a
      """.stripMargin
    util.verifyPlan(sql)
  }

  @Test
  def testAggWithFilterClause(): Unit = {
    val sql =
      """
        |SELECT
        |  a,
        |  SUM(b) FILTER (WHERE c = 'A'),
        |  COUNT(DISTINCT c) FILTER (WHERE d is true),
        |  MAX(b)
        |FROM T GROUP BY a
      """.stripMargin
    util.verifyPlan(sql)
  }

  @Test
  def testAggWithFilterClauseWithLocalGlobal(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true)
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, "1 s")

    val sql =
      """
        |SELECT
        |  a,
        |  SUM(b) FILTER (WHERE c = 'A'),
        |  COUNT(DISTINCT c) FILTER (WHERE d is true),
        |  COUNT(DISTINCT c) FILTER (WHERE b = 1),
        |  MAX(b)
        |FROM T GROUP BY a
      """.stripMargin
    util.verifyPlan(sql)
  }

  @Test
  def testAggOnDifferentTypes(): Unit = {
    // FlinkRelMdModifiedMonotonicity will analyse sum argument's column interval
    // this test covers all column interval types
    val sql =
    """
      |SELECT
      |  a,
      |  SUM(CAST(1 as INT)),
      |  SUM(CAST(2 as BIGINT)),
      |  SUM(CAST(3 as TINYINT)),
      |  SUM(CAST(4 as SMALLINT)),
      |  SUM(CAST(5 as FLOAT)),
      |  SUM(CAST(6 as DECIMAL)),
      |  SUM(CAST(7 as DOUBLE))
      |FROM T GROUP BY a
    """.stripMargin
    util.verifyPlanWithType(sql)
  }

  @Test
  def testAvgOnDifferentTypes(): Unit = {
    util.verifyPlanWithType(
      """
        |SELECT AVG(`byte`),
        |       AVG(`short`),
        |       AVG(`int`),
        |       AVG(`long`),
        |       AVG(`float`),
        |       AVG(`double`),
        |       AVG(`decimal3020`),
        |       AVG(`decimal105`)
        |FROM MyTable1
      """.stripMargin)
  }

  @Test
  def testAvgWithRetract(): Unit = {
    util.verifyPlan(
      "SELECT AVG(a) FROM (SELECT AVG(a) AS a FROM T GROUP BY b)", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testSum(): Unit = {
    util.verifyPlanWithType(
      """
        |SELECT SUM(`byte`),
        |       SUM(`short`),
        |       SUM(`int`),
        |       SUM(`long`),
        |       SUM(`float`),
        |       SUM(`double`),
        |       SUM(`decimal3020`),
        |       SUM(`decimal105`)
        |FROM MyTable1
      """.stripMargin)
  }

  @Test
  def testSumWithRetract(): Unit = {
    util.verifyPlan(
      "SELECT SUM(a) FROM (SELECT SUM(a) AS a FROM T GROUP BY b)", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testMinOnDifferentTypes(): Unit = {
    util.verifyPlanWithType(
      """
        |SELECT MIN(`byte`),
        |       MIN(`short`),
        |       MIN(`int`),
        |       MIN(`long`),
        |       MIN(`float`),
        |       MIN(`double`),
        |       MIN(`decimal3020`),
        |       MIN(`decimal105`),
        |       MIN(`boolean`),
        |       MIN(`date`),
        |       MIN(`time`),
        |       MIN(`timestamp`),
        |       MIN(`string`)
        |FROM MyTable1
      """.stripMargin)
  }

  @Test
  def testMinWithRetract(): Unit = {
    util.verifyPlan(
      "SELECT MIN(a) FROM (SELECT MIN(a) AS a FROM T GROUP BY b)", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testMaxOnDifferentTypes(): Unit = {
    util.verifyPlanWithType(
      """
        |SELECT MAX(`byte`),
        |       MAX(`short`),
        |       MAX(`int`),
        |       MAX(`long`),
        |       MAX(`float`),
        |       MAX(`double`),
        |       MAX(`decimal3020`),
        |       MAX(`decimal105`),
        |       MAX(`boolean`),
        |       MAX(`date`),
        |       MAX(`time`),
        |       MAX(`timestamp`),
        |       MAX(`string`)
        |FROM MyTable1
      """.stripMargin)
  }

  @Test
  def testMaxWithRetract(): Unit = {
    util.verifyPlan(
      "SELECT MAX(a) FROM (SELECT MAX(a) AS a FROM T GROUP BY b)", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testGroupByWithConstantKey(): Unit = {
    val sql =
      """
        |SELECT a, MAX(b), c FROM (SELECT a, 'test' AS c, b FROM T) t GROUP BY a, c
      """.stripMargin
    util.verifyPlan(sql)
  }
}
