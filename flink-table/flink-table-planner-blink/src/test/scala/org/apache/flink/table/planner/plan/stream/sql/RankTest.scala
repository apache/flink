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

import org.junit.Test

class RankTest extends TableTestBase {

  private val util = streamTestUtil()
  util.addDataStream[(Int, String, Long)](
    "MyTable", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)

  @Test
  def testRankEndMustSpecified(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT a, b, c,
        |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC) as rank_num
        |  FROM MyTable)
        |WHERE rank_num >= 10
      """.stripMargin

    thrown.expectMessage("Rank end is not specified.")
    thrown.expect(classOf[TableException])
    util.verifyPlan(sql)
  }

  @Test
  def testRankEndLessThanZero(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT a, b, c,
        |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC) as rank_num
        |  FROM MyTable)
        |WHERE rank_num <= 0
      """.stripMargin

    thrown.expectMessage("Rank end should not less than zero")
    thrown.expect(classOf[TableException])
    util.verifyPlan(sql)
  }

  @Test
  def testRankEndLessThan1(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT a, b, c,
        |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC) as row_num
        |  FROM MyTable)
        |WHERE row_num <= 1
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testRankFunctionInMiddle(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM (
        | SELECT a, RANK() OVER (PARTITION BY a ORDER BY a) rk, b, c FROM MyTable) t
        |WHERE rk < 10
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRowNumberWithRankEndLessThan1OrderByProctimeAsc(): Unit = {
    // be converted to StreamExecDeduplicate
    val sql =
      """
        |SELECT a, b, c
        |FROM (
        |  SELECT a, b, c, proctime,
        |       ROW_NUMBER() OVER (PARTITION BY a ORDER BY proctime ASC) as row_num
        |  FROM MyTable)
        |WHERE row_num <= 1
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testRowNumberWithRankEndLessThan1OrderByProctimeDesc(): Unit = {
    // be converted to StreamExecDeduplicate
    val sql =
      """
        |SELECT a, b, c
        |FROM (
        |  SELECT a, b, c, proctime,
        |       ROW_NUMBER() OVER (PARTITION BY a ORDER BY proctime DESC) as row_num
        |  FROM MyTable)
        |WHERE row_num <= 1
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testRowNumberWithRankEndLessThan1OrderByRowtimeAsc(): Unit = {
    // can not be converted to StreamExecDeduplicate
    val sql =
      """
        |SELECT a, b, c
        |FROM (
        |  SELECT a, b, c, rowtime,
        |       ROW_NUMBER() OVER (PARTITION BY a ORDER BY rowtime ASC) as row_num
        |  FROM MyTable)
        |WHERE row_num <= 1
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testRowNumberWithRankEndLessThan1OrderByRowtimeDesc(): Unit = {
    // can not be converted to StreamExecDeduplicate
    val sql =
      """
        |SELECT a, b, c
        |FROM (
        |  SELECT a, b, c, rowtime,
        |       ROW_NUMBER() OVER (PARTITION BY a ORDER BY rowtime DESC) as row_num
        |  FROM MyTable)
        |WHERE row_num <= 1
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testRankWithRankEndLessThan1OrderByProctimeAsc(): Unit = {
    // can not be converted to StreamExecDeduplicate
    val sql =
      """
        |SELECT a, b, c
        |FROM (
        |  SELECT a, b, c, proctime,
        |       RANK() OVER (PARTITION BY a ORDER BY proctime ASC) as rk
        |  FROM MyTable)
        |WHERE rk <= 1
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testRankWithRankEndLessThan1OrderByProctimeDesc(): Unit = {
    // can not be converted to StreamExecDeduplicate
    val sql =
      """
        |SELECT a, b, c
        |FROM (
        |  SELECT a, b, c, proctime,
        |       RANK() OVER (PARTITION BY a ORDER BY proctime DESC) as rk
        |  FROM MyTable)
        |WHERE rk <= 1
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test(expected = classOf[RuntimeException])
  def testRowNumberWithOutOrderBy(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT a, ROW_NUMBER() OVER (PARTITION BY b) as row_num
        |  FROM MyTable)
        |WHERE row_num <= a
      """.stripMargin
    util.verifyPlan(sql)
  }

  @Test(expected = classOf[ValidationException])
  def testRankWithOutOrderBy(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT a, RANK() OVER (PARTITION BY b) as rk
        |  FROM MyTable)
        |WHERE rk <= a
      """.stripMargin
    util.verifyPlan(sql)
  }

  @Test(expected = classOf[ValidationException])
  def testDenseRankWithOutOrderBy(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT a, DENSE_RANK() OVER (PARTITION BY b) as rk
        |  FROM MyTable)
        |WHERE rk <= a
      """.stripMargin
    util.verifyPlan(sql)
  }

  @Test(expected = classOf[RuntimeException])
  def testRowNumberWithMultiGroups(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT a, ROW_NUMBER() OVER (PARTITION BY b ORDER BY a) as row_num,
        |         ROW_NUMBER() OVER (PARTITION BY a) as row_num1
        |  FROM MyTable)
        |WHERE row_num <= a
      """.stripMargin
    util.verifyPlan(sql)
  }

  @Test(expected = classOf[ValidationException])
  def testRankWithMultiGroups(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT a, RANK() OVER (PARTITION BY b ORDER BY a) as rk,
        |         RANK() OVER (PARTITION BY a) as rk1
        |  FROM MyTable)
        |WHERE rk <= a
      """.stripMargin
    util.verifyPlan(sql)
  }

  @Test(expected = classOf[ValidationException])
  def testDenseRankWithMultiGroups(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT a, DENSE_RANK() OVER (PARTITION BY b ORDER BY a) as rk,
        |         DENSE_RANK() OVER (PARTITION BY a) as rk1
        |  FROM MyTable)
        |WHERE rk <= a
      """.stripMargin
    util.verifyPlan(sql)
  }

  @Test
  def testTopN(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT a, b, c,
        |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC) as row_num
        |  FROM MyTable)
        |WHERE row_num <= 10
      """.stripMargin

    util.verifyPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testTopN2(): Unit = {
    // change the rank_num filter direction
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT a, b, c,
        |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC) as row_num
        |  FROM MyTable)
        |WHERE 10 >= row_num
      """.stripMargin

    util.verifyPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testTopNth(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT a, b, c,
        |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC) as row_num
        |  FROM MyTable)
        |WHERE row_num = 10
      """.stripMargin

    util.verifyPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testTopNWithFilter(): Unit = {
    val sql =
      """
        |SELECT row_num, a, c
        |FROM (
        |  SELECT a, b, c,
        |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC) as row_num
        |  FROM MyTable
        |  WHERE c > 1000)
        |WHERE row_num <= 10 AND b IS NOT NULL
      """.stripMargin

    util.verifyPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testTopNAfterAgg(): Unit = {
    val subquery =
      """
        |SELECT a, b, SUM(c) as sum_c
        |FROM MyTable
        |GROUP BY a, b
      """.stripMargin

    val sql =
      s"""
         |SELECT *
         |FROM (
         |  SELECT a, b, sum_c,
         |      ROW_NUMBER() OVER (PARTITION BY b ORDER BY sum_c DESC) as row_num
         |  FROM ($subquery))
         |WHERE row_num <= 10
      """.stripMargin

    util.verifyPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testTopNWithKeyChanged(): Unit = {
    val subquery =
      """
        |SELECT a, last_value(b) as b, SUM(c) as sum_c
        |FROM MyTable
        |GROUP BY a
      """.stripMargin

    val sql =
      s"""
         |SELECT *
         |FROM (
         |  SELECT a, b, sum_c,
         |      ROW_NUMBER() OVER (PARTITION BY b ORDER BY sum_c DESC) as row_num
         |  FROM ($subquery))
         |WHERE row_num <= 10
      """.stripMargin

    util.verifyPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testUnarySortTopNOnString(): Unit = {
    util.addTableSource[(String, Int, String)]("T", 'category, 'shopId, 'price)
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT category, shopId, max_price,
        |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY max_price ASC) as row_num
        |  FROM (
        |     SELECT category, shopId, MAX(price) as max_price
        |     FROM T
        |     GROUP BY category, shopId
        |  ))
        |WHERE row_num <= 3
      """.stripMargin

    util.verifyPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testTopNOrderByCount(): Unit = {
    val subquery =
      """
        |SELECT a, b, COUNT(*) as count_c
        |FROM MyTable
        |GROUP BY a, b
      """.stripMargin

    val sql =
      s"""
         |SELECT *
         |FROM (
         |  SELECT a, b, count_c,
         |      ROW_NUMBER() OVER (PARTITION BY b ORDER BY count_c DESC) as row_num
         |  FROM ($subquery))
         |WHERE row_num <= 10
      """.stripMargin

    val sql2 =
      s"""
         |SELECT max(a) FROM ($sql)
       """.stripMargin

    util.verifyPlan(sql2, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testTopNOrderBySumWithCond(): Unit = {
    val subquery =
      """
        |SELECT a, b, SUM(c) AS sum_c
        |FROM MyTable
        |WHERE c >= 0
        |GROUP BY a, b
      """.stripMargin

    val sql =
      s"""
         |SELECT *
         |FROM (
         |  SELECT a, b, sum_c,
         |      ROW_NUMBER() OVER (PARTITION BY b ORDER BY sum_c DESC) AS row_num
         |  FROM ($subquery))
         |WHERE row_num <= 10
      """.stripMargin

    util.verifyPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testTopNOrderBySumWithCaseWhen(): Unit = {
    val subquery =
      """
        |SELECT a, b, SUM(CASE WHEN c > 10 THEN 1 WHEN c < 0 THEN 0 ELSE null END) AS sum_c
        |FROM MyTable
        |GROUP BY a, b
      """.stripMargin

    val sql =
      s"""
         |SELECT *
         |FROM (
         |  SELECT a, b, sum_c,
         |      ROW_NUMBER() OVER (PARTITION BY b ORDER BY sum_c DESC) AS row_num
         |  FROM ($subquery))
         |WHERE row_num <= 10
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testTopNOrderBySumWithIf(): Unit = {
    val subquery =
      """
        |SELECT a, b, SUM(IF(c > 10, 1, 0)) as sum_c
        |FROM MyTable
        |GROUP BY a, b
      """.stripMargin

    val sql =
      s"""
         |SELECT *
         |FROM (
         |  SELECT a, b, sum_c,
         |      ROW_NUMBER() OVER (PARTITION BY b ORDER BY sum_c DESC) as row_num
         |  FROM ($subquery))
         |WHERE row_num <= 10
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testTopNOrderBySumWithFilterClause(): Unit = {
    val subquery =
      """
        |SELECT a, b, SUM(c) filter (where c >= 0 and a < 0) as sum_c
        |FROM MyTable
        |GROUP BY a, b
      """.stripMargin

    val sql =
      s"""
         |SELECT *
         |FROM (
         |  SELECT a, b, sum_c,
         |      ROW_NUMBER() OVER (PARTITION BY b ORDER BY sum_c DESC) AS row_num
         |  FROM ($subquery))
         |WHERE row_num <= 10
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testTopNOrderBySumWithFilterClause2(): Unit = {
    val subquery =
      """
        |SELECT a, b, SUM(c) FILTER (WHERE c <= 0 AND a < 0) AS sum_c
        |FROM MyTable
        |GROUP BY a, b
      """.stripMargin

    val sql =
      s"""
         |SELECT *
         |FROM (
         |  SELECT a, b, sum_c,
         |      ROW_NUMBER() OVER (PARTITION BY b ORDER BY sum_c ASC) AS row_num
         |  FROM ($subquery))
         |WHERE row_num <= 10
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testTopNOrderByCountAndOtherField(): Unit = {
    val subquery =
      """
        |SELECT a, b, COUNT(*) AS count_c
        |FROM MyTable
        |GROUP BY a, b
      """.stripMargin

    val sql =
      s"""
         |SELECT *
         |FROM (
         |  SELECT a, b, count_c,
         |      ROW_NUMBER() OVER (PARTITION BY b ORDER BY count_c DESC, a ASC) AS row_num
         |  FROM ($subquery))
         |WHERE row_num <= 10
      """.stripMargin

    util.verifyPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testTopNWithGroupByConstantKey(): Unit = {
    val subquery =
      """
        |SELECT a, b, COUNT(*) AS count_c
        |FROM (
        |SELECT *, 'cn' AS cn
        |FROM MyTable
        |)
        |GROUP BY a, b, cn
      """.stripMargin

    val sql =
      s"""
         |SELECT *
         |FROM (
         |  SELECT a, b, count_c,
         |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY count_c DESC) AS row_num
         |  FROM ($subquery))
         |WHERE row_num <= 10
      """.stripMargin

    util.verifyPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testNestedTopN(): Unit = {
    val subquery =
      """
        |SELECT a, b, COUNT(*) as count_c
        |FROM (
        |SELECT *, 'cn' as cn
        |FROM MyTable
        |)
        |GROUP BY a, b, cn
      """.stripMargin

    val subquery2 =
      s"""
         |SELECT *
         |FROM (
         |  SELECT a, b, count_c,
         |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY count_c DESC) AS row_num
         |  FROM ($subquery))
         |WHERE row_num <= 10
      """.stripMargin

    val sql =
      s"""
         |SELECT *
         |FROM (
         |  SELECT a, b, count_c,
         |    ROW_NUMBER() OVER (ORDER BY count_c DESC) as rank_num
         |  FROM ($subquery2))
         |WHERE rank_num <= 10
      """.stripMargin

    util.verifyPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test(expected = classOf[ValidationException])
  // FIXME remove expected exception after ADD added
  def testTopNForVariableSize(): Unit = {
    val subquery =
      """
        |SELECT a, b, add(max_c) as c
        |FROM (
        |  SELECT MAX(a) as a, b, MAX(c) as max_c
        |  FROM MyTable
        |  GROUP BY b
        |)
      """.stripMargin

    val sql =
      s"""
         |SELECT *
         |FROM (
         |  SELECT a, b, c,
         |      ROW_NUMBER() OVER (PARTITION BY b ORDER BY c DESC) as row_num
         |  FROM ($subquery))
         |WHERE row_num <= a
      """.stripMargin

    util.verifyPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testCreateViewWithRowNumber(): Unit = {
    util.addTable(
      """
        |CREATE TABLE test_source (
        |  name STRING,
        |  eat STRING,
        |  age BIGINT
        |) WITH (
        |  'connector' = 'values',
        |  'bounded' = 'false'
        |)
      """.stripMargin)
    util.tableEnv.executeSql("create view view1 as select name, eat ,sum(age) as cnt\n"
      + "from test_source group by name, eat")
    util.tableEnv.executeSql("create view view2 as\n"
      + "select *, ROW_NUMBER() OVER (PARTITION BY name ORDER BY cnt DESC) as row_num\n"
      + "from view1")
    util.addTable(
      s"""
         |create table sink (
         |  name varchar,
         |  eat varchar,
         |  cnt bigint
         |)
         |with(
         |  'connector' = 'print'
         |)
         |""".stripMargin
    )
    util.verifyPlanInsert("insert into sink select name, eat, cnt\n"
      + "from view2 where row_num <= 3")
  }

  // TODO add tests about multi-sinks and udf
}
