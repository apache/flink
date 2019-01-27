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
package org.apache.flink.table.api.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.functions.ScalarFunction
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableConfig, TableConfigOptions, TableEnvironment, TableException}
import org.apache.flink.table.runtime.utils.TestingRetractTableSink
import org.apache.flink.table.util.{StreamTableTestUtil, TableTestBase}

import org.junit.Assert.assertEquals
import org.junit.{Before, Test}

import scala.io.Source

class RankTest extends TableTestBase {

  var streamUtil: StreamTableTestUtil = _

  @Before
  def testSetUp(): Unit = {
    streamUtil = streamTestUtil()
    streamUtil.addTable[(Int, String, Long)]("MyTable", 'a, 'b, 'c)
    streamUtil.addFunction("add", new AddUdf)
  }

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
    streamUtil.verifyPlan(sql)
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
    streamUtil.verifyPlan(sql)
  }

  @Test
  def testOrderByLimit(): Unit = {
    val sql = "SELECT * FROM MyTable ORDER BY a, b desc LIMIT 10"
    streamUtil.verifyPlanAndTrait(sql)
  }

  @Test
  def testOrderByFetch(): Unit = {
    val sql = "SELECT * FROM MyTable ORDER BY a, b desc FETCH FIRST 10 ROWS ONLY"
    streamUtil.verifyPlanAndTrait(sql)
  }

  @Test
  def testTopN(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT a, b, c,
        |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC) as rank_num
        |  FROM MyTable)
        |WHERE rank_num <= 10
      """.stripMargin

    streamUtil.verifyPlanAndTrait(sql)
  }

  @Test
  def testTopN2(): Unit = {
    // change the rank_num filter direction
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT a, b, c,
        |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC) as rank_num
        |  FROM MyTable)
        |WHERE 10 >= rank_num
      """.stripMargin

    streamUtil.verifyPlanAndTrait(sql)
  }

  @Test
  def testTopNth(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT a, b, c,
        |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC) as rank_num
        |  FROM MyTable)
        |WHERE rank_num = 10
      """.stripMargin

    streamUtil.verifyPlanAndTrait(sql)
  }

  @Test
  def testTopNWithFilter(): Unit = {
    val sql =
      """
        |SELECT rank_num, a, c
        |FROM (
        |  SELECT a, b, c,
        |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC) as rank_num
        |  FROM MyTable
        |  WHERE c > 1000)
        |WHERE rank_num <= 10 AND b IS NOT NULL
      """.stripMargin

    streamUtil.verifyPlanAndTrait(sql)
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
        |      ROW_NUMBER() OVER (PARTITION BY b ORDER BY sum_c DESC) as rank_num
        |  FROM ($subquery))
        |WHERE rank_num <= 10
      """.stripMargin

    streamUtil.verifyPlanAndTrait(sql)
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
         |      ROW_NUMBER() OVER (PARTITION BY b ORDER BY sum_c DESC) as rank_num
         |  FROM ($subquery))
         |WHERE rank_num <= 10
      """.stripMargin

    streamUtil.verifyPlanAndTrait(sql)
  }

  @Test
  def testUnarySortTopNOnString(): Unit = {
    streamUtil.addTable[(String, Int, String)]("T", 'category, 'shopId, 'price)
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT category, shopId, max_price,
        |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY max_price ASC) as rank_num
        |  FROM (
        |     SELECT category, shopId, max(price) as max_price
        |     FROM T
        |     GROUP BY category, shopId
        |  ))
        |WHERE rank_num <= 3
      """.stripMargin

    streamUtil.verifyPlanAndTrait(sql)
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
         |      ROW_NUMBER() OVER (PARTITION BY b ORDER BY count_c DESC) as rank_num
         |  FROM ($subquery))
         |WHERE rank_num <= 10
      """.stripMargin

    val sql2 =
      s"""
         |SELECT max(a) FROM ($sql)
       """.stripMargin

    streamUtil.verifyPlanAndTrait(sql2)
  }

  @Test
  def testTopNOrderBySumWithCond(): Unit = {
    val subquery =
      """
        |SELECT a, b, SUM(c) as sum_c
        |FROM MyTable
        |WHERE c >= 0
        |GROUP BY a, b
      """.stripMargin

    val sql =
      s"""
         |SELECT *
         |FROM (
         |  SELECT a, b, sum_c,
         |      ROW_NUMBER() OVER (PARTITION BY b ORDER BY sum_c DESC) as rank_num
         |  FROM ($subquery))
         |WHERE rank_num <= 10
      """.stripMargin

    streamUtil.verifyPlanAndTrait(sql)
  }


  @Test
  def testTopNOrderBySumWithCaseWhen(): Unit = {
    val subquery =
      """
        |SELECT a, b, SUM(case when c > 10 then 1 when c < 0 then 0 else null end) as sum_c
        |FROM MyTable
        |GROUP BY a, b
      """.stripMargin

    val sql =
      s"""
         |SELECT *
         |FROM (
         |  SELECT a, b, sum_c,
         |      ROW_NUMBER() OVER (PARTITION BY b ORDER BY sum_c DESC) as rank_num
         |  FROM ($subquery))
         |WHERE rank_num <= 10
      """.stripMargin

    streamUtil.verifyPlan(sql)
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
         |      ROW_NUMBER() OVER (PARTITION BY b ORDER BY sum_c DESC) as rank_num
         |  FROM ($subquery))
         |WHERE rank_num <= 10
      """.stripMargin

    streamUtil.verifyPlan(sql)
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
         |      ROW_NUMBER() OVER (PARTITION BY b ORDER BY sum_c DESC) as rank_num
         |  FROM ($subquery))
         |WHERE rank_num <= 10
      """.stripMargin

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testTopNOrderBySumWithFilterClause2(): Unit = {
    val subquery =
      """
        |SELECT a, b, SUM(c) filter (where c <= 0 and a < 0) as sum_c
        |FROM MyTable
        |GROUP BY a, b
      """.stripMargin

    val sql =
      s"""
         |SELECT *
         |FROM (
         |  SELECT a, b, sum_c,
         |      ROW_NUMBER() OVER (PARTITION BY b ORDER BY sum_c ASC) as rank_num
         |  FROM ($subquery))
         |WHERE rank_num <= 10
      """.stripMargin

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testTopNOrderByCountAndOtherField(): Unit = {
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
         |      ROW_NUMBER() OVER (PARTITION BY b ORDER BY count_c DESC, a ASC) as rank_num
         |  FROM ($subquery))
         |WHERE rank_num <= 10
      """.stripMargin

    streamUtil.verifyPlanAndTrait(sql)
  }

  @Test
  def testTopNWithGroupByConstantKey(): Unit = {
    val subquery =
      """
        |SELECT a, b, COUNT(*) as count_c
        |FROM (
        |SELECT *, 'cn' as cn
        |FROM MyTable
        |)
        |GROUP BY a, b, cn
      """.stripMargin

    val sql =
      s"""
         |SELECT *
         |FROM (
         |  SELECT a, b, count_c,
         |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY count_c DESC) as rank_num
         |  FROM ($subquery))
         |WHERE rank_num <= 10
      """.stripMargin

    streamUtil.verifyPlanAndTrait(sql)
  }

  @Test
  def testTopNOrderByIncrSum(): Unit = {
    val subquery =
      """
        |SELECT a, b, incr_sum(c) as sum_c
        |FROM MyTable
        |GROUP BY a, b
      """.stripMargin

    val sql =
      s"""
         |SELECT *
         |FROM (
         |  SELECT a, b, sum_c,
         |      ROW_NUMBER() OVER (PARTITION BY b ORDER BY sum_c DESC) as rank_num
         |  FROM ($subquery))
         |WHERE rank_num <= 10
      """.stripMargin

    streamUtil.verifyPlanAndTrait(sql)
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
         |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY count_c DESC) as rank_num
         |  FROM ($subquery))
         |WHERE rank_num <= 10
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

    streamUtil.verifyPlanAndTrait(sql)
  }

  @Test
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
         |      ROW_NUMBER() OVER (PARTITION BY b ORDER BY c DESC) as rank_num
         |  FROM ($subquery))
         |WHERE rank_num <= a
      """.stripMargin

    streamUtil.verifyPlanAndTrait(sql)
  }

  @Test
  def testTopNWithPartialFinalAgg(): Unit = {
    // BLINK-17146809: fix monotonicity derivation not works when partial final optimization
    streamUtil.tableEnv.getConfig.getConf.setLong(
      TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY, 1000L)
    streamUtil.tableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_DATA_SKEW_DISTINCT_AGG, true)
    streamUtil.tableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_EXEC_INCREMENTAL_AGG_ENABLED, false)

    val subquery =
      """
        |SELECT a, b, SUM(c) as c, COUNT(DISTINCT c) as cd
        |FROM MyTable
        |WHERE c >= 0
        |GROUP BY a, b
      """.stripMargin

    val sql =
      s"""
         |SELECT *
         |FROM (
         |  SELECT *,
         |      ROW_NUMBER() OVER (PARTITION BY b ORDER BY cd DESC) as rank_num
         |  FROM ($subquery))
         |WHERE rank_num <= 10
      """.stripMargin

    streamUtil.verifyPlanAndTrait(sql)
  }

  @Test
  def testTopNWithoutRowNumber2(): Unit = {
    streamUtil.addTable[(String, String, String, String, Long, String, Long, String)](
      "stream_source",
      'seller_id, 'sku_id, 'venture, 'stat_date, 'trd_amt, 'trd_buyer_id, 'log_pv, 'log_visitor_id)

    val group_sql =
      """
        |SELECT
        |    seller_id
        |    ,sku_id
        |    ,venture
        |    ,stat_date
        |    ,incr_sum(trd_amt) AS amt_dtr
        |    ,COUNT(DISTINCT trd_buyer_id) AS byr_cnt_dtr
        |    ,SUM(log_pv) AS pv_dtr
        |    ,COUNT(DISTINCT log_visitor_id) AS uv_dtr
        |FROM stream_source
        |GROUP BY seller_id,sku_id,venture,stat_date
      """.stripMargin

    val sql =
      s"""
         |SELECT
         |    CONCAT(seller_id, venture, stat_date, sku_id) as rowkey,
         |    seller_id,
         |    sku_id,
         |    venture,
         |    stat_date,
         |    amt_dtr,
         |    byr_cnt_dtr,
         |    pv_dtr,
         |    uv_dtr
         |FROM (
         |  SELECT
         |        seller_id,
         |        sku_id,
         |        venture,
         |        stat_date,
         |        amt_dtr,
         |        byr_cnt_dtr,
         |        pv_dtr,
         |        uv_dtr,
         |        ROW_NUMBER() OVER (PARTITION BY seller_id, venture, stat_date
         |           ORDER BY amt_dtr DESC) AS rownum
         |  FROM ($group_sql)
         |)
         |WHERE rownum <= 10
      """.stripMargin

    streamUtil.verifyPlanAndTrait(sql)
  }

  @Test
  def testMultipleRetractTopNAfterAgg(): Unit = {
    val conf = new TableConfig
    conf.setSubsectionOptimization(true)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, conf)

    val data = List(("book", 1, 12))

    val ds = env.fromCollection(data).toTable(tEnv, 'category, 'shopId, 'num)
    tEnv.registerTable("T", ds)

    val subquery =
      s"""
         |SELECT category, shopId, SUM(num) as sum_num, MAX(num) as max_num, AVG(num) as avg_num,
         |COUNT(num) as cnt_num
         |FROM T
         |GROUP BY category, shopId
         |""".stripMargin

    val t = tEnv.sqlQuery(subquery)
    tEnv.registerTable("MyView", t)

    val sink1 = new TestingRetractTableSink
    tEnv.sqlQuery(
      s"""
         |SELECT *
         |FROM (
         |  SELECT category, shopId, sum_num, max_num,
         |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY sum_num DESC,
         |      max_num ASC) as rank_num
         |  FROM MyView)
         |WHERE rank_num <= 2
         |""".stripMargin).writeToSink(sink1)

    val sink2 = new TestingRetractTableSink
    tEnv.sqlQuery(
      s"""
         |SELECT *
         |FROM (
         |  SELECT category, shopId, avg_num, cnt_num,
         |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY avg_num DESC,
         |      cnt_num ASC) as rank_num
         |  FROM MyView)
         |WHERE rank_num <= 2
         |""".stripMargin).writeToSink(sink2)

    val result = replaceString(tEnv.explain())

    val source = readFromResource("testMultipleRetractTopNAfterAgg.out")
    val expected = replaceString(source)
    assertEquals(expected, result)
  }

  @Test
  def testMultipleUpdateTopNAfterAgg(): Unit = {
    val conf = new TableConfig
    conf.setSubsectionOptimization(true)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, conf)

    val data = List(("book", 1, 12))

    val ds = env.fromCollection(data).toTable(tEnv, 'category, 'shopId, 'num)
    tEnv.registerTable("T", ds)

    val subquery =
      s"""
         |SELECT category, shopId, COUNT(num) as cnt_num, MAX(num) as max_num
         |FROM T
         |GROUP BY category, shopId
         |""".stripMargin

    val t = tEnv.sqlQuery(subquery)
    tEnv.registerTable("MyView", t)

    val sink1 = new TestingRetractTableSink
    tEnv.sqlQuery(
      s"""
         |SELECT *
         |FROM (
         |  SELECT category, shopId, cnt_num,
         |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY cnt_num DESC) as rank_num
         |  FROM MyView)
         |WHERE rank_num <= 2
         |""".stripMargin).writeToSink(sink1)

    val sink2 = new TestingRetractTableSink
    tEnv.sqlQuery(
      s"""
         |SELECT *
         |FROM (
         |  SELECT category, shopId, max_num,
         |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY max_num DESC) as rank_num
         |  FROM MyView)
         |WHERE rank_num <= 2
         |""".stripMargin).writeToSink(sink2)

    val result = replaceString(tEnv.explain())

    val source = readFromResource("testMultipleUpdateTopNAfterAgg.out")
    val expected = replaceString(source)
    assertEquals(expected, result)
  }

  private def replaceString(s: String): String = {
    /* Stage {id} is ignored, because id keeps incrementing in test class
     * while StreamExecutionEnvironment is up
     */
    s.replaceAll("\\r\\n", "\n").replaceAll("Stage \\d+", "")
  }

  private def readFromResource(name: String): String = {
    val inputStream = getClass.getResource("/explain/" + name).getFile
    Source.fromFile(inputStream).mkString
  }
}

class AddUdf extends ScalarFunction {
  def eval(a: Int): Int = a + 1
  def eval(a: Long): Long = a + 1
}
