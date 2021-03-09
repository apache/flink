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

import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.WeightedAvgWithMerge
import org.apache.flink.table.planner.utils.TableTestBase
import org.junit.Test

/**
 * Tests for window rank.
 */
class WindowRankTest extends TableTestBase {

  private val util = streamTestUtil()
  util.addTemporarySystemFunction("weightedAvg", classOf[WeightedAvgWithMerge])
  util.tableEnv.executeSql(
    s"""
       |CREATE TABLE MyTable (
       |  a INT,
       |  b BIGINT,
       |  c STRING NOT NULL,
       |  d DECIMAL(10, 3),
       |  e BIGINT,
       |  rowtime TIMESTAMP(3),
       |  proctime as PROCTIME(),
       |  WATERMARK FOR rowtime AS rowtime - INTERVAL '1' SECOND
       |) with (
       |  'connector' = 'values'
       |)
       |""".stripMargin)

  // ----------------------------------------------------------------------------------------
  // Tests for queries Rank on window TVF, Current does not support merge Window TVF into
  // WindowRank.
  // ----------------------------------------------------------------------------------------

  @Test
  def testCantMergeWindowTVF_Tumble(): Unit = {
    val sql =
      """
        |SELECT window_start, window_end, window_time, a, b, c, d, e
        |FROM (
        |SELECT *,
        |   ROW_NUMBER() OVER(PARTITION BY a, window_start, window_end ORDER BY b DESC) as rownum
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |)
        |WHERE rownum <= 3
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testCantMergeWindowTVF_TumbleOnProctime(): Unit = {
    val sql =
      """
        |SELECT window_start, window_end, window_time, a, b, c, d, e
        |FROM (
        |SELECT *,
        |   ROW_NUMBER() OVER(PARTITION BY a, window_start, window_end ORDER BY b DESC) as rownum
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(proctime), INTERVAL '15' MINUTE))
        |)
        |WHERE rownum <= 3
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testCantMergeWindowTVF_Hop(): Unit = {
    val sql =
      """
        |SELECT window_start, window_end, window_time, a, b, c, d, e
        |FROM (
        |SELECT *,
        |   ROW_NUMBER() OVER(PARTITION BY a, window_start, window_end ORDER BY b DESC) as rownum
        |FROM TABLE(
        |  HOP(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
        |)
        |WHERE rownum <= 3
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testCantMergeWindowTVF_HopOnProctime(): Unit = {
    val sql =
      """
        |SELECT window_start, window_end, window_time, a, b, c, d, e
        |FROM (
        |SELECT *,
        |   ROW_NUMBER() OVER(PARTITION BY a, window_start, window_end ORDER BY b DESC) as rownum
        |FROM TABLE(
        |  HOP(TABLE MyTable, DESCRIPTOR(proctime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
        |)
        |WHERE rownum <= 3
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testCantMergeWindowTVF_Cumulate(): Unit = {
    val sql =
      """
        |SELECT window_start, window_end, window_time, a, b, c, d, e
        |FROM (
        |SELECT *,
        |   ROW_NUMBER() OVER(PARTITION BY a, window_start, window_end ORDER BY b DESC) as rownum
        |FROM TABLE(
        |  CUMULATE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |)
        |WHERE rownum <= 3
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testCantMergeWindowTVF_CumulateOnProctime(): Unit = {
    val sql =
      """
        |SELECT window_start, window_end, window_time, a, b, c, d, e
        |FROM (
        |SELECT *,
        |   ROW_NUMBER() OVER(PARTITION BY a, window_start, window_end ORDER BY b DESC) as rownum
        |FROM TABLE(
        |  CUMULATE(TABLE MyTable, DESCRIPTOR(proctime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |)
        |WHERE rownum <= 3
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  // ----------------------------------------------------------------------------------------
  // Tests for queries Rank on window Aggregate
  // ----------------------------------------------------------------------------------------

  @Test
  def testOnTumbleWindowAggregate(): Unit = {
    val sql =
      """
        |SELECT window_start, window_end, window_time, a, cnt, sum_d, max_d, wAvg, uv
        |FROM (
        |SELECT *,
        |   ROW_NUMBER() OVER(PARTITION BY window_start, window_end ORDER BY cnt DESC) as rownum
        |FROM (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    sum(d) as sum_d,
        |    max(d) filter (where b > 1000) as max_d,
        |    weightedAvg(b, e) AS wAvg,
        |    count(distinct c) AS uv
        |  FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |  GROUP BY a, window_start, window_end, window_time
        |  )
        |)
        |WHERE rownum <= 3
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testOnTumbleWindowAggregateOnProctime(): Unit = {
    val sql =
      """
        |SELECT window_start, window_end, window_time, a, cnt, sum_d, max_d, wAvg, uv
        |FROM (
        |SELECT *,
        |   ROW_NUMBER() OVER(PARTITION BY a, window_start, window_end ORDER BY cnt DESC) as rownum
        |FROM (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    sum(d) as sum_d,
        |    max(d) filter (where b > 1000) as max_d,
        |    weightedAvg(b, e) AS wAvg,
        |    count(distinct c) AS uv
        |  FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(proctime), INTERVAL '15' MINUTE))
        |  GROUP BY a, window_start, window_end, window_time
        |  )
        |)
        |WHERE rownum <= 3
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testOnHopWindowAggregate(): Unit = {
    val sql =
      """
        |SELECT window_start, window_end, window_time, a, cnt, sum_d, max_d, wAvg, uv
        |FROM (
        |SELECT *,
        |   ROW_NUMBER() OVER(PARTITION BY a, window_start, window_end ORDER BY cnt DESC) as rownum
        |FROM (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    sum(d) as sum_d,
        |    max(d) filter (where b > 1000) as max_d,
        |    weightedAvg(b, e) AS wAvg,
        |    count(distinct c) AS uv
        |  FROM TABLE(
        |    HOP(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
        |  GROUP BY a, window_start, window_end, window_time
        |  )
        |)
        |WHERE rownum <= 3
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testOnHopWindowAggregateOnProctime(): Unit = {
    val sql =
      """
        |SELECT window_start, window_end, window_time, a, cnt, sum_d, max_d, wAvg, uv
        |FROM (
        |SELECT *,
        |   ROW_NUMBER() OVER(PARTITION BY a, window_start, window_end ORDER BY cnt DESC) as rownum
        |FROM (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    sum(d) as sum_d,
        |    max(d) filter (where b > 1000) as max_d,
        |    weightedAvg(b, e) AS wAvg,
        |    count(distinct c) AS uv
        |  FROM TABLE(
        |    HOP(TABLE MyTable, DESCRIPTOR(proctime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
        |  GROUP BY a, window_start, window_end, window_time
        |  )
        |)
        |WHERE rownum <= 3
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testOnCumulateWindowAggregate(): Unit = {
    val sql =
      """
        |SELECT window_start, window_end, window_time, a, cnt, sum_d, max_d, wAvg, uv
        |FROM (
        |SELECT *,
        |   ROW_NUMBER() OVER(
        |     PARTITION BY window_start, window_end ORDER BY cnt DESC) as rownum
        |FROM (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    sum(d) as sum_d,
        |    max(d) filter (where b > 1000) as max_d,
        |    weightedAvg(b, e) AS wAvg,
        |    count(distinct c) AS uv
        |  FROM TABLE(
        |    CUMULATE(
        |      TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |  GROUP BY a, window_start, window_end, window_time
        |  )
        |)
        |WHERE rownum <= 3
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testOnCumulateWindowAggregateOnProctime(): Unit = {
    val sql =
      """
        |SELECT window_start, window_end, window_time, a, cnt, sum_d, max_d, wAvg, uv
        |FROM (
        |SELECT *,
        |   ROW_NUMBER() OVER(
        |     PARTITION BY a, window_start, window_end ORDER BY cnt DESC) as rownum
        |FROM (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    sum(d) as sum_d,
        |    max(d) filter (where b > 1000) as max_d,
        |    weightedAvg(b, e) AS wAvg,
        |    count(distinct c) AS uv
        |  FROM TABLE(
        |    CUMULATE(
        |      TABLE MyTable, DESCRIPTOR(proctime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |  GROUP BY a, window_start, window_end, window_time
        |  )
        |)
        |WHERE rownum <= 3
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  // ----------------------------------------------------------------------------------------
  // Tests for queries window rank could propagate time attribute
  // ----------------------------------------------------------------------------------------
  @Test
  def testTimeAttributePropagateForWindowRank(): Unit = {
    util.tableEnv.executeSql(
      """
        |CREATE VIEW tmp AS
        |SELECT window_time as rowtime, a, b, c, d, e
        |FROM (
        |SELECT *,
        |   ROW_NUMBER() OVER(PARTITION BY a, window_start, window_end ORDER BY b DESC) as rownum
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |)
        |WHERE rownum <= 3
      """.stripMargin)
    val sql =
      """
        |SELECT
        |   a,
        |   window_start,
        |   window_end,
        |   count(*),
        |   sum(d),
        |   max(d) filter (where b > 1000),
        |   weightedAvg(b, e) AS wAvg,
        |   count(distinct c) AS uv
        |FROM TABLE(TUMBLE(TABLE tmp, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |GROUP BY a, window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testTimeAttributePropagateForWindowRank1(): Unit = {
    util.tableEnv.executeSql(
      """
        |CREATE VIEW tmp1 AS
        |SELECT window_time as rowtime, a, cnt, sum_d, max_d, wAvg, uv
        |FROM (
        |  SELECT *,
        |    ROW_NUMBER() OVER(PARTITION BY a, window_start, window_end ORDER BY cnt DESC) as rownum
        |  FROM (
        |    SELECT
        |      a,
        |      window_start,
        |      window_end,
        |      window_time,
        |      count(*) as cnt,
        |      sum(d) as sum_d,
        |      max(d) filter (where b > 1000) as max_d,
        |      weightedAvg(b, e) AS wAvg,
        |      count(distinct c) AS uv
        |    FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |    GROUP BY a, window_start, window_end, window_time
        |  )
        |)
        |WHERE rownum <= 3
      """.stripMargin)
    val sql =
      """
        |SELECT
        |   a,
        |   window_start,
        |   window_end,
        |   sum(cnt),
        |   sum(sum_d),
        |   max(max_d)
        |FROM TABLE(TUMBLE(TABLE tmp1, DESCRIPTOR(rowtime), INTERVAL '1' HOUR))
        |GROUP BY a, window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

}
