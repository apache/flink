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

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.WeightedAvgWithMerge
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test

/**
 * Tests for window deduplicate.
 */
class WindowDeduplicateTest extends TableTestBase {

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
  // Tests for queries Deduplicate on window TVF
  // ----------------------------------------------------------------------------------------

  @Test
  def testOnWindowTVFWithCalc(): Unit = {
    val sql =
      """
        |SELECT window_start, window_end, window_time, a, b, c, d, e
        |FROM (
        |SELECT *,
        |   ROW_NUMBER() OVER(PARTITION BY a, window_start, window_end
        |   ORDER BY rowtime DESC) as rownum
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |)
        |WHERE rownum <= 1
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testOnWindowTVF(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |SELECT *,
        |   ROW_NUMBER() OVER(PARTITION BY a, window_start, window_end
        |   ORDER BY rowtime DESC) as rownum
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |)
        |WHERE rownum <= 1
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testOnWindowTVFWithValidCondition(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT *,
        |    ROW_NUMBER() OVER(PARTITION BY a, window_start, window_end
        |    ORDER BY rowtime DESC) as rownum
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |)
        |WHERE rownum < 2
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testFallbackToWindowTopNForUnmatchedCondition(): Unit = {
    // the query would be translated to window topN instead of window deduplicate because of
    // unmatched filter condition
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT *,
        |    ROW_NUMBER() OVER(PARTITION BY a, window_start, window_end
        |    ORDER BY rowtime DESC) as rownum
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |)
        |WHERE rownum < 3
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testFallbackToWindowTopNForUnmatchedOrderKey(): Unit = {
    // the query would be translated to window topN instead of window deduplicate because of
    // unmatched order key
    val sql =
    """
      |SELECT *
      |FROM (
      |  SELECT *,
      |    ROW_NUMBER() OVER(PARTITION BY a, window_start, window_end
      |    ORDER BY b DESC) as rownum
      |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
      |)
      |WHERE rownum <= 1
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testOnWindowTVFKeepFirstRow(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |SELECT *,
        |   ROW_NUMBER() OVER(PARTITION BY a, window_start, window_end
        |   ORDER BY rowtime ASC) as rownum
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |)
        |WHERE rownum <= 1
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testUnsupportedWindowTVFOnProctime(): Unit = {
    val sql =
      """
        |SELECT window_start, window_end, window_time, a, b, c, d, e
        |FROM (
        |SELECT *,
        |   ROW_NUMBER() OVER(PARTITION BY a, window_start, window_end
        |   ORDER BY proctime DESC) as rownum
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(proctime), INTERVAL '15' MINUTE))
        |)
        |WHERE rownum <= 1
      """.stripMargin

    thrown.expectMessage("Processing time Window Deduplication is not supported yet.")
    thrown.expect(classOf[TableException])
    util.verifyExplain(sql)
  }

  @Test
  def testUnsupportedWindowTVFOnProctimeKeepFirstRow(): Unit = {
    val sql =
      """
        |SELECT window_start, window_end, window_time, a, b, c, d, e
        |FROM (
        |SELECT *,
        |   ROW_NUMBER() OVER(PARTITION BY a, window_start, window_end
        |   ORDER BY proctime) as rownum
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(proctime), INTERVAL '15' MINUTE))
        |)
        |WHERE rownum <= 1
      """.stripMargin

    thrown.expectMessage("Processing time Window Deduplication is not supported yet.")
    thrown.expect(classOf[TableException])
    util.verifyExplain(sql)
  }

  // ----------------------------------------------------------------------------------------
  // Tests for queries window deduplicate could propagate time attribute
  // ----------------------------------------------------------------------------------------
  @Test
  def testTimeAttributePropagateForWindowDeduplicate(): Unit = {
    util.tableEnv.executeSql(
      """
        |CREATE VIEW tmp AS
        |SELECT window_time as rowtime, a, b, c, d, e
        |FROM (
        |SELECT *,
        |   ROW_NUMBER() OVER(PARTITION BY a, window_start, window_end
        |   ORDER BY rowtime DESC) as rownum
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |)
        |WHERE rownum <= 1
      """.stripMargin)
    val sql =
      """
        |SELECT
        |   window_start,
        |   window_end,
        |   count(*),
        |   sum(d),
        |   max(d) filter (where b > 1000),
        |   weightedAvg(b, e) AS wAvg,
        |   count(distinct c) AS uv
        |FROM TABLE(TUMBLE(TABLE tmp, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |GROUP BY window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }
}
