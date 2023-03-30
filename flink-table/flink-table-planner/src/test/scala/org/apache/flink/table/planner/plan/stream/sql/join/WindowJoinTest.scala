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
package org.apache.flink.table.planner.plan.stream.sql.join

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.utils.{StreamTableTestUtil, TableTestBase}

import org.junit.Test

/** Tests for window join. */
class WindowJoinTest extends TableTestBase {

  private val util: StreamTableTestUtil = streamTestUtil()
  util.tableEnv.executeSql(s"""
                              |CREATE TABLE MyTable (
                              |  a INT,
                              |  b STRING NOT NULL,
                              |  c BIGINT,
                              |  rowtime TIMESTAMP(3),
                              |  proctime as PROCTIME(),
                              |  WATERMARK FOR rowtime AS rowtime - INTERVAL '1' SECOND
                              |) with (
                              |  'connector' = 'values'
                              |)
                              |""".stripMargin)

  util.tableEnv.executeSql(s"""
                              |CREATE TABLE MyTable2 (
                              |  a INT,
                              |  b STRING NOT NULL,
                              |  c BIGINT,
                              |  rowtime TIMESTAMP(3),
                              |  proctime as PROCTIME(),
                              |  WATERMARK FOR rowtime AS rowtime - INTERVAL '1' SECOND
                              |) with (
                              |  'connector' = 'values'
                              |)
                              |""".stripMargin)

  // ----------------------------------------------------------------------------------------
  // Tests for queries Join on window TVF
  // ----------------------------------------------------------------------------------------

  @Test
  def testSimplifyTumbleWindowTVFBeforeWindowJoinWithTwoCalc(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT *
        |  FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |  WHERE c > 10
        |) L
        |JOIN (
        |  SELECT *
        |  FROM TABLE(TUMBLE(TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |  WHERE c > 10
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.a = R.a
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testSimplifyTumbleWindowTVFBeforeWindowJoinWithLeftCalc(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT *
        |  FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |  WHERE c > 10
        |) L
        |JOIN (
        |  SELECT *
        |  FROM TABLE(TUMBLE(TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.a = R.a
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testSimplifyTumbleWindowTVFBeforeWindowJoinWithRightCalc(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT *
        |  FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |) L
        |JOIN (
        |  SELECT *
        |  FROM TABLE(TUMBLE(TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |  WHERE c > 10
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.a = R.a
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testSimplifyTumbleWindowTVFBeforeWindowJoin(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT *
        |  FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |) L
        |JOIN (
        |  SELECT *
        |  FROM TABLE(TUMBLE(TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.a = R.a
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testUnsupportedWindowTVF_TumbleOnProctime(): Unit = {
    val sql =
      """
        |SELECT L.a, L.b, L.c, R.a, R.b, R.c
        |FROM (
        |  SELECT *
        |  FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(proctime), INTERVAL '15' MINUTE))
        |) L
        |JOIN (
        |  SELECT *
        |  FROM TABLE(TUMBLE(TABLE MyTable2, DESCRIPTOR(proctime), INTERVAL '15' MINUTE))
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.a = R.a
      """.stripMargin

    thrown.expectMessage("Processing time Window Join is not supported yet.")
    thrown.expect(classOf[TableException])
    util.verifyExplain(sql)
  }

  @Test
  def testSimplifyHopWindowTVFBeforeWindowJoinWithTwoCalc(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT *
        |  FROM TABLE(
        |  HOP(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
        |  WHERE c > 10
        |) L
        |JOIN (
        |  SELECT *
        |  FROM TABLE(
        |  HOP(TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
        |  WHERE c > 10
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.a = R.a
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testSimplifyHopWindowTVFBeforeWindowJoinWithLeftCalc(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT *
        |  FROM TABLE(
        |  HOP(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
        |  WHERE c > 10
        |) L
        |JOIN (
        |  SELECT *
        |  FROM TABLE(
        |  HOP(TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.a = R.a
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testSimplifyHopWindowTVFBeforeWindowJoinWithRightCalc(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT *
        |  FROM TABLE(
        |  HOP(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
        |) L
        |JOIN (
        |  SELECT *
        |  FROM TABLE(
        |  HOP(TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
        |  WHERE c > 10
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.a = R.a
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testSimplifyHopWindowTVFBeforeWindowJoin(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT *
        |  FROM TABLE(
        |  HOP(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
        |) L
        |JOIN (
        |  SELECT *
        |  FROM TABLE(
        |  HOP(TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.a = R.a
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testUnsupportedWindowTVF_HopOnProctime(): Unit = {
    val sql =
      """
        |SELECT L.a, L.b, L.c, R.a, R.b, R.c
        |FROM (
        |  SELECT *
        |  FROM TABLE(
        |  HOP(TABLE MyTable, DESCRIPTOR(proctime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
        |) L
        |JOIN (
        |  SELECT *
        |  FROM TABLE(
        |  HOP(TABLE MyTable2, DESCRIPTOR(proctime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.a = R.a
      """.stripMargin

    thrown.expectMessage("Processing time Window Join is not supported yet.")
    thrown.expect(classOf[TableException])
    util.verifyExplain(sql)
  }

  @Test
  def testSimplifyCumulateWindowTVFBeforeWindowJoinWithTwoCalc(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT *
        |  FROM TABLE(
        |  CUMULATE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |  WHERE c > 10
        |) L
        |JOIN (
        |  SELECT *
        |  FROM TABLE(
        |  CUMULATE(TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |  WHERE c > 10
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.a = R.a
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testSimplifyCumulateWindowTVFBeforeWindowJoinWithLeftCalc(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT *
        |  FROM TABLE(
        |  CUMULATE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |  WHERE c > 10
        |) L
        |JOIN (
        |  SELECT *
        |  FROM TABLE(
        |  CUMULATE(TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.a = R.a
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testSimplifyCumulateWindowTVFBeforeWindowJoinWithRightCalc(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT *
        |  FROM TABLE(
        |  CUMULATE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |) L
        |JOIN (
        |  SELECT *
        |  FROM TABLE(
        |  CUMULATE(TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |  WHERE c > 10
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.a = R.a
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testSimplifyCumulateWindowTVFBeforeWindowJoin(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT *
        |  FROM TABLE(
        |  CUMULATE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |) L
        |JOIN (
        |  SELECT *
        |  FROM TABLE(
        |  CUMULATE(TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.a = R.a
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testUnsupportedWindowTVF_CumulateOnProctime(): Unit = {
    val sql =
      """
        |SELECT L.a, L.b, L.c, R.a, R.b, R.c
        |FROM (
        |  SELECT *
        |  FROM TABLE(
        |  CUMULATE(TABLE MyTable, DESCRIPTOR(proctime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |) L
        |JOIN (
        |  SELECT *
        |  FROM TABLE(
        |  CUMULATE(TABLE MyTable2, DESCRIPTOR(proctime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.a = R.a
      """.stripMargin

    thrown.expectMessage("Processing time Window Join is not supported yet.")
    thrown.expect(classOf[TableException])
    util.verifyExplain(sql)
  }

  // ----------------------------------------------------------------------------------------
  // Tests for invalid queries Join on window Aggregate
  // because left window strategy is not equals to right window strategy.
  // ----------------------------------------------------------------------------------------

  /** Window type in left and right child should be same * */
  @Test
  def testNotSameWindowType(): Unit = {
    val sql =
      """
        |SELECT L.*, R.*
        |FROM (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(
        |  HOP(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
        |  GROUP BY a, window_start, window_end, window_time
        |) L
        |JOIN (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(
        |  CUMULATE(TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |  GROUP BY a, window_start, window_end, window_time
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.a = R.a
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  /** Window spec in left and right child should be same * */
  @Test
  def testNotSameWindowSpec(): Unit = {
    val sql =
      """
        |SELECT L.*, R.*
        |FROM (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(
        |  CUMULATE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE, INTERVAL '2' HOUR))
        |  GROUP BY a, window_start, window_end, window_time
        |) L
        |JOIN (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(
        |  CUMULATE(TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |  GROUP BY a, window_start, window_end, window_time
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.a = R.a
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  /** Window spec in left and right child should be same * */
  @Test
  def testNotSameTimeAttributeType(): Unit = {
    val sql =
      """
        |SELECT L.*, R.*
        |FROM (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(
        |    CUMULATE(
        |      TABLE MyTable, DESCRIPTOR(proctime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |  GROUP BY a, window_start, window_end, window_time
        |) L
        |JOIN (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(
        |    CUMULATE(
        |      TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |  GROUP BY a, window_start, window_end, window_time
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.a = R.a
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  // ----------------------------------------------------------------------------------------
  // Window starts equality and window ends equality are both required for window join.
  // TODO: In the future, we could support join clause which only includes window starts
  //  equality or window ends equality for TUMBLE or HOP window.
  // ----------------------------------------------------------------------------------------

  @Test
  def testMissWindowEndInConditionForTumbleWindow(): Unit = {
    val sql =
      """
        |SELECT L.*, R.*
        |FROM (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |  GROUP BY a, window_start, window_end, window_time
        |) L
        |JOIN (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(TUMBLE(TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |  GROUP BY a, window_start, window_end, window_time
        |) R
        |ON L.window_start = R.window_start AND L.a = R.a
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testMissWindowStartInConditionForTumbleWindow(): Unit = {
    val sql =
      """
        |SELECT L.*, R.*
        |FROM (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |  GROUP BY a, window_start, window_end, window_time
        |) L
        |JOIN (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(TUMBLE(TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |  GROUP BY a, window_start, window_end, window_time
        |) R
        |ON L.window_end = R.window_end AND L.a = R.a
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testMissWindowEndInConditionForHopWindow(): Unit = {
    val sql =
      """
        |SELECT L.*, R.*
        |FROM (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(
        |  HOP(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
        |  GROUP BY a, window_start, window_end, window_time
        |) L
        |JOIN (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(
        |  HOP(TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
        |  GROUP BY a, window_start, window_end, window_time
        |) R
        |ON L.window_start = R.window_start AND L.a = R.a
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testMissWindowStartInConditionForHopWindow(): Unit = {
    val sql =
      """
        |SELECT L.*, R.*
        |FROM (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(
        |  HOP(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
        |  GROUP BY a, window_start, window_end, window_time
        |) L
        |JOIN (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(
        |  HOP(TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
        |  GROUP BY a, window_start, window_end, window_time
        |) R
        |ON L.window_end = R.window_end AND L.a = R.a
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testMissWindowEndInConditionForCumulateWindow(): Unit = {
    val sql =
      """
        |SELECT L.*, R.*
        |FROM (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(
        |    CUMULATE(
        |      TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |  GROUP BY a, window_start, window_end, window_time
        |) L
        |JOIN (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(
        |    CUMULATE(
        |      TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |  GROUP BY a, window_start, window_end, window_time
        |) R
        |ON L.window_start = R.window_start AND L.a = R.a
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testMissWindowStartInConditionForCumulateWindow(): Unit = {
    val sql =
      """
        |SELECT L.*, R.*
        |FROM (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(
        |    CUMULATE(
        |      TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |  GROUP BY a, window_start, window_end, window_time
        |) L
        |JOIN (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(
        |    CUMULATE(
        |      TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |  GROUP BY a, window_start, window_end, window_time
        |) R
        |ON L.window_end = R.window_end AND L.a = R.a
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  // ----------------------------------------------------------------------------------------
  // Tests for valid queries Window Join on window Aggregate.
  // ----------------------------------------------------------------------------------------

  @Test
  def testOnTumbleWindowAggregate(): Unit = {
    val sql =
      """
        |SELECT L.*, R.*
        |FROM (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |  GROUP BY a, window_start, window_end, window_time
        |) L
        |JOIN (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(TUMBLE(TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |  GROUP BY a, window_start, window_end, window_time
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.a = R.a
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testOnTumbleWindowAggregateOnProctime(): Unit = {
    val sql =
      """
        |SELECT L.*, R.*
        |FROM (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(proctime), INTERVAL '15' MINUTE))
        |  GROUP BY a, window_start, window_end, window_time
        |) L
        |JOIN (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(TUMBLE(TABLE MyTable2, DESCRIPTOR(proctime), INTERVAL '15' MINUTE))
        |  GROUP BY a, window_start, window_end, window_time
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.a = R.a
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testOnHopWindowAggregate(): Unit = {
    val sql =
      """
        |SELECT L.*, R.*
        |FROM (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(
        |  HOP(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
        |  GROUP BY a, window_start, window_end, window_time
        |) L
        |JOIN (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(
        |  HOP(TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
        |  GROUP BY a, window_start, window_end, window_time
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.a = R.a
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testOnHopWindowAggregateOnProctime(): Unit = {
    val sql =
      """
        |SELECT L.*, R.*
        |FROM (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(
        |  HOP(TABLE MyTable, DESCRIPTOR(proctime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
        |  GROUP BY a, window_start, window_end, window_time
        |) L
        |JOIN (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(
        |  HOP(TABLE MyTable2, DESCRIPTOR(proctime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
        |  GROUP BY a, window_start, window_end, window_time
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.a = R.a
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testOnCumulateWindowAggregate(): Unit = {
    val sql =
      """
        |SELECT L.*, R.*
        |FROM (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(
        |    CUMULATE(
        |      TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |  GROUP BY a, window_start, window_end, window_time
        |) L
        |JOIN (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(
        |    CUMULATE(
        |      TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |  GROUP BY a, window_start, window_end, window_time
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.a = R.a
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testOnCumulateWindowAggregateOnProctime(): Unit = {
    val sql =
      """
        |SELECT L.*, R.*
        |FROM (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(
        |    CUMULATE(
        |      TABLE MyTable, DESCRIPTOR(proctime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |  GROUP BY a, window_start, window_end, window_time
        |) L
        |JOIN (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(
        |    CUMULATE(
        |      TABLE MyTable2, DESCRIPTOR(proctime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |  GROUP BY a, window_start, window_end, window_time
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.a = R.a
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testWindowJoinWithNonEqui(): Unit = {
    val sql =
      """
        |SELECT L.*, R.*
        |FROM (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(
        |    CUMULATE(
        |      TABLE MyTable, DESCRIPTOR(proctime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |  GROUP BY a, window_start, window_end, window_time
        |) L
        |JOIN (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(
        |    CUMULATE(
        |      TABLE MyTable2, DESCRIPTOR(proctime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |  GROUP BY a, window_start, window_end, window_time
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.a = R.a AND
        | CAST(L.window_start AS BIGINT) > R.uv
      """.stripMargin
    util.verifyRelPlan(sql)
  }
  // ----------------------------------------------------------------------------------------
  // Window Join could propagate time attribute
  // ----------------------------------------------------------------------------------------

  @Test
  def testTimeAttributePropagateForWindowJoin(): Unit = {
    util.tableEnv.executeSql(s"""
                                |CREATE TABLE MyTable3 (
                                |  a INT,
                                |  b STRING NOT NULL,
                                |  c BIGINT,
                                |  rowtime TIMESTAMP(3),
                                |  proctime as PROCTIME(),
                                |  WATERMARK FOR rowtime AS rowtime - INTERVAL '1' SECOND
                                |) with (
                                |  'connector' = 'values'
                                |)
                                |""".stripMargin)

    util.tableEnv.executeSql(
      """
        |CREATE VIEW tmp AS
        |SELECT
        |  L.window_time as rowtime,
        |  L.a as a,
        |  L.b as l_b,
        |  L.c as l_c,
        |  R.b as r_b,
        |  R.c as r_c
        |FROM (
        |  SELECT *
        |  FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |) L
        |JOIN (
        |  SELECT *
        |  FROM TABLE(TUMBLE(TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.a = R.a
      """.stripMargin)

    val sql =
      """
        |SELECT tmp.*, MyTable3.* FROM tmp JOIN MyTable3 ON
        | tmp.a = MyTable3.a AND
        | tmp.rowtime BETWEEN
        |   MyTable3.rowtime - INTERVAL '10' SECOND AND
        |   MyTable3.rowtime + INTERVAL '1' HOUR
        |""".stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testTimeAttributePropagateForWindowJoin1(): Unit = {
    util.tableEnv.executeSql(s"""
                                |CREATE TABLE MyTable4 (
                                |  a INT,
                                |  b STRING NOT NULL,
                                |  c BIGINT,
                                |  rowtime TIMESTAMP(3),
                                |  proctime as PROCTIME(),
                                |  WATERMARK FOR rowtime AS rowtime - INTERVAL '1' SECOND
                                |) with (
                                |  'connector' = 'values'
                                |)
                                |""".stripMargin)

    util.tableEnv.executeSql(
      """
        |CREATE VIEW tmp1 AS
        |SELECT
        |  L.window_time as rowtime,
        |  L.a,
        |  L.cnt as l_cnt,
        |  L.uv as l_uv,
        |  R.cnt as r_cnt,
        |  R.uv as r_uv
        |FROM (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(
        |    CUMULATE(
        |      TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |  GROUP BY a, window_start, window_end, window_time
        |) L
        |JOIN (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(
        |    CUMULATE(
        |      TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |  GROUP BY a, window_start, window_end, window_time
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.a = R.a
      """.stripMargin)

    val sql =
      """
        |SELECT tmp1.*, MyTable4.* FROM tmp1 JOIN MyTable4 ON
        | tmp1.a = MyTable4.a AND
        | tmp1.rowtime BETWEEN
        |   MyTable4.rowtime - INTERVAL '10' SECOND AND
        |   MyTable4.rowtime + INTERVAL '1' HOUR
        |""".stripMargin
    util.verifyRelPlan(sql)
  }

  // ----------------------------------------------------------------------------------------
  // Window Join could propagate window properties
  // ----------------------------------------------------------------------------------------

  @Test
  def testWindowPropertyPropagateForWindowJoin(): Unit = {
    util.tableEnv.executeSql(
      """
        |CREATE VIEW tmp2 AS
        |SELECT
        |  L.window_start as window_start,
        |  L.window_end as window_end,
        |  L.a,
        |  L.cnt as l_cnt,
        |  L.uv as l_uv,
        |  R.cnt as r_cnt,
        |  R.uv as r_uv
        |FROM (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(
        |    CUMULATE(
        |      TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |  GROUP BY a, window_start, window_end, window_time
        |) L
        |JOIN (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(
        |    CUMULATE(
        |      TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |  GROUP BY a, window_start, window_end, window_time
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.a = R.a
      """.stripMargin)

    val sql =
      """
        |SELECT * FROM
        |(
        |  SELECT *,
        |    ROW_NUMBER() OVER(
        |      PARTITION BY window_start, window_end ORDER BY l_cnt DESC) as rownum
        |  FROM tmp2
        |)
        |WHERE rownum <= 3
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  // ----------------------------------------------------------------------------------------
  // Semi/AntiJoin
  // ----------------------------------------------------------------------------------------

  @Test
  def testSemiJoinIN(): Unit = {
    val sql =
      """
        |SELECT * FROM (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |  GROUP BY a, window_start, window_end, window_time
        |) L WHERE L.a IN (
        |SELECT a FROM (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(TUMBLE(TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |  GROUP BY a, window_start, window_end, window_time
        |) R
        |WHERE L.window_start = R.window_start AND L.window_end = R.window_end)
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testSemiExist(): Unit = {
    val sql =
      """
        |SELECT * FROM (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |  GROUP BY a, window_start, window_end, window_time
        |) L WHERE EXISTS (
        |SELECT * FROM (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(TUMBLE(TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |  GROUP BY a, window_start, window_end, window_time
        |) R
        |WHERE L.window_start = R.window_start AND L.window_end = R.window_end AND L.a = R.a)
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testAntiJoinNotIN(): Unit = {
    val sql =
      """
        |SELECT * FROM (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |  GROUP BY a, window_start, window_end, window_time
        |) L WHERE L.a NOT IN (
        |SELECT a FROM (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(TUMBLE(TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |  GROUP BY a, window_start, window_end, window_time
        |) R
        |WHERE L.window_start = R.window_start AND L.window_end = R.window_end)
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testAntiJoinNotExist(): Unit = {
    val sql =
      """
        |SELECT * FROM (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |  GROUP BY a, window_start, window_end, window_time
        |) L WHERE NOT EXISTS (
        |SELECT * FROM (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(TUMBLE(TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |  GROUP BY a, window_start, window_end, window_time
        |) R
        |WHERE L.window_start = R.window_start AND L.window_end = R.window_end AND L.a = R.a)
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  // ----------------------------------------------------------------------------------------
  // Test IS NOT DISTINCT FROM
  // ----------------------------------------------------------------------------------------

  @Test
  def testJoinWithIsNotDistinctFrom(): Unit = {
    val sql =
      """
        |SELECT L.*, R.*
        |FROM (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |  GROUP BY a, window_start, window_end, window_time
        |) L
        |JOIN (
        |  SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    window_time,
        |    count(*) as cnt,
        |    count(distinct c) AS uv
        |  FROM TABLE(TUMBLE(TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |  GROUP BY a, window_start, window_end, window_time
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND
        |L.a IS NOT DISTINCT FROM R.a
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testJoinToMultiSink(): Unit = {
    val sourceDdl =
      """
        |CREATE TABLE food_order (
        | user_id STRING,
        | order_id STRING,
        | amount INT,
        | event_time TIMESTAMP(3),
        | WATERMARK FOR event_time AS event_time
        |) WITH (
        |'connector' = 'values')
        |""".stripMargin
    util.tableEnv.executeSql(sourceDdl)

    val query =
      """
        |CREATE TEMPORARY VIEW food_view AS
        |WITH food AS ( 
        |  SELECT user_id, 
        |         window_start, 
        |         window_end 
        |  FROM TABLE(TUMBLE(TABLE food_order, DESCRIPTOR(event_time), INTERVAL '1' MINUTES)) 
        |  GROUP BY 
        |  user_id,
        |  window_start,
        |  window_end)
        |SELECT food.window_start
        |     ,food.window_end
        |     ,food.user_id
        |     ,DATE_FORMAT(food.window_end + INTERVAL '7' HOUR, 'yyyyMMdd') AS dt
        |     ,DATE_FORMAT(food.window_end + INTERVAL '7' HOUR, 'HH') AS `hour`
        |FROM food
        |LEFT JOIN food AS a ON food.user_id = a.user_id
        |AND food.window_start = a.window_start
        |AND food.window_end = a.window_end
        |""".stripMargin

    util.tableEnv.executeSql(query)

    val sinkDdl =
      """
        |CREATE TABLE %s (
        | window_start TIMESTAMP(3),
        | window_end TIMESTAMP(3),
        | user_id STRING,
        | dt STRING,
        | `hour` STRING
        |) WITH (
        | 'connector' = 'values')
        |""".stripMargin
    util.tableEnv.executeSql(sinkDdl.format("sink1"))
    util.tableEnv.executeSql(sinkDdl.format("sink2"))

    val statementSet = util.tableEnv.createStatementSet()
    statementSet.addInsertSql("INSERT INTO sink1 SELECT * FROM food_view")
    statementSet.addInsertSql("INSERT INTO sink2 SELECT * FROM food_view")
    util.verifyRelPlan(statementSet)
  }
}
