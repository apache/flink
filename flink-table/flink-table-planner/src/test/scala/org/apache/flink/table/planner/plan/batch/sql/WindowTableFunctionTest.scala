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
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.{Before, Test}

import java.sql.Timestamp

class WindowTableFunctionTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def before(): Unit = {
    util.addTableSource[(Timestamp, Long, Int, String)]("MyTable", 'ts, 'a, 'b, 'c)
    util.addTableSource[(Int, Long, String, Int, Timestamp)]("MyTable1", 'a, 'b, 'c, 'd, 'ts)
    util.tableEnv.executeSql(s"""
                                |create table MyTable2 (
                                |  a int,
                                |  b bigint,
                                |  c as proctime()
                                |) with (
                                |  'connector' = 'COLLECTION'
                                |)
                                |""".stripMargin)
  }

  @Test
  def testInvalidTimeColType(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM TABLE(TUMBLE(TABLE MyTable1, DESCRIPTOR(b), INTERVAL '15' MINUTE))
        |""".stripMargin
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage(
      "The window function TUMBLE(TABLE table_name, DESCRIPTOR(timecol), datetime interval"
        + "[, datetime interval]) requires the timecol to be TIMESTAMP or TIMESTAMP_LTZ, "
        + "but is BIGINT.")
    util.verifyExplain(sql)
  }

  @Test
  def testTumbleTVF(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM TABLE(TUMBLE(TABLE MyTable1, DESCRIPTOR(ts), INTERVAL '15' MINUTE))
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testTumbleTVFProctime(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM TABLE(TUMBLE(TABLE MyTable2, DESCRIPTOR(c), INTERVAL '15' MINUTE))
        |""".stripMargin
    expectedException.expect(classOf[TableException])
    expectedException.expectMessage("Processing time Window TableFunction is not supported yet.")
    util.verifyExplain(sql)
  }

  @Test
  def testHopTVF(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM TABLE(HOP(TABLE MyTable1, DESCRIPTOR(ts), INTERVAL '1' HOUR, INTERVAL '2' HOUR))
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testHopTVFProctime(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM TABLE(HOP(TABLE MyTable2, DESCRIPTOR(c), INTERVAL '1' HOUR, INTERVAL '2' HOUR))
        |""".stripMargin
    expectedException.expect(classOf[TableException])
    expectedException.expectMessage("Processing time Window TableFunction is not supported yet.")
    util.verifyExplain(sql)
  }

  @Test
  def testCumulateTVF(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM TABLE(
        | CUMULATE(TABLE MyTable1, DESCRIPTOR(ts), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testCumulateTVFProctime(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM TABLE(
        | CUMULATE(TABLE MyTable2, DESCRIPTOR(c), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |""".stripMargin
    expectedException.expect(classOf[TableException])
    expectedException.expectMessage("Processing time Window TableFunction is not supported yet.")
    util.verifyExplain(sql)
  }

  @Test
  def testWindowAggregate(): Unit = {
    val sql =
      """
        |SELECT
        |  window_start,
        |  window_end,
        |  a,
        |  MAX(c)
        |FROM TABLE(TUMBLE(TABLE MyTable1, DESCRIPTOR(ts), INTERVAL '3' SECOND))
        |GROUP BY window_start, window_end, a
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testCascadingWindowAggregate(): Unit = {
    val sql =
      """
        |SELECT window_start, window_end, b, SUM(cnt)
        |FROM (
        |  SELECT
        |    window_start, window_end, a, b, COUNT(1) AS cnt
        |  FROM TABLE(TUMBLE(TABLE MyTable1, DESCRIPTOR(ts), INTERVAL '3' SECOND))
        |  GROUP BY window_start, window_end, a, b
        |)
        |GROUP BY window_start, window_end, b
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testWindowJoin(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT *
        |  FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(ts), INTERVAL '15' MINUTE))
        |) L
        |JOIN (
        |  SELECT *
        |  FROM TABLE(TUMBLE(TABLE MyTable1, DESCRIPTOR(ts), INTERVAL '15' MINUTE))
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.a = R.b
      """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testWindowRank(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |SELECT *,
        |  RANK() OVER(PARTITION BY a, window_start, window_end ORDER BY b) as rownum
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(ts), INTERVAL '15' MINUTE))
        |)
        |WHERE rownum <= 3
      """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testProjectWTFTransposeRule(): Unit = {
    val sql =
      """
        |SELECT
        |  MAX(c)
        |FROM TABLE(TUMBLE(TABLE MyTable1, DESCRIPTOR(ts), INTERVAL '3' SECOND))
        |GROUP BY window_start, window_end, a
        |""".stripMargin
    util.verifyExecPlan(sql)
  }
}
