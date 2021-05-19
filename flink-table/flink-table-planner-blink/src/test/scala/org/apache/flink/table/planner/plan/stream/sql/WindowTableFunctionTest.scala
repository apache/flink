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

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.planner.utils.TableTestBase
import org.junit.Test

/**
 * Tests for window table-valued function.
 */
class WindowTableFunctionTest extends TableTestBase {

  private val util = streamTestUtil()
  util.tableEnv.executeSql(
    s"""
       |CREATE TABLE MyTable (
       |  a INT,
       |  b BIGINT,
       |  c STRING,
       |  d DECIMAL(10, 3),
       |  rowtime TIMESTAMP(3),
       |  proctime as PROCTIME(),
       |  WATERMARK FOR rowtime AS rowtime - INTERVAL '1' SECOND
       |) with (
       |  'connector' = 'values'
       |)
       |""".stripMargin)

  @Test
  def testTumbleTVF(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |""".stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testTumbleTVFProctime(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(proctime), INTERVAL '15' MINUTE))
        |""".stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testHopTVF(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM TABLE(
        | HOP(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
        |""".stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testCumulateTVF(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM TABLE(
        | CUMULATE(TABLE MyTable, DESCRIPTOR(proctime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |""".stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testWindowOnNonTimeAttribute(): Unit = {
    util.tableEnv.executeSql(
      """
        |CREATE VIEW v1 AS
        |SELECT *, LOCALTIMESTAMP AS cur_time
        |FROM MyTable
        |""".stripMargin)
    val sql =
      """
        |SELECT *
        |FROM TABLE(
        | TUMBLE(TABLE v1, DESCRIPTOR(cur_time), INTERVAL '15' MINUTE))
        |""".stripMargin
    thrown.expectMessage("requires the timecol is a time attribute type, but is TIMESTAMP(3)")
    thrown.expect(classOf[ValidationException])
    util.verifyRelPlan(sql)
  }

  @Test
  def testConflictingFieldNames(): Unit = {
    util.tableEnv.executeSql(
      """
        |CREATE VIEW v1 AS
        |SELECT *, rowtime AS window_start
        |FROM MyTable
        |""".stripMargin)
    val sql =
      """
        |SELECT *
        |FROM TABLE(
        | TUMBLE(TABLE v1, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |""".stripMargin

    thrown.expectMessage("Column 'window_start' is ambiguous")
    thrown.expect(classOf[ValidationException])
    util.verifyRelPlan(sql)
  }

  @Test
  def testUnsupported(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |""".stripMargin

    thrown.expectMessage("Currently Flink doesn't support individual window " +
      "table-valued function TUMBLE(time_col=[rowtime], size=[15 min]).\n " +
      "Please use window table-valued function with aggregate together " +
      "using window_start and window_end as group keys.")
    thrown.expect(classOf[UnsupportedOperationException])
    util.verifyExplain(sql)
  }

  @Test
  def testInvalidTumbleParameters(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM TABLE(TUMBLE(
        |   TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE, INTERVAL '5' MINUTE))
        |""".stripMargin

    thrown.expectMessage("Supported form(s): " +
      "TUMBLE(TABLE table_name, DESCRIPTOR(timecol), datetime interval)")
    thrown.expect(classOf[ValidationException])
    util.verifyExplain(sql)
  }

  @Test
  def testInvalidHopParameters(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM TABLE(
        |  HOP(
        |    TABLE MyTable,
        |    DESCRIPTOR(rowtime),
        |    INTERVAL '1' MINUTE,
        |    INTERVAL '15' MINUTE,
        |    INTERVAL '5' MINUTE))
        |""".stripMargin

    thrown.expectMessage("Supported form(s): " +
      "HOP(TABLE table_name, DESCRIPTOR(timecol), datetime interval, datetime interval)")
    thrown.expect(classOf[ValidationException])
    util.verifyExplain(sql)
  }

  @Test
  def testInvalidCumulateParameters(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM TABLE(
        |  CUMULATE(
        |    TABLE MyTable,
        |    DESCRIPTOR(rowtime),
        |    INTERVAL '1' MINUTE,
        |    INTERVAL '15' MINUTE,
        |    INTERVAL '5' MINUTE))
        |""".stripMargin

    thrown.expectMessage("Supported form(s): " +
      "CUMULATE(TABLE table_name, DESCRIPTOR(timecol), datetime interval, datetime interval)")
    thrown.expect(classOf[ValidationException])
    util.verifyExplain(sql)
  }

}
