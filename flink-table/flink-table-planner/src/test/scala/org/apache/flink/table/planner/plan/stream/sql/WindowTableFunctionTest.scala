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

import org.apache.flink.core.testutils.FlinkMatchers.containsCause
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test

/** Tests for window table-valued function. */
class WindowTableFunctionTest extends TableTestBase {

  private val util = streamTestUtil()
  util.tableEnv.executeSql(s"""
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
  def testHopTVFProctime(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM TABLE(
        | HOP(TABLE MyTable, DESCRIPTOR(proctime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
        |""".stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testCumulateTVF(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM TABLE(
        | CUMULATE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |""".stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testCumulateTVFProctime(): Unit = {
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
    util.tableEnv.executeSql("""
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
    thrown.expectCause(
      containsCause(
        new ValidationException(
          "The window function requires the timecol is a time attribute type, but is TIMESTAMP(3).")
      ))
    util.verifyRelPlan(sql)
  }

  @Test
  def testConflictingFieldNames(): Unit = {
    util.tableEnv.executeSql("""
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
  def testTumbleTVFWithOffset(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM TABLE(TUMBLE(
        |   TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE, INTERVAL '5' MINUTE))
        |""".stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testTumbleTVFWithNegativeOffset(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM TABLE(TUMBLE(
        |   TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE, INTERVAL '-5' MINUTE))
        |""".stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testTumbleTVFWithNamedParams(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM TABLE(TUMBLE(
        |   DATA => TABLE MyTable,
        |   TIMECOL => DESCRIPTOR(rowtime),
        |   SIZE => INTERVAL '15' MINUTE,
        |   `OFFSET` => INTERVAL '5' MINUTE))
        |""".stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testHopTVFWithOffset(): Unit = {
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
    util.verifyRelPlan(sql)
  }

  @Test
  def testHopTVFWithNegativeOffset(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM TABLE(
        |  HOP(
        |    TABLE MyTable,
        |    DESCRIPTOR(rowtime),
        |    INTERVAL '1' MINUTE,
        |    INTERVAL '15' MINUTE,
        |    INTERVAL '-5' MINUTE))
        |""".stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testHopTVFWithNamedParams(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM TABLE(TUMBLE(
        |   DATA => TABLE MyTable,
        |   TIMECOL => DESCRIPTOR(rowtime),
        |   SIZE => INTERVAL '15' MINUTE,
        |   `OFFSET` => INTERVAL '5' MINUTE))
        |""".stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testCumulateTVFWithOffset(): Unit = {
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
    util.verifyRelPlan(sql)
  }

  @Test
  def testCumulateTVFWithNegativeOffset(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM TABLE(
        |  CUMULATE(
        |    TABLE MyTable,
        |    DESCRIPTOR(rowtime),
        |    INTERVAL '1' MINUTE,
        |    INTERVAL '15' MINUTE,
        |    INTERVAL '-5' MINUTE))
        |""".stripMargin
    util.verifyRelPlan(sql)
  }

}
