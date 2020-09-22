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
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.planner.utils.{StreamTableTestUtil, TableTestBase}

import java.time.Duration

import org.junit.{Before, Test}

class DeduplicateTest extends TableTestBase {

  var util: StreamTableTestUtil = _

  @Before
  def setUp(): Unit = {
    util = streamTestUtil()
    util.addDataStream[(Int, String, Long)](
      "MyTable", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)
  }

  @Test
  def testInvalidRowNumberConditionOnProctime(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT a, ROW_NUMBER() OVER (PARTITION BY b ORDER BY proctime DESC) as rank_num
        |  FROM MyTable)
        |WHERE rank_num = 2
      """.stripMargin

    // the rank condition is not 1, so it will not be translate to LastRow, but Rank
    util.verifyPlan(sql)
  }

  @Test
  def testInvalidRowNumberConditionOnRowtime(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT a, ROW_NUMBER() OVER (PARTITION BY b ORDER BY rowtime DESC) as rank_num
        |  FROM MyTable)
        |WHERE rank_num = 3
      """.stripMargin

    // the rank condition is not 1, so it will not be translate to LastRow, but Rank
    util.verifyPlan(sql)
  }

  @Test
  def testLastRowWithWindowOnRowtime(): Unit = {
    // lastRow on rowtime followed by group window is not supported now.
    util.tableEnv.getConfig.getConfiguration
      .set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofMillis(500))
    util.addTable(
      """
        |CREATE TABLE T (
        | `a` INT,
        | `b` STRING,
        | `ts` TIMESTAMP(3),
        | WATERMARK FOR `ts` AS `ts`
        |) WITH (
        | 'connector' = 'COLLECTION',
        | 'is-bounded' = 'false'
        |)
      """.stripMargin
    )

    val deduplicateSQl =
      """
        |(
        |SELECT a, b, ts
        |FROM (
        |  SELECT *,
        |    ROW_NUMBER() OVER (PARTITION BY a ORDER BY ts DESC) as rowNum
        |  FROM T
        |)
        |WHERE rowNum = 1
        |)
      """.stripMargin
    val windowSql =
      s"""
         |select b, sum(a), TUMBLE_START(ts, INTERVAL '0.004' SECOND)
         |FROM $deduplicateSQl
         |GROUP BY b, TUMBLE(ts, INTERVAL '0.004' SECOND)
      """.stripMargin

    thrown.expect(classOf[TableException])
    thrown.expectMessage("GroupWindowAggregate doesn't support consuming update " +
      "and delete changes which is produced by node Rank(")
    util.verifyExplain(windowSql)
  }

  @Test
  def testSimpleFirstRowOnRowtime(): Unit = {
    // Deduplicate does not support sort on rowtime now, so it is translated to Rank currently
    val sql =
      """
        |SELECT a, b, c
        |FROM (
        |  SELECT *,
        |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY rowtime ASC) as rank_num
        |  FROM MyTable)
        |WHERE rank_num <= 1
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testSimpleLastRowOnRowtime(): Unit = {
    // Deduplicate does not support sort on rowtime now, so it is translated to Rank currently
    val sql =
      """
        |SELECT a, b, c
        |FROM (
        |  SELECT *,
        |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY rowtime DESC) as rank_num
        |  FROM MyTable)
        |WHERE rank_num = 1
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testSimpleLastRowOnProctime(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT *,
        |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY proctime DESC) as rank_num
        |  FROM MyTable)
        |WHERE rank_num = 1
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testSimpleLastRowOnBuiltinProctime(): Unit = {
    val sqlQuery =
      """
        |SELECT *
        |FROM (
        |  SELECT *,
        |    ROW_NUMBER() OVER (ORDER BY PROCTIME() DESC) as rowNum
        |  FROM MyTable
        |)
        |WHERE rowNum = 1
      """.stripMargin

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSimpleFirstRowOnProctime(): Unit = {
    val sql =
      """
        |SELECT a, b, c
        |FROM (
        |  SELECT *,
        |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY proctime ASC) as rank_num
        |  FROM MyTable)
        |WHERE rank_num = 1
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testSimpleFirstRowOnBuiltinProctime(): Unit = {
    val sqlQuery =
      """
        |SELECT *
        |FROM (
        |  SELECT *,
        |    ROW_NUMBER() OVER (PARTITION BY a ORDER BY PROCTIME() ASC) as rowNum
        |  FROM MyTable
        |)
        |WHERE rowNum = 1
      """.stripMargin

    util.verifyPlan(sqlQuery)
  }

}
