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

import org.apache.flink.table.api._
import org.apache.flink.table.api.config.ExecutionConfigOptions.{TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, TABLE_EXEC_MINIBATCH_ENABLED, TABLE_EXEC_MINIBATCH_SIZE}
import org.apache.flink.table.planner.utils.{StreamTableTestUtil, TableTestBase}

import org.junit.jupiter.api.{BeforeEach, Test}

import java.time.Duration

class DeduplicateTest extends TableTestBase {

  var util: StreamTableTestUtil = _

  @BeforeEach
  def setUp(): Unit = {
    util = streamTestUtil()
    util.addDataStream[(Int, String, Long)](
      "MyTable",
      'a,
      'b,
      'c,
      'proctime.proctime,
      'rowtime.rowtime)
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
    util.verifyExplain(sql, ExplainDetail.CHANGELOG_MODE)
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
    util.verifyExplain(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testInvalidChangelogInput(): Unit = {
    util.tableEnv.executeSql("""
                               |create temporary table cdc (
                               | a int,
                               | b bigint,
                               | ts timestamp_ltz(3),
                               | primary key (a) not enforced,
                               | watermark for ts as ts - interval '5' second
                               |) with (
                               | 'connector' = 'values',
                               | 'changelog-mode' = 'I,UA,UB,D'
                               |)""".stripMargin)
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT a, ROW_NUMBER() OVER (PARTITION BY b ORDER BY ts DESC) as rank_num
        |  FROM cdc)
        |WHERE rank_num = 1
      """.stripMargin

    // the input is not append-only, it will not be translate to LastRow, but Rank
    util.verifyExplain(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testLastRowWithWindowOnRowtime(): Unit = {
    util.tableEnv.getConfig
      .set(TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofMillis(500))
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

    util.verifyExplain(windowSql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testSimpleFirstRowOnRowtime(): Unit = {
    val sql =
      """
        |SELECT sum(a), b, sum(c) FROM (
        |  SELECT a, b, c
        |  FROM (
        |    SELECT *,
        |        ROW_NUMBER() OVER (PARTITION BY a ORDER BY rowtime ASC) as rank_num
        |    FROM MyTable)
        |  WHERE rank_num <= 1)
        |GROUP BY b
      """.stripMargin

    util.verifyExplain(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testMiniBatchInferFirstRowOnRowtime(): Unit = {
    util.tableEnv.getConfig.set(TABLE_EXEC_MINIBATCH_ENABLED, Boolean.box(true))
    util.tableEnv.getConfig.set(TABLE_EXEC_MINIBATCH_SIZE, Long.box(3L))
    util.tableEnv.getConfig.set(TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofSeconds(1))
    val ddl =
      s"""
         |CREATE TABLE T (
         |    a INT,
         |    b VARCHAR,
         |    rowtime TIMESTAMP(3),
         |    proctime as PROCTIME(),
         |    WATERMARK FOR rowtime AS rowtime
         |) WITH (
         | 'connector' = 'COLLECTION',
         | 'is-bounded' = 'false'
         |)
         |""".stripMargin
    util.tableEnv.executeSql(ddl)
    val sql =
      """
        |SELECT COUNT(b) FROM (
        |  SELECT a, b
        |  FROM (
        |    SELECT *,
        |        ROW_NUMBER() OVER (PARTITION BY a ORDER BY rowtime ASC) as rank_num
        |    FROM T)
        |  WHERE rank_num <= 1
        |)
      """.stripMargin

    util.verifyExplain(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testSimpleLastRowOnRowtime(): Unit = {
    // indirectly check output insert only via used SUM or SUM_RETRACT aggregation function
    val sql =
      """
        |SELECT sum(a), b, sum(c) FROM (
        |  SELECT a, b, c
        |  FROM (
        |    SELECT *,
        |        ROW_NUMBER() OVER (PARTITION BY a ORDER BY rowtime DESC) as rank_num
        |    FROM MyTable)
        |  WHERE rank_num = 1)
        |GROUP BY b
      """.stripMargin

    util.verifyExplain(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testMiniBatchInferLastRowOnRowtime(): Unit = {
    util.tableEnv.getConfig.set(TABLE_EXEC_MINIBATCH_ENABLED, Boolean.box(true))
    util.tableEnv.getConfig.set(TABLE_EXEC_MINIBATCH_SIZE, Long.box(3L))
    util.tableEnv.getConfig.set(TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofSeconds(1))
    val ddl =
      s"""
         |CREATE TABLE T (
         |    a INT,
         |    b VARCHAR,
         |    rowtime TIMESTAMP(3),
         |    proctime as PROCTIME(),
         |    WATERMARK FOR rowtime AS rowtime
         |) WITH (
         | 'connector' = 'COLLECTION',
         | 'is-bounded' = 'false'
         |)
         |""".stripMargin
    util.tableEnv.executeSql(ddl)
    val sql =
      """
        |SELECT COUNT(b) FROM (
        |  SELECT a, b
        |  FROM (
        |    SELECT *,
        |        ROW_NUMBER() OVER (PARTITION BY a ORDER BY rowtime DESC) as rank_num
        |    FROM T)
        |  WHERE rank_num = 1
        |)
      """.stripMargin

    util.verifyExplain(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testSimpleLastRowOnProctime(): Unit = {
    // indirectly check output insert only via used SUM or SUM_RETRACT aggregation function
    val sql =
      """
        |SELECT sum(a), b, sum(c) FROM (
        |  SELECT a, b, c
        |  FROM (
        |    SELECT *,
        |        ROW_NUMBER() OVER (PARTITION BY a ORDER BY proctime DESC) as rank_num
        |    FROM MyTable)
        |  WHERE rank_num = 1)
        |GROUP BY b
      """.stripMargin

    util.verifyExplain(sql, ExplainDetail.CHANGELOG_MODE)
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

    util.verifyExplain(sqlQuery, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testSimpleFirstRowOnProctime(): Unit = {
    val sql =
      """
        |SELECT sum(a), b, sum(c) FROM (
        |  SELECT a, b, c
        |  FROM (
        |    SELECT *,
        |        ROW_NUMBER() OVER (PARTITION BY a ORDER BY proctime ASC) as rank_num
        |    FROM MyTable)
        |  WHERE rank_num = 1)
        |GROUP BY b
      """.stripMargin

    util.verifyExplain(sql, ExplainDetail.CHANGELOG_MODE)
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

    util.verifyExplain(sqlQuery, ExplainDetail.CHANGELOG_MODE)
  }

}
