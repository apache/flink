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

import org.junit.{Before, Test}

// TODO add more union case after aggregation and join supported
class UnionTest extends TableTestBase {

  private val util = streamTestUtil()

  @Before
  def before(): Unit = {
    util.addTableSource[(Int, Long, String)]("MyTable1", 'a, 'b, 'c)
    util.addTableSource[(Int, Long, String)]("MyTable2", 'a, 'b, 'c)
    util.addTableSource[(Int, Long, String)]("MyTable3", 'a, 'b, 'c)

    util.tableEnv.executeSql(
      s"""
         |CREATE TABLE t1 (
         |  id int,
         |  ts bigint,
         |  name varchar(32),
         |  timestamp_col timestamp(3),
         |  val bigint,
         |  timestamp_ltz_col as TO_TIMESTAMP_LTZ(ts, 3),
         |  watermark for timestamp_col as timestamp_col
         |) WITH (
         |  'connector' = 'values',
         |  'bounded' = 'false'
         |)
       """.stripMargin)

    util.tableEnv.executeSql(
      s"""
         |CREATE TABLE t2 (
         |  id int,
         |  ts bigint,
         |  name string,
         |  timestamp_col timestamp(3),
         |  timestamp_ltz_col as TO_TIMESTAMP_LTZ(ts, 3),
         |  watermark for timestamp_ltz_col as timestamp_ltz_col
         |) WITH (
         |  'connector' = 'values',
         |  'bounded' = 'false'
         |)
       """.stripMargin)

    util.tableEnv.executeSql(
      s"""
         |CREATE TABLE t3 (
         |  id int,
         |  ts bigint,
         |  name string,
         |  timestamp_col timestamp(3),
         |  timestamp_ltz_col as TO_TIMESTAMP_LTZ(ts, 3),
         |  watermark for timestamp_ltz_col as timestamp_ltz_col
         |) WITH (
         |  'connector' = 'values',
         |  'bounded' = 'false'
         |)
       """.stripMargin)
  }

  @Test
  def testUnionAll(): Unit = {
    val sqlQuery =
      """
        |SELECT a, c FROM (
        | SELECT a, c FROM MyTable1
        | UNION ALL
        | SELECT a, c FROM MyTable2
        | UNION ALL
        | SELECT a, c FROM MyTable3
        |) WHERE a > 2
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testUnionAllDiffType(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM (
        | SELECT a, b FROM MyTable1
        | UNION ALL
        | SELECT a, CAST(0 aS DECIMAL(2, 1)) FROM MyTable2)
      """.stripMargin
    util.verifyRelPlanWithType(sqlQuery)
  }

  @Test
  def testUnionDiffRowTime(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM (
        | SELECT id, ts, name, timestamp_col FROM t1
        | UNION ALL
        | SELECT id, ts, name, timestamp_ltz_col FROM t2)
      """.stripMargin

    util.verifyRelPlanWithType(sqlQuery)
  }

  @Test
  def testUnionSameRowTime(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM (
        | SELECT id, ts, name, timestamp_col, timestamp_ltz_col FROM t2
        | UNION ALL
        | SELECT  id, ts, name, timestamp_col, timestamp_ltz_col FROM t3)
      """.stripMargin

    util.verifyRelPlanWithType(sqlQuery)
  }

}
