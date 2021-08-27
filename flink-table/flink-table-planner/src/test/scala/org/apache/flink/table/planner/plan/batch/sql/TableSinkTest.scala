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
import org.apache.flink.table.api.config.TableConfigOptions
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.plan.optimize.RelNodeBlockPlanBuilder
import org.apache.flink.table.planner.runtime.utils.BatchAbstractTestBase
import org.apache.flink.table.planner.runtime.utils.TestData.smallData3
import org.apache.flink.table.planner.utils.{TableTestBase, TableTestUtil}
import org.apache.flink.table.types.logical.{BigIntType, IntType}

import org.junit.{Assert, Test}

class TableSinkTest extends TableTestBase {

  val LONG = new BigIntType()
  val INT = new IntType()

  private val util = batchTestUtil()
  util.addDataStream[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

  @Test
  def testSingleSink(): Unit = {
    util.addTable(
      s"""
         |CREATE TABLE sink (
         |  `a` BIGINT
         |) WITH (
         |  'connector' = 'values'
         |)
         |""".stripMargin)
    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql("INSERT INTO sink SELECT COUNT(*) AS cnt FROM MyTable GROUP BY a")
    util.verifyRelPlan(stmtSet)
  }

  @Test
  def testMultiSinks(): Unit = {
    util.addTable(
      s"""
         |CREATE TABLE sink1 (
         |  `total_sum` INT
         |) WITH (
         |  'connector' = 'values'
         |)
         |""".stripMargin)
    util.addTable(
      s"""
         |CREATE TABLE sink2 (
         |  `total_min` INT
         |) WITH (
         |  'connector' = 'values'
         |)
         |""".stripMargin)

    util.tableEnv.getConfig.getConfiguration.setBoolean(
      RelNodeBlockPlanBuilder.TABLE_OPTIMIZER_REUSE_OPTIMIZE_BLOCK_WITH_DIGEST_ENABLED, true)
    val table1 = util.tableEnv.sqlQuery("SELECT SUM(a) AS sum_a, c FROM MyTable GROUP BY c")
    util.tableEnv.createTemporaryView("table1", table1)
    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql("INSERT INTO sink1 SELECT SUM(sum_a) AS total_sum FROM table1")
    stmtSet.addInsertSql("INSERT INTO sink2 SELECT MIN(sum_a) AS total_min FROM table1")

    util.verifyExecPlan(stmtSet)
  }

  @Test
  def testDynamicPartWithOrderBy(): Unit = {
    util.addTable(
      s"""
         |CREATE TABLE sink (
         |  `a` INT,
         |  `b` BIGINT
         |) PARTITIONED BY (
         |  `b`
         |) WITH (
         |  'connector' = 'values'
         |)
         |""".stripMargin)
    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql("INSERT INTO sink SELECT a,b FROM MyTable ORDER BY a")
    util.verifyExecPlan(stmtSet)
  }

  @Test
  def testTableHints(): Unit = {
    util.tableEnv.executeSql(
      s"""
         |CREATE TABLE MyTable (
         |  `a` INT,
         |  `b` BIGINT,
         |  `c` STRING
         |) WITH (
         |  'connector' = 'values',
         |  'bounded' = 'true'
         |)
       """.stripMargin)

    util.tableEnv.executeSql(
      s"""
         |CREATE TABLE MySink (
         |  `a` INT,
         |  `b` BIGINT,
         |  `c` STRING
         |) WITH (
         |  'connector' = 'filesystem',
         |  'format' = 'testcsv',
         |  'path' = '/tmp/test'
         |)
       """.stripMargin)
    val stmtSet= util.tableEnv.createStatementSet()
    stmtSet.addInsertSql(
      "insert into MySink /*+ OPTIONS('path' = '/tmp1') */ select * from MyTable")
    stmtSet.addInsertSql(
      "insert into MySink /*+ OPTIONS('path' = '/tmp2') */ select * from MyTable")

    util.verifyExecPlan(stmtSet)
  }
}
