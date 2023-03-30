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
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.planner.plan.optimize.RelNodeBlockPlanBuilder
import org.apache.flink.table.planner.utils.TableTestBase
import org.apache.flink.table.types.logical.{BigIntType, IntType}

import org.junit.Test

class LegacySinkTest extends TableTestBase {

  val LONG = new BigIntType()
  val INT = new IntType()

  private val util = batchTestUtil()
  util.addDataStream[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

  @Test
  def testSingleSink(): Unit = {
    val table = util.tableEnv.sqlQuery("SELECT COUNT(*) AS cnt FROM MyTable GROUP BY a")
    val sink = util.createCollectTableSink(Array("a"), Array(LONG))
    util.verifyRelPlanInsert(table, sink, "sink")
  }

  @Test
  def testMultiSinks(): Unit = {
    val stmtSet = util.tableEnv.createStatementSet()
    util.tableEnv.getConfig.set(
      RelNodeBlockPlanBuilder.TABLE_OPTIMIZER_REUSE_OPTIMIZE_BLOCK_WITH_DIGEST_ENABLED,
      Boolean.box(true))
    val table1 = util.tableEnv.sqlQuery("SELECT SUM(a) AS sum_a, c FROM MyTable GROUP BY c")
    util.tableEnv.createTemporaryView("table1", table1)
    val table2 = util.tableEnv.sqlQuery("SELECT SUM(sum_a) AS total_sum FROM table1")
    val table3 = util.tableEnv.sqlQuery("SELECT MIN(sum_a) AS total_min FROM table1")

    val sink1 = util.createCollectTableSink(Array("total_sum"), Array(INT))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("sink1", sink1)
    stmtSet.addInsert("sink1", table2)

    val sink2 = util.createCollectTableSink(Array("total_min"), Array(INT))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("sink2", sink2)
    stmtSet.addInsert("sink2", table3)

    util.verifyExecPlan(stmtSet)
  }

  @Test
  def testTableHints(): Unit = {
    util.tableEnv.executeSql(s"""
                                |CREATE TABLE MyTable (
                                |  `a` INT,
                                |  `b` BIGINT,
                                |  `c` STRING
                                |) WITH (
                                |  'connector' = 'values',
                                |  'bounded' = 'true'
                                |)
       """.stripMargin)

    util.tableEnv.executeSql(s"""
                                |CREATE TABLE MySink (
                                |  `a` INT,
                                |  `b` BIGINT,
                                |  `c` STRING
                                |) WITH (
                                |  'connector' = 'OPTIONS',
                                |  'path' = '/tmp/test'
                                |)
       """.stripMargin)
    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql(
      "insert into MySink /*+ OPTIONS('path' = '/tmp1') */ select * from MyTable")
    stmtSet.addInsertSql(
      "insert into MySink /*+ OPTIONS('path' = '/tmp2') */ select * from MyTable")

    util.verifyExecPlan(stmtSet)
  }
}
