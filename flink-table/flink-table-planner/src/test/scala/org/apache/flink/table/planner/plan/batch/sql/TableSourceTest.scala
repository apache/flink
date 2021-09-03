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

import org.apache.flink.table.api.config.TableConfigOptions
import org.apache.flink.table.planner.plan.optimize.RelNodeBlockPlanBuilder
import org.apache.flink.table.planner.utils._

import org.junit.{Before, Test}

class TableSourceTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    val ddl =
      s"""
         |CREATE TABLE ProjectableTable (
         |  a int,
         |  b bigint,
         |  c varchar(32)
         |) WITH (
         |  'connector' = 'values',
         |  'nested-projection-supported' = 'true',
         |  'bounded' = 'true'
         |)
       """.stripMargin
    util.tableEnv.executeSql(ddl)
    val ddl2 =
      """
        |CREATE TABLE NestedTable (
        |  id int,
        |  deepNested row<nested1 row<name string, `value` int>, nested2 row<num int, flag
        |  boolean>>,
        |  nested row<name string, `value` int>,
        |  name string
        |) WITH (
        | 'connector' = 'values',
        | 'nested-projection-supported' = 'true',
        | 'bounded' = 'true'
        |)
        |""".stripMargin
    util.tableEnv.executeSql(ddl2)
    val ddl3 =
      s"""
         |CREATE TABLE T (
         |  id int,
         |  deepNested row<nested1 row<name string, `value` int>,
         |    nested2 row<num int, flag boolean>>,
         |  metadata_1 int metadata,
         |  metadata_2 string metadata
         |) WITH (
         |  'connector' = 'values',
         |  'nested-projection-supported' = 'true',
         |  'bounded' = 'true',
         |  'readable-metadata' =
         |    'metadata_1:INT, metadata_2:STRING, metadata_3:BIGINT'
         |)
         |""".stripMargin
    util.tableEnv.executeSql(ddl3)
    val ddl4 =
      s"""
         |CREATE TABLE NestedItemTable (
         |  `id` INT,
         |  `name` STRING,
         |  `result` ROW<
         |     `data_arr` ROW<`value` BIGINT> ARRAY,
         |     `data_map` MAP<STRING, ROW<`value` BIGINT>>>,
         |  `extra` STRING
         |  ) WITH (
         |    'connector' = 'values',
         |    'nested-projection-supported' = 'true',
         |    'bounded' = 'true'
         |)
         |""".stripMargin
    util.tableEnv.executeSql(ddl4)
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
         |""".stripMargin)
  }

  @Test
  def testSimpleProject(): Unit = {
    util.verifyExecPlan("SELECT a, c FROM ProjectableTable")
  }

  @Test
  def testSimpleProjectWithProctime(): Unit = {
    util.verifyExecPlan("SELECT a, c, PROCTIME() FROM ProjectableTable")
  }

  @Test
  def testProjectWithoutInputRef(): Unit = {
    util.verifyExecPlan("SELECT COUNT(1) FROM ProjectableTable")
  }

  @Test
  def testNestedProject(): Unit = {
    val sqlQuery =
      """
        |SELECT id,
        |    deepNested.nested1.name AS nestedName,
        |    nested.`value` AS nestedValue,
        |    deepNested.nested2.flag AS nestedFlag,
        |    deepNested.nested2.num AS nestedNum
        |FROM NestedTable
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testNestProjectWithMetadata(): Unit = {
    val sqlQuery =
      """
        |SELECT id,
        |       deepNested.nested1 AS nested1,
        |       deepNested.nested1.`value` + deepNested.nested2.num + metadata_1 as results
        |FROM T
        |""".stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testNestedProjectFieldWithITEM(): Unit = {
    //TODO: always push projection into table source in FLINK-22118
    util.verifyExecPlan(
      s"""
         |SELECT
         |  `result`.`data_arr`[`id`].`value`,
         |  `result`.`data_map`['item'].`value`
         |FROM NestedItemTable
         |""".stripMargin
    )
  }

  @Test
  def testTableHintWithDifferentOptions(): Unit = {
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

    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql(
      """
        |insert into MySink select a,b,c from MyTable
        |  /*+ OPTIONS('source.num-element-to-skip'='1') */
        |""".stripMargin)
    stmtSet.addInsertSql(
      """
        |insert into MySink select a,b,c from MyTable
        |  /*+ OPTIONS('source.num-element-to-skip'='2') */
        |""".stripMargin)

    util.verifyExecPlan(stmtSet)
  }

  @Test
  def testTableHintWithSameOptions(): Unit = {
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

    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql(
      """
        |insert into MySink select a,b,c from MyTable
        |  /*+ OPTIONS('source.num-element-to-skip'='1') */
        |""".stripMargin)
    stmtSet.addInsertSql(
      """
        |insert into MySink select a,b,c from MyTable
        |  /*+ OPTIONS('source.num-element-to-skip'='1') */
        |""".stripMargin)

    util.verifyExecPlan(stmtSet)
  }

  @Test
  def testTableHintWithDigestReuseForLogicalTableScan(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      RelNodeBlockPlanBuilder.TABLE_OPTIMIZER_REUSE_OPTIMIZE_BLOCK_WITH_DIGEST_ENABLED, true)
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

    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql(
      """
        |insert into MySink
        |select a,b,c from MyTable /*+ OPTIONS('source.num-element-to-skip'='0') */
        |union all
        |select a,b,c from MyTable /*+ OPTIONS('source.num-element-to-skip'='1') */
        |""".stripMargin)
    stmtSet.addInsertSql(
      """
        |insert into MySink select a,b,c from MyTable
        |  /*+ OPTIONS('source.num-element-to-skip'='2') */
        |""".stripMargin)

    util.verifyExecPlan(stmtSet)
  }

  @Test
  def testBuiltInFunctionWithFilterPushdown(): Unit = {
    util.verifyExecPlan("SELECT a FROM ProjectableTable WHERE IFNULL(a, 1) = 1")
  }
}
