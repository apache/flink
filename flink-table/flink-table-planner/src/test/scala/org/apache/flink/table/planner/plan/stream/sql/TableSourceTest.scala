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

import org.apache.flink.core.testutils.FlinkMatchers.containsMessage
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.planner.utils._

import org.junit.Test

class TableSourceTest extends TableTestBase {

  private val util = streamTestUtil()

  @Test
  def testTableSourceWithTimestampRowTimeField(): Unit = {
    val ddl =
      s"""
         |CREATE TABLE rowTimeT (
         |  id int,
         |  rowtime timestamp(3),
         |  val bigint,
         |  name varchar(32),
         |  watermark for rowtime as rowtime
         |) WITH (
         |  'connector' = 'values',
         |  'bounded' = 'false'
         |)
       """.stripMargin
    util.tableEnv.executeSql(ddl)

    util.verifyExecPlan("SELECT rowtime, id, name, val FROM rowTimeT")
  }

  @Test
  def testTableSourceWithSourceWatermarks(): Unit = {
    val ddl =
      s"""
         |CREATE TABLE rowTimeT (
         |  id INT,
         |  rowtime TIMESTAMP(3),
         |  val BIGINT,
         |  name VARCHAR(32),
         |  WATERMARK FOR rowtime AS SOURCE_WATERMARK()
         |) WITH (
         |  'connector' = 'values',
         |  'bounded' = 'false',
         |  'disable-lookup' = 'true',
         |  'enable-watermark-push-down' = 'true'
         |)
       """.stripMargin
    util.tableEnv.executeSql(ddl)

    util.verifyExecPlan("SELECT rowtime, id, name, val FROM rowTimeT")
  }

  @Test
  def testRowTimeTableSourceGroupWindow(): Unit = {
    val ddl =
      s"""
         |CREATE TABLE rowTimeT (
         |  id int,
         |  rowtime timestamp(3),
         |  val bigint,
         |  name varchar(32),
         |  watermark for rowtime as rowtime
         |) WITH (
         |  'connector' = 'values',
         |  'bounded' = 'false'
         |)
       """.stripMargin
    util.tableEnv.executeSql(ddl)

    val sqlQuery =
      """
        |SELECT name,
        |    TUMBLE_END(rowtime, INTERVAL '10' MINUTE),
        |    AVG(val)
        |FROM rowTimeT WHERE val > 100
        |   GROUP BY name, TUMBLE(rowtime, INTERVAL '10' MINUTE)
      """.stripMargin

    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testProctimeOnWatermarkSpec(): Unit = {
    thrown.expect(classOf[ValidationException])
    thrown.expect(
      containsMessage("A watermark can not be defined for a processing-time attribute."))
    val ddl =
      s"""
         |CREATE TABLE procTimeT (
         |  id int,
         |  val bigint,
         |  name varchar(32),
         |  pTime as PROCTIME(),
         |  watermark for pTime as pTime
         |) WITH (
         |  'connector' = 'values',
         |  'bounded' = 'false'
         |)
       """.stripMargin
    util.tableEnv.executeSql(ddl)

    util.verifyExecPlan("SELECT pTime, id, name, val FROM procTimeT")
  }

  @Test
  def testProjectWithoutRowtime(): Unit = {
    val ddl =
      s"""
         |CREATE TABLE T (
         |  id int,
         |  rtime timestamp(3),
         |  val bigint,
         |  name varchar(32),
         |  ptime as PROCTIME(),
         |  watermark for rtime as rtime
         |) WITH (
         |  'connector' = 'values',
         |  'bounded' = 'false'
         |)
       """.stripMargin
    util.tableEnv.executeSql(ddl)

    util.verifyExecPlan("SELECT ptime, name, val, id FROM T")
  }

  @Test
  def testProjectWithoutProctime(): Unit = {
    val ddl =
      s"""
         |CREATE TABLE T (
         |  id int,
         |  rtime timestamp(3),
         |  val bigint,
         |  name varchar(32),
         |  ptime as PROCTIME(),
         |  watermark for rtime as rtime
         |) WITH (
         |  'connector' = 'values',
         |  'bounded' = 'false'
         |)
       """.stripMargin
    util.tableEnv.executeSql(ddl)

    util.verifyExecPlan("select name, val, rtime, id from T")
  }

  @Test
  def testProjectOnlyRowtime(): Unit = {
    val ddl =
      s"""
         |CREATE TABLE T (
         |  id int,
         |  rtime timestamp(3),
         |  val bigint,
         |  name varchar(32),
         |  ptime as PROCTIME(),
         |  watermark for rtime as rtime
         |) WITH (
         |  'connector' = 'values',
         |  'bounded' = 'false'
         |)
       """.stripMargin
    util.tableEnv.executeSql(ddl)

    util.verifyExecPlan("SELECT rtime FROM T")
  }

  @Test
  def testNestedProject(): Unit = {
    val ddl =
      s"""
         |CREATE TABLE T (
         |  id int,
         |  deepNested row<nested1 row<name string, `value` int>,
         |                 nested2 row<num int, flag boolean>>,
         |  nested row<name string, `value` int>,
         |  name string
         |) WITH (
         |  'connector' = 'values',
         |  'nested-projection-supported' = 'true',
         |  'bounded' = 'false'
         |)
       """.stripMargin
    util.tableEnv.executeSql(ddl)

    val sqlQuery =
      """
        |SELECT id,
        |    deepNested.nested1.name AS nestedName,
        |    nested.`value` AS nestedValue,
        |    deepNested.nested2.flag AS nestedFlag,
        |    deepNested.nested2.num AS nestedNum
        |FROM T
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testProjectWithoutInputRef(): Unit = {
    val ddl =
      s"""
         |CREATE TABLE T (
         |  id int,
         |  name varchar(32)
         |) WITH (
         |  'connector' = 'values',
         |  'bounded' = 'false'
         |)
       """.stripMargin
    util.tableEnv.executeSql(ddl)

    util.verifyExecPlan("SELECT COUNT(1) FROM T")
  }

  @Test
  def testNestedProjectWithMetadata(): Unit = {
    testNestedProjectWithMetadataBase(true)
  }

  @Test
  def testNoNestedProjectWithMetadata(): Unit = {
    testNestedProjectWithMetadataBase(false)
  }

  private def testNestedProjectWithMetadataBase(supportsNestedProjectionPushDown: Boolean): Unit = {
    val ddl =
      s"""
         |CREATE TABLE T (
         |  id int,
         |  deepNested row<nested1 row<name string, `value` int>,
         |    nested2 row<num int, flag boolean>>,
         |  metadata_1 int metadata,
         |  metadata_2 string metadata
         |) WITH (
         |  'connector' = 'values',
         |  'nested-projection-supported' = '$supportsNestedProjectionPushDown',
         |  'bounded' = 'true',
         |  'readable-metadata' = 'metadata_1:INT, metadata_2:STRING, metadata_3:BIGINT'
         |)
         |""".stripMargin
    util.tableEnv.executeSql(ddl)

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
  def testNestedProjectWithItem(): Unit = {
    util.tableEnv.executeSql(
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
    )

    // TODO: always push projection into table source in FLINK-22118
    util.verifyExecPlan(
      s"""
         |SELECT
         |  `result`.`data_arr`[`id`].`value`,
         |  `result`.`data_map`['item'].`value`
         |FROM NestedItemTable
         |""".stripMargin
    )
  }

  private def prepareDdlWithPushProjectAndMetaData(
      projectionPushDown: Boolean,
      readsMeta: Boolean): Unit = {
    val ddl =
      s"""
         |CREATE TABLE src (
         |  id int,
         |  name varchar,
         |  tags varchar ${if (readsMeta) "METADATA VIRTUAL" else ""},
         |  op varchar ${if (readsMeta) "METADATA VIRTUAL" else ""},
         |  ts timestamp(3) ${if (readsMeta) "METADATA VIRTUAL" else ""},
         |  ts1 as ts + interval '10' second
         |) WITH (
         |  'connector' = 'values',
         |  ${if (readsMeta) "'readable-metadata'='tags:varchar,op:varchar,ts:timestamp(3)'," else ""}
         |  'enable-projection-push-down' = '$projectionPushDown'
         |)""".stripMargin

    util.tableEnv.executeSql(ddl)
  }

  @Test
  def testReadsMetaDataWithDifferentOrder(): Unit = {
    prepareDdlWithPushProjectAndMetaData(false, true)

    util.verifyExecPlan("SELECT ts, id, name, tags, op FROM src")
  }

  @Test
  def testReadsMetaDataWithoutProjectionPushDown(): Unit = {
    prepareDdlWithPushProjectAndMetaData(false, true)

    util.verifyExecPlan("SELECT id, ts, tags FROM src")
  }

  @Test
  def testReadsComputedColumnWithoutProjectionPushDown(): Unit = {
    prepareDdlWithPushProjectAndMetaData(false, true)

    util.verifyExecPlan("SELECT id, ts1, op FROM src")
  }

  @Test
  def testReadsComputedColumnWithProjectionPushDown(): Unit = {
    prepareDdlWithPushProjectAndMetaData(true, true)

    util.verifyExecPlan("SELECT id, ts1, op FROM src")
  }

  @Test
  def testReadsMetaDataWithProjectionPushDown(): Unit = {
    prepareDdlWithPushProjectAndMetaData(true, true)

    util.verifyExecPlan("SELECT id, ts, tags FROM src")
  }

  @Test
  def testProjectionPushDownOnly(): Unit = {
    prepareDdlWithPushProjectAndMetaData(true, false)

    util.verifyExecPlan("SELECT id, ts1, tags FROM src")
  }
}
